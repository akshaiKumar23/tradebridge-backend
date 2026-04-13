import os
import json
import hashlib
import datetime
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
from openai import OpenAI

from db.dynamodb import (
    get_analytics_stats_table,
    get_equity_curve_table,
    get_drawdown_curve_table,
    get_pnl_weekly_table,
    get_session_performance_table,
    get_strategies_table,
    get_trades_table,
    get_atlas_stats_table,
    get_atlas_prompts_table,
)

import logging
logger = logging.getLogger("trading_data_compressor")


# ─── Change thresholds ────────────────────────────────────────────────────────
# Only call OpenAI when at least ONE of these deltas is exceeded.

WIN_RATE_DELTA    = 2.0   # percentage points
PNL_DELTA         = 200   # USD
PF_DELTA          = 0.1   # profit factor units
TRADE_COUNT_DELTA = 3     # number of new trades


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super(DecimalEncoder, self).default(obj)


STRUCTURED_SYSTEM_PROMPT = """You are a professional trading analyst.
Analyze the provided trading data and return ONLY a valid JSON object — no markdown,
no explanation, no code fences, no statement_1/statement_2 style output.

The JSON must EXACTLY follow this schema and no other format:

{
  "summary": {
    "positive_insights_count": <integer>,
    "areas_to_improve_count": <integer>,
    "opportunities_count": <integer>,
    "overall_score": <float 0-10>,
    "overall_score_label": <string e.g. "Good performance">,
    "win_loss_summary": <string e.g. "24 wins - 11 losses">
  },
  "insights": [
    {
      "title": <string>,
      "type": <"Performance" | "Behavior" | "Risk" | "Opportunity">,
      "variant": <"success" | "warning" | "error" | "info">,
      "date": <ISO date string YYYY-MM-DD>,
      "description": <string — 1-3 sentences explaining the insight>,
      "recommendation": <string — 1 sentence actionable recommendation>
    }
  ]
}

Rules:
- Generate between 3 and 8 insights total.
- "variant" must be "success" for positive findings, "warning" for moderate issues,
  "error" for serious risks, "info" for neutral opportunities.
- Base insights strictly on the trading data provided.
- Use today's date for all insight dates.
- overall_score must be a float between 0 and 10.
- CRITICAL: Return ONLY the JSON object. No statement_N keys. No extra keys.
  No other text whatsoever. If you return anything other than this exact schema,
  it will break the application.
"""


class TradingDataCompressor:
    def __init__(self):
        # Reuses the DynamoDB singleton from db/dynamodb.py — no boto3 here
        self.openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    # ─── DynamoDB helper ──────────────────────────────────────────────────────

    def _q(self, get_table_fn, uid, limit=None, forward=True):
        try:
            table = get_table_fn()
            args = {
                "KeyConditionExpression": Key("user_id").eq(uid),
                "ScanIndexForward": forward,
            }
            if limit:
                args["Limit"] = limit
            return table.query(**args).get("Items", [])
        except Exception as e:
            logger.warning(f"DynamoDB query failed for {get_table_fn.__name__}: {e}")
            return []

    # ─── Curve summarization ──────────────────────────────────────────────────
    # Collapses full time-series arrays into 5-point summaries.
    # Cuts 60-80% of tokens sent to OpenAI vs sending raw arrays.

    def _summarize_curve(self, rows: list, key: str) -> dict:
        if not rows:
            return {"start": 0, "end": 0, "min": 0, "max": 0, "trend": "flat"}
        nums = [float(r.get(key, 0)) if isinstance(r, dict) else float(r)
                for r in rows]
        return {
            "start": round(nums[0], 2),
            "end":   round(nums[-1], 2),
            "min":   round(min(nums), 2),
            "max":   round(max(nums), 2),
            "trend": "up" if nums[-1] > nums[0] else "down" if nums[-1] < nums[0] else "flat",
        }

    def _summarize_weekly(self, values: list) -> dict:
        if not values:
            return {"best": 0, "worst": 0, "avg": 0, "count": 0}
        nums = [float(v) for v in values]
        return {
            "best":  round(max(nums), 2),
            "worst": round(min(nums), 2),
            "avg":   round(sum(nums) / len(nums), 2),
            "count": len(nums),
        }

    # ─── Payload builder ──────────────────────────────────────────────────────

    def get_llm_payload(self, user_id: str) -> dict:
        stats_items = self._q(get_analytics_stats_table, user_id, limit=1, forward=False)
        s = stats_items[0] if stats_items else {}

        sym_raw = s.get("symbols", {})
        sym_compressed = {
            k: v.get("pnl", 0)
            for k, v in sym_raw.items()
            if v.get("pnl", 0) != 0
        }

        eq_raw  = self._q(get_equity_curve_table,  user_id)
        dd_raw  = self._q(get_drawdown_curve_table, user_id)
        wk_raw  = [i["pnl"] for i in self._q(get_pnl_weekly_table, user_id)]

        sessions = self._q(get_session_performance_table, user_id)
        sess_compressed = {
            i["session"]: [i["total_pnl"], i.get("trade_count", 0)]
            for i in sessions
        }

        strats = self._q(get_strategies_table, user_id)
        trades = self._q(get_trades_table,     user_id)

        strat_counts: dict = {}
        for t in trades:
            for tag in t.get("tags", []):
                if tag.startswith("strategy#"):
                    sid = tag.split("#")[1]
                    strat_counts[sid] = strat_counts.get(sid, 0) + 1

        strat_dist = {
            st.get("title", "NA"): strat_counts.get(st["strategy_id"], 0)
            for st in strats
            if strat_counts.get(st["strategy_id"], 0) > 0
        }

        return {
            "core": {
                "tp":       s.get("total_pnl", 0),
                "tt":       s.get("total_trades", 0),
                "wr":       s.get("win_rate", 0),
                "pf":       s.get("profit_factor", 0),
                "exp":      s.get("expectancy", 0),
                "mc":       s.get("max_consecutive_losses", 0),
                "rt":       s.get("revenge_trading_count", 0),
                "avg_win":  s.get("avg_win", 0),
                "avg_loss": s.get("avg_loss", 0),
                "wins":     s.get("wins", 0),
                "losses":   s.get("losses", 0),
            },
            "sym":  sym_compressed,
            "eq":   self._summarize_curve(eq_raw, key="equity"),
            "dd":   self._summarize_curve(dd_raw, key="drawdown"),
            "wk":   self._summarize_weekly(wk_raw),
            "sess": sess_compressed,
            "strat": strat_dist,
        }

    # ─── Hash fingerprint ─────────────────────────────────────────────────────
    # KEY FIX: exclude eq/dd/wk from the hash.
    # These curve summaries can shift slightly on every sync (e.g. a new equity
    # point is appended) even when zero trades were taken, causing false cache
    # misses and unnecessary OpenAI calls.
    # Only hash metrics that require actual trades to change.

    def _hash_payload(self, payload: dict) -> str:
        hashable = {
            "core":  payload["core"],   # pnl, win_rate, profit_factor, trade count
            "sess":  payload["sess"],   # session performance
            "sym":   payload["sym"],    # per-symbol pnl
            "strat": payload["strat"],  # strategy distribution
            # "eq", "dd", "wk" intentionally excluded — volatile without trades
        }
        canonical = json.dumps(
            hashable, cls=DecimalEncoder, separators=(",", ":"), sort_keys=True
        )
        return hashlib.sha256(canonical.encode()).hexdigest()

    # ─── Threshold check ──────────────────────────────────────────────────────
    # Even if the hash changes, only call OpenAI when a metric moved enough
    # to produce meaningfully different insights.

    def _has_meaningful_change(self, new_core: dict, old_core: dict) -> bool:
        return any([
            abs(float(new_core.get("wr", 0)) - float(old_core.get("wr", 0))) >= WIN_RATE_DELTA,
            abs(float(new_core.get("tp", 0)) - float(old_core.get("tp", 0))) >= PNL_DELTA,
            abs(float(new_core.get("pf", 0)) - float(old_core.get("pf", 0))) >= PF_DELTA,
            abs(int(new_core.get("tt",   0)) - int(old_core.get("tt",   0))) >= TRADE_COUNT_DELTA,
        ])

    # ─── Main entry point ─────────────────────────────────────────────────────

    def get_or_update_atlas_stats(
        self, user_id: str, prompt_type: str = "universal"
    ) -> str:

        # 1. Build compressed payload
        current_payload = self.get_llm_payload(user_id)
        current_hash    = self._hash_payload(current_payload)

        # 2. Fetch cache
        atlas_table   = get_atlas_stats_table()
        existing_item = atlas_table.get_item(Key={"user_id": user_id}).get("Item")

        if existing_item:
            cached_response = existing_item.get("altas_prompt_response", "")

            # Layer 1 — hash check (stable metrics only)
            if existing_item.get("atlas_payload_hash") == current_hash:
                logger.info(f"Atlas hash match — cache hit for {user_id}")
                return cached_response

            # Layer 2 — threshold check (hash changed, but is it meaningful?)
            old_core = existing_item.get("atlas_core_snapshot", {})
            if not self._has_meaningful_change(current_payload["core"], old_core):
                logger.info(f"Atlas below threshold — cache hit for {user_id}")
                return cached_response

        # 3. Meaningful change — fetch prompt context from DynamoDB
        logger.info(f"Atlas meaningful change — regenerating for {user_id}")

        prompts_table = get_atlas_prompts_table()
        items = prompts_table.scan(
            FilterExpression=Attr("prompt_type").eq(prompt_type)
        ).get("Items", [])
        base_prompt = (
            items[0].get("prompt", "Analyze this trading data and return insights:")
            if items
            else "Analyze this trading data and return insights:"
        )

        payload_json = json.dumps(
            current_payload, cls=DecimalEncoder, separators=(",", ":")
        )
        final_prompt = (
            f"Additional context: {base_prompt}\n\n"
            f"Trading Data: {payload_json}\n\n"
            f"Remember: Return ONLY the JSON object matching the exact schema "
            f"defined in your system instructions. No statement_N keys, "
            f"no markdown, no extra text."
        )

        # 4. Call OpenAI
        try:
            ai_completion = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": STRUCTURED_SYSTEM_PROMPT},
                    {"role": "user",   "content": final_prompt},
                ],
                response_format={"type": "json_object"},
            )
            ai_response = ai_completion.choices[0].message.content
        except Exception as e:
            logger.error(f"OpenAI call failed: {e}", exc_info=True)
            ai_response = json.dumps({
                "summary": {
                    "positive_insights_count": 0,
                    "areas_to_improve_count":  0,
                    "opportunities_count":     0,
                    "overall_score":           0.0,
                    "overall_score_label":     "Unavailable",
                    "win_loss_summary":        "—",
                },
                "insights": [],
            })

        # 5. Persist — store hash + core snapshot for next comparison
        atlas_table.put_item(Item={
            "user_id":               user_id,
            "altas_prompt_response": ai_response,
            "atlas_payload_hash":    current_hash,
            "atlas_core_snapshot":   current_payload["core"],
            "created_at":            datetime.datetime.now().isoformat(),
        })

        return ai_response