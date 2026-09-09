import MetaTrader5 as mt5
from datetime import datetime, timedelta
from collections import defaultdict
import time

# Risk assumed for a trade that carried no stop loss, as a fraction of the
# account's realised balance. Any dataset-derived fallback (e.g. "the average
# loss so far") cannot be window-independent -- the same trade would score
# differently at 30 days than at 90 -- so the fallback is anchored to the
# account instead.
DEFAULT_RISK_PCT = 0.01

# How far before the reporting window to reach when pulling deals, so that a
# position opened earlier still arrives with its opening deal attached. Trades
# are cut back to the requested window by *close* time afterwards.
POSITION_LOOKBACK_DAYS = 365

# MT5 pulls an account's deal history from the broker asynchronously after a
# login, and history_deals_get() does not wait for it. Query straight after
# connecting and the terminal answers 0 -- truthfully, because its local cache
# really is empty at that instant. That is why a first sync recorded an empty
# account and a second one, run against a cache the terminal had since filled,
# returned everything.
HISTORY_READY_TIMEOUT = 120      # longest to wait for the download
HISTORY_EMPTY_GRACE = 15         # give up sooner when the account looks new
HISTORY_POLL_INTERVAL = 0.5
HISTORY_STABLE_POLLS = 2         # count must repeat this often to count as done

# Deposits are themselves deals, so any funded account has history somewhere in
# all time even when the reporting window is empty. Readiness is judged against
# this range, never the reporting window.
ALL_TIME_START = datetime(2000, 1, 1)


def _deal_count(from_ts, to_ts):
    """Cheap count of deals in a range, without materialising them."""
    if hasattr(mt5, "history_deals_total"):
        try:
            total = mt5.history_deals_total(from_ts, to_ts)
            if total is not None:
                return int(total)
        except Exception:
            pass
    try:
        return len(mt5.history_deals_get(from_ts, to_ts) or [])
    except Exception:
        return 0


def _wait_for_history(from_ts, to_ts, balance, reason):
    """Block until the terminal has finished downloading the account history.

    A download in progress reports a count that keeps climbing, so wait for the
    number to repeat rather than merely be non-zero. An account with no deals
    AND no balance is plausibly just new, so that case gives up early instead of
    making every empty sync sit out the full timeout.
    """
    started = time.time()
    deadline = started + HISTORY_READY_TIMEOUT
    last_total = -1
    stable = 0

    while time.time() < deadline:
        total = _deal_count(from_ts, to_ts)
        waited = time.time() - started

        if total > 0 and total == last_total:
            stable += 1
            if stable >= HISTORY_STABLE_POLLS:
                print(f"History ready ({reason}): {total} deals after {waited:.1f}s")
                return total
        else:
            stable = 0

        if total == 0 and not balance and waited >= HISTORY_EMPTY_GRACE:
            print(
                f"No history and no balance after {waited:.1f}s ({reason}) - "
                f"treating this as a genuinely new account"
            )
            return 0

        last_total = total
        time.sleep(HISTORY_POLL_INTERVAL)

    print(
        f"History still settling after {HISTORY_READY_TIMEOUT}s ({reason}) - "
        f"proceeding with {max(last_total, 0)} deals"
    )
    return max(last_total, 0)



def fetch_mt5_analytics(server, login, password, days=None, progress_cb=None):
    """Pull closed-trade analytics from the MT5 terminal.

    progress_cb, when given, is called as progress_cb(done, total) while
    positions are processed so a long sync can report progress instead of
    looking wedged.
    """

    try:

        # ---------------- INITIALIZE ----------------

        if not mt5.initialize():
            mt5.shutdown()
            time.sleep(2)
            if not mt5.initialize():
                return {
                    "status": "error",
                    "message": f"MT5 init failed: {mt5.last_error()}"
                }

        # Only login if not already on the right account
        did_login = False
        account_info = mt5.account_info()
        if account_info is None or account_info.login != login:
            if not mt5.login(login=login, password=password, server=server):
                mt5.shutdown()
                return {
                    "status": "error",
                    "message": f"MT5 login failed: {mt5.last_error()}"
                }
            did_login = True

        account = mt5.account_info()

        if account is None:
            mt5.shutdown()
            return {
                "status": "error",
                "message": "Account info failed"
            }

        # ---------------- ACCOUNT ----------------

        account_data = {
            "login": account.login,
            "balance": str(account.balance),
            "equity": str(account.equity),
            "profit": str(account.profit),
            "margin_level":
                str((account.equity / account.margin) * 100)
                if account.margin > 0 else "0",
            "company":
                getattr(account, "company", "N/A")
        }

        # ---------------- OPEN POSITIONS ----------------

        positions_data = []
        positions = mt5.positions_get()

        if positions:
            for pos in positions:
                positions_data.append({
                    "ticket": int(pos.ticket),
                    "symbol": pos.symbol,
                    "profit": str(pos.profit),
                    "volume": float(pos.volume),
                    "opened_at":
                        datetime.fromtimestamp(pos.time).isoformat()
                })

        # ---------------- FETCH ALL DEALS ----------------

        # MT5 uses Eastern Summer Time (UTC-4), so we add a 6-hour buffer
        # to datetime.now() to ensure all recent trades are captured
        # regardless of the timezone difference between the server and MT5
        now = datetime.now()
        end_time = now + timedelta(hours=6)
        start_time = (
            now - timedelta(days=days)
            if days is not None
            else datetime(2000, 1, 1)
        )

        # Asking the broker only for the reporting window is what made the same
        # trade come back with a different lot size and direction at 30 days than
        # at 90: a position opened before the window arrived holding just its
        # closing deal. Reach further back for the deals, then filter the trades
        # by close time below.
        window_start_ts = int(start_time.timestamp())

        fetch_start = (
            start_time - timedelta(days=POSITION_LOOKBACK_DAYS)
            if days is not None
            else start_time
        )

        fetch_from_ts = int(fetch_start.timestamp())
        fetch_to_ts = int(end_time.timestamp())
        all_time_from_ts = int(ALL_TIME_START.timestamp())

        # A fresh login means the terminal is still pulling this account's
        # history in the background. Without this wait the very next call reads
        # an empty cache and the sync stores a zero-trade account.
        if did_login:
            _wait_for_history(
                all_time_from_ts, fetch_to_ts, account.balance, "after login"
            )

        deals = mt5.history_deals_get(fetch_from_ts, fetch_to_ts) or []
        deals = sorted(deals, key=lambda d: d.time)

        print(f"\n=== FETCHING {'ALL TIME' if days is None else f'LAST {days} DAYS'} ===")
        print(f"Reporting window: {start_time.date()}  To: {end_time.date()}")
        print(f"Deals pulled from: {fetch_start.date()} (lookback for opening deals)")
        print(f"Total deals fetched: {len(deals)}")

        # Safety net for the case this call did not log in -- the terminal was
        # already on the account but had not finished syncing, e.g. it had just
        # been restarted. A funded account with no deals anywhere in all time can
        # only mean the history has not arrived: a deposit is itself a deal.
        if (not deals and account.balance
                and _deal_count(all_time_from_ts, fetch_to_ts) == 0):
            print(
                "0 deals but the account holds a balance - history has not "
                "arrived yet, waiting for it"
            )
            _wait_for_history(
                all_time_from_ts, fetch_to_ts, account.balance, "late arrival"
            )
            deals = sorted(
                mt5.history_deals_get(fetch_from_ts, fetch_to_ts) or [],
                key=lambda d: d.time,
            )
            print(f"Total deals fetched after waiting: {len(deals)}")

        # Refuse to report an empty account we do not believe in.
        #
        # Deposits are deals, so an account holding money must have history. If
        # the terminal still shows none after waiting for the download, the
        # history is missing, not absent. Reporting success here would hand the
        # stores an empty result and they would wipe and rewrite the user's real
        # trades -- along with their tags, which cannot be re-fetched from MT5.
        # Failing instead leaves every stored table untouched.
        #
        # A genuinely new account has no balance and no deals, and still returns
        # success below, so first-time users are unaffected.
        if not deals and account.balance:
            message = (
                f"Account {login} reports a balance of {account.balance} but the "
                f"terminal returned no deal history. Refusing to overwrite stored "
                f"trades with an empty result."
            )
            print(f"REFUSING TO SYNC: {message}")
            return {
                "status": "error",
                "code": "history_unavailable",
                "message": message,
            }

        # ---------------- GROUP BY POSITION ----------------

        positions_map = defaultdict(list)

        for deal in deals:
            # Skip balance operations
            if deal.type == mt5.DEAL_TYPE_BALANCE:
                continue
            positions_map[deal.position_id].append(deal)

        print(f"Unique positions: {len(positions_map)}")

        # ------------- BACK-FILL POSITIONS THAT STRADDLE start_time -----------
        # The window above filters *deals*, not positions. A position opened
        # before start_time but closed inside the window comes back holding only
        # its exit deal, which silently corrupts volume, direction, entry_price,
        # commission and hold time -- and makes the very same trade look
        # different at 30 days than at 90 days. Re-fetch the complete deal set
        # for those (history_deals_get(position=...) ignores the time window).

        back_filled = set()

        for position_id, position_deals in list(positions_map.items()):

            if any(d.entry == mt5.DEAL_ENTRY_IN for d in position_deals):
                continue

            full_history = mt5.history_deals_get(position=position_id) or []
            full_deals = [
                d for d in full_history if d.type != mt5.DEAL_TYPE_BALANCE
            ]

            if full_deals:
                positions_map[position_id] = sorted(
                    full_deals, key=lambda d: d.time
                )
                back_filled.add(position_id)

        if back_filled:
            print(f"Back-filled {len(back_filled)} positions opened before the window")

        # ---------------- ORDERS, FETCHED ONCE ----------------
        # Stop losses used to be looked up with one history_orders_get() call per
        # position. Every one of those is a blocking IPC round trip to the MT5
        # terminal, so an account with thousands of trades spent most of the sync
        # waiting on the terminal. Pull the whole order history in a single call
        # and index it instead.

        orders = mt5.history_orders_get(fetch_from_ts, fetch_to_ts) or []

        orders_by_position = defaultdict(list)
        for order in orders:
            orders_by_position[order.position_id].append(order)

        for pid in orders_by_position:
            orders_by_position[pid].sort(
                key=lambda o: getattr(o, "time_setup", 0)
            )

        print(f"Orders fetched: {len(orders)} in 1 call "
              f"({len(orders_by_position)} positions indexed)")

        # Positions recovered by the back-fill opened before the order window, so
        # their orders are not in the bulk pull. This stays a per-position call,
        # but only for that handful.
        for position_id in back_filled:
            if position_id in orders_by_position:
                continue
            recovered = mt5.history_orders_get(position=position_id) or []
            if recovered:
                orders_by_position[position_id] = sorted(
                    recovered, key=lambda o: getattr(o, "time_setup", 0)
                )

        # ---------------- ANALYTICS ----------------

        trades_list = []
        daily_pnl_map = defaultdict(float)
        wins = []
        losses = []
        net_pnl = 0.0

        # ---------------- PROCESS EACH POSITION ----------------

        total_positions = len(positions_map)
        processed = 0

        for position_id, position_deals in positions_map.items():

            processed += 1
            if progress_cb and processed % 500 == 0:
                progress_cb(processed, total_positions)

            entry_deal = None
            exit_deal = None

            total_profit = 0.0
            total_swap = 0.0
            total_commission = 0.0
            entry_volume = 0.0

            symbol = None
            open_time = None
            close_time = None

            # Aggregate all deals for this position
            for deal in sorted(position_deals, key=lambda d: d.time):
                symbol = deal.symbol

                if deal.entry == mt5.DEAL_ENTRY_IN:
                    if entry_deal is None:
                        entry_deal = deal
                        open_time = deal.time

                    # Position size is the volume that was *opened*. Summing
                    # every deal counted the matching exit as well, and reported
                    # a 0.2 lot trade as 0.4.
                    entry_volume += deal.volume

                if deal.entry in (mt5.DEAL_ENTRY_OUT, mt5.DEAL_ENTRY_INOUT):
                    exit_deal = deal
                    close_time = deal.time

                total_profit += deal.profit or 0.0
                total_swap += deal.swap or 0.0
                total_commission += deal.commission or 0.0

            # Skip if position not closed
            if close_time is None:
                continue

            # Cut back to the requested window. The deal fetch reached further
            # back only to pick up opening deals, so a position that also closed
            # before the window must not be reported.
            if close_time < window_start_ts:
                continue

            # No IN deal even after the back-fill (broker history gap, or a
            # netting reversal booked as INOUT) -- fall back to the closed
            # volume rather than reporting 0.
            if entry_volume <= 0:
                entry_volume = sum(
                    d.volume for d in position_deals
                    if d.entry in (mt5.DEAL_ENTRY_OUT, mt5.DEAL_ENTRY_INOUT)
                )

            # Calculate net P&L for entire position
            trade_net = total_profit + total_swap + total_commission
            net_pnl += trade_net

            # Classify win/loss
            if trade_net > 0:
                wins.append(trade_net)
            elif trade_net < 0:
                losses.append(abs(trade_net))

            # Calculate hold time
            hold_time_minutes = 0
            if open_time:
                hold_time_minutes = (close_time - open_time) / 60

            # Daily P&L
            date_str = datetime.fromtimestamp(close_time).strftime('%Y-%m-%d')
            daily_pnl_map[date_str] += trade_net

            # Risk from the stop loss, when the order carried one. Sorted so a
            # position with several orders always resolves to the same one.
            risk_amount = None

            for order in orders_by_position.get(position_id, ()):
                if getattr(order, "sl", 0) > 0 and entry_deal:
                    risk_amount = abs(
                        order.price_open - order.sl
                    ) * order.volume_initial
                    break

            # Direction comes from the opening deal; if only the closing deal
            # survives, invert it -- a long is closed by a SELL and vice versa.
            if entry_deal is not None:
                direction = (
                    "LONG" if entry_deal.type == mt5.DEAL_TYPE_BUY else
                    "SHORT" if entry_deal.type == mt5.DEAL_TYPE_SELL else
                    "UNKNOWN"
                )
            elif exit_deal is not None:
                direction = (
                    "LONG" if exit_deal.type == mt5.DEAL_TYPE_SELL else
                    "SHORT" if exit_deal.type == mt5.DEAL_TYPE_BUY else
                    "UNKNOWN"
                )
            else:
                direction = "UNKNOWN"

            # Save complete trade (risk/r_multiple filled in by the second pass)
            trades_list.append({
                "ticket": int(exit_deal.ticket) if exit_deal else 0,
                "position_id": int(position_id),
                "symbol": symbol,
                "pnl": float(round(trade_net, 2)),
                "open_time": open_time,
                "close_time": close_time,
                "hold_time_minutes": round(hold_time_minutes, 2),
                "volume": round(entry_volume, 2),
                "r_multiple": 0.0,
                "risk_amount": risk_amount,
                "timestamp": close_time,
                "trade_id": int(position_id),
                "entry_price": float(entry_deal.price) if entry_deal else 0,
                "exit_price": float(exit_deal.price) if exit_deal else 0,
                "direction": direction,
            })

        trades_list.sort(key=lambda t: (t["close_time"], t["position_id"]))

        # ---------------- SECOND PASS: RISK / R-MULTIPLE ----------------
        # The old fallback averaged the losses seen *so far* in iteration order,
        # so a trade's r_multiple depended on how many losers happened to precede
        # it inside the window -- and collapsed to a flat 1.0 for every trade
        # whenever the window held no losses at all.

        fallback_risk = abs(account.balance) * DEFAULT_RISK_PCT

        for trade in trades_list:
            risk_amount = trade["risk_amount"]

            if not risk_amount:
                risk_amount = fallback_risk or abs(trade["pnl"]) or 1.0

            trade["risk_amount"] = round(risk_amount, 2)
            trade["r_multiple"] = round(trade["pnl"] / risk_amount, 2)

        # ---------------- EQUITY CURVE ----------------
        # The curve has to *end* at the account's realised balance. Seeding it
        # with that balance instead left every point net_pnl too high -- by a
        # different amount for each window length.

        running_equity = account.balance - net_pnl
        equity_curve = []

        for trade in trades_list:
            running_equity += trade["pnl"]
            equity_curve.append({
                "timestamp": trade["close_time"],
                "equity": round(running_equity, 2)
            })

        # ---------------- METRICS -------------------

        total_trades = len(trades_list)
        avg_win = sum(wins) / len(wins) if wins else 0
        avg_loss = sum(losses) / len(losses) if losses else 0
        expectancy = net_pnl / total_trades if total_trades else 0
        profit_factor = sum(wins) / sum(losses) if losses else 0

        metrics = {
            "net_pnl_30d": str(round(net_pnl, 2)),
            "total_trades": total_trades,
            "wins": len(wins),
            "losses": len(losses),
            "win_percentage":
                round((len(wins) / total_trades) * 100, 2)
                if total_trades else 0,
            "profit_factor": round(profit_factor, 2),
            "expectancy": round(expectancy, 2),
            "avg_win": round(avg_win, 2),
            "avg_loss": round(avg_loss, 2)
        }

        print("\n=== FINAL SUMMARY ===")
        print(f"Closed positions: {len(trades_list)}")
        print(f"Wins: {len(wins)}, Losses: {len(losses)}")
        print(f"Equity points: {len(equity_curve)}")

        if trades_list:
            print(f"\n=== SAMPLE TRADES ===")
            for i, t in enumerate(trades_list[:3]):
                dt = datetime.fromtimestamp(t['close_time']).strftime('%Y-%m-%d %H:%M')
                print(f"{i+1}. {t['symbol']} PnL={t['pnl']} @ {dt}")

        # ---------------- RETURN ----------------

        return {
            "status": "success",
            "data": {
                "account": account_data,
                "open_positions": positions_data,
                "performance_metrics": metrics,
                "equity_vs_time": equity_curve,
                "daily_pnl": [
                    {"date": d, "pnl": str(round(p, 2))}
                    for d, p in sorted(daily_pnl_map.items())
                ],
                "trades": trades_list
            }
        }

    finally:
        mt5.shutdown()
