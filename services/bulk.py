"""Bulk rewrite helpers for the tables that hold one row per trade.

boto3's batch_writer sends 25 items per request and sends them serially, so an
account with ~20,000 trades meant thousands of sequential HTTPS round trips
inside a single store function -- which is where a large sync appeared to hang.

Two things happen here:

  * chunks are written concurrently rather than one request at a time, and
  * rows the new write is about to overwrite are no longer deleted first.

The second is the structural win. The stores wipe the partition and rewrite it,
but a trade's primary key (user_id + close timestamp) is stable across syncs, so
on a re-sync almost every key in the new set already exists. Deleting only the
keys that are genuinely going away halves the work and leaves the partition in
exactly the same end state as delete-everything-then-write-everything.
"""
import logging
from concurrent.futures import ThreadPoolExecutor
from boto3.dynamodb.conditions import Key

logger = logging.getLogger(__name__)

# Items per batch_writer context. Each context flushes 25 at a time, so this is
# how much serial work one thread takes on.
CHUNK_SIZE = 500

# Kept deliberately modest: the sync already runs up to 12 store functions in
# parallel, and every one of these threads borrows a boto3 pool connection.
MAX_WORKERS = 4


def _chunk(seq, size):
    return [seq[i:i + size] for i in range(0, len(seq), size)]


def _run_parallel(fn, parts):
    if not parts:
        return
    if len(parts) == 1:
        fn(parts[0])
        return
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(parts))) as pool:
        # list() so any exception inside a worker propagates to the caller.
        list(pool.map(fn, parts))


def collect_existing_keys(table, user_id, sort_key, extra_fields=None):
    """Page the whole partition, returning (keys, items).

    keys is ready to hand to delete_keys(); items carries any extra projected
    attributes the caller asked for (trades needs the user's tags).
    """
    names = {"#sk": sort_key}
    projection = ["user_id", "#sk"]

    for i, field in enumerate(extra_fields or []):
        alias = f"#x{i}"
        names[alias] = field
        projection.append(alias)

    keys, items = [], []
    last_key = None

    while True:
        kwargs = {
            "KeyConditionExpression": Key("user_id").eq(user_id),
            "ProjectionExpression": ", ".join(projection),
            "ExpressionAttributeNames": names,
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key

        response = table.query(**kwargs)

        for item in response.get("Items", []):
            keys.append({"user_id": item["user_id"], sort_key: item[sort_key]})
            items.append(item)

        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            return keys, items


def delete_keys(table, keys):
    def _delete(part):
        with table.batch_writer() as batch:
            for key in part:
                batch.delete_item(Key=key)

    _run_parallel(_delete, _chunk(keys, CHUNK_SIZE))


def dedupe_by_key(items, sort_key, label=""):
    """Keep one item per primary key, the last one wins.

    DynamoDB rejects an entire BatchWriteItem request whose 25 items name the
    same key twice, so a single duplicate anywhere fails the whole sync. Callers
    are expected to produce unique keys; this is the net that stops a bug in one
    store from taking the run down with it.
    """
    unique = {}
    for item in items:
        unique[(item["user_id"], item[sort_key])] = item

    dropped = len(items) - len(unique)
    if dropped:
        logger.warning(
            f"{label}: dropped {dropped} items sharing a primary key before "
            f"writing -- the caller should be producing unique {sort_key} values"
        )

    return list(unique.values())


def put_items(table, items, sort_key=None, label=""):
    if sort_key is not None:
        items = dedupe_by_key(items, sort_key, label)

    def _put(part):
        with table.batch_writer() as batch:
            for item in part:
                batch.put_item(Item=item)

    _run_parallel(_put, _chunk(items, CHUNK_SIZE))
    return items


def replace_partition(table, sort_key, existing_keys, items, label=""):
    """Leave the partition holding exactly `items`, doing the least work.

    Numbers come back from DynamoDB as Decimal while freshly built items hold
    ints; Decimal(5) == 5 and hashes alike, so set membership works across both.
    """
    items = dedupe_by_key(items, sort_key, label)

    surviving = {item[sort_key] for item in items}
    stale = [k for k in existing_keys if k[sort_key] not in surviving]

    delete_keys(table, stale)
    put_items(table, items)

    logger.info(
        f"{label}: {len(existing_keys)} existing, {len(stale)} deleted, "
        f"{len(items)} written"
    )
    return len(stale)
