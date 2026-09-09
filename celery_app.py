import os
from celery import Celery
from dotenv import load_dotenv

load_dotenv()
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

celery_app = Celery("mt5_tasks", broker=REDIS_URL, backend=REDIS_URL)

celery_app.conf.update(
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    timezone='UTC',
    include=['tasks'],

    # A sync wedged on a blocking MT5 terminal call would otherwise hold its
    # worker slot forever. The soft limit raises SoftTimeLimitExceeded inside the
    # task so it can release the MT5 lock and shut the terminal session down; the
    # hard limit kills the process if that cleanup itself hangs.
    task_soft_time_limit=25 * 60,
    task_time_limit=30 * 60,

    # Redis has no server-side ack. It re-queues any task still running after the
    # visibility timeout, so with the 1 hour default a long sync could be handed
    # to a second worker while the first was still going -- two MT5 logins then
    # fight over one terminal. Must stay comfortably above task_time_limit.
    broker_transport_options={"visibility_timeout": 2 * 60 * 60},
    result_expires=6 * 60 * 60,

    # Without this a worker greedily reserves syncs it cannot start for many
    # minutes, so they look queued forever while another worker sits idle.
    worker_prefetch_multiplier=1,

    # Ack only on completion, so a killed worker's sync is re-run rather than
    # lost. Safe here because a sync wipes and rewrites -- it is idempotent.
    task_acks_late=True,
    task_reject_on_worker_lost=True,
)