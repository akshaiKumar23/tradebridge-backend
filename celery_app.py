# import os
# from celery import Celery
# from dotenv import load_dotenv

# load_dotenv()
# REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# celery_app = Celery("mt5_tasks", broker=REDIS_URL, backend=REDIS_URL)

# celery_app.conf.update(
#     task_serializer='json',
#     result_serializer='json',
#     accept_content=['json'],
#     timezone='UTC',
#     include=['tasks'] 
# )


# @celery.task(name="get_account_summary", bind=True) # Adding bind=True can help debugging
# def get_account_summary(self, user_id, server, login, password, from_date, to_date):
#     from mt5_logic import sync_mt5_trades_to_db
#     try:
#         return sync_mt5_trades_to_db(user_id, server, login, password, from_date, to_date)
#     except Exception as exc:
#         logger.error(f"Task failed for user {user_id}: {exc}")
#         raise self.retry(exc=exc, countdown=5)




import os
from celery import Celery # This is the Class
from dotenv import load_dotenv

load_dotenv()
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# You named your instance 'celery_app' here
celery_app = Celery("mt5_tasks", broker=REDIS_URL, backend=REDIS_URL)

celery_app.conf.update(
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    timezone='UTC',
    include=['tasks'] 
)

# FIX: Change '@celery.task' to '@celery_app.task'
@celery_app.task(name="get_account_summary", bind=True) 
def get_account_summary(self, user_id, server, login, password, from_date, to_date):
    from mt5_logic import sync_mt5_trades_to_db
    return sync_mt5_trades_to_db(user_id, server, login, password, from_date, to_date)