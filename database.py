import boto3
from datetime import datetime


dynamodb = boto3.resource(
    "dynamodb",
    region_name="ap-south-1"
)
table = dynamodb.Table("Users")


def save_user_performance_data(account_login, processed_data):
    try:
        item_to_save = {
            "id": str(account_login),
            "account": processed_data.get("account"),
            "open_positions": processed_data.get("open_positions"),
            "performance_metrics": processed_data.get("performance_metrics"),
            "equity_vs_time": processed_data.get("equity_vs_time"),
            "daily_pnl": processed_data.get("daily_pnl"),
            "recent_trades": processed_data.get("recent_trades"),
            "last_updated": datetime.now().isoformat()
        }

        table.put_item(Item=item_to_save)
        return True
    except Exception as e:
        print(f"DynamoDB Save Error: {str(e)}")
        return False
