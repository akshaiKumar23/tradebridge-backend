import boto3
from datetime import datetime
from uuid import uuid4
from boto3.dynamodb.conditions import Key

# Initialize DynamoDB Resource
dynamodb = boto3.resource("dynamodb", region_name="ap-south-1")
notes_table = dynamodb.Table("UserNotes")

def create_journal_entry(user_id: str, journal_data: dict):
    """
    Saves a new journal entry to the UserNotes table.
    user_id acts as the Partition Key.
    """
    try:
        note_id = str(uuid4()) # Generate a unique identifier for the note
        item = {
            "id": str(user_id),             # Partition Key: Link to user from Users table
            "note_id": note_id,              # Sort Key: Unique ID for this specific note
            "title": journal_data.get("title"),
            "content": journal_data.get("content"),
            "session_type": journal_data.get("session_type"), # e.g., NY, London
            "trading_date": journal_data.get("trading_date"),
            "created_at": datetime.utcnow().isoformat()
        }
        
        notes_table.put_item(Item=item)
        return {"status": "success", "note_id": note_id}
    except Exception as e:
        return {"status": "error", "message": str(e)}

def get_user_journals(user_id: str):
    """
    Queries all journal entries for a specific user using their ID.
    """
    try:
        # Efficient lookup using the Partition Key
        response = notes_table.query(
            KeyConditionExpression=Key('id').eq(str(user_id))
        )
        return {"status": "success", "data": response.get('Items', [])}
    except Exception as e:
        return {"status": "error", "message": str(e)}