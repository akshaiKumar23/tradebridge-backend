import boto3
import os

def get_dynamodb():
    return boto3.resource(
        "dynamodb",
        region_name=os.getenv("AWS_REGION", "ap-south-1"),
    )

def get_strategies_table():
    dynamodb = get_dynamodb()
    return dynamodb.Table("UserStrategies")


def get_journals_table():
    dynamodb = boto3.resource("dynamodb")
    return dynamodb.Table("DailyJournals")
