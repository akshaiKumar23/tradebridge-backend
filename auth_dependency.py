import os
import logging
import boto3
from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials
from auth import security, cognito_verifier

from botocore.config import Config

logger = logging.getLogger(__name__)

boto_config = Config(
    max_pool_connections=25,
    retries={"max_attempts": 2, "mode": "standard"}
)

cognito_client = boto3.client(
    "cognito-idp",
    region_name=os.getenv("AWS_REGION", "ap-south-1"),
    config=boto_config
)

# Simple in-memory cache to avoid calling Cognito on every request
_email_cache: dict = {}


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> dict:
    token = credentials.credentials
    claims = cognito_verifier.verify_token(token)

    user_id = claims.get('sub')
    email = claims.get('email')

    # Access tokens don't carry email — fetch from Cognito if missing
    if not email:
        if user_id in _email_cache:
            email = _email_cache[user_id]
        else:
            try:
                response = cognito_client.admin_get_user(
                    UserPoolId=os.getenv("COGNITO_USER_POOL_ID"),
                    Username=user_id
                )
                attrs = {a["Name"]: a["Value"]
                         for a in response["UserAttributes"]}
                email = attrs.get("email")
                _email_cache[user_id] = email
                logger.info(f"Fetched email from Cognito for user {user_id}")
            except Exception as e:
                logger.error(
                    f"Failed to fetch email from Cognito for {user_id}: {e}")

    return {
        'user_id': user_id,
        'email': email,
        'username': claims.get('cognito:username'),
        'claims': claims
    }


async def verify_token_only(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> dict:
    token = credentials.credentials
    return cognito_verifier.verify_token(token)
