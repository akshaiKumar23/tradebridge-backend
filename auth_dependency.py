import os
import logging
import boto3
from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials
from auth import security, cognito_verifier

logger = logging.getLogger(__name__)

cognito_client = boto3.client(
    "cognito-idp", region_name=os.getenv("AWS_REGION"))

# Simple in-memory cache to avoid calling Cognito on every request
_email_cache: dict = {}


async def get_current_user(
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
