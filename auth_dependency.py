from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials
from auth import security, cognito_verifier


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> dict:
    token = credentials.credentials
    claims = cognito_verifier.verify_token(token)

    return {
        'user_id': claims.get('sub'),
        'email': claims.get('email'),
        'username': claims.get('cognito:username'),
        'claims': claims
    }


async def verify_token_only(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> dict:
    token = credentials.credentials
    return cognito_verifier.verify_token(token)
