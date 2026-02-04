import jwt
import requests
from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
import os

class CognitoAuthMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)
        self.region = os.getenv("AWS_REGION", "ap-south-1")
        self.user_pool_id = os.getenv("COGNITO_USER_POOL_ID")
        self.app_client_id = os.getenv("COGNITO_APP_CLIENT_ID")
        # Cognito JWKS URL
        self.jwks_url = f"https://cognito-idp.{self.region}.amazonaws.com/{self.user_pool_id}/.well-known/jwks.json"
        self.jwks = requests.get(self.jwks_url).json()

    async def dispatch(self, request: Request, call_next):
        # Allow documentation routes without auth
        if request.url.path in ["/docs", "/openapi.json", "/redoc"]:
            return await call_next(request)

        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return JSONResponse(status_code=401, content={"detail": "Missing or invalid token"})

        token = auth_header.split(" ")[1]

        try:
            # 1. Get the kid from the header to find the right public key
            header = jwt.get_unverified_header(token)
            kid = header["kid"]
            key = next(k for k in self.jwks["keys"] if k["kid"] == kid)

            # 2. Construct public key and verify
            # Note: For production, use a library like 'python-jose' for robust RSA verification
            # This logic assumes the token is verified against the Cognito Issuer
            public_key = jwt.algorithms.RSAAlgorithm.from_jwk(key)
            
            decoded_token = jwt.decode(
                token,
                public_key,
                algorithms=["RS256"],
                audience=self.app_client_id,
                issuer=f"https://cognito-idp.{self.region}.amazonaws.com/{self.user_pool_id}"
            )

            # Attach user info to request state for use in routes
            request.state.user = decoded_token
            
        except Exception as e:
            return JSONResponse(status_code=401, content={"detail": f"Token validation failed: {str(e)}"})

        return await call_next(request)