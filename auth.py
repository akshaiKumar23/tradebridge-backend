from fastapi import HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwk, jwt
from jose.utils import base64url_decode
import requests
import time
import os
from dotenv import load_dotenv
security = HTTPBearer()

load_dotenv()


class CognitoTokenVerifier:
    def __init__(self, region: str, user_pool_id: str, app_client_id: str):
        self.region = region
        self.user_pool_id = user_pool_id
        self.app_client_id = app_client_id
        self.jwks_url = f'https://cognito-idp.{region}.amazonaws.com/{user_pool_id}/.well-known/jwks.json'
        self._jwks = None
        self._jwks_last_fetched = 0
    
    @property
    def jwks(self):
        if not self._jwks or (time.time() - self._jwks_last_fetched) > 3600:
            print("🔐 Fetching JWKS from:", self.jwks_url)

            response = requests.get(self.jwks_url, timeout=5)
            print("🔐 JWKS status code:", response.status_code)
            print("🔐 JWKS raw response:", response.text)

            data = response.json()

            if "keys" not in data:
                raise RuntimeError(
                    f"Invalid JWKS response (no keys): {data}"
                )

            self._jwks = data
            self._jwks_last_fetched = time.time()

        return self._jwks

    
    def verify_token(self, token: str) -> dict:
        try:
           
            headers = jwt.get_unverified_headers(token)
            kid = headers['kid']
            
        
            key = None
            for jwk_key in self.jwks['keys']:
                if jwk_key['kid'] == kid:
                    key = jwk_key
                    break
            
            if not key:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail='Public key not found in JWKS'
                )
           
            public_key = jwk.construct(key)
            

            message, encoded_signature = token.rsplit('.', 1)
         
            decoded_signature = base64url_decode(encoded_signature.encode())
          
            if not public_key.verify(message.encode(), decoded_signature):
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail='Signature verification failed'
                )
            
         
            claims = jwt.get_unverified_claims(token)
            
        
            if time.time() > claims['exp']:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail='Token is expired'
                )
            
          
            if claims.get('aud') != self.app_client_id and claims.get('client_id') != self.app_client_id:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail='Token was not issued for this audience'
                )
            
       
            expected_issuer = f'https://cognito-idp.{self.region}.amazonaws.com/{self.user_pool_id}'
            if claims['iss'] != expected_issuer:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail='Token issuer is invalid'
                )
            
   
            if claims.get('token_use') not in ['id', 'access']:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail='Invalid token type'
                )
            
            return claims
            
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f'Unable to verify token: {str(e)}'
            )

cognito_verifier = CognitoTokenVerifier(
    region=os.getenv('AWS_REGION', 'us-east-1'),
    user_pool_id=os.getenv('COGNITO_USER_POOL_ID'),
    app_client_id=os.getenv('COGNITO_APP_CLIENT_ID')
)