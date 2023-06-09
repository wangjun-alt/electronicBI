import jwt
from django.conf import settings
from rest_framework.authentication import BaseAuthentication
from rest_framework.exceptions import AuthenticationFailed
from jwt import exceptions
import datetime


class JwtQueryParamsAuthentication(BaseAuthentication):
    # 获取并判断token的合法性
    def authenticate(self, request):
        token = request.headers.get('token')
        salt = settings.SECRET_KEY
        try:
            payload = jwt.decode(token, salt, algorithms=['HS256'])
        except exceptions.ExpiredSignatureError:
            raise AuthenticationFailed({'code': 400, 'error': "token已失效,请重新登录"})
        except jwt.DecodeError:
            raise AuthenticationFailed({'code': 400, 'error': "token认证失败"})
        except jwt.InvalidTokenError:
            raise AuthenticationFailed({'code': 400, 'error': "非法的token"})
        return payload, token


class CreateToken:
    def __init__(self, payload):
        self.payload = payload
    # 获取新的token
    def create(self):
        salt = settings.SECRET_KEY
        # 构造header
        headers = {
            'typ': 'jwt',
            'alg': 'HS256'
        }
        # 构造payload
        self.payload['exp'] = datetime.datetime.utcnow() + datetime.timedelta(minutes=2000)
        token = jwt.encode(payload=self.payload, key=salt, algorithm='HS256', headers=headers)
        return token