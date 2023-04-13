import json
import os
import datetime
import requests
from django.http import JsonResponse
from rest_framework.views import APIView
from utils.auth import CreateToken
from apps.users.models import User


class LoginView(APIView):
    authentication_classes = []

    def post(self, request):
        # 1、接收数据
        data = json.loads(request.body.decode())
        username = data.get('username')
        password = data.get('password')
        # 2、验证数据
        if not all([username, password]):
            return JsonResponse({'code': 400, 'errmsg': '参数不全'})
        try:
            user = User.objects.get(username=username)
        except Exception:
            return JsonResponse({'code': 400, 'errmsg': '用户名或密码错误'})
        if user.password != password:
            return JsonResponse({'code': 400, 'errmsg': '用户名或密码错误'})
        token = CreateToken(payload={'username': username}).create()
        response = JsonResponse({'code': 200, 'errmsg': 'ok', 'token': token})
        return response


class UserinfoView(APIView):
    def get(self, request):
        result = request.user
        username = result.get('username')
        user = User.objects.get(username=username)
        response = JsonResponse({'code': 200, 'errmsg': 'ok', 'username': user.username})
        return response