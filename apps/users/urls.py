from django.urls import path
from apps.users.views import LoginView, UserinfoView


urlpatterns = [
    # 判断用户名是否重复
    path('login/', LoginView.as_view()),
    path('user/', UserinfoView.as_view())
]