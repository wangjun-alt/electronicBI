from django.urls import path
from apps.contracts.views import  SetContractDataView



urlpatterns = [
    # 判断用户名是否重复
    path('form-upload/', SetContractDataView.as_view()),
]