from django.urls import path
from apps.contracts.views import SetContractDataView, GetContractNumView, SearchContractView, GetTableDataView,UploadContractView



urlpatterns = [
    # 判断用户名是否重复
    path('form-upload/', SetContractDataView.as_view()),
    path('nums/', GetContractNumView.as_view()),
    path('search/', SearchContractView.as_view()),
    path('tabledata/', GetTableDataView.as_view()),
    path('upload/', UploadContractView.as_view())
]