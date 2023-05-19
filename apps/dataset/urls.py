from django.urls import path
from apps.dataset.views import DataSetsView, TableInfoView, TableDataView, TableBIView, DatasetDelView,GetColumnsView, \
    GetHeaderView, SetBarDataView,SetPieDataView, GetDatasetNum


urlpatterns = [
    path('info/', DataSetsView.as_view()),
    path('table/info/', TableInfoView.as_view()),
    path('table/data/', TableDataView.as_view()),
    path('table/bi/', TableBIView.as_view()),
    path('delete/', DatasetDelView.as_view()),
    path('columns/', GetColumnsView.as_view()),
    path('headers/', GetHeaderView.as_view()),
    path('bar/', SetBarDataView.as_view()),
    path('pie/', SetPieDataView.as_view()),
    path('getnum/', GetDatasetNum.as_view())
]