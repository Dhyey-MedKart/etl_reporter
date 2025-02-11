from django.contrib import admin
from django.urls import path
from .views import test,report1,report2,report3,report4,report5
from django.views.decorators.cache import cache_page
app_name = 'leads'

urlpatterns = [
    path('', test, name='test'),
    path('report1/', cache_page(60*60)(report1), name='report1'),
    path('report2/', cache_page(60*60)(report2), name='report2'),
    path('report3/', cache_page(60*60)(report3), name='report3'),
    path('report4/', cache_page(60*60)(report4), name='report4'),
    path('report5/', cache_page(60*60)(report5), name='report5'),
]