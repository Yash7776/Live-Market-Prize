from django.contrib import admin
from django.urls import path
from market.views import market_view

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', market_view),
]
