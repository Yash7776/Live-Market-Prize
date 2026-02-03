from django.urls import path
from .consumers import MarketConsumer

websocket_urlpatterns = [
    path("ws/market/", MarketConsumer.as_asgi()),
]
