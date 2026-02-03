import os
from channels.routing import ProtocolTypeRouter, URLRouter
from django.core.asgi import get_asgi_application
import market.WebsocketUtility.routing

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'smart_trading.settings')

application = ProtocolTypeRouter({
    "http": get_asgi_application(),
    "websocket": URLRouter(
        market.WebsocketUtility.routing.websocket_urlpatterns
    ),
})
