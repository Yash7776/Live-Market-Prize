from django.db import models

# Create your models here.
class Position(models.Model):
    symbol = models.CharField(max_length=50)
    exchange = models.CharField(max_length=10)
    token = models.CharField(max_length=20)
    entry_price = models.FloatField(null=True, blank=True)
    entry_datetime = models.DateTimeField(auto_now_add=True)  # auto set on create
    exit_price = models.FloatField(null=True, blank=True)
    exit_datetime = models.DateTimeField(null=True, blank=True)
    target = models.FloatField(null=True, blank=True)
    stoploss = models.FloatField(null=True, blank=True)
    mtm = models.FloatField(default=0.0)
    status = models.CharField(max_length=20, default="OPEN")  # OPEN, CLOSED, SQUARED_OFF

    def __str__(self):
        return f"{self.symbol} {self.status}"