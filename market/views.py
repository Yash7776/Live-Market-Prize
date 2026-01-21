from django.shortcuts import render

def market_view(request):
    return render(request, "market.html")