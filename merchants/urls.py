# merchants/urls.py

from django.urls import path
from . import views

urlpatterns = [
    # Merchant balance + ledger
    path('merchants/<uuid:merchant_id>/balance/', views.MerchantBalanceView.as_view()),
    path('merchants/<uuid:merchant_id>/ledger/', views.MerchantLedgerView.as_view()),

    # Payouts
    path('payouts/', views.PayoutCreateView.as_view()),
    path('payouts/<uuid:payout_id>/', views.PayoutDetailView.as_view()),
    path('merchants/<uuid:merchant_id>/payouts/', views.MerchantPayoutHistoryView.as_view()),

    # Merchants list (for dashboard dropdown)
    path('merchants/', views.MerchantListView.as_view()),
]