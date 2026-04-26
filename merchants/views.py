# merchants/views.py

import logging
import uuid

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from .models import (
    Merchant,
    Payout,
    InsufficientFundsError,
)
from .services import (
    create_payout,
    get_merchant_balance,
    get_merchant_ledger,
    get_payout_history,
)
from .tasks import process_payout

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────
# MERCHANT VIEWS
# ─────────────────────────────────────────

class MerchantListView(APIView):
    """
    GET /api/v1/merchants/
    Returns all merchants.
    Used by dashboard dropdown to select merchant.
    """
    def get(self, request):
        merchants = Merchant.objects.all().order_by('created_at')
        data = [
            {
                'id': str(m.id),
                'name': m.name,
                'email': m.email,
                'created_at': m.created_at.isoformat(),
            }
            for m in merchants
        ]
        return Response(data)


class MerchantBalanceView(APIView):
    """
    GET /api/v1/merchants/<merchant_id>/balance/
    Returns total, held, and available balance.
    This is the main number on the dashboard.
    """
    def get(self, request, merchant_id):
        try:
            data = get_merchant_balance(merchant_id)
            return Response(data)
        except ValueError as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_404_NOT_FOUND
            )


class MerchantLedgerView(APIView):
    """
    GET /api/v1/merchants/<merchant_id>/ledger/
    Returns recent credits and debits.
    Shown as transaction history on dashboard.
    """
    def get(self, request, merchant_id):
        try:
            limit = int(request.query_params.get('limit', 20))
            data = get_merchant_ledger(merchant_id, limit=limit)
            return Response(data)
        except ValueError as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_404_NOT_FOUND
            )


# ─────────────────────────────────────────
# PAYOUT VIEWS
# ─────────────────────────────────────────

class PayoutCreateView(APIView):
    """
    POST /api/v1/payouts/
    Header: Idempotency-Key: <uuid>
    Body: { merchant_id, amount_paise, bank_account_id }

    Creates a payout in pending state.
    Queues Celery task for background processing.
    Returns same response if called twice with same key.
    """
    def post(self, request):
        # ── 1. Validate idempotency key
        idempotency_key = request.headers.get('Idempotency-Key')
        if not idempotency_key:
            return Response(
                {'error': 'Idempotency-Key header is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Validate it's a valid UUID
        try:
            uuid.UUID(idempotency_key)
        except ValueError:
            return Response(
                {'error': 'Idempotency-Key must be a valid UUID'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # ── 2. Validate request body
        merchant_id = request.data.get('merchant_id')
        amount_paise = request.data.get('amount_paise')
        bank_account_id = request.data.get('bank_account_id')

        if not all([merchant_id, amount_paise, bank_account_id]):
            return Response(
                {'error': 'merchant_id, amount_paise, bank_account_id are required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        if not isinstance(amount_paise, int) or amount_paise <= 0:
            return Response(
                {'error': 'amount_paise must be a positive integer'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # ── 3. Create payout via service layer
        try:
            response_data, created = create_payout(
                merchant_id=merchant_id,
                amount_paise=amount_paise,
                bank_account_id=bank_account_id,
                idempotency_key=idempotency_key,
            )

            # Queue Celery task only if newly created
            # Don't re-queue on idempotent replay
            if created:
                process_payout.delay(response_data['id'])
                logger.info(
                    f"Payout {response_data['id']} queued for processing"
                )

            return Response(
                response_data,
                status=status.HTTP_201_CREATED if created
                else status.HTTP_200_OK
                # 201 = new payout created
                # 200 = idempotent replay, same response
            )

        except InsufficientFundsError as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_422_UNPROCESSABLE_ENTITY
            )
        except ValueError as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )
        except Exception:
            logger.exception("Unexpected error creating payout")
            return Response(
                {'error': 'Internal server error'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class PayoutDetailView(APIView):
    """
    GET /api/v1/payouts/<payout_id>/
    Returns payout with full event history.
    Frontend polls this for live status updates.
    """
    def get(self, request, payout_id):
        try:
            payout = Payout.objects.prefetch_related(
                'events',
                'bank_account'
            ).get(id=payout_id)

            events = [
                {
                    'event_type': e.event_type,
                    'metadata': e.metadata,
                    'failure_reason': e.failure_reason,
                    'created_at': e.created_at.isoformat(),
                }
                for e in payout.events.all()
            ]

            return Response({
                'id': str(payout.id),
                'merchant_id': str(payout.merchant_id),
                'amount_paise': payout.amount_paise,
                'amount_rupees': payout.amount_paise / 100,
                'current_status': payout.current_status,
                'bank_account': str(payout.bank_account),
                'attempts': payout.attempts,
                'failure_reason': payout.failure_reason,
                'created_at': payout.created_at.isoformat(),
                'updated_at': payout.updated_at.isoformat(),
                'events': events,
            })

        except Payout.DoesNotExist:
            return Response(
                {'error': 'Payout not found'},
                status=status.HTTP_404_NOT_FOUND
            )


class MerchantPayoutHistoryView(APIView):
    """
    GET /api/v1/merchants/<merchant_id>/payouts/
    Returns payout history with event trails.
    Shown as payout table on dashboard.
    """
    def get(self, request, merchant_id):
        try:
            limit = int(request.query_params.get('limit', 20))
            data = get_payout_history(merchant_id, limit=limit)
            return Response(data)
        except ValueError as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_404_NOT_FOUND
            )
