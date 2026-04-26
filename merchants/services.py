# merchants/services.py

import logging
from datetime import timedelta

from django.db import transaction, connection, OperationalError
from django.db.models import Sum
from django.utils import timezone

from .models import (
    Merchant,
    Payout,
    PayoutEvent,
    BankAccount,
    IdempotencyKey,
    InsufficientFundsError,
    InvalidTransitionError,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────
# SERIALIZABLE RETRY DECORATOR
# ─────────────────────────────────────────

def with_serializable_retry(fn, max_retries=3):
    """
    Serializable transactions can fail with:
    ERROR: could not serialize access due to concurrent update

    This is not a bug. This is Postgres doing its job.
    It detected a conflict. It's telling you to retry.

    We retry up to max_retries times with a small backoff.
    If all retries fail — something is genuinely wrong.

    Why not select_for_update()?
    select_for_update() = pessimistic locking
      → lock first, then read
      → under concurrency: queue builds up, latency spikes

    SERIALIZABLE = optimistic concurrency control
      → read freely, write carefully
      → Postgres detects conflicts at commit time
      → conflict is rare for different merchants
      → no lock queue, lower latency
    """
    import time

    def wrapper(*args, **kwargs):
        last_error = None
        for attempt in range(max_retries):
            try:
                return fn(*args, **kwargs)
            except OperationalError as e:
                # Postgres serialization failure error code: 40001
                if '40001' in str(e) or 'serialize' in str(e).lower():
                    last_error = e
                    wait = 0.1 * (2 ** attempt)  # 0.1s, 0.2s, 0.4s
                    logger.warning(
                        f"Serialization conflict on attempt "
                        f"{attempt + 1}, retrying in {wait}s"
                    )
                    time.sleep(wait)
                    continue
                raise  # not a serialization error — don't retry
        raise last_error  # all retries exhausted
    return wrapper


# ─────────────────────────────────────────
# IDEMPOTENCY HELPERS
# ─────────────────────────────────────────

def get_valid_idempotency_key(merchant, key):
    """
    Check if this merchant has used this key before.
    Returns IdempotencyKey if found and not expired.
    Returns None if not found or expired.
    """
    try:
        record = IdempotencyKey.objects.get(
            merchant=merchant,
            key=key,
            expires_at__gt=timezone.now()  # not expired
        )
        return record
    except IdempotencyKey.DoesNotExist:
        return None


def store_idempotency_key(merchant, key, status_code, response_body):
    """
    Use update_or_create so expired keys get overwritten.
    If key exists but expired — update it with new response.
    If key doesn't exist — create it fresh.
    """
    IdempotencyKey.objects.update_or_create(
        merchant=merchant,
        key=key,
        defaults={
            'response_status_code': status_code,
            'response_body': response_body,
            'expires_at': timezone.now() + timedelta(hours=24),
        }
    )


# ─────────────────────────────────────────
# CORE SERVICE
# ─────────────────────────────────────────

def _create_payout_inner(
    merchant_id,
    amount_paise,
    bank_account_id,
    idempotency_key
):
    """
    Inner function — wrapped by retry decorator.
    Contains the actual transaction logic.

    SERIALIZABLE isolation means:
    Postgres guarantees that concurrent transactions
    produce the same result as if they ran serially.

    If two requests try to overdraw the same merchant:
    - Both read balance = 10000
    - Both think amount 6000 is valid
    - First commits successfully
    - Second → Postgres detects the conflict → raises 40001
    - Retry decorator catches 40001 → retries
    - On retry → balance now shows held funds → rejected cleanly

    No explicit locks. Postgres does the work.
    """
    with transaction.atomic():
        # Set SERIALIZABLE for this transaction
        # Must be the first statement inside atomic()
        connection.cursor().execute(
            'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE'
        )

        # ── 1. Get merchant
        try:
            merchant = Merchant.objects.get(id=merchant_id)
        except Merchant.DoesNotExist:
            raise ValueError(f"Merchant {merchant_id} not found")

        # ── 2. Check idempotency INSIDE transaction
        # Why inside? If first request is still in flight
        # and second arrives — the unique_together constraint
        # on IdempotencyKey will catch it at the DB level.
        # No duplicate payouts. Ever.
        existing = get_valid_idempotency_key(merchant, idempotency_key)
        if existing:
            logger.info(
                f"Idempotency key {idempotency_key} already seen "
                f"for merchant {merchant_id} — returning cached response"
            )
            return existing.response_body, False
            # False = not newly created

        # ── 3. Get bank account
        try:
            bank_account = BankAccount.objects.get(
                id=bank_account_id,
                merchant=merchant,
                is_active=True
            )
        except BankAccount.DoesNotExist:
            raise ValueError(
                f"Bank account {bank_account_id} not found "
                f"or not active for merchant {merchant_id}"
            )

        # ── 4. Calculate available balance
        # All at DB level. No Python arithmetic on fetched rows.
        total = merchant.ledger_entries.aggregate(
            total=Sum('amount_paise')
        )['total'] or 0

        held = merchant.payouts.filter(
            current_status__in=[Payout.PENDING, Payout.PROCESSING]
        ).aggregate(
            total=Sum('amount_paise')
        )['total'] or 0

        available = total - held

        logger.info(
            f"Merchant {merchant_id} balance check: "
            f"total={total} held={held} available={available} "
            f"requested={amount_paise}"
        )

        # ── 5. Check funds
        if amount_paise > available:
            raise InsufficientFundsError(
                f"Insufficient funds. "
                f"Requested: ₹{amount_paise/100:.2f}, "
                f"Available: ₹{available/100:.2f}"
            )

        if amount_paise <= 0:
            raise ValueError(
                f"Amount must be positive. Got: {amount_paise}"
            )

        # ── 6. Create payout
        payout = Payout.objects.create(
            merchant=merchant,
            amount_paise=amount_paise,
            bank_account=bank_account,
            current_status=Payout.PENDING,
            idempotency_key=idempotency_key,
        )

        # ── 7. Create first event — REQUESTED
        # This is the event sourcing write.
        # From this moment, this payout exists in the event stream.
        PayoutEvent.objects.create(
            payout=payout,
            event_type=PayoutEvent.REQUESTED,
            metadata={
                'amount_paise': amount_paise,
                'bank_account_id': str(bank_account_id),
                'idempotency_key': idempotency_key,
                'available_balance_at_request': available,
            }
        )

        # ── 8. Build response body
        # Build it here so we store the exact same response
        # in idempotency table
        response_data = {
            'id': str(payout.id),
            'merchant_id': str(merchant.id),
            'amount_paise': payout.amount_paise,
            'amount_rupees': payout.amount_paise / 100,
            'bank_account_id': str(bank_account.id),
            'current_status': payout.current_status,
            'idempotency_key': payout.idempotency_key,
            'created_at': payout.created_at.isoformat(),
        }

        # ── 9. Store idempotency key + response
        # Inside same transaction.
        # If anything fails after this — key is not stored.
        # Next request will try again fresh. Correct.
        store_idempotency_key(
            merchant=merchant,
            key=idempotency_key,
            status_code=201,
            response_body=response_data
        )

        logger.info(
            f"Payout {payout.id} created for merchant "
            f"{merchant_id} — ₹{amount_paise/100:.2f}"
        )

        return response_data, True
        # True = newly created


# ─────────────────────────────────────────
# PUBLIC INTERFACE
# ─────────────────────────────────────────

def create_payout(merchant_id, amount_paise, bank_account_id, idempotency_key):
    """
    Public function. Called by views.py.
    Wraps inner function with serializable retry logic.

    Returns: (response_data, created)
    - response_data: dict ready to return as JSON
    - created: True if new payout, False if idempotent replay
    """
    fn = with_serializable_retry(_create_payout_inner)
    return fn(merchant_id, amount_paise, bank_account_id, idempotency_key)


def get_merchant_balance(merchant_id):
    """
    Returns complete balance picture for dashboard.
    All calculations at DB level.
    """
    try:
        merchant = Merchant.objects.get(id=merchant_id)
    except Merchant.DoesNotExist:
        raise ValueError(f"Merchant {merchant_id} not found")

    total = merchant.get_total_balance()
    held = merchant.get_held_balance()
    available = merchant.get_available_balance()
    bank_accounts = merchant.bank_accounts.filter(is_active=True).order_by('created_at')

    return {
        'merchant_id': str(merchant.id),
        'merchant_name': merchant.name,
        'bank_accounts': [
            {
                'id': str(bank.id),
                'account_holder_name': bank.account_holder_name,
                'account_number_last4': bank.account_number[-4:],
                'ifsc_code': bank.ifsc_code,
            }
            for bank in bank_accounts
        ],
        'total_balance_paise': total,
        'held_balance_paise': held,
        'available_balance_paise': available,
        'total_balance_rupees': total / 100,
        'held_balance_rupees': held / 100,
        'available_balance_rupees': available / 100,
    }


def get_merchant_ledger(merchant_id, limit=20):
    """
    Recent ledger entries for the dashboard.
    Credits and debits in chronological order.
    """
    try:
        merchant = Merchant.objects.get(id=merchant_id)
    except Merchant.DoesNotExist:
        raise ValueError(f"Merchant {merchant_id} not found")

    entries = merchant.ledger_entries.select_related(
        'merchant'
    ).order_by('-created_at')[:limit]

    return [
        {
            'id': str(e.id),
            'amount_paise': e.amount_paise,
            'amount_rupees': e.amount_paise / 100,
            'entry_type': e.entry_type,
            'description': e.description,
            'reference_id': str(e.reference_id) if e.reference_id else None,
            'created_at': e.created_at.isoformat(),
        }
        for e in entries
    ]


def get_payout_history(merchant_id, limit=20):
    """
    Payout history with full event trail.
    Used for the payout table on dashboard.
    """
    try:
        merchant = Merchant.objects.get(id=merchant_id)
    except Merchant.DoesNotExist:
        raise ValueError(f"Merchant {merchant_id} not found")

    payouts = merchant.payouts.prefetch_related(
        'events',
        'bank_account'
    ).order_by('-created_at')[:limit]

    result = []
    for p in payouts:
        # Get full event history for this payout
        # This is event sourcing paying off —
        # complete audit trail, one query per payout
        events = [
            {
                'event_type': e.event_type,
                'metadata': e.metadata,
                'failure_reason': e.failure_reason,
                'created_at': e.created_at.isoformat(),
            }
            for e in p.events.all()
        ]

        result.append({
            'id': str(p.id),
            'amount_paise': p.amount_paise,
            'amount_rupees': p.amount_paise / 100,
            'current_status': p.current_status,
            'bank_account': str(p.bank_account),
            'attempts': p.attempts,
            'failure_reason': p.failure_reason,
            'created_at': p.created_at.isoformat(),
            'updated_at': p.updated_at.isoformat(),
            'events': events,
        })

    return result
