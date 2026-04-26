# merchants/models.py

import uuid
from django.db import models
from django.db.models import Sum
from django.utils import timezone
from datetime import timedelta


# ─────────────────────────────────────────
# EXCEPTIONS
# ─────────────────────────────────────────

class InvalidTransitionError(Exception):
    """
    Raised when code tries an illegal state transition.
    Example: completed → pending. Never allowed.
    """
    pass


class InsufficientFundsError(Exception):
    """
    Raised when payout amount exceeds available balance.
    View catches this and returns 422.
    """
    pass


# ─────────────────────────────────────────
# MERCHANT
# ─────────────────────────────────────────

class Merchant(models.Model):
    """
    Identity only. No balance column. Ever.
    Balance is always derived from LedgerEntry stream.
    Storing balance here would be a lie —
    it would drift from reality under concurrency.
    """
    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False
    )
    name = models.CharField(max_length=255)
    email = models.EmailField(unique=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def get_total_balance(self):
        """
        SUM of all ledger entries for this merchant.
        Credits are positive. Debits are negative.
        One SQL query. Never Python arithmetic on fetched rows.

        SQL:
        SELECT SUM(amount_paise)
        FROM ledger_entries
        WHERE merchant_id = <id>
        """
        result = self.ledger_entries.aggregate(
            total=Sum('amount_paise')
        )
        return result['total'] or 0

    def get_held_balance(self):
        """
        Money locked in active payouts.
        Merchant cannot touch this.

        pending   → queued, not yet picked up
        processing → worker running, bank call in flight

        Both mean: money is committed, not settled.
        Don't let merchant withdraw this.
        """
        result = self.payouts.filter(
            current_status__in=[
                Payout.PENDING,
                Payout.PROCESSING
            ]
        ).aggregate(total=Sum('amount_paise'))
        return result['total'] or 0

    def get_available_balance(self):
        """
        What merchant can actually withdraw right now.
        This is the number shown on the dashboard.
        This is what services.py checks before creating a payout.
        """
        return self.get_total_balance() - self.get_held_balance()

    def __str__(self):
        return f"{self.name} ({self.email})"

    class Meta:
        db_table = 'merchants'


# ─────────────────────────────────────────
# LEDGER ENTRY — Event Stream #1
# ─────────────────────────────────────────

class LedgerEntry(models.Model):
    """
    THE source of truth for money movement.
    Append only. INSERT only. Never UPDATE. Never DELETE.

    This IS event sourcing applied to the money layer.
    Every rupee that ever moved through this system
    is recorded here permanently.

    Credits  → positive amount_paise
    Debits   → negative amount_paise
    Balance  → SUM(amount_paise) for a merchant

    Why single amount_paise with sign instead of
    separate credit_amount and debit_amount columns?

    Because SUM(amount_paise) gives balance directly.
    No subtraction. No Python math. One clean aggregation.
    The sign IS the semantics.
    """
    CREDIT = 'credit'   # money coming in — customer paid
    DEBIT = 'debit'     # money going out — payout completed

    TYPE_CHOICES = [
        (CREDIT, 'Credit'),
        (DEBIT, 'Debit'),
    ]

    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False
    )
    merchant = models.ForeignKey(
        'Merchant',
        on_delete=models.PROTECT,
        # PROTECT: raises error if you try to delete a merchant
        # with ledger entries. Financial history is sacred.
        # CASCADE would silently destroy audit trail. Never.
        related_name='ledger_entries'
    )
    amount_paise = models.BigIntegerField()
    # BigIntegerField → maps to bigint in postgres
    # Range: -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807
    # That's 92 trillion rupees. More than enough.
    # Positive = credit. Negative = debit.
    # NEVER FloatField. NEVER DecimalField here.
    # Integers are exact. Floats are approximations.

    entry_type = models.CharField(
        max_length=10,
        choices=TYPE_CHOICES
    )
    # Redundant with sign of amount_paise? Yes. Intentionally.
    # amount_paise sign → for math (SUM)
    # entry_type       → for filtering and display
    # "show all credits" is a clean readable query.

    reference_id = models.UUIDField(
        null=True,
        blank=True
    )
    # Which payout caused this debit?
    # Links debit entry back to specific payout.
    # Every debit must be traceable to a payout event.
    # Credits reference customer payment ID (future).

    description = models.TextField()
    # Human readable audit trail.
    # "Customer payment from Acme Corp via Stripe"
    # "Payout to HDFC xxxx1234 — Ref TXN20240101"

    created_at = models.DateTimeField(auto_now_add=True)
    # auto_now_add: set once at INSERT. Never changes.
    # This timestamp is the event time. Immutable.

    class Meta:
        db_table = 'ledger_entries'
        indexes = [
            models.Index(fields=['merchant', 'created_at']),
            # Most common query:
            # "all entries for merchant X ordered by time"
            # Composite index makes this instant.
            models.Index(fields=['reference_id']),
            # "find ledger entry for payout X"
            # Used in audit lookups.
        ]
        ordering = ['-created_at']


# ─────────────────────────────────────────
# BANK ACCOUNT
# ─────────────────────────────────────────

class BankAccount(models.Model):
    """
    Where the merchant receives INR payouts.
    One merchant can have multiple bank accounts.
    Payout specifies which one to use.
    """
    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False
    )
    merchant = models.ForeignKey(
        'Merchant',
        on_delete=models.PROTECT,
        related_name='bank_accounts'
    )
    account_number = models.CharField(max_length=20)
    ifsc_code = models.CharField(max_length=11)
    # IFSC is exactly 11 characters. Always.
    # First 4: bank code. 5th: 0 (reserved). Last 6: branch.
    account_holder_name = models.CharField(max_length=255)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        # Show last 4 digits only. Basic security.
        return f"{self.account_holder_name} — xxxx{self.account_number[-4:]}"

    class Meta:
        db_table = 'bank_accounts'


# ─────────────────────────────────────────
# PAYOUT — Projection / Read Cache
# ─────────────────────────────────────────

class Payout(models.Model):
    """
    A withdrawal request from merchant to bank account.

    IMPORTANT: This table is a PROJECTION.
    current_status here is a read cache.
    The SOURCE OF TRUTH is PayoutEvent table.

    Why keep both?
    Pure event sourcing: replay all events to get current status.
    For 1 payout → fine.
    For dashboard showing 1000 payouts → 1000 × N event queries.
    Slow and expensive.

    Solution: denormalize current_status onto Payout.
    PayoutEvent = truth (audit, history, replay)
    Payout.current_status = cache (fast reads, dashboard)

    Both updated atomically in transition_to().
    They are always in sync.
    """
    PENDING    = 'pending'
    PROCESSING = 'processing'
    COMPLETED  = 'completed'
    FAILED     = 'failed'

    STATUS_CHOICES = [
        (PENDING,    'Pending'),
        (PROCESSING, 'Processing'),
        (COMPLETED,  'Completed'),
        (FAILED,     'Failed'),
    ]

    # State machine definition.
    # Lives here. Used everywhere.
    # One place defines all legal transitions.
    LEGAL_TRANSITIONS = {
        PENDING:    [PROCESSING],
        PROCESSING: [COMPLETED, FAILED],
        COMPLETED:  [],   # terminal — nothing follows
        FAILED:     [],   # terminal — nothing follows
    }

    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False
    )
    merchant = models.ForeignKey(
        'Merchant',
        on_delete=models.PROTECT,
        related_name='payouts'
    )
    amount_paise = models.BigIntegerField()
    # Always positive on Payout.
    # The LedgerEntry debit created on completion is negative.
    # Payout records gross amount requested.

    bank_account = models.ForeignKey(
        'BankAccount',
        on_delete=models.PROTECT,
        related_name='payouts'
    )
    current_status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default=PENDING
        # Denormalized cache of latest PayoutEvent.event_type
        # Updated atomically with PayoutEvent INSERT
        # in transition_to()
    )
    idempotency_key = models.CharField(max_length=255)
    attempts = models.IntegerField(default=0)
    # How many times has Celery tried this?
    # Max 3 attempts then → FAILED

    failure_reason = models.TextField(null=True, blank=True)
    processing_started_at = models.DateTimeField(null=True, blank=True)
    # When did worker pick this up?
    # is_stuck() checks: now() - processing_started_at > 30s

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def transition_to(self, new_status, reason=None, metadata=None):
        """
        THE ONLY way to change payout status. No exceptions.
        Direct assignment (payout.current_status = 'completed')
        is forbidden — it bypasses state machine and event creation.

        What this does atomically:
        1. Validates transition is legal
        2. Creates immutable PayoutEvent (the truth)
        3. Updates current_status cache on this Payout

        Caller MUST be inside transaction.atomic().
        Both writes succeed together or neither does.
        """
        if new_status not in self.LEGAL_TRANSITIONS[self.current_status]:
            raise InvalidTransitionError(
                f"Illegal transition: "
                f"{self.current_status} → {new_status}. "
                f"Legal from '{self.current_status}': "
                f"{self.LEGAL_TRANSITIONS[self.current_status]}"
            )

        # Write the immutable event first
        # This is the truth that can never be changed
        PayoutEvent.objects.create(
            payout=self,
            event_type=new_status,
            metadata=metadata or {},
            failure_reason=reason
        )

        # Update the read cache
        self.current_status = new_status
        if reason:
            self.failure_reason = reason

        # Caller does payout.save() after this
        # We don't save here — caller controls the transaction

    def is_stuck(self):
        """
        Has this payout been processing for more than 30 seconds?
        Called by the Celery beat task every 60 seconds.
        Stuck payouts get retried or moved to failed.
        """
        if self.current_status != self.PROCESSING:
            return False
        if not self.processing_started_at:
            return False
        return (
            timezone.now() - self.processing_started_at
        ) > timedelta(seconds=30)

    def __str__(self):
        rupees = self.amount_paise / 100
        return (
            f"Payout {str(self.id)[:8]}... | "
            f"{self.merchant.name} | "
            f"₹{rupees:.2f} | "
            f"{self.current_status}"
        )

    class Meta:
        db_table = 'payouts'
        indexes = [
            models.Index(fields=['current_status', 'created_at']),
            # "all pending payouts ordered by time" — fast
            models.Index(fields=['merchant', 'current_status']),
            # "all payouts for merchant X with status Y" — fast
            models.Index(fields=['processing_started_at']),
            # "find stuck payouts" — fast
        ]
        ordering = ['-created_at']


# ─────────────────────────────────────────
# PAYOUT EVENT — Event Stream #2
# ─────────────────────────────────────────

class PayoutEvent(models.Model):
    """
    THE source of truth for payout state transitions.
    Append only. INSERT only. Never UPDATE. Never DELETE.

    Every state change creates one record here.
    Payout history = read PayoutEvent in order.
    Current state  = last PayoutEvent for a payout.

    This answers questions like:
    - When exactly did this payout start processing?
    - How many attempts before it failed?
    - What did the bank return when it completed?
    - Why did attempt 2 fail but attempt 3 succeed?

    Payout.current_status cannot answer these.
    PayoutEvent can. Always.
    """
    REQUESTED  = 'requested'
    PROCESSING = 'processing'
    COMPLETED  = 'completed'
    FAILED     = 'failed'

    EVENT_TYPES = [
        (REQUESTED,  'Payout Requested'),
        (PROCESSING, 'Processing Started'),
        (COMPLETED,  'Payout Completed'),
        (FAILED,     'Payout Failed'),
    ]

    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False
    )
    payout = models.ForeignKey(
        'Payout',
        on_delete=models.PROTECT,
        related_name='events'
        # PROTECT: never delete a payout that has events
        # Events are the historical record
    )
    event_type = models.CharField(
        max_length=20,
        choices=EVENT_TYPES
    )
    metadata = models.JSONField(default=dict)
    # Flexible context per event type.
    # REQUESTED:  {bank_account_id, idempotency_key}
    # PROCESSING: {attempt: 1, worker_id: "celery-1"}
    # COMPLETED:  {bank_reference: "TXN123", settled_at: "..."}
    # FAILED:     {bank_error: "TIMEOUT", attempt: 3}

    failure_reason = models.TextField(null=True, blank=True)
    # Populated only on FAILED events.
    # What went wrong. Why. From the bank or from us.

    created_at = models.DateTimeField(auto_now_add=True)
    # This IS the event timestamp.
    # Immutable. The moment this transition happened.
    # Never changes after INSERT.

    class Meta:
        db_table = 'payout_events'
        indexes = [
            models.Index(fields=['payout', 'created_at']),
            # "all events for payout X in chronological order"
            # Most critical query for this table.
        ]
        ordering = ['created_at']
        # Oldest first — natural event stream order
        # Replay from beginning to reconstruct state


# ─────────────────────────────────────────
# IDEMPOTENCY KEY
# ─────────────────────────────────────────

class IdempotencyKey(models.Model):
    """
    Every idempotency key we've ever seen.
    Before creating a payout — check here first.

    Key seen before → return cached response. Done.
    Key not seen    → create payout, store key + response.

    Scoped per merchant:
    merchant_A + "key-abc" and merchant_B + "key-abc"
    are completely independent records.

    Expires after 24 hours:
    After expiry, same key can be reused.
    Old enough that it's a genuinely new request.
    """
    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False
    )
    merchant = models.ForeignKey(
        'Merchant',
        on_delete=models.CASCADE,
        related_name='idempotency_keys'
    )
    key = models.CharField(max_length=255)
    # The UUID the merchant sent in Idempotency-Key header

    response_status_code = models.IntegerField()
    # 201 created, 422 insufficient funds, etc.
    # Second call returns this exact status code.

    response_body = models.JSONField()
    # Exact JSON returned on first call.
    # Second call gets byte-for-byte identical response.
    # Not "a similar response". The SAME response.

    created_at = models.DateTimeField(auto_now_add=True)
    expires_at = models.DateTimeField()
    # Set to created_at + 24 hours in services.py

    class Meta:
        db_table = 'idempotency_keys'
        unique_together = [['merchant', 'key']]
        # Database enforces uniqueness.
        # Not Python. Not application logic.
        # The DB constraint is the guarantee.
        indexes = [
            models.Index(fields=['merchant', 'key']),
            # "has this merchant used this key?" — fast
            models.Index(fields=['expires_at']),
            # cleanup job: "delete expired keys" — fast
        ]