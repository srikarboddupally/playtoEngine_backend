# merchants/tests.py

import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from django.test import TestCase, TransactionTestCase
from django.db import close_old_connections, connections

from merchants.models import (
    Merchant, LedgerEntry, BankAccount,
    Payout, PayoutEvent, IdempotencyKey
)
from merchants.services import create_payout
from merchants.tasks import process_payout


# ─────────────────────────────────────────
# BASE SETUP
# ─────────────────────────────────────────

def create_test_merchant(name="Test Merchant", email=None, balance_paise=0):
    """Helper — creates merchant with optional seeded balance."""
    email = email or f"{uuid.uuid4()}@test.com"
    merchant = Merchant.objects.create(name=name, email=email)
    bank = BankAccount.objects.create(
        merchant=merchant,
        account_number="1234567890123",
        ifsc_code="HDFC0001234",
        account_holder_name=name
    )
    if balance_paise > 0:
        LedgerEntry.objects.create(
            merchant=merchant,
            amount_paise=balance_paise,
            entry_type='credit',
            description='Test seed credit'
        )
    return merchant, bank


# ─────────────────────────────────────────
# TEST 1 — CONCURRENCY
# Uses TransactionTestCase, not TestCase.
# Why?
# TestCase wraps everything in one transaction
# that never commits — threads can't see each other's writes.
# TransactionTestCase actually commits between operations.
# Real concurrency requires real commits.
# ─────────────────────────────────────────

class ConcurrencyTest(TransactionTestCase):
    """
    The most critical test.
    Merchant has 100 rupees.
    Two simultaneous requests for 60 rupees each.
    Exactly one must succeed.
    Exactly one must be rejected.
    No overdraft. Ever.
    """

    def test_concurrent_payouts_exactly_one_succeeds(self):
        merchant, bank = create_test_merchant(
            name="Concurrency Test Merchant",
            balance_paise=10000  # 100 rupees
        )

        results = []
        errors = []

        def attempt_payout(key):
            close_old_connections()
            try:
                response, created = create_payout(
                    merchant_id=str(merchant.id),
                    amount_paise=6000,  # 60 rupees
                    bank_account_id=str(bank.id),
                    idempotency_key=key
                )
                results.append(('success', created))
            except Exception as e:
                errors.append(str(e))
            finally:
                connections.close_all()

        # Fire two requests simultaneously
        # Different idempotency keys — genuinely two separate requests
        keys = [str(uuid.uuid4()), str(uuid.uuid4())]

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(attempt_payout, k) for k in keys]
            for f in as_completed(futures):
                f.result()

        # Count outcomes
        successes = len(results)
        rejections = len(errors)

        print(f"\nConcurrency test results:")
        print(f"  Successes: {successes}")
        print(f"  Rejections: {rejections}")
        print(f"  Errors: {errors}")

        # Core assertion — exactly one success
        self.assertEqual(
            successes, 1,
            f"Expected exactly 1 success, got {successes}. "
            f"Errors: {errors}"
        )
        self.assertEqual(
            rejections, 1,
            f"Expected exactly 1 rejection, got {rejections}"
        )

        # Verify DB state — only one payout created
        payout_count = Payout.objects.filter(merchant=merchant).count()
        self.assertEqual(
            payout_count, 1,
            f"Expected 1 payout in DB, found {payout_count}. "
            f"Overdraft occurred."
        )

        # Verify balance invariant
        # Available balance must be >= 0
        merchant.refresh_from_db()
        available = merchant.get_available_balance()
        self.assertGreaterEqual(
            available, 0,
            f"Available balance is negative: {available}. Overdraft."
        )

        print(f"  Payout count in DB: {payout_count}")
        print(f"  Available balance after: ₹{available/100:.2f}")
        print("  ✓ Concurrency test passed")

    def test_balance_invariant_holds(self):
        """
        Sum of all ledger entries must always equal
        total_balance shown on dashboard.
        This is the invariant Playto said they check.
        """
        merchant, bank = create_test_merchant(
            balance_paise=100000  # 1000 rupees
        )

        # Add more credits
        LedgerEntry.objects.create(
            merchant=merchant,
            amount_paise=50000,
            entry_type='credit',
            description='Second credit'
        )

        # Add a debit
        LedgerEntry.objects.create(
            merchant=merchant,
            amount_paise=-30000,
            entry_type='debit',
            description='Manual debit'
        )

        from django.db.models import Sum
        raw_sum = LedgerEntry.objects.filter(
            merchant=merchant
        ).aggregate(total=Sum('amount_paise'))['total'] or 0

        displayed = merchant.get_total_balance()

        self.assertEqual(
            raw_sum, displayed,
            f"Invariant broken: raw_sum={raw_sum} != displayed={displayed}"
        )
        print(f"\n  ✓ Balance invariant holds: ₹{displayed/100:.2f}")


# ─────────────────────────────────────────
# TEST 2 — IDEMPOTENCY
# ─────────────────────────────────────────

class IdempotencyTest(TransactionTestCase):
    """
    Same idempotency key sent twice.
    Must return identical response both times.
    Must create exactly one payout in DB.
    No duplicate. No double-charge.
    """

    def test_same_key_returns_same_response(self):
        merchant, bank = create_test_merchant(
            balance_paise=100000  # 1000 rupees
        )
        key = str(uuid.uuid4())

        # First call
        response1, created1 = create_payout(
            merchant_id=str(merchant.id),
            amount_paise=5000,
            bank_account_id=str(bank.id),
            idempotency_key=key
        )

        # Second call — same key
        response2, created2 = create_payout(
            merchant_id=str(merchant.id),
            amount_paise=5000,
            bank_account_id=str(bank.id),
            idempotency_key=key
        )

        # First call must be newly created
        self.assertTrue(created1, "First call should create a new payout")

        # Second call must NOT be newly created
        self.assertFalse(created2, "Second call should return cached response")

        # Responses must be identical
        self.assertEqual(
            response1['id'], response2['id'],
            "Payout IDs must match across idempotent calls"
        )
        self.assertEqual(
            response1['amount_paise'], response2['amount_paise'],
            "Amounts must match"
        )
        self.assertEqual(
            response1['current_status'], response2['current_status'],
            "Status must match"
        )

        # Only one payout in DB
        payout_count = Payout.objects.filter(merchant=merchant).count()
        self.assertEqual(
            payout_count, 1,
            f"Expected 1 payout, found {payout_count}. Duplicate created."
        )

        # Only one idempotency key record
        key_count = IdempotencyKey.objects.filter(
            merchant=merchant,
            key=key
        ).count()
        self.assertEqual(key_count, 1, "Duplicate idempotency key stored")

        print(f"\nIdempotency test results:")
        print(f"  Payout ID: {response1['id']}")
        print(f"  Created on first call: {created1}")
        print(f"  Created on second call: {created2}")
        print(f"  Responses identical: {response1['id'] == response2['id']}")
        print(f"  Payouts in DB: {payout_count}")
        print("  ✓ Idempotency test passed")

    def test_different_keys_create_different_payouts(self):
        """
        Two different keys = two different payouts.
        Idempotency must be scoped to the key.
        Not merchant-level deduplication.
        """
        merchant, bank = create_test_merchant(
            balance_paise=100000
        )

        response1, created1 = create_payout(
            merchant_id=str(merchant.id),
            amount_paise=5000,
            bank_account_id=str(bank.id),
            idempotency_key=str(uuid.uuid4())
        )

        response2, created2 = create_payout(
            merchant_id=str(merchant.id),
            amount_paise=5000,
            bank_account_id=str(bank.id),
            idempotency_key=str(uuid.uuid4())
        )

        self.assertTrue(created1)
        self.assertTrue(created2)
        self.assertNotEqual(response1['id'], response2['id'])

        payout_count = Payout.objects.filter(merchant=merchant).count()
        self.assertEqual(payout_count, 2)
        print(f"\n  ✓ Different keys create different payouts: {payout_count} payouts")

    def test_expired_key_allows_new_payout(self):
        """
        Keys expire after 24 hours.
        After expiry — same key can be reused.
        New payout is created, not cached response.
        """
        from django.utils import timezone
        from datetime import timedelta

        merchant, bank = create_test_merchant(
            balance_paise=100000
        )
        key = str(uuid.uuid4())

        # Create first payout normally
        response1, created1 = create_payout(
            merchant_id=str(merchant.id),
            amount_paise=5000,
            bank_account_id=str(bank.id),
            idempotency_key=key
        )
        self.assertTrue(created1)

        # Manually expire the key
        IdempotencyKey.objects.filter(
            merchant=merchant,
            key=key
        ).update(expires_at=timezone.now() - timedelta(hours=1))

        # Same key — but expired — should create new payout
        response2, created2 = create_payout(
            merchant_id=str(merchant.id),
            amount_paise=5000,
            bank_account_id=str(bank.id),
            idempotency_key=key
        )
        self.assertTrue(created2, "Expired key should allow new payout creation")
        self.assertNotEqual(response1['id'], response2['id'])
        print(f"\n  ✓ Expired key allows new payout")


# ─────────────────────────────────────────
# TEST 3 — STATE MACHINE
# ─────────────────────────────────────────

class StateMachineTest(TestCase):
    """
    Illegal transitions must be rejected.
    completed → pending: illegal
    failed → completed: illegal
    pending → completed: illegal (must go through processing)
    """

    def test_illegal_transitions_are_rejected(self):
        merchant, bank = create_test_merchant(balance_paise=100000)

        payout = Payout.objects.create(
            merchant=merchant,
            amount_paise=5000,
            bank_account=bank,
            current_status=Payout.COMPLETED,
            idempotency_key=str(uuid.uuid4())
        )

        from merchants.models import InvalidTransitionError

        # completed → pending: illegal
        with self.assertRaises(InvalidTransitionError):
            payout.transition_to(Payout.PENDING)

        # completed → processing: illegal
        with self.assertRaises(InvalidTransitionError):
            payout.transition_to(Payout.PROCESSING)

        # completed → failed: illegal
        with self.assertRaises(InvalidTransitionError):
            payout.transition_to(Payout.FAILED)

        print("\n  ✓ All illegal transitions correctly rejected")

    def test_legal_transitions_succeed(self):
        merchant, bank = create_test_merchant(balance_paise=100000)

        payout = Payout.objects.create(
            merchant=merchant,
            amount_paise=5000,
            bank_account=bank,
            current_status=Payout.PENDING,
            idempotency_key=str(uuid.uuid4())
        )

        # pending → processing: legal
        payout.transition_to(Payout.PROCESSING)
        self.assertEqual(payout.current_status, Payout.PROCESSING)

        # processing → completed: legal
        payout.transition_to(Payout.COMPLETED)
        self.assertEqual(payout.current_status, Payout.COMPLETED)

        print("\n  ✓ Legal transitions succeed")

    def test_failed_payout_returns_funds(self):
        """
        When payout fails — funds must return to available.
        No explicit fund return operation.
        Available balance recalculates automatically
        because failed payouts are excluded from held.
        """
        merchant, bank = create_test_merchant(
            balance_paise=10000  # 100 rupees
        )

        payout = Payout.objects.create(
            merchant=merchant,
            amount_paise=6000,  # 60 rupees held
            bank_account=bank,
            current_status=Payout.PENDING,
            idempotency_key=str(uuid.uuid4())
        )

        # Available should be 40 rupees while pending
        available_while_pending = merchant.get_available_balance()
        self.assertEqual(available_while_pending, 4000)

        # Move to processing
        payout.transition_to(Payout.PROCESSING)
        payout.save()

        # Still held while processing
        available_while_processing = merchant.get_available_balance()
        self.assertEqual(available_while_processing, 4000)

        # Move to failed — funds return
        payout.transition_to(Payout.FAILED, reason="Bank declined")
        payout.save()

        # Now available = full 100 rupees again
        available_after_failure = merchant.get_available_balance()
        self.assertEqual(
            available_after_failure, 10000,
            "Funds must return to available after failure"
        )

        print(f"\n  Available while pending:    ₹{available_while_pending/100:.2f}")
        print(f"  Available while processing: ₹{available_while_processing/100:.2f}")
        print(f"  Available after failure:    ₹{available_after_failure/100:.2f}")
        print("  ✓ Funds returned correctly on failure")
