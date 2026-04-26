# merchants/seed.py

import os
import sys
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from merchants.models import Merchant, LedgerEntry, BankAccount, Payout, PayoutEvent, IdempotencyKey
from django.utils import timezone
import uuid

def clear_data():
    print("Clearing existing data...")
    IdempotencyKey.objects.all().delete()
    PayoutEvent.objects.all().delete()
    Payout.objects.all().delete()
    LedgerEntry.objects.all().delete()
    BankAccount.objects.all().delete()
    Merchant.objects.all().delete()
    print("Done.\n")


def seed():
    clear_data()

    # ── Merchant 1: Arjun — healthy balance
    arjun = Merchant.objects.create(
        name="Arjun Sharma",
        email="arjun@designstudio.in"
    )
    arjun_bank = BankAccount.objects.create(
        merchant=arjun,
        account_number="9876543210123",
        ifsc_code="HDFC0001234",
        account_holder_name="Arjun Sharma"
    )
    # Credit history — international clients paying
    LedgerEntry.objects.create(
        merchant=arjun,
        amount_paise=500000,   # ₹5000
        entry_type='credit',
        description='Payment from Acme Corp USA — Invoice #001'
    )
    LedgerEntry.objects.create(
        merchant=arjun,
        amount_paise=750000,   # ₹7500
        entry_type='credit',
        description='Payment from TechStart Berlin — Invoice #002'
    )
    LedgerEntry.objects.create(
        merchant=arjun,
        amount_paise=300000,   # ₹3000
        entry_type='credit',
        description='Payment from ClientX Singapore — Invoice #003'
    )
    # One completed payout already
    completed_payout = Payout.objects.create(
        merchant=arjun,
        amount_paise=200000,   # ₹2000
        bank_account=arjun_bank,
        current_status=Payout.COMPLETED,
        idempotency_key=str(uuid.uuid4()),
        attempts=1,
    )
    PayoutEvent.objects.create(
        payout=completed_payout,
        event_type=PayoutEvent.REQUESTED,
        metadata={'amount_paise': 200000}
    )
    PayoutEvent.objects.create(
        payout=completed_payout,
        event_type=PayoutEvent.PROCESSING,
        metadata={'attempt': 1}
    )
    PayoutEvent.objects.create(
        payout=completed_payout,
        event_type=PayoutEvent.COMPLETED,
        metadata={'bank_reference': 'TXN00000001'}
    )
    # Debit ledger entry for that completed payout
    LedgerEntry.objects.create(
        merchant=arjun,
        amount_paise=-200000,  # ₹2000 debit
        entry_type='debit',
        reference_id=completed_payout.id,
        description=f'Payout to HDFC xxxx0123 — Ref TXN00000001'
    )
    print(f"✓ Merchant: {arjun.name}")
    print(f"  Total balance:     ₹{arjun.get_total_balance()/100:.2f}")
    print(f"  Held balance:      ₹{arjun.get_held_balance()/100:.2f}")
    print(f"  Available balance: ₹{arjun.get_available_balance()/100:.2f}\n")


    # ── Merchant 2: Priya — pending payout in flight
    priya = Merchant.objects.create(
        name="Priya Nair",
        email="priya@freelancedev.in"
    )
    priya_bank = BankAccount.objects.create(
        merchant=priya,
        account_number="1122334455667",
        ifsc_code="SBIN0005678",
        account_holder_name="Priya Nair"
    )
    LedgerEntry.objects.create(
        merchant=priya,
        amount_paise=1000000,  # ₹10000
        entry_type='credit',
        description='Payment from DataFlow Inc Canada — Invoice #101'
    )
    LedgerEntry.objects.create(
        merchant=priya,
        amount_paise=600000,   # ₹6000
        entry_type='credit',
        description='Payment from NovaTech UK — Invoice #102'
    )
    # One pending payout — funds held
    pending_payout = Payout.objects.create(
        merchant=priya,
        amount_paise=500000,   # ₹5000 held
        bank_account=priya_bank,
        current_status=Payout.PENDING,
        idempotency_key=str(uuid.uuid4()),
        attempts=0,
    )
    PayoutEvent.objects.create(
        payout=pending_payout,
        event_type=PayoutEvent.REQUESTED,
        metadata={'amount_paise': 500000}
    )
    print(f"✓ Merchant: {priya.name}")
    print(f"  Total balance:     ₹{priya.get_total_balance()/100:.2f}")
    print(f"  Held balance:      ₹{priya.get_held_balance()/100:.2f}")
    print(f"  Available balance: ₹{priya.get_available_balance()/100:.2f}\n")


    # ── Merchant 3: Karan — failed payout, full balance available
    karan = Merchant.objects.create(
        name="Karan Mehta",
        email="karan@contentco.in"
    )
    karan_bank = BankAccount.objects.create(
        merchant=karan,
        account_number="9988776655443",
        ifsc_code="ICIC0009012",
        account_holder_name="Karan Mehta"
    )
    LedgerEntry.objects.create(
        merchant=karan,
        amount_paise=800000,   # ₹8000
        entry_type='credit',
        description='Payment from Globex Australia — Invoice #201'
    )
    LedgerEntry.objects.create(
        merchant=karan,
        amount_paise=400000,   # ₹4000
        entry_type='credit',
        description='Payment from MediaWave Japan — Invoice #202'
    )
    # Failed payout — funds returned automatically
    failed_payout = Payout.objects.create(
        merchant=karan,
        amount_paise=300000,   # ₹3000
        bank_account=karan_bank,
        current_status=Payout.FAILED,
        idempotency_key=str(uuid.uuid4()),
        attempts=3,
        failure_reason='Bank did not respond after 3 attempts'
    )
    PayoutEvent.objects.create(
        payout=failed_payout,
        event_type=PayoutEvent.REQUESTED,
        metadata={'amount_paise': 300000}
    )
    PayoutEvent.objects.create(
        payout=failed_payout,
        event_type=PayoutEvent.PROCESSING,
        metadata={'attempt': 1}
    )
    PayoutEvent.objects.create(
        payout=failed_payout,
        event_type=PayoutEvent.FAILED,
        metadata={'attempt': 3},
        failure_reason='Bank did not respond after 3 attempts'
    )
    print(f"✓ Merchant: {karan.name}")
    print(f"  Total balance:     ₹{karan.get_total_balance()/100:.2f}")
    print(f"  Held balance:      ₹{karan.get_held_balance()/100:.2f}")
    print(f"  Available balance: ₹{karan.get_available_balance()/100:.2f}\n")

    print("Seed complete.")
    print("\nMerchant IDs for testing:")
    print(f"  Arjun: {arjun.id}")
    print(f"  Priya: {priya.id}")
    print(f"  Karan: {karan.id}")


if __name__ == '__main__':
    seed()