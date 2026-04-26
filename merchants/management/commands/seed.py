# merchants/management/commands/seed.py

from django.core.management.base import BaseCommand
from merchants.models import (
    Merchant, LedgerEntry, BankAccount,
    Payout, PayoutEvent, IdempotencyKey
)
import uuid


class Command(BaseCommand):
    help = 'Seed database with test merchants and data'

    def handle(self, *args, **kwargs):
        self.stdout.write("Clearing existing data...")
        IdempotencyKey.objects.all().delete()
        PayoutEvent.objects.all().delete()
        Payout.objects.all().delete()
        LedgerEntry.objects.all().delete()
        BankAccount.objects.all().delete()
        Merchant.objects.all().delete()

        # ── Merchant 1: Arjun
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
        LedgerEntry.objects.create(
            merchant=arjun,
            amount_paise=500000,
            entry_type='credit',
            description='Payment from Acme Corp USA — Invoice #001'
        )
        LedgerEntry.objects.create(
            merchant=arjun,
            amount_paise=750000,
            entry_type='credit',
            description='Payment from TechStart Berlin — Invoice #002'
        )
        LedgerEntry.objects.create(
            merchant=arjun,
            amount_paise=300000,
            entry_type='credit',
            description='Payment from ClientX Singapore — Invoice #003'
        )
        completed_payout = Payout.objects.create(
            merchant=arjun,
            amount_paise=200000,
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
            event_type=PayoutEvent.COMPLETED,
            metadata={'bank_reference': 'TXN00000001'}
        )
        LedgerEntry.objects.create(
            merchant=arjun,
            amount_paise=-200000,
            entry_type='debit',
            reference_id=completed_payout.id,
            description='Payout to HDFC xxxx0123 — Ref TXN00000001'
        )

        # ── Merchant 2: Priya
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
            amount_paise=1000000,
            entry_type='credit',
            description='Payment from DataFlow Inc Canada — Invoice #101'
        )
        LedgerEntry.objects.create(
            merchant=priya,
            amount_paise=600000,
            entry_type='credit',
            description='Payment from NovaTech UK — Invoice #102'
        )
        pending_payout = Payout.objects.create(
            merchant=priya,
            amount_paise=500000,
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

        # ── Merchant 3: Karan
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
            amount_paise=800000,
            entry_type='credit',
            description='Payment from Globex Australia — Invoice #201'
        )
        LedgerEntry.objects.create(
            merchant=karan,
            amount_paise=400000,
            entry_type='credit',
            description='Payment from MediaWave Japan — Invoice #202'
        )
        failed_payout = Payout.objects.create(
            merchant=karan,
            amount_paise=300000,
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
            event_type=PayoutEvent.FAILED,
            metadata={'attempt': 3},
            failure_reason='Bank did not respond after 3 attempts'
        )

        # ── Print summary
        self.stdout.write("\n" + "="*50)
        for m in [arjun, priya, karan]:
            self.stdout.write(f"\n✓ {m.name} ({m.email})")
            self.stdout.write(f"  Total:     ₹{m.get_total_balance()/100:.2f}")
            self.stdout.write(f"  Held:      ₹{m.get_held_balance()/100:.2f}")
            self.stdout.write(f"  Available: ₹{m.get_available_balance()/100:.2f}")

        self.stdout.write("\n" + "="*50)
        self.stdout.write("\nMerchant IDs:")
        self.stdout.write(f"  Arjun: {arjun.id}")
        self.stdout.write(f"  Priya: {priya.id}")
        self.stdout.write(f"  Karan: {karan.id}")
        self.stdout.write("\nSeed complete.")