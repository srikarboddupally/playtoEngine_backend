import random 
import logging

from datetime import timedelta

from celery import shared_task
from django.db import transaction, connection
from django.utils import timezone


from .models import (
    Payout,
    PayoutEvent,
    LedgerEntry,
    InsufficientFundsError,
    InvalidTransitionError,
)

logger = logging.getLogger(__name__)

def sim_bank_statements():
    roll = random.random()
    if roll < 0.70:
        return 'success'
    elif roll < 0.90:
        return 'failure'
    else:
        return 'processing'


@shared_task(bind=True, max_retries=3)
def process_payout(self, payout_id):
    logger.info(f"Processing payout {payout_id} - attempt {self.request.retries + 1}")


    try:
        with transaction.atomic():
            connection.cursor().execute(
                'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE'
            )
            payout = Payout.objects.get(id=payout_id)

            if payout.current_status == Payout.COMPLETED:
                logger.info(f"Payout {payout_id} already completed. Skipping")
                return
            if payout.current_status == Payout.FAILED:
                logger.info(f"Payout {payout_id} already failed. skipping.")
                return 
            if payout.current_status != Payout.PENDING:
                logger.warning (
                    f"Payout {payout_id} is {payout.current_status}."
                    f"Expected PENDING. Skipping."
                )
                return
            
            payout.transition_to(Payout.PROCESSING, metadata={
                'attempt' : self.request.retries + 1,
                'worker_id' : self.request.hostname or 'unknown',
            })
            payout.processing_started_at = timezone.now()
            payout.attempts += 1
            payout.save()

    except InvalidTransitionError as e:
        logger.error(f"Invalid transition for payout {payout_id}: {e}")
        return
    except Exception as e:
        logger.error(f"Error transitioning payout {payout_id} to processing: {e}")
        raise self.retry(exc=e, countdown=10)


    output = sim_bank_statements()
    logger.info(f"Bank outcome for payout {payout_id}: {output}")


    if output == 'success':
        _handle_success(payout_id)
    elif output == 'failure':
        _handle_failure(payout_id, reason = ' Bank declined the transfer')

    else:
        _handle_stuck(self, payout_id)

# Outcome Handlers

def _handle_success(payout_id):
    """
    Payout completed successfully.

    Atomically:
    1. Transition payout → COMPLETED (writes PayoutEvent)
    2. Write debit LedgerEntry

    Both happen in one transaction.
    Either both commit or neither does.

    Why atomic?
    If we write COMPLETED but crash before writing
    the debit ledger entry — balance is wrong.
    Merchant shows funds available but bank already paid out.
    Atomicity prevents this impossible state.
    """
    with transaction.atomic():
        connection.cursor().execute(
            'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE'
        )

        payout = Payout.objects.get(id=payout_id)

        payout.transition_to(
            Payout.COMPLETED,
            metadata={
                'settled_at': timezone.now().isoformat(),
                'bank_reference': f"TXN{payout_id[:8].upper()}",
            }
        )
        payout.save()

        # Write debit ledger entry
        # This is what actually moves money in the ledger.
        # Negative amount = debit.
        # Atomically with COMPLETED transition.
        LedgerEntry.objects.create(
            merchant=payout.merchant,
            amount_paise=-payout.amount_paise,  # negative = debit
            entry_type=LedgerEntry.DEBIT,
            reference_id=payout.id,
            description=(
                f"Payout to {payout.bank_account} "
                f"— Ref TXN{str(payout.id)[:8].upper()}"
            )
        )

    logger.info(
        f"Payout {payout_id} completed. "
        f"₹{payout.amount_paise/100:.2f} debited from ledger."
    )


def _handle_failure(payout_id, reason):
    """
    Payout failed. Funds return to merchant.

    How funds return:
    Available balance = total_ledger - held_in_active_payouts
    held_in_active_payouts = SUM of pending + processing payouts

    When we transition to FAILED:
    This payout is no longer pending or processing.
    It's no longer counted in held.
    Available balance increases automatically.

    No extra ledger entry needed.
    No explicit "return funds" operation.
    The math handles it by design.
    """
    with transaction.atomic():
        connection.cursor().execute(
            'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE'
        )

        payout = Payout.objects.get(id=payout_id)

        payout.transition_to(
            Payout.FAILED,
            reason=reason,
            metadata={
                'failed_at': timezone.now().isoformat(),
                'reason': reason,
                'final_attempt': payout.attempts,
            }
        )
        payout.save()

    logger.info(
        f"Payout {payout_id} failed: {reason}. "
        f"₹{payout.amount_paise/100:.2f} returned to available balance."
    )


def _handle_stuck(task_self, payout_id):
    """
    Bank is hanging. Payout is in limbo.

    If we have retries left — retry with exponential backoff.
    If we've exhausted retries — move to FAILED.

    Exponential backoff:
    Attempt 1 → wait 10 seconds
    Attempt 2 → wait 20 seconds
    Attempt 3 → wait 40 seconds
    After 3 → FAILED

    Why exponential?
    If the bank is struggling, hammering it every second
    makes it worse. Back off. Give it time to recover.
    """
    if task_self.request.retries < task_self.max_retries:
        countdown = 10 * (2 ** task_self.request.retries)
        logger.warning(
            f"Payout {payout_id} bank hung. "
            f"Retry {task_self.request.retries + 1} "
            f"in {countdown}s"
        )
        # Reset to pending so process_payout
        # can transition to processing again on retry
        with transaction.atomic():
            payout = Payout.objects.get(id=payout_id)
            payout.current_status = Payout.PENDING
            payout.save()

        raise task_self.retry(countdown=countdown)
    else:
        logger.error(
            f"Payout {payout_id} exhausted all retries. Moving to FAILED."
        )
        _handle_failure(
            payout_id,
            reason="Bank did not respond after 3 attempts"
        )

@shared_task(name='merchants.tasks.retry_stuck_payouts')

def retry_stuck_payouts():
    cutoff = timezone.now() - timedelta(seconds=30)
    stuck_payouts = Payout.objects.filter(
        current_status = Payout.PROCESSING,
        processing_started_at__lt=cutoff
    )

    cnt = stuck_payouts.count()

    if cnt:
        logger.warning(f"Found {cnt} stuck payouts. Processing...")

        for payout in stuck_payouts:
            if payout.attempts < 3:
                logger.info(f"Retrying stuck payout {payout.id}"
                            f"(attempt {payout.attempts})")
                with transaction.atomic():
                    payout.current_status = Payout.PENDING
                    payout.save()
                
                process_payout.delay(str(payout.id))
            else:
                logger.error(
                f"Payout {payout.id} stuck after "
                f"{payout.attempts} attempts. Failing."
                )
                _handle_failure(
                    str(payout.id),
                    reason="Timed out after maximum retry attempts"
                )



