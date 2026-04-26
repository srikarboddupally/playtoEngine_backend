-- Check all merchants and balances
SELECT id, name, email FROM merchants;

-- Check ledger entries
SELECT 
    m.name,
    l.amount_paise,
    l.entry_type,
    l.description,
    l.created_at
FROM ledger_entries l
JOIN merchants m ON m.id = l.merchant_id
ORDER BY l.created_at DESC;

-- Check payouts with status
SELECT
    m.name,
    p.amount_paise,
    p.current_status,
    p.attempts,
    p.created_at
FROM payouts p
JOIN merchants m ON m.id = p.merchant_id
ORDER BY p.created_at DESC;

-- Check payout events — the event stream
SELECT
    p.amount_paise,
    pe.event_type,
    pe.metadata,
    pe.created_at
FROM payout_events pe
JOIN payouts p ON p.id = pe.payout_id
ORDER BY pe.created_at ASC;

-- The invariant check Playto said they run
SELECT
    m.name,
    SUM(l.amount_paise) as total_balance_paise,
    SUM(l.amount_paise) / 100.0 as total_balance_rupees
FROM ledger_entries l
JOIN merchants m ON m.id = l.merchant_id
GROUP BY m.id, m.name;