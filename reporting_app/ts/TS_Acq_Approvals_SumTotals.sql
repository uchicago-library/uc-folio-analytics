-- metadb:function TS_Acq_Approvals_SumTotals

DROP FUNCTION IF EXISTS TS_Acq_Approvals_SumTotals;

CREATE FUNCTION TS_Acq_Approvals_SumTotals(
    end_date date --DEFAULT current_date
)
RETURNS TABLE (
    bill_to TEXT,
    vendor TEXT,
    pol_fund_codes TEXT,
    pol_requester TEXT,
    pol_account TEXT,
    pol_estimated_cost_total NUMERIC(19,4),
    invoice_fund_code TEXT,
    invoice_line_total NUMERIC(19,4)
)
AS
$$
WITH
invoice_report AS (
    SELECT
        fti.po_line_id AS pol_id,
        fti.effective_fund_code AS invoice_fund_code,
        sum(fti.invoice_line_total) AS invoice_line_total
    FROM folio_derived.finance_transaction_invoices fti
    LEFT JOIN folio_derived.finance_invoice_transactions fit ON fit.invoice_line_id = fti.invoice_line_id
    WHERE fit.invoice_status != 'Cancelled'
    GROUP BY fti.po_line_id, fti.effective_fund_code
)
SELECT
    po.bill_to AS bill_to,
    po.vendor_code AS po_vendor,
    ftpo.transaction_from_fund_code AS pol_fund_code,
    po.requester AS pol_requester,
    jsonb_extract_path_text (pol. jsonb, 'vendorDetail', 'vendorAccount') AS pol_account,
    sum(ftpo.transaction_encumbrance_initial_amount) AS pol_estimated_cost_total,
    ir.invoice_fund_code AS invoice_fund_code,
    sum(ir.invoice_line_total) AS invoice_line_total
FROM folio_derived.po_instance po
LEFT JOIN folio_orders.po_line pol ON pol.id = po.po_line_id
LEFT JOIN folio_derived.finance_transaction_purchase_order ftpo ON ftpo.po_line_id = po.po_line_id
LEFT JOIN invoice_report ir ON ir.pol_id::uuid = po.po_line_id 
WHERE 
    --po.bill_to = 'JRL Acquisitions' AND 
    jsonb_extract_path_text(pol.jsonb, 'acquisitionMethod') = '796596c4-62b5-4b64-a2ce-524c747afaa2' -- UUID for Approval Plan
    AND po.created_date::date >= '2026-07-01'   -- Beginning of Fiscal Year
    AND po.created_date::date < end_date    -- Enter Friday report is being run
GROUP BY po.vendor_code,
    po.bill_to, ftpo.transaction_from_fund_code, jsonb_extract_path_text (pol. jsonb, 'vendorDetail', 'vendorAccount'), po.requester, 
    ir.invoice_fund_code
 $$
 LANGUAGE SQL
 STABLE
 PARALLEL SAFE;
