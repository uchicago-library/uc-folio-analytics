--metadb:function TS_Acq_Default_Fund

DROP FUNCTION IF EXISTS TS_Acq_Default_Fund;

CREATE FUNCTION TS_Acq_Default_Fund(
    )
RETURNS TABLE (
    vendor text,
    fund text,
    account text,
    est_price text,
    po_line_number text,
    title text,
    publisher text,
    edition text,
    internal_note text,
    date_created date,
    created_by text,
    po_status text
    )
AS $$
SELECT 
    org.name as vendor,
    jsonb_extract_path_text(ff.jsonb, 'code') as fund,
    jsonb_extract_path_text(pol.jsonb, 'vendorDetail', 'vendorAccount') as account,
    jsonb_extract_path_text(pol.jsonb, 'cost', 'poLineEstimatedPrice') as est_price,
    jsonb_extract_path_text(pol.jsonb, 'poLineNumber') as po_line_number,
    jsonb_extract_path_text(pol.jsonb, 'titleOrPackage') as title,
    jsonb_extract_path_text(pol.jsonb, 'publisher') as publisher,
    jsonb_extract_path_text(pol.jsonb, 'edition') as edition,
    jsonb_extract_path_text(pol.jsonb, 'description') AS internal_note,
    pol.creation_date::date as date_created,
    u.username as created_by,
    po.workflow_status as po_status
FROM folio_orders.po_line pol
LEFT JOIN folio_orders.purchase_order__t po on pol.purchaseorderid = po.id
LEFT JOIN folio_finance.transaction ft 
    ON pol.id = jsonb_extract_path_text(ft.jsonb, 'encumbrance', 'sourcePoLineId')::uuid    
LEFT JOIN folio_organizations.organizations__t org on po.vendor = org.id
LEFT JOIN folio_finance.fund ff on ft.fromfundid = ff.id
LEFT JOIN folio_users.users__t u on pol.created_by = u.id
WHERE 
    jsonb_extract_path_text(ff.jsonb, 'code') = 'GCOMG'
    AND u.username in ('lschiller', 'clthomas', 'hbruch', 'jpoe', 'mhwhite19')
    AND pol.creation_date::date > current_date - INTERVAL '7 days'
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;
