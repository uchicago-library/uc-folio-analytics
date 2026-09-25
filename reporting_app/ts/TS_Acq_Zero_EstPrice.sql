--metadb:function TS_Acq_Zero_EstPrice

DROP FUNCTION TS_Acq_Zero_EstPrice;

CREATE FUNCTION TS_Acq_Zero_EstPrice(
    )
RETURNS TABLE (
	vendor text,
	fund text,
	account text,
	est_price text,
	po_line_number text,
	title text,
	ISBNs text,
	publisher text,
	edition text,
	date_created date,
	created_by text,
	po_status text,
	acquisitions_method text
)
AS
$$
WITH 
prod_ids_agg as (
	SELECT
		pol2.id AS id,
		string_agg(((prodIds.jsonb #> '{}') ->> 'productId'), ' | ') AS prod_ids
	FROM folio_orders.po_line pol2
	    CROSS JOIN LATERAL jsonb_array_elements(jsonb_extract_path(jsonb, 'details', 'productIds'))
	        AS prodIds (jsonb)
	GROUP BY pol2.id
	)
SELECT 
	org.name AS vendor,
	jsonb_extract_path_text(ff.jsonb, 'code') AS fund,
	jsonb_extract_path_text(pol.jsonb, 'vendorDetail', 'vendorAccount') AS account,
	jsonb_extract_path_text(pol.jsonb, 'cost', 'poLineEstimatedPrice') AS est_price,
	jsonb_extract_path_text(pol.jsonb, 'poLineNumber') AS po_line_number,
	jsonb_extract_path_text(pol.jsonb, 'titleOrPackage') AS title,
	pia.prod_ids AS ISBNs,
	jsonb_extract_path_text(pol.jsonb, 'publisher') AS publisher,
	jsonb_extract_path_text(pol.jsonb, 'edition') AS edition,
	pol.creation_date::date AS date_created,
	u.username AS created_by,
	po.workflow_status as po_status,
	amt.value AS acquisitions_method
FROM folio_orders.po_line pol
LEFT JOIN folio_orders.purchase_order__t po on pol.purchaseorderid = po.id
LEFT JOIN folio_finance.transaction ft 
	ON pol.id = jsonb_extract_path_text(ft.jsonb, 'encumbrance', 'sourcePoLineId')::uuid	
LEFT JOIN folio_organizations.organizations__t org ON po.vendor = org.id
LEFT JOIN folio_finance.fund ff ON ft.fromfundid = ff.id
LEFT JOIN folio_users.users__t u ON pol.created_by = u.id
LEFT JOIN prod_ids_agg pia ON pia.id = pol.id
LEFT JOIN folio_orders.acquisition_method__t amt ON amt.id = jsonb_extract_path_text(pol.jsonb, 'acquisitionMethod')::uuid
WHERE 
	jsonb_extract_path_text(pol.jsonb, 'cost', 'poLineEstimatedPrice') = '0.0'
	AND amt.value IN ('Purchase At Vendor System', 'Approval Plan')
	AND u.username in ('lschiller', 'clthomas', 'hbruch', 'jpoe', 'mhwhite19')
	AND jsonb_extract_path_text(pol.jsonb, 'details', 'receivingNote') NOT LIKE '%replacement%'
	AND po.workflow_status = 'Open'
	AND pol.creation_date::date > current_date - INTERVAL '7 days'
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;
