--metadb:function TS_Acq_Approval_Orders

DROP FUNCTION IF EXISTS TS_Acq_Approval_Orders;

CREATE FUNCTION TS_Acq_Approval_Orders(
    start_date date, --DEFAULT current_date - 7,
    end_date date --DEFAULT current_date
)
RETURNS TABLE (
	vendor text,
	pol_fund text,
	pol_donor_codes text,
	pol_account text,
	pol_rec_note text,
	pol_internal_note text,
	pol_requester text,
	pol_est_price numeric (19,4),
	po_line_number text,
	pol_creation_date date,
	title text,
	pol_contributors text,
	pol_product_id text,
	pol_publisher text,
	pol_edition text,
	pol_invoice_number text
)
AS
$$
    WITH prod_ids_agg as (
	SELECT
		plt2.po_line_number AS pol_number,
		string_agg(((prodIds.jsonb #> '{}') ->> 'productId'), ' | ') AS prod_ids
	FROM folio_orders.po_line pol2
	    CROSS JOIN LATERAL jsonb_array_elements(jsonb_extract_path(jsonb, 'details', 'productIds'))
	        AS prodIds (jsonb)
	LEFT JOIN folio_orders.po_line__t as plt2 on plt2.po_line_number = jsonb_extract_path_text(pol2.jsonb, 'poLineNumber')
	WHERE prodIds.jsonb #>> '{productIdType}' != '37b65e79-0392-450d-adc6-e2a1f47de452'
	GROUP BY plt2.po_line_number
	),
inv_agg as (
	SELECT
		plt3.po_line_number AS pol_number,
		string_agg(((prodIds.jsonb #> '{}') ->> 'productId'), ' | ') AS invoice
	FROM folio_orders.po_line pol3
	    CROSS JOIN LATERAL jsonb_array_elements(jsonb_extract_path(jsonb, 'details', 'productIds'))
	        AS prodIds (jsonb)
	LEFT JOIN folio_orders.po_line__t AS plt3 ON plt3.po_line_number = jsonb_extract_path_text(pol3.jsonb, 'poLineNumber')
	WHERE prodIds.jsonb #>> '{productIdType}' = '37b65e79-0392-450d-adc6-e2a1f47de452' -- uuid for Report number
	GROUP BY plt3.po_line_number
	),
contributors_agg as (
	SELECT
		string_agg((contributors.jsonb #>> '{contributor}'), ' | ') AS contributors,
		pol4.id AS id
	FROM folio_orders.po_line pol4
		CROSS JOIN LATERAL jsonb_array_elements(jsonb_extract_path(jsonb, 'contributors')) AS contributors (jsonb)
	GROUP BY pol4.id
	),
donor_codes AS ( 
	SELECT 
		string_agg(ot.code, ' | ') AS donor_codes,
		pol2.id as id
	FROM folio_orders.po_line pol2
	CROSS JOIN LATERAL jsonb_array_elements(jsonb_extract_path(jsonb, 'donorOrganizationIds'))
	       AS donor_uuids (jsonb)
	LEFT JOIN folio_organizations.organizations__t ot ON ot.id = (donor_uuids.jsonb #>> '{}')::uuid
	GROUP BY pol2.id
	),
fund_codes AS (
	SELECT
		string_agg((fundDis.jsonb #>> '{code}'), ' | ') AS funds,
	--	string_agg(ft.code, ' | ') AS funds2,
		pol3.id AS id
	FROM folio_orders.po_line pol3 
		CROSS JOIN LATERAL jsonb_array_elements(jsonb_extract_path(jsonb, 'fundDistribution')) AS fundDis (jsonb)
	--LEFT JOIN folio_finance.fund__t ft ON ft.id = (fundDis.jsonb #>> '{fundId}')::uuid
	GROUP BY pol3.id
)
SELECT 
	org.name AS vendor,
	fc.funds AS pol_fund,
	dc.donor_codes AS pol_donor_codes,
	jsonb_extract_path_text(pol.jsonb, 'vendorDetail', 'vendorAccount') AS pol_account,
	jsonb_extract_path_text(pol.jsonb, 'details', 'receivingNote') AS pol_rec_note,
	jsonb_extract_path_text(pol.jsonb, 'description') AS pol_internal_note,
	jsonb_extract_path_text(pol.jsonb, 'requester') AS pol_requester,
	jsonb_extract_path_text(pol.jsonb, 'cost', 'poLineEstimatedPrice')::numeric(19,4) AS pol_est_price,
	plt.po_line_number AS po_line_number,
	pol.creation_date AS pol_creation_date,
	plt.title_or_package AS title,
	cont.contributors AS pol_contributors,
	pia.prod_ids AS pol_product_id,
	plt.publisher AS pol_publisher,
	plt.edition AS pol_edition,
	inva.invoice AS pol_invoice_number
FROM folio_orders.po_line pol
LEFT JOIN folio_orders.po_line__t AS plt ON plt.po_line_number = jsonb_extract_path_text(pol.jsonb, 'poLineNumber')
LEFT JOIN folio_orders.purchase_order__t po ON pol.purchaseorderid = po.id
LEFT JOIN donor_codes AS dc ON dc.id = pol.id
LEFT JOIN prod_ids_agg pia ON plt.po_line_number = pia.pol_number
LEFT JOIN inv_agg inva ON plt.po_line_number = inva.pol_number
LEFT JOIN contributors_agg cont ON pol.id = cont.id
LEFT JOIN fund_codes AS fc ON fc.id = pol.id
LEFT JOIN folio_organizations.organizations__t org ON po.vendor = org.id
LEFT JOIN folio_configuration.config_data__t cd ON cd.id = po.bill_to -- added 7/10
WHERE 
	jsonb_extract_path_text(pol.jsonb, 'acquisitionMethod') = '796596c4-62b5-4b64-a2ce-524c747afaa2' -- UUID for Approval Plan
	--AND po.bill_to = '9846770c-905b-496f-a2f7-85812769211b' -- UUID for JRL Acquisitions
	AND cd.value::json#>>'{name}' = 'JRL Acquisitions' -- added 7/13
	AND pol.creation_date::date >= start_date -- enter previous Friday's date
	AND pol.creation_date::date < end_date -- enter Friday report is being run - report will not include orders created that day 
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;
