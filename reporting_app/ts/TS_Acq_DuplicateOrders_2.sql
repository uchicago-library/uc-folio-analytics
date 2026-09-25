--metadb:function TS_Acq_DuplicateOrders

DROP FUNCTION IF EXISTS TS_Acq_DuplicateOrders;

CREATE FUNCTION TS_Acq_DuplicateOrders(
    start_date date --DEFAULT CURRENT_DATE - 1,
    end_date date --DEFAULT CURRENT_DATE
)
RETURNS TABLE ( -- dup orders results mapping
    po_number text,
    po_instance_hrid text,
    vendor_code text, 
    po_date_ordered date,
    workflow_status text, 
    fund text,
    price text,
    order_format text,
    rush text,
    po_instance_title text,
    po_publication_date text, 
    po_publisher text, 
    po_identifier_type text,
    po_raw_identifier text,
    po_identifier text,
    matched_instance_hrid text,
    matched_title text,
    matched_suppressed bool,
    matched_status text,
    matched_stat_code text,
    matched_contributors text,
    matched_pub_dates text,
    matched_editions text,
    matched_holdings_types text,
    matched_holdings_locations text
)
AS
$$
WITH
new_orders as (
    WITH 
    orders as (
        SELECT
            po.po_number AS po_number,
            po.date_ordered::date as date_ordered,
            po.workflow_status AS workflow_status,
            org.code as vendor_code,
            CASE 
                WHEN jsonb_extract_path_text(pol.jsonb, 'cost', 'listUnitPrice') IS NOT NULL THEN jsonb_extract_path_text(pol.jsonb, 'cost', 'listUnitPrice')
                ELSE jsonb_extract_path_text(pol.jsonb, 'cost', 'listUnitPriceElectronic')
            END AS price,
            plt.order_format AS order_format,
            plt.rush AS rush,
            plt.title_or_package AS po_instance_title,
            plt.publication_date AS po_publication_date,
            plt.publisher AS po_publisher,
            plt.instance_id as instance_id,
            plt.id as pol_id
        FROM folio_orders.purchase_order__t po
        LEFT JOIN folio_orders.po_line as pol on pol.purchaseorderid = po.id
        LEFT JOIN folio_orders.po_line__t as plt on plt.po_line_number = jsonb_extract_path_text(pol.jsonb, 'poLineNumber')
        LEFT JOIN folio_organizations.organizations__t org on po.vendor = org.id
        WHERE 
            po.date_ordered::date >= start_date
            AND po.date_ordered::date < end_date
        ),
    new_identifiers as (
        SELECT
            ino.id as instance_id,
            it.hrid AS instance_hrid,
            itt.name as identifier_type_name,
            identifiers.jsonb #>> '{value}' as raw_identifier,
            CASE
            WHEN itt.name IN ('ISBN', 'Invalid ISBN') THEN
                NULLIF(regexp_replace(upper(trim(identifiers.jsonb #>> '{value}')), '( .*)|[^0-9X]', '', 'g'), '')
            ELSE
                regexp_replace(trim(identifiers.jsonb #>> '{value}'), '^\(OCoLC\)(ocm|ocn|om|on)?', '')
            END AS identifier
        FROM orders oi
        LEFT JOIN folio_inventory.instance ino ON ino.id = oi.instance_id
            CROSS JOIN LATERAL jsonb_array_elements(jsonb_extract_path(ino.jsonb, 'identifiers')) as identifiers (jsonb)
        LEFT JOIN folio_inventory.identifier_type__t itt on (identifiers.jsonb #>> '{identifierTypeId}')::uuid = itt.id
        LEFT JOIN folio_inventory.instance__t it ON it.id = ino.id
        WHERE itt.name IN ('ISBN', 'Invalid ISBN', 'OCLC', 'Cancelled system control number') 
        ),
    donor_codes AS ( 
        SELECT 
            string_agg(ot.code, ' | ') as donor_codes,
            pol2.id as pol_id
        FROM folio_orders.po_line pol2
        CROSS JOIN LATERAL jsonb_array_elements(jsonb_extract_path(jsonb, 'donorOrganizationIds'))
               AS donor_uuids (jsonb)
        LEFT JOIN folio_organizations.organizations__t ot on ot.id = (donor_uuids.jsonb #>> '{}')::uuid
        GROUP BY pol2.id
        ),
    fund_codes AS (
        SELECT
            string_agg((fundDis.jsonb #>> '{code}'), ' | ') AS funds,
            pol3.id AS pol_id
        FROM folio_orders.po_line pol3 
            CROSS JOIN LATERAL jsonb_array_elements(jsonb_extract_path(jsonb, 'fundDistribution')) AS fundDis (jsonb)
        GROUP BY pol3.id
    )
    SELECT
        o.po_number AS po_number,
        it.hrid as po_instance_hrid,
        o.vendor_code as vendor_code,
        o.date_ordered::date as date_ordered,
        o.workflow_status AS workflow_status,
        fc.funds as fund,
        o.price as list_price,
        o.order_format AS order_format,
        o.rush AS rush,
        o.po_instance_title AS po_instance_title,
        o.po_publication_date AS po_publication_date,
        o.po_publisher AS po_publisher, 
        noi.identifier_type_name AS identifier_type_name,
        noi.raw_identifier AS raw_identifier,
        noi.identifier AS order_identifier
    FROM orders o
    LEFT JOIN folio_inventory.instance__t it on it.id = o.instance_id
    LEFT JOIN donor_codes as dc on dc.pol_id = o.pol_id
    LEFT JOIN fund_codes as fc on fc.pol_id = o.pol_id
    LEFT JOIN new_identifiers noi on noi.instance_id = o.instance_id
    ),
identifiers AS ( -- all identifiers in FOLIO
    SELECT
        i.id as instance_id,
        it.hrid AS instance_hrid,
        itt.name as identifier_type_name,
        identifiers.jsonb #>> '{value}' as raw_identifier,
    CASE
        WHEN itt.name IN ('ISBN', 'Invalid ISBN') THEN
            NULLIF(regexp_replace(upper(trim(identifiers.jsonb #>> '{value}')), '( .*)|[^0-9X]', '', 'g'), '')
        ELSE
            regexp_replace(trim(identifiers.jsonb #>> '{value}'), '^\(OCoLC\)(ocm|ocn|om|on)?', '')
        END AS identifier
    FROM folio_inventory.instance i
        CROSS JOIN LATERAL jsonb_array_elements(jsonb_extract_path(jsonb, 'identifiers')) as identifiers (jsonb)
    LEFT JOIN folio_inventory.identifier_type__t itt on (identifiers.jsonb #>> '{identifierTypeId}')::uuid = itt.id
        LEFT JOIN folio_inventory.instance__t it ON it.id = i.id
    WHERE itt.name IN ('ISBN', 'Invalid ISBN', 'OCLC', 'Cancelled system control number') 
    ),
folio_matches AS (
	SELECT DISTINCT ON (matched_ids.instance_id)
		ord.po_number AS new_po_number,
		matched_ids.instance_id AS instance_id
	FROM new_orders ord 
	JOIN identifiers AS matched_ids ON (ord.order_identifier = matched_ids.identifier AND ord.po_instance_hrid != matched_ids.instance_hrid)
),   
contributors_agg AS ( --Step #5 Check for instances that match on the ISBN or the OCN in the instances attached to the PO.
	SELECT
		ic.id AS instance_id,
		string_agg((contributors.jsonb #>> '{name}'), ' | ') AS contributors
	FROM folio_matches fmc
	LEFT JOIN folio_inventory.INSTANCE ic ON ic.id = fmc.instance_id
		CROSS JOIN LATERAL jsonb_array_elements(jsonb_extract_path(ic.jsonb, 'contributors')) AS contributors (jsonb)
	GROUP BY ic.id
    ),
pub_dates_agg AS (
	SELECT
		ip.id AS instance_id,
		string_agg((pub_info.jsonb #>> '{dateOfPublication}'), ' | ') AS dates
	FROM folio_matches fmp
	LEFT JOIN folio_inventory.INSTANCE ip ON ip.id = fmp.instance_id
		CROSS JOIN LATERAL jsonb_array_elements(jsonb_extract_path(ip.jsonb, 'publication')) AS pub_info (jsonb)
	GROUP BY ip.id
    ),
editions_agg AS (
	SELECT
		ie.id AS instance_id,
		string_agg((ed.jsonb #>> '{}'), ' | ') AS editions
	FROM folio_matches fme
	LEFT JOIN folio_inventory.INSTANCE ie ON ie.id = fme.instance_id
		CROSS JOIN LATERAL jsonb_array_elements(jsonb_extract_path(ie.jsonb, 'editions')) AS ed (jsonb)
	GROUP BY ie.id
    ),
stat_code_agg AS (
	SELECT
		isc.id AS instance_id,
		string_agg(isct.code, ' | ') AS stat_codes
	FROM folio_matches fmsc
	LEFT JOIN folio_inventory.INSTANCE isc ON isc.id = fmsc.instance_id
    	CROSS JOIN LATERAL jsonb_array_elements(jsonb_extract_path(isc.jsonb, 'statisticalCodeIds')) AS stat_codes (jsonb)
	LEFT JOIN folio_inventory.statistical_code__t isct ON isct.id = (stat_codes.jsonb #>> '{}')::uuid
	GROUP BY isc.id
),
holdings_agg AS (
    SELECT
        hrt.instance_id AS instance_id,
        string_agg(htt.name, ',') AS holdings_types,
        string_agg(DISTINCT lt.name, ' | ') AS holdings_locs
    FROM folio_inventory.holdings_record__t hrt
    LEFT JOIN folio_inventory.holdings_type__t htt ON hrt.holdings_type_id = htt.id
    LEFT JOIN folio_inventory.location__t lt ON hrt.permanent_location_id = lt.id 
    GROUP BY hrt.instance_id
    )
SELECT DISTINCT ON (ord.po_instance_hrid, fm.instance_id)
    ord.po_number AS po_number,
    ord.po_instance_hrid AS po_instance_hrid,
    ord.vendor_code AS vendor_code, 
    ord.date_ordered AS po_date_ordered,
    ord.workflow_status AS workflow_status, 
    ord.fund AS fund,
    ord.list_price AS price,
    ord.order_format AS order_format,
    ord.rush AS rush,
    ord.po_instance_title AS po_instance_title,
    ord.po_publication_date AS po_publication_date, 
    ord.po_publisher AS po_publisher, 
    ord.identifier_type_name AS po_identifier_type,
    ord.raw_identifier AS po_raw_identifier,
    ord.order_identifier AS po_identifier,
    iton.hrid as matched_instance_hrid,
    iton.title as matched_title,
    iton.discovery_suppress AS matched_suppressed,
    ist.name as matched_status,
    sca.stat_codes AS matched_stat_codes,
    cont.contributors as matched_contributors,
    pd.dates as matched_pub_dates,
    ed.editions as matched_editions,
    ha.holdings_types as matched_holdings_types,
    ha.holdings_locs as matched_holdings_locations
FROM new_orders ord 
JOIN folio_matches fm ON fm.new_po_number = ord.po_number
LEFT JOIN folio_inventory.instance__t iton ON iton.id = fm.instance_id
LEFT JOIN folio_inventory.instance_status__t ist ON ist.id = iton.status_id
LEFT JOIN contributors_agg AS cont ON fm.instance_id = cont.instance_id
LEFT JOIN pub_dates_agg AS pd ON fm.instance_id = pd.instance_id
LEFT JOIN editions_agg AS ed ON fm.instance_id = ed.instance_id
LEFT JOIN stat_code_agg sca ON fm.instance_id = sca.instance_id
LEFT JOIN holdings_agg AS ha ON fm.instance_id = ha.instance_id
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;