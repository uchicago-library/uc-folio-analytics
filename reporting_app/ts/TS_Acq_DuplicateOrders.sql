--metadb:function TS_Acq_DuplicateOrders

DROP FUNCTION IF EXISTS TS_Acq_DuplicateOrders;

CREATE FUNCTION TS_Acq_DuplicateOrders(
    start_date date, --DEFAULT CURRENT_DATE - 1,
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
            po.created_date::date as date_ordered,
            po.po_workflow_status AS workflow_status,
            po.vendor_code as vendor_code,
            CASE 
                WHEN pol_c.po_line_list_unit_price_phys IS NOT NULL THEN pol_c.po_line_list_unit_price_phys
                ELSE pol_c.po_line_list_unit_price_elec
            END AS price,
            plt.order_format AS order_format, -- can't find in derived tables
            po.rush AS rush,
            po.title AS po_instance_title,
            po.publication_date AS po_publication_date,
            po.publisher AS po_publisher,
            po.pol_instance_id as instance_id,
            po.po_line_id as pol_id
        FROM folio_derived.po_instance po
        LEFT JOIN folio_derived.po_lines_cost pol_c ON pol_c.pol_id = po.po_line_id
        LEFT JOIN folio_orders.po_line__t plt ON plt.id = po.po_line_id
        WHERE 
			po.created_date::date >= start_date
			AND created_date::date < end_date
        ),
    new_identifiers as (
        SELECT
            ni.instance_id as instance_id,
            ni.instance_hrid AS instance_hrid,
            ni.identifier_type_name as identifier_type_name,
            ni.identifier as raw_identifier,
            CASE
            WHEN ni.identifier_type_name IN ('ISBN', 'Invalid ISBN') THEN
                NULLIF(regexp_replace(upper(trim(ni.identifier)), '( .*)|[^0-9X]', '', 'g'), '')
            ELSE
                regexp_replace(trim(ni.identifier), '^\(OCoLC\)(ocm|ocn|om|on)?', '')
            END AS identifier
        FROM orders oi
        LEFT JOIN folio_derived.instance_identifiers ni ON ni.instance_id = oi.instance_id
        WHERE ni.identifier_type_name IN ('ISBN', 'Invalid ISBN', 'OCLC', 'Cancelled system control number')
        ),
    donor_codes AS ( -- no derived table version?
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
            string_agg(po_f.fund_code, ' | ') AS funds,
            po_f.po_line_id AS pol_id
        FROM folio_derived.po_lines_fund_distribution_transactions po_f
        GROUP BY po_f.po_line_id
    )
    SELECT
        o.po_number AS po_number,
        it.instance_hrid as po_instance_hrid,
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
    LEFT JOIN folio_derived.instance_ext it on it.instance_id = o.instance_id
    LEFT JOIN donor_codes as dc on dc.pol_id = o.pol_id
    LEFT JOIN fund_codes as fc on fc.pol_id = o.pol_id
    LEFT JOIN new_identifiers noi on noi.instance_id = o.instance_id
    ),
identifiers AS ( -- all identifiers in FOLIO
	SELECT
		ii.instance_id as instance_id,
		ii.instance_hrid AS instance_hrid,
		ii.identifier_type_name as identifier_type_name,
		ii.identifier as raw_identifier,
		CASE
		WHEN ii.identifier_type_name IN ('ISBN', 'Invalid ISBN') THEN
			NULLIF(regexp_replace(upper(trim(ii.identifier)), '( .*)|[^0-9X]', '', 'g'), '')
		ELSE
			regexp_replace(trim(ii.identifier), '^\(OCoLC\)(ocm|ocn|om|on)?', '')
		END AS identifier
	FROM folio_derived.instance_identifiers ii
	WHERE ii.identifier_type_name IN ('ISBN', 'Invalid ISBN', 'OCLC', 'Cancelled system control number')
    ),
folio_matches AS (
	SELECT
		ord.po_number AS new_po_number,
		matched_ids.instance_id AS instance_id
	FROM new_orders ord 
	JOIN identifiers AS matched_ids ON (ord.order_identifier = matched_ids.identifier AND ord.po_instance_hrid != matched_ids.instance_hrid)
),   
contributors_agg AS ( --Step #5 Check for instances that match on the ISBN or the OCN in the instances attached to the PO.
	SELECT
		ic.instance_id AS instance_id,
		string_agg(ic.contributor_name, ' | ') AS contributors
	FROM folio_matches fmc
	LEFT JOIN folio_derived.instance_contributors ic ON ic.instance_id = fmc.instance_id
	GROUP BY ic.instance_id
    ),
pub_dates_agg AS (
	SELECT
		ip.instance_id AS instance_id,
		string_agg(ip.date_of_publication, ' | ') AS dates
	FROM folio_matches fmp
	LEFT JOIN folio_derived.instance_publication ip ON ip.instance_id = fmp.instance_id
	GROUP BY ip.instance_id
    ),
editions_agg AS (
	SELECT
		ie.instance_id AS instance_id,
		string_agg(ie.edition, ' | ') AS editions
	FROM folio_matches fme
	LEFT JOIN folio_derived.instance_editions ie ON ie.instance_id = fme.instance_id
	GROUP BY ie.instance_id
    ),
stat_code_agg AS (
	SELECT
		isc.instance_id AS instance_id,
		string_agg(isc.statistical_code, ' | ') AS stat_codes
	FROM folio_matches fmsc
	LEFT JOIN folio_derived.instance_statistical_codes isc ON isc.instance_id = fmsc.instance_id
	GROUP BY isc.instance_id
),
holdings_agg AS (
    SELECT
        hrt.instance_id AS instance_id,
        string_agg(hrt.type_name, ' | ') AS holdings_types,
        string_agg(DISTINCT hrt.permanent_location_name, ' | ') AS holdings_locs
    FROM folio_derived.holdings_ext hrt
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
    itm.instance_hrid as matched_instance_hrid,
    itm.title as matched_title,
    itm.discovery_suppress AS matched_suppressed,
    itm.status_name as matched_status,
    sca.stat_codes AS matched_stat_codes,
    cont.contributors as matched_contributors,
    pd.dates as matched_pub_dates,
    ed.editions as matched_editions,
    ha.holdings_types as matched_holdings_types,
    ha.holdings_locs as matched_holdings_locations
FROM new_orders ord 
JOIN folio_matches fm ON fm.new_po_number = ord.po_number
LEFT JOIN folio_derived.instance_ext itm ON itm.instance_id = fm.instance_id
LEFT JOIN contributors_agg AS cont ON fm.instance_id = cont.instance_id
LEFT JOIN pub_dates_agg AS pd ON fm.instance_id = pd.instance_id
LEFT JOIN editions_agg AS ed ON fm.instance_id = ed.instance_id
LEFT JOIN stat_code_agg sca ON fm.instance_id = sca.instance_id
LEFT JOIN holdings_agg AS ha ON fm.instance_id = ha.instance_id
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;