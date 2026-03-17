DROP TABLE IF EXISTS access_services.barcodes_with_effective_locations ; 
CREATE TABLE access_services.barcodes_with_effective_locations AS
SELECT
    CAST(folio_inventory.item__t.barcode AS varchar),
    CAST(folio_inventory.location__t.name AS varchar),
    folio_inventory.item__t.id
FROM
    folio_inventory.item__t
INNER JOIN folio_inventory.location__t ON
    folio_inventory.item__t.effective_location_id = folio_inventory.location__t.id;
CREATE INDEX ON access_services.effective_locations (barcode, id) ; 

GRANT SELECT, INSERT, UPDATE, DELETE ON access_services.effective_locations TO access_services_role


DROP TABLE IF EXISTS access_services.user_customfields;
CREATE TABLE access_services.user_customfields AS
SELECT
    id AS uuid,
    CAST(jsonb_extract_path_text(folio_users.users.jsonb, 'customFields', 'source') AS varchar) AS sourcer,
    CAST(jsonb_extract_path_text(folio_users.users.jsonb, 'customFields', 'category') AS varchar) AS category,
    CAST(jsonb_extract_path_text(folio_users.users.jsonb, 'customFields', 'status') AS varchar) AS status,
    CAST(jsonb_extract_path_text(folio_users.users.jsonb, 'customFields', 'statuses') AS varchar) AS statuses,
    CAST(jsonb_extract_path_text(folio_users.users.jsonb, 'customFields', 'staffStatus') AS varchar) AS staffStatus,
    CAST(jsonb_extract_path_text(folio_users.users.jsonb, 'customFields', 'staffDivision') AS varchar) AS staffDivision,
    CAST(jsonb_extract_path_text(folio_users.users.jsonb, 'customFields', 'staffDepartment') AS varchar) AS staffDepartment,
    CAST(jsonb_extract_path_text(folio_users.users.jsonb, 'customFields', 'staffPrivileges') AS varchar) AS staffPrivileges,
    CAST(jsonb_extract_path_text(folio_users.users.jsonb, 'customFields', 'studentId') AS varchar) AS studentid,
    CAST(jsonb_extract_path_text(folio_users.users.jsonb, 'customFields', 'studentStatus') AS varchar) AS studentStatus,
    CAST(jsonb_extract_path_text(folio_users.users.jsonb, 'customFields', 'studentDivision') AS varchar) AS studentDivision,
    CAST(jsonb_extract_path_text(folio_users.users.jsonb, 'customFields', 'studentDepartment') AS varchar) AS studentDepartment,
    CAST(jsonb_extract_path_text(folio_users.users.jsonb, 'customFields', 'deceased') AS varchar) AS deceased,
    CAST(jsonb_extract_path_text(folio_users.users.jsonb, 'customFields', 'collections') AS varchar) AS collections,
    CAST(jsonb->'personal'->>'lastName' AS varchar) AS last_name,
    CAST(jsonb->'personal'->>'firstName' AS varchar) AS first_name,
    CAST(jsonb->'personal'->>'preferredFirstName' AS varchar) AS preferred_first_name,
    CAST(jsonb->'personal'->>'email' AS varchar) AS email
FROM
    folio_users.users ;

GRANT SELECT, INSERT, UPDATE, DELETE ON access_services.user_customfields TO access_services_role


DROP TABLE IF EXISTS access_services.barcodes_with_circstatus ; 
CREATE TABLE access_services.barcodes_with_circstatus AS
SELECT
    folio_derived.item_ext.item_ID,
    CAST(folio_derived.item_ext.barcode AS varchar),
    CAST(folio_derived.item_ext.chronology AS varchar),
    CAST(folio_derived.item_ext.copy_number AS varchar),
    CAST(folio_derived.item_ext.enumeration AS varchar),
    CAST(folio_derived.item_ext.volume AS varchar),
    CAST(folio_derived.item_ext.effective_call_number AS varchar),
    CAST(folio_derived.item_ext.effective_call_number_suffix AS varchar),
    CAST(folio_derived.item_ext.effective_location_name AS varchar),
    CAST(folio_derived.item_ext.status_date AS varchar),
    CAST(folio_derived.item_ext.status_name AS varchar)
FROM
    folio_derived.item_ext;
CREATE INDEX ON access_services.barcodes_with_circstatus (barcode) ;

GRANT SELECT, INSERT, UPDATE, DELETE ON access_services.barcodes_with_circstatus TO access_services_role

--
-- Duplicate of folio_derived.items_ext friendly to MS Access
--

DROP TABLE IF EXISTS access_services.item_ext_uc;
CREATE TABLE access_services.item_ext_uc AS
SELECT
    folio_derived.item_ext.item_id,
    CAST(folio_derived.item_ext.item_hrid AS varchar(255)),
    CAST(folio_derived.item_ext.accession_number AS varchar(255)),
    CAST(folio_derived.item_ext.barcode AS varchar(255)),
    CAST(folio_derived.item_ext.chronology AS varchar(255)),
    CAST(folio_derived.item_ext.copy_number AS varchar(255)),
    CAST(folio_derived.item_ext.enumeration AS varchar(255)),
    CAST(folio_derived.item_ext.volume AS varchar(255)),
    folio_derived.item_ext.in_transit_destination_service_point_id,
    CAST(folio_derived.item_ext.in_transit_destination_service_point_name AS varchar(255)),
    CAST(folio_derived.item_ext.identifier AS varchar(255)),
    CAST(folio_derived.item_ext.call_number AS varchar(255)),
    folio_derived.item_ext.call_number_type_id,
    CAST(folio_derived.item_ext.call_number_type_name AS varchar(255)),
    CAST(folio_derived.item_ext.effective_call_number_prefix AS varchar(255)),
    CAST(folio_derived.item_ext.effective_call_number AS varchar(255)),
    CAST(folio_derived.item_ext.effective_call_number_suffix AS varchar(255)),
    folio_derived.item_ext.effective_call_number_type_id,
    CAST(folio_derived.item_ext.effective_call_number_type_name AS varchar(255)),
    folio_derived.item_ext.damaged_status_id,
    CAST(folio_derived.item_ext.damaged_status_name AS varchar(255)),
    folio_derived.item_ext.material_type_id,
    CAST(folio_derived.item_ext.material_type_name AS varchar(255)),
    CAST(folio_derived.item_ext.number_of_pieces AS varchar(255)),
    CAST(folio_derived.item_ext.number_of_missing_pieces AS varchar(255)),
    folio_derived.item_ext.permanent_loan_type_id,
    CAST(folio_derived.item_ext.permanent_loan_type_name AS varchar(255)),
    folio_derived.item_ext.temporary_loan_type_id,
    CAST(folio_derived.item_ext.temporary_loan_type_name AS varchar(255)),
    folio_derived.item_ext.permanent_location_id,
    CAST(folio_derived.item_ext.permanent_location_name AS varchar(255)),
    folio_derived.item_ext.temporary_location_id,
    CAST(folio_derived.item_ext.temporary_location_name AS varchar(255)),
    folio_derived.item_ext.effective_location_id,
    CAST(folio_derived.item_ext.effective_location_name AS varchar(255)),
    CAST(folio_derived.item_ext.description_of_pieces AS varchar(255)),
    CAST(folio_derived.item_ext.status_date AS varchar(255)),
    CAST(folio_derived.item_ext.status_name AS varchar(255)),
    folio_derived.item_ext.holdings_record_id,
    folio_derived.item_ext.discovery_suppress,
    folio_derived.item_ext.created_date,
    folio_derived.item_ext.updated_by_user_id,
    CAST(folio_derived.item_ext.updated_date AS varchar(255))
FROM
    folio_derived.item_ext;
CREATE INDEX ON access_services.item_ext_uc (item_id);
CREATE INDEX ON access_services.item_ext_uc (item_hrid);
CREATE INDEX ON access_services.item_ext_uc (barcode);

GRANT SELECT, INSERT, UPDATE, DELETE ON access_services.item_ext_uc TO access_services_role

