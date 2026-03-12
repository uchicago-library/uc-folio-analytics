DROP TABLE IF EXISTS access_services.barcodes_with_effective_locations ; 
create table access_services.barcodes_with_effective_locations as
select
    cast(folio_inventory.item__t.barcode as varchar),
    cast(folio_inventory.location__t.name as varchar),
    folio_inventory.item__t.id
from
    folio_inventory.item__t
inner join folio_inventory.location__t on
    folio_inventory.item__t.effective_location_id = folio_inventory.location__t.id;
CREATE INDEX ON access_services.effective_locations (barcode, id) ; 

DROP TABLE IF EXISTS access_services.user_customfields;
create table access_services.user_customfields as
select
    id as uuid,
    cast(jsonb_extract_path_text(folio_users.users.jsonb, 'customFields', 'source') as varchar) as sourcer,
    cast(jsonb_extract_path_text(folio_users.users.jsonb, 'customFields', 'category') as varchar) as category,
    cast(jsonb_extract_path_text(folio_users.users.jsonb, 'customFields', 'status') as varchar) as status,
    cast(jsonb_extract_path_text(folio_users.users.jsonb, 'customFields', 'statuses') as varchar) as statuses,
    cast(jsonb_extract_path_text(folio_users.users.jsonb, 'customFields', 'staffStatus') as varchar) as staffStatus,
    cast(jsonb_extract_path_text(folio_users.users.jsonb, 'customFields', 'staffDivision') as varchar) as staffDivision,
    cast(jsonb_extract_path_text(folio_users.users.jsonb, 'customFields', 'staffDepartment') as varchar) as staffDepartment,
    cast(jsonb_extract_path_text(folio_users.users.jsonb, 'customFields', 'staffPrivileges') as varchar) as staffPrivileges,
    cast(jsonb_extract_path_text(folio_users.users.jsonb, 'customFields', 'studentId') as varchar) as studentid,
    cast(jsonb_extract_path_text(folio_users.users.jsonb, 'customFields', 'studentStatus') as varchar) as studentStatus,
    cast(jsonb_extract_path_text(folio_users.users.jsonb, 'customFields', 'studentDivision') as varchar) as studentDivision,
    cast(jsonb_extract_path_text(folio_users.users.jsonb, 'customFields', 'studentDepartment') as varchar) as studentDepartment,
    cast(jsonb_extract_path_text(folio_users.users.jsonb, 'customFields', 'deceased') as varchar) as deceased,
    cast(jsonb_extract_path_text(folio_users.users.jsonb, 'customFields', 'collections') as varchar) as collections,
    cast(jsonb->'personal'->>'lastName' as varchar) as last_name,
    cast(jsonb->'personal'->>'firstName' as varchar) as first_name,
    cast(jsonb->'personal'->>'preferredFirstName' as varchar) as preferred_first_name,
    cast(jsonb->'personal'->>'email' as varchar) as email
from
    folio_users.users ;


DROP TABLE IF EXISTS access_services.barcodes_with_circstatus ; 
create table access_services.barcodes_with_circstatus as
select
    folio_derived.item_ext.item_ID,
    cast(folio_derived.item_ext.barcode as varchar),
    cast(folio_derived.item_ext.chronology as varchar),
    cast(folio_derived.item_ext.copy_number as varchar),
    cast(folio_derived.item_ext.enumeration as varchar),
    cast(folio_derived.item_ext.volume as varchar),
    cast(folio_derived.item_ext.effective_call_number as varchar),
    cast(folio_derived.item_ext.effective_call_number_suffix as varchar),
    cast(folio_derived.item_ext.effective_location_name as varchar),
    cast(folio_derived.item_ext.status_date as varchar),
    cast(folio_derived.item_ext.status_name as varchar)
from
    folio_derived.item_ext;
CREATE INDEX ON access_services.barcodes_with_circstatus (barcode) ;
