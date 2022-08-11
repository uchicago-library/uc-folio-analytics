-- RapidILL report for UChicago

-- create table of aggregated holdings since can be more than one. 
drop table if exists local.rapidill_holdings_agg;

create table local.rapidill_holdings_agg as
        select
            he.instance_id as instance_id,
            string_agg( distinct he.type_name, ',') as holdings_types,
            string_agg(distinct he.permanent_location_name, ',') as holdings_locs,
            string_agg(distinct he.call_number, ',') as holdings_callno
        from folio_reporting.holdings_ext as he
        where he.discovery_suppress != true
        	and he.type_name = 'Physical'
        group by he.instance_id;

create index on local.rapidill_holdings_agg (instance_id);

vacuum analyze local.rapidill_holdings_agg;

-- create table of aggregated ISBNs since can be more than one. 
drop table if exists local.rapidill_isbns;

create table local.rapidill_isbns as
    select
		iii.instance_id as instance_id,
		string_agg(iii.identifier, ', ') as identifiers
	from folio_reporting.instance_identifiers as iii
	where iii.identifier_type_name = 'ISBN'
	group by iii.instance_id;

create index on local.rapidill_isbns (instance_id);

vacuum analyze local.rapidill_isbns;

-- create table of aggregated ISSNs since can be more than one. 
drop table if exists local.rapidill_issns;

create table local.rapidill_issns as
    select
		iis.instance_id as instance_id,
		string_agg(iis.identifier, ', ') as identifiers
	from folio_reporting.instance_identifiers as iis
	where iis.identifier_type_name = 'ISSN'
	group by iis.instance_id;

create index on local.rapidill_issns (instance_id);

vacuum analyze local.rapidill_issns;

-- create table of aggregated OCLC numbers since can be more than one.
drop table if exists local.rapidill_ocns;

create table local.rapidill_ocns as
    select
		iio.instance_id as instance_id,
		string_agg(iio.identifier, ', ') as identifiers
	from folio_reporting.instance_identifiers as iio
	where iio.identifier_type_name = 'OCLC'
		and iio.identifier not like '(OCoLC)o%'
	group by iio.instance_id;

create index on local.rapidill_ocns (instance_id);

vacuum analyze local.rapidill_ocns;

-- Create table of aggregated pub dates since can be more than one.
drop table if exists local.rapidill_pubdates;

create table local.rapidill_pubdates as
   select
		ip.instance_id as instance_id,
		string_agg(ip.date_of_publication, ', ') as dates
	from folio_reporting.instance_publication as ip 
	group by ip.instance_id;

create index on local.rapidill_pubdates (instance_id);

vacuum analyze local.rapidill_pubdates;

-- create table of aggregated holdings statements
drop table if exists local.rapidill_holstatements;

create table local.rapidill_holstatements as
   select
		hs.holdings_id as holdings_id,
		hs.statement as hol_statements
	from folio_reporting.holdings_statements hs
	union 
	select
		hsi.holdings_id as holdings_id,
		hsi.statement as hol_statements
	from folio_reporting.holdings_statements_indexes hsi
	union 
		select
		hss.holdings_id as holdings_id,
		hss.statement as hol_statements
	from folio_reporting.holdings_statements_supplements hss;

create index on local.rapidill_holstatements (holdings_id);

vacuum analyze local.rapidill_holstatements;

-- create eoo agg table
drop table if exists local.rapidill_eoo_agg;

create table local.rapidill_eoo_agg as
   select
   		hols.holdings_id as holdings_id,
   		string_agg(hols.hol_statements, ',') as holdings
   from local.rapidill_holstatements hols
   group by holdings_id;

create index on local.rapidill_eoo_agg (holdings_id);

vacuum analyze local.rapidill_eoo_agg;

-- RapidILL Monographs Report.
-- ISBNs, OCNs, Title, Locations (where not on Order), Call numbers, pub year 
-- Not discovery suppress; moi not b, s; 
drop table if exists local.rapidill_printmonos;

create table local.rapidill_printmonos as 
select 
	isbns.identifiers as ISBNs,
	ocns.identifiers as OCLC_numbers,
	ie.title as Title,
	hol.holdings_locs as Locations, 
	hol.holdings_callno as Call_Numbers,
	pd.dates as Pub_dates
from folio_reporting.instance_ext ie 
left join local.rapidill_isbns as isbns on ie.instance_id = isbns.instance_id
left join local.rapidill_ocns as ocns on ie.instance_id = ocns.instance_id
left join local.rapidill_pubdates pd on ie.instance_id = pd.instance_id
left join local.rapidill_holdings_agg hol on ie.instance_id = hol.instance_id
where 
	ie.discovery_suppress != TRUE
--	and ie.staff_suppress != True
	and ie.mode_of_issuance_name != 'serial'
	and ie.status_name not in ('Circulation', 
								'DDA discovery', 
								'Electronic resource', 
								'Eresource temporary', 
								'Fast add',
								'Monograph classed separately',
								'OCLC collection manager',
								'Uncataloged',
								'',
								' ')
	and ie.status_name is not null 
	and hol.holdings_types like '%Physical%'
;

-- create electronic monographs report. 
drop table if exists local.rapidill_elecmonos;

create table local.rapidill_elecmonos as 
select 
	isbns.identifiers as ISBNs,
	ocns.identifiers as OCLC_numbers,
	ie.title as Title,
	hea2.link_text as platform,
	hea2.uri as url,
	pd.dates as Pub_dates
from folio_reporting.instance_ext ie 
left join local.rapidill_isbns as isbns on ie.instance_id = isbns.instance_id
left join local.rapidill_ocns as ocns on ie.instance_id = ocns.instance_id
left join local.rapidill_pubdates pd on ie.instance_id = pd.instance_id
left join folio_reporting.holdings_ext hee on ie.instance_id = hee.instance_id
left join folio_reporting.holdings_electronic_access hea2 on hee.holdings_id = hea2.holdings_id
where 
	ie.discovery_suppress != true 
--	and ie.staff_suppress != True
	and ie.mode_of_issuance_name != 'serial'
	and ie.status_name not in ('Circulation', 
								'DDA discovery',
								'Fast add',
								'Monograph classed separately',
								'Uncataloged',
								'',
								' ')
	and ie.status_name is not null 
	and hee.type_name = 'Electronic'
	and hee.discovery_suppress != true;

-- create physical serials report
drop table if exists local.rapidill_printserials;

create table local.rapidill_printserials as 
select 
	issns.identifiers as ISSNs,
	ocns.identifiers as OCLC_numbers,
	ie.title as Title,
	hee.permanent_location_name as location, 
	hee.call_number as call_number,
	eoo.holdings as holdings
from folio_reporting.instance_ext ie 
left join local.rapidill_issns as issns on ie.instance_id = issns.instance_id
left join local.rapidill_ocns as ocns on ie.instance_id = ocns.instance_id
left join folio_reporting.holdings_ext hee on ie.instance_id = hee.instance_id
left join local.rapidill_eoo_agg eoo on hee.holdings_id = eoo.holdings_id
where 
	ie.discovery_suppress != True
--	and ie.staff_suppress != True
	and ie.mode_of_issuance_name = 'serial'
	and ie.status_name not in ('Circulation', 
								'DDA discovery', 
								'Electronic resource', 
								'Eresource temporary', 
								'Fast add',
								'Monograph classed separately',
								'OCLC collection manager',
								'Uncataloged',
								'',
								' ')
	and ie.status_name is not null 
	and hee.type_name = 'Physical'
	and hee.discovery_suppress != true
;

select * from local.rapidill_printserials where title like '%Astrophysical%';
