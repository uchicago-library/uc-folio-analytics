-- Query for Chicago Budgets

with encumbrance_summary as (
	select
		ftt.from_fund_id as fid,
		sum(ftt.amount) as e_encumbered
	from folio_finance.transaction__t AS ftt
		LEFT JOIN folio_finance.TRANSACTION AS fto ON fto.id = ftt.id
	where ftt.transaction_type = 'Encumbrance'
		AND ftt.fiscal_year_id = '123c7e89-3f86-4086-9964-68145f789c87'
		AND jsonb_extract_path_text(fto.jsonb,'encumbrance','orderStatus') = 'Open'
	GROUP BY ftt.from_fund_id
	),
	pp_summary as (
	select  	
		from_fund_id as fid2,
		sum(amount) as pp_amount
	from folio_finance.transaction__t
	where transaction_type = 'Pending payment'
		and fiscal_year_id = '123c7e89-3f86-4086-9964-68145f789c87'
	group by from_fund_id
	),
	p_summary as (
	select
		from_fund_id as fid3,
		sum(amount) as p_amount
	from folio_finance.transaction__t
	where transaction_type = 'Payment'
		and fiscal_year_id = '123c7e89-3f86-4086-9964-68145f789c87'
	group by from_fund_id
	),
	credit_summary as (
	select
		to_fund_id  as fid4,
		sum(amount) as cred_amount
	from folio_finance.transaction__t
	where transaction_type = 'Credit'
		and fiscal_year_id = '123c7e89-3f86-4086-9964-68145f789c87'
	group by to_fund_id
	)
select
	fb."name",
	fb.awaiting_payment as bs_awaiting_payment,
	coalesce(pps.pp_amount,0) as pending_payment,
	fb.encumbered as bs_encumbered,
	coalesce(es.e_encumbered,0) as enc_encumbered,
	fb.expenditures as bs_expended,
	coalesce(ps.p_amount,0)-coalesce(cs.cred_amount,0) AS trans_exp,
	fb.id as budget_id
from folio_finance.budget__t as fb
	left join encumbrance_summary AS es on es.fid = fb.fund_id
	left join pp_summary AS pps on pps.fid2 = fb.fund_id
	left join p_summary AS ps on ps.fid3 = fb.fund_id
	left join credit_summary AS cs on cs.fid4 = fb.fund_id
where fb.budget_status = 'Active'
	AND (fb.awaiting_payment <> coalesce(pps.pp_amount,0)
	OR fb.encumbered <> coalesce(es.e_encumbered,0)
	OR fb.expenditures <> (coalesce(ps.p_amount,0)-coalesce(cs.cred_amount,0)))
ORDER BY fb."name"
;

