-- metadb: budget_values_confirm_w_credits
--
-- To Do: need to confirm preferred schema for this function, currently creates
-- the function in the user's default schema.
--
DROP FUNCTION IF EXISTS budget_values_confirm_w_credits;

CREATE FUNCTION budget_values_confirm_w_credits (
    fy_code text
)
    RETURNS TABLE (
        name text,
        bs_awaiting_payment NUMERIC,
        pending_payment NUMERIC,
        bs_encumbered NUMERIC,
        enc_encumbered NUMERIC,
        bs_expended NUMERIC,
        trans_exp NUMERIC,
        bs_credits NUMERIC,
        budget_id uuid
)
AS $$
WITH
variables AS (
    SELECT
            (
            SELECT
                    fyt.id
            FROM
                    folio_finance.fiscal_year__t fyt
            WHERE
                    fyt.code = fy_code
        ) AS fy_id
),
encumbrance_summary AS (
    SELECT
        ftt.from_fund_id AS fid,
        sum(ftt.amount) AS e_encumbered
    FROM
        folio_finance.transaction__t AS ftt
    LEFT JOIN folio_finance.TRANSACTION AS fto ON
        fto.id = ftt.id
    WHERE
        ftt.transaction_type = 'Encumbrance'
        AND ftt.fiscal_year_id = (
            SELECT
                var.fy_id
            FROM
                variables AS var
        )
        AND jsonb_extract_path_text(fto.jsonb, 'encumbrance', 'orderStatus') = 'Open'
    GROUP BY
        ftt.from_fund_id
),
pp_summary AS (
    SELECT
        from_fund_id AS fid2,
        sum(amount) AS pp_amount
    FROM
        folio_finance.transaction__t AS ftt
    WHERE
        transaction_type = 'Pending payment'
        AND ftt.fiscal_year_id = (
            SELECT
                var.fy_id
            FROM
                variables AS var
        )
    GROUP BY
        from_fund_id
),
p_summary AS (
    SELECT
        from_fund_id AS fid3,
        sum(amount) AS p_amount
    FROM
        folio_finance.transaction__t AS ftt
    WHERE
        transaction_type = 'Payment'
        AND ftt.fiscal_year_id = (
            SELECT
                var.fy_id
            FROM
                variables AS var
        )
    GROUP BY
        from_fund_id
),
credit_summary AS (
    SELECT
        to_fund_id AS fid4,
        sum(amount) AS cred_amount
    FROM
        folio_finance.transaction__t AS ftt
    WHERE
        transaction_type = 'Credit'
        AND ftt.fiscal_year_id = (
            SELECT
                var.fy_id
            FROM
                variables AS var
        )
    GROUP BY
        to_fund_id
)
SELECT
    fb."name",
    fb.awaiting_payment AS bs_awaiting_payment,
    COALESCE(pps.pp_amount, 0) AS pending_payment,
    fb.encumbered AS bs_encumbered,
    COALESCE(es.e_encumbered, 0) AS enc_encumbered,
    fb.expenditures AS bs_expended,
    COALESCE(ps.p_amount, 0)-COALESCE(cs.cred_amount, 0) AS trans_exp,
    fb.credits AS bs_credits,
    fb.id AS budget_id
FROM
    folio_finance.budget__t AS fb
LEFT JOIN encumbrance_summary AS es ON
    es.fid = fb.fund_id
LEFT JOIN pp_summary AS pps ON
    pps.fid2 = fb.fund_id
LEFT JOIN p_summary AS ps ON
    ps.fid3 = fb.fund_id
LEFT JOIN credit_summary AS cs ON
    cs.fid4 = fb.fund_id
WHERE
    fb.budget_status = 'Active'
    AND (
        fb.awaiting_payment <> COALESCE(pps.pp_amount, 0)
            OR fb.encumbered <> COALESCE(es.e_encumbered, 0)
                OR fb.expenditures <> (
                    COALESCE(ps.p_amount, 0)-COALESCE(cs.cred_amount, 0)
                )
    )
ORDER BY
    fb."name"
$$
LANGUAGE SQL
stable parallel SAFE;
