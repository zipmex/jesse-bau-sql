--- deposit + withdrawl 
WITH deposit_ AS ( 
	SELECT 
		date_trunc('day', d.updated_at) AS month_  
		, d.ap_account_id 
		, d.signup_hostcountry 
		, d.product_type 
		, d.product_symbol 
		, COUNT(d.*) AS deposit_number 
		, SUM(d.amount) AS deposit_amount 
		, SUM(d.amount_usd) deposit_usd
	FROM 
		analytics.deposit_tickets_master d 
	WHERE 
		d.status = 'FullyProcessed' 
		AND d.signup_hostcountry IN ('TH','AU','ID','global')
	--	AND d.updated_at::date >= '2021-01-01' AND d.updated_at::date < NOW()::date 
		AND d.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping) 
	GROUP  BY 
		1,2,3,4,5
), withdraw_ AS (
		SELECT 
			date_trunc('day', w.updated_at) AS month_  
			, w.ap_account_id 
			, w.signup_hostcountry 
			, w.product_type 
			, w.product_symbol 
			, COUNT(w.*) AS withdraw_number 
			, SUM(w.amount) AS withdraw_amount 
			, SUM(w.amount_usd) withdraw_usd
		FROM  
			analytics.withdraw_tickets_master w 
		WHERE 
			w.status = 'FullyProcessed'
			AND w.signup_hostcountry IN ('TH','AU','ID','global')
		--	AND w.updated_at::date >= '2021-01-01' AND w.updated_at::date < NOW()::date 
			AND w.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
		GROUP BY 
			1,2,3,4,5
)	--, active_user AS (
SELECT 
	DATE_TRUNC('year', COALESCE(d.month_, w.month_))::DATE created_at  
--	, COALESCE(d.signup_hostcountry, w.signup_hostcountry) signup_hostcountry
--	, COALESCE (d.ap_account_id, w.ap_account_id) ap_account_id 
--	, COALESCE (d.product_type, w.product_type) product_type 
	, COALESCE (d.product_symbol, w.product_symbol) symbol 
--	, SUM( COALESCE(d.deposit_number, 0)) depost_count 
	, SUM( deposit_amount) deposit_amount
--	, SUM( COALESCE(d.deposit_usd, 0)) deposit_usd
--	, SUM( COALESCE(w.withdraw_number, 0)) withdraw_count
	, SUM( withdraw_amount) withdraw_amount
--	, SUM( COALESCE(w.withdraw_usd, 0)) withdraw_usd
FROM 
	deposit_ d 
	FULL OUTER JOIN 
		withdraw_ w 
		ON d.ap_account_id = w.ap_account_id 
		AND d.signup_hostcountry = w.signup_hostcountry 
		AND d.product_type = w.product_type 
		AND d.month_ = w.month_ 
		AND d.product_symbol = w.product_symbol 
WHERE 
	COALESCE(d.month_, w.month_) >= '2021-01-01'
	AND COALESCE(d.month_, w.month_) < DATE_TRUNC('year', NOW())
--	AND COALESCE (d.product_type, w.product_type) = 'NationalCurrency'
GROUP BY 
	1,2
ORDER BY 
	1,2 
;




-- avg deposit 
WITH base_deposit AS (
	SELECT 
		DATE_TRUNC('month', d.created_at) created_at 
		, d.ap_account_id 
		, d.signup_hostcountry 
		, SUM(d.amount_usd) deposit_usd
	FROM 
		analytics.deposit_tickets_master d 
		RIGHT JOIN mappings.commercial_pcs_id_account_id cp
			ON d.ap_account_id = cp.ap_account_id::INT 
	WHERE 
		d.status = 'FullyProcessed' 
		AND d.signup_hostcountry IN ('TH','AU','ID','global')
		AND DATE_TRUNC('day', d.created_at) >= '2021-01-01'
		AND DATE_TRUNC('day', d.created_at) < '2022-01-01'
		AND d.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping) 
	GROUP  BY 
		1,2,3
)
SELECT 
	created_at::DATE 
	, signup_hostcountry 
	, COUNT(DISTINCT ap_account_id) depositor_count
	, SUM(deposit_usd) deposit_usd
	, SUM(deposit_usd) / COUNT(DISTINCT ap_account_id) avg_deposit_usd
FROM base_deposit bd
GROUP BY 1,2
;



WITH base AS (
SELECT 
	tick_to_timestamp(updated_on_ticks)::DATE 
	, SUM(fee_amount::NUMERIC) fee_amount 
FROM 
	apex.deposit_tickets dt  
WHERE 
	dt.status IN (5)
	AND dt.asset_id = 12
	AND deposit_info IS NOT NULL 
GROUP BY 1
ORDER BY 1 DESC
)
SELECT 
*
FROM base 
WHERE fee_amount > 0


SELECT *
FROM analytics.fees_master fm 
WHERE fee_type = 'Withdraw'