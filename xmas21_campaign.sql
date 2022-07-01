-- day 2
WITH base AS (
	SELECT 
		DATE_TRUNC('day', dtm.created_at + '8 hour'::INTERVAL)::DATE created_at_gmt8
		, dtm.signup_hostcountry 
		, dtm.ap_account_id 
		, um.email 
		, SUM(amount_usd) deposit_amount_usd
	FROM analytics.deposit_tickets_master dtm 
		LEFT JOIN analytics.users_master um 
			ON dtm.ap_account_id = um.ap_account_id 
	WHERE 
		dtm.created_at + '8 hour'::INTERVAL >= '2021-12-04'
		AND dtm.created_at + '8 hour'::INTERVAL < '2021-12-05'
		AND dtm.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
		AND dtm.signup_hostcountry IN ('TH','ID','AU','global')
	--	AND dtm.ap_account_id = 665505
	GROUP BY 1,2,3,4
)	, sgd_convert AS (
	SELECT 
		b.*
		, deposit_amount_usd * r.price deposit_amount_sgd
	FROM base b 
		LEFT JOIN analytics.rates_master r
			ON b.created_at_gmt8 = r.created_at 
			AND r.product_1_symbol = 'SGD'
)
SELECT 
	created_at_gmt8
	, signup_hostcountry 
	, ap_account_id 
	, email
	, CASE WHEN deposit_amount_sgd >= 99.99 THEN TRUE ELSE FALSE END AS is_eligible
	, deposit_amount_usd
	, deposit_amount_sgd
	, COUNT(DISTINCT ap_account_id) total_user_count
	, COUNT(DISTINCT CASE WHEN deposit_amount_sgd >= 99.99 THEN ap_account_id END) eligible_count
	, COUNT(DISTINCT CASE WHEN deposit_amount_sgd >= 99.99 THEN ap_account_id END) * 6.0609 airdrop_eligible_amount
FROM sgd_convert
WHERE signup_hostcountry = 'global'
GROUP BY 1,2,3,4,5,6,7
;



-- day 5
WITH base AS (
	SELECT 
		DATE_TRUNC('day', dtm.created_at + '8 hour'::INTERVAL)::DATE created_at_gmt8
		, dtm.signup_hostcountry 
		, dtm.ap_account_id 
		, um.email 
		, SUM(amount_usd) trade_amount_usd
	FROM analytics.trades_master dtm 
		LEFT JOIN analytics.users_master um 
			ON dtm.ap_account_id = um.ap_account_id 
	WHERE 
		dtm.created_at + '8 hour'::INTERVAL >= '2021-12-07'
		AND dtm.created_at + '8 hour'::INTERVAL < '2021-12-08'
		AND dtm.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
		AND dtm.signup_hostcountry IN ('TH','ID','AU','global')
	--	AND dtm.ap_account_id = 665505
	GROUP BY 1,2,3,4
)	, sgd_convert AS (
	SELECT 
		b.*
		, trade_amount_usd * r.price trade_amount_sgd
	FROM base b 
		LEFT JOIN analytics.rates_master r
			ON b.created_at_gmt8 = r.created_at 
			AND r.product_1_symbol = 'SGD'
)
SELECT 
	created_at_gmt8
	, signup_hostcountry 
	, ap_account_id 
	, email
	, CASE WHEN trade_amount_sgd >= 100.0 THEN TRUE ELSE FALSE END AS is_eligible
	, trade_amount_usd
	, trade_amount_sgd
	, COUNT(DISTINCT ap_account_id) total_user_count
	, COUNT(DISTINCT CASE WHEN trade_amount_sgd >= 100.0 THEN ap_account_id END) eligible_count
	, COUNT(DISTINCT CASE WHEN trade_amount_sgd >= 100.0 THEN ap_account_id END) * 6.0609 airdrop_eligible_amount
FROM sgd_convert
WHERE signup_hostcountry = 'global'
GROUP BY 1,2,3,4,5,6,7
;

