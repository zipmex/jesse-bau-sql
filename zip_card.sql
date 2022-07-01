-- average spending per active card user
SELECT
	c.gps_transaction_date AS created_at
	, c.crypto_currency 
	, c.billing_currency 
	, COUNT(DISTINCT transaction_id) AS transactions_count
	, AVG(c.amt_usd) AS avg_usd_spend_per_transaction
	, SUM(c.amt_usd) sum_usd_spend
	, COUNT(DISTINCT c.user_id) user_count
	, SUM(c.amt_usd) / COUNT(DISTINCT c.user_id)::NUMERIC avg_usd_spend_per_user
FROM
	reportings_data.dm_cards_transactions c
WHERE TRUE
	AND c.transaction_type = 'Payment Autorization'
	AND c.tr_complete=1
GROUP BY 1,2,3
;


WITH card_user AS (
	SELECT 
		user_id 
		, application_started_at register_at
		, kyc_verified_at verified_at 
		, card_issued_at 
		, card_activated_at 
	FROM reportings_data.dm_cards_users dcu 
	WHERE application_status IS NOT NULL 
)	, user_register AS (
	SELECT 
		register_at::DATE 
		, COUNT(DISTINCT user_id) register_count
	FROM card_user
	GROUP BY 1
)	, user_verified AS (
	SELECT 
		verified_at::DATE 
		, COUNT(DISTINCT CASE WHEN verified_at IS NOT NULL THEN user_id END) verified_count
	FROM card_user
	GROUP BY 1
)	, user_activated AS (
	SELECT 
		card_activated_at::DATE 
		, COUNT(DISTINCT CASE WHEN card_activated_at IS NOT NULL THEN user_id END) activated_count
	FROM card_user
	GROUP BY 1
)	, tmp_final AS (
	SELECT 
--		COALESCE (r.register_at, v.verified_at, a.card_activated_at) created_at
		DATE_TRUNC('week', COALESCE (r.register_at, v.verified_at, a.card_activated_at))::DATE created_at
--		DATE_TRUNC('month', COALESCE (r.register_at, v.verified_at, a.card_activated_at))::DATE created_at
		, SUM(COALESCE (r.register_count, 0)) register_count
		, SUM(COALESCE (v.verified_count, 0)) verified_count
		, SUM(COALESCE (a.activated_count, 0)) activated_count
	FROM user_register r
		FULL OUTER JOIN user_verified v 
			ON r.register_at = v.verified_at
		FULL OUTER JOIN user_activated a 
			ON COALESCE (r.register_at, v.verified_at) = a.card_activated_at
	WHERE COALESCE (r.register_at, v.verified_at, a.card_activated_at) IS NOT NULL 
	GROUP BY 1
	ORDER BY 1
)
SELECT 
	*
	, SUM(register_count) OVER(ORDER BY created_at) cumu_register
	, SUM(verified_count) OVER(ORDER BY created_at) cumu_verified
	, SUM(verified_count) OVER(ORDER BY created_at) / (SUM(register_count) OVER(ORDER BY created_at))::NUMERIC verified_rate
	, SUM(activated_count) OVER(ORDER BY created_at) cumu_activated
	, SUM(activated_count) OVER(ORDER BY created_at) / (SUM(verified_count) OVER(ORDER BY created_at))::NUMERIC activated_verified_rate	
	, SUM(activated_count) OVER(ORDER BY created_at) / (SUM(register_count) OVER(ORDER BY created_at))::NUMERIC activated_register_rate	
FROM tmp_final
;



-- weekly avg spending sgd-usd 
WITH avg_all AS (
	SELECT
		AVG(amt_usd) AS avg_all_time
	FROM 
		reportings_data.dm_cards_transactions
	WHERE tr_complete = 1
) 
SELECT
	date_trunc('week',gps_transaction_date)::DATE AS "Week"
	, count(DISTINCT user_id) AS "# Users"
	, AVG(amt_usd) AS "Averge Weekly $USD"
	, b.avg_all_time AS "Average All time $USD"
FROM 
	reportings_data.dm_cards_transactions, avg_all b
WHERE TRUE
	AND tr_complete=1
	AND transaction_type = 'Payment Autorization'
--	[[AND gps_transaction_date <= {{start_date}}]]
--	[[AND gps_transaction_date <= {{end_date}}]]
GROUP BY 1,4
;


