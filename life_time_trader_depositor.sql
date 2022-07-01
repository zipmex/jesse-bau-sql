WITH traded_user AS (
SELECT 
	DISTINCT 
	ap_account_id
	, COUNT(DISTINCT trade_id) trade_count
	, COUNT(DISTINCT order_id) order_count
FROM analytics.trades_master t
GROUP BY 1
)	, deposit_user AS (
SELECT
	ap_account_id 
	, status 
	, COUNT(DISTINCT ticket_id) ticket_count
FROM analytics.deposit_tickets_master d
WHERE status = 'FullyProcessed'
GROUP BY 1,2
)	, final_t AS (
SELECT
	COALESCE (t.ap_account_id, d.ap_account_id) ap_account_id 
	, t.ap_account_id traded_once
	, d.ap_account_id deposit_once
FROM traded_user t 
	FULL OUTER JOIN deposit_user d 
	ON t.ap_account_id = d.ap_account_id 
)
SELECT
	u.signup_hostcountry 
--	, is_zipup_subscribed
	, COUNT(DISTINCT f.ap_account_id) total_count
	, COUNT(DISTINCT traded_once) trader_count
	, COUNT(DISTINCT deposit_once) depositor_count
FROM final_t f 
	LEFT JOIN analytics.users_master u
	ON f.ap_account_id = u.ap_account_id
WHERE 
	u.signup_hostcountry IN ('TH','AU','ID','global')
	AND f.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
GROUP BY 1
;

SELECT DISTINCT ap_account_id FROM mappings.users_mapping

SELECT 
	signup_hostcountry 
	, COUNT(DISTINCT user_id) register_count
	, COUNT(DISTINCT CASE WHEN is_verified = TRUE THEN user_id END) verified_count
	, COUNT(DISTINCT CASE WHEN has_traded = TRUE THEN user_id END) traded_count
	, COUNT(DISTINCT CASE WHEN has_deposited = TRUE THEN user_id END) deposited_count
FROM analytics.users_master u
GROUP BY 1