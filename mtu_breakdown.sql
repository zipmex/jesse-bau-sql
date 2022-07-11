WITH mtu_base AS (
	SELECT 
		DISTINCT 
		dmm.mtu_month::DATE 
		, dmm.signup_hostcountry
		, dmm.ap_account_id
	FROM analytics.dm_mtu_monthly dmm 
	WHERE 
		mtu = TRUE 
		AND mtu_month = '2022-06-01'
	ORDER BY 1,2
)	, fiat_balance AS (
	SELECT
		w.created_at 
		, mb.signup_hostcountry
		, mb.ap_account_id 
		, rm.product_type 
		, SUM(w.trade_wallet_amount) fiat_amount
		, SUM(w.trade_wallet_amount * 1/rm.price) fiat_amount_usd
	FROM 
		analytics.wallets_balance_eod w 
		RIGHT JOIN mtu_base mb
			ON w.ap_account_id = mb.ap_account_id
		LEFT JOIN analytics.rates_master rm 
			ON w.symbol = rm.product_1_symbol 
			AND w.created_at = rm.created_at 
	WHERE 
		w.created_at = '2022-06-30'
--		AND rm.product_type = 1
	GROUP BY 1,2,3,4
)
SELECT 
	created_at::DATE 
	, signup_hostcountry
	, SUM(CASE WHEN product_type = 1 THEN fiat_amount_usd END) fiat_amount_usd
	, COUNT(DISTINCT CASE WHEN fiat_amount_usd > 0 THEN ap_account_id END) total_fiat_holder
	, COUNT(DISTINCT CASE WHEN product_type = 1 AND fiat_amount_usd > 0 THEN ap_account_id END) total_fiat_holder
	, COUNT(DISTINCT CASE WHEN product_type = 1 AND fiat_amount_usd >= 1 THEN ap_account_id END) fiat_holder_1usd
FROM fiat_balance
GROUP BY 1,2
;


SELECT 
	dmm.mtu_month::DATE 
	, signup_hostcountry
	, COUNT(DISTINCT ap_account_id) mtu_count
	, COUNT(DISTINCT CASE WHEN dmm.trade_count > 0 THEN ap_account_id END) count_trader
	, COUNT(DISTINCT CASE WHEN dmm.avg_z_wallet_amount_usd >= 1 OR avg_ziplock_amount_usd >= 1 THEN ap_account_id END) count_staker
FROM 
	analytics.dm_mtu_monthly dmm 
WHERE 
	mtu = TRUE 
	AND mtu_month = '2022-06-01'
	AND ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id NOT IN (38121))
GROUP BY 1,2