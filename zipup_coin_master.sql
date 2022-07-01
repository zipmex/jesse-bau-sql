WITH base AS (
SELECT 
	DISTINCT UPPER(SPLIT_PART(product_id,'.',1)) symbol
	, started_at effective_date
	, ended_at expired_date
FROM zip_up_service_public.interest_rates
ORDER BY 1
)--	, fill_expired_date
SELECT 
	DISTINCT
	symbol
	, (CASE WHEN effective_date < '2022-03-22' THEN '2018-01-01' ELSE effective_date END)::DATE AS effective_date
	, (CASE WHEN expired_date IS NULL THEN COALESCE( LEAD(effective_date) OVER(PARTITION BY symbol),'2999-12-31') ELSE expired_date END)::DATE AS expired_date
FROM base 
ORDER BY 3,2

FROM data_team_staging.zip_up_rates_master



WITH coin_base AS (
	SELECT 
		DISTINCT UPPER(SPLIT_PART(product_id,'.',1)) symbol
		, started_at effective_date
		, ended_at expired_date
	FROM zip_up_service_public.interest_rates
	ORDER BY 1
)	, zipup_coin AS (
	SELECT 
		DISTINCT
		symbol
		, (CASE WHEN effective_date < '2022-03-22' THEN '2018-01-01' ELSE effective_date END)::DATE AS effective_date
		, (CASE WHEN expired_date IS NULL THEN COALESCE( LEAD(effective_date) OVER(PARTITION BY symbol),'2999-12-31') ELSE expired_date END)::DATE AS expired_date
	FROM coin_base 
	ORDER BY 3,2
)	, base AS (
	SELECT 
		a.created_at::DATE 
		, a.ap_account_id 
		, a.symbol
		, CASE WHEN a.symbol = 'ZMT' THEN TRUE WHEN zc.symbol IS NOT NULL THEN TRUE ELSE FALSE END AS zipup_coin 
		, r.price usd_rate 
		, trade_wallet_amount
		, z_wallet_amount
		, ziplock_amount
		, zlaunch_amount
	FROM 
		analytics.wallets_balance_eod a 
		LEFT JOIN 
			zipup_coin zc 
			ON a.symbol = zc.symbol
			AND a.created_at >= zc.effective_date
			AND a.created_at < zc.expired_date
		LEFT JOIN 
			analytics.rates_master r 
			ON a.symbol = r.product_1_symbol
			AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
	WHERE 
		a.created_at >= '2022-05-01' --AND a.created_at < '2022-01-01'
	-- exclude test products
		AND a.symbol NOT IN ('TST1','TST2')
	    AND ((a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
--		AND a.ap_account_id = 
	ORDER BY 1 DESC 
	
	
	symbol <> 'ZMT' AND zipup_coin = TRUE THEN 'zipup_coin' 