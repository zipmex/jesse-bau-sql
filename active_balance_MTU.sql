---- MTU 2021-09-21
WITH base AS (
	SELECT 
		a.created_at 
		, CASE WHEN u.signup_hostcountry IN ('test', 'error','xbullion') THEN 'test' ELSE u.signup_hostcountry END AS signup_hostcountry 
		, a.ap_account_id 
		, CASE WHEN a.ap_account_id IN (0, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347, 317029, 496001) 
				THEN TRUE ELSE FALSE END AS is_nominee
		, a.symbol 
		, u.zipup_subscribed_at 
		, u.is_zipup_subscribed 
		, SUM(trade_wallet_amount) trade_wallet_amount
		, SUM(z_wallet_amount) z_wallet_amount
		, SUM(ziplock_amount) ziplock_amount
--		, SUM( CASE WHEN a.symbol = 'USD' THEN trade_wallet_amount * 1
--					ELSE trade_wallet_amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate) END) trade_wallet_amount_usd
--		, SUM( z_wallet_amount * COALESCE(c.average_high_low, g.mid_price, z.price) ) z_wallet_amount_usd
--		, SUM( ziplock_amount * COALESCE(c.average_high_low, z.price) ) ziplock_amount_usd
		, SUM( CASE WHEN a.symbol = 'USD' THEN trade_wallet_amount * 1
					WHEN r.product_type = 1 THEN trade_wallet_amount * 1/r.price 
					WHEN r.product_type = 2 THEN trade_wallet_amount * r.price END) trade_wallet_amount_usd
		, SUM( z_wallet_amount * r.price ) z_wallet_amount_usd
		, SUM( ziplock_amount * r.price ) ziplock_amount_usd
	FROM 
		analytics.wallets_balance_eod a 
		LEFT JOIN 
			analytics.users_master u 
			ON a.ap_account_id = u.ap_account_id 
		LEFT JOIN 
			data_team_staging.rates_master_staging r 
			ON a.symbol = r.product_1_symbol 
		    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
	WHERE 
		a.created_at >= '2021-01-01 00:00:00' AND a.created_at < DATE_TRUNC('day', NOW()) 
		AND a.symbol NOT IN ('TST1','TST2')
		AND ((a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
		AND u.signup_hostcountry IN ('TH','ID','AU','global')
		AND a.symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
	--	AND a.ap_account_id = 143639
	GROUP BY 1,2,3,4,5,6,7
	ORDER BY 1 DESC 
)	, aum_snapshot AS (
	SELECT 
		a.created_at 
		, a.signup_hostcountry 
		, a.ap_account_id 
		, CASE WHEN is_zipup_subscribed = TRUE AND a.created_at >= DATE_TRUNC('day', zipup_subscribed_at) THEN TRUE ELSE FALSE END AS is_zipup
		, CASE WHEN symbol = 'ZMT' THEN 'ZMT' 
				WHEN symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH') THEN 'zipup_coin'
				ELSE 'non_zipup' END AS asset_type
		, SUM( COALESCE (trade_wallet_amount, 0)) trade_wallet_amount
		, SUM( COALESCE (trade_wallet_amount_usd,0)) trade_wallet_amount_usd
		, SUM( COALESCE (z_wallet_amount, 0)) z_wallet_amount
		, SUM( COALESCE (z_wallet_amount_usd, 0)) z_wallet_amount_usd
		, SUM( COALESCE (trade_wallet_amount, 0) + COALESCE (z_wallet_amount, 0)) total_wallet_amount 
		, SUM( COALESCE (trade_wallet_amount_usd,0) + COALESCE (z_wallet_amount_usd, 0)) total_wallet_usd
		, SUM( COALESCE (ziplock_amount, 0)) ziplock_amount
		, SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
	FROM 
		base a 
	WHERE  
		signup_hostcountry IS NOT NULL AND signup_hostcountry <> 'test'
		AND is_nominee = FALSE
	GROUP BY 
		1,2,3,4,5
	ORDER BY 
		1 DESC
)	, active_zipup_balance AS (
	SELECT 
		created_at 
		, signup_hostcountry
		, ap_account_id 
		, SUM( CASE WHEN asset_type = 'ZMT' THEN total_wallet_usd END) zmt_usd_amount
		, SUM( CASE WHEN asset_type <> 'ZMT' THEN total_wallet_usd END) nonzmt_usd_amount
		, SUM( CASE WHEN asset_type = 'ZMT' THEN z_wallet_amount_usd END) zmt_zw_usd_amount
		, SUM( CASE WHEN asset_type <> 'ZMT' THEN z_wallet_amount_usd END) nonzmt_zw_usd_amount
	FROM 
		aum_snapshot a 
	WHERE 
		asset_type <> 'non_zipup'
		AND is_zipup = TRUE
	GROUP BY 1,2,3
)	, active_ziplock_balance AS (
	SELECT 
		created_at 
		, signup_hostcountry
		, ap_account_id 
		, SUM( CASE WHEN asset_type = 'ZMT' THEN ziplock_amount_usd END) zmt_lock_usd_amount
		, SUM( CASE WHEN asset_type <> 'ZMT' THEN ziplock_amount_usd END) nonzmt_lock_usd_amount
	FROM 
		aum_snapshot a 
	WHERE 
		asset_type <> 'non_zipup'
	GROUP BY 1,2,3
)	, active_user AS (
SELECT 
	COALESCE (DATE_TRUNC('month', u.created_at), DATE_TRUNC('month', l.created_at)) created_at 
	, COALESCE (u.signup_hostcountry, l.signup_hostcountry) signup_hostcountry
	, COALESCE (u.ap_account_id, l.ap_account_id) ap_account_id
	, CASE WHEN nonzmt_usd_amount >= 1 THEN u.ap_account_id END AS zipup_user
	, CASE WHEN nonzmt_zw_usd_amount >= 1 THEN u.ap_account_id END AS zipup_zw_user
	, CASE WHEN (COALESCE (zmt_lock_usd_amount,0) + COALESCE (nonzmt_lock_usd_amount,0)) >= 1 THEN l.ap_account_id END AS total_ziplock_user
	, CASE WHEN COALESCE (zmt_lock_usd_amount,0) >= 1 AND COALESCE (nonzmt_lock_usd_amount,0) >= 1 THEN l.ap_account_id END AS ziplock_mix_user
	, CASE WHEN COALESCE (zmt_lock_usd_amount,0) < 1 AND COALESCE (nonzmt_lock_usd_amount,0) >= 1 THEN l.ap_account_id END AS ziplock_nozmt_user
	, CASE WHEN COALESCE (zmt_lock_usd_amount,0) >= 1 AND COALESCE (nonzmt_lock_usd_amount,0) < 1 THEN l.ap_account_id END AS ziplock_zmt_user
	, CASE WHEN u.created_at < '2021-09-01 00:00:00' THEN 
			(CASE WHEN (COALESCE (nonzmt_usd_amount,0) >= 1 OR COALESCE (zmt_lock_usd_amount,0) >= 1 OR COALESCE (nonzmt_lock_usd_amount,0) >= 1) THEN u.ap_account_id END)
			ELSE 
			(CASE WHEN (COALESCE (nonzmt_zw_usd_amount,0) >= 1 OR COALESCE (zmt_lock_usd_amount,0) >= 1 OR COALESCE (nonzmt_lock_usd_amount,0) >= 1) THEN u.ap_account_id END)
			END AS active_balance_user
FROM 
	active_zipup_balance u 
	FULL OUTER JOIN active_ziplock_balance l 
		ON u.created_at = l.created_at
		AND u.ap_account_id = l.ap_account_id
		AND u.signup_hostcountry = l.signup_hostcountry
WHERE
	(nonzmt_usd_amount >= 1 OR zmt_lock_usd_amount >= 1 OR nonzmt_lock_usd_amount >= 1 OR nonzmt_zw_usd_amount >= 1)
)
SELECT 
	created_at 
	, signup_hostcountry
	, COUNT( DISTINCT zipup_user) zipup_user_count
	, COUNT( DISTINCT total_ziplock_user) total_ziplock_user
	, COUNT( DISTINCT ziplock_mix_user) ziplock_mix_user
	, COUNT( DISTINCT ziplock_nozmt_user) ziplock_nozmt_user
	, COUNT( DISTINCT ziplock_zmt_user) ziplock_zmt_user
	, COUNT( DISTINCT active_balance_user) active_balance_count
	, COUNT( DISTINCT zipup_zw_user) zipup_zw_user_count
FROM active_user
GROUP BY 1,2


), 

WITH active_trader AS (
SELECT 
	DISTINCT DATE_TRUNC('month', created_at) created_at 
	, ap_account_id
	, signup_hostcountry 
FROM analytics.trades_master 
WHERE 
	ap_account_id NOT IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227','27443'
	,'38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','44057','161347','316078','44056','63152',
	0, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347, 317029, 496001)
	AND signup_hostcountry NOT IN ('test', 'error','xbullion')
	AND created_at >= '2021-01-01 00:00:00' AND created_at < DATE_TRUNC('day', NOW()) 
)
SELECT 
	created_at 
	, signup_hostcountry 
	, COUNT(DISTINCT ap_account_id) active_trader
FROM active_trader
GROUP BY 1,2


)
SELECT 
	DATE_TRUNC('month', COALESCE (a.created_at, t.created_at)) created_at 
	, COALESCE (a.signup_hostcountry, t.signup_hostcountry) signup_hostcountry
	, COUNT( DISTINCT t.ap_account_id) trader_count 
	, COUNT( DISTINCT zipup_user) zipup_user_count
	, COUNT( DISTINCT total_ziplock_user) total_ziplock_user
	, COUNT( DISTINCT ziplock_mix_user) ziplock_mix_user
	, COUNT( DISTINCT ziplock_nozmt_user) ziplock_nozmt_user
	, COUNT( DISTINCT ziplock_zmt_user) ziplock_zmt_user
	, COUNT( DISTINCT COALESCE (a.ap_account_id, t.ap_account_id)) mtu_count 
	, COUNT( DISTINCT active_balance_user) active_balance_count
	, COUNT( DISTINCT zipup_zw_user) zipup_zw_user_count
FROM active_user a
	FULL OUTER JOIN active_trader t 
		ON a.ap_account_id = t.ap_account_id
		AND a.created_at = t.created_at
		AND a.signup_hostcountry = t.signup_hostcountry
GROUP BY 1,2
ORDER BY 1 DESC, 2 DESC 


