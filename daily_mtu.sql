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
	, SUM( CASE WHEN a.symbol = 'USD' THEN trade_wallet_amount * 1
				ELSE trade_wallet_amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate) END) trade_wallet_amount_usd
	, SUM( z_wallet_amount * COALESCE(c.average_high_low, g.mid_price, z.price) ) z_wallet_amount_usd
	, SUM( ziplock_amount * COALESCE(c.average_high_low, g.mid_price, z.price) ) ziplock_amount_usd
FROM 
	oms_data.data_team_staging.wallets_balance_eod a 
	LEFT JOIN 
		analytics.users_master u 
		ON a.ap_account_id = u.ap_account_id 
	LEFT JOIN oms_data.public.cryptocurrency_prices c 
	    ON ((CONCAT(a.symbol, 'USD') = c.instrument_symbol) OR (c.instrument_symbol = 'MIOTAUSD' AND a.symbol ='IOTA'))
	    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', c.last_updated) + '1 day'::INTERVAL 
	LEFT JOIN oms_data.public.daily_closing_gold_prices g 
		ON ((DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', g.created_at)) 
		OR (DATE_TRUNC('day', a.created_at) = '2021-07-31 00:00:00' AND DATE_TRUNC('day', g.created_at) = '2021-07-30 00:00:00'))
		AND a.symbol = 'GOLD'
	LEFT JOIN oms_data.public.daily_ap_prices z
		ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', z.created_at)
		AND z.instrument_symbol  = 'ZMTUSD'
		AND a.symbol = 'ZMT'
	LEFT JOIN public.exchange_rates e
		ON date_trunc('day', e.created_at) = date_trunc('day', a.created_at)
		AND e.product_2_symbol  = a.symbol
		AND e."source" = 'coinmarketcap'
WHERE 
	a.created_at >= '2021-06-01 00:00:00' AND a.created_at < DATE_TRUNC('month', NOW()) 
	AND ((a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
	AND a.symbol NOT IN ('TST1','TST2')
	AND a.symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
--	AND a.ap_account_id = 143639
GROUP BY 1,2,3,4,5,6,7
ORDER BY 1 DESC 
)	, aum_snapshot AS (
SELECT 
	a.created_at 
	, a.signup_hostcountry 
	, a.ap_account_id 
	, CASE WHEN a.ap_account_id IN ('15',	'221',	'634',	'746',	'1002',	'1182',	'1202',	'1272',	'1708',	'6074',	'6828',	'11284',	'16293',	'19763',	'24108',	'24315',	'25431',	'37276',	'38526',	'39858',	'40119',	'40438',	'40890',	'48300',	'51313',	'51333',	'52266',	'54172',	'54231',	'54644',	'55224',	'55660',	'57262',	'58998',	'59049',	'59693',	'62663',	'63292',	'63314',	'63914',	'66402',	'67813',	'82129',	'84431',	'84461',	'84799',	'91297',	'92285',	'93791',	'94663',	'94993',	'96434',	'96535',	'101786',	'103488',	'103855',	'104832',	'106308',	'108014',	'127491',	'128405',	'131484',	'139503',	'141711',	'146194',	'146356',	'147984',	'157600',	'159685',	'161863',	'180376',	'183004')
			THEN TRUE ELSE FALSE END AS is_pcs
--	, symbol 
	, CASE WHEN is_zipup_subscribed = TRUE AND a.created_at >= DATE_TRUNC('day', zipup_subscribed_at) THEN TRUE ELSE FALSE END AS is_zipup
	, CASE WHEN symbol = 'ZMT' THEN 'ZMT' WHEN symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH') THEN 'zipup_coin'
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
	1,2,3,4,5,6
ORDER BY 
	1 DESC
)--	, active_balance AS (
SELECT 
	DATE_TRUNC('month', created_at) created_at 
	, signup_hostcountry
	, is_pcs
	, is_zipup 
--	, SUM( CASE WHEN asset_type = 'non_zipup' THEN total_wallet_usd END) nonzipup_usd_amount
	, SUM( CASE WHEN asset_type = 'ZMT' THEN total_wallet_usd END) zmt_usd_amount
	, SUM( CASE WHEN asset_type = 'zipup_coin' THEN total_wallet_usd END) nonzmt_usd_amount
	, SUM( CASE WHEN asset_type = 'ZMT' THEN ziplock_amount_usd END) zmt_lock_usd_amount
	, SUM( CASE WHEN asset_type <> 'ZMT' THEN ziplock_amount_usd END) nonzmt_lock_usd_amount
FROM 
	aum_snapshot a 
GROUP BY 1,2,3,4 






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
	, SUM( CASE WHEN a.symbol = 'USD' THEN trade_wallet_amount * 1
				ELSE trade_wallet_amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate) END) trade_wallet_amount_usd
	, SUM( z_wallet_amount * COALESCE(c.average_high_low, g.mid_price, z.price) ) z_wallet_amount_usd
	, SUM( ziplock_amount * COALESCE(c.average_high_low, g.mid_price, z.price) ) ziplock_amount_usd
FROM 
	oms_data.analytics.wallets_balance_eod a 
	LEFT JOIN 
		analytics.users_master u 
		ON a.ap_account_id = u.ap_account_id 
	LEFT JOIN oms_data.public.cryptocurrency_prices c 
	    ON ((CONCAT(a.symbol, 'USD') = c.instrument_symbol) OR (c.instrument_symbol = 'MIOTAUSD' AND a.symbol ='IOTA'))
	    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', c.last_updated) + '1 day'::INTERVAL 
	LEFT JOIN oms_data.public.daily_closing_gold_prices g 
		ON ((DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', g.created_at)) 
		OR (DATE_TRUNC('day', a.created_at) = '2021-07-31 00:00:00' AND DATE_TRUNC('day', g.created_at) = '2021-07-30 00:00:00'))
		AND a.symbol = 'GOLD'
	LEFT JOIN oms_data.public.daily_ap_prices z
		ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', z.created_at)
		AND z.instrument_symbol  = 'ZMTUSD'
		AND a.symbol = 'ZMT'
	LEFT JOIN public.exchange_rates e
		ON date_trunc('day', e.created_at) = date_trunc('day', a.created_at)
		AND e.product_2_symbol  = a.symbol
		AND e."source" = 'coinmarketcap'
WHERE 
	a.created_at >= '2021-01-01 00:00:00' AND a.created_at < DATE_TRUNC('day', NOW()) 
	AND a.symbol NOT IN ('TST1','TST2')
	AND ((a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
	AND a.symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
--	AND a.ap_account_id = 143639
GROUP BY 1,2,3,4,5,6,7
ORDER BY 1 DESC 
)	, aum_snapshot AS (
SELECT 
	a.created_at 
	, a.signup_hostcountry 
	, a.ap_account_id 
--	, symbol 
	, CASE WHEN is_zipup_subscribed = TRUE AND a.created_at >= DATE_TRUNC('day', zipup_subscribed_at) THEN TRUE ELSE FALSE END AS is_zipup
	, CASE WHEN symbol = 'ZMT' THEN 'ZMT' WHEN symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH') THEN 'zipup_coin'
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
)	, active_balance AS (
SELECT 
	created_at 
	, signup_hostcountry
	, ap_account_id 
	, SUM( CASE WHEN asset_type = 'ZMT' THEN total_wallet_usd END) zmt_usd_amount
	, SUM( CASE WHEN asset_type <> 'ZMT' THEN total_wallet_usd END) nonzmt_usd_amount
	, SUM( CASE WHEN asset_type = 'ZMT' THEN ziplock_amount_usd END) zmt_lock_usd_amount
	, SUM( CASE WHEN asset_type <> 'ZMT' THEN ziplock_amount_usd END) nonzmt_lock_usd_amount
FROM 
	aum_snapshot a 
WHERE 
	asset_type <> 'non_zipup'
	AND is_zipup = TRUE
GROUP BY 1,2,3
)	, active_user AS (
SELECT 
	DATE_TRUNC('day', created_at) created_at 
	, signup_hostcountry
	, ap_account_id 
	, CASE WHEN nonzmt_usd_amount >= 1 THEN ap_account_id END AS zipup_user
	, CASE WHEN (COALESCE (zmt_lock_usd_amount,0) + COALESCE (nonzmt_lock_usd_amount,0)) >= 1 THEN ap_account_id END AS total_ziplock_user
	, CASE WHEN COALESCE (zmt_lock_usd_amount,0) >= 1 AND COALESCE (nonzmt_lock_usd_amount,0) >= 1 THEN ap_account_id END AS ziplock_mix_user
	, CASE WHEN COALESCE (zmt_lock_usd_amount,0) < 1 AND COALESCE (nonzmt_lock_usd_amount,0) >= 1 THEN ap_account_id END AS ziplock_nozmt_user
	, CASE WHEN COALESCE (zmt_lock_usd_amount,0) >= 1 AND COALESCE (nonzmt_lock_usd_amount,0) < 1 THEN ap_account_id END AS ziplock_zmt_user
	, CASE WHEN (COALESCE (nonzmt_usd_amount,0) >= 1 OR COALESCE (zmt_lock_usd_amount,0) >= 1 OR COALESCE (nonzmt_lock_usd_amount,0) >= 1) THEN ap_account_id END AS active_balance_user
FROM active_balance
WHERE (nonzmt_usd_amount >= 1 OR zmt_lock_usd_amount >= 1 OR nonzmt_lock_usd_amount >= 1) 
), active_trader AS (
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
FROM active_user a
	FULL OUTER JOIN active_trader t 
		ON a.ap_account_id = t.ap_account_id
		AND a.created_at = t.created_at
		AND a.signup_hostcountry = t.signup_hostcountry
GROUP BY 1,2
ORDER BY 1 DESC, 2 DESC 

