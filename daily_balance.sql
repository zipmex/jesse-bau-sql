-- AUM before Nov 18 2021 -- join all crypto prices tables
WITH base AS (
SELECT 
	a.created_at 
	, CASE WHEN u.signup_hostcountry IS NULL THEN 'unknown' ELSE u.signup_hostcountry END AS signup_hostcountry
	, a.ap_account_id 
	, CASE WHEN a.created_at < '2021-11-01 00:00:00' THEN 
			(CASE WHEN a.ap_account_id IN (0, 3, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347, 317029, 11045)
			THEN TRUE ELSE FALSE END)			
		ELSE
			(CASE WHEN a.ap_account_id IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id <> 496001)
			THEN TRUE ELSE FALSE END) 
		END AS is_nominee 
	, CASE WHEN a.ap_account_id = 496001 THEN TRUE ELSE FALSE END AS is_asset_manager
	, a.symbol 
	, CASE WHEN u.signup_hostcountry = 'TH' THEN
		(CASE WHEN a.created_at < '2022-05-08' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
		WHEN u.signup_hostcountry = 'ID' THEN
		(CASE WHEN a.created_at < '2022-07-04' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
		WHEN u.signup_hostcountry IN ('AU','global') THEN
		(CASE WHEN a.created_at < '2022-06-29' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
		END AS zipup_subscribed_at	, trade_wallet_amount
	, z_wallet_amount
	, ziplock_amount
	, COALESCE(c.average_high_low, g.mid_price, z1.price, 1/e.exchange_rate) usd_rate 
	, CASE 
			WHEN a.created_at < '2021-11-01' THEN
				(CASE WHEN a.symbol = 'USD' THEN trade_wallet_amount * 1
						ELSE trade_wallet_amount * COALESCE(c.average_high_low, g.mid_price, z1.price, 1/e.exchange_rate) END)
			ELSE 
				(CASE WHEN r.product_type = 1 THEN trade_wallet_amount * 1/r.price 
					WHEN r.product_type = 2 THEN trade_wallet_amount * r.price END)
			END AS trade_wallet_amount_usd
	, CASE 
			WHEN a.created_at <= '2021-09-15 00:00:00' THEN z_wallet_amount * COALESCE(c.average_high_low, g.mid_price, z.price) 
			WHEN a.created_at < '2021-11-01 00:00:00' THEN z_wallet_amount * COALESCE(c.average_high_low, g.mid_price, z1.price)
			ELSE z_wallet_amount * r.price 
			END AS z_wallet_amount_usd
	, CASE 
			WHEN a.created_at <= '2021-09-15 00:00:00' THEN ziplock_amount * COALESCE(c.average_high_low, g.mid_price, z.price) 
			WHEN a.created_at < '2021-11-01 00:00:00' THEN ziplock_amount * COALESCE(c.average_high_low, g.mid_price, z1.price) 
			ELSE ziplock_amount * r.price 
			END AS ziplock_amount_usd
FROM 
	analytics.wallets_balance_eod a 
	LEFT JOIN 
		analytics.users_master u 
		ON a.ap_account_id = u.ap_account_id 
	LEFT JOIN 
		warehouse.zip_up_service_public.user_settings s
		ON u.user_id = s.user_id 
	LEFT JOIN oms_data_public.cryptocurrency_prices c 
	    ON ((CONCAT(a.symbol, 'USD') = c.instrument_symbol) 
	    OR (c.instrument_symbol = 'MIOTAUSD' AND a.symbol ='IOTA') 
	    OR (c.instrument_symbol = 'USDPUSD' AND a.symbol ='PAX'))
	    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', c.last_updated) + '1 day'::INTERVAL 
	LEFT JOIN public.daily_closing_gold_prices g 
		ON ((DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', g.created_at)) 
		OR (DATE_TRUNC('day', a.created_at) = '2021-07-31 00:00:00' AND DATE_TRUNC('day', g.created_at) = '2021-07-30 00:00:00'))
		AND a.symbol = 'GOLD'
	LEFT JOIN public.daily_ap_prices z
		ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', z.created_at) + '1 day'::INTERVAL
		AND ((z.instrument_symbol = 'ZMTUSD' AND a.symbol = 'ZMT')
		OR (z.instrument_symbol = 'C8PUSDT' AND a.symbol = 'C8P')
		OR (z.instrument_symbol = 'TOKUSD' AND a.symbol = 'TOK'))
	LEFT JOIN public.daily_ap_prices z1
		ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', z1.created_at)
		AND ((z1.instrument_symbol = 'ZMTUSD' AND a.symbol = 'ZMT')
		OR (z1.instrument_symbol = 'C8PUSDT' AND a.symbol = 'C8P')
		OR (z1.instrument_symbol = 'TOKUSD' AND a.symbol = 'TOK'))
	LEFT JOIN oms_data_public.exchange_rates e
		ON date_trunc('day', e.created_at) = date_trunc('day', a.created_at)
		AND e.product_2_symbol  = a.symbol
		AND e."source" = 'coinmarketcap'
	LEFT JOIN analytics.rates_master r
		ON a.created_at = r.created_at 
		AND a.symbol = r.product_1_symbol 
WHERE 
	a.created_at >= '2021-01-01 00:00:00' AND a.created_at < '2021-09-01 00:00:00' -- DATE_TRUNC('day', NOW()) 
	AND u.signup_hostcountry IN ('TH','ID','AU','global')
	AND ((a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
	AND a.symbol NOT IN ('TST1','TST2')
ORDER BY 1 DESC 
)	--, aum_snapshot AS (
SELECT 
	DATE_TRUNC('month', created_at) created_at 
	, signup_hostcountry
	, ap_account_id 
--	, symbol 
--	, CASE	--WHEN symbol IN ('BTC','ETH','USDT','USDC','GOLD','LTC') THEN 'zipup_coin'
--			WHEN symbol IN ('ZMT') THEN 'ZMT'
--			ELSE 'other' END AS asset_group
--	, CASE WHEN ap_account_id IN (SELECT DISTINCT ap_account_id FROM mappings.commercial_is_whale) THEN TRUE ELSE FALSE END AS is_whale
--	, CASE WHEN is_zipup_subscribed = TRUE AND created_at >= DATE_TRUNC('day', zipup_subscribed_at)
--			AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT') THEN TRUE 
--			ELSE FALSE END AS is_zipup_amount
--	, COUNT(DISTINCT created_at) day_count
--	, SUM( COALESCE (trade_wallet_amount, 0)) trade_wallet_amount
--	, SUM( COALESCE (z_wallet_amount, 0)) z_wallet_amount
--	, SUM( COALESCE (ziplock_amount, 0)) ziplock_amount
--	, SUM( COALESCE (trade_wallet_amount_usd, 0)) trade_wallet_amount_usd
	, SUM( COALESCE (z_wallet_amount_usd, 0)) z_wallet_amount_usd
	, SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
	, SUM( COALESCE (CASE WHEN zipup_subscribed_at IS NOT NULL AND created_at >= DATE_TRUNC('day', zipup_subscribed_at) AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
				THEN
					(CASE 	WHEN created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
							WHEN created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
				END, 0)) AS zwallet_subscribed_usd
	, SUM( COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0) + COALESCE (ziplock_amount_usd, 0)) total_usd_amount
FROM 
	base 
WHERE 
	is_asset_manager = FALSE AND is_nominee = FALSE 
GROUP BY 
	1,2,3
ORDER BY
	1 DESC 
;


/*	WHEN symbol IN ('AXS') THEN 'AXS'
	WHEN symbol IN ('TOK',	'EOS',	'BTT',	'PAX',	'ONT',	'DGB',	'ICX',	'ANKR',	'WRX',	'KNC',	'XVS'
								,	'KAVA',	'OGN',	'CTSI',	'SRM',	'BAL',	'BTS',	'JST',	'COTI',	'ZEN',	'ANT'
								,	'RLC',	'STORJ',	'WTC',	'XVG',	'LSK',	'EGLD',	'FTM',	'RVN',	'UMA',	'LRC',	'FIL',	'AAVE'
								,	'TFUEL',	'RUNE',	'HBAR',	'CHZ',	'HOT',	'SUSHI',	'GRT',	'BNT',	'IOST',	'SLP')
			THEN 'batch_08_25'
	WHEN symbol IN ('MATIC',	'AAVE',	'HOT',	'SNX',	'BAT',	'FTT',	'UNI',	'1INCH',	'CHZ',	'CRV',	'ZRX',	'BNT',	'KNC')
			THEN 'batch_09_17'
	WHEN symbol IN ('ALPHA','BAND') THEN 'batch_band_alpha'
	WHEN t.product_1_symbol IN ('GALA','SUSHI','GRT','SLP','ATOM','LUNA','RUNE','AVAX','TRX','ALGO','XTZ')
			THEN 'batch_10_28'
	WHEN t.product_1_symbol IN ('ADA','BNB','SOL','DOT') THEN 'ada_bnb_sol_dot'
*/



---- aum after nov 18 2021 - using rates_master 
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
		, CASE WHEN u.signup_hostcountry IS NULL THEN 'unknown' ELSE u.signup_hostcountry END AS signup_hostcountry
		, a.ap_account_id , up.email 
	-- filter nominee accounts from users_mapping
		, CASE WHEN a.created_at < '2022-05-05' THEN  
			( CASE WHEN a.ap_account_id IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id NOT IN (496001))
			THEN TRUE ELSE FALSE END)
			ELSE
			( CASE WHEN a.ap_account_id IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id NOT IN (38121 ,496001))
			THEN TRUE ELSE FALSE END)
			END AS is_nominee 
	-- filter asset_manager account
		, CASE WHEN a.ap_account_id = 496001 THEN TRUE ELSE FALSE END AS is_asset_manager
	-- zipup subscribe status to identify zipup amount
		, (CASE WHEN u.signup_hostcountry = 'TH' THEN
			(CASE WHEN a.created_at < '2022-05-24' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
			WHEN u.signup_hostcountry = 'ID' THEN
			(CASE WHEN a.created_at < '2022-07-04' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
			WHEN u.signup_hostcountry IN ('AU','global') THEN
			(CASE WHEN a.created_at < '2022-06-29' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
			END)::DATE AS zipup_subscribed_at
		, CASE WHEN ult.tier_name IS NULL THEN 'no_zmt' ELSE ult.tier_name END AS vip_tier
		, a.symbol
		, CASE WHEN a.symbol = 'ZMT' THEN TRUE 
				WHEN zc.symbol IS NOT NULL THEN TRUE 
				ELSE FALSE END AS zipup_coin 
		, r.price usd_rate , r.product_type 
		, trade_wallet_amount
		, z_wallet_amount
		, ziplock_amount
		, zlaunch_amount
		, CASE	WHEN r.product_type = 1 THEN trade_wallet_amount * 1/r.price 
				WHEN r.product_type = 2 THEN trade_wallet_amount * r.price
				END AS trade_wallet_amount_usd
		, z_wallet_amount * r.price z_wallet_amount_usd
		, ziplock_amount * r.price ziplock_amount_usd
		, zlaunch_amount * r.price zlaunch_amount_usd
	FROM 
		analytics.wallets_balance_eod a 
	-- get country and join with pii data
		LEFT JOIN 
			analytics.users_master u 
			ON a.ap_account_id = u.ap_account_id 
		LEFT JOIN 
			zipup_coin zc 
			ON a.symbol = zc.symbol
			AND a.created_at >= zc.effective_date
			AND a.created_at < zc.expired_date
		LEFT JOIN 
			analytics.rates_master r 
			ON a.symbol = r.product_1_symbol
			AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
		LEFT JOIN 
			warehouse.zip_up_service_public.user_settings s
			ON u.user_id = s.user_id 
		LEFT JOIN 
			zip_lock_service_public.user_loyalty_tiers ult 
			ON u.user_id = ult.user_id 
		LEFT JOIN 
			mappings.users_mapping um 
			ON a.ap_account_id = um.ap_account_id 
		LEFT JOIN analytics_pii.users_pii up 
			ON u.user_id = up.user_id 
	WHERE 
		a.created_at >= '2022-05-01' AND a.created_at < '2022-08-01'
	-- exclude test products
		AND a.symbol NOT IN ('TST1','TST2')
--	    AND ((a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
		AND u.signup_hostcountry IN ('TH','ID','AU','global')
		AND a.symbol IN ('BTC','ETH','USDC','USDT')
	ORDER BY 1 DESC 
)--	, aum_snapshot AS (
	SELECT 
		DATE_TRUNC('day', b.created_at)::DATE created_at
		, b.signup_hostcountry
--		, b.symbol asset_group
		, CASE WHEN symbol <> 'ZMT' AND zipup_coin = TRUE THEN symbol 
				WHEN symbol = 'ZMT' THEN 'ZMT' 
				ELSE 'other' END AS asset_group
--		, vip_tier
--		, SUM( COALESCE (trade_wallet_amount, 0)) trade_wallet_amount
		, SUM( COALESCE (z_wallet_amount, 0)) z_wallet_amount
--		, SUM( COALESCE (ziplock_amount, 0)) ziplock_amount
--		, SUM( COALESCE (zlaunch_amount, 0)) zlaunch_amount
		, SUM( COALESCE (CASE WHEN zipup_subscribed_at IS NOT NULL AND created_at >= DATE_TRUNC('day', zipup_subscribed_at) AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
					THEN
						(CASE 	WHEN created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount, 0) + COALESCE (z_wallet_amount, 0)
								WHEN created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount, 0) END)
					END, 0)) AS zwallet_subscribed_amount
--		, SUM( COALESCE (trade_wallet_amount, 0) + COALESCE (z_wallet_amount, 0) 
--					+ COALESCE (ziplock_amount, 0) + COALESCE (zlaunch_amount, 0)) total_coin_amount
--		, SUM( COALESCE (trade_wallet_amount_usd, 0)) trade_wallet_amount_usd
--		, SUM( COALESCE (z_wallet_amount_usd, 0)) z_wallet_amount_usd
--		, SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
--		, SUM( COALESCE (zlaunch_amount_usd, 0)) zlaunch_amount_usd
--		, SUM( COALESCE (CASE WHEN zipup_subscribed_at IS NOT NULL AND b.created_at >= DATE_TRUNC('day', zipup_subscribed_at) AND zipup_coin = TRUE
--					THEN
--						(CASE 	WHEN b.created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
--								WHEN b.created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
--					END, 0)) AS zipup_subscribed_usd
--		, SUM( COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0) 
--					+ COALESCE (ziplock_amount_usd, 0) + COALESCE (zlaunch_amount_usd, 0)) total_aum_usd
	FROM 
		base b
	WHERE 
		is_asset_manager = FALSE AND is_nominee = FALSE
--		AND z_wallet_amount > 0
	GROUP BY 
		1,2,3
	ORDER BY 1,2



)
SELECT 
	created_at::DATE
--	, email 
	, signup_hostcountry
	, asset_group
-- trade wallet
	, SUM( COALESCE (trade_wallet_amount, 0)) trade_wallet_amount
	, SUM( COALESCE (trade_wallet_amount_usd, 0)) trade_wallet_amount_usd
	, percentile_disc(0.5) WITHIN GROUP (ORDER BY trade_wallet_amount_usd) median_trade_wallet_amount_usd
	, COUNT( DISTINCT CASE WHEN trade_wallet_amount > 0 THEN ap_account_id END) user_count_trade_wallet
	, CASE WHEN COUNT( DISTINCT CASE WHEN trade_wallet_amount > 0 THEN ap_account_id END) = 0 THEN 0 ELSE
		SUM( CASE WHEN trade_wallet_amount > 0 THEN COALESCE (trade_wallet_amount_usd, 0) END)
			/ COUNT( DISTINCT CASE WHEN trade_wallet_amount > 0 THEN ap_account_id END) 
		END AS avg_trade_wallet_amount_usd
-- z wallet
	, SUM( COALESCE (z_wallet_amount, 0)) z_wallet_amount
	, SUM( COALESCE (z_wallet_amount_usd, 0)) z_wallet_amount_usd
	, percentile_disc(0.5) WITHIN GROUP (ORDER BY z_wallet_amount_usd) median_z_wallet_amount_usd
	, COUNT( DISTINCT CASE WHEN z_wallet_amount > 0 THEN ap_account_id END) user_count_z_wallet
	, CASE WHEN COUNT( DISTINCT CASE WHEN z_wallet_amount > 0 THEN ap_account_id END) = 0 THEN 0 ELSE
		SUM( CASE WHEN z_wallet_amount > 0 THEN COALESCE (z_wallet_amount_usd, 0) END)
			/ COUNT( DISTINCT CASE WHEN z_wallet_amount > 0 THEN ap_account_id END) 
		END AS avg_z_wallet_amount_usd
-- zipup AUM
	, SUM( COALESCE (zwallet_subscribed_amount, 0)) zipup_subscribed_amount
	, SUM( COALESCE (zipup_subscribed_usd, 0)) zipup_subscribed_usd
	, percentile_disc(0.5) WITHIN GROUP (ORDER BY zipup_subscribed_usd) median_zipup_amount_usd
	, COUNT( DISTINCT CASE WHEN zwallet_subscribed_amount > 0 THEN ap_account_id END) user_count_zipup
	, CASE WHEN COUNT( DISTINCT CASE WHEN zwallet_subscribed_amount > 0 THEN ap_account_id END) = 0 THEN 0 ELSE
		SUM( CASE WHEN zwallet_subscribed_amount >= 0 THEN COALESCE (zipup_subscribed_usd, 0) END)
			/ COUNT( DISTINCT CASE WHEN zwallet_subscribed_amount > 0 THEN ap_account_id END) 
		END AS avg_zipup_subscribed_usd
FROM aum_snapshot
GROUP BY
	1,2,3
;


SELECT *
FROM bo_testing.dm_double_wallet ddw 
;