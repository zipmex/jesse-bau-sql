-- trading volume
SELECT
	t.signup_hostcountry 
	, SUM( CASE WHEN t.created_at >= DATE_TRUNC('week',NOW()::DATE - '1 day'::INTERVAL) THEN t.amount_usd END) lastweek_trade_usd
	, SUM( CASE WHEN t.created_at >= DATE_TRUNC('month',NOW()::DATE ) THEN t.amount_usd END) mtd_trade_usd
	, SUM( CASE WHEN t.created_at >= DATE_TRUNC('quarter',NOW()::DATE ) THEN t.amount_usd END) qtd_trade_usd
	, SUM( CASE WHEN t.created_at >= DATE_TRUNC('year',NOW()::DATE ) THEN t.amount_usd END) ytd_trade_usd
FROM 
	analytics.trades_master t
	LEFT JOIN analytics.users_master u
		ON t.ap_account_id = u.ap_account_id
WHERE 
	t.created_at >= DATE_TRUNC('year',NOW()::DATE )
	AND t.signup_hostcountry IN ('TH','ID','AU','global')
	AND CASE WHEN t.created_at < '2022-05-05' THEN (t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping))
		ELSE (t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id NOT IN (38121))) END
GROUP BY 1
;


---- daily user funnel
WITH temp_ AS (
	SELECT 
	u.signup_hostcountry ,u.user_id ,u.ap_user_id,u.ap_account_id 
	,u.created_at AS register_date
	,u.onfido_completed_at AS kyc_date 
	,u.is_verified 
	,u.level_increase_status 
	FROM 
		analytics.users_master u
	WHERE 
		u.signup_hostcountry IN ('TH','ID','AU','global')  
		AND u.created_at > '2022-01-01' 
),temp_m AS (
	SELECT 
		signup_hostcountry
		, DATE_TRUNC('day', register_date)::DATE AS register_month
		,COUNT(distinct user_id) AS user_id_c
	FROM temp_
	GROUP BY 1, 2
),temp_kyc AS (
	SELECT 
		signup_hostcountry
		, DATE_TRUNC('day', kyc_date)::DATE AS kyc_month
		,COUNT(DISTINCT CASE WHEN kyc_date IS NOT NULL AND is_verified = TRUE THEN user_id END) AS user_id_kyc_new ---> this one count the status by kyc date, number is fixed level_increase_status = 'pass'
	FROM temp_
	GROUP BY 1,2
), final_temp AS (
	SELECT 
		b.signup_hostcountry, b.register_month
		, COALESCE(b.user_id_c,0) user_id_c
		, COALESCE(user_id_kyc_new,0) user_id_kyc_new 
	FROM temp_m b
		LEFT JOIN temp_kyc k ON k.signup_hostcountry = b.signup_hostcountry AND k.kyc_month = b.register_month 
	ORDER BY 1,2
)
SELECT 
	signup_hostcountry 
	, SUM( CASE WHEN register_month >= DATE_TRUNC('week',NOW()::DATE - '1 day'::INTERVAL) THEN user_id_kyc_new END) lastweek_verified_count
	, SUM( CASE WHEN register_month >= DATE_TRUNC('month',NOW()::DATE ) THEN user_id_kyc_new END) mtd_verified_count
	, SUM( CASE WHEN register_month >= DATE_TRUNC('quarter',NOW()::DATE ) THEN user_id_kyc_new END) qtd_verified_count
	, SUM( CASE WHEN register_month >= DATE_TRUNC('year',NOW()::DATE ) THEN user_id_kyc_new END) ytd_verified_count
--	,sum(user_id_c) OVER(PARTITION BY signup_hostcountry ORDER BY register_month ) AS total_registered_user
--	,sum(user_id_kyc_new) OVER(PARTITION BY signup_hostcountry ORDER BY register_month) AS total_kyc_user
--	,SUM(user_id_kyc_new) OVER() zipmex_total_kyc
FROM final_temp 
GROUP BY 1
;


-- AUM
SELECT 
	signup_hostcountry 
-- total aum aka asset on platform (AoP) include ZMT
	, SUM( CASE WHEN created_at = DATE_TRUNC('day', NOW()::DATE) - '1 day'::INTERVAL THEN total_aum_usd END) yesterday_aop
	, SUM( CASE WHEN created_at = DATE_TRUNC('week', NOW()::DATE) + '6 day' - '1 week'::INTERVAL THEN total_aum_usd END) lastweek_aop
	, SUM( CASE WHEN created_at = DATE_TRUNC('week', NOW()::DATE) + '6 day' - '2 week'::INTERVAL THEN total_aum_usd END) last2week_aop
	, SUM( CASE WHEN created_at = DATE_TRUNC('month', NOW()::DATE) THEN total_aum_usd END) "1stofmonth_aop"
	, SUM( CASE WHEN created_at = DATE_TRUNC('quarter', NOW()::DATE) THEN total_aum_usd END) "1stofquarter_aop"
	, SUM( CASE WHEN created_at = DATE_TRUNC('year', NOW()::DATE) THEN total_aum_usd END) "1stofyear_aop"
-- interest bearing (IBB) exclude ZMT 
	, SUM( CASE WHEN created_at = DATE_TRUNC('day', NOW()::DATE) - '1 day'::INTERVAL THEN nonzmt_interest_bearing_usd  END) yesterday_ibb
	, SUM( CASE WHEN created_at = DATE_TRUNC('week', NOW()::DATE) + '6 day' - '1 week'::INTERVAL THEN nonzmt_interest_bearing_usd END) lastweek_ibb
	, SUM( CASE WHEN created_at = DATE_TRUNC('week', NOW()::DATE) + '6 day' - '2 week'::INTERVAL THEN nonzmt_interest_bearing_usd END) last2week_ibb
	, SUM( CASE WHEN created_at = DATE_TRUNC('month', NOW()::DATE) THEN nonzmt_interest_bearing_usd END) "1stofmonth_ibb"
	, SUM( CASE WHEN created_at = DATE_TRUNC('quarter', NOW()::DATE) THEN nonzmt_interest_bearing_usd END) "1stofquarter_ibb"
	, SUM( CASE WHEN created_at = DATE_TRUNC('year', NOW()::DATE) THEN nonzmt_interest_bearing_usd END) "1stofyear_ibb"
FROM reportings_data.dm_aum_daily dad 
GROUP BY 1
;

-- mtu
SELECT 
	signup_hostcountry
	, COUNT(DISTINCT CASE WHEN dmd.mtu_day = DATE_TRUNC('day', NOW()::DATE) - '1 day'::INTERVAL THEN ap_account_id END) yesterday_mtu
	, COUNT(DISTINCT CASE WHEN dmd.mtu_day = DATE_TRUNC('week', NOW()::DATE) + '6 day' - '1 week'::INTERVAL THEN ap_account_id END) lastweek_mtu
	, COUNT(DISTINCT CASE WHEN dmd.mtu_day = DATE_TRUNC('week', NOW()::DATE) + '6 day' - '2 week'::INTERVAL THEN ap_account_id END) last2week_mtu
	, COUNT(DISTINCT CASE WHEN dmd.mtu_day = DATE_TRUNC('month', NOW()::DATE) THEN ap_account_id END) "1stofmonth_mtu"
	, COUNT(DISTINCT CASE WHEN dmd.mtu_day = DATE_TRUNC('quarter', NOW()::DATE) THEN ap_account_id END) "1stofquarter_mtu"
	, COUNT(DISTINCT CASE WHEN dmd.mtu_day = DATE_TRUNC('year', NOW()::DATE) THEN ap_account_id END) "1stofyear_mtu"
FROM 
	analytics.dm_mtu_daily dmd 
WHERE 
	mtu = TRUE 
GROUP BY 1
;


-- monthly aum
SELECT 
	created_at 
	, SUM(nonzmt_interest_bearing_usd) nonzmt_interest_bearing_usd 
FROM reportings_data.dm_aum_daily dad
WHERE 
	created_at = DATE_TRUNC('month',created_at) + '1 month' - '1day'::INTERVAL
GROUP BY 1
;



-- mtu subscribed/ nonsubscribed
WITH base AS (
	SELECT
		dmm.signup_hostcountry
		, ap_account_id
		, COUNT(DISTINCT CASE WHEN mtu IS TRUE THEN ap_account_id END) mtu_count
		, COUNT(DISTINCT CASE WHEN mtu IS TRUE AND is_zipup_subscribed IS TRUE THEN ap_account_id END) mtu_subscribed
		, COUNT(DISTINCT CASE WHEN mtu IS TRUE AND is_zipup_subscribed IS FALSE THEN ap_account_id END) mtu_unsubscribed
	FROM analytics.dm_mtu_monthly dmm 
	WHERE mtu_month = DATE_TRUNC('month', NOW())
	GROUP BY 1,2
)	, aum_sum AS (
	SELECT 
		a.created_at 
		, CASE WHEN u.signup_hostcountry IS NULL THEN 'unknown' ELSE u.signup_hostcountry END AS signup_hostcountry
		, a.ap_account_id 
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
		, CASE WHEN u.signup_hostcountry = 'TH' THEN
			(CASE WHEN a.created_at < '2022-05-08' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
			WHEN u.signup_hostcountry = 'ID' THEN
			(CASE WHEN a.created_at < '2022-07-04' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
			WHEN u.signup_hostcountry IN ('AU','global') THEN
			(CASE WHEN a.created_at < '2022-06-29' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
			END AS zipup_subscribed_at
--		, CASE WHEN ult.tier_name IS NULL THEN 'no_zmt' ELSE ult.tier_name END AS vip_tier
		, symbol
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
			mappings.users_mapping um 
			ON a.ap_account_id = um.ap_account_id
	-- coin prices and exchange rates (USD)
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
		RIGHT JOIN 
			base b 
			ON a.ap_account_id = b.ap_account_id
			AND mtu_unsubscribed = 1
	WHERE 
		a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL
	-- exclude test products
		AND a.symbol NOT IN ('TST1','TST2')
		AND u.signup_hostcountry IN ('AU','ID','global','TH')
	ORDER BY 1 DESC 
)	, aum_snapshot AS (
	SELECT 
		signup_hostcountry
		, ap_account_id 
		, CASE WHEN zipup_subscribed_at IS NOT NULL THEN TRUE ELSE FALSE END AS is_zipup_subscribed
		, SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
		, SUM( COALESCE (CASE WHEN zipup_subscribed_at IS NOT NULL AND b.created_at >= DATE_TRUNC('day', zipup_subscribed_at) AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
					THEN
						(CASE 	WHEN b.created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
								WHEN b.created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
					END, 0)) AS zipup_subscribed_usd
		, SUM( COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0) 
					+ COALESCE (ziplock_amount_usd, 0) + COALESCE (zlaunch_amount_usd, 0)) total_aum_usd
	FROM aum_sum b
	WHERE 
		is_nominee IS FALSE AND is_asset_manager IS FALSE 
	GROUP BY 1,2,3
)	, sum_avg AS (
SELECT 
	signup_hostcountry
	, is_zipup_subscribed
	, COUNT(DISTINCT ap_account_id) user_count
	, SUM(ziplock_amount_usd + zipup_subscribed_usd) zipup_aum
	, SUM(total_aum_usd) total_aum_usd
	, AVG(ziplock_amount_usd + zipup_subscribed_usd) avg_zipup_aum
	, AVG(total_aum_usd) avg_total_aum
FROM aum_snapshot
GROUP BY 1,2
)
SELECT 
	*
	, SUM(zipup_aum) OVER(PARTITION BY is_zipup_subscribed) zm_zipup_aum
	, SUM(total_aum_usd) OVER(PARTITION BY is_zipup_subscribed) zm_total_aum
	, SUM(user_count) OVER(PARTITION BY is_zipup_subscribed) total_user
FROM sum_avg
;



-- trade volume
WITH mtu_sub AS (
	SELECT
		dmm.signup_hostcountry
		, ap_account_id
		, COUNT(DISTINCT CASE WHEN mtu IS TRUE THEN ap_account_id END) mtu_count
		, COUNT(DISTINCT CASE WHEN mtu IS TRUE AND is_zipup_subscribed IS TRUE THEN ap_account_id END) mtu_subscribed
		, COUNT(DISTINCT CASE WHEN mtu IS TRUE AND is_zipup_subscribed IS FALSE THEN ap_account_id END) mtu_unsubscribed
	FROM analytics.dm_mtu_monthly dmm 
	WHERE mtu_month = DATE_TRUNC('month', NOW())
	GROUP BY 1,2
)	, base AS (
	SELECT
		t.signup_hostcountry 
		, t.ap_account_id 
--		, CASE WHEN u.signup_hostcountry = 'TH' THEN u.zipup_subscribed_at
--			WHEN u.signup_hostcountry = 'ID' THEN s.tnc_accepted_at
--			WHEN u.signup_hostcountry IN ('AU','global') THEN s.tnc_accepted_at
--			END AS zipup_subscribed_at
		, COUNT(DISTINCT t.ap_account_id) trader_count
		, SUM(t.amount_usd) trade_vol_usd
	FROM 
		analytics.trades_master t
		LEFT JOIN 
			analytics.users_master u 
			ON t.ap_account_id = u.ap_account_id 
		RIGHT JOIN 
			mtu_sub m 
			ON t.ap_account_id = m.ap_account_id
			AND m.mtu_unsubscribed = 1 
	WHERE 
		t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id NOT IN (38121))
		AND t.signup_hostcountry IN ('TH','AU','global','ID')
		AND DATE_TRUNC('month', t.created_at) >= '2021-09-01'
	GROUP BY
		1,2
	ORDER BY 1
)	, total_trade AS (
	SELECT 
		signup_hostcountry 
		, ap_account_id 
--		, CASE WHEN zipup_subscribed_at IS NOT NULL THEN TRUE ELSE FALSE END AS zipup_subscribed
		, SUM(trade_vol_usd) trade_vol_usd
	FROM base 
	GROUP BY 1,2
)	, final_calc AS (
SELECT 
	signup_hostcountry 
--	, zipup_subscribed
	, SUM(trade_vol_usd) trade_vol_usd
	, COUNT(DISTINCT ap_account_id) trader_count
	, AVG(trade_vol_usd) avg_trade_vol
FROM total_trade 
GROUP BY 1
)
SELECT 
	*
	, SUM(trade_vol_usd) OVER() total_trade_usd
	, SUM(trader_count) OVER() total_trader_count
	, SUM(trade_vol_usd) OVER() / SUM(trader_count) OVER()::NUMERIC avg_total
FROM final_calc
;


-- trade volume
WITH mtu_sub AS (
	SELECT
		dmm.signup_hostcountry
		, ap_account_id
		, COUNT(DISTINCT CASE WHEN mtu IS TRUE THEN ap_account_id END) mtu_count
		, COUNT(DISTINCT CASE WHEN mtu IS TRUE AND is_zipup_subscribed IS TRUE THEN ap_account_id END) mtu_subscribed
		, COUNT(DISTINCT CASE WHEN mtu IS TRUE AND is_zipup_subscribed IS FALSE THEN ap_account_id END) mtu_unsubscribed
	FROM analytics.dm_mtu_monthly dmm 
	WHERE mtu_month = DATE_TRUNC('month', NOW())
	GROUP BY 1,2
)	, base AS (
	SELECT
		t.signup_hostcountry 
		, t.ap_account_id 
		, COUNT(DISTINCT t.ap_account_id) trader_count
		, SUM(t.amount_usd) trade_vol_usd
	FROM 
		analytics.trades_master t
		LEFT JOIN 
			analytics.users_master u 
			ON t.ap_account_id = u.ap_account_id 
		RIGHT JOIN 
			mtu_sub m 
			ON t.ap_account_id = m.ap_account_id
			AND m.mtu_unsubscribed = 1 
	WHERE 
		t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id NOT IN (38121))
		AND t.signup_hostcountry IN ('TH','AU','global','ID')
		AND DATE_TRUNC('month', t.created_at) >= '2021-09-01'
	GROUP BY
		1,2
	ORDER BY 1
)	, total_trade AS (
	SELECT 
		signup_hostcountry 
		, ap_account_id 
		, SUM(trade_vol_usd) trade_vol_usd
	FROM base 
	GROUP BY 1,2
)	, final_calc AS (
SELECT 
	signup_hostcountry 
--	, zipup_subscribed
	, SUM(trade_vol_usd) total_trade_vol_usd
	, COUNT(DISTINCT ap_account_id) trader_count
	, AVG(trade_vol_usd) avg_trade_vol_usd
FROM total_trade 
GROUP BY 1
)
SELECT 
	*
	, SUM(total_trade_vol_usd) OVER() total_trade_vol_usd
	, SUM(trader_count) OVER() total_trader_count
	, SUM(total_trade_vol_usd) OVER() / SUM(trader_count) OVER()::NUMERIC avg_trade_total
FROM final_calc
;


