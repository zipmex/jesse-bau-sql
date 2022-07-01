-- pcs list daily report 20211217
WITH pcs_id_base AS (
	SELECT 
		p.ap_account_id
		, p.email 
		, u.mobile_number 
		, u.first_name 
		, u.last_name 
		, u.signup_hostcountry 
		, u.created_at::date register_date
		, u.onfido_completed_at::date verified_date
		, u.last_traded_at::date last_traded_at
		, u.last_deposit_at::date last_deposit_at
		, u.zipup_subscribed_at::date zipup_subscribed_at
		, u.is_zipup_subscribed
	FROM bo_testing.pcs_id_20211213 p
		LEFT JOIN analytics.users_master u
			ON p.ap_account_id = u.ap_account_id 
	);

-- top 5 asset - daily trade volume
WITH monthly_trade AS (
	SELECT 
		DATE_TRUNC('month', created_at) created_at
		, product_1_symbol 
		, SUM(amount_usd) sum_trade_vol
	FROM 
		analytics.trades_master t
			RIGHT JOIN bo_testing.pcs_id_20211213 pi2 
				ON t.ap_account_id = pi2.ap_account_id 
	WHERE 
		DATE_TRUNC('day', t.created_at) >= DATE_TRUNC('month', NOW()) 
		AND DATE_TRUNC('day', t.created_at) < DATE_TRUNC('day', NOW())
		AND t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
		AND t.signup_hostcountry IN ('ID') -- ('TH','ID','AU','global')
	GROUP BY 1,2
	)	, top_10_month AS (
	SELECT 
		*
		, ROW_NUMBER () OVER(PARTITION BY created_at ORDER BY sum_trade_vol DESC) row_ 
	FROM monthly_trade
	)
SELECT
	DATE_TRUNC('day', t.created_at)::DATE traded_date 
	, DATE_TRUNC('week', t.created_at)::DATE traded_week 
	, t1.product_1_symbol
	, t.signup_hostcountry 
	, COUNT(DISTINCT t.order_id) "count_orders"
	, COUNT(DISTINCT t.trade_id) "count_trades"
	, SUM(t.quantity) "sum_coin_volume"
	, SUM(t.amount_usd) "sum_usd_volume" 
FROM top_10_month t1
	LEFT JOIN
		analytics.trades_master t
		ON t1.product_1_symbol = t.product_1_symbol
WHERE 
	DATE_TRUNC('day', t.created_at) >= DATE_TRUNC('month', NOW()) - '1 month'::INTERVAL 
	AND DATE_TRUNC('day', t.created_at) < DATE_TRUNC('day', NOW())
	AND t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
	AND t.signup_hostcountry IN ('ID') -- ('TH','ID','AU','global')
	AND t1.row_ <= 5
GROUP BY 1,2,3,4
ORDER BY 1
;


-- id verified user base
WITH pcs_id_base AS (
	SELECT 
		u.ap_account_id 
		, CASE WHEN pi2.ap_account_id IS NOT NULL THEN 1 ELSE 0 END AS is_pcs
		, up.email 
		, up.mobile_number 
		, up.first_name 
		, up.last_name 
		, u.signup_hostcountry 
		, u.created_at::date register_date
		, u.onfido_completed_at::date verified_date
		, u.last_traded_at::date last_traded_at
		, u.last_deposit_at::date last_deposit_at
		, u.zipup_subscribed_at::date zipup_subscribed_at
		, u.is_zipup_subscribed
		, zte.vip_tier 
	FROM 
		analytics.users_master u
		LEFT JOIN analytics_pii.users_pii up 
			ON u.user_id = up.user_id 
		LEFT JOIN mappings.commercial_pcs_id_account_id pi2 
			ON u.ap_account_id = pi2.ap_account_id::INT 
		LEFT JOIN analytics.zmt_tier_endofmonth zte 
			ON u.ap_account_id = zte.ap_account_id 
			AND DATE_TRUNC('month', zte.created_at) = DATE_TRUNC('month', NOW()) - '1 month'::INTERVAL 
	WHERE 
		u.signup_hostcountry = 'ID'
		AND level_increase_status = 'pass'
)	, base AS (
	SELECT DISTINCT 
		p.*
		, a.created_at 
		, a.symbol  
		, SUM(COALESCE (trade_wallet_amount, 0)) trade_wallet_amount
		, SUM(COALESCE (z_wallet_amount, 0)) z_wallet_amount
		, SUM(COALESCE (ziplock_amount, 0)) ziplock_amount
		, SUM(CASE	WHEN r.product_type = 1 THEN COALESCE (trade_wallet_amount, 0) * 1/r.price 
				WHEN r.product_type = 2 THEN COALESCE (trade_wallet_amount, 0) * r.price
				END) AS trade_wallet_amount_usd
		, SUM(COALESCE (z_wallet_amount, 0) * r.price) z_wallet_amount_usd
		, SUM(COALESCE (ziplock_amount, 0) * r.price) ziplock_amount_usd
	FROM 
		pcs_id_base p
		LEFT JOIN 
			analytics.wallets_balance_eod a 
			ON p.ap_account_id = a.ap_account_id 
			AND a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL
		LEFT JOIN 
			analytics.rates_master r 
			ON a.symbol = r.product_1_symbol
			AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
	ORDER BY 1 
)	, aum_snapshot AS (
	SELECT 
		ap_account_id
		, is_pcs
		, email 
		, mobile_number
		, first_name
		, last_name
		, signup_hostcountry
		, register_date
		, verified_date
		, last_traded_at
		, last_deposit_at
		, zipup_subscribed_at 
		, is_zipup_subscribed 
		, vip_tier
		, DATE_TRUNC('day', created_at) balance_at
		, SUM( COALESCE (trade_wallet_amount_usd, 0)) trade_wallet_amount_usd
		, SUM( COALESCE (z_wallet_amount_usd, 0)) z_wallet_amount_usd
		, SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
		, SUM( COALESCE (CASE WHEN is_zipup_subscribed = TRUE AND created_at >= DATE_TRUNC('day', zipup_subscribed_at) AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
					THEN
						(CASE 	WHEN created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
								WHEN created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
					END, 0)) AS zwallet_subscribed_usd
		, SUM( COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0) + COALESCE (ziplock_amount_usd, 0)) total_aum_usd
	FROM 
		base 
	GROUP BY 
		1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
	ORDER BY 
		1 
)	, zipmex_time AS (
	SELECT 
		a.*
		, EXTRACT(epoch FROM (DATE_TRUNC('day', NOW()) - register_date)) / 3600/ 24 day_from_register
		, EXTRACT(epoch FROM (DATE_TRUNC('day', NOW()) - verified_date)) / 3600/ 24 day_from_verified
		, EXTRACT(epoch FROM (DATE_TRUNC('day', NOW()) - last_traded_at)) / 3600/ 24 day_from_last_trade
		, EXTRACT(epoch FROM (DATE_TRUNC('day', NOW()) - last_deposit_at)) / 3600/ 24 day_from_last_deposit
		, CASE WHEN is_pcs = 0 AND (vip_tier IN ('vip3','vip4') OR total_aum_usd >= 25000) THEN TRUE ELSE FALSE END AS is_near_pcs
	FROM aum_snapshot a
)	, pcs_trade AS (
-- PCS trade data
	SELECT 
		p.ap_account_id 
		, COUNT(DISTINCT CASE WHEN created_at >= DATE_TRUNC('day', NOW()) - '7 day'::INTERVAL THEN product_1_symbol END) "count_7d_trade_asset"
		, COUNT(DISTINCT CASE WHEN created_at >= DATE_TRUNC('day', NOW()) - '30 day'::INTERVAL THEN product_1_symbol END) "count_30d_trade_asset"
		, COUNT(DISTINCT CASE WHEN created_at >= DATE_TRUNC('day', NOW()) - '90 day'::INTERVAL THEN product_1_symbol END) "count_90d_trade_asset"
		, COUNT(DISTINCT CASE WHEN created_at >= DATE_TRUNC('year', NOW()) THEN product_1_symbol END) "count_YTD_trade_asset"
		, SUM(CASE WHEN side = 'Buy' AND created_at >= DATE_TRUNC('day', NOW()) - '7 day'::INTERVAL THEN t.amount_usd END) "buy_7d_usd_volume"
		, SUM(CASE WHEN side = 'Buy' AND created_at >= DATE_TRUNC('day', NOW()) - '30 day'::INTERVAL THEN t.amount_usd END) "buy_30d_usd_volume"
		, SUM(CASE WHEN side = 'Buy' AND created_at >= DATE_TRUNC('day', NOW()) - '90 day'::INTERVAL THEN t.amount_usd END) "buy_90d_usd_volume"
		, SUM(CASE WHEN side = 'Buy' AND created_at >= DATE_TRUNC('year', NOW()) THEN t.amount_usd END) "buy_YTD_usd_volume"
		, SUM(CASE WHEN side = 'Sell' AND created_at >= DATE_TRUNC('day', NOW()) - '7 day'::INTERVAL THEN t.amount_usd END) "sell_7d_usd_volume"
		, SUM(CASE WHEN side = 'Sell' AND created_at >= DATE_TRUNC('day', NOW()) - '30 day'::INTERVAL THEN t.amount_usd END) "sell_30d_usd_volume"
		, SUM(CASE WHEN side = 'Sell' AND created_at >= DATE_TRUNC('day', NOW()) - '90 day'::INTERVAL THEN t.amount_usd END) "sell_90d_usd_volume"
		, SUM(CASE WHEN side = 'Sell' AND created_at >= DATE_TRUNC('year', NOW()) THEN t.amount_usd END) "sell_YTD_usd_volume"
		, SUM(CASE WHEN created_at >= DATE_TRUNC('day', NOW()) - '7 day'::INTERVAL THEN t.amount_usd END) "total_7d_usd_volume"
		, SUM(CASE WHEN created_at >= DATE_TRUNC('day', NOW()) - '30 day'::INTERVAL THEN t.amount_usd END) "total_30d_usd_volume"
		, SUM(CASE WHEN created_at >= DATE_TRUNC('day', NOW()) - '90 day'::INTERVAL THEN t.amount_usd END) "total_90d_usd_volume"
		, SUM(CASE WHEN created_at >= DATE_TRUNC('year', NOW()) THEN t.amount_usd END) "total_YTD_usd_volume"
	FROM  pcs_id_base p
		LEFT JOIN analytics.trades_master t
			ON t.ap_account_id = p.ap_account_id
			AND DATE_TRUNC('day', t.created_at) >= '2022-01-01 00:00:00' 
			AND DATE_TRUNC('day', t.created_at) < DATE_TRUNC('day', NOW())
	GROUP BY 1
)
SELECT 
	z.*
	, CASE WHEN day_from_register < 7 THEN 'A_<_7D'
			WHEN day_from_register >= 7 AND day_from_register < 30 THEN 'B_7_30D'
			WHEN day_from_register >= 30 AND day_from_register < 90 THEN 'C_30_90D'
			WHEN day_from_register >= 90 AND day_from_register < 180 THEN 'D_90_180D'
			WHEN day_from_register >= 180 THEN 'E_>_180D'
			END AS register_group
	, CASE WHEN day_from_verified < 7 THEN 'A_<_7D'
			WHEN day_from_verified >= 7 AND day_from_verified < 30 THEN 'B_7_30D'
			WHEN day_from_verified >= 30 AND day_from_verified < 90 THEN 'C_30_90D'
			WHEN day_from_verified >= 90 AND day_from_verified < 180 THEN 'D_90_180D'
			WHEN day_from_verified >= 180 THEN 'E_>_180D'
			END AS verified_group
	, CASE WHEN day_from_last_trade < 7 THEN 'A_active'
			WHEN day_from_last_trade >= 7 AND day_from_last_trade < 30 THEN 'B_>_7D'
			WHEN day_from_last_trade >= 30 AND day_from_last_trade < 90 THEN 'C_>_30D'
			WHEN day_from_last_trade >= 90 AND day_from_last_trade < 180 THEN 'D_>_90D'
			WHEN day_from_last_trade >= 180 THEN 'E_>_180D'
			END AS last_trade_group
	, CASE WHEN day_from_last_deposit < 7 THEN 'A_active'
			WHEN day_from_last_deposit >= 7 AND day_from_last_deposit < 30 THEN 'B_>_7D'
			WHEN day_from_last_deposit >= 30 AND day_from_last_deposit < 90 THEN 'C_>_30D'
			WHEN day_from_last_deposit >= 90 AND day_from_last_deposit < 180 THEN 'D_>_90D'
			WHEN day_from_last_deposit >= 180 THEN 'E_>_180D'
			END AS last_deposit_group
	, t.*
FROM zipmex_time z
	LEFT JOIN pcs_trade t 
		ON z.ap_account_id = t.ap_account_id
;


-- PCS daily acquisition growth
WITH pcs_id_base AS (
	SELECT 
		p.ap_account_id
		, p.email 
		, u.signup_hostcountry 
		, u.created_at::date register_date
		, u.onfido_completed_at::date verified_date
		, u.last_traded_at::date last_traded_at
		, u.last_deposit_at::date last_deposit_at
		, u.zipup_subscribed_at::date zipup_subscribed_at
		, u.is_zipup_subscribed
	FROM bo_testing.pcs_id_20211213 p
		LEFT JOIN analytics.users_master u
			ON p.ap_account_id = u.ap_account_id
)	, base_pcs AS (
	SELECT 
		pm.created_at::DATE created_at
		, COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', register_date)::DATE < pm.created_at THEN ap_account_id END) existing_pcs
		, COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', register_date)::DATE = pm.created_at THEN ap_account_id END) new_pcs
	FROM analytics.period_master pm
		LEFT JOIN pcs_id_base pb 
			ON pm.created_at >= DATE_TRUNC('day', register_date)
	WHERE pm."period" = 'day'
		AND pm.created_at >= '2019-07-08'
		AND pm.created_at < NOW()::DATE
	GROUP BY 1
)
SELECT
	*
	, existing_pcs + new_pcs total_pcs
	, CASE WHEN existing_pcs = 0 THEN 0 ELSE new_pcs/ existing_pcs::float END pcs_growth
FROM base_pcs
;


-- PCS weekly acquisition growth
WITH pcs_id_base AS (
	SELECT 
		p.ap_account_id
		, p.email 
		, u.signup_hostcountry 
		, u.created_at::date register_date
		, u.onfido_completed_at::date verified_date
		, u.last_traded_at::date last_traded_at
		, u.last_deposit_at::date last_deposit_at
		, u.zipup_subscribed_at::date zipup_subscribed_at
		, u.is_zipup_subscribed
	FROM bo_testing.pcs_id_20211213 p
		LEFT JOIN analytics.users_master u
			ON p.ap_account_id = u.ap_account_id
)	, base_pcs AS (
	SELECT 
		pm.created_at::DATE created_week
		, COUNT(DISTINCT CASE WHEN DATE_TRUNC('week', register_date)::DATE < pm.created_at THEN ap_account_id END) existing_pcs
		, COUNT(DISTINCT CASE WHEN DATE_TRUNC('week', register_date)::DATE = pm.created_at THEN ap_account_id END) new_pcs
	FROM analytics.period_master pm
		LEFT JOIN pcs_id_base pb 
			ON pm.created_at >= DATE_TRUNC('week', register_date)
	WHERE pm."period" = 'week'
		AND pm.created_at >= '2019-07-08'
		AND pm.created_at < NOW()::DATE
	GROUP BY 1
)
SELECT
	*
	, existing_pcs + new_pcs total_pcs
	, CASE WHEN existing_pcs = 0 THEN 0 ELSE new_pcs/ existing_pcs::float END pcs_growth
FROM base_pcs
;


-- PCS monthly acquisition growth
WITH pcs_id_base AS (
	SELECT 
		p.ap_account_id
		, p.email 
		, u.signup_hostcountry 
		, u.created_at::date register_date
		, u.onfido_completed_at::date verified_date
		, u.last_traded_at::date last_traded_at
		, u.last_deposit_at::date last_deposit_at
		, u.zipup_subscribed_at::date zipup_subscribed_at
		, u.is_zipup_subscribed
	FROM bo_testing.pcs_id_20211213 p
		LEFT JOIN analytics.users_master u
			ON p.ap_account_id = u.ap_account_id
)	, base_pcs AS (
	SELECT 
		pm.created_at::DATE created_month
		, COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', register_date)::DATE < pm.created_at THEN ap_account_id END) existing_pcs
		, COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', register_date)::DATE = pm.created_at THEN ap_account_id END) new_pcs
	FROM analytics.period_master pm
		LEFT JOIN pcs_id_base pb 
			ON pm.created_at >= DATE_TRUNC('month', register_date)
	WHERE pm."period" = 'month'
		AND pm.created_at >= '2019-07-08'
		AND pm.created_at < NOW()::DATE
	GROUP BY 1
)
SELECT
	*
	, existing_pcs + new_pcs total_pcs
	, CASE WHEN existing_pcs = 0 THEN 0 ELSE new_pcs/ existing_pcs::float END pcs_growth
FROM base_pcs
;


-- PCS daily trade growth
WITH pcs_id_base AS (
	SELECT 
		p.ap_account_id::INT
		, p.email 
		, u.signup_hostcountry 
		, u.created_at::date register_date
		, u.onfido_completed_at::date verified_date
		, u.last_traded_at::date last_traded_at
		, u.last_deposit_at::date last_deposit_at
		, u.zipup_subscribed_at::date zipup_subscribed_at
		, u.is_zipup_subscribed
	FROM bo_testing.pcs_id_20211213 p
		LEFT JOIN analytics.users_master u
			ON p.ap_account_id::INT = u.ap_account_id 
)	, daily_trade AS (
	SELECT 
		p.signup_hostcountry 
		, DATE_TRUNC('day', t.created_at)::DATE traded_week
		, COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', register_date)::DATE = DATE_TRUNC('day', t.created_at)::DATE THEN p.ap_account_id END) AS new_pcs
		, COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', register_date)::DATE < DATE_TRUNC('day', t.created_at)::DATE THEN p.ap_account_id END) AS existing_pcs
		, COALESCE (SUM( CASE WHEN DATE_TRUNC('day', register_date)::DATE = DATE_TRUNC('day', t.created_at)::DATE THEN t.amount_usd END), 0) AS new_pcs_trade_usd
		, SUM( CASE WHEN DATE_TRUNC('day', register_date)::DATE < DATE_TRUNC('day', t.created_at)::DATE THEN t.amount_usd END) AS existing_pcs_trade_usd
	FROM pcs_id_base p
		LEFT JOIN analytics.trades_master t
			ON t.ap_account_id = p.ap_account_id
			AND DATE_TRUNC('day', t.created_at) >= '2019-07-08 00:00:00' 
			AND DATE_TRUNC('day', t.created_at) < DATE_TRUNC('day', NOW())
	GROUP BY 1,2
)	, base_trade_1 AS (
	SELECT 
		pm.created_at::DATE created_at
		, wt.*
		, COALESCE (new_pcs, 0) + existing_pcs total_pcs_trader
		, COALESCE (new_pcs_trade_usd, 0) + existing_pcs_trade_usd total_pcs_trade_usd
	FROM analytics.period_master pm 
		LEFT JOIN daily_trade wt 
			ON pm.created_at = wt.traded_week
	WHERE 
		pm."period" = 'day'
		AND pm.created_at >= '2019-07-09'
		AND pm.created_at < NOW()::DATE
)	, base_trade_lag AS (
	SELECT 
		*
		, LAG(total_pcs_trade_usd) OVER(PARTITION BY signup_hostcountry ORDER BY created_at) previous_day_trade
	FROM base_trade_1	
	ORDER BY 1
)
SELECT 
	*
	, (total_pcs_trade_usd - COALESCE (previous_day_trade, 0)) / previous_day_trade AS daily_trade_growth
	, SUM(total_pcs_trade_usd) OVER(PARTITION BY DATE_TRUNC('month', created_at) ORDER BY created_at) monthly_cumulative_trade
FROM base_trade_lag
;


-- PCS weekly trade growth
WITH pcs_id_base AS (
	SELECT 
		p.ap_account_id::INT 
		, p.email 
		, u.signup_hostcountry 
		, u.created_at::date register_date
		, u.onfido_completed_at::date verified_date
		, u.last_traded_at::date last_traded_at
		, u.last_deposit_at::date last_deposit_at
		, u.zipup_subscribed_at::date zipup_subscribed_at
		, u.is_zipup_subscribed
	FROM bo_testing.pcs_id_20211213 p
		LEFT JOIN analytics.users_master u
			ON p.ap_account_id::INT = u.ap_account_id 
)	, weekly_trade AS (
	SELECT 
		p.signup_hostcountry 
		, DATE_TRUNC('week', t.created_at)::DATE traded_week
		, COUNT(DISTINCT CASE WHEN DATE_TRUNC('week', register_date)::DATE = DATE_TRUNC('week', t.created_at)::DATE THEN p.ap_account_id END) AS new_pcs
		, COUNT(DISTINCT CASE WHEN DATE_TRUNC('week', register_date)::DATE < DATE_TRUNC('week', t.created_at)::DATE THEN p.ap_account_id END) AS existing_pcs
		, COALESCE (SUM( CASE WHEN DATE_TRUNC('week', register_date)::DATE = DATE_TRUNC('week', t.created_at)::DATE THEN t.amount_usd END), 0) AS new_pcs_trade_usd
		, SUM( CASE WHEN DATE_TRUNC('week', register_date)::DATE < DATE_TRUNC('week', t.created_at)::DATE THEN t.amount_usd END) AS existing_pcs_trade_usd
	FROM pcs_id_base p
		LEFT JOIN analytics.trades_master t
			ON t.ap_account_id = p.ap_account_id
			AND DATE_TRUNC('day', t.created_at) >= '2019-07-08 00:00:00' 
			AND DATE_TRUNC('day', t.created_at) < DATE_TRUNC('day', NOW())
	GROUP BY 1,2
)	, base_trade_1 AS (
	SELECT 
		pm.created_at::DATE created_week
		, wt.*
		, COALESCE (new_pcs, 0) + existing_pcs total_pcs_trader
		, COALESCE (new_pcs_trade_usd, 0) + existing_pcs_trade_usd total_pcs_trade_usd
	FROM analytics.period_master pm 
		LEFT JOIN weekly_trade wt 
			ON pm.created_at = wt.traded_week
	WHERE 
		pm."period" = 'week'
		AND pm.created_at >= '2019-07-09'
		AND pm.created_at < NOW()::DATE
)	, base_trade_lag AS (
	SELECT 
		*
		, LAG(total_pcs_trade_usd) OVER(PARTITION BY signup_hostcountry ORDER BY created_week) previous_week_trade
	FROM base_trade_1	
	ORDER BY 1
)
SELECT 
	*
	, (total_pcs_trade_usd - COALESCE (previous_week_trade, 0)) / previous_week_trade AS weekly_trade_growth
	, SUM(total_pcs_trade_usd) OVER(PARTITION BY DATE_TRUNC('month', created_week) ORDER BY created_week) monthly_cumulative_trade
FROM base_trade_lag
;


-- PCS monthly trade growth
WITH pcs_id_base AS (
	SELECT 
		p.ap_account_id::INT
		, p.email 
		, u.signup_hostcountry 
		, u.created_at::date register_date
		, u.onfido_completed_at::date verified_date
		, u.last_traded_at::date last_traded_at
		, u.last_deposit_at::date last_deposit_at
		, u.zipup_subscribed_at::date zipup_subscribed_at
		, u.is_zipup_subscribed
	FROM mappings.commercial_pcs_id_account_id p
		LEFT JOIN analytics.users_master u
			ON p.ap_account_id::INT = u.ap_account_id 
)	, base_trade AS (
	SELECT 
		p.signup_hostcountry 
		, DATE_TRUNC('month', t.created_at)::DATE traded_at
		, COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', register_date)::DATE = DATE_TRUNC('month', t.created_at)::DATE THEN p.ap_account_id END) AS new_pcs
		, COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', register_date)::DATE < DATE_TRUNC('month', t.created_at)::DATE THEN p.ap_account_id END) AS existing_pcs
		, COALESCE (SUM( CASE WHEN DATE_TRUNC('month', register_date)::DATE = DATE_TRUNC('month', t.created_at)::DATE THEN t.amount_usd END), 0) AS new_pcs_trade_usd
		, SUM( CASE WHEN DATE_TRUNC('month', register_date)::DATE < DATE_TRUNC('month', t.created_at)::DATE THEN t.amount_usd END) AS existing_pcs_trade_usd
	FROM pcs_id_base p
		LEFT JOIN analytics.trades_master t
			ON t.ap_account_id = p.ap_account_id
			AND DATE_TRUNC('day', t.created_at) >= '2019-07-08 00:00:00' 
			AND DATE_TRUNC('day', t.created_at) < DATE_TRUNC('day', NOW())
	GROUP BY 1,2
)	, base_trade_1 AS (
	SELECT 
		pm.created_at::DATE created_at
		, wt.*
		, COALESCE (new_pcs, 0) + existing_pcs total_pcs_trader
		, COALESCE (new_pcs_trade_usd, 0) + existing_pcs_trade_usd total_pcs_trade_usd
	FROM analytics.period_master pm 
		LEFT JOIN base_trade wt 
			ON pm.created_at = wt.traded_at
	WHERE 
		pm."period" = 'month'
		AND pm.created_at >= '2022-05-01'
		AND pm.created_at < NOW()::DATE
)	, base_trade_lag AS (
	SELECT 
		*
		, LAG(total_pcs_trade_usd) OVER(PARTITION BY signup_hostcountry ORDER BY created_at) previous_month_trade
	FROM base_trade_1	
	ORDER BY 1
)
SELECT 
	*
	, (total_pcs_trade_usd - COALESCE (previous_month_trade, 0)) / previous_month_trade AS monthly_trade_growth
	, SUM(total_pcs_trade_usd) OVER(PARTITION BY DATE_TRUNC('month', created_at) ORDER BY created_at) monthly_cumulative_trade
FROM base_trade_lag
;


-- PCS daily deposit growth
WITH pcs_id_base AS (
	SELECT 
		p.ap_account_id
		, p.email 
		, u.signup_hostcountry 
		, u.created_at::date register_date
		, u.onfido_completed_at::date verified_date
		, u.last_traded_at::date last_traded_at
		, u.last_deposit_at::date last_deposit_at
		, u.zipup_subscribed_at::date zipup_subscribed_at
		, u.is_zipup_subscribed
	FROM bo_testing.pcs_id_20211213 p
		LEFT JOIN analytics.users_master u
			ON p.ap_account_id = u.ap_account_id 
)	, base_deposit AS (
	SELECT 
		p.signup_hostcountry 
		, DATE_TRUNC('day', t.created_at)::DATE deposit_at
		, COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', register_date)::DATE = DATE_TRUNC('day', t.created_at)::DATE THEN p.ap_account_id END) AS new_pcs
		, COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', register_date)::DATE < DATE_TRUNC('day', t.created_at)::DATE THEN p.ap_account_id END) AS existing_pcs
		, SUM( CASE WHEN DATE_TRUNC('day', register_date)::DATE = DATE_TRUNC('day', t.created_at)::DATE THEN t.amount_usd END) AS new_pcs_deposit_usd
		, SUM( CASE WHEN DATE_TRUNC('day', register_date)::DATE < DATE_TRUNC('day', t.created_at)::DATE THEN t.amount_usd END) AS existing_pcs_deposit_usd
	FROM pcs_id_base p
		LEFT JOIN analytics.deposit_tickets_master t
			ON t.ap_account_id = p.ap_account_id
			AND DATE_TRUNC('day', t.created_at) >= '2019-07-08 00:00:00' 
			AND DATE_TRUNC('day', t.created_at) < DATE_TRUNC('day', NOW())
	GROUP BY 1,2
)	, base_deposit_1 AS (
	SELECT 
		pm.created_at::DATE created_at
		, wt.*
		, COALESCE (new_pcs, 0) + existing_pcs total_pcs_depositor
		, COALESCE (new_pcs_deposit_usd, 0) + existing_pcs_deposit_usd total_pcs_deposit_usd
	FROM analytics.period_master pm 
		LEFT JOIN base_deposit wt 
			ON pm.created_at = wt.deposit_at
	WHERE 
		pm."period" = 'day'
		AND pm.created_at >= '2019-07-09'
		AND pm.created_at < NOW()::DATE
)	, base_deposit_lag AS (
	SELECT 
		*
		, LAG(total_pcs_deposit_usd) OVER(PARTITION BY signup_hostcountry ORDER BY created_at) previous_week_deposit
	FROM base_deposit_1	
	ORDER BY 1
)
SELECT 
	*
	, (total_pcs_deposit_usd - COALESCE (previous_week_deposit, 0)) / previous_week_deposit AS weekly_deposit_growth
	, SUM(total_pcs_deposit_usd) OVER(PARTITION BY DATE_TRUNC('month', created_at) ORDER BY created_at) monthly_cumulative_deposit
FROM base_deposit_lag
;


-- PCS weekly deposit growth
WITH pcs_id_base AS (
	SELECT 
		p.ap_account_id
		, p.email 
		, u.signup_hostcountry 
		, u.created_at::date register_date
		, u.onfido_completed_at::date verified_date
		, u.last_traded_at::date last_traded_at
		, u.last_deposit_at::date last_deposit_at
		, u.zipup_subscribed_at::date zipup_subscribed_at
		, u.is_zipup_subscribed
	FROM bo_testing.pcs_id_20211213 p
		LEFT JOIN analytics.users_master u
			ON p.ap_account_id = u.ap_account_id 
)	, base_deposit AS (
	SELECT 
		p.signup_hostcountry 
		, DATE_TRUNC('week', t.created_at)::DATE deposit_at
		, COUNT(DISTINCT CASE WHEN DATE_TRUNC('week', register_date)::DATE = DATE_TRUNC('week', t.created_at)::DATE THEN p.ap_account_id END) AS new_pcs
		, COUNT(DISTINCT CASE WHEN DATE_TRUNC('week', register_date)::DATE < DATE_TRUNC('week', t.created_at)::DATE THEN p.ap_account_id END) AS existing_pcs
		, SUM( CASE WHEN DATE_TRUNC('week', register_date)::DATE = DATE_TRUNC('week', t.created_at)::DATE THEN t.amount_usd END) AS new_pcs_deposit_usd
		, SUM( CASE WHEN DATE_TRUNC('week', register_date)::DATE < DATE_TRUNC('week', t.created_at)::DATE THEN t.amount_usd END) AS existing_pcs_deposit_usd
	FROM pcs_id_base p
		LEFT JOIN analytics.deposit_tickets_master t
			ON t.ap_account_id = p.ap_account_id
			AND DATE_TRUNC('day', t.created_at) >= '2019-07-08 00:00:00' 
			AND DATE_TRUNC('day', t.created_at) < DATE_TRUNC('day', NOW())
	GROUP BY 1,2
)	, base_deposit_1 AS (
	SELECT 
		pm.created_at::DATE created_at
		, wt.*
		, COALESCE (new_pcs, 0) + existing_pcs total_pcs_depositor
		, COALESCE (new_pcs_deposit_usd, 0) + existing_pcs_deposit_usd total_pcs_deposit_usd
	FROM analytics.period_master pm 
		LEFT JOIN base_deposit wt 
			ON pm.created_at = wt.deposit_at
	WHERE 
		pm."period" = 'week'
		AND pm.created_at >= '2019-07-09'
		AND pm.created_at < NOW()::DATE
)	, base_deposit_lag AS (
	SELECT 
		*
		, LAG(total_pcs_deposit_usd) OVER(PARTITION BY signup_hostcountry ORDER BY created_at) previous_week_deposit
	FROM base_deposit_1	
	ORDER BY 1
)
SELECT 
	*
	, (total_pcs_deposit_usd - COALESCE (previous_week_deposit, 0)) / previous_week_deposit AS weekly_deposit_growth
	, SUM(total_pcs_deposit_usd) OVER(PARTITION BY DATE_TRUNC('month', created_at) ORDER BY created_at) monthly_cumulative_deposit
FROM base_deposit_lag
;


-- PCS monthly deposit growth
WITH pcs_id_base AS (
	SELECT 
		p.ap_account_id
		, p.email 
		, u.signup_hostcountry 
		, u.created_at::date register_date
		, u.onfido_completed_at::date verified_date
		, u.last_traded_at::date last_traded_at
		, u.last_deposit_at::date last_deposit_at
		, u.zipup_subscribed_at::date zipup_subscribed_at
		, u.is_zipup_subscribed
	FROM bo_testing.pcs_id_20211213 p
		LEFT JOIN analytics.users_master u
			ON p.ap_account_id = u.ap_account_id 
)	, weekly_deposit AS (
	SELECT 
		p.signup_hostcountry 
		, DATE_TRUNC('month', t.created_at)::DATE deposit_at
		, COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', register_date)::DATE = DATE_TRUNC('month', t.created_at)::DATE THEN p.ap_account_id END) AS new_pcs
		, COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', register_date)::DATE < DATE_TRUNC('month', t.created_at)::DATE THEN p.ap_account_id END) AS existing_pcs
		, SUM( CASE WHEN DATE_TRUNC('month', register_date)::DATE = DATE_TRUNC('month', t.created_at)::DATE THEN t.amount_usd END) AS new_pcs_deposit_usd
		, SUM( CASE WHEN DATE_TRUNC('month', register_date)::DATE < DATE_TRUNC('month', t.created_at)::DATE THEN t.amount_usd END) AS existing_pcs_deposit_usd
	FROM pcs_id_base p
		LEFT JOIN analytics.deposit_tickets_master t
			ON t.ap_account_id = p.ap_account_id
			AND DATE_TRUNC('day', t.created_at) >= '2019-07-08 00:00:00' 
			AND DATE_TRUNC('day', t.created_at) < DATE_TRUNC('day', NOW())
	GROUP BY 1,2
)	, base_deposit_1 AS (
	SELECT 
		pm.created_at::DATE created_at
		, wt.*
		, COALESCE (new_pcs, 0) + existing_pcs total_pcs_depositor
		, COALESCE (new_pcs_deposit_usd, 0) + existing_pcs_deposit_usd total_pcs_deposit_usd
	FROM analytics.period_master pm 
		LEFT JOIN weekly_deposit wt 
			ON pm.created_at = wt.deposit_at
	WHERE 
		pm."period" = 'month'
		AND pm.created_at >= '2019-07-08'
		AND pm.created_at < NOW()::DATE
)	, base_deposit_lag AS (
	SELECT 
		*
		, LAG(total_pcs_deposit_usd) OVER(PARTITION BY signup_hostcountry ORDER BY created_at) previous_month_deposit
	FROM base_deposit_1	
	ORDER BY 1
)
SELECT 
	*
	, (total_pcs_deposit_usd - COALESCE (previous_month_deposit, 0)) / previous_month_deposit AS monthly_deposit_growth
	, SUM(total_pcs_deposit_usd) OVER(PARTITION BY DATE_TRUNC('month', created_at) ORDER BY created_at) monthly_cumulative_deposit
FROM base_deposit_lag
;


-- PCS daily withdraw growth
WITH pcs_id_base AS (
	SELECT 
		p.ap_account_id
		, p.email 
		, u.signup_hostcountry 
		, u.created_at::date register_date
		, u.onfido_completed_at::date verified_date
		, u.last_traded_at::date last_traded_at
		, u.last_deposit_at::date last_deposit_at
		, u.zipup_subscribed_at::date zipup_subscribed_at
		, u.is_zipup_subscribed
	FROM bo_testing.pcs_id_20211213 p
		LEFT JOIN analytics.users_master u
			ON p.ap_account_id = u.ap_account_id 
)	, base_withdraw AS (
	SELECT 
		p.signup_hostcountry 
		, DATE_TRUNC('day', t.created_at)::DATE withdraw_at
		, COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', register_date)::DATE = DATE_TRUNC('day', t.created_at)::DATE THEN p.ap_account_id END) AS new_pcs
		, COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', register_date)::DATE < DATE_TRUNC('day', t.created_at)::DATE THEN p.ap_account_id END) AS existing_pcs
		, SUM( CASE WHEN DATE_TRUNC('day', register_date)::DATE = DATE_TRUNC('day', t.created_at)::DATE THEN t.amount_usd END) AS new_pcs_withdraw_usd
		, SUM( CASE WHEN DATE_TRUNC('day', register_date)::DATE < DATE_TRUNC('day', t.created_at)::DATE THEN t.amount_usd END) AS existing_pcs_withdraw_usd
	FROM pcs_id_base p
		LEFT JOIN analytics.withdraw_tickets_master t
			ON t.ap_account_id = p.ap_account_id
			AND DATE_TRUNC('day', t.created_at) >= '2019-07-08 00:00:00' 
			AND DATE_TRUNC('day', t.created_at) < DATE_TRUNC('day', NOW())
	GROUP BY 1,2
)	, base_withdraw_1 AS (
	SELECT 
		pm.created_at::DATE created_at
		, wt.*
		, COALESCE (new_pcs, 0) + existing_pcs total_pcs_withdrawer
		, COALESCE (new_pcs_withdraw_usd, 0) + existing_pcs_withdraw_usd total_pcs_withdraw_usd
	FROM analytics.period_master pm 
		LEFT JOIN base_withdraw wt 
			ON pm.created_at = wt.withdraw_at
	WHERE 
		pm."period" = 'day'
		AND pm.created_at >= '2019-07-08'
		AND pm.created_at < NOW()::DATE
)	, base_withdraw_lag AS (
	SELECT 
		*
		, LAG(total_pcs_withdraw_usd) OVER(PARTITION BY signup_hostcountry ORDER BY created_at) previous_day_withdraw
	FROM base_withdraw_1	
	ORDER BY 1
)
SELECT 
	*
	, (total_pcs_withdraw_usd - COALESCE (previous_day_withdraw, 0)) / previous_day_withdraw AS daily_withdraw_growth
	, SUM(total_pcs_withdraw_usd) OVER(PARTITION BY DATE_TRUNC('month', created_at) ORDER BY created_at) monthly_cumulative_withdraw
FROM base_withdraw_lag
;


-- PCS weekly withdraw growth
WITH pcs_id_base AS (
	SELECT 
		p.ap_account_id
		, p.email 
		, u.signup_hostcountry 
		, u.created_at::date register_date
		, u.onfido_completed_at::date verified_date
		, u.last_traded_at::date last_traded_at
		, u.last_deposit_at::date last_deposit_at
		, u.zipup_subscribed_at::date zipup_subscribed_at
		, u.is_zipup_subscribed
	FROM bo_testing.pcs_id_20211213 p
		LEFT JOIN analytics.users_master u
			ON p.ap_account_id = u.ap_account_id 
)	, base_withdraw AS (
	SELECT 
		p.signup_hostcountry 
		, DATE_TRUNC('week', t.created_at)::DATE withdraw_at
		, COUNT(DISTINCT CASE WHEN DATE_TRUNC('week', register_date)::DATE = DATE_TRUNC('week', t.created_at)::DATE THEN p.ap_account_id END) AS new_pcs
		, COUNT(DISTINCT CASE WHEN DATE_TRUNC('week', register_date)::DATE < DATE_TRUNC('week', t.created_at)::DATE THEN p.ap_account_id END) AS existing_pcs
		, SUM( CASE WHEN DATE_TRUNC('week', register_date)::DATE = DATE_TRUNC('week', t.created_at)::DATE THEN t.amount_usd END) AS new_pcs_withdraw_usd
		, SUM( CASE WHEN DATE_TRUNC('week', register_date)::DATE < DATE_TRUNC('week', t.created_at)::DATE THEN t.amount_usd END) AS existing_pcs_withdraw_usd
	FROM pcs_id_base p
		LEFT JOIN analytics.withdraw_tickets_master t
			ON t.ap_account_id = p.ap_account_id
			AND DATE_TRUNC('day', t.created_at) >= '2019-07-08 00:00:00' 
			AND DATE_TRUNC('day', t.created_at) < DATE_TRUNC('day', NOW())
	GROUP BY 1,2
)	, base_withdraw_1 AS (
	SELECT 
		pm.created_at::DATE created_at
		, wt.*
		, COALESCE (new_pcs, 0) + existing_pcs total_pcs_withdrawer
		, COALESCE (new_pcs_withdraw_usd, 0) + existing_pcs_withdraw_usd total_pcs_withdraw_usd
	FROM analytics.period_master pm 
		LEFT JOIN base_withdraw wt 
			ON pm.created_at = wt.withdraw_at
	WHERE 
		pm."period" = 'week'
		AND pm.created_at >= '2019-07-08'
		AND pm.created_at < NOW()::DATE
)	, base_withdraw_lag AS (
	SELECT 
		*
		, LAG(total_pcs_withdraw_usd) OVER(PARTITION BY signup_hostcountry ORDER BY created_at) previous_week_withdraw
	FROM base_withdraw_1	
	ORDER BY 1
)
SELECT 
	*
	, (total_pcs_withdraw_usd - COALESCE (previous_week_withdraw, 0)) / previous_week_withdraw AS weekly_withdraw_growth
	, SUM(total_pcs_withdraw_usd) OVER(PARTITION BY DATE_TRUNC('month', created_at) ORDER BY created_at) monthly_cumulative_withdraw
FROM base_withdraw_lag
;


-- PCS monthly withdraw growth
WITH pcs_id_base AS (
	SELECT 
		p.ap_account_id
		, p.email 
		, u.signup_hostcountry 
		, u.created_at::date register_date
		, u.onfido_completed_at::date verified_date
		, u.last_traded_at::date last_traded_at
		, u.last_deposit_at::date last_deposit_at
		, u.zipup_subscribed_at::date zipup_subscribed_at
		, u.is_zipup_subscribed
	FROM bo_testing.pcs_id_20211213 p
		LEFT JOIN analytics.users_master u
			ON p.ap_account_id = u.ap_account_id 
)	, base_withdraw AS (
	SELECT 
		p.signup_hostcountry 
		, DATE_TRUNC('month', t.created_at)::DATE withdraw_at
		, COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', register_date)::DATE = DATE_TRUNC('month', t.created_at)::DATE THEN p.ap_account_id END) AS new_pcs
		, COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', register_date)::DATE < DATE_TRUNC('month', t.created_at)::DATE THEN p.ap_account_id END) AS existing_pcs
		, SUM( CASE WHEN DATE_TRUNC('month', register_date)::DATE = DATE_TRUNC('month', t.created_at)::DATE THEN t.amount_usd END) AS new_pcs_withdraw_usd
		, SUM( CASE WHEN DATE_TRUNC('month', register_date)::DATE < DATE_TRUNC('month', t.created_at)::DATE THEN t.amount_usd END) AS existing_pcs_withdraw_usd
	FROM pcs_id_base p
		LEFT JOIN analytics.withdraw_tickets_master t
			ON t.ap_account_id = p.ap_account_id
			AND DATE_TRUNC('day', t.created_at) >= '2019-07-08 00:00:00' 
			AND DATE_TRUNC('day', t.created_at) < DATE_TRUNC('day', NOW())
	GROUP BY 1,2
)	, base_withdraw_1 AS (
	SELECT 
		pm.created_at::DATE created_at
		, wt.*
		, COALESCE (new_pcs, 0) + existing_pcs total_pcs_withdrawer
		, COALESCE (new_pcs_withdraw_usd, 0) + existing_pcs_withdraw_usd total_pcs_withdraw_usd
	FROM analytics.period_master pm 
		LEFT JOIN base_withdraw wt 
			ON pm.created_at = wt.withdraw_at
	WHERE 
		pm."period" = 'month'
		AND pm.created_at >= '2019-07-08'
		AND pm.created_at < NOW()::DATE
)	, base_withdraw_lag AS (
	SELECT 
		*
		, LAG(total_pcs_withdraw_usd) OVER(PARTITION BY signup_hostcountry ORDER BY created_at) previous_month_withdraw
	FROM base_withdraw_1	
	ORDER BY 1
)
SELECT 
	*
	, (total_pcs_withdraw_usd - COALESCE (previous_month_withdraw, 0)) / previous_month_withdraw AS monthly_withdraw_growth
	, SUM(total_pcs_withdraw_usd) OVER(PARTITION BY DATE_TRUNC('month', created_at) ORDER BY created_at) monthly_cumulative_withdraw
FROM base_withdraw_lag
;


-- pcs AUM daily
WITH pcs_id_base AS (
	SELECT 
		p.ap_account_id
		, p.email 
		, u.signup_hostcountry 
		, u.created_at::date register_date
		, u.onfido_completed_at::date verified_date
		, u.last_traded_at::date last_traded_at
		, u.last_deposit_at::date last_deposit_at
		, u.zipup_subscribed_at::date zipup_subscribed_at
		, u.is_zipup_subscribed
	FROM bo_testing.pcs_id_20211213 p
		LEFT JOIN analytics.users_master u
			ON p.ap_account_id = u.ap_account_id 
)	, pcs_aum AS (
	SELECT DISTINCT 
		p.*
		, DATE_TRUNC('week', register_date)::timestamp register_week
		, DATE_TRUNC('week', verified_date)::timestamp verified_week
		, DATE_TRUNC('day', a.created_at)::timestamp created_at
		, a.symbol  
		, SUM(COALESCE (trade_wallet_amount, 0)) trade_wallet_amount
		, SUM(COALESCE (z_wallet_amount, 0)) z_wallet_amount
		, SUM(COALESCE (ziplock_amount, 0)) ziplock_amount
		, SUM(CASE	WHEN r.product_type = 1 THEN COALESCE (trade_wallet_amount, 0) * 1/r.price 
				WHEN r.product_type = 2 THEN COALESCE (trade_wallet_amount, 0) * r.price
				END) AS trade_wallet_amount_usd
		, SUM(COALESCE (z_wallet_amount, 0) * r.price) z_wallet_amount_usd
		, SUM(COALESCE (ziplock_amount, 0) * r.price) ziplock_amount_usd
	FROM 
		pcs_id_base p
		LEFT JOIN 
			analytics.wallets_balance_eod a 
			ON p.ap_account_id = a.ap_account_id 
			AND a.created_at >= DATE_TRUNC('month', NOW()::DATE - '1 day'::INTERVAL) - '3 month'::INTERVAL
		LEFT JOIN 
			analytics.rates_master r 
			ON a.symbol = r.product_1_symbol
			AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
	ORDER BY 1 
)	, aum_snapshot AS (
	SELECT 
		ap_account_id
		, signup_hostcountry
		, register_date
		, verified_date
		, created_at::timestamp  balance_at
		, SUM( COALESCE (trade_wallet_amount_usd, 0)) trade_wallet_amount_usd
		, SUM( COALESCE (z_wallet_amount_usd, 0)) z_wallet_amount_usd
		, SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
		, SUM( COALESCE (CASE WHEN is_zipup_subscribed = TRUE AND created_at >= DATE_TRUNC('day', zipup_subscribed_at) AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
					THEN
						(CASE 	WHEN created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
								WHEN created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
					END, 0)) AS zipup_amount_usd
		, SUM( COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0) + COALESCE (ziplock_amount_usd, 0)) total_aum_usd
	FROM 
		pcs_aum 
	GROUP BY 
		1,2,3,4,5
	ORDER BY 
		1 
)	, base_aum AS (
	SELECT 
		balance_at
		, signup_hostcountry
		, COUNT(DISTINCT CASE WHEN balance_at = register_date THEN ap_account_id END) new_pcs
		, COUNT(DISTINCT CASE WHEN balance_at > register_date THEN ap_account_id END) existing_pcs
		, COALESCE (SUM( CASE WHEN balance_at = register_date THEN total_aum_usd END), 0) new_pcs_total_aum_usd
		, SUM( CASE WHEN balance_at > register_date THEN total_aum_usd END) existing_pcs_total_aum_usd
		, COALESCE (SUM( CASE WHEN balance_at = register_date THEN COALESCE (zipup_amount_usd, 0) + COALESCE (ziplock_amount_usd, 0) END), 0) new_pcs_interest_bearing_usd
		, SUM( CASE WHEN balance_at > register_date THEN COALESCE (zipup_amount_usd, 0) + COALESCE (ziplock_amount_usd, 0) END) existing_pcs_interest_bearing_usd
	FROM aum_snapshot
	GROUP BY 1,2
)	, total_aum AS (
	SELECT 
		*
		, new_pcs + existing_pcs total_active_pcs
		, (new_pcs_total_aum_usd + existing_pcs_total_aum_usd)  / 1000000.0 total_aum_m
		, (new_pcs_interest_bearing_usd + existing_pcs_interest_bearing_usd)  / 1000000.0 total_interest_bearing_usd_m
	FROM base_aum
)	, base_aum_lag AS (
	SELECT
		*
		, LAG(total_aum_m) OVER(PARTITION BY signup_hostcountry ORDER BY balance_at) previous_total_aum
		, LAG(total_interest_bearing_usd_m) OVER(PARTITION BY signup_hostcountry ORDER BY balance_at) previous_total_interest_bearing
	FROM total_aum
)
SELECT
	*
	, CASE WHEN previous_total_aum = 0 THEN 0 ELSE total_aum_m / previous_total_aum END AS total_aum_growth
	, CASE WHEN previous_total_interest_bearing = 0 THEN 0 ELSE total_interest_bearing_usd_m / previous_total_interest_bearing END AS interest_bearing_growth
FROM base_aum_lag
;


-- pcs AUM weekly
WITH pcs_id_base AS (
	SELECT 
		p.ap_account_id
		, p.email 
		, u.signup_hostcountry 
		, u.created_at::date register_date
		, u.onfido_completed_at::date verified_date
		, u.last_traded_at::date last_traded_at
		, u.last_deposit_at::date last_deposit_at
		, u.zipup_subscribed_at::date zipup_subscribed_at
		, u.is_zipup_subscribed
	FROM bo_testing.pcs_id_20211213 p
		LEFT JOIN analytics.users_master u
			ON p.ap_account_id = u.ap_account_id 
)	, pcs_aum AS (
	SELECT DISTINCT 
		p.*
		, DATE_TRUNC('week', register_date)::timestamp register_week
		, DATE_TRUNC('week', verified_date)::timestamp verified_week
		, DATE_TRUNC('week', a.created_at)::timestamp created_at
		, a.symbol  
		, SUM(COALESCE (trade_wallet_amount, 0)) trade_wallet_amount
		, SUM(COALESCE (z_wallet_amount, 0)) z_wallet_amount
		, SUM(COALESCE (ziplock_amount, 0)) ziplock_amount
		, SUM(CASE	WHEN r.product_type = 1 THEN COALESCE (trade_wallet_amount, 0) * 1/r.price 
				WHEN r.product_type = 2 THEN COALESCE (trade_wallet_amount, 0) * r.price
				END) AS trade_wallet_amount_usd
		, SUM(COALESCE (z_wallet_amount, 0) * r.price) z_wallet_amount_usd
		, SUM(COALESCE (ziplock_amount, 0) * r.price) ziplock_amount_usd
	FROM 
		pcs_id_base p
		LEFT JOIN 
			analytics.wallets_balance_eod a 
			ON p.ap_account_id = a.ap_account_id 
			AND (a.created_at = DATE_TRUNC('week', a.created_at) + '6 day'::INTERVAL
				OR a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL)
		LEFT JOIN 
			analytics.rates_master r 
			ON a.symbol = r.product_1_symbol
			AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
	ORDER BY 1 
)	, aum_snapshot AS (
	SELECT 
		ap_account_id
		, signup_hostcountry
		, register_week
		, verified_week
		, created_at::timestamp  balance_at
		, SUM( COALESCE (trade_wallet_amount_usd, 0)) trade_wallet_amount_usd
		, SUM( COALESCE (z_wallet_amount_usd, 0)) z_wallet_amount_usd
		, SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
		, SUM( COALESCE (CASE WHEN is_zipup_subscribed = TRUE AND created_at >= DATE_TRUNC('day', zipup_subscribed_at) AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
					THEN
						(CASE 	WHEN created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
								WHEN created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
					END, 0)) AS zipup_amount_usd
		, SUM( COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0) + COALESCE (ziplock_amount_usd, 0)) total_aum_usd
	FROM 
		pcs_aum 
	GROUP BY 
		1,2,3,4,5
	ORDER BY 
		1 
)	, base_aum AS (
	SELECT 
		balance_at
		, signup_hostcountry
		, COUNT(DISTINCT CASE WHEN balance_at = register_week THEN ap_account_id END) new_pcs
		, COUNT(DISTINCT CASE WHEN balance_at > register_week THEN ap_account_id END) existing_pcs
		, COALESCE (SUM( CASE WHEN balance_at = register_week THEN total_aum_usd END), 0) new_pcs_total_aum_usd
		, SUM( CASE WHEN balance_at > register_week THEN total_aum_usd END) existing_pcs_total_aum_usd
		, COALESCE (SUM( CASE WHEN balance_at = register_week THEN COALESCE (zipup_amount_usd, 0) + COALESCE (ziplock_amount_usd, 0) END), 0) new_pcs_interest_bearing_usd
		, SUM( CASE WHEN balance_at > register_week THEN COALESCE (zipup_amount_usd, 0) + COALESCE (ziplock_amount_usd, 0) END) existing_pcs_interest_bearing_usd
	FROM aum_snapshot
	GROUP BY 1,2
)	, total_aum AS (
	SELECT 
		*
		, new_pcs + existing_pcs total_active_pcs
		, (new_pcs_total_aum_usd + existing_pcs_total_aum_usd)  / 1000000.0 total_aum_m
		, (new_pcs_interest_bearing_usd + existing_pcs_interest_bearing_usd)  / 1000000.0 total_interest_bearing_usd_m
	FROM base_aum
)	, base_aum_lag AS (
	SELECT
		*
		, LAG(total_aum_m) OVER(PARTITION BY signup_hostcountry ORDER BY balance_at) previous_total_aum
		, LAG(total_interest_bearing_usd_m) OVER(PARTITION BY signup_hostcountry ORDER BY balance_at) previous_total_interest_bearing
	FROM total_aum
)
SELECT
	*
	, CASE WHEN previous_total_aum = 0 THEN 0 ELSE total_aum_m / previous_total_aum END AS total_aum_growth
	, CASE WHEN previous_total_interest_bearing = 0 THEN 0 ELSE total_interest_bearing_usd_m / previous_total_interest_bearing END AS interest_bearing_growth
FROM base_aum_lag
;


-- pcs AUM monthly
WITH pcs_id_base AS (
	SELECT 
		p.ap_account_id
		, p.email 
		, u.signup_hostcountry 
		, u.created_at::date register_date
		, u.onfido_completed_at::date verified_date
		, u.last_traded_at::date last_traded_at
		, u.last_deposit_at::date last_deposit_at
		, u.zipup_subscribed_at::date zipup_subscribed_at
		, u.is_zipup_subscribed
	FROM bo_testing.pcs_id_20211213 p
		LEFT JOIN analytics.users_master u
			ON p.ap_account_id = u.ap_account_id 
)	, pcs_aum AS (
	SELECT DISTINCT 
		p.*
		, DATE_TRUNC('month', register_date)::timestamp register_week
		, DATE_TRUNC('month', verified_date)::timestamp verified_week
		, DATE_TRUNC('month', a.created_at)::timestamp created_at
		, a.symbol  
		, SUM(COALESCE (trade_wallet_amount, 0)) trade_wallet_amount
		, SUM(COALESCE (z_wallet_amount, 0)) z_wallet_amount
		, SUM(COALESCE (ziplock_amount, 0)) ziplock_amount
		, SUM(CASE	WHEN r.product_type = 1 THEN COALESCE (trade_wallet_amount, 0) * 1/r.price 
				WHEN r.product_type = 2 THEN COALESCE (trade_wallet_amount, 0) * r.price
				END) AS trade_wallet_amount_usd
		, SUM(COALESCE (z_wallet_amount, 0) * r.price) z_wallet_amount_usd
		, SUM(COALESCE (ziplock_amount, 0) * r.price) ziplock_amount_usd
	FROM 
		pcs_id_base p
		LEFT JOIN 
			analytics.wallets_balance_eod a 
			ON p.ap_account_id = a.ap_account_id 
			AND (a.created_at = DATE_TRUNC('month', a.created_at) + '1 month - 1 day'::INTERVAL
				OR a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL)
		LEFT JOIN 
			analytics.rates_master r 
			ON a.symbol = r.product_1_symbol
			AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
	ORDER BY 1 
)	, aum_snapshot AS (
	SELECT 
		ap_account_id
		, signup_hostcountry
		, register_week
		, verified_week
		, created_at::timestamp  balance_at
		, SUM( COALESCE (trade_wallet_amount_usd, 0)) trade_wallet_amount_usd
		, SUM( COALESCE (z_wallet_amount_usd, 0)) z_wallet_amount_usd
		, SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
		, SUM( COALESCE (CASE WHEN is_zipup_subscribed = TRUE AND created_at >= DATE_TRUNC('day', zipup_subscribed_at) AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
					THEN
						(CASE 	WHEN created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
								WHEN created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
					END, 0)) AS zipup_amount_usd
		, SUM( COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0) + COALESCE (ziplock_amount_usd, 0)) total_aum_usd
	FROM 
		pcs_aum 
	GROUP BY 
		1,2,3,4,5
	ORDER BY 
		1 
)	, base_aum AS (
	SELECT 
		balance_at
		, signup_hostcountry
		, COUNT(DISTINCT CASE WHEN balance_at = register_week THEN ap_account_id END) new_pcs
		, COUNT(DISTINCT CASE WHEN balance_at > register_week THEN ap_account_id END) existing_pcs
		, COALESCE (SUM( CASE WHEN balance_at = register_week THEN total_aum_usd END), 0) new_pcs_total_aum_usd
		, SUM( CASE WHEN balance_at > register_week THEN total_aum_usd END) existing_pcs_total_aum_usd
		, COALESCE (SUM( CASE WHEN balance_at = register_week THEN COALESCE (zipup_amount_usd, 0) + COALESCE (ziplock_amount_usd, 0) END), 0) new_pcs_interest_bearing_usd
		, SUM( CASE WHEN balance_at > register_week THEN COALESCE (zipup_amount_usd, 0) + COALESCE (ziplock_amount_usd, 0) END) existing_pcs_interest_bearing_usd
	FROM aum_snapshot
	GROUP BY 1,2
)	, total_aum AS (
	SELECT 
		*
		, new_pcs + existing_pcs total_active_pcs
		, (new_pcs_total_aum_usd + existing_pcs_total_aum_usd)  / 1000000.0 total_aum_m
		, (new_pcs_interest_bearing_usd + existing_pcs_interest_bearing_usd)  / 1000000.0 total_interest_bearing_usd_m
	FROM base_aum
)	, base_aum_lag AS (
	SELECT
		*
		, LAG(total_aum_m) OVER(PARTITION BY signup_hostcountry ORDER BY balance_at) previous_total_aum
		, LAG(total_interest_bearing_usd_m) OVER(PARTITION BY signup_hostcountry ORDER BY balance_at) previous_total_interest_bearing
	FROM total_aum
)
SELECT
	*
	, CASE WHEN previous_total_aum = 0 THEN 0 ELSE total_aum_m / previous_total_aum END AS total_aum_growth
	, CASE WHEN previous_total_interest_bearing = 0 THEN 0 ELSE total_interest_bearing_usd_m / previous_total_interest_bearing END AS interest_bearing_growth
FROM base_aum_lag
;



-- pcs id - avg aum
WITH base AS (
	SELECT 
		a.created_at 
		, CASE WHEN u.signup_hostcountry IS NULL THEN 'unknown' ELSE u.signup_hostcountry END AS signup_hostcountry
		, a.ap_account_id , up.email , u.user_id 
	-- filter nominee accounts from users_mapping
		, CASE WHEN a.ap_account_id IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id NOT IN (38121,496001))
				THEN TRUE ELSE FALSE END AS is_nominee 
	-- filter asset_manager account
		, CASE WHEN a.ap_account_id = 496001 THEN TRUE ELSE FALSE END AS is_asset_manager
	-- zipup subscribe status to identify zipup amount
		, u.zipup_subscribed_at , u.is_zipup_subscribed 
		, a.symbol 
		, r.price usd_rate 
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
		RIGHT JOIN mappings.commercial_pcs_id_account_id cpiai 
			ON a.ap_account_id = cpiai.ap_account_id::INT 
		LEFT JOIN 
			analytics.users_master u 
			ON a.ap_account_id = u.ap_account_id 
	-- get pii data 
		LEFT JOIN 
			analytics_pii.users_pii up 
			ON u.user_id = up.user_id 
	-- coin prices and exchange rates (USD)
		LEFT JOIN 
			analytics.rates_master r 
			ON a.symbol = r.product_1_symbol
			AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
	WHERE 
		a.created_at >= '2021-01-01' AND a.created_at < DATE_TRUNC('year', NOW())::DATE
		AND u.signup_hostcountry IN ('TH','ID','AU','global')
	-- snapshot by end of month or yesterday
--		AND ((a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
	-- exclude test products
		AND a.symbol NOT IN ('TST1','TST2')
	ORDER BY 1 DESC 
)
SELECT 
	DATE_TRUNC('month', created_at)::DATE created_at
	, signup_hostcountry
	, CASE 
			WHEN symbol = 'ZMT' THEN 'ZMT' 
			ELSE 'other' END AS asset_group
	, COUNT( DISTINCT ap_account_id) user_count
	, SUM( COALESCE (ziplock_amount_usd, 0)) +  SUM( COALESCE (CASE WHEN is_zipup_subscribed = TRUE AND created_at >= DATE_TRUNC('day', zipup_subscribed_at) AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
				THEN
					(CASE 	WHEN created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
							WHEN created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
				END, 0)) + SUM( COALESCE (zlaunch_amount_usd, 0)) AS interest_aum_usd
	, SUM( COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0) 
				+ COALESCE (ziplock_amount_usd, 0) + COALESCE (zlaunch_amount_usd, 0)) total_aum_usd
	, (SUM( COALESCE (ziplock_amount_usd, 0)) +  SUM( COALESCE (CASE WHEN is_zipup_subscribed = TRUE AND created_at >= DATE_TRUNC('day', zipup_subscribed_at) AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
				THEN
					(CASE 	WHEN created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
							WHEN created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
				END, 0)) + SUM( COALESCE (zlaunch_amount_usd, 0))) / COUNT( DISTINCT ap_account_id) AS avg_interest_aum_usd
	, (SUM( COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0) 
				+ COALESCE (ziplock_amount_usd, 0) + COALESCE (zlaunch_amount_usd, 0))) / COUNT( DISTINCT ap_account_id) avg_total_aum_usd
FROM 
	base 
WHERE 
	is_asset_manager = FALSE AND is_nominee = FALSE
GROUP BY 
	1,2,3
ORDER BY 
	1 
;



-- avg deposit 
WITH base_deposit AS (
	SELECT 
		DATE_TRUNC('month', d.created_at) created_at 
		, d.ap_account_id 
		, d.signup_hostcountry 
		, SUM(d.amount_usd) deposit_usd
	FROM 
		analytics.withdraw_tickets_master d 
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



-- 2022-05-12 pcs net money outflow
WITH deposit_sum AS (
	SELECT 
		created_at::DATE 
--		DATE_TRUNC('week', created_at)::DATE created_at
--		DATE_TRUNC('month', created_at)::DATE created_at
		, ap_account_id 
		, product_symbol 
		, SUM(amount) deposit_unit
		, SUM(amount_usd) deposit_usd
	FROM 
		analytics.deposit_tickets_master dtm 
	WHERE 
		status = 'FullyProcessed'
	GROUP BY 1,2,3
)	, withdraw_sum AS (
	SELECT 
		created_at::DATE 
--		DATE_TRUNC('week', created_at)::DATE created_at
--		DATE_TRUNC('month', created_at)::DATE created_at
		, ap_account_id 
		, product_symbol 
		, SUM(amount) withdraw_unit
		, SUM(amount_usd) withdraw_usd
	FROM 
		analytics.withdraw_tickets_master wtm  
	WHERE 
		status = 'FullyProcessed'
	GROUP BY 1,2,3
)	--, aum_sum AS (
	SELECT 
		wbe.created_at::DATE 
		, wbe.ap_account_id 
		, u.email
		, u.mobile_number
		, f.code account_type
		, CASE WHEN ult.tier_name IS NULL THEN 'no_zmt' ELSE ult.tier_name END AS vip_tier
		, wbe.symbol 
--		, wbe.trade_wallet_amount 
--		, wbe.z_wallet_amount 
--		, wbe.ziplock_amount 
--		, AVG(rm.price) avg_token_price 
--		, SUM( CASE WHEN rm.product_type = 1 THEN COALESCE (wbe.trade_wallet_amount , 0) * 1/rm.price 
--					WHEN rm.product_type = 2 THEN COALESCE (wbe.trade_wallet_amount , 0) * rm.price 
--					END) AS trade_wallet_usd 
--		, SUM( COALESCE (wbe.z_wallet_amount, 0) * rm.price) z_wallet_usd
--		, SUM( COALESCE (wbe.ziplock_amount , 0) * rm.price) ziplock_usd
--		, SUM( COALESCE (wbe.zlaunch_amount , 0) * rm.price) zlaunch_usd
--		, SUM( CASE WHEN rm.product_type = 1 THEN 
--					(COALESCE (wbe.trade_wallet_amount , 0) + COALESCE (wbe.z_wallet_amount, 0) 
--					+ COALESCE (wbe.ziplock_amount , 0) + COALESCE (wbe.zlaunch_amount , 0)) * 1/rm.price 
--					WHEN rm.product_type = 2 THEN 
--					(COALESCE (wbe.trade_wallet_amount , 0) + COALESCE (wbe.z_wallet_amount, 0) 
--					+ COALESCE (wbe.ziplock_amount , 0) + COALESCE (wbe.zlaunch_amount , 0)) * rm.price 
--					END) AS total_aum_usd 
		, SUM( COALESCE (ds.deposit_unit, 0)) deposit_unit
		, SUM( COALESCE (ws.withdraw_unit, 0)) withdraw_unit
		, SUM( COALESCE (ds.deposit_unit, 0)) - SUM( COALESCE (ws.withdraw_unit, 0)) net_inflow_unit
		, SUM( COALESCE (ds.deposit_usd, 0)) deposit_usd
		, SUM( COALESCE (ws.withdraw_usd, 0)) withdraw_usd
		, SUM( COALESCE (ds.deposit_usd, 0)) - SUM( COALESCE (ws.withdraw_usd, 0)) net_inflow_usd
	FROM 
		analytics.wallets_balance_eod wbe 
		RIGHT JOIN
			mappings.commercial_pcs_id_account_id cp 
			ON wbe.ap_account_id = cp.ap_account_id::INT 
		LEFT JOIN 
			analytics.users_master um 
			ON wbe.ap_account_id = um.ap_account_id
		LEFT JOIN 
			user_app_public.user_features uf 
			ON um.user_id = uf.user_id
		LEFT JOIN 
			user_app_public.features f 
			ON uf.feature_id = f.id
		LEFT JOIN 
			user_app_public.users u 
			ON um.user_id = u.id
		LEFT JOIN 
			zip_lock_service_public.user_loyalty_tiers ult 
			ON um.user_id = ult.user_id 
		LEFT JOIN 
			deposit_sum ds 
			ON wbe.created_at::DATE = ds.created_at::DATE 
--			ON DATE_TRUNC('week', wbe.created_at)::DATE = DATE_TRUNC('week', ds.created_at)::DATE 
--			ON DATE_TRUNC('month', wbe.created_at)::DATE = DATE_TRUNC('month', ds.created_at)::DATE 
			AND wbe.ap_account_id = ds.ap_account_id 
			AND wbe.symbol = ds.product_symbol
		LEFT JOIN 
			withdraw_sum ws 
			ON wbe.created_at::DATE = ws.created_at::DATE 
--			ON DATE_TRUNC('week', wbe.created_at)::DATE = DATE_TRUNC('week', ws.created_at)::DATE 
--			ON DATE_TRUNC('month', wbe.created_at)::DATE = DATE_TRUNC('month', ws.created_at)::DATE 
			AND wbe.ap_account_id = ws.ap_account_id 
			AND wbe.symbol = ws.product_symbol
		LEFT JOIN 
			analytics.rates_master rm 
			ON wbe.created_at::DATE = rm.created_at::DATE 
			AND wbe.symbol = rm.product_1_symbol 
	WHERE 
		wbe.created_at = NOW()::DATE - '1 day'::INTERVAL
		AND wbe.symbol IN ('USDC','USDT')
		AND withdraw_usd >= 20000
--	    AND ((wbe.created_at = DATE_TRUNC('week', wbe.created_at) + '1 week' - '1 day'::INTERVAL) OR (wbe.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
--	    AND ((wbe.created_at = DATE_TRUNC('month', wbe.created_at) + '1 month' - '1 day'::INTERVAL) OR (wbe.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
--		AND (f.code NOT IN ('INTERNAL','TEST') OR f.code IS NULL)
	GROUP BY 1,2,3,4,5,6,7
;



SELECT 
	cp.email 
	, DATE_TRUNC('month', created_at)::DATE traded_month
	, tm.product_1_symbol 
	, SUM(tm.amount_usd) trade_vol_usd
FROM analytics.trades_master tm 
	RIGHT JOIN 
		mappings.commercial_pcs_id_account_id cp
		ON tm.ap_account_id = cp.ap_account_id::INT 
WHERE 
	tm.created_at >= '2022-05-01'
GROUP BY 1,2,3
ORDER BY 4 DESC 
;

