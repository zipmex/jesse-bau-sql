-- pcs campaign net by 125SGD (all coins, exclude fee) earn 25SGD (in ZMT)
-- from 24/02/2022 21:00 to 25/02/2022 21:00 (GMT+8)

WITH hour_trade AS (
	SELECT 
		DATE_TRUNC('hour', tm.created_at + '8 hour'::INTERVAL)::timestamp traded_time_gmt8
		, tm.ap_account_id 
		, (um.created_at + '8 hour'::INTERVAL)::DATE registered_date_gmt8
		, CASE WHEN '2022-02-25'::DATE - (um.created_at + '8 hour'::INTERVAL)::DATE BETWEEN 0 AND 7 THEN 'A_<_8_day'
				WHEN '2022-02-25'::DATE - (um.created_at + '8 hour'::INTERVAL)::DATE BETWEEN 8 AND 30 THEN 'B_8-30_day'
				WHEN '2022-02-25'::DATE - (um.created_at + '8 hour'::INTERVAL)::DATE BETWEEN 31 AND 60 THEN 'C_31-60_day'
				WHEN '2022-02-25'::DATE - (um.created_at + '8 hour'::INTERVAL)::DATE BETWEEN 61 AND 90 THEN 'D_61-90_day'
				ELSE 'E_>_91_day'
				END AS register_group
		, um.user_id 
		, tm.signup_hostcountry 
		, COALESCE (SUM(CASE WHEN side = 'Buy' THEN tm.amount_usd END), 0) AS buy_amount_usd
		, COALESCE (SUM(CASE WHEN side = 'Sell' THEN tm.amount_usd END), 0) AS sell_amount_usd
		, COALESCE (SUM(CASE WHEN tm.side = 'Buy' THEN fm.fee_usd_amount END), 0) AS fee_buy_usd
		, COALESCE (SUM(CASE WHEN tm.side = 'Sell' THEN fm.fee_usd_amount END), 0) AS fee_sell_usd
	FROM analytics.trades_master tm 
	-- get fee amount
		LEFT JOIN analytics.fees_master fm 
			ON tm.execution_id = fm.fee_reference_id 
		LEFT JOIN analytics.users_master um 
			ON tm.ap_account_id = um.ap_account_id 
		LEFT JOIN analytics_pii.users_pii up 
			ON um.user_id = up.user_id 
	WHERE 
	-- filter global users
		tm.signup_hostcountry = 'global'
	-- campaign period 2022-02-24 21:00 to 2022-02-25 21:00 (GMT+8)
		AND tm.created_at + '8 hour'::INTERVAL >= '2022-02-24 21:00:00'
		AND tm.created_at + '8 hour'::INTERVAL < '2022-02-25 21:00:00'
        AND tm.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping) 
        AND up.email not like '%zipmex%'
	GROUP BY 1,2,3,4,5,6
	ORDER BY 1,2
)	, net_buy AS (
	SELECT 
		*
	-- net buy calculation
		, buy_amount_usd - (sell_amount_usd + fee_buy_usd + fee_sell_usd) net_buy_usd
	FROM hour_trade
	ORDER BY 2
)	, net_buy_sgd AS (
	SELECT 
		nb.*
	-- convert USD to SGD
		, net_buy_usd * rm.price net_buy_sgd
	FROM net_buy nb
	-- get SGD conversion rate
		LEFT JOIN analytics.rates_master rm 
			ON rm.product_1_symbol = 'SGD'
			AND nb.traded_time_gmt8::DATE = rm.created_at
)	, cumulative_net_buy AS (
	SELECT 
		*
	-- total net buy volume by hour
		, SUM(net_buy_usd) OVER(PARTITION BY ap_account_id ORDER BY traded_time_gmt8) cumulative_net_buy_usd
		, SUM(net_buy_sgd) OVER(PARTITION BY ap_account_id ORDER BY traded_time_gmt8) cumulative_net_buy_sgd
	FROM net_buy_sgd
)
SELECT
	*
	-- eligible user when cumulative net buy reached 125 SGD
	, CASE WHEN cumulative_net_buy_sgd >= 125 THEN TRUE ELSE FALSE END AS is_eligible
FROM cumulative_net_buy
ORDER BY is_eligible DESC 
;

