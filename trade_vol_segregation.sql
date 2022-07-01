----- trade vol by country
WITH register_base AS (
	SELECT 
		DATE_TRUNC('month', um2.created_at)::DATE register_month
		, COUNT( DISTINCT um2.user_id) register_count
	FROM analytics.users_master um2 
	WHERE 
		um2.signup_hostcountry IN ('TH','ID','AU','global')
		AND um2.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
	GROUP BY 1
)	, verified_base AS (
	SELECT 
		DATE_TRUNC('month', um2.onfido_completed_at)::DATE verified_month
		, COUNT( DISTINCT CASE WHEN is_verified IS TRUE THEN um2.user_id END) verify_count
	FROM analytics.users_master um2 
	WHERE 
		um2.signup_hostcountry IN ('TH','ID','AU','global')
		AND um2.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
	GROUP BY 1
)	, user_base AS (
	SELECT 
		register_month 
		, register_count
		, COALESCE (verify_count, 0) verify_count
		, SUM(register_count) OVER ( ORDER BY register_month) cumulative_register
		, CASE WHEN verified_month IS NULL THEN 0 
			ELSE SUM(verify_count) OVER ( ORDER BY verified_month) END cumulative_verify
	FROM register_base r
		LEFT JOIN verified_base v 
			ON r.register_month = verified_month
)	, pluang_trade_all AS (
	SELECT 
		DATE_TRUNC('day', q.created_at) created_at 
		, 'ID' signup_hostcountry
		, q.user_id
		, UPPER(LEFT(SPLIT_PART(q.instrument_id,'.',1),3)) product_1_symbol  
		, q.quote_id
		, q.order_id
		, q.side
		, CASE WHEN q.side IS NOT NULL THEN TRUE ELSE FALSE END AS is_organic_trade
		, UPPER(SPLIT_PART(q.instrument_id,'.',1)) instrument_symbol 
		, UPPER(RIGHT(SPLIT_PART(q.instrument_id,'.',1),3)) product_2_symbol 
		, q.quoted_quantity 
		, q.quoted_price 
		, SUM(q.quoted_quantity) "quantity"
		, SUM(q.quoted_value) "amount_idr"
		, SUM(q.quoted_value * 1/e.exchange_rate) amount_usd
	FROM 
		zipmex_otc_public.quote_statuses q
		LEFT JOIN 
			oms_data_public.exchange_rates e
			ON DATE_TRUNC('day', e.created_at) = DATE_TRUNC('day', q.created_at)
			AND UPPER(RIGHT(SPLIT_PART(q.instrument_id,'.',1),3))  = e.product_2_symbol
			AND e."source" = 'coinmarketcap'
	WHERE
		q.status='completed'
		AND q.user_id IN ('01F14GTKR63YS7QSPGCQDNVJRR')
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
	ORDER BY 1 DESC 
)	, pluang_trade AS (
	SELECT 
		DATE_TRUNC('day', created_at) created_at 
		, signup_hostcountry
		, 0101 ap_account_id 
		, 'pluang' user_type
		, product_1_symbol
		, side 
		, is_organic_trade 
		, CASE WHEN product_1_symbol = 'ZMT' THEN TRUE ELSE FALSE END AS is_zmt_trade
		, CASE WHEN user_id IN (SELECT DISTINCT ap_account_id::TEXT FROM mappings.commercial_is_whale) THEN TRUE ELSE FALSE END AS is_whale
		, FALSE is_july_gaming
		, COUNT(DISTINCT order_id) count_orders
		, COUNT(DISTINCT quote_id) count_trades 
		, SUM(quantity) quantity 
		, SUM(amount_usd) amount_usd
	FROM 
		pluang_trade_all
	GROUP BY 1,2,3,4,5,6,7,8,9
)	, zipmex_trade AS (
	SELECT
		DATE_TRUNC('day', t.created_at) created_at 
		, t.signup_hostcountry 
		, t.ap_account_id 
		, 'zipmex' user_type
		, t.product_1_symbol
		, t.side 
		, CASE WHEN t.counter_party IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE is_non_organic = TRUE) 
			THEN FALSE ELSE TRUE END "is_organic_trade" 
		, CASE WHEN product_1_id IN (16,50) THEN TRUE ELSE FALSE END AS is_zmt_trade
		, CASE WHEN t.ap_account_id IN (SELECT DISTINCT ap_account_id FROM mappings.commercial_is_whale) THEN TRUE ELSE FALSE END AS is_whale
		, CASE 	WHEN t.ap_account_id IN ('85191','73926','88108','152636','140459','140652','55796','56951','52826','54687')
					AND t.product_1_symbol IN ('USDC')
					AND DATE_TRUNC('day', t.created_at) >= '2021-07-01 07:00:00'
					AND DATE_TRUNC('day', t.created_at) < '2021-07-11 07:00:00'
					THEN TRUE ELSE FALSE 
				END AS is_july_gaming
		, COUNT(DISTINCT t.order_id) "count_orders"
		, COUNT(DISTINCT t.trade_id) "count_trades"
	--	, COUNT(DISTINCT t.execution_id) "count_executions"
		, SUM(t.quantity) "sum_coin_volume"
		, SUM(t.amount_usd) "sum_usd_trade_volume" 
	FROM 
		analytics.trades_master t
	WHERE 
		CASE WHEN t.created_at < '2022-05-05' THEN (t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping))
			ELSE (t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id NOT IN (38121)))
			END
		AND t.signup_hostcountry IN ('TH','ID','AU','global')
		AND DATE_TRUNC('day', t.created_at) >= '2022-03-01'
--		AND DATE_TRUNC('day', t.created_at) < DATE_TRUNC('month', NOW())
	GROUP BY 
		1,2,3,4,5,6,7,8,9
	ORDER BY 1,2,3
)	, all_trade AS (
	SELECT * FROM zipmex_trade
	UNION ALL
	SELECT * FROM pluang_trade
)	, temp_t AS (
SELECT 
	DATE_TRUNC('month', a.created_at)::DATE created_at 
	, a.signup_hostcountry 
	, a.ap_account_id 
	, user_type
	, COUNT(DISTINCT ap_account_id) count_traders
	, SUM( COALESCE(count_orders, 0) ) count_orders
	, SUM( COALESCE(count_trades, 0) ) count_trades
	, SUM( COALESCE(sum_coin_volume, 0)) sum_coin_volume 
	, SUM( COALESCE(sum_usd_trade_volume, 0)) sum_usd_trade_volume
FROM 
	all_trade a 
WHERE 
	DATE_TRUNC('day', a.created_at) >= DATE_TRUNC('month', NOW()) - '2 month'::INTERVAL
	AND DATE_TRUNC('day', a.created_at) < DATE_TRUNC('month', NOW())
	AND is_july_gaming IS FALSE
GROUP BY 
	1,2,3,4
ORDER BY 1 DESC
---- rank trade volume to get top 50 traders, count account id to get total trader and calculate trader attribution
)	, rank_trade AS (
	SELECT 
		created_at 
		, ap_account_id
		, count_orders
		, count_trades
		, sum_coin_volume
		, sum_usd_trade_volume
		, RANK() OVER(PARTITION BY created_at ORDER BY sum_usd_trade_volume DESC) rank_ 
		, 1.0 / COUNT(ap_account_id) OVER(PARTITION BY created_at) trader_attribution
		, 1.0 / cumulative_register register_attribution
		, CASE WHEN cumulative_verify = 0 THEN 0
				ELSE 1.0 / cumulative_verify END verify_attribution
	FROM temp_t t 
		LEFT JOIN user_base u 
		ON t.created_at = u.register_month 
--	WHERE is_july_gaming = FALSE 
---- calculate cumulative attribution of traders
)	, trade_percentage AS (
	SELECT 
		created_at 
		, ap_account_id
		, count_orders
		, count_trades
		, sum_coin_volume
		, sum_usd_trade_volume
	--	, SUM(sum_usd_trade_volume) OVER(PARTITION BY created_at ORDER BY sum_usd_trade_volume DESC) cumulative_trade_volume
		, rank_
		, trader_attribution 
		, register_attribution
		, verify_attribution
		, SUM(trader_attribution) OVER(PARTITION BY created_at ORDER BY sum_usd_trade_volume DESC) cumulative_attribution
		, SUM(register_attribution) OVER(PARTITION BY created_at ORDER BY sum_usd_trade_volume DESC) cumulative_register_attribution
		, SUM(verify_attribution) OVER(PARTITION BY created_at ORDER BY sum_usd_trade_volume DESC) cumulative_verify_attribution
	FROM rank_trade
---- SUM trade volume USD to get the result
)
SELECT
	created_at 
--	, signup_hostcountry
	, SUM( CASE WHEN rank_ <= 50 THEN sum_usd_trade_volume END) AS top50_usd_trade_volume
    , SUM( CASE WHEN cumulative_attribution <= 0.01 THEN sum_usd_trade_volume END) AS top1p_usd_trade_volume
	, SUM( CASE WHEN cumulative_attribution <= 0.001 THEN sum_usd_trade_volume END) AS top01p_usd_trade_volume
	, SUM( CASE WHEN cumulative_attribution > 0.001 AND cumulative_attribution <= 0.005 THEN sum_usd_trade_volume END) AS top05p_usd_trade_volume
	, SUM( CASE WHEN cumulative_attribution > 0.005 AND cumulative_attribution <= 0.01 THEN sum_usd_trade_volume END) AS top05to1p_usd_trade_volume
	, SUM( CASE WHEN cumulative_attribution > 0.01 AND cumulative_attribution <= 0.05 THEN sum_usd_trade_volume END) AS top2to5p_usd_trade_volume
	, SUM( CASE WHEN cumulative_attribution > 0.05 AND cumulative_attribution <= 0.1 THEN sum_usd_trade_volume END) AS top5to10p_usd_trade_volume
	, SUM( CASE WHEN cumulative_attribution > 0.1 AND cumulative_attribution <= 0.2 THEN sum_usd_trade_volume END) AS top10to20p_usd_trade_volume
	, SUM( CASE WHEN cumulative_attribution > 0.2 AND cumulative_attribution <= 0.5 THEN sum_usd_trade_volume END) AS top20to50p_usd_trade_volume
	, SUM( CASE WHEN cumulative_attribution > 0.5 AND cumulative_attribution <= 0.8 THEN sum_usd_trade_volume END) AS top50to80p_usd_trade_volume
	, SUM( CASE WHEN cumulative_attribution > 0.8 THEN sum_usd_trade_volume END) AS top80to100p_usd_trade_volume
	, SUM( CASE WHEN cumulative_register_attribution <= 0.01 THEN sum_usd_trade_volume END) AS top1p_reg_usd_trade_volume
	, SUM( CASE WHEN cumulative_register_attribution <= 0.001 THEN sum_usd_trade_volume END) AS top01p_reg_usd_trade_volume
	, SUM( CASE WHEN cumulative_register_attribution > 0.001 AND cumulative_register_attribution <= 0.005 THEN sum_usd_trade_volume END) AS top05p_reg_usd_trade_volume
	, SUM( CASE WHEN cumulative_register_attribution > 0.005 AND cumulative_register_attribution <= 0.01 THEN sum_usd_trade_volume END) AS top05to1p_reg_usd_trade_volume
	, SUM( CASE WHEN cumulative_register_attribution > 0.01 AND cumulative_register_attribution <= 0.05 THEN sum_usd_trade_volume END) AS top2to5p_reg_usd_trade_volume
	, SUM( CASE WHEN cumulative_register_attribution > 0.05 AND cumulative_register_attribution <= 0.1 THEN sum_usd_trade_volume END) AS top5to10p_reg_usd_trade_volume
	, SUM( CASE WHEN cumulative_register_attribution > 0.1 AND cumulative_register_attribution <= 0.2 THEN sum_usd_trade_volume END) AS top10to20p_reg_usd_trade_volume
	, SUM( CASE WHEN cumulative_register_attribution > 0.2 AND cumulative_register_attribution <= 0.5 THEN sum_usd_trade_volume END) AS top20to50p_reg_usd_trade_volume
	, SUM( CASE WHEN cumulative_register_attribution > 0.5 AND cumulative_register_attribution <= 0.8 THEN sum_usd_trade_volume END) AS top50to80p_reg_usd_trade_volume
	, SUM( CASE WHEN cumulative_register_attribution > 0.8 THEN sum_usd_trade_volume END) AS top80to100p_reg_usd_trade_volume
	, SUM( CASE WHEN cumulative_verify_attribution <= 0.01 THEN sum_usd_trade_volume END) AS top1p_ver_usd_trade_volume
	, SUM( CASE WHEN cumulative_verify_attribution <= 0.001 THEN sum_usd_trade_volume END) AS top01p_ver_usd_trade_volume
	, SUM( CASE WHEN cumulative_verify_attribution > 0.001 AND cumulative_verify_attribution <= 0.005 THEN sum_usd_trade_volume END) AS top05p_ver_usd_trade_volume
	, SUM( CASE WHEN cumulative_verify_attribution > 0.005 AND cumulative_verify_attribution <= 0.01 THEN sum_usd_trade_volume END) AS top05to1p_ver_usd_trade_volume
	, SUM( CASE WHEN cumulative_verify_attribution > 0.01 AND cumulative_verify_attribution <= 0.05 THEN sum_usd_trade_volume END) AS top2to5p_ver_usd_trade_volume
	, SUM( CASE WHEN cumulative_verify_attribution > 0.05 AND cumulative_verify_attribution <= 0.1 THEN sum_usd_trade_volume END) AS top5to10p_ver_usd_trade_volume
	, SUM( CASE WHEN cumulative_verify_attribution > 0.1 AND cumulative_verify_attribution <= 0.2 THEN sum_usd_trade_volume END) AS top10to20p_ver_usd_trade_volume
	, SUM( CASE WHEN cumulative_verify_attribution > 0.2 AND cumulative_verify_attribution <= 0.5 THEN sum_usd_trade_volume END) AS top20to50p_ver_usd_trade_volume
	, SUM( CASE WHEN cumulative_verify_attribution > 0.5 AND cumulative_verify_attribution <= 0.8 THEN sum_usd_trade_volume END) AS top50to80p_ver_usd_trade_volume
	, SUM( CASE WHEN cumulative_verify_attribution > 0.8 THEN sum_usd_trade_volume END) AS top80to100p_ver_usd_trade_volume
FROM trade_percentage
GROUP BY 1
;



