----- trade vol by country
WITH pluang_trade_all AS (
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
	--	AND DATE_TRUNC('day',q.created_at) >= '2021-01-01 00:00:00'
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
		, 'vip0' vip_tier
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
	GROUP BY 1,2,3,4,5,6,7,8,9,10
)	, zipmex_trade AS (
	SELECT
		DATE_TRUNC('day', t.created_at) created_at 
		, t.signup_hostcountry 
		, t.ap_account_id 
		, 'zipmex' user_type
		, t.product_1_symbol
		, t.side 
		, CASE WHEN cwts.rank_ <= 10 THEN 'top10' ELSE 
		       (CASE WHEN zte.vip_tier IS NULL THEN 'no_zmt' ELSE zte.vip_tier END)
		      END AS vip_tier
		, CASE WHEN t.counter_party IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE is_non_organic = TRUE) 
			THEN FALSE ELSE TRUE END "is_organic_trade" 
		, CASE WHEN product_1_id IN (16,50) THEN TRUE ELSE FALSE END AS is_zmt_trade
		, CASE WHEN cwts.is_whale IS NOT NULL THEN TRUE ELSE FALSE END AS is_whale
--		, CASE WHEN t.ap_account_id IN (SELECT DISTINCT ap_account_id FROM mappings.commercial_is_whale) THEN TRUE ELSE FALSE END AS is_whale
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
		LEFT JOIN analytics.users_master u
			ON t.ap_account_id = u.ap_account_id
		LEFT JOIN 
            mappings.commercial_whale_tagging_sample cwts 
            ON t.ap_account_id = cwts.ap_account_id 
            AND DATE_TRUNC('month', t.created_at)::DATE = cwts.created_at::DATE
		LEFT JOIN analytics.zmt_tier_endofmonth zte 
            ON t.ap_account_id = zte.ap_account_id 
            AND DATE_TRUNC('month', t.created_at)::DATE = DATE_TRUNC('month', zte.created_at)::DATE
	WHERE 
		CASE WHEN t.created_at < '2022-05-05' THEN (t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping))
			ELSE (t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id NOT IN (38121)))
			END
		AND t.signup_hostcountry IN ('TH','ID','AU','global')
	GROUP BY
		1,2,3,4,5,6,7,8,9,10
	ORDER BY 1,2,3
)	, all_trade AS (
	SELECT * FROM zipmex_trade
	UNION ALL
	SELECT * FROM pluang_trade
)--	, temp_t AS (
SELECT 
	DATE_TRUNC('month', a.created_at)::DATE created_at 
	, a.signup_hostcountry 
    , is_organic_trade
--    , vip_tier
--    , is_whale
--    , product_1_symbol
--    , is_zmt_trade 	
    , SUM( COALESCE(count_orders, 0) ) count_orders
	, SUM( COALESCE(count_trades, 0) ) count_trades
	, SUM( COALESCE(sum_coin_volume, 0)) sum_coin_volume 
	, SUM( COALESCE(sum_usd_trade_volume, 0)) sum_usd_trade_volume
FROM 
	all_trade a 
WHERE 
	DATE_TRUNC('day', a.created_at) >= '2022-01-01'
	AND DATE_TRUNC('day', a.created_at) < DATE_TRUNC('month', NOW())
	AND is_july_gaming = FALSE 
GROUP BY 
	1,2,3
ORDER BY 1
;

	-- july exclusion --
/*	, CASE 	WHEN t.ap_account_id IN ('85191','73926','88108','152636','140459','140652','55796','56951','52826','54687')
			AND t.product_1_symbol IN ('USDC')
			AND DATE_TRUNC('day', t.created_at) >= '2021-07-01 07:00:00'
			AND DATE_TRUNC('day', t.created_at) < '2021-07-11 07:00:00'
			THEN TRUE ELSE FALSE END AS is_july_gaming
	-- layer 1
--	, CASE WHEN product_1_symbol IN ('BTC','ETH','BNB',	'SOL',	'ADA',	'DOT',	'ATOM',	'LUNA',	'AVAX',	'ALGO',	'TRON',	'FTM',	'HBAR',	'ONE') THEN 'layer1' 
	-- 100 coins project
--	, CASE WHEN product_1_symbol IN ('ADA','UNI','THETA','SOL','DOT','MATIC','VET','AXS','BTT','PAX','ONT','DGB','ICX','ANKR','WRX','KNC'
--					,'FTT','ZRX','BAND','DOGE','ALGO','CAKE','MIOTA','NEO','KSM','XTZ','OGN','CTSI','SRM','ALPHA','BAL','BTS','JST','COTI'
--					,'AAVE','BNT','BNB','WAVES','XEM','DASH','ZIL','MANA','RLC','STORJ','WTC','XVG','LSK','EGLD','FTM','RVN','UMA','HOT','1INCH'
--					,'ONE','BAKE','NANO','REN','TFUEL','RUNE','HBAR','CHZ','SUSHI','GRT','SNX','ATOM','QTUM','TRX','KAVA','ANT','FIL','SLP'
--					,'XVS','ZEN','LRC','IOST','EOS','BAT','CRV') 
--				THEN TRUE ELSE FALSE END AS is_100coin
*/


SELECT
	DATE_TRUNC('day', t.created_at)::DATE created_at 
	, t.product_1_symbol 
	, SUM(t.quantity) "sum_coin_volume"
	, SUM(t.amount_usd) "sum_usd_trade_volume" 
FROM 
	analytics.trades_master t
WHERE 
	t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
	AND t.signup_hostcountry IN ('TH','ID','AU','global')
	AND DATE_TRUNC('day', t.created_at) >= DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL
	AND DATE_TRUNC('day', t.created_at) < DATE_TRUNC('day', NOW())
GROUP BY
	1,2
ORDER BY 1
;




SELECT
	DATE_TRUNC('day', t.created_at) created_at 
	, t.signup_hostcountry 
	, t.ap_account_id 
	, 'zipmex' user_type
	, t.product_1_symbol
	, t.side 
	, CASE WHEN cwts.rank_ <= 10 THEN 'top10' ELSE 
	       (CASE WHEN zte.vip_tier IS NULL THEN 'no_zmt' ELSE zte.vip_tier END)
	      END AS vip_tier
	, CASE WHEN t.counter_party IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE is_non_organic = TRUE) 
		THEN FALSE ELSE TRUE END "is_organic_trade" 
	, CASE WHEN product_1_id IN (16,50) THEN TRUE ELSE FALSE END AS is_zmt_trade
	, CASE WHEN cwts.is_whale IS NOT NULL THEN TRUE ELSE FALSE END AS is_whale
--		, CASE WHEN t.ap_account_id IN (SELECT DISTINCT ap_account_id FROM mappings.commercial_is_whale) THEN TRUE ELSE FALSE END AS is_whale
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
	LEFT JOIN analytics.users_master u
		ON t.ap_account_id = u.ap_account_id
	LEFT JOIN 
        mappings.commercial_whale_tagging_sample cwts 
        ON t.ap_account_id = cwts.ap_account_id 
        AND DATE_TRUNC('month', t.created_at)::DATE = cwts.created_at::DATE
	LEFT JOIN analytics.zmt_tier_endofmonth zte 
        ON t.ap_account_id = zte.ap_account_id 
        AND DATE_TRUNC('month', t.created_at)::DATE = DATE_TRUNC('month', zte.created_at)::DATE
WHERE 
	CASE WHEN t.created_at < '2022-05-05' THEN (t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping))
		ELSE (t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id NOT IN (38121)))
		END
	AND t.signup_hostcountry IN ('TH','ID','AU','global')
GROUP BY
	1,2,3,4,5,6,7,8,9,10
ORDER BY 1,2,3

-- max trade_id per token
WITH base AS (
SELECT 
	product_1_symbol 
	, trade_id 
	, created_at
	, amount_usd 
FROM analytics.trades_master tm 
WHERE amount_usd IS NOT NULL
ORDER BY amount_usd DESC 
)	, ticket_rank AS (
	SELECT 
		*
		, RANK() OVER(PARTITION BY product_1_symbol ORDER BY amount_usd DESC) ticket_rank
	FROM base 
	ORDER BY product_1_symbol , 5
)
SELECT *
FROM ticket_rank
WHERE ticket_rank = 1
;


-- max order_id
SELECT 
	order_id  
	, SUM(amount_usd) amount_usd 
FROM analytics.trades_master tm 
WHERE amount_usd IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC 
;


WITH interval_period AS (
SELECT 
	*
FROM generate_series('2018-12-01 00:00:00'::timestamp, '2022-07-12 00:00:00'::timestamp, '15 minute'::INTERVAL) interval_time 
)	, interval_trade AS (
	SELECT 
		interval_time
		, tm.product_1_symbol 
		, tm.trade_id 
		, tm.amount_usd 
	FROM interval_period ip
		LEFT JOIN 
			analytics.trades_master tm 
			ON DATE_TRUNC('minute', ip.interval_time) = DATE_TRUNC('minute', tm.created_at)
	WHERE 
		tm.signup_hostcountry IN ('TH','ID','AU','global')
)	, max_trade AS (
	SELECT 
		*
		, RANK() OVER(PARTITION BY product_1_symbol, interval_time ORDER BY amount_usd DESC) max_trade_id
	FROM interval_trade
)
SELECT 
	product_1_symbol
	, AVG(amount_usd) avg_max_trade_id
FROM max_trade 
WHERE 
	max_trade_id = 1
GROUP BY 1
;

