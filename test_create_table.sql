-- datamart daily - trade volume by asset 
DROP TABLE IF EXISTS warehouse.bo_testing.dm_trade_asset_daily;
DROP TABLE IF EXISTS warehouse.bo_testing.dm_aum_daily;
DROP TABLE IF EXISTS warehouse.bo_testing.dm_trade_asset_monthly;

CREATE TABLE IF NOT EXISTS warehouse.bo_testing.dm_trade_asset_daily 
(
	id										SERIAL PRIMARY KEY 
	, created_at		 					DATE
	, signup_hostcountry 					VARCHAR(255)
	, ap_account_id							INTEGER
	, product_1_symbol						VARCHAR(255)
	, is_july_gaming						BOOLEAN
	, count_orders			 				INTEGER
	, count_trades			 				INTEGER
	, sum_coin_volume			 			NUMERIC
	, sum_fiat_trade_volume					NUMERIC
	, sum_usd_trade_volume			 		NUMERIC
);

CREATE INDEX IF NOT EXISTS idx_dm_trade_asset_daily ON warehouse.bo_testing.dm_trade_asset_daily 
(created_at, signup_hostcountry, ap_account_id, product_1_symbol);

DROP TABLE IF EXISTS tmp_dm_trade_asset_daily;

CREATE TEMP TABLE tmp_dm_trade_asset_daily AS 
(
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
			, NULL product_2_symbol
			, side 
			, is_organic_trade 
			, CASE WHEN product_1_symbol = 'ZMT' THEN TRUE ELSE FALSE END AS is_zmt_trade
			, CASE WHEN user_id IN (SELECT DISTINCT ap_account_id::TEXT FROM mappings.commercial_is_whale) THEN TRUE ELSE FALSE END AS is_whale
			, FALSE is_july_gaming
			, COUNT(DISTINCT order_id) count_orders
			, COUNT(DISTINCT quote_id) count_trades 
			, SUM(quantity) quantity 
			, SUM(amount_usd) amount_usd
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
			, t.product_2_symbol
			, t.side 
			, CASE WHEN t.counter_party IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping) THEN FALSE ELSE TRUE END "is_organic_trade" 
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
			, SUM(t.amount_base_fiat) "sum_fiat_trade_volume" 
			, SUM(t.amount_usd) "sum_usd_trade_volume" 
		FROM 
			analytics.trades_master t
			LEFT JOIN analytics.users_master u
				ON t.ap_account_id = u.ap_account_id
		WHERE 
			t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
			AND t.signup_hostcountry IN ('TH','ID','AU','global')
		GROUP BY
			1,2,3,4,5,6,7,8,9,10
		ORDER BY 1,2,3
	)	, all_trade AS (
		SELECT * FROM zipmex_trade
		UNION ALL
		SELECT * FROM pluang_trade
	)
	SELECT 
		DATE_TRUNC('day', a.created_at)::DATE created_at 
		, a.signup_hostcountry 
		, ap_account_id
		, product_1_symbol
		, is_july_gaming
		, SUM( COALESCE(count_orders, 0) ) count_orders
		, SUM( COALESCE(count_trades, 0) ) count_trades
		, SUM( COALESCE(sum_coin_volume, 0)) sum_coin_volume 
		, SUM( COALESCE(sum_fiat_trade_volume, 0)) sum_fiat_trade_volume 
		, SUM( COALESCE(sum_usd_trade_volume, 0)) sum_usd_trade_volume
	FROM 
		all_trade a 
--	WHERE signup_hostcountry = 'TH'
--		AND created_at >= '2022-02-01'
	GROUP BY 
		1,2,3,4,5
	ORDER BY 1
);

INSERT INTO warehouse.bo_testing.dm_trade_asset_daily (created_at, signup_hostcountry, ap_account_id, product_1_symbol, product_2_symbol, is_july_gaming, count_orders, count_trades, sum_coin_volume, sum_fiat_trade_volume, sum_usd_trade_volume)
(SELECT * FROM tmp_dm_trade_asset_daily);

DROP TABLE IF EXISTS tmp_dm_trade_asset_daily;