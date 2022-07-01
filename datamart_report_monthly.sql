/* Datamart library - monthly
 * 	1. User Funnel
 * 	2. Trade Volume by ZMT/ non-ZMT and organic/ in-organic
 *  3. Trade Volume by Whale/ non-Whale
 *  4. Trade Volume by Asset
 *  5. Deposit + Withdraw Volume
 *
 */



---- datamart monthly - user funnel
DROP TABLE IF EXISTS warehouse.reportings_data.dm_user_funnel_monthly;

CREATE TABLE IF NOT EXISTS warehouse.reportings_data.dm_user_funnel_monthly 
(
	id										SERIAL PRIMARY KEY 
	, register_month	 					DATE
	, signup_hostcountry 					VARCHAR(255)
	, registered_user_count	 				INTEGER 
	, cohort_verified_user_count	 		INTEGER 
	, cohort_zipup_subscriber_count	 		INTEGER 
	, reporting_verified_user_count	 		INTEGER 
	, reporting_zipup_subscriber_count	 	INTEGER 
	, total_registered_user	 				INTEGER 
	, total_cohort_verified_user	 		INTEGER 
	, total_reporting_verified_user	 		INTEGER 
	, total_cohort_zipup_subscriber	 		INTEGER 
	, total_reporting_zipup_subscriber	 	INTEGER 
	, zipmex_registered_user_count	 		INTEGER 
	, total_zipmex_registered_user	 		INTEGER 
);

CREATE INDEX IF NOT EXISTS idx_dm_user_funnel_monthly ON warehouse.reportings_data.dm_user_funnel_monthly 
(register_month, signup_hostcountry);

DROP TABLE IF EXISTS tmp_user_funnel_monthly;

CREATE TEMP TABLE tmp_user_funnel_monthly AS 
(
	WITH monthly_base AS 
	(
		SELECT
			u.created_at AS register_date
			, u.onfido_completed_at verified_date 
			, u.zipup_subscribed_at zip_up_date  
			, u.signup_hostcountry 
			, u.user_id 
			, u.is_verified
			, u.is_zipup_subscribed 
		FROM 
			analytics.users_master u
		WHERE 
			u.signup_hostcountry IN ('TH','ID','AU','global')  
	)	
		,base_month AS 
	(
		SELECT 
			DATE_TRUNC('month', register_date)::DATE AS register_month
			, signup_hostcountry
			, count(DISTINCT user_id) AS  registered_user_count
			---> this one only count the status. meaning everytime we report, number will change AND cannot capture true monthly performance
			, count(DISTINCT 
						CASE WHEN register_date IS NOT NULL 
							AND  is_verified = TRUE 
					THEN user_id END ) AS  cohort_verified_user_count
			, count(DISTINCT 
						CASE WHEN register_date IS NOT NULL 
							AND is_zipup_subscribed = TRUE 
					THEN user_id END ) AS  cohort_zipup_subscriber_count
		FROM 
			monthly_base
		GROUP BY 1, 2
	)	
		,base_month_z_up AS 
	(
		SELECT
			DATE_TRUNC('month', zip_up_date) AS zip_up_month
			, signup_hostcountry
			---> this one count the status by subscribe date, number is fixed
			, count(DISTINCT 
						CASE WHEN zip_up_date IS NOT NULL 
							AND is_zipup_subscribed = TRUE 
					THEN user_id END) AS reporting_zipup_subscriber_count 
		FROM 
			monthly_base
		GROUP BY 1,2
	)	
		,base_month_verified AS 
	(
		SELECT
			DATE_TRUNC('month', verified_date) AS verified_month
			, signup_hostcountry
			---> this one count the status by verified date, number is fixed
			, count(DISTINCT 
						CASE WHEN is_verified = TRUE 
					THEN user_id END) AS reporting_verified_user_count 
		FROM 
			monthly_base
		GROUP BY 1,2
	)
	SELECT
		b.* 
		, k.reporting_verified_user_count
		, z.reporting_zipup_subscriber_count
		,sum(b.registered_user_count) OVER(PARTITION BY b.signup_hostcountry ORDER BY register_month ) AS  total_registered_user
		,sum(b.cohort_verified_user_count) OVER(PARTITION BY b.signup_hostcountry ORDER BY register_month) AS  total_cohort_verified_user
		,sum(k.reporting_verified_user_count) OVER(PARTITION BY k.signup_hostcountry ORDER BY verified_month) AS  total_reporting_verified_user
		,sum(b.cohort_zipup_subscriber_count) OVER(PARTITION BY b.signup_hostcountry ORDER BY register_month ) AS  total_cohort_zipup_subscriber
		,sum(z.reporting_zipup_subscriber_count) OVER(PARTITION BY z.signup_hostcountry ORDER BY zip_up_month) AS  total_reporting_zipup_subscriber
		,sum(b.registered_user_count) OVER() AS zipmex_registered_user_count
		,sum(b.cohort_verified_user_count) OVER() AS total_zipmex_registered_user
	FROM
		base_month b
		LEFT JOIN base_month_verified k ON  k.signup_hostcountry = b.signup_hostcountry AND k.verified_month = b.register_month 
		LEFT JOIN base_month_z_up z ON  z.signup_hostcountry = b.signup_hostcountry AND z.zip_up_month = b.register_month 
	ORDER BY 
		1 ,2 DESC	 
);


INSERT INTO warehouse.reportings_data.dm_user_funnel_monthly (register_month,signup_hostcountry,registered_user_count,cohort_verified_user_count,cohort_zipup_subscriber_count,reporting_verified_user_count,reporting_zipup_subscriber_count,total_registered_user,total_cohort_verified_user,total_reporting_verified_user,total_cohort_zipup_subscriber,total_reporting_zipup_subscriber,zipmex_registered_user_count,total_zipmex_registered_user)
(SELECT * FROM tmp_user_funnel_monthly);

DROP TABLE IF EXISTS tmp_user_funnel_monthly;

---- datamart monthly - trade volume by ZMT
DROP TABLE IF EXISTS warehouse.reportings_data.dm_trade_zmt_organic_monthly;

CREATE TABLE IF NOT EXISTS warehouse.reportings_data.dm_trade_zmt_organic_monthly 
(
	id										SERIAL PRIMARY KEY 
	, created_at		 					DATE
	, signup_hostcountry 					VARCHAR(255)
	, is_zmt_trade							BOOLEAN
	, is_organic_trade						BOOLEAN
	, is_july_gaming						BOOLEAN
	, count_trader			 				INTEGER
	, count_orders			 				INTEGER
	, count_trades			 				INTEGER
	, sum_coin_volume			 			NUMERIC
	, sum_usd_trade_volume			 		NUMERIC
);

CREATE INDEX IF NOT EXISTS idx_dm_trade_zmt_monthly ON warehouse.reportings_data.dm_trade_zmt_organic_monthly 
(created_at, signup_hostcountry);


DROP TABLE IF EXISTS tmp_dm_trade_zmt_organic_monthly;

CREATE TEMP TABLE tmp_dm_trade_zmt_organic_monthly AS 
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
			, SUM(t.amount_usd) "sum_usd_trade_volume" 
		FROM 
			analytics.trades_master t
			LEFT JOIN analytics.users_master u
				ON t.ap_account_id = u.ap_account_id
		WHERE 
			t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
			AND t.signup_hostcountry IN ('TH','ID','AU','global')
		GROUP BY
			1,2,3,4,5,6,7,8,9
		ORDER BY 1,2,3
	)	, all_trade AS (
		SELECT * FROM zipmex_trade
		UNION ALL
		SELECT * FROM pluang_trade
	)
	SELECT 
		DATE_TRUNC('month', a.created_at)::DATE created_at 
		, a.signup_hostcountry 
		, is_zmt_trade
		, is_organic_trade
		, is_july_gaming
		, COUNT( DISTINCT ap_account_id) count_trader
		, SUM( COALESCE(count_orders, 0) ) count_orders
		, SUM( COALESCE(count_trades, 0) ) count_trades
		, SUM( COALESCE(sum_coin_volume, 0)) sum_coin_volume 
		, SUM( COALESCE(sum_usd_trade_volume, 0)) sum_usd_trade_volume
	FROM 
		all_trade a 
	GROUP BY 
		1,2,3,4,5
	ORDER BY 1
);

INSERT INTO warehouse.reportings_data.dm_trade_zmt_organic_monthly (created_at,signup_hostcountry,is_zmt_trade,is_organic_trade,is_july_gaming,count_trader,count_orders,count_trades,sum_coin_volume,sum_usd_trade_volume)
(SELECT * FROM tmp_dm_trade_zmt_organic_monthly);

DROP TABLE IF EXISTS tmp_dm_trade_zmt_organic_monthly;

---- datamart monthly - trade volume by whales
DROP TABLE IF EXISTS warehouse.reportings_data.dm_trade_whale_monthly;

CREATE TABLE IF NOT EXISTS warehouse.reportings_data.dm_trade_whale_monthly 
(
	id										SERIAL PRIMARY KEY 
	, created_at		 					DATE
	, signup_hostcountry 					VARCHAR(255)
	, is_whales								BOOLEAN
	, is_july_gaming						BOOLEAN
	, count_trader			 				INTEGER
	, count_orders			 				INTEGER
	, count_trades			 				INTEGER
	, sum_coin_volume			 			NUMERIC
	, sum_usd_trade_volume			 		NUMERIC
);

CREATE INDEX IF NOT EXISTS idx_dm_trade_organic_monthly ON warehouse.reportings_data.dm_trade_whale_monthly 
(created_at, signup_hostcountry);


DROP TABLE IF EXISTS tmp_dm_trade_whale_monthly;

CREATE TEMP TABLE tmp_dm_trade_whale_monthly AS 
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
			, SUM(t.amount_usd) "sum_usd_trade_volume" 
		FROM 
			analytics.trades_master t
			LEFT JOIN analytics.users_master u
				ON t.ap_account_id = u.ap_account_id
		WHERE 
			t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
			AND t.signup_hostcountry IN ('TH','ID','AU','global')
		GROUP BY
			1,2,3,4,5,6,7,8,9
		ORDER BY 1,2,3
	)	, all_trade AS (
		SELECT * FROM zipmex_trade
		UNION ALL
		SELECT * FROM pluang_trade
	)
	SELECT 
		DATE_TRUNC('month', a.created_at)::DATE created_at 
		, a.signup_hostcountry 
		, is_whale
		, is_july_gaming
		, COUNT( DISTINCT ap_account_id) count_trader
		, SUM( COALESCE(count_orders, 0) ) count_orders
		, SUM( COALESCE(count_trades, 0) ) count_trades
		, SUM( COALESCE(sum_coin_volume, 0)) sum_coin_volume 
		, SUM( COALESCE(sum_usd_trade_volume, 0)) sum_usd_trade_volume
	FROM 
		all_trade a 
	GROUP BY 
		1,2,3,4
	ORDER BY 1
);

INSERT INTO warehouse.reportings_data.dm_trade_whale_monthly (created_at,signup_hostcountry,is_whales,is_july_gaming,count_trader,count_orders,count_trades,sum_coin_volume,sum_usd_trade_volume)
(SELECT * FROM tmp_dm_trade_whale_monthly);

DROP TABLE IF EXISTS tmp_dm_trade_whale_monthly;


---- datamart monthly - trade volume by asset 
DROP TABLE IF EXISTS warehouse.reportings_data.dm_trade_asset_monthly;

CREATE TABLE IF NOT EXISTS warehouse.reportings_data.dm_trade_asset_monthly 
(
	id										SERIAL PRIMARY KEY 
	, created_at		 					DATE
	, signup_hostcountry 					VARCHAR(255)
	, product_1_symbol						VARCHAR(255)
	, is_july_gaming						BOOLEAN
	, count_trader			 				INTEGER
	, count_orders			 				INTEGER
	, count_trades			 				INTEGER
	, sum_coin_volume			 			NUMERIC
	, sum_usd_trade_volume			 		NUMERIC
);

CREATE INDEX IF NOT EXISTS idx_dm_trade_asset_monthly ON warehouse.reportings_data.dm_trade_asset_monthly 
(created_at, signup_hostcountry);


DROP TABLE IF EXISTS tmp_dm_trade_asset_monthly;

CREATE TEMP TABLE tmp_dm_trade_asset_monthly AS 
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
			, SUM(t.amount_usd) "sum_usd_trade_volume" 
		FROM 
			analytics.trades_master t
			LEFT JOIN analytics.users_master u
				ON t.ap_account_id = u.ap_account_id
		WHERE 
			t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
			AND t.signup_hostcountry IN ('TH','ID','AU','global')
		GROUP BY
			1,2,3,4,5,6,7,8,9
		ORDER BY 1,2,3
	)	, all_trade AS (
		SELECT * FROM zipmex_trade
		UNION ALL
		SELECT * FROM pluang_trade
	)
	SELECT 
		DATE_TRUNC('month', a.created_at)::DATE created_at 
		, a.signup_hostcountry 
		, product_1_symbol
		, is_july_gaming
		, COUNT( DISTINCT ap_account_id) count_trader
		, SUM( COALESCE(count_orders, 0) ) count_orders
		, SUM( COALESCE(count_trades, 0) ) count_trades
		, SUM( COALESCE(sum_coin_volume, 0)) sum_coin_volume 
		, SUM( COALESCE(sum_usd_trade_volume, 0)) sum_usd_trade_volume
	FROM 
		all_trade a 
	GROUP BY 
		1,2,3,4
	ORDER BY 1
);

INSERT INTO warehouse.reportings_data.dm_trade_asset_monthly (created_at,signup_hostcountry,product_1_symbol,is_organic_trade,is_july_gaming,count_trader,count_orders,count_trades,sum_coin_volume,sum_usd_trade_volume)
(SELECT * FROM tmp_dm_trade_asset_monthly);

DROP TABLE IF EXISTS tmp_dm_trade_asset_monthly;

-- datamart monthly - deposit and withdraw
DROP TABLE IF EXISTS warehouse.reportings_data.dm_deposit_withdraw_monthly;

CREATE TABLE IF NOT EXISTS warehouse.reportings_data.dm_deposit_withdraw_monthly 
(
	id								SERIAL PRIMARY KEY 
	, created_at		 			DATE
	, signup_hostcountry 			VARCHAR(255)
	, product_type 					VARCHAR(255)
	, symbol 						VARCHAR(255)
	, is_whales						BOOLEAN
	, deposit_count					INTEGER
	, deposit_amount_unit			NUMERIC
	, deposit_amount_usd			NUMERIC
	, withdraw_count				INTEGER
	, withdraw_amount_unit			NUMERIC
	, withdraw_amount_usd			NUMERIC
);

CREATE INDEX IF NOT EXISTS idx_dm_deposit_withdraw_monthly ON warehouse.reportings_data.dm_deposit_withdraw_monthly 
(created_at, signup_hostcountry);

DROP TABLE IF EXISTS tmp_dm_deposit_withdraw_monthly;

CREATE TEMP TABLE tmp_dm_deposit_withdraw_monthly AS 
(
	WITH deposit_ AS 
	( 
		SELECT 
			date_trunc('day', d.updated_at) AS month_  
			, d.ap_account_id 
			, d.signup_hostcountry 
			, d.product_type 
			, d.product_symbol 
			, CASE WHEN d.ap_account_id IN (SELECT DISTINCT ap_account_id FROM mappings.commercial_is_whale) THEN TRUE ELSE FALSE END AS is_whale
			, COUNT( DISTINCT d.ticket_id) AS deposit_number 
			, SUM(d.amount) AS deposit_amount 
			, SUM(d.amount_usd) deposit_usd
		FROM 
			analytics.deposit_tickets_master d 
		WHERE 
			d.status = 'FullyProcessed' 
			AND d.signup_hostcountry IN ('TH','AU','ID','global')
		--	AND d.updated_at::date >= '2021-01-01' AND d.updated_at::date < NOW()::date 
			AND d.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping) 
		GROUP  BY 
			1,2,3,4,5,6
	)
		, withdraw_ AS 
	(
		SELECT 
			date_trunc('day', w.updated_at) AS month_  
			, w.ap_account_id 
			, w.signup_hostcountry 
			, w.product_type 
			, w.product_symbol 
			, CASE WHEN w.ap_account_id IN (SELECT DISTINCT ap_account_id FROM mappings.commercial_is_whale) THEN TRUE ELSE FALSE END AS is_whale
			, COUNT( DISTINCT w.ticket_id) AS withdraw_number 
			, SUM(w.amount) AS withdraw_amount 
            , SUM(fm.fee_usd_amount) withdraw_fee_usd
        FROM  
            analytics.withdraw_tickets_master w 
            LEFT JOIN 
                 analytics.fees_master fm 
                 ON w.ticket_id = fm.fee_reference_id 
		WHERE 
			w.status = 'FullyProcessed'
			AND w.signup_hostcountry IN ('TH','AU','ID','global')
		--	AND w.updated_at::date >= '2021-01-01' AND w.updated_at::date < NOW()::date 
			AND w.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
		GROUP BY 
			1,2,3,4,5
	)
	SELECT 
		DATE_TRUNC('month', COALESCE(d.month_, w.month_))::DATE created_at  
		, COALESCE(d.signup_hostcountry, w.signup_hostcountry) signup_hostcountry
		, COALESCE (d.product_type, w.product_type) product_type 
		, COALESCE (d.product_symbol, w.product_symbol) symbol 
		, COALESCE (d.is_whale, w.is_whale) is_whale 
		, SUM( COALESCE(d.deposit_number, 0)) deposit_count 
		, SUM( deposit_amount) deposit_amount_unit
		, SUM( COALESCE(d.deposit_usd, 0)) deposit_amount_usd
		, SUM( COALESCE(w.withdraw_number, 0)) withdraw_count
		, SUM( withdraw_amount) withdraw_amount_unit
		, SUM( COALESCE(w.withdraw_usd, 0)) withdraw_amount_usd
        , SUM( COALESCE(w.withdraw_fee_usd, 0)) withdraw_fee_usd
	FROM 
		deposit_ d 
		FULL OUTER JOIN 
			withdraw_ w 
			ON d.ap_account_id = w.ap_account_id 
			AND d.signup_hostcountry = w.signup_hostcountry 
			AND d.product_type = w.product_type 
			AND d.month_ = w.month_ 
			AND d.product_symbol = w.product_symbol 
--	WHERE 
--		COALESCE(d.month_, w.month_) >= DATE_TRUNC('month', NOW()) 
--		AND COALESCE(d.month_, w.month_) < DATE_TRUNC('day', NOW())
	GROUP BY 
		1,2,3,4,5
	ORDER BY 
		1,2 
);

INSERT INTO warehouse.reportings_data.dm_deposit_withdraw_monthly (created_at,signup_hostcountry,product_type,symbol,is_whales,deposit_count,deposit_amount_unit,deposit_amount_usd,withdraw_count,withdraw_amount_unit,withdraw_amount_usd)
(SELECT * FROM tmp_dm_deposit_withdraw_monthly);

DROP TABLE IF EXISTS tmp_dm_deposit_withdraw_monthly;


