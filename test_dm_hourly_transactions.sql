/*
 * user hourly transactions dwtf
 * Deposit - Withdraw - Trade (Buy/ Sell/ Total) - Withdraw Fee - Trade Fee (total amount of coin / fiat/ usd )
 * historical data upto 18 months
 * Scope: 4 instances
 * Excludes Market Makers + Wash Trade (July 2021)
 */


/*
 * To validate trade-deposit-withdraw volume by month
SELECT 
    DATE_TRUNC('month', created_at)::DATE created_at_utc
    , SUM(sum_usd_trade_amount) sum_trade_volume
    , SUM(sum_usd_deposit_amount) sum_deposit_volume
    , SUM(sum_usd_withdraw_amount) sum_withdraw_volume
FROM reportings_data.dm_user_transactions_dwt_daily dutdd
GROUP BY 1
ORDER BY 1 DESC 
;
*/


DROP TABLE IF EXISTS warehouse.bo_testing.dm_user_transactions_dwt_hourly;

CREATE TABLE IF NOT EXISTS warehouse.bo_testing.dm_user_transactions_dwt_hourly 
(
	id									  SERIAL PRIMARY KEY 
	, invitation_code						VARCHAR(255)
	, user_id								VARCHAR(255)
	, created_at							TIMESTAMP
	, signup_hostcountry					VARCHAR(255)
	, ap_account_id						 INTEGER
	, product_1_symbol					  VARCHAR(255)
	, order_count							INTEGER
	, trade_count							 INTEGER
	, coin_buy_amount						NUMERIC
	, coin_net_buy_amount					NUMERIC
	, sum_coin_trade_amount				 NUMERIC
	, usd_buy_amount						NUMERIC
	, usd_net_buy_amount					NUMERIC
	, sum_usd_trade_amount				  NUMERIC
	, deposit_count						 INTEGER
	, sum_coin_deposit_amount				NUMERIC
	, sum_usd_deposit_amount				NUMERIC
	, withdraw_count						INTEGER
	, sum_coin_withdraw_amount			  NUMERIC
	, sum_usd_withdraw_amount				NUMERIC
	, sum_usd_fee_withdraw				  NUMERIC
	, sum_coin_fee_trade					NUMERIC
	, sum_usd_fee_trade					 NUMERIC
);

CREATE INDEX IF NOT EXISTS idx_dm_user_transactions_dwt_hourly ON warehouse.bo_testing.dm_user_transactions_dwt_hourly 
(created_at, signup_hostcountry, ap_account_id, product_1_symbol);

ALTER TABLE warehouse.bo_testing.dm_user_transactions_dwt_hourly REPLICA IDENTITY FULL;

--TRUNCATE TABLE warehouse.bo_testing.dm_user_transactions_dwt_hourly;


CREATE TEMP TABLE tmp_dm_trade_asset_user_daily AS 
(
	WITH pluang_trade_all AS (
	-- Pluang has a secondary account, data stored in zipmex_otc_public
		SELECT 
			DATE_TRUNC('hour', q.created_at) created_at
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
	-- trade value in IDR, convert to USD
			LEFT JOIN 
				oms_data_public.exchange_rates e
				ON DATE_TRUNC('day', e.created_at) = DATE_TRUNC('day', q.created_at)
				AND UPPER(RIGHT(SPLIT_PART(q.instrument_id,'.',1),3))  = e.product_2_symbol
				AND e."source" = 'coinmarketcap'
		WHERE
			q.status='completed'
	-- pluang user_id 
			AND q.user_id IN ('01F14GTKR63YS7QSPGCQDNVJRR')
		--  AND DATE_TRUNC('day',q.created_at) >= '2021-01-01 00:00:00'
		GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
		ORDER BY 1 DESC 
	)   , pluang_trade AS (
	-- populate the same amount of columns to UNION with Trades_master data 
		SELECT 
			created_at
			, signup_hostcountry
			, 0101 ap_account_id 
			, 'pluang' user_type
			, product_1_symbol
			, NULL product_2_symbol
			, side 
--			, is_organic_trade 
			, CASE WHEN product_1_symbol = 'ZMT' THEN TRUE ELSE FALSE END AS is_zmt_trade
--			, CASE WHEN user_id IN (SELECT DISTINCT ap_account_id::TEXT FROM mappings.commercial_is_whale) THEN TRUE ELSE FALSE END AS is_whale
			, FALSE is_july_gaming
			, 'IDR' trade_base_fiat
			, COUNT(DISTINCT order_id) count_orders
			, COUNT(DISTINCT quote_id) count_trades 
			, SUM(quantity) quantity 
			, SUM(0) amount_usd
			, SUM(amount_usd) amount_usd
			, SUM( 0) sum_coin_fee_trade
			, SUM( 0) sum_fiat_fee_trade
			, SUM( 0) sum_usd_fee_trade
		FROM 
			pluang_trade_all
		GROUP BY 1,2,3,4,5,6,7,8,9,10
	)   , zipmex_trade AS (
		SELECT
			DATE_TRUNC('hour', t.created_at) created_at
			, t.signup_hostcountry 
			, t.ap_account_id 
			, 'zipmex' user_type
			, t.product_1_symbol
			, t.product_2_symbol
			, t.side 
			, CASE WHEN product_1_id IN (16,50) THEN TRUE ELSE FALSE END AS is_zmt_trade
	-- filter for gaming_trade in July 2021
			, CASE  WHEN t.ap_account_id IN ('85191','73926','88108','152636','140459','140652','55796','56951','52826','54687')
						AND t.product_1_symbol IN ('USDC')
						AND DATE_TRUNC('day', t.created_at) >= '2021-07-01 07:00:00'
						AND DATE_TRUNC('day', t.created_at) < '2021-07-11 07:00:00'
						THEN TRUE ELSE FALSE 
					END AS is_july_gaming
			, t.base_fiat trade_base_fiat
			, COUNT(DISTINCT t.order_id) "count_orders"
			, COUNT(DISTINCT t.trade_id) "count_trades"
		--  , COUNT(DISTINCT t.execution_id) "count_executions"
			, SUM(t.quantity) "sum_coin_amount"
			, SUM(t.amount_base_fiat) "sum_fiat_trade_amount" 
			, SUM(t.amount_usd) "sum_usd_trade_amount" 
			, SUM(fm.fee_amount) "sum_coin_fee_trade"
			, SUM(fm.fee_base_fiat_amount) "sum_fiat_fee_trade"
			, SUM(fm.fee_usd_amount) "sum_usd_fee_trade"
		FROM 
			analytics.trades_master t
			LEFT JOIN 
				analytics.users_master u
				ON t.ap_account_id = u.ap_account_id
			LEFT JOIN 
				analytics.fees_master fm 
				ON t.execution_id = fm.fee_reference_id 
		WHERE 
--	 exclude market maker
			CASE WHEN t.created_at < '2022-05-05' THEN t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
					ELSE t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id NOT IN (38121))
					END
			AND t.signup_hostcountry IN ('TH','ID','AU','global')
		GROUP BY
			1,2,3,4,5,6,7,8,9,10
		ORDER BY 1,2,3
	)   , all_trade AS (
		SELECT * FROM zipmex_trade
		UNION ALL
		SELECT * FROM pluang_trade
	)
	SELECT 
		created_at
		, a.signup_hostcountry 
		, ap_account_id
		, product_1_symbol 
		, SUM( COALESCE(count_orders, 0) ) count_orders
		, SUM( COALESCE(count_trades, 0) ) count_trades
		, SUM( CASE WHEN side = 'Buy' THEN COALESCE(sum_coin_amount, 0) END) coin_buy_amount 
		, SUM( CASE WHEN side = 'Sell' THEN COALESCE(sum_coin_amount, 0) END) coin_sell_amount 
		, SUM( COALESCE(sum_coin_amount, 0)) sum_coin_trade_amount 
		, SUM( CASE WHEN side = 'Buy' THEN COALESCE(sum_usd_trade_amount, 0) END) usd_buy_amount
		, SUM( CASE WHEN side = 'Sell' THEN COALESCE(sum_usd_trade_amount, 0) END) usd_sell_amount
		, SUM( COALESCE(sum_usd_trade_amount, 0)) sum_usd_trade_amount
		, SUM( COALESCE(sum_coin_fee_trade, 0)) sum_coin_fee_trade
		, SUM( COALESCE(sum_usd_fee_trade, 0)) sum_usd_fee_trade
	FROM 
		all_trade a 
	WHERE 
		created_at >= DATE_TRUNC('month', NOW()::DATE) - '18 month'::INTERVAL
		AND created_at < NOW()::DATE
		AND is_july_gaming IS FALSE
	GROUP BY 
		1,2,3,4
	ORDER BY 1
);


-- deposit and withdraw user level
CREATE TEMP TABLE tmp_dm_deposit_withdraw_user_daily AS 
(
	WITH deposit_ AS 
	( 
		SELECT 
			date_trunc('hour', d.updated_at) AS updated_at
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
	-- only take FullyProcessed Tickets
			d.status = 'FullyProcessed' 
			AND d.signup_hostcountry IN ('TH','AU','ID','global')
			AND CASE WHEN d.created_at < '2022-05-05' THEN d.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
					ELSE d.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id NOT IN (38121))
					END
		GROUP  BY 
			1,2,3,4,5,6
	)
		, withdraw_ AS 
	(
		SELECT 
			date_trunc('hour', w.updated_at) AS updated_at
			, w.ap_account_id 
			, w.signup_hostcountry 
			, w.product_type 
			, w.product_symbol 
			, CASE WHEN w.ap_account_id IN (SELECT DISTINCT ap_account_id FROM mappings.commercial_is_whale) THEN TRUE ELSE FALSE END AS is_whale
			, COUNT( DISTINCT w.ticket_id) AS withdraw_number 
			, SUM(w.amount) AS withdraw_amount 
			, SUM(w.amount_usd) withdraw_usd
			, SUM(fm.fee_usd_amount) withdraw_fee_usd
		FROM  
			analytics.withdraw_tickets_master w 
			LEFT JOIN 
				 analytics.fees_master fm 
				 ON w.ticket_id = fm.fee_reference_id 
		WHERE 
	-- only take FullyProcessed Tickets
			w.status = 'FullyProcessed'
			AND w.signup_hostcountry IN ('TH','AU','ID','global')
			AND CASE WHEN w.created_at < '2022-05-05' THEN w.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
					ELSE w.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id NOT IN (38121))
					END
		GROUP BY 
			1,2,3,4,5
	)
	SELECT 
		COALESCE(d.updated_at, w.updated_at) created_at 
		, COALESCE(d.signup_hostcountry, w.signup_hostcountry) signup_hostcountry
		, COALESCE(d.ap_account_id, w.ap_account_id) ap_account_id
		, COALESCE (d.product_symbol, w.product_symbol) product_1_symbol 
		, SUM( COALESCE(d.deposit_number, 0)) deposit_count 
		, SUM( deposit_amount) sum_coin_deposit_amount
		, SUM( COALESCE(d.deposit_usd, 0)) sum_usd_deposit_amount
		, SUM( COALESCE(w.withdraw_number, 0)) withdraw_count
		, SUM( withdraw_amount) sum_coin_withdraw_amount
		, SUM( COALESCE(w.withdraw_usd, 0)) sum_usd_withdraw_amount
		, SUM( COALESCE(w.withdraw_fee_usd, 0)) sum_usd_withdraw_fee
	FROM 
		deposit_ d 
	-- FOJ here in case there is either deposit or withdraw in 1 day
		FULL OUTER JOIN 
			withdraw_ w 
			ON d.ap_account_id = w.ap_account_id 
			AND d.signup_hostcountry = w.signup_hostcountry 
			AND d.product_type = w.product_type 
			AND d.updated_at = w.updated_at 
			AND d.product_symbol = w.product_symbol 
	WHERE 
		COALESCE(d.updated_at, w.updated_at)::DATE >= DATE_TRUNC('month', NOW()::DATE) - '18 month'::INTERVAL
	AND COALESCE(d.updated_at, w.updated_at)::DATE < NOW()::DATE
	GROUP BY 
		1,2,3,4
	ORDER BY 
		1,2 
);


CREATE TEMP TABLE tmp_user_transactions_dwt AS 
(
	WITH temp_transactions AS (
		SELECT 
			COALESCE(d.created_at, t.created_at) created_at  
			, COALESCE(d.signup_hostcountry, t.signup_hostcountry) signup_hostcountry
			, COALESCE(d.ap_account_id, t.ap_account_id) ap_account_id
			, COALESCE (d.product_1_symbol, t.product_1_symbol) product_1_symbol 
			, COALESCE (count_orders, 0) order_count 
			, COALESCE (count_trades, 0) trade_count
			, COALESCE (coin_buy_amount, 0) coin_buy_amount
			, (COALESCE (coin_buy_amount, 0) - COALESCE (coin_sell_amount, 0)) coin_net_buy_amount
			, COALESCE (sum_coin_trade_amount, 0) sum_coin_trade_amount
			, COALESCE (usd_buy_amount, 0) usd_buy_amount
			, (COALESCE (usd_buy_amount, 0) - COALESCE (usd_sell_amount, 0)) usd_net_buy_amount
			, COALESCE (sum_usd_trade_amount, 0) sum_usd_trade_amount
			, COALESCE (deposit_count, 0) deposit_count
			, COALESCE (sum_coin_deposit_amount, 0) sum_coin_deposit_amount
			, COALESCE (sum_usd_deposit_amount, 0) sum_usd_deposit_amount
			, COALESCE (withdraw_count, 0) withdraw_count
			, COALESCE (sum_coin_withdraw_amount, 0) sum_coin_withdraw_amount
			, COALESCE (sum_usd_withdraw_amount, 0) sum_usd_withdraw_amount
			, COALESCE (sum_usd_withdraw_fee, 0) sum_usd_fee_withdraw
			, COALESCE (sum_coin_fee_trade, 0) sum_coin_fee_trade
			, COALESCE (sum_usd_fee_trade, 0) sum_usd_fee_trade
		FROM 
			tmp_dm_trade_asset_user_daily t 
			FULL OUTER JOIN 
				tmp_dm_deposit_withdraw_user_daily d 
				ON t.created_at = d.created_at 
				AND t.ap_account_id = d.ap_account_id
				AND t.signup_hostcountry = d.signup_hostcountry 
				AND t.product_1_symbol = d.product_1_symbol 
	)
	SELECT 
		um.invitation_code 
		, um.user_id
		, t.*
	FROM temp_transactions t
		LEFT JOIN 
			analytics.users_master um 
			ON t.ap_account_id = um.ap_account_id 
);

INSERT INTO warehouse.bo_testing.dm_user_transactions_dwt_hourly (invitation_code, user_id, created_at , signup_hostcountry , ap_account_id
, product_1_symbol  , order_count, trade_count, coin_buy_amount	, coin_net_buy_amount	, sum_coin_trade_amount	, usd_buy_amount	, usd_net_buy_amount	 
, sum_usd_trade_amount , deposit_count , sum_coin_deposit_amount , sum_usd_deposit_amount , withdraw_count , sum_coin_withdraw_amount  
, sum_usd_withdraw_amount , sum_usd_fee_withdraw , sum_coin_fee_trade , sum_usd_fee_trade)
(SELECT * FROM tmp_user_transactions_dwt)
;

DROP TABLE IF EXISTS tmp_dm_trade_asset_user_daily;
DROP TABLE IF EXISTS tmp_dm_deposit_withdraw_user_daily;
DROP TABLE IF EXISTS tmp_user_transactions_dwt;