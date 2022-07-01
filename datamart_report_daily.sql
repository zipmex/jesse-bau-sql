/* Datamart library - daily
 * 	1. User Funnel : Country Level
 * 	2. Trade volume by product : user level
 *  3. Deposit + Withdraw Volume by product: user level 
 */



-- datamart daily - trade volume by asset 
DROP TABLE IF EXISTS warehouse.reportings_data.dm_trade_asset_daily;

CREATE TABLE IF NOT EXISTS warehouse.reportings_data.dm_trade_asset_daily 
(
	id										SERIAL PRIMARY KEY 
	, created_at		 					DATE
	, signup_hostcountry 					VARCHAR(255)
	, ap_account_id							INTEGER
	, product_1_symbol						VARCHAR(255)
	, count_orders			 				INTEGER
	, count_trades			 				INTEGER
    , sum_buy_coin_volume                   NUMERIC
    , sum_sell_coin_volume                  NUMERIC
	, sum_coin_volume			 			NUMERIC
    , sum_buy_fiat_volume                   NUMERIC
    , sum_sell_fiat_volume                  NUMERIC
	, sum_fiat_trade_volume					NUMERIC
	, sum_buy_usd_volume                   NUMERIC
	, sum_sell_usd_volume                  NUMERIC
    , sum_usd_trade_volume                  NUMERIC
);

CREATE INDEX IF NOT EXISTS idx_dm_trade_asset_daily ON warehouse.reportings_data.dm_trade_asset_daily 
(created_at, signup_hostcountry, ap_account_id, product_1_symbol);

DROP TABLE IF EXISTS tmp_dm_trade_asset_daily;

CREATE TEMP TABLE tmp_dm_trade_asset_daily AS 
(
	WITH pluang_trade_all AS (
	-- Pluang has a secondary account, data stored in zipmex_otc_public
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
		--	AND DATE_TRUNC('day',q.created_at) >= '2021-01-01 00:00:00'
		GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
		ORDER BY 1 DESC 
	)	, pluang_trade AS (
	-- populate the same amount of columns to UNION with Trades_master data 
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
			DATE_TRUNC('day', t.created_at)::DATE created_at 
			, t.signup_hostcountry 
			, t.ap_account_id 
			, 'zipmex' user_type
			, t.product_1_symbol
			, t.product_2_symbol
			, t.side 
			, CASE WHEN t.counter_party IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping) THEN FALSE ELSE TRUE END "is_organic_trade" 
			, CASE WHEN product_1_id IN (16,50) THEN TRUE ELSE FALSE END AS is_zmt_trade
			, CASE WHEN t.ap_account_id IN (SELECT DISTINCT ap_account_id FROM mappings.commercial_is_whale) THEN TRUE ELSE FALSE END AS is_whale
	-- filter for gaming_trade in July 2021
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
	-- exclude market maker
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
		, SUM( COALESCE(count_orders, 0) ) count_orders
		, SUM( COALESCE(count_trades, 0) ) count_trades
        , SUM( CASE WHEN side = 'Buy' THEN COALESCE(sum_coin_volume, 0) END) sum_buy_coin_volume 
        , SUM( CASE WHEN side = 'Sell' THEN COALESCE(sum_coin_volume, 0) END) sum_sell_coin_volume 
        , SUM( COALESCE(sum_coin_volume, 0)) sum_coin_volume 
        , SUM( CASE WHEN side = 'Buy' THEN COALESCE(sum_fiat_trade_volume, 0)) sum_fiat_trade_volume 
        , SUM( COALESCE(sum_fiat_trade_volume, 0)) sum_fiat_trade_volume 
		, SUM( COALESCE(sum_usd_trade_volume, 0)) sum_usd_trade_volume
	FROM 
		all_trade a 
	WHERE 
	   created_at >= DATE_TRUNC('month', NOW()::DATE) - '18 month'::INTERVAL
	   AND is_july_gaming IS FALSE
	GROUP BY 
		1,2,3,4
	ORDER BY 1
);


INSERT INTO warehouse.reportings_data.dm_trade_asset_daily (created_at, signup_hostcountry, ap_account_id, product_1_symbol, product_2_symbol, is_july_gaming, count_orders, count_trades, sum_coin_volume, sum_fiat_trade_volume, sum_usd_trade_volume)
(SELECT * FROM tmp_dm_trade_asset_daily);


DROP TABLE IF EXISTS tmp_dm_trade_asset_daily;

-- datamart daily - deposit and withdraw
DROP TABLE IF EXISTS warehouse.reportings_data.dm_deposit_withdraw_daily;

CREATE TABLE IF NOT EXISTS warehouse.reportings_data.dm_deposit_withdraw_daily 
(
	id								SERIAL PRIMARY KEY 
	, created_at		 			DATE
	, signup_hostcountry 			VARCHAR(255)
	, ap_account_id					INTEGER
	, product_type 					VARCHAR(255)
	, symbol 						VARCHAR(255)
	, is_whales						BOOLEAN
	, deposit_count					INTEGER
	, deposit_amount_unit			NUMERIC
	, deposit_amount_usd			NUMERIC
	, withdraw_count				INTEGER
	, withdraw_amount_unit			NUMERIC
	, withdraw_amount_usd			NUMERIC
    , withdraw_fee_usd              NUMERIC
);


CREATE INDEX IF NOT EXISTS idx_dm_deposit_withdraw_monthly ON warehouse.reportings_data.dm_deposit_withdraw_daily 
(created_at, signup_hostcountry, ap_account_id, symbol);

DROP TABLE IF EXISTS tmp_dm_deposit_withdraw_daily;

CREATE TEMP TABLE tmp_dm_deposit_withdraw_daily AS 
(
	WITH deposit_ AS 
	( 
		SELECT 
			date_trunc('day', d.updated_at) AS updated_at  
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
		--	AND d.updated_at::date >= '2021-01-01' AND d.updated_at::date < NOW()::date 
			AND d.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping) 
		GROUP  BY 
			1,2,3,4,5,6
	)
		, withdraw_ AS 
	(
		SELECT 
			date_trunc('day', w.updated_at) AS updated_at  
			, w.ap_account_id 
			, w.signup_hostcountry 
			, w.product_type 
			, w.product_symbol 
			, CASE WHEN w.ap_account_id IN (SELECT DISTINCT ap_account_id FROM mappings.commercial_is_whale) THEN TRUE ELSE FALSE END AS is_whale
			, COUNT( DISTINCT w.ticket_id) AS withdraw_number 
			, SUM(w.amount) AS withdraw_amount 
			, SUM(w.amount_usd) withdraw_usd
		FROM  
			analytics.withdraw_tickets_master w 
		WHERE 
	-- only take FullyProcessed Tickets
			w.status = 'FullyProcessed'
			AND w.signup_hostcountry IN ('TH','AU','ID','global')
		--	AND w.updated_at::date >= '2021-01-01' AND w.updated_at::date < NOW()::date 
			AND w.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
		GROUP BY 
			1,2,3,4,5
	)
	SELECT 
		COALESCE(d.updated_at, w.updated_at)::DATE created_at  
		, COALESCE(d.signup_hostcountry, w.signup_hostcountry) signup_hostcountry
		, COALESCE(d.ap_account_id, w.ap_account_id) ap_account_id
		, COALESCE (d.product_type, w.product_type) product_type 
		, COALESCE (d.product_symbol, w.product_symbol) symbol 
		, COALESCE (d.is_whale, w.is_whale) is_whale 
		, SUM( COALESCE(d.deposit_number, 0)) deposit_count 
		, SUM( deposit_amount) deposit_amount_unit
		, SUM( COALESCE(d.deposit_usd, 0)) deposit_amount_usd
		, SUM( COALESCE(w.withdraw_number, 0)) withdraw_count
		, SUM( withdraw_amount) withdraw_amount_unit
		, SUM( COALESCE(w.withdraw_usd, 0)) withdraw_amount_usd
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
       updated_at >= DATE_TRUNC('month', NOW()::DATE) - '18 month'::INTERVAL
	GROUP BY 
		1,2,3,4,5,6
	ORDER BY 
		1,2 
);

INSERT INTO warehouse.reportings_data.dm_deposit_withdraw_daily (created_at,signup_hostcountry,product_type,symbol,is_whales,deposit_count,deposit_amount_unit,deposit_amount_usd,withdraw_count,withdraw_amount_unit,withdraw_amount_usd,withdraw_fee_usd)
(SELECT * FROM tmp_dm_deposit_withdraw_daily);

DROP TABLE IF EXISTS tmp_dm_deposit_withdraw_daily;

-- datamart daily - aum breakdown

--DROP TABLE IF EXISTS warehouse.reportings_data.dm_aum_daily;

DELETE FROM warehouse.reportings_data.dm_aum_daily  WHERE created_at >= DATE_TRUNC('day', NOW()) - '3 day'::INTERVAL;

CREATE TABLE IF NOT EXISTS warehouse.reportings_data.dm_aum_daily 
(
	--id					SERIAL PRIMARY KEY 
	created_at				DATE
	,signup_hostcountry			VARCHAR(255)
	,user_aum_count				INTEGER
	,total_aum_usd				NUMERIC
	,user_tradewallet_count			INTEGER
	,tradewallet_usd			NUMERIC
	,user_zw_total_count			INTEGER
	,zw_total_usd				NUMERIC
	,user_zw_zipup_count			INTEGER
	,zw_zipup_usd				NUMERIC
	,user_ziplock_total_count		INTEGER
	,ziplock_total_usd			NUMERIC
	,user_zlaunch_count			INTEGER
	,zlaunch_total_usd			NUMERIC
	,user_aum_zmt_count			INTEGER
	,total_aum_zmt_usd			NUMERIC	
	,user_tradewallet_zmt_count		INTEGER
	,tradewallet_zmt_usd			NUMERIC
	,user_zw_zmt_total_count		INTEGER
	,zw_zmt_total_usd			NUMERIC
	,user_zw_zipup_zmt_count		INTEGER
	,zw_zipup_zmt_usd			NUMERIC
	,user_ziplock_zmt_count			INTEGER
	,ziplock_zmt_usd			NUMERIC
	,user_zlaunch_zmt_count			INTEGER
	,zlaunch_zmt_usd			NUMERIC
	,user_aum_nonzmt_count			INTEGER
	,total_aum_nonzmt_usd			NUMERIC
	,user_tradewallet_nonzmt_count		INTEGER
	,tradewallet_nonzmt_usd			NUMERIC
	,user_zw_nonzmt_total_count		INTEGER
	,zw_nonzmt_total_usd			NUMERIC
	,user_zw_zipup_nonzmt_count		INTEGER
	,zw_zipup_nonzmt_usd			NUMERIC
	,user_ziplock_nonzmt_count		INTEGER
	,ziplock_nonzmt_usd			NUMERIC
	,user_zlaunch_nonzmt_count		INTEGER
	,zlaunch_nonzmt_usd			NUMERIC
	,user_interest_bearing_count		NUMERIC
	,total_interest_bearing_usd		NUMERIC
	,user_interest_zmt_count		NUMERIC
	,zmt_interest_bearing_usd		NUMERIC
	,user_interest_non_zmt_count		NUMERIC
	,nonzmt_interest_bearing_usd		NUMERIC
);

CREATE INDEX IF NOT EXISTS idx_dm_aum_daily ON warehouse.reportings_data.dm_aum_daily 
(created_at, signup_hostcountry);

DROP TABLE IF EXISTS tmp_dm_aum_daily;

CREATE TEMP TABLE tmp_dm_aum_daily AS 
(
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
		-- zipup+ grace period for each country
				(CASE WHEN a.created_at < '2022-05-24' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
				WHEN u.signup_hostcountry = 'ID' THEN
				(CASE WHEN a.created_at < '2022-07-04' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
				WHEN u.signup_hostcountry IN ('AU','global') THEN
				(CASE WHEN a.created_at < '2022-06-29' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
				END AS zipup_subscribed_at
			, a.symbol 
			, CASE WHEN a.symbol = 'ZMT' THEN TRUE WHEN zc.symbol IS NOT NULL THEN TRUE ELSE FALSE END AS zipup_coin 
		-- wallet amount in unit
			, trade_wallet_amount	, z_wallet_amount	, ziplock_amount	, zlaunch_amount
		-- wallet amount in USD
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
				zipup_coin zc 
				ON a.symbol = zc.symbol
				AND a.created_at >= zc.effective_date
				AND a.created_at < zc.expired_date
			LEFT JOIN 
				analytics.users_master u 
				ON a.ap_account_id = u.ap_account_id 
		-- coin prices and exchange rates (USD)
			LEFT JOIN 
				analytics.rates_master r 
				ON a.symbol = r.product_1_symbol
				AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
			LEFT JOIN 
				warehouse.zip_up_service_public.user_settings s
				ON u.user_id = s.user_id 
		WHERE 
			u.signup_hostcountry IN ('TH','ID','AU','global')
			AND a.created_at >= DATE_TRUNC('day', NOW()) - '3 day'::INTERVAL
		-- exclude test products
			AND a.symbol NOT IN ('TST1','TST2')
		ORDER BY 1 DESC 
	)	
		, aum_snapshot AS 
	(
		SELECT 
			DATE_TRUNC('day', created_at)::DATE created_at
			, signup_hostcountry
			, ap_account_id
			, CASE WHEN symbol <> 'ZMT' AND zipup_coin = TRUE THEN 'zipup_coin' 
					WHEN symbol = 'ZMT' THEN 'ZMT' 
					ELSE 'other' END AS asset_group
			, SUM( COALESCE (trade_wallet_amount_usd, 0)) trade_wallet_amount_usd
			, SUM( COALESCE (z_wallet_amount_usd, 0)) z_wallet_amount_usd
			, SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
			, SUM( COALESCE (zlaunch_amount_usd, 0)) zlaunch_amount_usd
			, SUM( COALESCE (CASE WHEN zipup_subscribed_at IS NOT NULL AND created_at >= DATE_TRUNC('day', zipup_subscribed_at) AND zipup_coin = TRUE
						THEN
							(CASE 	WHEN created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
									WHEN created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
						END, 0)) AS zwallet_subscribed_usd
			, SUM( COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0) 
						+ COALESCE (ziplock_amount_usd, 0) + COALESCE (zlaunch_amount_usd, 0)) total_aum_usd
		FROM 
			base 
		WHERE 
			is_asset_manager = FALSE AND is_nominee = FALSE
		GROUP BY 
			1,2,3,4
		ORDER BY 
			1 
	)
	SELECT 
		DATE_TRUNC('day', created_at)::DATE created_at
		, signup_hostcountry
	-- total AUM section: combine ZMT + non ZMT
		-- total AUM - user count - USD value
			, COUNT(DISTINCT ap_account_id) user_aum_count
			, SUM(total_aum_usd) total_aum_usd
		-- trade wallet AUM - user count - USD value
			, COUNT(DISTINCT CASE WHEN trade_wallet_amount_usd > 0 THEN ap_account_id END) user_tradewallet_count
			, SUM(trade_wallet_amount_usd) tradewallet_usd		
		-- z wallet total AUM - user count - AUM USD
			, COUNT(DISTINCT CASE WHEN z_wallet_amount_usd > 0 THEN ap_account_id END) user_zw_total_count
			, SUM(z_wallet_amount_usd) zw_total_usd
		-- z wallet zipup subscribed AUM - user count - AUM USD
			, COUNT(DISTINCT CASE WHEN zwallet_subscribed_usd > 0 THEN ap_account_id END) user_zw_zipup_count
			, SUM(zwallet_subscribed_usd) zw_zipup_usd
		-- ziplock AUM - user count - USD value
			, COUNT(DISTINCT CASE WHEN ziplock_amount_usd > 0 THEN ap_account_id END) user_ziplock_total_count
			, SUM(ziplock_amount_usd) ziplock_total_usd
		-- Zlaunch AUM - user count - USD value
			, COUNT(DISTINCT CASE WHEN zlaunch_amount_usd > 0 THEN ap_account_id END) user_zlaunch_count
			, SUM(COALESCE (zlaunch_amount_usd, 0)) zlaunch_total_usd
	-- AUM ZMT section
		-- total AUM ZMT - user count - USD value
			, COUNT(DISTINCT CASE WHEN asset_group = 'ZMT' AND asset_group = 'ZMT' THEN ap_account_id END) user_aum_ZMT_count
			, SUM( CASE WHEN asset_group = 'ZMT' THEN total_aum_usd END) total_aum_ZMT_usd
		-- trade wallet AUM ZMT - user count - USD value
			, COUNT(DISTINCT CASE WHEN asset_group = 'ZMT' AND trade_wallet_amount_usd > 0 THEN ap_account_id END) user_tradewallet_ZMT_count
			, SUM( CASE WHEN asset_group = 'ZMT' THEN trade_wallet_amount_usd END) tradewallet_ZMT_usd		
		-- z wallet total AUM ZMT - user count - AUM USD
			, COUNT(DISTINCT CASE WHEN asset_group = 'ZMT' AND z_wallet_amount_usd > 0 THEN ap_account_id END) user_zw_ZMT_total_count
			, SUM( CASE WHEN asset_group = 'ZMT' THEN z_wallet_amount_usd END) zw_ZMT_total_usd
		-- z wallet zipup subscribed AUM ZMT - user count - AUM USD
			, COUNT(DISTINCT CASE WHEN asset_group = 'ZMT' AND zwallet_subscribed_usd > 0 THEN ap_account_id END) user_zw_zipup_ZMT_count
			, SUM( CASE WHEN asset_group = 'ZMT' THEN zwallet_subscribed_usd END) zw_zipup_ZMT_usd
		-- ziplock AUM ZMT - user count - AUM USD
			, COUNT(DISTINCT CASE WHEN asset_group = 'ZMT' AND ziplock_amount_usd > 0 THEN ap_account_id END) user_ziplock_ZMT_count
			, SUM( CASE WHEN asset_group = 'ZMT' THEN ziplock_amount_usd END) ziplock_ZMT_usd
		-- Zlaunch AUM ZMT - user count - USD value
			, COUNT(DISTINCT CASE WHEN asset_group = 'ZMT' AND zlaunch_amount_usd > 0 THEN ap_account_id END) user_zlaunch_ZMT_count
			, SUM( CASE WHEN asset_group = 'ZMT' THEN COALESCE (zlaunch_amount_usd, 0) END) zlaunch_ZMT_usd
	-- AUM NON ZMT section
		-- total AUM nonZMT - user count - USD value
			, COUNT(DISTINCT CASE WHEN asset_group <> 'ZMT' THEN ap_account_id END) user_aum_nonZMT_count
			, SUM( CASE WHEN asset_group <> 'ZMT' THEN total_aum_usd END) total_aum_nonZMT_usd
		-- trade wallet AUM nonZMT - user count - USD value
			, COUNT(DISTINCT CASE WHEN asset_group <> 'ZMT' AND trade_wallet_amount_usd > 0 THEN ap_account_id END) user_tradewallet_nonZMT_count
			, SUM( CASE WHEN asset_group <> 'ZMT' THEN trade_wallet_amount_usd END) tradewallet_nonZMT_usd		
		-- z wallet total AUM nonZMT - user count - AUM USD
			, COUNT(DISTINCT CASE WHEN asset_group <> 'ZMT' AND z_wallet_amount_usd > 0 THEN ap_account_id END) user_zw_nonZMT_total_count
			, SUM( CASE WHEN asset_group <> 'ZMT' THEN z_wallet_amount_usd END) zw_nonZMT_total_usd
		-- z wallet zipup subscribed AUM nonZMT- user count - AUM USD
			, COUNT(DISTINCT CASE WHEN asset_group <> 'ZMT' AND zwallet_subscribed_usd > 0 THEN ap_account_id END) user_zw_zipup_nonZMT_count
			, SUM( CASE WHEN asset_group <> 'ZMT' THEN zwallet_subscribed_usd END) zw_zipup_nonZMT_usd
		-- ziplock AUM nonZMT - user count - AUM USD
			, COUNT(DISTINCT CASE WHEN asset_group <> 'ZMT' AND ziplock_amount_usd > 0 THEN ap_account_id END) user_ziplock_nonZMT_count
			, SUM( CASE WHEN asset_group <> 'ZMT' THEN ziplock_amount_usd END) ziplock_nonZMT_usd
		-- Zlaunch AUM nonZMT - user count - USD value
			, COUNT(DISTINCT CASE WHEN asset_group <> 'ZMT' AND zlaunch_amount_usd > 0 THEN ap_account_id END) user_zlaunch_nonZMT_count
			, SUM( CASE WHEN asset_group <> 'ZMT' THEN COALESCE (zlaunch_amount_usd, 0) END) zlaunch_nonZMT_usd
	-- AUM interest bearing section	
		-- Total interest bearing AUM = z wallet (subscribed) + ziplock + z launch
			, COUNT( DISTINCT CASE WHEN (COALESCE (zwallet_subscribed_usd, 0) + COALESCE (ziplock_amount_usd, 0) 
							+ COALESCE (zlaunch_amount_usd, 0)) > 0 THEN ap_account_id END) AS  user_interest_bearing_count
			, SUM( COALESCE (zwallet_subscribed_usd, 0) + COALESCE (ziplock_amount_usd, 0) + COALESCE (zlaunch_amount_usd, 0) ) total_interest_bearing_usd
			, COUNT( DISTINCT CASE WHEN asset_group = 'ZMT' AND (COALESCE (zwallet_subscribed_usd, 0) 
						+ COALESCE (ziplock_amount_usd, 0) + COALESCE (zlaunch_amount_usd, 0)) > 0 THEN ap_account_id END) user_interest_zmt_count
			, SUM( CASE WHEN asset_group = 'ZMT' 
					THEN COALESCE (zwallet_subscribed_usd, 0) + COALESCE (ziplock_amount_usd, 0) + COALESCE (zlaunch_amount_usd, 0) 
					END) zmt_interest_bearing_usd
			, COUNT( DISTINCT CASE WHEN asset_group <> 'ZMT' AND (COALESCE (zwallet_subscribed_usd, 0) 
						+ COALESCE (ziplock_amount_usd, 0) + COALESCE (zlaunch_amount_usd, 0)) > 0 THEN ap_account_id END) user_interest_non_zmt_count
			, SUM( CASE WHEN asset_group <> 'ZMT' 
					THEN COALESCE (zwallet_subscribed_usd, 0) + COALESCE (ziplock_amount_usd, 0) + COALESCE (zlaunch_amount_usd, 0) 
					END) nonZMT_interest_bearing_usd
	FROM aum_snapshot
	GROUP BY
		1,2
);

INSERT INTO warehouse.reportings_data.dm_aum_daily (created_at,	signup_hostcountry,	user_aum_count,	total_aum_usd
,	user_tradewallet_count,	tradewallet_usd,	user_zw_total_count,	zw_total_usd,	user_zw_zipup_count
,	zw_zipup_usd,	user_ziplock_total_count,	ziplock_total_usd,	user_zlaunch_count,	zlaunch_total_usd
,	user_aum_zmt_count,	total_aum_zmt_usd,	user_tradewallet_zmt_count,	tradewallet_zmt_usd,	user_zw_zmt_total_count
,	zw_zmt_total_usd,	user_zw_zipup_zmt_count,	zw_zipup_zmt_usd,	user_ziplock_zmt_count,	ziplock_zmt_usd
,	user_zlaunch_zmt_count,	zlaunch_zmt_usd,	user_aum_nonzmt_count,	total_aum_nonzmt_usd,	user_tradewallet_nonzmt_count
,	tradewallet_nonzmt_usd,	user_zw_nonzmt_total_count,	zw_nonzmt_total_usd,	user_zw_zipup_nonzmt_count,	zw_zipup_nonzmt_usd
,	user_ziplock_nonzmt_count,	ziplock_nonzmt_usd,	user_zlaunch_nonzmt_count,	zlaunch_nonzmt_usd,	user_interest_bearing_count
,	total_interest_bearing_usd,	user_interest_zmt_count, zmt_interest_bearing_usd,	user_interest_non_zmt_count
,	nonzmt_interest_bearing_usd)
(SELECT * FROM tmp_dm_aum_daily);

DROP TABLE IF EXISTS tmp_dm_aum_daily;
