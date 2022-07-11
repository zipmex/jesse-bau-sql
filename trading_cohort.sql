-- weekly cohort
WITH base AS (
	SELECT 
		DATE_TRUNC('day', created_at) created_at 
		, DATE_TRUNC('week', created_at) created_week
		, DATE_TRUNC('month', created_at) created_month
		, signup_hostcountry 
		, ap_account_id 
		, product_1_symbol 
		, CASE WHEN product_1_symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH') THEN 'zipup_coin'
				WHEN product_1_symbol IN ('ZMT') THEN 'ZMT'
				WHEN product_1_symbol IN ('AXS') THEN 'AXS'
				WHEN product_1_symbol IN ('TOK',	'EOS',	'BTT',	'PAX',	'ONT',	'DGB',	'ICX',	'ANKR',	'WRX',	'KNC',	'XVS'
										,	'KAVA',	'OGN',	'CTSI',	'SRM',	'BAL',	'BTS',	'JST',	'COTI',	'ZEN',	'ANT'
										,	'RLC',	'STORJ',	'WTC',	'XVG',	'LSK',	'EGLD',	'FTM',	'RVN',	'UMA',	'LRC',	'FIL',	'AAVE'
										,	'TFUEL',	'RUNE',	'HBAR',	'CHZ',	'HOT',	'SUSHI',	'GRT',	'BNT',	'IOST',	'SLP')
					THEN 'batch_08_25'
				WHEN product_1_symbol IN ('MATIC',	'AAVE',	'HOT',	'SNX',	'BAT',	'FTT',	'UNI',	'1INCH',	'CHZ',	'CRV',	'ZRX',	'BNT',	'KNC')
					THEN 'batch_09_17'
				WHEN product_1_symbol IN ('ALPHA','BAND') THEN 'batch_band_alpha'
				ELSE 'other'
				END AS product_group
		, SUM(quantity) sum_trade_amount
		, SUM(amount_usd) sum_trade_usd_amount
	FROM 
		analytics.trades_master t
	WHERE 
		created_at >= '2021-08-01 00:00:00'	--AND created_at < '2021-10-01 00:00:00'
		AND t.ap_account_id NOT IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227','27443'
			,'38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','44057','161347','316078','44056','63152','710015','729499')
		AND t.signup_hostcountry IN ('TH','ID','AU','global')
	--	AND product_1_symbol = 'ZMT'
	GROUP BY 1,2,3,4,5
)	, w0_trader AS (
	SELECT 
		DISTINCT 
		created_week w0
		, created_month m0
		, signup_hostcountry 
		, ap_account_id 
		, product_group
	FROM 
		base
	WHERE
		created_week = DATE_TRUNC('week', '2021-08-23'::timestamp) ---- CHANGE WEEK 0 HERE
)
SELECT 
	w0
	, b.product_group
	, b.signup_hostcountry
	, COUNT( DISTINCT CASE WHEN created_week = w0 THEN w.ap_account_id END) w0_repeat_trader
	, COUNT( DISTINCT CASE WHEN created_week = w0 + '1 week'::INTERVAL THEN w.ap_account_id END) w1_repeat_trader
	, COUNT( DISTINCT CASE WHEN created_week = w0 + '2 week'::INTERVAL THEN w.ap_account_id END) w2_repeat_trader
	, COUNT( DISTINCT CASE WHEN created_week = w0 + '3 week'::INTERVAL THEN w.ap_account_id END) w3_repeat_trader
	, COUNT( DISTINCT CASE WHEN created_week = w0 + '4 week'::INTERVAL THEN w.ap_account_id END) w4_repeat_trader
	, COUNT( DISTINCT CASE WHEN created_week = w0 + '5 week'::INTERVAL THEN w.ap_account_id END) w5_repeat_trader
	, COUNT( DISTINCT CASE WHEN created_week = w0 + '6 week'::INTERVAL THEN w.ap_account_id END) w6_repeat_trader
	, COUNT( DISTINCT CASE WHEN created_week = w0 + '7 week'::INTERVAL THEN w.ap_account_id END) w7_repeat_trader
	, COUNT( DISTINCT CASE WHEN created_week = w0 + '8 week'::INTERVAL THEN w.ap_account_id END) w8_repeat_trader
--	, COUNT( DISTINCT CASE WHEN created_week = w0 + '9 week'::INTERVAL THEN w.ap_account_id END) w9_repeat_trader
--	, COUNT( DISTINCT CASE WHEN created_week = w0 + '10 week'::INTERVAL THEN w.ap_account_id END) w10_repeat_trader
	, SUM( CASE WHEN created_week = w0 THEN COALESCE(sum_trade_usd_amount, 0) END) w0_trade_volume
	, SUM( CASE WHEN created_week = w0 + '1 week'::INTERVAL THEN COALESCE(sum_trade_usd_amount, 0) END) w1_trade_volume
	, SUM( CASE WHEN created_week = w0 + '2 week'::INTERVAL THEN COALESCE(sum_trade_usd_amount, 0) END) w2_trade_volume
	, SUM( CASE WHEN created_week = w0 + '3 week'::INTERVAL THEN COALESCE(sum_trade_usd_amount, 0) END) w3_trade_volume
	, SUM( CASE WHEN created_week = w0 + '4 week'::INTERVAL THEN COALESCE(sum_trade_usd_amount, 0) END) w4_trade_volume
	, SUM( CASE WHEN created_week = w0 + '5 week'::INTERVAL THEN COALESCE(sum_trade_usd_amount, 0) END) w5_trade_volume
	, SUM( CASE WHEN created_week = w0 + '6 week'::INTERVAL THEN COALESCE(sum_trade_usd_amount, 0) END) w6_trade_volume
	, SUM( CASE WHEN created_week = w0 + '7 week'::INTERVAL THEN COALESCE(sum_trade_usd_amount, 0) END) w7_trade_volume
	, SUM( CASE WHEN created_week = w0 + '8 week'::INTERVAL THEN COALESCE(sum_trade_usd_amount, 0) END) w8_trade_volume
FROM base b
	INNER JOIN w0_trader w 
		ON b.ap_account_id = w.ap_account_id
		AND b.product_group = w.product_group
		AND b.signup_hostcountry = w.signup_hostcountry
WHERE w0 IS NOT NULL
GROUP BY 1,2,3
;

-- monthly cohort
WITH base AS (
	SELECT 
		DATE_TRUNC('day', created_at) created_at 
		, DATE_TRUNC('week', created_at) created_week
		, DATE_TRUNC('month', created_at) created_month
		, signup_hostcountry 
		, ap_account_id 
		, product_1_symbol 
		, CASE WHEN product_1_symbol IN ('BTC', 'LTC', 'ETH', 'ZMT') THEN product_1_symbol
				WHEN product_1_symbol IN ('USDT', 'USDC') THEN 'usdc_usdt'
				ELSE 'other'
				END AS product_group
		, SUM(quantity) sum_trade_amount
		, SUM(amount_usd) sum_trade_usd_amount
	FROM 
		analytics.trades_master t
	WHERE 
		created_at >= '2021-04-01 00:00:00'	--AND created_at < '2021-10-01 00:00:00'
		AND t.ap_account_id NOT IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227','27443'
			,'38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','44057','161347','316078','44056','63152','710015','729499')
		AND t.signup_hostcountry IN ('TH','ID','AU','global')
	--	AND product_1_symbol = 'ZMT'
	GROUP BY 1,2,3,4,5,6
)	, w0_trader AS (
	SELECT 
		DISTINCT 
		created_week w0
		, created_month m0
		, signup_hostcountry 
		, ap_account_id 
		, product_group
	FROM 
		base
	WHERE
		created_month >= '2021-05-01'::timestamp -- DATE_TRUNC('week', '2021-08-23'::timestamp) ---- CHANGE WEEK 0 HERE
)
SELECT 
	m0
	, b.product_group
	, b.signup_hostcountry
	, COUNT( DISTINCT CASE WHEN created_month = m0 THEN w.ap_account_id END) m0_repeat_trader
	, COUNT( DISTINCT CASE WHEN created_month = m0 + '1 month'::INTERVAL THEN w.ap_account_id END) m1_repeat_trader
	, COUNT( DISTINCT CASE WHEN created_month = m0 + '2 month'::INTERVAL THEN w.ap_account_id END) m2_repeat_trader
	, COUNT( DISTINCT CASE WHEN created_month = m0 + '3 month'::INTERVAL THEN w.ap_account_id END) m3_repeat_trader
	, COUNT( DISTINCT CASE WHEN created_month = m0 + '4 month'::INTERVAL THEN w.ap_account_id END) m4_repeat_trader
	, COUNT( DISTINCT CASE WHEN created_month = m0 + '5 month'::INTERVAL THEN w.ap_account_id END) m5_repeat_trader
	, COUNT( DISTINCT CASE WHEN created_month = m0 + '6 month'::INTERVAL THEN w.ap_account_id END) m6_repeat_trader
	, SUM( CASE WHEN created_month = m0 THEN COALESCE(sum_trade_usd_amount, 0) END) m0_trade_volume
	, SUM( CASE WHEN created_month = m0 + '1 month'::INTERVAL THEN COALESCE(sum_trade_usd_amount, 0) END) m1_trade_volume
	, SUM( CASE WHEN created_month = m0 + '2 month'::INTERVAL THEN COALESCE(sum_trade_usd_amount, 0) END) m2_trade_volume
	, SUM( CASE WHEN created_month = m0 + '3 month'::INTERVAL THEN COALESCE(sum_trade_usd_amount, 0) END) m3_trade_volume
	, SUM( CASE WHEN created_month = m0 + '4 month'::INTERVAL THEN COALESCE(sum_trade_usd_amount, 0) END) m4_trade_volume
	, SUM( CASE WHEN created_month = m0 + '5 month'::INTERVAL THEN COALESCE(sum_trade_usd_amount, 0) END) m5_trade_volume
	, SUM( CASE WHEN created_month = m0 + '6 month'::INTERVAL THEN COALESCE(sum_trade_usd_amount, 0) END) m6_trade_volume
FROM base b
	INNER JOIN w0_trader w 
		ON b.ap_account_id = w.ap_account_id
		AND b.product_group = w.product_group
		AND b.signup_hostcountry = w.signup_hostcountry
WHERE w0 IS NOT NULL
GROUP BY 1,2,3
;

-- user preference
WITH base AS (
	SELECT 
		DATE_TRUNC('day', created_at) created_at 
		, DATE_TRUNC('week', created_at) created_week
		, DATE_TRUNC('month', created_at) created_month
		, signup_hostcountry 
		, ap_account_id 
		, product_1_symbol 
		, CASE WHEN product_1_symbol IN ('BTC', 'LTC', 'ETH', 'ZMT') THEN product_1_symbol
				WHEN product_1_symbol IN ('USDT', 'USDC') THEN 'usdc_usdt'
				ELSE 'other'
				END AS product_group
		, SUM(quantity) sum_trade_amount
		, SUM(amount_usd) sum_trade_usd_amount
	FROM 
		analytics.trades_master t
	WHERE 
		created_at >= '2021-05-01 00:00:00'	--AND created_at < '2021-10-01 00:00:00'
		AND t.ap_account_id NOT IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227','27443'
			,'38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','44057','161347','316078','44056','63152','710015','729499')
		AND t.signup_hostcountry IN ('TH','ID','AU','global')
	--	AND product_1_symbol = 'ZMT'
	GROUP BY 1,2,3,4,5,6
)	, w0_trader AS (
	SELECT 
		DISTINCT 
		created_week w0
		, created_month m0
		, signup_hostcountry 
		, ap_account_id 
		, product_group
	FROM 
		base
	WHERE
		created_month >= '2021-05-01'::timestamp -- DATE_TRUNC('week', '2021-08-23'::timestamp) ---- CHANGE WEEK 0 HERE
		AND product_1_symbol = 'BTC'
)	, aum_snapshot AS (
	SELECT
		DATE_TRUNC('month', created_at) created_at 
		, ap_account_id 
		, symbol 
		, ziplock_amount
	FROM analytics.wallets_balance_eod w
	WHERE 
		created_at >= '2021-05-01 00:00:00'
		AND ((created_at = DATE_TRUNC('month', created_at) + '1 month' - '1 day'::INTERVAL) OR (created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
		AND ziplock_amount > 0
		AND symbol = 'BTC'
)	, deposit_ AS ( 
	SELECT 
		date_trunc('day', d.updated_at) AS month_  
		, d.ap_account_id 
		, d.signup_hostcountry 
		, d.product_type 
		, d.product_symbol 
		,CASE WHEN d.ap_account_id in (1373,1432,13266,16211,16308,22576,34535,48900,53463,80871,84319) THEN TRUE ELSE FALSE END AS is_whale
		, COUNT(d.*) AS deposit_number 
		, SUM(d.amount) AS deposit_amount 
	--	, SUM( CASE WHEN amount_usd IS NOT NULL THEN amount_usd		WHEN product_symbol = 'USD' THEN amount_usd * 1 			WHEN r.product_type = 1 THEN amount * 1/r.price 
	--			WHEN r.product_type = 2 THEN amount * r.price 			END) AS deposit_usd
	--			ELSE (CASE WHEN product_symbol = 'USD' THEN amount		ELSE amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate) END)	END) AS deposit_usd
		, SUM(d.amount_usd) deposit_usd
	FROM 
		analytics.deposit_tickets_master d 
	--	LEFT JOIN 		data_team_staging.rates_master_staging r 
	--		ON d.product_symbol = r.product_1_symbol 	    AND DATE_TRUNC('day', d.created_at) = DATE_TRUNC('day', r.created_at)
	WHERE 
		d.status = 'FullyProcessed' 
		AND d.signup_hostcountry IN ('TH','AU','ID','global')
		AND d.updated_at::date >= '2021-01-01' AND d.updated_at::date < NOW()::date 
		AND d.ap_account_id NOT IN (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63312, 63313, 161347, 317029, 496001) 
	GROUP  BY 
		1,2,3,4,5,6
)	, withdraw_ AS (
	SELECT 
		date_trunc('day', w.updated_at) AS month_  
		, w.ap_account_id 
		, w.signup_hostcountry 
		, w.product_type 
		, w.product_symbol 
		,CASE WHEN w.ap_account_id IN (1373,1432,13266,16211,16308,22576,34535,48900,53463,80871,84319) THEN TRUE ELSE FALSE END AS is_whale
		, COUNT(w.*) AS withdraw_number 
		, SUM(w.amount) AS withdraw_amount 
	--	, SUM( CASE WHEN amount_usd IS NOT NULL THEN amount_usd			WHEN product_symbol = 'USD' THEN amount_usd * 1 			WHEN r.product_type = 1 THEN amount * 1/r.price 
	--			WHEN r.product_type = 2 THEN amount * r.price 			END) AS withdraw_usd	
	--			ELSE (CASE WHEN product_symbol = 'USD' THEN amount ELSE amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate) END) END) AS withdraw_usd 
		, SUM(w.amount_usd) withdraw_usd
	FROM  
		analytics.withdraw_tickets_master w 
	--	LEFT JOIN 		data_team_staging.rates_master_staging r 
	--		ON w.product_symbol = r.product_1_symbol 	    AND DATE_TRUNC('day', w.created_at) = DATE_TRUNC('day', r.created_at)
	WHERE 
		w.status = 'FullyProcessed'
		AND w.signup_hostcountry IN ('TH','AU','ID','global')
		AND w.updated_at::date >= '2021-01-01' AND w.updated_at::date < NOW()::date 
		AND w.ap_account_id NOT IN (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63312, 63313, 161347, 317029, 496001)
	GROUP BY 
		1,2,3,4,5,6
)	, btc_deposit_withdraw AS (
	SELECT 
		DATE_TRUNC('month', COALESCE(d.month_, w.month_)) created_at  
		, COALESCE(d.signup_hostcountry, w.signup_hostcountry) signup_hostcountry
		, COALESCE (d.ap_account_id, w.ap_account_id) ap_account_id 
	--	, COALESCE (d.product_type, w.product_type) product_type 
		, COALESCE (d.product_symbol, w.product_symbol) symbol 
	--	, COALESCE(d.is_whale, w.is_whale) is_whale
		, SUM( COALESCE(d.deposit_number, 0)) depost_count 
		, SUM( deposit_amount) deposit_amount
		, SUM( COALESCE(d.deposit_usd, 0)) deposit_usd
		, SUM( COALESCE(w.withdraw_number, 0)) withdraw_count
		, SUM( withdraw_amount) withdraw_amount
		, SUM( COALESCE(w.withdraw_usd, 0)) withdraw_usd
	FROM 
		deposit_ d 
		FULL OUTER JOIN 
			withdraw_ w 
			ON d.ap_account_id = w.ap_account_id 
			AND d.signup_hostcountry = w.signup_hostcountry 
			AND d.product_type = w.product_type 
			AND d.month_ = w.month_ 
			AND d.product_symbol = w.product_symbol 
	WHERE 
		COALESCE(d.month_, w.month_) >= '2021-05-01 00:00:00' -- DATE_TRUNC('month', NOW()) - '6 month'::INTERVAL --
	--	AND COALESCE(d.month_, w.month_) < '2021-02-01 00:00:00' --DATE_TRUNC('day', NOW())
	--	AND COALESCE (d.ap_account_id, w.ap_account_id) IN (709822,709823) --<<<<<< change test account HERE
		AND COALESCE (d.product_symbol, w.product_symbol) = 'BTC'
	GROUP BY 
		1,2,3,4
	ORDER BY 
		1,2 
)
SELECT
	m0
	, created_month
	, w.ap_account_id
	, b.product_group
	, b.signup_hostcountry
	, SUM(sum_trade_amount) sum_trade_amount
	, SUM(sum_trade_usd_amount) sum_trade_usd_amount
	, COALESCE (a.ziplock_amount, 0)::float ziplock_amount
	, COALESCE (deposit_amount, 0)::float deposit_amount
	, COALESCE (withdraw_amount, 0)::float withdraw_amount
--	, COUNT( DISTINCT CASE WHEN created_month = m0 THEN w.ap_account_id END) m0_repeat_trader
--	, COUNT( DISTINCT CASE WHEN created_month = m0 + '1 month'::INTERVAL THEN w.ap_account_id END) m1_repeat_trader
--	, COUNT( DISTINCT CASE WHEN created_month = m0 + '2 month'::INTERVAL THEN w.ap_account_id END) m2_repeat_trader
--	, COUNT( DISTINCT CASE WHEN created_month = m0 + '3 month'::INTERVAL THEN w.ap_account_id END) m3_repeat_trader
--	, COUNT( DISTINCT CASE WHEN created_month = m0 + '4 month'::INTERVAL THEN w.ap_account_id END) m4_repeat_trader
--	, COUNT( DISTINCT CASE WHEN created_month = m0 + '5 month'::INTERVAL THEN w.ap_account_id END) m5_repeat_trader
--	, SUM( CASE WHEN created_month = m0 THEN COALESCE(sum_trade_usd_amount, 0) END) m0_trade_volume
--	, SUM( CASE WHEN created_month = m0 + '1 month'::INTERVAL THEN COALESCE(sum_trade_usd_amount, 0) END) m1_trade_volume
--	, SUM( CASE WHEN created_month = m0 + '2 month'::INTERVAL THEN COALESCE(sum_trade_usd_amount, 0) END) m2_trade_volume
--	, SUM( CASE WHEN created_month = m0 + '3 month'::INTERVAL THEN COALESCE(sum_trade_usd_amount, 0) END) m3_trade_volume
--	, SUM( CASE WHEN created_month = m0 + '4 month'::INTERVAL THEN COALESCE(sum_trade_usd_amount, 0) END) m4_trade_volume
--	, SUM( CASE WHEN created_month = m0 + '5 month'::INTERVAL THEN COALESCE(sum_trade_usd_amount, 0) END) m5_trade_volume
FROM w0_trader w 
	INNER JOIN base b 
		ON w.ap_account_id = b.ap_account_id
	LEFT JOIN aum_snapshot a 
		ON w.ap_account_id = a.ap_account_id
		AND m0 = a.created_at
		AND w.product_group = a.symbol
	LEFT JOIN btc_deposit_withdraw d 
		ON w.ap_account_id = d.ap_account_id
		AND m0 = d.created_at
		AND w.product_group = d.symbol
WHERE 
	created_month >= m0
GROUP BY 1,2,3,4,5,8,9,10
;


-- monthly cohort by first traded month
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
		, CASE WHEN t.created_at < '2022-05-05' THEN
			(CASE WHEN t.counter_party IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping) THEN FALSE ELSE TRUE END)
			ELSE 
			(CASE WHEN t.counter_party IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id NOT IN (38121)) THEN FALSE	ELSE TRUE END)		
			END "is_organic_trade" 
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
		CASE WHEN t.created_at < '2022-05-05' THEN (t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping))
			ELSE (t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id NOT IN (38121)))
			END
		AND t.signup_hostcountry IN ('TH','ID','AU','global')
	GROUP BY
		1,2,3,4,5,6,7,8,9
	ORDER BY 1,2,3
)	, all_trade AS (
	SELECT * FROM zipmex_trade WHERE is_july_gaming IS FALSE
	UNION ALL
	SELECT * FROM pluang_trade
)	, all_trade_base AS (
	SELECT
		DATE_TRUNC('month', created_at)::DATE trade_month 
		, signup_hostcountry
		, ap_account_id
		, is_zmt_trade
		, SUM(sum_usd_trade_volume) sum_trade_usd
	FROM 
		all_trade
	WHERE 
		created_at >= '2021-01-01'
		AND created_at < '2022-04-01'
	GROUP BY 
		1,2,3,4
)	, first_trade_month AS (
	SELECT 
		ap_account_id
		, MIN(trade_month) first_trade_month
	FROM all_trade_base
	GROUP BY 1
)	, cohort_group AS (
	SELECT 
		a.*
		, f.first_trade_month
		, CASE WHEN trade_month = first_trade_month THEN 'm0'
				WHEN trade_month = first_trade_month + '1 month'::INTERVAL THEN 'm1'
				WHEN trade_month = first_trade_month + '2 month'::INTERVAL THEN 'm2'
				WHEN trade_month = first_trade_month + '3 month'::INTERVAL THEN 'm3'
				WHEN trade_month = first_trade_month + '4 month'::INTERVAL THEN 'm4'
				WHEN trade_month = first_trade_month + '5 month'::INTERVAL THEN 'm5'
				WHEN trade_month = first_trade_month + '6 month'::INTERVAL THEN 'm6'
				WHEN trade_month = first_trade_month + '7 month'::INTERVAL THEN 'm7'
				WHEN trade_month = first_trade_month + '8 month'::INTERVAL THEN 'm8'
				WHEN trade_month = first_trade_month + '9 month'::INTERVAL THEN 'm9'
				WHEN trade_month = first_trade_month + '10 month'::INTERVAL THEN 'm10'
				WHEN trade_month = first_trade_month + '11 month'::INTERVAL THEN 'm11'
				WHEN trade_month = first_trade_month + '12 month'::INTERVAL THEN 'm12'
				WHEN trade_month = first_trade_month + '13 month'::INTERVAL THEN 'm13'
				WHEN trade_month = first_trade_month + '14 month'::INTERVAL THEN 'm14'
				WHEN trade_month = first_trade_month + '15 month'::INTERVAL THEN 'm15'
			END AS cohort_group
	FROM 
		all_trade_base a
		LEFT JOIN first_trade_month f 
			ON a.ap_account_id = f.ap_account_id
)
SELECT
	first_trade_month
	, cohort_group
	, is_zmt_trade
	, SUM(sum_trade_usd) sum_trade_usd
FROM cohort_group
GROUP BY 1,2,3
ORDER BY 1,2,3
;

