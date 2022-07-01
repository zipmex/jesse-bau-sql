---- Work in Progress
WITH user_temp AS (
SELECT 
	u.id user_id 
	, u.email 
	, a.ap_account_id 
	, a.ap_user_id 
	,CASE	WHEN signup_hostname IN ('au.zipmex.com', 'trade.zipmex.com.au') 						THEN 'AU'
			WHEN signup_hostname IN ('id.zipmex.com', 'trade.zipmex.co.id') 						THEN 'ID'
			WHEN signup_hostname IN ('th.zipmex.com', 'trade.zipmex.co.th') 						THEN 'TH'
			WHEN signup_hostname IN ('sg.zipmex.com', 'exchange.zipmex.com', 'trade.zipmex.com') 	THEN 'global'
			WHEN signup_hostname IN ('trade.xbullion.io')											THEN 'xbullion'
			WHEN signup_hostname IN ('global-staging.zipmex.com', 'localhost')						THEN 'test'
			ELSE 'error'
	END "signup_hostcountry"
FROM 
	user_app_public.users u 
	LEFT JOIN user_app_public.alpha_point_users a 
	ON u.id = a.user_id 
), hour_trade AS (
SELECT 
--	DATE_TRUNC('day', t.converted_trade_time) trading_date
	DATE_TRUNC('hour', converted_trade_time) converted_trade_time
--	, u.ap_user_id 
	, t.account_id 
	, t.trade_id , order_id , t.execution_id , t.counter_party 
	, u.signup_hostcountry 
	, i.symbol --, is_block_trade 
--	, CASE WHEN ap_account_id = 1356 THEN TRUE ELSE FALSE END AS "is_seedfive"
--	, quantity , price 
--	, CASE WHEN RIGHT(i.symbol, 4) = 'USDT' THEN 'USD' ELSE RIGHT(i.symbol, 3) END AS base_fiat
	, SUM(t.quantity) quantity 
	, SUM(t.quantity * price) fiat_vol 
--	, SUM( CASE WHEN RIGHT(i.symbol,3) = 'USD' THEN (t.quantity * price) * 1 
--				WHEN RIGHT(i.symbol,4) = 'USDT' THEN (t.quantity * price) * 1
--				ELSE (t.quantity * price) * 1/COALESCE(e.exchange_rate,b.exchange_rate) 
--				END) AS usd_vol 
FROM 
	public.trades t 
	LEFT JOIN user_temp u 
		ON t.account_id = u.ap_account_id 
	LEFT JOIN mysql_replica_apex.instruments i 
		ON t.instrument_id = i.instrument_id 
	LEFT JOIN mysql_replica_apex.products p 
		ON i.product_1_id = p.product_id 
	LEFT JOIN public.cryptocurrency_prices_hourly h 
		ON DATE_TRUNC('hour', converted_trade_time) = DATE_TRUNC('hour', h.last_updated)
		AND i.symbol = h.instrument_symbol
		AND h."source" = 'coinmarketcap'
	LEFT JOIN public.exchange_rates e 
		ON RIGHT(i.symbol, 3) = RIGHT(e.instrument_symbol, 3) 
		AND DATE_TRUNC('day', converted_trade_time) - '1 day'::INTERVAL = DATE_TRUNC('day', e.created_at::timestamp) 
		AND e."source" = 'coinmarketcap'
		AND RIGHT(i.symbol, 3) <> 'THB'
	LEFT JOIN public.bank_of_thailand_usdthb_filled_holes b 
		ON DATE_TRUNC('day', converted_trade_time) - '1 day'::INTERVAL = DATE_TRUNC('day', b.created_at::timestamp)
		AND RIGHT(i.symbol, 3) = 'THB'
WHERE 
	u.signup_hostcountry IN ('ID') --('AU','ID','TH','global')
	AND t.is_block_trade = FALSE 
	AND t.account_id IS NOT NULL 
	AND t.account_id NOT IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227',27443
	,'38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','49659','49658','52018','52019','44057','161347')
--	AND t.side = 'Buy'
--	t.converted_trade_time >= DATE_TRUNC('hour', NOW()) - '24 hour'::INTERVAL -- '2021-08-01 00:00:00' 
--	AND t.converted_trade_time < DATE_TRUNC('day', NOW())
GROUP BY 1,2,3
ORDER BY 1 DESC 
)
SELECT 
	DATE_TRUNC('day', converted_trade_time) converted_trade_time
	, signup_hostcountry
	, symbol
	, SUM(quantity) quantity
	, SUM(fiat_vol) fiat_vol
	, SUM(usd_vol) usd_vol
FROM hour_trade 
GROUP BY 1,2,3
ORDER BY 1
;


---- trades_master raw --real-time data
WITH "trades_list" AS
	(
		SELECT
			t.trade_id
			,t.converted_trade_time "created_at"
			,t.order_id
			,t.execution_id
			,u.ap_user_id
			,u.ap_account_id
			,u.document_country
			,u.signup_hostcountry
			,t.counter_party
			,CASE	WHEN pt2.type_name = 'NationalCurrency' 	THEN 'crypto_to_fiat'
					WHEN pt2.type_name = 'CryptoCurrency' 		THEN 'crypto_to_crypto'
					ELSE 'error'
			END "trade_type"
			,t.instrument_id
			,i.symbol
			,i.product_1_id "product_1_id"
			,p1.symbol "product_1_symbol"
			,i.product_2_id "product_2_id"
			,p2.symbol "product_2_symbol"
			,pt2.type_name "product_2_type"
			,t.side
			,t.quantity
			,t.price
			,t.value
			,CASE	-- if trade in NationalCurrency keep trade volume as is
					WHEN pt2.type_name = 'NationalCurrency' THEN p2.symbol
					-- if trade in stable coin use fiat exchange rate
					WHEN p2.symbol = 'USDT' THEN 'USD'
					WHEN p2.symbol = 'IDRT' THEN 'IDR'
					-- if trade in CryptoCurrency use base_fiat from user signup_hostcountry to calculate trade volume for fiat
					WHEN pt2.type_name = 'CryptoCurrency' THEN u.base_fiat
					ELSE 'error'
			END "base_fiat"
		FROM
			oms_data.public.trades t
		LEFT JOIN
			oms_data.mysql_replica_apex.instruments i
			ON t.instrument_id = i.instrument_id
		LEFT JOIN
			oms_data.mysql_replica_apex.products p1
			ON i.product_1_id = p1.product_id
		LEFT JOIN
			oms_data.mysql_replica_apex.products p2
			ON i.product_2_id = p2.product_id
		LEFT JOIN
			oms_data.public.products_types pt2
			ON p2.type = pt2.id
		LEFT JOIN
			oms_data.analytics.users_master u
			ON t.account_id = u.ap_account_id
		WHERE
			-- remove test accounts
			(t.account_id NOT IN (186, 187, 869, 870, 1356, 1357) AND t.counter_party NOT IN ('186', '187', '869', '870', '1049', '1356', '1357'))
			/*
			ap_user_id		ap_account_id		user_name
			184			186				james+seedone@zipmex.com
			185			187				james+seedtwo@zipmex.com
			867			869				james+seedthree@zipmex.com
			868			870				james+seedfour@zipmex.com
			1354		1356			seedfive
			1355		1357			seedsix
			*/
	) 
	,"ref_pairs" AS
	(
		SELECT
			f.*
			-- derive crypto pair to convert CryptoCurrency trade volume into fiat
			-- if fiat (NationalCurrency) trade keep as base_fiat
			,CASE	WHEN f.base_fiat = 'error' THEN 'error'
					WHEN f.trade_type = 'crypto_to_fiat' THEN NULL
					-- if trade in stable coin use fiat exchange rate
					WHEN f.product_2_symbol = 'USDT' THEN NULL
					WHEN f.product_2_symbol = 'IDRT' THEN NULL
					WHEN f.trade_type = 'crypto_to_crypto' THEN CONCAT(f.product_1_symbol, f.base_fiat)
					ELSE 'error'
			END "cryptobase_pair"
			-- derive usd currency pair to convert trade volume to USD
			,CASE	WHEN f.base_fiat = 'error' THEN 'error'
					WHEN f.base_fiat = 'USD' THEN NULL
					ELSE CONCAT('USD', f.base_fiat)
			END "usdbase_pair"
		FROM
			trades_list f
	)
--	,"amount_type" AS	(
		SELECT
			f.*
			,CASE	-- -1 missing base_fiat cannot determine fiat to convert trade volume to
					WHEN f.trade_type = 'error' THEN -1
					WHEN f.trade_type = 'crypto_to_fiat' THEN
					-- 1 crypto_to_fiat trade in USD > keep trade volume in USD
						CASE	WHEN f.base_fiat = 'USD' THEN 1
					-- 2 crypto_to_fiat trade NOT in USD > requires conversion of trade volume to USD
								ELSE 2
						END
					WHEN f.trade_type = 'crypto_to_crypto' THEN
						CASE	
					-- stable coin trades
								WHEN f.product_2_symbol = 'USDT' THEN 1
								WHEN f.product_2_symbol = 'IDRT' THEN 2
					-- 3 crypto_to_crypto trade base_fiat in USD > convert crypto deposit amount to USD
								WHEN f.base_fiat = 'USD' THEN 3
					-- 4 crypto_to_crypto trade base_fiat NOT in USD > convert trade volume to base_fiat then convert to USD
								ELSE 4
						END
					ELSE -1
			END "amount_type"
		FROM
			ref_pairs f
		ORDER BY created_at DESC 
			
	)
	,"ref_rates" AS
	(
		SELECT
			f.*
			,CASE 	WHEN f.amount_type IN (3, 4) THEN c.average_high_low
					WHEN f.amount_type IN (1, 2) THEN 1
					ELSE NULL
			END "cryptobase_price"
			,CASE	WHEN f.amount_type IN (1, 3) THEN 1
					WHEN f.amount_type IN (2, 4) THEN COALESCE(e.exchange_rate, b.exchange_rate)
					ELSE NULL
			END "usdbase_rate"
		FROM
			amount_type f
		LEFT JOIN
			oms_data.public.cryptocurrency_prices c
			ON f.cryptobase_pair = c.instrument_symbol
			AND DATE_TRUNC('day', f.created_at) = DATE_TRUNC('day', c.last_updated)
		LEFT JOIN
			oms_data.public.exchange_rates e
			ON f.usdbase_pair = e.instrument_symbol
			AND DATE_TRUNC('day', f.created_at) = DATE_TRUNC('day', e.last_updated)
			AND f.usdbase_pair <> 'USDTHB'
		LEFT JOIN
			oms_data.public.bank_of_thailand_usdthb_filled_holes b
			ON DATE_TRUNC('day', f.created_at) = DATE_TRUNC('day', b.created_at)
			AND f.usdbase_pair = 'USDTHB'
	)
	, "amount_calc" AS
	(
		SELECT
			t.*
			,t.value * t.cryptobase_price "amount_base_fiat"
			,(t.value * t.cryptobase_price) / t.usdbase_rate "amount_usd"
		FROM
			ref_rates t
	)
SELECT
*
FROM amount_calc
WHERE amount_usd IS NULL 
AND created_at <= date_trunc('day', NOW())
ORDER BY created_at DESC 
LIMIT 100


SELECT
	DATE_TRUNC('day', created_at) trade_date 
	, signup_hostcountry 
--	, ap_account_id 
	, symbol 
--	, trade_id 
--	, order_id 
--	, counter_party 
	, SUM(quantity) quantity 
	, SUM(amount_base_fiat) amount_base_fiat
	, SUM(amount_usd) amount_usd 
FROM
	amount_calc t 
WHERE 
	symbol LIKE 'AXS%' 
	AND signup_hostcountry IN ('AU','ID','TH','global')
	AND ap_account_id IS NOT NULL 
	AND ap_account_id NOT IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227',27443
	,'38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','49659','49658','52018','52019','44057','161347')
GROUP BY 1,2,3 
;



SELECT
COUNT(*)
FROM public.trades
WHERE trade_time <= 637660511999990000 