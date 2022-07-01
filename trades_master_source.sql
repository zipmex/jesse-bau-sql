-- trades_master_2021
WITH "trades_list" AS
	(
		SELECT
			t.trade_id
			,tick_to_timestamp(t.trade_time) "created_at"
			,t.order_id
			,t.execution_id
			,u.ap_user_id
			,u.ap_account_id
			,u.document_country
			,u.signup_hostcountry
			,t.counter_party_account_id
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
			,CASE WHEN t.side = 0 THEN 'Buy' WHEN t.side = 1 THEN 'Sell' END AS side
			,cast(t.quantity as numeric(60, 30))
			,cast(t.price as numeric(60, 30))
			,cast(t.value as numeric(60, 30))
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
			warehouse.apex.oms_trades t
		LEFT JOIN
			warehouse.apex.instruments i
			ON t.instrument_id = i.instrument_id
		LEFT JOIN
			warehouse.apex.products p1
			ON i.product_1_id = p1.product_id
		LEFT JOIN
			warehouse.apex.products p2
			ON i.product_2_id = p2.product_id
		LEFT JOIN
			warehouse.oms_data_public.products_types pt2
			ON p2.type = pt2.id
		LEFT JOIN
			warehouse.analytics.users_master u
			ON t.account_id = u.ap_account_id
		WHERE
			-- remove test accounts
			(t.account_id NOT IN (186, 187, 869, 870, 1356, 1357) AND t.counter_party_account_id NOT IN ('186', '187', '869', '870', '1049', '1356', '1357'))
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
	,"amount_type" AS
	(
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
			warehouse.oms_data_public.cryptocurrency_prices c
			ON f.cryptobase_pair = c.instrument_symbol
			AND DATE_TRUNC('day',f.created_at) = DATE_TRUNC('day', c.last_updated)
		LEFT JOIN
			warehouse.oms_data_public.exchange_rates e
			ON f.usdbase_pair = e.instrument_symbol
			AND DATE_TRUNC('day', f.created_at) = DATE_TRUNC('day', e.last_updated)
			AND f.usdbase_pair <> 'USDTHB'
		LEFT JOIN
			warehouse.public.bank_of_thailand_usdthb_filled_holes b
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
FROM
	amount_calc
WHERE 
	product_1_symbol IN ('ADA','BNB','GALA','SUSHI','GRT','SLP','SOL','DOT','ATOM','LUNA','RUNE','AVAX','TRX','ALGO','XTZ')
	AND ap_account_id NOT IN (SELECT ap_account_id FROM mappings.users_mapping um)
	AND created_at >= '2021-10-28 00:00:00'
;


