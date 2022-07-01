-- crypto price fluctuation
WITH price_base AS (
	SELECT 
		DISTINCT 
		last_updated::DATE
		, instrument_symbol  
		, product_2_symbol 
		, AVG( CASE WHEN EXTRACT('hour' FROM last_updated::TIMESTAMP) = 0 THEN COALESCE("close", 0) END) am12
		, AVG( CASE WHEN EXTRACT('hour' FROM last_updated::TIMESTAMP) = 1 THEN "close" END) am1
		, AVG( CASE WHEN EXTRACT('hour' FROM last_updated::TIMESTAMP) = 2 THEN "close" END) am2
		, AVG( CASE WHEN EXTRACT('hour' FROM last_updated::TIMESTAMP) = 3 THEN "close" END) am3
		, AVG( CASE WHEN EXTRACT('hour' FROM last_updated::TIMESTAMP) = 4 THEN "close" END) am4
		, AVG( CASE WHEN EXTRACT('hour' FROM last_updated::TIMESTAMP) = 5 THEN "close" END) am5
		, AVG( CASE WHEN EXTRACT('hour' FROM last_updated::TIMESTAMP) = 6 THEN "close" END) am6
		, AVG( CASE WHEN EXTRACT('hour' FROM last_updated::TIMESTAMP) = 7 THEN "close" END) am7
		, AVG( CASE WHEN EXTRACT('hour' FROM last_updated::TIMESTAMP) = 8 THEN "close" END) am8
		, AVG( CASE WHEN EXTRACT('hour' FROM last_updated::TIMESTAMP) = 9 THEN "close" END) am9
		, AVG( CASE WHEN EXTRACT('hour' FROM last_updated::TIMESTAMP) = 10 THEN "close" END) am10
		, AVG( CASE WHEN EXTRACT('hour' FROM last_updated::TIMESTAMP) = 11 THEN "close" END) am11
		, AVG( CASE WHEN EXTRACT('hour' FROM last_updated::TIMESTAMP) = 12 THEN "close" END) pm12 
		, AVG( CASE WHEN EXTRACT('hour' FROM last_updated::TIMESTAMP) = 13 THEN "close" END) pm1 
		, AVG( CASE WHEN EXTRACT('hour' FROM last_updated::TIMESTAMP) = 14 THEN "close" END) pm2 
		, AVG( CASE WHEN EXTRACT('hour' FROM last_updated::TIMESTAMP) = 15 THEN "close" END) pm3
		, AVG( CASE WHEN EXTRACT('hour' FROM last_updated::TIMESTAMP) = 16 THEN "close" END) pm4
		, AVG( CASE WHEN EXTRACT('hour' FROM last_updated::TIMESTAMP) = 17 THEN "close" END) pm5
		, AVG( CASE WHEN EXTRACT('hour' FROM last_updated::TIMESTAMP) = 18 THEN "close" END) pm6
		, AVG( CASE WHEN EXTRACT('hour' FROM last_updated::TIMESTAMP) = 19 THEN "close" END) pm7
		, AVG( CASE WHEN EXTRACT('hour' FROM last_updated::TIMESTAMP) = 20 THEN "close" END) pm8
		, AVG( CASE WHEN EXTRACT('hour' FROM last_updated::TIMESTAMP) = 21 THEN "close" END) pm9
		, AVG( CASE WHEN EXTRACT('hour' FROM last_updated::TIMESTAMP) = 22 THEN "close" END) pm10
		, AVG( CASE WHEN EXTRACT('hour' FROM last_updated::TIMESTAMP) = 23 THEN "close" END) pm11
	FROM 
--		oms_data_public.cryptocurrency_prices_hourly cph 
		(	SELECT 
				DISTINCT 
				DATE_TRUNC('minute', inserted_at)::TIMESTAMP last_updated
				, instrument_symbol 
				, product_2_symbol 
				, price "close"
				, ROW_NUMBER() OVER(PARTITION BY instrument_symbol, DATE_TRUNC('hour', inserted_at) ORDER BY inserted_at DESC) row_ 
			FROM oms_data_public.ap_prices ap 
			WHERE 
				product_2_symbol = 'USD'
				AND inserted_at >= '2022-01-01' 
			ORDER BY 1 DESC ) cph
	WHERE 
--		product_1_symbol IN ('BTC','ETH','LTC') AND 
		product_2_symbol = 'USD' 
		AND row_ = 1 AND "close" <> 0
		AND last_updated >= '2022-01-01'
	GROUP BY 1,2,3
)	, price_lag AS (
	SELECT 
		last_updated
		, instrument_symbol
		, am12
		, LAG(am12) OVER(PARTITION BY instrument_symbol ORDER BY last_updated) pre_am12
		, am1
		, LAG(am1) OVER(PARTITION BY instrument_symbol ORDER BY last_updated) pre_am1
		, am2
		, LAG(am2) OVER(PARTITION BY instrument_symbol ORDER BY last_updated) pre_am2
		, am3
		, LAG(am3) OVER(PARTITION BY instrument_symbol ORDER BY last_updated) pre_am3
		, am4
		, LAG(am4) OVER(PARTITION BY instrument_symbol ORDER BY last_updated) pre_am4
		, am5
		, LAG(am5) OVER(PARTITION BY instrument_symbol ORDER BY last_updated) pre_am5
		, am6
		, LAG(am6) OVER(PARTITION BY instrument_symbol ORDER BY last_updated) pre_am6
		, am7
		, LAG(am7) OVER(PARTITION BY instrument_symbol ORDER BY last_updated) pre_am7
		, am8
		, LAG(am8) OVER(PARTITION BY instrument_symbol ORDER BY last_updated) pre_am8
		, am9
		, LAG(am9) OVER(PARTITION BY instrument_symbol ORDER BY last_updated) pre_am9
		, am10
		, LAG(am10) OVER(PARTITION BY instrument_symbol ORDER BY last_updated) pre_am10
		, am11
		, LAG(am11) OVER(PARTITION BY instrument_symbol ORDER BY last_updated) pre_am11
		, pm12
		, LAG(pm12) OVER(PARTITION BY instrument_symbol ORDER BY last_updated) pre_pm12
		, pm1
		, LAG(pm1) OVER(PARTITION BY instrument_symbol ORDER BY last_updated) pre_pm1
		, pm2
		, LAG(pm2) OVER(PARTITION BY instrument_symbol ORDER BY last_updated) pre_pm2
		, pm3
		, LAG(pm3) OVER(PARTITION BY instrument_symbol ORDER BY last_updated) pre_pm3
		, pm4
		, LAG(pm4) OVER(PARTITION BY instrument_symbol ORDER BY last_updated) pre_pm4
		, pm5
		, LAG(pm5) OVER(PARTITION BY instrument_symbol ORDER BY last_updated) pre_pm5
		, pm6
		, LAG(pm6) OVER(PARTITION BY instrument_symbol ORDER BY last_updated) pre_pm6
		, pm7
		, LAG(pm7) OVER(PARTITION BY instrument_symbol ORDER BY last_updated) pre_pm7
		, pm8
		, LAG(pm8) OVER(PARTITION BY instrument_symbol ORDER BY last_updated) pre_pm8
		, pm9
		, LAG(pm9) OVER(PARTITION BY instrument_symbol ORDER BY last_updated) pre_pm9
		, pm10
		, LAG(pm10) OVER(PARTITION BY instrument_symbol ORDER BY last_updated) pre_pm10
		, pm11
		, LAG(pm11) OVER(PARTITION BY instrument_symbol ORDER BY last_updated) pre_pm11
	FROM price_base
)--	, fluctuate_percent AS (
	SELECT 
		last_updated
		, instrument_symbol
		, ROUND (( CASE WHEN pre_am12 IS NULL THEN NULL ELSE 
				( CASE WHEN pre_am12 < am12 
						THEN (am12 - pre_am12) / am12 
						ELSE (pre_am12 - am12) / am12 END) * 100.0
			END)::NUMERIC, 2) fluct_am12
		, ROUND (( CASE WHEN pre_am1 IS NULL THEN NULL ELSE 
				( CASE WHEN pre_am1 < am1 
						THEN (am1 - pre_am1) / am1 
						ELSE (pre_am1 - am1) / am1 END) * 100.0
			END)::NUMERIC, 2) fluct_am1
		, ROUND (( CASE WHEN pre_am2 IS NULL THEN NULL ELSE 
				( CASE WHEN pre_am2 < am2 
						THEN (am2 - pre_am2) / am2 
						ELSE (pre_am2 - am2) / am2 END) * 100.0
			END)::NUMERIC, 2) fluct_am2
		, ROUND (( CASE WHEN pre_am3 IS NULL THEN NULL ELSE 
				( CASE WHEN pre_am3 < am3 
						THEN (am3 - pre_am3) / am3 
						ELSE (pre_am3 - am3) / am3 END) * 100.0
			END)::NUMERIC, 2) fluct_am3
		, ROUND (( CASE WHEN pre_am4 IS NULL THEN NULL ELSE 
				( CASE WHEN pre_am4 < am4 
						THEN (am4 - pre_am4) / am4 
						ELSE (pre_am4 - am4) / am4 END) * 100.0
			END)::NUMERIC, 2) fluct_am4
		, ROUND (( CASE WHEN pre_am5 IS NULL THEN NULL ELSE 
				( CASE WHEN pre_am5 < am5 
						THEN (am5 - pre_am5) / am5 
						ELSE (pre_am5 - am5) / am5 END) * 100.0
			END)::NUMERIC, 2) fluct_am5
		, ROUND (( CASE WHEN pre_am6 IS NULL THEN NULL ELSE 
				( CASE WHEN pre_am6 < am6 
						THEN (am6 - pre_am6) / am6 
						ELSE (pre_am6 - am6) / am6 END) * 100.0
			END)::NUMERIC, 2) fluct_am6
		, ROUND (( CASE WHEN pre_am7 IS NULL THEN NULL ELSE 
				( CASE WHEN pre_am7 < am7 
						THEN (am7 - pre_am7) / am7 
						ELSE (pre_am7 - am7) / am7 END) * 100.0
			END)::NUMERIC, 2) fluct_am7
		, ROUND (( CASE WHEN pre_am8 IS NULL THEN NULL ELSE 
				( CASE WHEN pre_am8 < am8 
						THEN (am8 - pre_am8) / am8 
						ELSE (pre_am8 - am8) / am8 END) * 100.0
			END)::NUMERIC, 2) fluct_am8
		, ROUND (( CASE WHEN pre_am9 IS NULL THEN NULL ELSE 
				( CASE WHEN pre_am9 < am9 
						THEN (am9 - pre_am9) / am9 
						ELSE (pre_am9 - am9) / am9 END) * 100.0
			END)::NUMERIC, 2) fluct_am9
		, ROUND (( CASE WHEN pre_am10 IS NULL THEN NULL ELSE 
				( CASE WHEN pre_am10 < am10 
						THEN (am10 - pre_am10) / am10 
						ELSE (pre_am10 - am10) / am10 END) * 100.0
			END)::NUMERIC, 2) fluct_am10
		, ROUND (( CASE WHEN pre_am11 IS NULL THEN NULL ELSE 
				( CASE WHEN pre_am11 < am11 
						THEN (am11 - pre_am11) / am11 
						ELSE (pre_am11 - am11) / am11 END) * 100.0
			END)::NUMERIC, 2) fluct_am11
		, ROUND (( CASE WHEN pre_pm12 IS NULL THEN NULL ELSE 
				( CASE WHEN pre_pm12 < pm12 
						THEN (pm12 - pre_pm12) / pm12 
						ELSE (pre_pm12 - pm12) / pm12 END) * 100.0
			END)::NUMERIC, 2) fluct_pm12
		, ROUND (( CASE WHEN pre_pm1 IS NULL THEN NULL ELSE 
				( CASE WHEN pre_pm1 < pm1 
						THEN (pm1 - pre_pm1) / pm1 
						ELSE (pre_pm1 - pm1) / pm1 END) * 100.0
			END)::NUMERIC, 2) fluct_pm1
		, ROUND (( CASE WHEN pre_pm2 IS NULL THEN NULL ELSE 
				( CASE WHEN pre_pm2 < pm2 
						THEN (pm2 - pre_pm2) / pm2 
						ELSE (pre_pm2 - pm2) / pm2 END) * 100.0
			END)::NUMERIC, 2) fluct_pm2
		, ROUND (( CASE WHEN pre_pm3 IS NULL THEN NULL ELSE 
				( CASE WHEN pre_pm3 < pm3 
						THEN (pm3 - pre_pm3) / pm3 
						ELSE (pre_pm3 - pm3) / pm3 END) * 100.0
			END)::NUMERIC, 2) fluct_pm3
		, ROUND (( CASE WHEN pre_pm4 IS NULL THEN NULL ELSE 
				( CASE WHEN pre_pm4 < pm4 
						THEN (pm4 - pre_pm4) / pm4 
						ELSE (pre_pm4 - pm4) / pm4 END) * 100.0
			END)::NUMERIC, 2) fluct_pm4
		, ROUND (( CASE WHEN pre_pm5 IS NULL THEN NULL ELSE 
				( CASE WHEN pre_pm5 < pm5 
						THEN (pm5 - pre_pm5) / pm5 
						ELSE (pre_pm5 - pm5) / pm5 END) * 100.0
			END)::NUMERIC, 2) fluct_pm5
		, ROUND (( CASE WHEN pre_pm6 IS NULL THEN NULL ELSE 
				( CASE WHEN pre_pm6 < pm6 
						THEN (pm6 - pre_pm6) / pm6 
						ELSE (pre_pm6 - pm6) / pm6 END) * 100.0
			END)::NUMERIC, 2) fluct_pm6
		, ROUND (( CASE WHEN pre_pm7 IS NULL THEN NULL ELSE 
				( CASE WHEN pre_pm7 < pm7 
						THEN (pm7 - pre_pm7) / pm7 
						ELSE (pre_pm7 - pm7) / pm7 END) * 100.0
			END)::NUMERIC, 2) fluct_pm7
		, ROUND (( CASE WHEN pre_pm8 IS NULL THEN NULL ELSE 
				( CASE WHEN pre_pm8 < pm8 
						THEN (pm8 - pre_pm8) / pm8 
						ELSE (pre_pm8 - pm8) / pm8 END) * 100.0
			END)::NUMERIC, 2) fluct_pm8
		, ROUND (( CASE WHEN pre_pm9 IS NULL THEN NULL ELSE 
				( CASE WHEN pre_pm9 < pm9 
						THEN (pm9 - pre_pm9) / pm9 
						ELSE (pre_pm9 - pm9) / pm9 END) * 100.0
			END)::NUMERIC, 2) fluct_pm9
		, ROUND (( CASE WHEN pre_pm10 IS NULL THEN NULL ELSE 
				( CASE WHEN pre_pm10 < pm10 
						THEN (pm10 - pre_pm10) / pm10 
						ELSE (pre_pm10 - pm10) / pm10 END) * 100.0
			END)::NUMERIC, 2) fluct_pm10
		, ROUND (( CASE WHEN pre_pm11 IS NULL THEN NULL ELSE 
				( CASE WHEN pre_pm11 < pm11 
						THEN (pm11 - pre_pm11) / pm11 
						ELSE (pre_pm11 - pm11) / pm11 END) * 100.0
			END)::NUMERIC, 2) fluct_pm11
	FROM price_lag

)
SELECT 
--	last_updated
	instrument_symbol
	, CASE WHEN fluct_am2 >= 5 THEN '>_5p' 
	WHEN fluct_am2 >= 10 THEN '>_10p' 
	WHEN fluct_am2 >= 15 THEN '>_15p' 
	WHEN fluct_am2 >= 20 THEN '>_20p' 
		ELSE '<_5p'
		END AS fluct_group
	, COUNT(fluct_am2) fluct_count
FROM fluctuate_percent
GROUP BY 1,2
;



WITH price_base AS (

	SELECT 
		DISTINCT 
		DATE_TRUNC('minute', inserted_at)::TIMESTAMP last_updated
		, instrument_symbol 
		, product_2_symbol 
		, price 
		, ROW_NUMBER() OVER(PARTITION BY instrument_symbol, DATE_TRUNC('hour', inserted_at) ORDER BY inserted_at DESC) row_ 
	FROM oms_data_public.ap_prices ap 
	WHERE 
		product_2_symbol = 'USD'
		AND inserted_at >= '2022-01-01' 
		AND price = 0
	ORDER BY 1 DESC 
--	GROUP BY 1,2,3 
	
)	, lag_base AS (
	SELECT 
		*
		, LAG(hourly_close_price) OVER( PARTITION BY instrument_symbol ORDER BY last_updated ) pre_price
	FROM price_base
)	, fluctuate_percent AS (
	SELECT 
		*
		, CASE WHEN hourly_close_price = 0 THEN NULL ELSE 
				ROUND((((CASE WHEN (pre_price - hourly_close_price) < 0 THEN (pre_price - hourly_close_price)*(-1) 
				ELSE (pre_price - hourly_close_price) END) / hourly_close_price::NUMERIC )* 100.0)::NUMERIC, 2) 
			END hourly_fluct_percent
	FROM 
		lag_base
)	, fluct_group AS (
	SELECT 
		*
		, CASE WHEN hourly_fluct_percent < 5 THEN 'A_<_5p'
			WHEN hourly_fluct_percent >= 5 AND hourly_fluct_percent < 10 THEN 'B_5-10p'
			WHEN hourly_fluct_percent >= 10 AND hourly_fluct_percent < 15 THEN 'C_10-15p'
			WHEN hourly_fluct_percent >= 15 AND hourly_fluct_percent < 20 THEN 'D_15-20p'
			WHEN hourly_fluct_percent IS NULL THEN NULL
			ELSE 'E_>_20p'
			END AS hourly_fluct_group
	FROM fluctuate_percent
)
SELECT 
	DATE_TRUNC('month', last_updated)::DATE last_updated 
	, hourly_fluct_group
	, instrument_symbol 
	, COUNT(hourly_fluct_percent) fluct_freq
FROM fluct_group
GROUP BY 1,2,3
ORDER BY 1,2
;







---- never deposit/ withdraw crypto
WITH base_deposit AS (
SELECT account_id
	, COUNT(CASE WHEN product_type = 'CryptoCurrency' THEN ticket_id END) AS crypto_d_count
	, COUNT(CASE WHEN product_type = 'NationalCurrency' THEN ticket_id END) AS fiat_d_count
	, SUM(amount_usd) amount_usd
FROM analytics.deposit_tickets_master d 
GROUP BY 1
), base_withdraw AS (
SELECT account_id
	, COUNT(CASE WHEN product_type = 'CryptoCurrency' THEN ticket_id END) AS crypto_wd_count
	, COUNT(CASE WHEN product_type = 'NationalCurrency' THEN ticket_id END) AS fiat_wd_count
	, SUM(amount_usd) amount_usd
FROM analytics.withdraw_tickets_master w  
GROUP BY 1
), final_list AS (
SELECT COALESCE(d.account_id, w.account_id) account_id
	, COALESCE(crypto_d_count,0) crypto_d_count
	, COALESCE(crypto_wd_count,0) crypto_wd_count
	, d.amount_usd deposit_vol 
	, w.amount_usd withdrawal_vol 
FROM base_deposit d 
	FULL OUTER JOIN base_withdraw w 
		ON d.account_id = w.account_id 
), user_list AS (
SELECT DISTINCT 
	CASE WHEN crypto_d_count = 0 AND crypto_wd_count = 0 THEN account_id END AS account_id 
FROM final_list 
WHERE account_id NOT IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225'
			,'25226','25227','38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','49659'
			,'49658','52018','52019','44057','161347')
ORDER BY 1 
)--, trade_report AS (
SELECT t.created_at 
	, a.account_id
	, t.product_1_symbol
	, t.price/ t.usdbase_rate usd_price 
	, SUM( CASE WHEN t.side = 'Buy' THEN t.quantity END) AS buy_coin_vol
	, SUM( CASE WHEN t.side = 'Buy' THEN t.amount_usd END) AS buy_usd_vol
	, SUM( CASE WHEN t.side = 'Sell' THEN t.quantity END) AS sell_coin_vol
	, SUM( CASE WHEN t.side = 'Sell' THEN t.amount_usd END) AS sell_usd_vol 
	, SUM( f.fee_usd_amount) sum_fee_usd
FROM user_list a 
	LEFT JOIN analytics.trades_master t 
		ON a.account_id = t.account_id
	LEFT JOIN analytics.fees_master f 
		ON t.account_id = f.account_id 
		AND t.execution_id = f.fee_reference_id
		AND DATE_TRUNC('day', t.created_at) = DATE_TRUNC('day', f.created_at)
WHERE DATE_TRUNC('day', t.created_at) >= '2021-01-01 00:00:00'
AND DATE_TRUNC('day', t.created_at) < '2021-04-01 00:00:00'
AND a.account_id = 87971
GROUP BY 1,2,3,4 
ORDER BY 1,2


), wallet_balance AS (
SELECT date_trunc('day',a.created_at) datamonth
	, signup_hostcountry 
	, account_id 
	, symbol 
	, SUM(amount) quantity 
	, SUM(usd_amount) as usd_amount 
FROM (
	SELECT date_trunc('day',a.created_at) AS created_at ,a.account_id , a.product_id, p.symbol, u.signup_hostcountry 
		, c.average_high_low , g.mid_price , z.price, 1/e.exchange_rate as exchange_rate 
		,SUM(amount) amount 
		,SUM(a.amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate)) usd_amount
	FROM oms_data.public.accounts_positions_daily a
		LEFT JOIN analytics.users_master u on a.account_id = u.account_id 
		LEFT JOIN oms_data.mysql_replica_apex.products p
			ON a.product_id = p.product_id
		LEFT JOIN oms_data.public.cryptocurrency_prices c 
		    ON CONCAT(p.symbol, 'USD') = c.instrument_symbol
		    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', c.created_at)
		LEFT JOIN oms_data.public.daily_closing_gold_prices g
			ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', g.created_at)
			AND a.product_id IN (15,	 35)
		LEFT JOIN oms_data.public.daily_ap_prices z
			ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', z.created_at)
			AND z.instrument_symbol  = 'ZMTUSD'
			AND a.product_id in (16, 50)
		LEFT JOIN public.exchange_rates e
			ON date_trunc('day', e.created_at) = date_trunc('day', a.created_at)
			AND e.product_2_symbol  = p.symbol
			AND e.source = 'coinmarketcap'
	WHERE a.created_at >= '2021-01-01 00:00:00' AND a.created_at < '2021-07-08 00:00:00' --<<<<<<<<CHANGE DATE HERE
	AND u.signup_hostcountry  NOT IN ('test', 'error','xbullion') 
	AND a.account_id NOT IN (0, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347) 
	GROUP BY 1,2,3,4,5,6,7,8,9 
	) a
WHERE created_at = date_trunc('month',a.created_at)
GROUP BY 1,2,3,4
ORDER BY 1 DESC  
)
SELECT b.datamonth
	, a.account_id
	, b.symbol
	, b.quantity
	, b.usd_amount
FROM user_list a 
	LEFT JOIN wallet_balance b ON a.account_id = b.account_id 
WHERE datamonth >= '2021-01-01 00:00:00'
AND datamonth < '2021-05-01 00:00:00'



SELECT *
FROM analytics.fees_master f 
WHERE fee_reference_id = 33632045 

SELECT *
FROM analytics.trades_master tm 
WHERE execution_id = 33632045


SELECT t.created_at 
	, t.ap_account_id
	, t.product_1_symbol
	, t.price/ t.usdbase_rate usd_price 
	, SUM( CASE WHEN t.side = 'Buy' THEN t.quantity END) AS buy_coin_vol
	, SUM( CASE WHEN t.side = 'Buy' THEN t.amount_usd END) AS buy_usd_vol
	, SUM( CASE WHEN t.side = 'Sell' THEN t.quantity END) AS sell_coin_vol
	, SUM( CASE WHEN t.side = 'Sell' THEN t.amount_usd END) AS sell_usd_vol 
	, SUM( f.fee_usd_amount) sum_fee_usd
FROM analytics.trades_master t 
	LEFT JOIN analytics.fees_master f 
		ON t.ap_account_id = f.ap_account_id 
		AND t.execution_id = f.fee_reference_id
		AND DATE_TRUNC('day', t.created_at) = DATE_TRUNC('day', f.created_at)
WHERE DATE_TRUNC('day', t.created_at) >= '2021-01-01 00:00:00'
AND DATE_TRUNC('day', t.created_at) < '2021-04-01 00:00:00'
AND t.ap_account_id = 87971
GROUP BY 1,2,3,4 
ORDER BY 1,2



SELECT date_trunc('day',a.created_at) datadate 
	, signup_hostcountry 
	, account_id 
	, symbol 
	, thbusd_rate
	, SUM(amount) quantity 
	, SUM(usd_amount) AS usd_amount 
	, SUM(usd_amount * thbusd_rate) AS fiat_amount
FROM (
	SELECT date_trunc('day',a.created_at) AS created_at ,a.account_id , a.product_id, p.symbol, u.signup_hostcountry 
		, c.average_high_low , g.mid_price , z.price, 1/e.exchange_rate as exchange_rate 
		, e2.exchange_rate thbusd_rate 
		,SUM(amount) amount 
		,SUM(CASE WHEN a.product_id = 6 THEN a.amount * 1
			ELSE a.amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate) END) usd_amount
	FROM oms_data.public.accounts_positions_daily a
		LEFT JOIN analytics.users_master u on a.account_id = u.ap_account_id 
		LEFT JOIN oms_data.mysql_replica_apex.products p
			ON a.product_id = p.product_id
		LEFT JOIN oms_data.public.cryptocurrency_prices c -- crypto price EXCLUDING ZMT, GOLD 
		    ON CONCAT(p.symbol, 'USD') = c.instrument_symbol
		    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', c.last_updated)
		LEFT JOIN oms_data.public.daily_closing_gold_prices g -- GOLD price 
			ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', g.created_at)
			AND a.product_id IN (15,	 35)
		LEFT JOIN oms_data.public.daily_ap_prices z -- ZMT price
			ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', z.created_at)
			AND z.instrument_symbol  = 'ZMTUSD'
			AND a.product_id in (16, 50)
		LEFT JOIN public.exchange_rates e -- USD exchange rate
			ON date_trunc('day', e.created_at) = date_trunc('day', a.created_at)
			AND e.product_2_symbol  = p.symbol
			AND e.source = 'coinmarketcap'
		LEFT JOIN public.exchange_rates e2 -- getting LOCAL fiat exchange rate TO CONVERT FROM USD
			ON date_trunc('day', e2.created_at) = date_trunc('day', a.created_at)
			AND e2.product_2_symbol  = 'THB'
			AND e2.source = 'coinmarketcap'
	WHERE a.created_at >= '2021-01-01 00:00:00' AND a.created_at < '2021-05-01 00:00:00' --<<<<<<<<CHANGE DATE HERE
	AND u.signup_hostcountry  NOT IN ('test', 'error','xbullion') 
	AND a.account_id NOT IN (0, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347) 
	GROUP BY 1,2,3,4,5,6,7,8,9,10
	) a 
WHERE account_id = 87971
GROUP BY 1,2,3,4,5
ORDER BY 1 