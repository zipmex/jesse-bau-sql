---- new coins trade volume
SELECT DATE_TRUNC('month',created_at) datadate  
	, signup_hostcountry 
--	, CONCAT(product_1_symbol,product_2_symbol) symbol 
--	, instrument_id 
--	, CASE WHEN product_1_symbol IN ('SIX','ADA','DOGE','BNB','XLM','COMP','SAND') THEN product_1_symbol ELSE 'other' END AS product_symbol 
	, COUNT(DISTINCT ap_account_id) trader_count
	, SUM(quantity) quantity 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) trade_vol_usd
FROM analytics.trades_master t
WHERE signup_hostcountry  NOT IN ('test','error','xbullion')
AND ap_account_id NOT IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227','27443'
	,'38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','44057','161347','316078','44056','63152')
AND product_1_symbol IN ('ADA','DOGE','BNB','AAPL', 'AMZN', 'ABNB', 'FB', 'BABA', 'GOOGL', 'NFLX', 'PYPL', 'TWTR', 'TSLA', 'ZM')
--AND quantity IS NOT NULL AND amount_usd IS NOT NULL 
AND created_at >= '2021-06-15 00:00:00' 
GROUP BY 1,2
;



---- zipstocks trade volume by product_1_id 
SELECT 
	DATE_TRUNC('day',created_at) datadate 
	, signup_hostcountry 
	, product_1_symbol 
	, SUM(quantity) quantity 
	, SUM(amount_usd) trade_vol_usd 
FROM analytics.trades_master t 
WHERE product_1_id IN (17,21,72,19,67,18,20,69,70,68,71) --  product_1_symbol IN ('AAPL', 'AMZN', 'ABNB', 'FB', 'BABA', 'GOOGL', 'NFLX', 'PYPL', 'TWTR', 'TSLA', 'ZM')
AND date_trunc('day',created_at) >= '2021-06-24 00:00:00' 
GROUP BY 1,2,3
ORDER BY 2
;



---- zipstock by instrument_id 
WITH base AS (
SELECT 
	DATE_TRUNC('day', converted_trade_time) datadate 
	, u.signup_hostcountry 
--	, t.instrument_id 
	, LEFT(i.symbol,3) symbol 
	, SUM(t.quantity) quantity  
	, SUM(t.quantity * t.price) fiat_vol  
FROM oms_data.public.trades t 
	LEFT JOIN oms_data.mysql_replica_apex.instruments i 
		ON t.instrument_id = i.instrument_id 
	LEFT JOIN analytics.users_master u 
		ON t.account_id = u.account_id 
WHERE t.instrument_id IN (42,43,45,46,48,167,168,169,170,171,172) -- symbol IN ('AAPL', 'AMZN', 'ABNB', 'FB', 'BABA', 'GOOGL', 'NFLX', 'PYPL', 'TWTR', 'TSLA', 'ZM')
AND is_block_trade = FALSE 
GROUP BY 1,2,3
ORDER BY 2,1
)
SELECT a.datadate 
	, a.symbol 
	, SUM(quantity) quantity 
	, SUM(fiat_vol) fiat_vol 
	, SUM(a.fiat_vol * 1/e.exchange_rate) usd_amount 
FROM base a 
	LEFT JOIN public.exchange_rates e 
		ON a.datadate = DATE_TRUNC('day',e."createdAt") 
		AND e.product_2_symbol = 'IDR' 
		AND e."source" = 'coinmarketcap'
WHERE datadate >= '2021-06-23'
GROUP BY 1,2
ORDER BY 1 


