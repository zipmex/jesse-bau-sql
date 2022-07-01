SELECT DATE_TRUNC('month',created_at) datamonth 
	, account_id 
	, SUM(quantity) buy_volume
	, SUM(amount_base_fiat) buy_vol_fiat 
	, SUM(amount_usd) buy_vol_usd 
FROM analytics.trades_master t 
WHERE side = 'Buy' 
AND signup_hostcountry NOT IN ('test','error','xbullion','AU','ID')
AND DATE_TRUNC('day',created_at) >= '2021-06-01 00:00:00' 
AND DATE_TRUNC('day',created_at) < '2021-06-25 00:00:00' 
AND account_id NOT IN NOT IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225'
			,'25226','25227','38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','49659'
			,'49658','52018','52019','44057','161347') 
GROUP BY 1,2 
ORDER BY 1 



SELECT DATE_TRUNC('month',created_at) datamonth 
	, account_id 
	, product_1_id 
	, SUM(quantity) buy_volume
	, SUM(amount_base_fiat) vol_fiat 
	, SUM(amount_usd) vol_usd 
FROM analytics.trades_master t 
WHERE side = 'Buy'
AND product_1_symbol = 'USDC'
AND product_2_symbol = 'THB'
AND signup_hostcountry NOT IN ('test','error','xbullion','AU','ID')
AND DATE_TRUNC('day',created_at) >= '2021-06-01 00:00:00' 
AND DATE_TRUNC('day',created_at) < '2021-07-01 00:00:00' 
AND account_id NOT IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225'
			,'25226','25227','38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','49659'
			,'49658','52018','52019','44057','161347') 
GROUP BY 1,2,3
ORDER BY 1 


SELECT date_trunc('month',a.created_at) datamonth
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
	WHERE a.created_at >= '2021-07-07 00:00:00' AND a.created_at < '2021-07-08 00:00:00' --<<<<<<<<CHANGE DATE HERE
	AND u.signup_hostcountry NOT IN ('test', 'error','xbullion','AU','ID') 
	AND a.product_id IN (34)
	AND a.account_id NOT IN (0, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347) 
	GROUP BY 1,2,3,4,5,6,7,8,9 
	) a
GROUP BY 1,2,3,4
ORDER BY 1 DESC  