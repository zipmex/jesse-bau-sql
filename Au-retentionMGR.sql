SELECT date_trunc('month',a.created_at) datamonth
	, signup_hostcountry 
	, account_id 
--	, SUM(amount) coin_balance 
	, SUM(usd_amount) aum_balance 
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
		    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', c.last_updated)
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
	WHERE a.created_at >= '2021-01-01 00:00:00' AND a.created_at < date_trunc('day', NOW()) --'2021-07-15 00:00:00' --<<<<<<<<CHANGE DATE HERE
	AND u.signup_hostcountry  NOT IN ('test', 'error','xbullion') 
	AND u.is_zipup_subscribed = FALSE -- Users didn't subscribed to ZipUp  <<<<<<<<<<<<<<<<<<<<<<<<< 
--	AND a.product_id IN (34,33) --(16,50)
--	AND a.product_id IN (1, 2, 3, 14, 15, 25, 26, 27, 30, 33, 34, 35) -- zipup AUM
	AND a.account_id NOT IN (0, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347) 
	GROUP BY 1,2,3,4,5,6,7,8,9 
	) a 
WHERE date_trunc('day',a.created_at) = date_trunc('day', NOW()) - '1 day'::INTERVAL 
GROUP BY 1,2,3 
ORDER BY 1 