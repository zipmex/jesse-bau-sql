SELECT 
	DATE_TRUNC('day',a.created_at) AS created_at 
	, CASE WHEN u.signup_hostcountry IS NULL THEN 'unknown' ELSE u.signup_hostcountry END AS signup_hostcountry
--	, a.account_id  
	, CASE WHEN a.account_id IN (0, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347, 317029) 
	THEN TRUE ELSE FALSE END AS is_nominee
	, CASE WHEN a.account_id = 496001 THEN TRUE ELSE FALSE END AS is_asset_manager -- this account holds z_wallet balance
	, p.symbol 
--	, CASE WHEN a.product_id IN (1, 2, 3, 14, 15, 25, 26, 27, 30, 33, 34, 35) THEN 'zipup_coin'
--			WHEN a.product_id IN (16, 50) THEN 'ZMT'
--			ELSE 'non_zipup' END AS asset 
	, CASE WHEN u.is_zipup_subscribed = TRUE AND DATE_TRUNC('day',a.created_at) >= DATE_TRUNC('day', u.zipup_subscribed_at)
			AND a.product_id IN (1, 2, 3, 14, 15, 25, 26, 27, 30, 33, 34, 35, 16, 50) 
			THEN TRUE ELSE FALSE END AS is_zipup_amount
	, SUM(amount) amount 
	, SUM(CASE WHEN a.product_id = 6 THEN a.amount * 1
				ELSE a.amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate) END) AS usd_amount
FROM warehouse.public.accounts_positions_daily a
	LEFT JOIN analytics.users_master u 
		ON a.account_id = u.ap_account_id  
	LEFT JOIN apex.products p
		ON a.product_id = p.product_id
	LEFT JOIN oms_data_public.cryptocurrency_prices c 
	    ON ((CONCAT(p.symbol, 'USD') = c.instrument_symbol) OR (c.instrument_symbol = 'MIOTAUSD' AND p.symbol ='IOTA') OR (c.instrument_symbol = 'USDPUSD' AND p.symbol ='PAX'))
	    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', c.last_updated) + '1 day'::INTERVAL 
	LEFT join public.daily_closing_gold_prices g
		ON ((DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', g.created_at)) 
		OR (DATE_TRUNC('day', a.created_at) = '2021-07-31 00:00:00' AND DATE_TRUNC('day', g.created_at) = '2021-07-30 00:00:00'))
		AND a.product_id IN (15, 35)
	LEFT JOIN public.daily_ap_prices z
		ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', z.created_at) + '1 day'::INTERVAL
		AND ((z.instrument_symbol = 'ZMTUSD' AND p.symbol = 'ZMT')
		OR (z.instrument_symbol = 'C8PUSDT' AND p.symbol = 'C8P'))
	LEFT JOIN oms_data_public.exchange_rates e
		ON date_trunc('day', e.created_at) = date_trunc('day', a.created_at)
		AND e.product_2_symbol  = p.symbol
		AND e."source" = 'coinmarketcap'
WHERE
	DATE_TRUNC('day',a.created_at) >= '2021-09-01 00:00:00' -- CHANGE DATE HERE
	AND ((DATE_TRUNC('day', a.created_at) = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL ) OR (DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
--	AND account_id = 143639
GROUP BY 1,2,3,4,5,6
;


SELECT 
	DATE_TRUNC('day',a.created_at) AS created_at 
	, u.signup_hostcountry 
	, p.symbol 
	, SUM(amount) amount 
FROM 
	warehouse.oms_data_public.accounts_positions_daily a
	LEFT JOIN analytics.users_master u 
		ON a.account_id = u.ap_account_id  
	LEFT JOIN apex.products p
		ON a.product_id = p.product_id
WHERE
	a.created_at::DATE >= '2022-06-21' -- CHANGE DATE HERE
--		AND a.created_at::DATE < '2022-02-25'
	AND a.account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id NOT IN (38121)) 
--	AND a.account_id NOT IN (0, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347, 317029, 496001)
GROUP BY 1,2,3



