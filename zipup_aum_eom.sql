-- ZIP UP AUM v.3 by end of month, non Zero balance with Staked and Non-ZMT by zipup subscribed date ----- 
WITH daily_user_balance AS (
	SELECT 
		created_at
		, account_id
		, signup_hostcountry 
--		, symbol
		, SUM(amount) amount 
		, SUM(usd_amount) AS usd_amount
	, avg(price) AS zmt_usd 
--	, avg(average_high_low) AS crypto_avg_price 
--	, avg(mid_price) as gold_avg_price 
	FROM (
	
		SELECT 
		--	DATE_TRUNC('day', a.created_at) AS created_at 
			CASE WHEN u.signup_hostcountry IS NULL THEN 'unknown' ELSE u.signup_hostcountry END AS signup_hostcountry
			, a.account_id  
			, CASE WHEN a.account_id IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id <> 496001) 
			THEN TRUE ELSE FALSE END AS is_nominee
			, CASE WHEN a.account_id = 496001 THEN TRUE ELSE FALSE END AS is_asset_manager -- this account holds z_wallet balance
		--	, p.symbol 
		--	, CASE -- WHEN a.product_id IN (1, 2, 3, 14, 15, 25, 26, 27, 30, 33, 34, 35) THEN 'zipup_coin'
		--			WHEN a.product_id IN (16, 50) THEN 'ZMT'
		--			ELSE 'other' END AS asset 
		--	, CASE WHEN u.is_zipup_subscribed = TRUE AND DATE_TRUNC('day',a.created_at) >= DATE_TRUNC('day', u.zipup_subscribed_at)
		--			AND a.product_id IN (1, 2, 3, 14, 15, 25, 26, 27, 30, 33, 34, 35, 16, 50) 
		--			THEN TRUE ELSE FALSE END AS is_zipup_amount
		--	, COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate) coin_price
			, COUNT(DISTINCT DATE_TRUNC('day', a.created_at) ) day_count
			, SUM(amount) amount 
			, SUM	(CASE WHEN DATE_TRUNC('day',a.created_at) < '2021-11-01 00:00:00' THEN 
					(CASE WHEN a.product_id = 6 THEN a.amount * 1
							ELSE a.amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate) END) 
					ELSE
					(CASE WHEN r.product_type = 1 THEN a.amount * 1/r.price
							WHEN r.product_type = 2 THEN a.amount * r.price END) 
					END ) AS usd_amount
		FROM oms_data_public.accounts_positions_daily a
			LEFT JOIN analytics.users_master u 
				ON a.account_id = u.ap_account_id  
			LEFT JOIN apex.products p
				ON a.product_id = p.product_id
			LEFT JOIN oms_data_public.cryptocurrency_prices c 
			    ON ((CONCAT(p.symbol, 'USD') = c.instrument_symbol) 
			    OR (c.instrument_symbol = 'MIOTAUSD' AND p.symbol ='IOTA') 
			    OR (c.instrument_symbol = 'USDPUSD' AND p.symbol ='PAX'))
			    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', c.last_updated) + '1 day'::INTERVAL
			LEFT join public.daily_closing_gold_prices g
				ON ((DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', g.created_at)) 
				OR (DATE_TRUNC('day', a.created_at) = '2021-07-31 00:00:00' AND DATE_TRUNC('day', g.created_at) = '2021-07-30 00:00:00'))
				AND a.product_id IN (15, 35)
			LEFT JOIN public.daily_ap_prices z
				ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', z.created_at)
				AND ((z.instrument_symbol = 'ZMTUSD' AND a.product_id in (16, 50))
				OR (z.instrument_symbol = 'C8PUSDT' AND p.symbol = 'C8P') 
				OR (z.instrument_symbol = 'TOKUSD' AND p.symbol = 'TOK') )
			LEFT JOIN oms_data_public.exchange_rates e
				ON date_trunc('day', e.created_at) = date_trunc('day', a.created_at)
				AND e.product_2_symbol  = p.symbol
				AND e."source" = 'coinmarketcap'
			LEFT JOIN analytics.rates_master r
				ON DATE_TRUNC('day',a.created_at) = r.created_at 
				AND p.symbol = r.product_1_symbol 
		WHERE
			DATE_TRUNC('day',a.created_at) >= '2021-01-01 00:00:00' -- CHANGE DATE HERE
			AND DATE_TRUNC('day',a.created_at) <  DATE_TRUNC('day', NOW())
			AND ((DATE_TRUNC('day', a.created_at) = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL ) OR (DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
			AND u.signup_hostcountry IN ('TH','ID','AU','global')
		GROUP BY 1,2,3,4


		) a
--	WHERE 	((created_at = DATE_TRUNC('month', created_at) + '1 month' - '1 day'::INTERVAL) OR (created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
	GROUP BY 1,2,3
	ORDER BY 1 DESC 
) , asset_holding AS (
	SELECT 
		date_trunc ('month' , d.created_at) created_at
	, d.account_id
	, d.signup_hostcountry 
--	, COALESCE(e.usd_amount,y.usd_amount) eom_aum 
	, COUNT(d.account_id) account_id_c
	, SUM(d.usd_amount) eom_aum
	, AVG(d.zmt_usd) as zmt_usd 
	FROM daily_user_balance d 
	GROUP BY 1,2,3
), staked_eom as ( ----- this section provide end of month zmt staked
		SELECT
			d.month ,u.ap_account_id account_id ,u.signup_hostcountry
			,SUM(s.amount) "zmt_staked_amount"
			,SUM(s.amount* c.price) "zmt_staked_usd_amount"
		FROM (
			SELECT DISTINCT 
				DATE_TRUNC('month', "date") + '1 MONTH - 1 day'::INTERVAL "month"
				,u.user_id
			FROM  GENERATE_SERIES('2020-12-01'::DATE, '2021-08-03'::DATE, '1 month') "date"
			CROSS JOIN (SELECT DISTINCT user_id FROM oms_data.user_app_public.zip_crew_stakes) u
			ORDER BY 1 ASC 
			) d --date_series
		LEFT JOIN oms_data.user_app_public.zip_crew_stakes s
			ON d.user_id = s.user_id
			AND DATE_TRUNC('day', d.month) >= DATE_TRUNC('day', s.staked_at)
			AND DATE_TRUNC('day', d.month) < COALESCE(DATE_TRUNC('day', s.released_at), DATE_TRUNC('day', s.releasing_at)) 
		LEFT JOIN oms_data.analytics.users_master u
			ON s.user_id = u.user_id
		LEFT JOIN oms_data.mysql_replica_apex.products p
			ON s.product_id = p.product_id
		-- join crypto usd prices
		LEFT JOIN oms_data.public.prices_eod_gmt0 c
			ON p.symbol = c.product_1_symbol
			AND c.product_2_symbol = 'USD'
			AND d.month = DATE_TRUNC('day', c.actual_timestamp)
			AND p."type" = 2
		WHERE u.ap_account_id IS NOT NULL
			AND u.is_zipup_subscribed = TRUE -- zipup users only 
			AND d.month >= date_trunc('month', u.zipup_subscribed_at) -- AUM balance starting after subcribed to zipup
			AND u.ap_account_id NOT IN (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347)
			AND u.signup_hostcountry IN ('TH','ID','AU','global')
		GROUP BY 1,2,3
), staked_yesterday AS (
		SELECT
			DATE_TRUNC('day',d.month) month_ 
			,u.ap_account_id account_id 
			,u.signup_hostcountry
			,SUM(s.amount) "zmt_staked_amount"
			,SUM(s.amount* c.price) "zmt_staked_usd_amount"
		FROM (
			SELECT DISTINCT date(DATE_TRUNC('day', date)) "month"
				,u.user_id
			FROM  GENERATE_SERIES('2020-12-01'::DATE, '2021-08-03'::DATE, '1 day') "date"
			CROSS JOIN (SELECT DISTINCT user_id FROM oms_data.user_app_public.zip_crew_stakes) u
			ORDER BY 1 ASC
			) d --date_series
		LEFT JOIN oms_data.user_app_public.zip_crew_stakes s
			ON d.user_id = s.user_id
			AND DATE_TRUNC('day', d.month) >= DATE_TRUNC('day', s.staked_at)
			AND DATE_TRUNC('day', d.month) < COALESCE(DATE_TRUNC('day', s.released_at), DATE_TRUNC('day', s.releasing_at)) 
		LEFT JOIN oms_data.analytics.users_master u
			ON s.user_id = u.user_id
		LEFT JOIN oms_data.mysql_replica_apex.products p
			ON s.product_id = p.product_id
		-- join crypto usd prices
		LEFT JOIN oms_data.public.prices_eod_gmt0 c
			ON p.symbol = c.product_1_symbol
			AND c.product_2_symbol = 'USD'
			AND d.month = DATE_TRUNC('day', c.actual_timestamp)
			AND p."type" = 2
		WHERE u.ap_account_id IS NOT NULL
			AND u.is_zipup_subscribed = TRUE -- zipup users only 
			AND d.month >= date_trunc('month', u.zipup_subscribed_at) -- AUM balance starting after subcribed to zipup
			AND u.ap_account_id NOT IN (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347)
			AND u.signup_hostcountry IN ('TH','ID','AU','global') 
			AND d.month = DATE_TRUNC('day',NOW()) - '1 day'::INTERVAL 
		GROUP BY 1,2,3
)	
SELECT 
	COALESCE(a.created_at, date_trunc('month',e.month), date_trunc('month',y.month_)) AS created_at 
--	, COALESCE(a.account_id, e.account_id, y.account_id) AS account_id
	, COALESCE(a.signup_hostcountry, e.signup_hostcountry, y.signup_hostcountry) signup_hostcountry
	, CASE WHEN a.account_id IN ('15',	'221',	'634',	'746',	'1002',	'1182',	'1202',	'1272',	'1708',	'6074',	'6828',	'11284',	'16293',	'19763',	'24108',	'24315',	'25431',	'37276',	'38526',	'39858',	'40119',	'40438',	'40890',	'48300',	'51313',	'51333',	'52266',	'54172',	'54231',	'54644',	'55224',	'55660',	'57262',	'58998',	'59049',	'59693',	'62663',	'63292',	'63314',	'63914',	'66402',	'67813',	'82129',	'84431',	'84461',	'84799',	'91297',	'92285',	'93791',	'94663',	'94993',	'96434',	'96535',	'101786',	'103488',	'103855',	'104832',	'106308',	'108014',	'127491',	'128405',	'131484',	'139503',	'141711',	'146194',	'146356',	'147984',	'157600',	'159685',	'161863',	'180376',	'183004')
		OR e.account_id IN ('15',	'221',	'634',	'746',	'1002',	'1182',	'1202',	'1272',	'1708',	'6074',	'6828',	'11284',	'16293',	'19763',	'24108',	'24315',	'25431',	'37276',	'38526',	'39858',	'40119',	'40438',	'40890',	'48300',	'51313',	'51333',	'52266',	'54172',	'54231',	'54644',	'55224',	'55660',	'57262',	'58998',	'59049',	'59693',	'62663',	'63292',	'63314',	'63914',	'66402',	'67813',	'82129',	'84431',	'84461',	'84799',	'91297',	'92285',	'93791',	'94663',	'94993',	'96434',	'96535',	'101786',	'103488',	'103855',	'104832',	'106308',	'108014',	'127491',	'128405',	'131484',	'139503',	'141711',	'146194',	'146356',	'147984',	'157600',	'159685',	'161863',	'180376',	'183004')
		OR y.account_id IN ('15',	'221',	'634',	'746',	'1002',	'1182',	'1202',	'1272',	'1708',	'6074',	'6828',	'11284',	'16293',	'19763',	'24108',	'24315',	'25431',	'37276',	'38526',	'39858',	'40119',	'40438',	'40890',	'48300',	'51313',	'51333',	'52266',	'54172',	'54231',	'54644',	'55224',	'55660',	'57262',	'58998',	'59049',	'59693',	'62663',	'63292',	'63314',	'63914',	'66402',	'67813',	'82129',	'84431',	'84461',	'84799',	'91297',	'92285',	'93791',	'94663',	'94993',	'96434',	'96535',	'101786',	'103488',	'103855',	'104832',	'106308',	'108014',	'127491',	'128405',	'131484',	'139503',	'141711',	'146194',	'146356',	'147984',	'157600',	'159685',	'161863',	'180376',	'183004')
			THEN TRUE ELSE FALSE END AS is_pcs
--	, account_id_c, usd_amount
	, SUM(eom_aum) eom_aum 
--	, account_id_c_l1y, usd_amount_l1y
	, SUM( COALESCE(e.zmt_staked_amount, y.zmt_staked_amount)) zmt_staked_amount
	, SUM( COALESCE(e.zmt_staked_usd_amount, y.zmt_staked_usd_amount)) zmt_staked_usd_amount
FROM asset_holding a 
	FULL OUTER JOIN staked_eom e 
		ON date_trunc('month',e.month) = a.created_at 
		AND e.account_id = a.account_id
	FULL OUTER JOIN staked_yesterday y 
		ON date_trunc('month',y.month_) = a.created_at 
		AND y.account_id = a.account_id
--WHERE coalesce(a.account_id, e.account_id, y.account_id) IN (143639)  
GROUP BY 1,2,3 
ORDER BY 1 DESC 
;






SELECT 
	DATE_TRUNC('day',a.created_at) AS created_at 
	, u.signup_hostcountry 
--	, a.product_id
--	, CASE WHEN a.account_id IN (0, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347, 317029) 
--			THEN TRUE ELSE FALSE END AS is_nominee
--	, CASE WHEN a.account_id = 496001 THEN TRUE ELSE FALSE END AS is_asset_manager -- this account holds z_wallet balance
--	, p.symbol 
	, CASE WHEN p.symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH') THEN 'zipup_coin'
			WHEN p.symbol IN ('ZMT') THEN 'ZMT'
			ELSE 'non_zipup' END AS asset_group
	, CASE WHEN u.is_zipup_subscribed = TRUE AND DATE_TRUNC('day',a.created_at) >= DATE_TRUNC('day', u.zipup_subscribed_at)
			AND a.product_id IN (1, 2, 3, 14, 15, 25, 26, 27, 30, 33, 34, 35, 16, 50) THEN TRUE ELSE FALSE END AS is_zipup_amount
	, SUM(amount) amount 
	, SUM( CASE WHEN p.symbol = 'USD' THEN amount * 1 
			WHEN r.product_type = 1 THEN amount * 1/r.price 
			WHEN r.product_type = 2 THEN amount * r.price 
			END) AS amount_usd
FROM public.accounts_positions_daily a
	LEFT JOIN analytics.users_master u 
		ON a.account_id = u.ap_account_id  
	LEFT JOIN apex.products p
		ON a.product_id = p.product_id
	LEFT JOIN 
		analytics.rates_master r 
		ON p.symbol = r.product_1_symbol 
	    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
WHERE
	DATE_TRUNC('day',a.created_at) >= '2021-09-01 00:00:00' 
--	DATE_TRUNC('day',a.created_at) = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL
	AND u.signup_hostcountry IN ('TH','ID','AU','global')
	AND a.account_id NOT IN (0, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347, 317029, 496001) 
	AND p.symbol NOT IN ('TST1','TST2')
GROUP BY 1,2,3,4



----PLUANG AUM - UTC 12 
WITH hourly_accumulated_balances AS (
	SELECT *
	FROM (
		SELECT * , date_trunc('day', created_at) AS thour
		, ROW_NUMBER() OVER(PARTITION BY user_id, product_id , date_trunc('day', created_at) ORDER BY created_at DESC) AS r
		FROM zipmex_otc_prod_public.accumulated_balances
		) t
	WHERE t.r = 1
)	--, plaung_aum AS (
SELECT
	thour, user_id, UPPER(h.product_id) symbol , h.balance, h.created_at, h.id
--	, h.balance * c.average_high_low usd_amount
	, CASE WHEN UPPER(h.product_id) = 'IDR' THEN h.balance * 1/e.exchange_rate ELSE h.balance * c.average_high_low END usd_amount
	, ROW_NUMBER() OVER(PARTITION BY user_id, UPPER(h.product_id), DATE_TRUNC('month', thour) ORDER BY thour DESC) rank_ 
FROM 
	hourly_accumulated_balances h 
	LEFT JOIN 
		oms_data_public.cryptocurrency_prices c 
	    ON CONCAT(UPPER(h.product_id), 'USD') = c.instrument_symbol
	    AND DATE_TRUNC('day', thour) = DATE_TRUNC('day', c.last_updated) + '1 day'::INTERVAL 
	LEFT JOIN 
		oms_data_public.exchange_rates e
		ON date_trunc('day', e.created_at) = DATE_TRUNC('day', thour)
		AND e.product_2_symbol = UPPER(h.product_id)
		AND e."source" = 'coinmarketcap'
WHERE
	user_id = '01F14GTKR63YS7QSPGCQDNVJRR'
--	AND extract(hour from thour) = 23
ORDER BY thour DESC, user_id, product_id
;

