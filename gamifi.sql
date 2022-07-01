'somphol.pamoncanasavit@gmail.com'
'ppuang6@gmail.com'
'yakuza_nu@hotmail.com'
'teeptanin.j@gmail.com'
'dr.veerapong@gmail.com'
'antiga.pps@gmail.com'
'jumboboey@hotmail.com'
'wpiyanart@live.com'
'hydrathetentagon@gmail.com'
'aiai.pnw@gmail.com'

WITH gamer_profile AS (
SELECT 
	u.user_id 
	, ap_account_id 
	, u.created_at register_date 
	, COALESCE (first_name , p.info ->> 'first_name') first_name
	, COALESCE (last_name , p.info ->> 'last_name') last_name
	, email 
	, signup_hostcountry 
	, signup_hostname 
	, COALESCE (dob , (p.info ->> 'dob')::timestamp) dob
	, COALESCE (gender , p.info ->> 'gender') gender
	, COALESCE (document_country , p.info ->> 'address_in_id_card_country') document_country
	, COALESCE (document_type , p.info ->> 'document_type') document_type
FROM 
	analytics.users_master u
	LEFT JOIN 
		user_app_public.personal_infos p 
		ON u.user_id = p.user_id 
		AND p.archived_at IS NULL 
WHERE 
	email IN ( 'somphol.pamoncanasavit@gmail.com','ppuang6@gmail.com','yakuza_nu@hotmail.com','teeptanin.j@gmail.com','dr.veerapong@gmail.com',
'antiga.pps@gmail.com','jumboboey@hotmail.com','wpiyanart@live.com','hydrathetentagon@gmail.com','aiai.pnw@gmail.com')
ORDER BY 1
)	, trade_wallet_balance AS (
		SELECT 
			DATE_TRUNC('month',a.created_at) AS created_at 
			, u.* 
			, CASE WHEN a.product_id IN (1, 2, 3, 14, 15, 25, 26, 27, 30, 33, 34, 35) THEN 'zipup_coin'
					WHEN a.product_id IN (16,50) THEN 'ZMT'
					ELSE 'other' END AS asset_type 
--			, p.symbol 
			, SUM(amount) trade_wallet_amount 
			, SUM(CASE WHEN a.product_id = 6 THEN a.amount * 1 ELSE a.amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate, pu.price) END) trade_wallet_amount_usd
		FROM oms_data.public.accounts_positions_daily a
			LEFT JOIN gamer_profile u 
				ON a.account_id = u.ap_account_id  
			LEFT JOIN oms_data.mysql_replica_apex.products p
				ON a.product_id = p.product_id
			LEFT JOIN oms_data.public.cryptocurrency_prices c 
			    ON ((CONCAT(p.symbol, 'USD') = c.instrument_symbol) OR (c.instrument_symbol = 'MIOTAUSD' AND p.symbol ='IOTA'))
			    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', c.last_updated) + '1 day'::INTERVAL 
			LEFT join oms_data.public.daily_closing_gold_prices g
				ON ((DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', g.created_at)) 
				OR (DATE_TRUNC('day', a.created_at) = '2021-07-31 00:00:00' AND DATE_TRUNC('day', g.created_at) = '2021-07-30 00:00:00'))
				AND a.product_id IN (15, 35)
			LEFT JOIN oms_data.public.daily_ap_prices z
				ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', z.created_at)
				AND z.instrument_symbol  = 'ZMTUSD'
				AND a.product_id IN (16, 50)
			LEFT JOIN public.exchange_rates e
				ON date_trunc('day', e.created_at) = date_trunc('day', a.created_at)
				AND e.product_2_symbol  = p.symbol
				AND e."source" = 'coinmarketcap'
			LEFT JOIN (
				SELECT *, RANK() OVER(PARTITION BY product_1_symbol, product_2_symbol, DATE_TRUNC('day', "timestamp") ORDER BY "timestamp" DESC) rank_
				FROM public.prices_union
				) pu 
				ON pu.product_1_symbol = p.symbol AND pu.product_2_symbol = 'USDT'
				AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', pu."timestamp")
				AND pu.rank_ = 1 
		WHERE
			a.created_at >= '2021-07-01 00:00:00' 
			AND ((DATE_TRUNC('day', a.created_at) = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL ) OR (DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
			AND u.signup_hostcountry NOT IN ('test', 'error','xbullion')
			AND a.account_id NOT IN (0, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347, 317029, 496001) 
			AND p.symbol NOT IN ('TST1','TST2')
			AND u.ap_account_id IS NOT NULL 
--			AND a.product_id NOT IN (1, 2, 3, 14, 15, 25, 26, 27, 30, 33, 34, 35, 16, 50)
		GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)	, period_master AS (  
SELECT 
	p.created_at 
	, u.user_id  
	, p2.symbol
	, COALESCE ( c.average_high_low , g.mid_price , z.price ) coin_price
FROM 
	analytics.period_master p
	CROSS JOIN (SELECT DISTINCT user_id FROM gamer_profile ) u 
	CROSS JOIN (SELECT DISTINCT symbol FROM mysql_replica_apex.products --) p2
				WHERE symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')) p2
	LEFT JOIN 
		oms_data.public.cryptocurrency_prices c 
	    	ON CONCAT(p2.symbol, 'USD') = c.instrument_symbol
	    	AND DATE_TRUNC('day', p.created_at) = DATE_TRUNC('day', c.last_updated) + '1 day'::INTERVAL 
	LEFT JOIN 
		oms_data.public.daily_closing_gold_prices g
			ON DATE_TRUNC('day', p.created_at) = DATE_TRUNC('day', g.created_at)
			AND p2.symbol = 'GOLD'
	LEFT JOIN 
		oms_data.public.daily_ap_prices z
			ON DATE_TRUNC('day', p.created_at) = DATE_TRUNC('day', z.created_at) + '1 day'::INTERVAL 
			AND z.instrument_symbol  = 'ZMTUSD'
			AND p2.symbol = 'ZMT'
WHERE 
		p."period" = 'day' 
	AND p.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL
)	, z_wallet_balance AS (
	SELECT 
		d.created_at 
		, d.user_id 
		, CASE WHEN d.symbol = 'ZMT' THEN 'ZMT' ELSE 'zipup_coin' END AS asset_type  
		, SUM( CASE WHEN l.service_id = 'main_wallet' THEN COALESCE (credit,0) - COALESCE (debit,0) END) z_wallet_amount  
		, SUM( CASE WHEN l.service_id = 'zip_lock' THEN COALESCE (credit,0) - COALESCE (debit,0) END) ziplock_amount  
		, SUM( CASE WHEN l.service_id = 'main_wallet' THEN (COALESCE (credit,0) - COALESCE (debit,0)) * d.coin_price END) z_wallet_usd  
		, SUM( CASE WHEN l.service_id = 'zip_lock' THEN (COALESCE (credit,0) - COALESCE (debit,0)) * d.coin_price END) ziplock_usd  
	FROM period_master d 
		LEFT JOIN 
			asset_manager_public.ledgers l 
			ON d.user_id = l.account_id 
			AND d.created_at >= DATE_TRUNC('day', l.updated_at)
			AND d.symbol = UPPER(SPLIT_PART(l.product_id,'.',1))
	WHERE 
		l.account_id IS NOT NULL 
	GROUP BY 1,2,3
)
SELECT 
	t.*
	, COALESCE (z.z_wallet_usd, 0) z_wallet_usd
	, COALESCE (z.ziplock_usd, 0) ziplock_usd
FROM 
	trade_wallet_balance t 
	LEFT JOIN z_wallet_balance z 
	ON t.user_id = z.user_id 
	AND t.created_at = DATE_TRUNC('month', z.created_at)
	AND t.asset_type = z.asset_type
ORDER BY 1,3






---- trade info
SELECT 
	created_at 
	, ap_user_id 
	, ap_account_id 
	, counter_party 
	, trade_id 
	, order_id 
	, product_1_symbol 
	, price 
	, quantity 
	, base_fiat 
	, amount_base_fiat 
	, amount_usd 
FROM analytics.trades_master t
WHERE 
	t.ap_account_id IN (52826,54687,55796,56951,73926,85191,88108,140459,140652,152636)
	AND DATE_TRUNC('day', created_at) >= '2021-07-01 00:00:00'
	AND DATE_TRUNC('day', created_at) < '2021-08-01 00:00:00'
	AND t.counter_party NOT IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227','27443','317029'
,'38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','49659','49658','52018','52019','44057','161347')
	
	
	
	