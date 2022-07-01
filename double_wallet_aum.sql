---- double wallet country level 
WITH period_master AS (  
SELECT 
	p.created_at 
	, u.user_id 
	, u.ap_account_id 
	, u.signup_hostcountry 
	, p2.symbol
	, COALESCE ( c.average_high_low , g.mid_price , z.price ) coin_price
FROM 
	analytics.period_master p
	CROSS JOIN ( ---- getting USER info FROM users_master 
				SELECT DISTINCT user_id , ap_account_id, signup_hostcountry FROM analytics.users_master ) u 
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
	AND p.created_at >= '2021-01-01 00:00:00'
	AND p.created_at < DATE_TRUNC('day', NOW()) 
	AND ((p.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL) OR (p.created_at = DATE_TRUNC('month', p.created_at) + '1 month - 1 day'::INTERVAL))	
	AND	u.signup_hostcountry NOT IN ('test', 'error','xbullion')
	AND u.ap_account_id NOT IN (0, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347, 317029, 496001)
--	AND u.ap_account_id = 143639 ----- TEST ACCOUNT HERE
)	
	, z_wallet_balance AS (
	SELECT 
		d.created_at 
		, d.signup_hostcountry 
		, d.ap_account_id
		, d.symbol  
		, COALESCE (SUM( CASE WHEN l.service_id = 'main_wallet' THEN credit - debit END), 0) zipup_balance 
		, COALESCE (SUM( CASE WHEN l.service_id = 'zip_lock' THEN credit - debit END), 0) zlock_balance 
	FROM period_master d 
		LEFT JOIN asset_manager_public.ledgers l 
			ON d.user_id = l.account_id 
			AND d.created_at >= DATE_TRUNC('day', l.updated_at)
			AND d.symbol = UPPER(SPLIT_PART(l.product_id,'.',1))
		LEFT JOIN
			oms_data.analytics.users_master u
			ON l.account_id = u.user_id
	WHERE 
		d.created_at >= '2021-08-04 00:00:00'
		AND l.service_id IS NOT NULL 
		AND u.is_zipup_subscribed = TRUE 
		AND d.created_at >= DATE_TRUNC('day', u.zipup_subscribed_at)
	GROUP BY 1,2,3,4
)	
	, zmt_staked_single AS (
		SELECT
			d.created_at 
			,d.signup_hostcountry
			,d.ap_account_id
			,d.symbol 
			,SUM(s.amount) "zmt_staked_amount"
		FROM
			period_master d
		LEFT JOIN
			oms_data.mysql_replica_apex.products p
			ON d.symbol = p.symbol
		LEFT JOIN
			oms_data.user_app_public.zip_crew_stakes s
			ON d.user_id = s.user_id
			AND DATE_TRUNC('day', d.created_at) >= DATE_TRUNC('day', s.staked_at)
			AND DATE_TRUNC('day', d.created_at) < DATE_TRUNC('day', COALESCE(s.released_at, '2021-08-04 00:00:00')) --COALESCE(DATE_TRUNC('day', s.released_at), DATE_TRUNC('day', s.releasing_at)) 
			AND p.product_id = s.product_id 
		LEFT JOIN
			oms_data.analytics.users_master u
			ON s.user_id = u.user_id
		WHERE
			amount IS NOT NULL 
--			d.ap_account_id IS NOT NULL
			AND d.symbol = 'ZMT'
			AND u.is_zipup_subscribed = TRUE 
			AND d.created_at >= DATE_TRUNC('day', u.zipup_subscribed_at)
		GROUP BY
			1,2,3,4
		ORDER BY 1 DESC 
)	
	, trade_w_balance AS (
		SELECT
			d.created_at
			, d.signup_hostcountry 
			, d.ap_account_id 
--			, a.product_id
			, d.symbol 
			, SUM(amount) coin_balance  
		FROM 
			period_master d 
			LEFT JOIN oms_data.mysql_replica_apex.products p 
				ON d.symbol = p.symbol
			LEFT JOIN 
				oms_data.public.accounts_positions_daily a
				ON d.ap_account_id = a.account_id 
				AND d.created_at = DATE_TRUNC('day', a.created_at)
				AND a.product_id = p.product_id
			LEFT JOIN analytics.users_master u 
				ON a.account_id = u.ap_account_id  
		WHERE 
			amount IS NOT NULL 
			AND a.product_id IN (1, 2, 3, 14, 15, 25, 26, 27, 30, 33, 34, 35, 16, 50) --<<<<<<<<<<========= run this for BTC, USDT, USDC, GOLD, LTC, ETH only, without ZMT 
			AND u.is_zipup_subscribed = TRUE 
			AND d.created_at >= DATE_TRUNC('day', u.zipup_subscribed_at)
		GROUP BY 1,2,3,4
		ORDER BY 1 DESC 
)	
	, coin_balance AS (
	SELECT 
		d.created_at 
		, d.signup_hostcountry 
		, d.ap_account_id 
--		, d.symbol
		, CASE WHEN d.symbol = 'ZMT' THEN 'ZMT' ELSE 'other' END AS symbol 
--		, d.coin_price
		, SUM( COALESCE (a.coin_balance, 0)) tw_balance
		, SUM( COALESCE (w.zipup_balance, 0)) zw_balance 
		, SUM( COALESCE (a.coin_balance,0) + COALESCE (w.zipup_balance, 0)) zipup_balance
		, SUM( COALESCE(z.zmt_staked_amount, w.zlock_balance,0)) zlock_balance
		, SUM( (COALESCE (a.coin_balance,0) + COALESCE (w.zipup_balance, 0)) * d.coin_price) zipup_usd_balance
		, SUM( COALESCE(w.zlock_balance, z.zmt_staked_amount ,0) * coin_price) zlock_usd_balance
	FROM 
		period_master d 
		LEFT JOIN 
			trade_w_balance a 
			ON d.ap_account_id = a.ap_account_id 
			AND d.signup_hostcountry = a.signup_hostcountry
			AND d.created_at = a.created_at 
			AND d.symbol = a.symbol
		LEFT JOIN 
			zmt_staked_single z 
			ON d.ap_account_id = z.ap_account_id
			AND d.created_at = z.created_at
			AND d.signup_hostcountry = z.signup_hostcountry
			AND d.symbol = z.symbol
		LEFT JOIN 
			z_wallet_balance w 
			ON d.ap_account_id = w.ap_account_id
			AND d.created_at = w.created_at 
			AND d.signup_hostcountry = w.signup_hostcountry
			AND d.symbol = w.symbol
	WHERE 
		((a.coin_balance IS NOT NULL) OR (z.zmt_staked_amount IS NOT NULL) OR (w.zipup_balance IS NOT NULL) OR (w.zlock_balance IS NOT NULL))
	GROUP BY 1,2,3,4
	ORDER BY 1 DESC 
)
	, usd_balance AS (
	SELECT 
		DATE_TRUNC('day', created_at) created_at 
		, signup_hostcountry
		, ap_account_id 
		, CASE WHEN ap_account_id IN ('15',	'221',	'634',	'746',	'1002',	'1182',	'1202',	'1272',	'1708',	'6074',	'6828',	'11284',	'16293',	'19763',	'24108',	'24315',	'25431',	'37276',	'38526',	'39858',	'40119',	'40438',	'40890',	'48300',	'51313',	'51333',	'52266',	'54172',	'54231',	'54644',	'55224',	'55660',	'57262',	'58998',	'59049',	'59693',	'62663',	'63292',	'63314',	'63914',	'66402',	'67813',	'82129',	'84431',	'84461',	'84799',	'91297',	'92285',	'93791',	'94663',	'94993',	'96434',	'96535',	'101654',	'101786',	'103488',	'103855',	'104832',	'106308',	'108014',	'127491',	'128405',	'131484',	'139503',	'141711',	'146194',	'146356',	'147984',	'157600',	'159685',	'161863',	'180376',	'183004')
			THEN TRUE ELSE FALSE END AS is_pcs
--		, COALESCE(SUM(CASE WHEN symbol = 'ZMT' THEN zipup_balance END), 0) zipup_zmt_coin_balance 
--		, COALESCE(SUM(CASE WHEN symbol = 'ZMT' THEN zipup_usd_balance END), 0) zipup_zmt_usd_balance 
		, COALESCE(SUM(CASE WHEN symbol <> 'ZMT' THEN zipup_balance END), 0) zipup_nonzmt_coin_balance 
		, COALESCE(SUM(CASE WHEN symbol <> 'ZMT' THEN zipup_usd_balance END), 0) zipup_nonzmt_usd_balance 
		, COALESCE(SUM(CASE WHEN symbol = 'ZMT' THEN zlock_balance END), 0) lock_zmt_balance 
		, COALESCE(SUM(CASE WHEN symbol = 'ZMT' THEN zlock_usd_balance END), 0) lock_zmt_usd_balance
		, COALESCE(SUM(CASE WHEN symbol <> 'ZMT' THEN zlock_balance END), 0) lock_nonzmt_balance 
		, COALESCE(SUM(CASE WHEN symbol <> 'ZMT' THEN zlock_usd_balance END), 0) lock_nonzmt_usd_balance 
	FROM 
		coin_balance 
	GROUP BY 1,2,3,4
	ORDER BY 1 DESC 
)
SELECT 
	created_at 
	, signup_hostcountry 
	, is_pcs
--	, SUM(zipup_zmt_usd_balance) zipup_zmt_usd_balance
	, SUM(zipup_nonzmt_usd_balance) zipup_nonzmt_usd_balance
	, SUM(lock_nonzmt_usd_balance) lock_nonzmt_usd_balance
	, SUM(lock_zmt_usd_balance) lock_zmt_usd_balance
	, COUNT(DISTINCT ap_account_id) user_count
	, COUNT(DISTINCT CASE WHEN zipup_nonzmt_usd_balance >= 1 THEN ap_account_id END) zipup_user_count
	, COUNT(DISTINCT CASE WHEN (lock_zmt_usd_balance >= 1 OR lock_nonzmt_usd_balance >= 1) THEN ap_account_id END) zlock_user_count
	, COUNT(DISTINCT CASE WHEN lock_zmt_usd_balance >= 1 AND lock_nonzmt_usd_balance >= 1 THEN ap_account_id END) zlock_both_count
	, COUNT(DISTINCT CASE WHEN lock_zmt_usd_balance < 1 AND lock_nonzmt_usd_balance >= 1 THEN ap_account_id END) nonzmt_lock_count
	, COUNT(DISTINCT CASE WHEN lock_zmt_usd_balance >= 1 AND lock_nonzmt_usd_balance < 1 THEN ap_account_id END) zmt_lock_count 
FROM 
	usd_balance  
GROUP BY 1,2,3
ORDER BY 1 DESC 
;



---- double wallet asset level
WITH period_master AS (  
SELECT 
	p.created_at 
	, u.user_id 
	, u.ap_account_id 
	, u.signup_hostcountry 
	, p2.symbol
	, COALESCE ( c.average_high_low , g.mid_price , z.price ) coin_price
FROM 
	analytics.period_master p
	CROSS JOIN ( ---- getting USER info FROM users_master 
				SELECT DISTINCT user_id , ap_account_id, signup_hostcountry FROM analytics.users_master u ) u 
	CROSS JOIN (SELECT DISTINCT symbol FROM mysql_replica_apex.products
				WHERE symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')) p2
	LEFT JOIN 
		oms_data.public.cryptocurrency_prices c 
	    	ON ((CONCAT(p2.symbol, 'USD') = c.instrument_symbol) OR (c.instrument_symbol = 'MIOTAUSD' AND p2.symbol = 'IOTA'))
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
	AND p.created_at >= '2021-01-01 00:00:00'
	AND p.created_at < DATE_TRUNC('day', NOW()) 
	AND p.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL	
	AND	u.signup_hostcountry NOT IN ('test', 'error','xbullion')
	AND u.ap_account_id NOT IN (0, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347, 317029, 496001)
	AND u.ap_account_id = 53073 ----- TEST ACCOUNT HERE
)	
	, z_wallet_balance AS (
	SELECT 
		d.created_at 
		, d.signup_hostcountry 
		, d.ap_account_id
		, d.symbol  
		, SUM( CASE WHEN l.service_id = 'main_wallet' THEN credit - debit END) zipup_balance 
		, SUM( CASE WHEN l.service_id = 'zip_lock' THEN credit - debit END) zlock_balance 
	FROM period_master d 
		LEFT JOIN asset_manager_public.ledgers l 
			ON d.user_id = l.account_id 
			AND d.created_at >= DATE_TRUNC('day', l.updated_at)
			AND d.symbol = UPPER(SPLIT_PART(l.product_id,'.',1))
		LEFT JOIN
			oms_data.analytics.users_master u
			ON l.account_id = u.user_id
	WHERE 
		d.created_at >= '2021-08-04 00:00:00'
		AND l.service_id IS NOT NULL 
		AND u.is_zipup_subscribed = TRUE 
		AND d.created_at >= DATE_TRUNC('day', u.zipup_subscribed_at)
	GROUP BY 1,2,3,4
)	
	, zmt_staked_single AS (
		SELECT
			d.created_at 
			,d.signup_hostcountry
			,d.ap_account_id
			,d.symbol 
			,SUM(s.amount) "zmt_staked_amount"
		FROM
			period_master d
		LEFT JOIN
			oms_data.mysql_replica_apex.products p
			ON d.symbol = p.symbol
		LEFT JOIN
			oms_data.user_app_public.zip_crew_stakes s
			ON d.user_id = s.user_id
			AND DATE_TRUNC('day', d.created_at) >= DATE_TRUNC('day', s.staked_at)
			AND DATE_TRUNC('day', d.created_at) < COALESCE(DATE_TRUNC('day', s.released_at), '2021-08-04 00:00:00') --COALESCE(DATE_TRUNC('day', s.released_at), DATE_TRUNC('day', s.releasing_at)) 
			AND p.product_id = s.product_id 
		LEFT JOIN
			oms_data.analytics.users_master u
			ON s.user_id = u.user_id
		WHERE
			d.ap_account_id IS NOT NULL
			AND d.symbol = 'ZMT'
			AND u.is_zipup_subscribed = TRUE 
			AND d.created_at >= DATE_TRUNC('day', u.zipup_subscribed_at)
		GROUP BY
			1,2,3,4
		ORDER BY 1 DESC 
)	
	, trade_w_balance AS (
		SELECT
			d.created_at
			, d.signup_hostcountry 
			, d.ap_account_id 
--			, a.product_id
			, p.symbol 
			, SUM(amount) coin_balance  
		FROM 
			period_master d 
			LEFT JOIN oms_data.mysql_replica_apex.products p 
				ON d.symbol = p.symbol
			LEFT JOIN 
				oms_data.public.accounts_positions_daily a
				ON d.ap_account_id = a.account_id 
				AND d.created_at = DATE_TRUNC('day', a.created_at)
				AND a.product_id = p.product_id
			LEFT JOIN analytics.users_master u 
				ON a.account_id = u.ap_account_id  
		WHERE 
			a.product_id IN (1, 2, 3, 14, 15, 25, 26, 27, 30, 33, 34, 35, 16, 50) --<<<<<<<<<<========= run this for BTC, USDT, USDC, GOLD, LTC, ETH only, without ZMT 
			AND u.is_zipup_subscribed = TRUE 
			AND d.created_at >= DATE_TRUNC('day', u.zipup_subscribed_at)
		GROUP BY 1,2,3,4
		ORDER BY 1 DESC 
)	
SELECT 
	d.created_at 
	, d.signup_hostcountry
--		, d.ap_account_id 
	, d.symbol
--		, CASE WHEN d.symbol = 'ZMT' THEN 'ZMT' ELSE 'other' END AS symbol 
	, COUNT(DISTINCT d.ap_account_id) user_count 
	, SUM( COALESCE (a.coin_balance, 0)) tw_balance
	, SUM( COALESCE (w.zipup_balance, 0)) zw_balance 
	, SUM( COALESCE (a.coin_balance,0) + COALESCE (w.zipup_balance, 0)) zipup_balance
	, SUM( COALESCE(z.zmt_staked_amount, w.zlock_balance,0)) zlock_balance
	, SUM( (COALESCE (a.coin_balance,0) + COALESCE (w.zipup_balance, 0)) * d.coin_price) zipup_usd_balance
	, SUM( COALESCE(w.zlock_balance, z.zmt_staked_amount, 0) * coin_price) zlock_usd_balance
FROM 
	period_master d 
	LEFT JOIN 
		trade_w_balance a 
		ON d.ap_account_id = a.ap_account_id 
		AND d.signup_hostcountry = a.signup_hostcountry
		AND d.created_at = a.created_at 
		AND d.symbol = a.symbol
	LEFT JOIN 
		zmt_staked_single z 
		ON d.ap_account_id = z.ap_account_id
		AND d.created_at = z.created_at
		AND d.signup_hostcountry = z.signup_hostcountry
		AND d.symbol = z.symbol
	LEFT JOIN 
		z_wallet_balance w 
		ON d.ap_account_id = w.ap_account_id
		AND d.created_at = w.created_at
		AND d.signup_hostcountry = w.signup_hostcountry
		AND d.symbol = w.symbol
--	WHERE 		d.symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
GROUP BY 1,2,3
ORDER BY 1 DESC 
;