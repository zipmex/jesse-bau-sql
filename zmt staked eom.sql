------zmt staked monthly
WITH "date_series" AS
(
	SELECT
		DISTINCT
		date(DATE_TRUNC('month', date)) + INTERVAL '1 MONTH - 1 day' "month"
		,u.user_id
	FROM 
		GENERATE_SERIES('2020-12-01'::DATE, '2021-08-04'::DATE, '1 month') "date"
	CROSS JOIN
		(SELECT DISTINCT user_id FROM user_app_public.zip_crew_stakes) u
	ORDER BY
		1 ASC
)	--, staked_final as (
SELECT
	d.month
	,u.signup_hostcountry
--	, CASE WHEN u.ap_account_id IN (63312,63313,161347,40706,38260,37955,37807,38263,40683,38262,38121,27308,48870,48948,0)
--			THEN TRUE ELSE FALSE END AS is_nominee
--	,u.ap_account_id
--	,c.price 
	,SUM(s.amount) "zmt_staked_amount"
	,SUM(s.amount* z.price) "zmt_staked_usd_amount"
FROM
	date_series d
LEFT JOIN
	user_app_public.zip_crew_stakes s
	ON d.user_id = s.user_id
	AND DATE_TRUNC('day', d.month) >= DATE_TRUNC('day', s.staked_at)
	AND DATE_TRUNC('day', d.month) < COALESCE(DATE_TRUNC('day', s.released_at), '2021-08-04 00:00:00') --COALESCE(DATE_TRUNC('day', s.released_at), DATE_TRUNC('day', s.releasing_at)) 
LEFT JOIN
	analytics.users_master u
	ON s.user_id = u.user_id
LEFT JOIN
	apex.products p
	ON s.product_id = p.product_id
-- join crypto usd prices
LEFT JOIN public.daily_ap_prices z
	ON DATE_TRUNC('day', d.month) = DATE_TRUNC('day', z.created_at) + '1 day'::INTERVAL 
	AND z.instrument_symbol = 'ZMTUSD'
WHERE
	u.ap_account_id IS NOT NULL
	and u.ap_account_id NOT IN (63312,63313,161347,40706,38260,37955,37807,38263,40683,38262,38121,27308,48870,48948,0)
	AND u.signup_hostcountry in ('TH','ID','AU','global') 
--	AND u.ap_account_id = 143639 
GROUP BY
	1,2
ORDER BY 1 DESC 

)
SELECT 
	*
	, CASE 	WHEN zmt_staked_amount >= 20000 THEN 'Zip_Crew'
			WHEN zmt_staked_amount >= 100 AND zmt_staked_amount < 20000 THEN 'Zip_Member' 
--			WHEN zmt_staked_amount >= 0 AND zmt_staked_amount < 100 THEN 'Zip_Starter'
			ELSE 'Zip_Starter'
			END AS user_tier 
FROM 
	staked_final

 







--- membership level 1st of month - double wallet
WITH period_master AS (  
SELECT 
	p.created_at 
	, u.user_id 
	, u.ap_account_id 
	, u.signup_hostcountry 
	, p2.symbol
FROM 
	analytics.period_master p
	CROSS JOIN ( ---- getting USER info FROM users_master 
				SELECT DISTINCT user_id , ap_account_id, signup_hostcountry FROM analytics.users_master ) u 
	CROSS JOIN (SELECT DISTINCT symbol FROM mysql_replica_apex.products --) p2
				WHERE symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')) p2
WHERE 
		p."period" = 'day' 
	AND p.created_at = DATE_TRUNC('month', NOW()) 
--	AND u.ap_account_id = 143639 ----- TEST ACCOUNT HERE
)	
	, zmt_stake_balance AS (
	SELECT 
		d.created_at 
		, CASE WHEN d.signup_hostcountry IS NULL THEN 'unknown' ELSE d.signup_hostcountry END AS signup_hostcountry
		, d.ap_account_id
		, CASE WHEN d.ap_account_id IN (0, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347, 317029, 496001)
			THEN TRUE ELSE FALSE END AS is_nominee
		, d.symbol
		, l.service_id 
		, SUM( COALESCE (credit,0) - COALESCE (debit,0) ) amount  
	FROM period_master d 
		LEFT JOIN 
			asset_manager_public.ledgers l 
			ON d.user_id = l.account_id 
			AND d.created_at >= DATE_TRUNC('day', l.updated_at)
			AND d.symbol = UPPER(SPLIT_PART(l.product_id,'.',1))
		LEFT JOIN
			oms_data.analytics.users_master u
			ON l.account_id = u.user_id
	WHERE 
		l.account_id IS NOT NULL 
		AND l.service_id = 'zip_lock'
		AND d.symbol = 'ZMT'
	GROUP BY 1,2,3,4,5,6
)
SELECT 
	z.created_at
	, z.ap_account_id
	, CASE WHEN amount >= 100 AND amount < 20000 THEN 'Zip_Member' 
			WHEN amount >= 20000 THEN 'Zip_Crew'
			ELSE 'Zip_Starter'
			END AS membership_level 
	, SUM(t.amount_usd) trade_vol
FROM zmt_stake_balance z 
	LEFT JOIN analytics.trades_master t
	ON z.created_at = DATE_TRUNC('month', t.created_at)
	AND z.ap_account_id = t.ap_account_id 
WHERE 
	is_nominee = FALSE
	AND z.signup_hostcountry = 'global'
	AND amount >= 20000 
	AND t.amount_usd IS NOT NULL 
GROUP BY 1,2,3