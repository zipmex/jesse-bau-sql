--- z_wallet balance
WITH base AS (
	SELECT 
		d.created_at::DATE created_at
		, u.ap_account_id
		, UPPER(SPLIT_PART(l.product_id,'.',1)) symbol
		, 0.0 trade_wallet_amount
		, COALESCE (SUM( CASE WHEN l.service_id = 'main_wallet' THEN credit - debit END), 0) z_wallet_amount 
		, COALESCE (SUM( CASE WHEN l.service_id = 'zip_lock' THEN credit - debit END), 0) ziplock_amount
	FROM (
		SELECT
			DISTINCT  "date" AS created_at 
			,u.account_id 
		FROM 
			GENERATE_SERIES(DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL, DATE_TRUNC('day', NOW()), '1 day') "date"
			CROSS JOIN (SELECT DISTINCT account_id FROM asset_manager_public.ledgers_v2 ) u
		)d 
		LEFT JOIN 
			asset_manager_public.ledgers_v2 l 
				ON d.account_id = l.account_id 
				AND d.created_at >= DATE_TRUNC('day', l.created_at)
		LEFT JOIN 
			analytics.users_master u 
				ON l.account_id = u.user_id 
	WHERE 
		u.ap_account_id IS NOT NULL
		AND d.created_at > '2022-06-20' -- DATE_TRUNC('day', NOW())
--		AND l.account_id = '01F0BV36CJX570T14YFQ1BFWC0'
	GROUP BY 1,2,3,4
	ORDER BY 1
)
SELECT 
	created_at
	, symbol
	, SUM(z_wallet_amount) z_wallet_amount
	, SUM(ziplock_amount) ziplock_amount
FROM 
	base
WHERE ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id NOT IN (38121 ,496001) )
GROUP BY 1,2
;



-- zip-up daily positions
WITH base AS (
SELECT
	d.snapshot_utc::DATE created_at
	, ap_account_id
	, UPPER(SPLIT_PART(s.product_id,'.',1)) symbol
	, sum(s.balance) zipup_amount
FROM
	generate_series('2022-06-20'::DATE, '2022-06-21'::DATE, '1 day'::INTERVAL) d (snapshot_utc)
	LEFT JOIN LATERAL (
		SELECT 
			DISTINCT ON (user_id, product_id) user_id, product_id, balance, created_at
		FROM zip_up_service_public.balance_snapshots
		WHERE DATE_TRUNC('day', balance_snapshots.created_at) <= d.snapshot_utc
		ORDER BY user_id, product_id, created_at DESC 
			) s ON TRUE
	LEFT JOIN analytics.users_master u
		ON s.user_id = u.user_id 
WHERE u.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id NOT IN (38121 ,496001) )
--AND UPPER(SPLIT_PART(s.product_id,'.',1)) = 'ETH'
GROUP BY 1,2,3
ORDER BY 1,2
)
SELECT 
	created_at
	, symbol
	, SUM(zipup_amount) zipup_amount
FROM base 
GROUP BY 1,2
;



--zip-lock daily positions
SELECT
	d.snapshot_utc::DATE
	, ap_account_id
	, UPPER(SPLIT_PART(s.product_id,'.',1)) symbol
	, sum(s.balance) ziplock_amount
FROM
	generate_series('2022-01-31'::DATE, '2022-02-01'::DATE, '1 day'::INTERVAL) d (snapshot_utc)
	LEFT JOIN LATERAL (
		SELECT 
			DISTINCT ON (user_id, product_id) user_id, product_id, balance, balance_datetime + '7 hour'::INTERVAL
		FROM zip_lock_service_public.vault_accumulated_balances
		WHERE DATE_TRUNC('day', balance_datetime + '7 hour'::INTERVAL) <= d.snapshot_utc
		ORDER BY user_id, product_id, balance_datetime DESC
			) s ON TRUE
	LEFT JOIN analytics.users_master u
		ON s.user_id = u.user_id 
WHERE 
	u.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
GROUP BY 1,2,3
ORDER BY 1,2
;



-- zwallet balance from Tuan - GMT+7--
    WITH base AS (
        SELECT 
            d.created_at
            , u.ap_account_id
            , UPPER(SPLIT_PART(l.product_id,'.',1)) symbol
            , UPPER(SPLIT_PART(l.product_id,'.',2)) region
            --, 0.0 trade_wallet_amount
            , COALESCE (SUM( CASE WHEN l.service_id = 'main_wallet' THEN credit - debit END), 0) z_wallet_amount 
            , COALESCE (SUM( CASE WHEN l.service_id = 'zip_lock' THEN credit - debit END), 0) ziplock_amount
        FROM (
            SELECT
                DISTINCT  "date" AS created_at 
                ,u.account_id 
            FROM 
                GENERATE_SERIES(DATE_TRUNC('day', NOW()) - '1 day'::interval, DATE_TRUNC('day', NOW()), '1 day') "date"
                CROSS JOIN (SELECT DISTINCT account_id FROM asset_manager_public.ledgers_v2 ) u
            )d 
            left join 
                asset_manager_public.ledgers_v2 l 
                    on d.account_id = l.account_id 
                    and d.created_at >= DATE_TRUNC('day', l.updated_at + interval '7 hour')
            left join 
                analytics.users_master u 
                    on l.account_id = u.user_id 
        where 
            u.ap_account_id is not null
            and date_trunc('day',d.created_at) = DATE_TRUNC('day', NOW() - interval '1 day')
            and u.signup_hostcountry IN ('TH','ID','AU','global')
            and u.is_zipup_subscribed is false
            and u.ap_account_id <> 496001 
            and d.created_at >= DATE_TRUNC('day', l.updated_at + interval '7 hour')
        group by 1,2,3,4
        order by 1 desc
    ), report as (
        select  
            a.created_at 
            , a.region
            , a.symbol 
            , SUM( COALESCE (a.z_wallet_amount, 0)) non_zipup_zwallet_amount
        from 
            base a 
        left join analytics.users_master u 
            on a.ap_account_id = u.ap_account_id
        where 
            a.symbol NOT IN ('TST1','TST2')
            and a.region = 'TH'
            and a.z_wallet_amount > 0
        group by 1,2,3
    )
    select * from report
;



-- z wallet treasury
WITH period_master AS (  
SELECT 
	p.created_at 
	, u.user_id 
	, u.ap_account_id 
	, u.signup_hostcountry 
	, p2.symbol
FROM 
	analytics.period_master p
	CROSS JOIN (
				SELECT DISTINCT user_id , ap_account_id, signup_hostcountry FROM analytics.users_master ) u 
	CROSS JOIN (SELECT DISTINCT symbol FROM apex.products
				WHERE symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')) p2
WHERE 
		p."period" = 'day' 
	AND p.created_at = NOW()::DATE - '1 day'::INTERVAL --  '{bf_date_param} 07:00:00'
)	, users_zipup_status AS (
	SELECT 
		um.user_id 
		, um.signup_hostcountry 
		, um.ap_account_id 
		, CASE WHEN um.signup_hostcountry = 'TH' THEN
			(CASE WHEN NOW()::DATE < '2022-05-08' THEN s.tnc_accepted_at ELSE um.zipup_subscribed_at END)
			WHEN um.signup_hostcountry = 'ID' THEN
			(CASE WHEN NOW()::DATE < '2022-07-04' THEN s.tnc_accepted_at ELSE um.zipup_subscribed_at END)
			WHEN um.signup_hostcountry IN ('AU','global') THEN
			(CASE WHEN NOW()::DATE < '2022-06-29' THEN s.tnc_accepted_at ELSE um.zipup_subscribed_at END)
			END AS zipup_subscribed_at
	FROM 
		analytics.users_master um 
		LEFT JOIN 
			warehouse.zip_up_service_public.user_settings s
			ON um.user_id = s.user_id 
)
	SELECT 
		d.created_at
		, CASE WHEN d.signup_hostcountry IS NULL THEN 'unknown' ELSE d.signup_hostcountry END AS signup_hostcountry
		, CASE WHEN d.ap_account_id IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id NOT IN (38121))
			THEN TRUE ELSE FALSE END AS is_nominee
		, CASE WHEN l.service_id = 'main_wallet' AND u.zipup_subscribed_at IS NOT NULL AND DATE_TRUNC('day', d.created_at) >= DATE_TRUNC('day', u.zipup_subscribed_at)
				THEN TRUE WHEN l.service_id = 'zip_lock' THEN TRUE 
				ELSE FALSE END AS is_zipup_amount 
		, d.symbol 
		, SUM( CASE WHEN l.service_id = 'main_wallet' THEN COALESCE (credit,0) - COALESCE (debit,0) END) zw_amount  
		, SUM( CASE WHEN l.service_id = 'zip_lock' THEN COALESCE (credit,0) - COALESCE (debit,0) END) zlock_amount  
	FROM period_master d 
		LEFT JOIN 
			asset_manager_public.ledgers_v2 l 
			ON d.user_id = l.account_id 
			AND d.created_at >= DATE_TRUNC('day', l.updated_at)
			AND d.symbol = UPPER(SPLIT_PART(l.product_id,'.',1))
		LEFT JOIN
			users_zipup_status u
			ON l.account_id = u.user_id
	WHERE 
		l.account_id IS NOT NULL 
	GROUP BY 1,2,3,4,5
;


