-- pcs global definition: 20K ZMT (all wallets) OR 50K USD (total AUM)
WITH base AS (
	SELECT 
		a.created_at 
		, u.signup_hostcountry
		, a.ap_account_id  
		, a.symbol 
		, (trade_wallet_amount + z_wallet_amount + ziplock_amount + zlaunch_amount) total_unit
		, CASE	WHEN r.product_type = 1 
			THEN (trade_wallet_amount + z_wallet_amount + ziplock_amount + zlaunch_amount) * 1/r.price 
				WHEN r.product_type = 2 
			THEN (trade_wallet_amount + z_wallet_amount + ziplock_amount + zlaunch_amount) * r.price
				END AS total_aum_usd
	FROM 
		analytics.wallets_balance_eod a 
	-- get country and join with pii data
		LEFT JOIN 
			analytics.users_master u 
			ON a.ap_account_id = u.ap_account_id 
	-- coin prices and exchange rates (USD)
		LEFT JOIN 
			analytics.rates_master r 
			ON a.symbol = r.product_1_symbol
			AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
	WHERE 
		a.created_at >= '2022-01-01' AND a.created_at < DATE_TRUNC('month', NOW())::DATE
		AND u.signup_hostcountry IN ('global')
	-- filter accounts from users_mapping
		AND a.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
	-- snapshot by 1st of month 
		AND a.created_at = DATE_TRUNC('month', a.created_at)
	-- exclude test products
		AND a.symbol NOT IN ('TST1','TST2')
	ORDER BY 1 DESC 
)	, aum_snapshot AS (
	SELECT 
		DATE_TRUNC('day', created_at)::DATE created_at
		, signup_hostcountry
		, ap_account_id
		, COALESCE( SUM( CASE WHEN symbol = 'ZMT' THEN total_unit END), 0) zmt_amount
		, SUM( COALESCE (total_aum_usd, 0)) total_aum_usd
	FROM 
		base 
	GROUP BY 
		1,2,3
	ORDER BY 
		1 
)	, pcs_base AS (
	SELECT 
		a.*
		, CASE WHEN zts.vip_tier IS NULL THEN 'no_tier' ELSE zts.vip_tier END AS vip_tier
		, CASE WHEN zmt_amount >= 20000 OR total_aum_usd >= 50000 THEN 'pcs' ELSE 'other' END AS status
	FROM 
		aum_snapshot a
		LEFT JOIN analytics.zmt_tier_1stofmonth zts 
			ON a.created_at = zts.created_at::DATE
			AND a.ap_account_id = zts.ap_account_id 
)
SELECT
	status
	, vip_tier
	, COUNT(DISTINCT ap_account_id) user_count
	, SUM(total_aum_usd) total_aum_usd
FROM pcs_base
GROUP BY 1,2
;



-- pcs global list 
WITH base AS (
	SELECT 
		a.created_at
		, u.signup_hostcountry
		, a.ap_account_id  
		, a.symbol 
		, CASE WHEN a.symbol = 'ZMT' THEN COALESCE(ziplock_amount,0) 
				END AS zmt_locked_amount 
		, (COALESCE(trade_wallet_amount,0) + COALESCE(z_wallet_amount,0) + COALESCE(ziplock_amount,0) + COALESCE(zlaunch_amount,0)) total_unit
		, CASE  WHEN r.product_type = 1 
			THEN (COALESCE(trade_wallet_amount,0) + COALESCE(z_wallet_amount,0) + COALESCE(ziplock_amount,0) + COALESCE(zlaunch_amount,0)) * 1/r.price 
				WHEN r.product_type = 2 
			THEN (COALESCE(trade_wallet_amount,0) + COALESCE(z_wallet_amount,0) + COALESCE(ziplock_amount,0) + COALESCE(zlaunch_amount,0)) * r.price
				END AS total_aum_usd
	FROM 
		analytics.wallets_balance_eod a 
	-- get country and join with pii data
		LEFT JOIN 
			analytics.users_master u 
			ON a.ap_account_id = u.ap_account_id 
	-- coin prices and exchange rates (USD)
		LEFT JOIN 
			analytics.rates_master r 
			ON a.symbol = r.product_1_symbol 
			AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
	WHERE 
	-- snapshot by 1st of last week 
		a.created_at = DATE_TRUNC('week', NOW()::DATE - '1 day'::INTERVAL)
		AND u.signup_hostcountry IN ('global')
	-- filter accounts from users_mapping
		AND a.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
	-- exclude test products
		AND a.symbol NOT IN ('TST1','TST2')
	ORDER BY 1 DESC 
)   , aum_snapshot AS (
	SELECT 
		DATE_TRUNC('week', created_at)::DATE created_at
		, signup_hostcountry
		, ap_account_id
		, SUM( COALESCE (zmt_locked_amount ,0)) zmt_locked_amount 
		, COALESCE( SUM( CASE WHEN symbol = 'ZMT' THEN total_unit END), 0) zmt_amount
		, SUM( COALESCE (total_aum_usd, 0)) total_aum_usd
	FROM base 
	GROUP BY 1,2,3
    ORDER BY 1 
)
,user_pii AS (
	SELECT um.*, pii.email AS pii_email
	FROM analytics.users_master um 
		LEFT JOIN analytics_pii.users_pii pii 
		ON um.user_id = pii.user_id 
)
,pcs_users AS (
	SELECT 
    	*
    	, CASE WHEN zmt_amount >= 20000 OR total_aum_usd >= 50000 THEN 'pcs' ELSE 'near_pcs' END AS status
    FROM aum_snapshot
    WHERE 
    	zmt_amount >= 20000
    	OR total_aum_usd >= 30000
)
SELECT 
	pu.created_at balance_at,
	pu.ap_account_id,
	up.pii_email,
	pu.status,
	zmt_amount AS total_zmt_balance,
	total_aum_usd,
	zmt_locked_amount AS total_zmt_lock_balance,
	SUM(tm.amount_usd) AS total_trade_volume_usd,
	SUM(dtm.amount_usd) AS total_deposit_volume_usd,
	SUM(wtm.amount_usd) AS total_withdraw_volume_usd
FROM 
	pcs_users pu
	LEFT JOIN user_pii up 
		ON up.ap_account_id = pu.ap_account_id 
	LEFT JOIN analytics.trades_master tm 
		ON tm.ap_account_id = pu.ap_account_id 
		AND pu.created_at = DATE_TRUNC('week',tm.created_at)::DATE
	LEFT JOIN analytics.deposit_tickets_master dtm 
		ON pu.ap_account_id = dtm.ap_account_id 
		AND pu.created_at = DATE_TRUNC('week',dtm.created_at)::DATE
		AND dtm.status = 'FullyProcessed'
	LEFT JOIN analytics.withdraw_tickets_master wtm 
		ON pu.ap_account_id = wtm.ap_account_id 
		AND pu.created_at = DATE_TRUNC('week',wtm.created_at)::DATE
		AND wtm.status = 'FullyProcessed'
GROUP BY 1,2,3,4,5,6,7
;



-- pcs global list 
WITH base AS (
    SELECT 
        a.created_at
        , u.signup_hostcountry
        , a.ap_account_id  
        , a.symbol 
        , CASE WHEN a.symbol = 'ZMT' THEN COALESCE(ziplock_amount,0) 
                END AS zmt_locked_amount 
        , (COALESCE(trade_wallet_amount,0) + COALESCE(z_wallet_amount,0) + COALESCE(ziplock_amount,0) + COALESCE(zlaunch_amount,0)) total_unit
        , CASE  WHEN r.product_type = 1 
            THEN (COALESCE(trade_wallet_amount,0) + COALESCE(z_wallet_amount,0) + COALESCE(ziplock_amount,0) + COALESCE(zlaunch_amount,0)) * 1/r.price 
                WHEN r.product_type = 2 
            THEN (COALESCE(trade_wallet_amount,0) + COALESCE(z_wallet_amount,0) + COALESCE(ziplock_amount,0) + COALESCE(zlaunch_amount,0)) * r.price
                END AS total_aum_usd
    FROM 
        analytics.wallets_balance_eod a 
    -- get country and join with pii data
        LEFT JOIN 
            analytics.users_master u 
            ON a.ap_account_id = u.ap_account_id 
    -- coin prices and exchange rates (USD)
        LEFT JOIN 
            analytics.rates_master r 
            ON a.symbol = r.product_1_symbol 
            AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
    WHERE 
    -- snapshot by 1st of last week 
        a.created_at = DATE_TRUNC('week', NOW()::DATE - '1 day'::INTERVAL)
        AND u.signup_hostcountry IN ('global')
    -- filter accounts from users_mapping
        AND a.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
    -- exclude test products
        AND a.symbol NOT IN ('TST1','TST2')
    ORDER BY 1 DESC 
)   , aum_snapshot AS (
    SELECT 
        DATE_TRUNC('week', created_at)::DATE created_at
        , signup_hostcountry
        , ap_account_id
        , SUM( COALESCE (zmt_locked_amount ,0)) zmt_locked_amount 
        , COALESCE( SUM( CASE WHEN symbol = 'ZMT' THEN total_unit END), 0) zmt_amount
        , SUM( COALESCE (total_aum_usd, 0)) total_aum_usd
    FROM base 
    GROUP BY 1,2,3
    ORDER BY 1 
)
,user_pii AS (
    SELECT um.*, pii.email AS pii_email ,pii.first_name AS pii_firstname ,pii.last_name  AS pii_lastname ,pii.mobile_number AS pii_mobilenumber 
    FROM analytics.users_master um 
        LEFT JOIN analytics_pii.users_pii pii 
        ON um.user_id = pii.user_id 
)
,pcs_users AS (
    SELECT 
        *
        , CASE WHEN zmt_amount >= 20000 OR total_aum_usd >= 50000 THEN 'pcs' ELSE 'near_pcs' END AS status
    FROM aum_snapshot
    WHERE 
        zmt_amount >= 20000
        OR total_aum_usd >= 30000
)
SELECT 
    pu.created_at balance_at,
    pu.ap_account_id,
    up.pii_firstname,
    up.pii_lastname,
    up.pii_email,
    up.pii_mobilenumber,
    pu.status,
    zmt_amount AS total_zmt_balance,
    total_aum_usd,
    zmt_locked_amount AS total_zmt_lock_balance,
    SUM(tm.amount_usd) AS total_trade_volume_usd,
    SUM(dtm.amount_usd) AS total_deposit_volume_usd,
    SUM(wtm.amount_usd) AS total_withdraw_volume_usd
FROM 
    pcs_users pu
    LEFT JOIN user_pii up 
        ON up.ap_account_id = pu.ap_account_id 
    LEFT JOIN analytics.trades_master tm 
        ON tm.ap_account_id = pu.ap_account_id 
        AND pu.created_at = DATE_TRUNC('week',tm.created_at)::DATE
    LEFT JOIN analytics.deposit_tickets_master dtm 
        ON pu.ap_account_id = dtm.ap_account_id 
        AND pu.created_at = DATE_TRUNC('week',dtm.created_at)::DATE
        AND dtm.status = 'FullyProcessed'
    LEFT JOIN analytics.withdraw_tickets_master wtm 
        ON pu.ap_account_id = wtm.ap_account_id 
        AND pu.created_at = DATE_TRUNC('week',wtm.created_at)::DATE
        AND wtm.status = 'FullyProcessed'
GROUP BY 1,2,3,4,5,6,7,8,9,10
;



/*
 * VIP definition:
 * ZMT VIP tier
 * OR
 * VIP2 >= USD 5k AUM
 * VIP3 >= USD 25k AUM
 * VIP4 >= USD 100k AUM
 * which ever is bigger
*/ 

-- 2022-05-11 pcs detailed list with net money inflow
WITH deposit_sum AS (
	SELECT 
--		created_at::DATE 
		DATE_TRUNC('week', created_at)::DATE created_at
--		DATE_TRUNC('month', created_at)::DATE created_at
		, ap_account_id 
		, product_symbol 
		, SUM(amount) deposit_unit
		, SUM(amount_usd) deposit_usd
	FROM 
		analytics.deposit_tickets_master dtm 
	WHERE 
		status = 'FullyProcessed'
	GROUP BY 1,2,3
)	, withdraw_sum AS (
	SELECT 
--		created_at::DATE 
		DATE_TRUNC('week', created_at)::DATE created_at
--		DATE_TRUNC('month', created_at)::DATE created_at
		, ap_account_id 
		, product_symbol 
		, SUM(amount) withdraw_unit
		, SUM(amount_usd) withdraw_usd
	FROM 
		analytics.withdraw_tickets_master wtm  
	WHERE 
		status = 'FullyProcessed'
	GROUP BY 1,2,3
)	, aum_sum AS (
	SELECT 
		wbe.created_at::DATE 
		, wbe.ap_account_id 
		, um.user_id 
		, CASE WHEN ult.tier_name IS NULL THEN 'no_zmt' ELSE ult.tier_name END AS vip_tier
		, SUM( CASE WHEN rm.product_type = 1 THEN 
					(COALESCE (wbe.trade_wallet_amount , 0) + COALESCE (wbe.z_wallet_amount, 0) 
					+ COALESCE (wbe.ziplock_amount , 0) + COALESCE (wbe.zlaunch_amount , 0)) * 1/rm.price 
					WHEN rm.product_type = 2 THEN 
					(COALESCE (wbe.trade_wallet_amount , 0) + COALESCE (wbe.z_wallet_amount, 0) 
					+ COALESCE (wbe.ziplock_amount , 0) + COALESCE (wbe.zlaunch_amount , 0)) * rm.price 
					END) AS total_aum_usd 
	FROM 
		analytics.wallets_balance_eod wbe 
		LEFT JOIN 
			analytics.users_master um 
			ON wbe.ap_account_id = um.ap_account_id
		LEFT JOIN 
			analytics.rates_master rm 
			ON wbe.created_at::DATE = rm.created_at::DATE 
			AND wbe.symbol = rm.product_1_symbol 
		LEFT JOIN 
			zip_lock_service_public.user_loyalty_tiers ult 
			ON um.user_id = ult.user_id 
	WHERE 
		wbe.created_at = NOW()::DATE - '1 day'::INTERVAL
		AND um.signup_hostcountry = 'global'
	GROUP BY 1,2,3,4
)	, gl_tier AS (
	SELECT 
		*
		, CASE WHEN total_aum_usd >= 100000 THEN 'gl_vip4'
				WHEN total_aum_usd >= 25000 THEN 'gl_vip3'
				WHEN total_aum_usd >= 5000 THEN 'gl_vip2'
				ELSE NULL
				END AS global_pcs_tier
	FROM aum_sum
)	, final_rep AS (
	SELECT 
		wbe.created_at::DATE 
		, wbe.ap_account_id 
		, u.email
		, u.mobile_number
		, f.code account_type
		, CASE WHEN um.global_pcs_tier IS NOT NULL THEN um.global_pcs_tier ELSE um.vip_tier END AS vip_tier
		, wbe.symbol 
		, SUM( CASE WHEN rm.product_type = 1 THEN 
					(COALESCE (wbe.trade_wallet_amount , 0) + COALESCE (wbe.z_wallet_amount, 0) 
					+ COALESCE (wbe.ziplock_amount , 0) + COALESCE (wbe.zlaunch_amount , 0)) * 1/rm.price 
					WHEN rm.product_type = 2 THEN 
					(COALESCE (wbe.trade_wallet_amount , 0) + COALESCE (wbe.z_wallet_amount, 0) 
					+ COALESCE (wbe.ziplock_amount , 0) + COALESCE (wbe.zlaunch_amount , 0)) * rm.price 
					END) AS total_aum_usd 
		, SUM( COALESCE (ds.deposit_unit, 0)) deposit_unit
		, SUM( COALESCE (ws.withdraw_unit, 0)) withdraw_unit
		, SUM( COALESCE (ds.deposit_unit, 0)) - SUM( COALESCE (ws.withdraw_unit, 0)) net_inflow_unit
		, SUM( COALESCE (ds.deposit_usd, 0)) deposit_usd
		, SUM( COALESCE (ws.withdraw_usd, 0)) withdraw_usd
		, SUM( COALESCE (ds.deposit_usd, 0)) - SUM( COALESCE (ws.withdraw_usd, 0)) net_inflow_usd
	FROM 
		analytics.wallets_balance_eod wbe
		RIGHT JOIN 
			gl_tier um 
			ON wbe.ap_account_id = um.ap_account_id 
		LEFT JOIN 
			user_app_public.user_features uf 
			ON um.user_id = uf.user_id
		LEFT JOIN 
			user_app_public.features f 
			ON uf.feature_id = f.id
		LEFT JOIN 
			user_app_public.users u 
			ON um.user_id = u.id
		LEFT JOIN 
			analytics.rates_master rm 
			ON wbe.created_at::DATE = rm.created_at::DATE 
			AND wbe.symbol = rm.product_1_symbol 
		LEFT JOIN 
			deposit_sum ds 
--			ON wbe.created_at::DATE = ds.created_at::DATE 
			ON DATE_TRUNC('week', wbe.created_at)::DATE = DATE_TRUNC('week', ds.created_at)::DATE 
--			ON DATE_TRUNC('month', wbe.created_at)::DATE = DATE_TRUNC('month', ds.created_at)::DATE 
			AND wbe.ap_account_id = ds.ap_account_id 
			AND wbe.symbol = ds.product_symbol
		LEFT JOIN 
			withdraw_sum ws 
--			ON wbe.created_at::DATE = ws.created_at::DATE 
			ON DATE_TRUNC('week', wbe.created_at)::DATE = DATE_TRUNC('week', ws.created_at)::DATE 
--			ON DATE_TRUNC('month', wbe.created_at)::DATE = DATE_TRUNC('month', ws.created_at)::DATE 
			AND wbe.ap_account_id = ws.ap_account_id 
			AND wbe.symbol = ws.product_symbol 
	WHERE 
--	    wbe.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL
		wbe.created_at::DATE >= DATE_TRUNC('week', NOW())::DATE - '1 week'::INTERVAL
--		wbe.created_at::DATE >= DATE_TRUNC('month', NOW())::DATE - '1 month'::INTERVAL
	    AND ((wbe.created_at = DATE_TRUNC('week', wbe.created_at) + '1 week' - '1 day'::INTERVAL))
--	    AND ((wbe.created_at = DATE_TRUNC('month', wbe.created_at) + '1 month' - '1 day'::INTERVAL))
		AND (f.code NOT IN ('TEST') OR f.code IS NULL)
		AND u.email NOT LIKE '%zipmex.com'
	GROUP BY 1,2,3,4,5,6,7
)
SELECT 
	created_at 
	, vip_tier
	, COUNT( DISTINCT ap_account_id) user_count
FROM final_rep
GROUP BY 1,2
;


