--user kyc passed during feb receives 200 THB airdrop in BTC 
 WITH user_pii AS (
    SELECT um.*
          ,pii.email AS pii_email 
    FROM 
        analytics.users_master um 
        LEFT JOIN analytics_pii.users_pii pii
        ON um.user_id = pii.user_id
)
,eligible_users AS ( 
    SELECT um.user_id
           ,um.ap_account_id
           ,up.pii_email
           ,um.created_at 
       -- Jesse -- um.created_at + interval '7h' created_at_gmt7
           ,um.invitation_code 
           ,um.signup_hostcountry 
           ,um.level_increase_status 
           ,um.verification_approved_at 
    FROM 
        analytics.users_master um 
        LEFT JOIN user_pii up
        ON up.user_id = um.user_id 
            WHERE 
                (um.invitation_code ='ZIPMEX200' or um.invitation_code = 'EXP22')
    --Jesse-- um.invitation_code IN ('ZIPMEX200', 'EXP22')
            AND um.ap_account_id not in (select distinct ap_account_id from mappings.users_mapping)
            --kyc passed during feb 
            AND um.verification_approved_at + interval '7h' >= '2022/02/01 00:00:00'
            AND um.verification_approved_at + interval '7h'<= '2022/02/28 23:59:59'
            AND pii_email not like '%zipmex%'
)
,latest_btc AS(
    SELECT 
        average_high_low 
    FROM 
        warehouse.oms_data_public.cryptocurrency_prices cp
        WHERE product_2_symbol ='THB'
        AND product_1_symbol = 'BTC'
    ORDER BY created_at DESC 
    LIMIT 1 
) 
,airdrop_calculation AS ( 
    SELECT a.user_id,
           ,a.pii_email
           ,round(200/usdt.average_high_low:: NUMERIC,8) as airdrop_amount 
    FROM amount_traded a 
    CROSS JOIN latest_btc as btc
)
,airdrop AS (
    SELECT 25 AS product_id 
           , ac.airdrop_amount AS amount 
           , 'AIRDROP_2203XX_TH_GROWTH_ACQUI_ZIPMEX200_BTC' as notes
           , 27308 as from_account_id
           , ac.pii_email as to_email
    FROM airdrop_calculation ac
)
SELECT * FROM airdrop
;


-- Crypto Arcade user sign up and kyc passed within 11/01/2022 to 31/01/2022 receives free trading 
-- max fee = 60,000 THB
 WITH user_pii AS (
    SELECT um.*
          ,pii.email AS pii_email 
    FROM 
        analytics.users_master um 
        LEFT JOIN analytics_pii.users_pii pii
        ON um.user_id = pii.user_id
)
,eligible_users AS ( 
    SELECT um.user_id
           ,um.ap_account_id
           ,up.pii_email
           ,um.created_at 
           ,um.invitation_code 
           ,um.signup_hostcountry 
           ,um.level_increase_status 
           ,sum(fee_usd_amount) as total_fees_usd
           ,sum(fee_base_fiat_amount) as total_fees_thb    
    FROM 
        analytics.users_master um 
        LEFT JOIN user_pii up
        ON up.user_id = um.user_id 
        LEFT JOIN analytics.fees_master fm 
        ON fm.ap_account_id = um.ap_account_id
            WHERE 
                (um.invitation_code ='ARCADE22' or um.invitation_code = 'EXP22')
            AND um.ap_account_id not in (select distinct ap_account_id from mappings.users_mapping)
            AND um.created_at + interval '7h'>= '2022/01/11 00:00:00'
            AND um.created_at + interval '7h' <= '2022/01/31 23:59:59' 
            AND um.verification_approved_at + interval '7h' >= '2022/01/11 00:00:00'
            AND um.verification_approved_at + interval '7h'<= '2022/01/31 23:59:59'
            AND pii_email not like '%zipmex%'
            AND fm.created_at + interval '7h'>= '2022/02/01 00:00:00'
            AND fm.created_at + interval '7h' <= '2022/02/28 23:59:59'
            AND fee_type = 'Trade'
    GROUP BY 1,2,3,4,5,6,7 
)
,amount_traded AS (
    SELECT eu.user_id
          ,eu.pii_email
          ,eu.total_fees_usd 
          ,eu.total_fees_thb
          ,CASE WHEN eu.total_fees_thb >= 60000 THEN 60000 
            ELSE eu.total_fees_thb END AS amount_earn_thb 
    FROM 
        eligibler_users eu 
)
,latest_USDT AS(
    SELECT 
        average_high_low 
    FROM 
        warehouse.oms_data_public.cryptocurrency_prices cp
        WHERE product_2_symbol ='THB'
        AND product_1_symbol = 'USDT'
    ORDER BY created_at DESC 
    LIMIT 1 
) 
,airdrop_calculation AS ( 
    SELECT a.user_id,
           ,a.pii_email
           ,round(a.amount_earn_thb/usdt.average_high_low:: NUMERIC,8) as airdrop_amount 
    FROM amount_traded a 
    CROSS JOIN latest_usdt as usdt
)
,airdrop AS (
    SELECT 30 AS product_id 
           , ac.airdrop_amount AS amount 
           , 'AIRDROP_2203XX_TH_GROWTH_ACQUI_CRYPTOARCADE_USDT' as notes
           , 27308 as from_account_id
           , ac.pii_email as to_email
    FROM airdrop_calculation ac
)
SELECT * FROM airdrop
;

 

--xbuillion gold trading campaign --21-28 feb 2022
WITH user_pii AS (
    SELECT um.* 
           , pii.email AS pii_email 
    FROM 
        analytics.users_master um
        LEFT JOIN analytics_pii.users_pii pii 
        ON um.user_id = pii.user_id 
)
,user_trade AS (
    SELECT  
		    up.pii_email
            ,up.user_id
            , tm.ap_account_id
            ,SUM(tm.quantity) AS gold_amount 
            ,SUM(fm.fee_amount) AS fee_gold_amount 
    FROM 
        analytics.trades_master tm 
        LEFT JOIN analytics.fees_master fm 
        ON tm.execution_id = fm.fee_reference_id
        AND fee_product = 'GOLD'
        LEFT JOIN user_pii up 
        ON tm.ap_account_id = up.ap_account_id 
            WHERE tm.signup_hostcountry = 'TH'
            AND tm.created_at + interval '7h' >= '2022/02/21 00:00:00'
            AND tm.created_at + interval '7h' <= '2022/02/28 23:59:59'
            AND product_1_symbol = 'GOLD'
            AND pii_email NOT LIKE '%zipmex%'
            AND tm.ap_account_id NOT IN 
            (SELECT DISTINCT ap_account_id FROM mappings.users_mapping) 
    GROUP BY 1,2,3
)
,net_gold AS (
    SELECT 
          ut.*
          ,gold_amount- COALESCE(fee_gold_amount,0) AS net_gold_amount 
    FROM user_trade ut 
)
--SELECT * FROM net_gold_amount 
,eligible_users_top15 AS (
    SELECT *
    FROM net_gold 
        WHERE net_gold_amount >= 9 
    ORDER BY net_gold_amount DESC 
    LIMIT 15 
)
--SELECT * FROM eligible_users_top15  
,eligible_users_others AS (
    SELECT *
    FROM net_gold 
        WHERE net_gold_amount >= 9 
        AND ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM eligible_users_top15)
    ORDER BY net_gold_amount DESC 
)
--SELECT * FROM eligible_users_others  
,total_pool_calculation_top15 AS (
    SELECT 
        SUM(net_gold_amount) AS total_gold_amount 
    FROM eligible_users_top15 
)
--SELECT * FROM total_pool_calculation_top15 
,total_pool_calculation_others AS (
    SELECT 
        SUM(net_gold_amount) AS total_gold_amount 
    FROM eligible_users_others
)
--SELECT * FROM total_pool_calculation_others
,latest_usd AS (
    SELECT 
         created_at
         ,price 
    FROM 
        analytics.rates_master rm 
            WHERE product_2_symbol ='USD'
            AND product_1_symbol = 'THB'
    ORDER BY 
        created_at DESC
    LIMIT 1 
)
,top_15_users AS (
    SELECT *
--    pii_email
--    ,(net_gold_amount/tpc.total_gold_amount)* (65000/usd.price) AS amount_earn
    FROM eligible_users_top15 
    CROSS JOIN total_pool_calculation_top15 AS tpc
    CROSS JOIN latest_usd AS usd 
)
,other_users AS (
    SELECT pii_email 
    ,(net_gold_amount/tpco.total_gold_amount)* (15000/usd.price) AS amount_earn
    FROM eligible_users_others 
    CROSS JOIN total_pool_calculation_others AS tpco 
    CROSS JOIN latest_usd AS usd 
)
,total_users AS (
    SELECT pii_email
           ,amount_earn
    FROM top_15_users 
    UNION ALL 
    SELECT pii_email
           ,amount_earn 
    FROM other_users 
)
--SELECT * FROM total_users 
,latest_gold AS (
    SELECT 
         created_at
         ,price 
    FROM 
        analytics.rates_master rm 
            WHERE product_2_symbol ='USD'
            AND product_1_symbol = 'GOLD'
    ORDER BY 
        created_at DESC
    LIMIT 1 
)
,airdrop_calculation AS (
    SELECT *
           ,round(tu.amount_earn/gold.price,8) AS airdrop_amount 
    FROM 
        total_users tu 
    CROSS JOIN latest_gold AS gold 
)
,airdrop AS ( 
    SELECT 35 AS product_id 
          ,airdrop_amount AS amount 
          ,'AIRDROP_2203XX_TH_GROWTH_ACQUI_XBULLIONTRADINGCAMPAIGN_GOLD' as notes
          , 27308 AS from_account_id 
          , pii_email AS to_email 
    FROM 
        airdrop_calculation
)
SELECT * FROM airdrop 
;


-- refactor for metabase
WITH zip_prod_aum as (
    SELECT ap_account_id, avg(avg_z_wallet_amount_usd) as avg_z_wallet_amount_last30d_usd, avg(avg_ziplock_amount_usd) as avg_ziplock_amount_last30d_usd
    FROM analytics.dm_mtu_daily
    WHERE mtu_day >= NOW() - INTERVAL '1 MONTH'
    GROUP BY 1
),
new as
(
SELECT date_trunc('day',um.created_at) as date
            , um.invitation_code as referral_code_raw
            , LOWER(um.invitation_code) as referral_code_lower
            , um.signup_hostcountry
            , um.signup_platform
            , COALESCE(COUNT(DISTINCT um.user_id),0) user_register -- COALESCE IS used here so that cumulative sum still populate WHEN the results ARE NULL
            , COALESCE(COUNT(DISTINCT CASE WHEN um.is_email_verified IS TRUE THEN um.user_id END),0) email_verified
            , COALESCE(COUNT(DISTINCT CASE WHEN um.is_mobile_verified IS TRUE THEN um.user_id END),0) mobile_verified
            , COALESCE(COUNT(DISTINCT CASE WHEN um.frankieone_smart_ui_submitted_at IS NOT NULL THEN um.user_id END),0) frankieone_submitted
            , COALESCE(COUNT(DISTINCT CASE WHEN um.onfido_submitted_at IS NOT NULL THEN um.user_id END),0) onfido_submitted
            , COALESCE(COUNT(DISTINCT CASE WHEN um.is_onfido_verified IS TRUE THEN um.user_id END),0) onfido_verified
            , COALESCE(COUNT(DISTINCT CASE WHEN um.is_verified IS TRUE THEN um.user_id END),0) user_verified
            , COALESCE(COUNT(DISTINCT CASE WHEN um.has_deposited IS TRUE THEN um.user_id END),0) user_deposited
            , COALESCE(COUNT(DISTINCT CASE WHEN um.has_traded IS TRUE THEN um.user_id END),0) user_traded
            ,COALESCE(COUNT(DISTINCT CASE WHEN um.is_zipup_subscribed IS TRUE AND dt.source = 'alpha_point' and dt.source_ref = 'accounts+zipmexbalance@zipmex.com'THEN um.user_id END),0) user_zipup
            , COALESCE(SUM(um.sum_trade_volume_usd), 0) as sum_trade_volume_usd
            , COALESCE(SUM(um.count_trades), 0) as trade_count
            , COALESCE(SUM(um.sum_withdraw_amount_usd), 0) as sum_withdraw_amount_usd
            , COALESCE(SUM(um.count_withdraws), 0) as withdraw_count
            , COALESCE(SUM(um.sum_deposit_amount_usd), 0) as sum_deposit_amount_usd
            , COALESCE(SUM(um.count_deposits), 0) as deposit_count
            , COALESCE(SUM(zpa.avg_z_wallet_amount_last30d_usd),0) as sum_avg_z_wallet_amount_last30d_usd
            , COALESCE(SUM(zpa.avg_ziplock_amount_last30d_usd),0) as sum_avg_ziplock_amount_last30d_usd
FROM
     analytics.users_master um
LEFT JOIN
     user_app_public.bank_accounts b
     ON um.user_id = b.user_id
LEFT JOIN zip_prod_aum zpa
     ON zpa.ap_account_id = um.ap_account_id
LEFT JOIN asset_manager_public.deposit_transactions dt
on um.user_id = dt.account_id
    -- platform, traffic_source, traffic_medium, traffic_channel
GROUP BY 1,2,3,4,5
)
--SELECT * FROM NEW 
--WHERE referral_code_raw LIKE 'The1%'
,
old as
(
SELECT um.user_id,
       um.referral_code as referral_code_raw,
       LOWER(um.referral_code) as referral_code_lower,
       rc.referral_group
FROM
    analytics.users_master um
LEFT JOIN mappings.growth_referral_code rc
ON um.referral_code = rc.referral_code
)
,base1 AS (
SELECT new.*, COALESCE(old.referral_group, 'peer2peer') AS referral_group,
CASE WHEN new.referral_code_raw = old.referral_code_raw THEN 'YES'
ELSE 'NO' END as is_match_original,
CASE WHEN NEW.referral_code_raw LIKE 'The1%' THEN 'the1' 
ELSE NULL END AS referral_campaign_group 
FROM new
LEFT JOIN old
ON new.referral_code_lower = old.referral_code_lower
WHERE new.referral_code_lower NOTNULL
AND old.user_id NOTNULL
)
--SELECT count(*) FROM base1 
,base2 AS (
SELECT new.*, COALESCE(old.referral_group, 'the1') AS referral_group,
CASE WHEN new.referral_code_raw = old.referral_code_raw THEN 'YES'
ELSE 'NO' END as is_match_original,
CASE WHEN NEW.referral_code_raw LIKE 'The1%' THEN 'the1' 
ELSE NULL END AS referral_campaign_group 
FROM new
LEFT JOIN old
ON new.referral_code_lower = old.referral_code_lower
WHERE new.referral_code_lower NOTNULL
AND NEW.referral_code_raw LIKE 'The1%'
AND OLD.user_id IS NULL 
)
--SELECT count(*) FROM base2 
,final_table AS (
SELECT * FROM base1 
UNION ALL 
SELECT * FROM base2 
) 
SELECT * FROM final_table 



-- TH - PCS list
WITH user_pii AS (
	SELECT um.*, pii.email AS pii_email ,pii.first_name AS pii_firstname ,pii.last_name  AS pii_lastname ,pii.mobile_number AS pii_mobilenumber 
	FROM analytics.users_master um 
		LEFT JOIN analytics_pii.users_pii pii 
		ON um.user_id = pii.user_id 
)
,user_info AS (
	SELECT up.ap_account_id
		   ,t.info
	FROM 
		user_pii up 
	LEFT JOIN user_app_public.personal_infos t
		ON up.user_id = t.user_id 
        AND t.archived_at IS NULL 
)
,personna AS (		
	SELECT 
		DISTINCT ap_account_id , 
		info ->> 'dob' AS dob, 
		info ->> 'present_address' AS present_address,
		info ->> 'present_address_district' AS present_address_district, 
		info ->> 'present_address_sub_district' AS present_address_sub_district,
		info ->> 'present_address_province'AS present_address_province ,
		info ->> 'present_address_postal_code' AS present_address_postal_code ,
		info ->> 'occupation' AS occupation
	FROM user_info
)
,aum_base AS (
	SELECT 
		a.created_at
		, u.signup_hostcountry
		, a.ap_account_id  
		, a.symbol 
		, CASE WHEN a.symbol = 'ZMT' THEN COALESCE(ziplock_amount,0) 
				END AS zmt_locked_amount 
		, CASE WHEN a.symbol = 'ZMT' THEN COALESCE(trade_wallet_amount,0) 
				END AS zmt_trade_wallet_amount
		, CASE WHEN a.symbol = 'ZMT' THEN COALESCE(z_wallet_amount,0) 
				END AS zmt_zwallet_amount
		, CASE WHEN a.symbol = 'ZMT' THEN COALESCE(zlaunch_amount,0) 
				END AS zmt_zlaunch_amount		
		, (COALESCE(trade_wallet_amount,0) + COALESCE(z_wallet_amount,0) + COALESCE(ziplock_amount,0) + COALESCE(zlaunch_amount,0)) total_unit
		, (COALESCE(trade_wallet_amount*r.price,0) + COALESCE(z_wallet_amount*r.price,0) + COALESCE(ziplock_amount*r.price,0) + COALESCE(zlaunch_amount*r.price,0)) total_unit_usd 
		, CASE  WHEN r.product_type = 1 
			THEN (COALESCE(trade_wallet_amount,0) + COALESCE(z_wallet_amount,0) + COALESCE(ziplock_amount,0) + COALESCE(zlaunch_amount,0)) * 1/r.price 
				WHEN r.product_type = 2 
			THEN (COALESCE(trade_wallet_amount,0) + COALESCE(z_wallet_amount,0) + COALESCE(ziplock_amount,0) + COALESCE(zlaunch_amount,0)) * r.price
				END AS total_aum_usd
	FROM 
		analytics.wallets_balance_eod a 
		LEFT JOIN 
			analytics.users_master u 
			ON a.ap_account_id = u.ap_account_id 
		LEFT JOIN 
			analytics.rates_master r 
			ON a.symbol = r.product_1_symbol
			AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
	WHERE 
		a.created_at >= NOW()::DATE - '12 MONTH' :: INTERVAL AND a.created_at < NOW()::DATE
-- Jesse - a.created_at >= NOW()::DATE - '3 month'::INTERVAL AND a.created_at < NOW()::DATE
		AND u.signup_hostcountry IN ('TH')
		AND a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL 
		AND a.symbol NOT IN ('TST1','TST2')
	ORDER BY 1 DESC 
) 
,aum_snapshot1 AS (
	SELECT 
		DATE_TRUNC('day', created_at)::DATE created_at
		, signup_hostcountry
		, ap_account_id
		, SUM( COALESCE (zmt_locked_amount ,0)) zmt_locked_amount 
		, SUM( COALESCE (zmt_trade_wallet_amount ,0)) zmt_trade_wallet_amount 
		, SUM( COALESCE (zmt_zwallet_amount ,0)) zmt_zwallet_amount 
		, SUM( COALESCE (zmt_zlaunch_amount ,0)) zmt_zlaunch_amount 
		, COALESCE( SUM( CASE WHEN symbol = 'ZMT' THEN total_unit END), 0) total_zmt_amount
		, COALESCE( SUM( CASE WHEN symbol = 'ZMT' THEN total_unit_usd END), 0) total_zmt_usd 
		, COALESCE( SUM( CASE WHEN symbol != 'ZMT' THEN total_unit_usd END), 0) other_coins_all_wallet_usd 
		, SUM( COALESCE (total_aum_usd, 0)) total_aum_usd
	FROM aum_base 
		WHERE 
			DATE_TRUNC('MONTH',created_at) >= DATE_TRUNC('MONTH', NOW() ) - '3 MONTH' :: INTERVAL 
	GROUP BY 1,2,3
    ORDER BY 1 
)
--SELECT DISTINCT created_at FROM aum_snapshot1 
,aum_snapshot2 AS (
	SELECT 
		DATE_TRUNC('day', created_at)::DATE created_at
		, ap_account_id
		, SUM( COALESCE (total_aum_usd, 0)) total_aum_usd
	FROM aum_base 
	GROUP BY 1,2
    ORDER BY 1 
)
,aum_yearly AS (
	SELECT ap_account_id 
		 ,SUM(COALESCE(total_aum_usd,0)) total_aum_1y_usd 
	FROM 
		aum_snapshot2 
	GROUP BY 1 
)
,avg_3m_aum AS (
	SELECT 
	ap_account_id 
	,avg(total_aum_usd) AS avg_aum_3months_usd 
	FROM 
		aum_snapshot1 
	GROUP BY 1 
)
,pcs_users AS (
	SELECT 
		 created_at 
    	,signup_hostcountry
    	, a.ap_account_id 
    	, zmt_locked_amount
    	, zmt_trade_wallet_amount
    	, zmt_zwallet_amount 
    	, zmt_zlaunch_amount 
    	, total_zmt_amount
    	, total_zmt_usd 
    	, other_coins_all_wallet_usd 
    	, total_aum_usd
    	, avg_aum_3months_usd
		,CASE WHEN (zmt_locked_amount >= 20000 OR avg_aum_3months_usd >= 50000) THEN 'pcs'
		 ELSE 'vip3' END AS vip_tier
	FROM aum_snapshot1 a
	LEFT JOIN avg_3m_aum aa  
		ON aa.ap_account_id = a.ap_account_id 
	WHERE (zmt_locked_amount >= 5000 OR avg_aum_3months_usd >= 50000)
	AND created_at >= NOW()::DATE - '1 month'::INTERVAL
-- Jesse - created_at = NOW()::DATE - '1 month'::INTERVAL -- imagine we send this report every 1st of month, it should get the result of previous month
)
--SELECT created_at FROM pcs_users 
,base AS (
	SELECT 
	       pu.ap_account_id 
	      ,up.pii_firstname AS first_name 
	      ,up.pii_lastname  AS last_name 
	      ,up.pii_mobilenumber AS mobile_number 
	      ,up.pii_email AS email 
	      ,up.dob
	      ,up.referral_code 
	      ,vip_tier 
	      ,is_zipup_subscribed 
	      ,zmt_locked_amount
	      ,avg_aum_3months_usd
	      ,zmt_trade_wallet_amount
    	  ,zmt_zwallet_amount 
    	  ,zmt_zlaunch_amount 
    	  ,total_zmt_amount
    	  ,total_zmt_usd 
    	  ,other_coins_all_wallet_usd 
    	  ,total_aum_usd AS eom_aum_usd -- eom_aum_usd
	FROM pcs_users pu
	LEFT JOIN user_pii up 
	ON pu.ap_account_id = up.ap_account_id  
)
,referral AS (
	SELECT  
		u.invitation_code 
		,COUNT(DISTINCT ap_account_id) AS number_of_referral 
	FROM 
		analytics.users_master u 
		WHERE invitation_code NOT IN (SELECT DISTINCT referral_code FROM mappings.growth_referral_code)
			AND invitation_code IN 
			(SELECT DISTINCT referral_code FROM base)
			AND level_increase_status = 'pass'
	GROUP BY 1 
)
,zmt_release AS (
	SELECT 
		  b1.ap_account_id 
		  ,SUM(CASE WHEN release_datetime >= DATE_TRUNC('month', NOW()) - '1 month' :: INTERVAL AND release_datetime  < DATE_TRUNC('month', NOW()) -- RELEASE ON feb
--		  ,SUM(CASE WHEN release_datetime >= DATE_TRUNC('month', NOW()) AND release_datetime < DATE_TRUNC('month', NOW()) + '1 month'::INTERVAL 
		   THEN COALESCE(amount,0)
		   ELSE 0 END) AS zmt_release_this_month 
		  ,SUM(CASE WHEN release_datetime >= DATE_TRUNC('month', NOW()) AND release_datetime < DATE_TRUNC('month', NOW()) + '1 month'::INTERVAL -- RELEASE ON march 
--		  ,SUM(CASE WHEN release_datetime >= DATE_TRUNC('month', NOW()) + '1 month'::INTERVAL AND release_datetime < DATE_TRUNC('month', NOW()) + '2 month'::INTERVAL
		    THEN COALESCE(amount,0)
		    ELSE 0 END) AS zmt_release_next_month 		   
	FROM 
		base b1 
	LEFT JOIN user_pii up 
		ON b1.ap_account_id = up.ap_account_id 
	LEFT JOIN zip_lock_service_public.vault_lock_statuses v
		ON up.user_id = v.user_id 
	    WHERE 
	    	product_id = 'zmt.th' 
	    GROUP BY 1 
--) 
--,trade_volume_month AS (
--	SELECT 
--		  b1.ap_account_id 
--		  ,SUM(tm.amount_usd) AS trade_vol_1m_usd 
--    FROM 
--    	base b1 
--    	LEFT JOIN analytics.trades_master tm
--    		ON tm.ap_account_id = b1.ap_account_id 
--    WHERE tm.created_at >= DATE_TRUNC('month', NOW()) - '1 month' :: INTERVAL
--    	AND tm.created_at < DATE_TRUNC('month', NOW()) 
--    GROUP BY 1 
) 
,trade_volume AS ( 
		SELECT 
		  b1.ap_account_id 
		  ,SUM( CASE WHEN tm.created_at >= DATE_TRUNC('month', NOW()) - '1 month' :: INTERVAL 
		  				THEN tm.amount_usd END) AS trade_vol_1m_usd 
		  ,SUM( CASE WHEN tm.created_at >= DATE_TRUNC('month', NOW()) - '12 month' :: INTERVAL 
		  				THEN tm.amount_usd END) AS trade_vol_1y_usd 
    FROM 
    	base b1 
    	LEFT JOIN analytics.trades_master tm
    		ON tm.ap_account_id = b1.ap_account_id 
    WHERE tm.created_at < DATE_TRUNC('month', NOW())
    GROUP BY 1 
)
	SELECT 
		b1.ap_account_id 
		,b1.first_name 
		,b1.last_name 
		,b1.mobile_number
		,b1.email 
		,p.dob 
		,present_address
		,present_address_district
		,present_address_sub_district
		,present_address_province 
 		,present_address_postal_code 
        ,occupation
		,vip_tier 
		,b1.is_zipup_subscribed 
		,number_of_referral
		,zmt_trade_wallet_amount
		,zmt_zwallet_amount 
		,zmt_locked_amount
		,total_zmt_amount
		,total_zmt_usd 
		,other_coins_all_wallet_usd 
		,eom_aum_usd
		,total_aum_1y_usd 
		,trade_vol_1m_usd 
		,trade_vol_1y_usd 
		,zmt_release_this_month 
		,zmt_release_next_month
	FROM 
		base b1 
		LEFT JOIN referral r 
			ON b1.referral_code = r.invitation_code 
		LEFT JOIN zmt_release zr 
			ON b1.ap_account_id = zr.ap_account_id 
		LEFT JOIN trade_volume_month tvm
			ON b1.ap_account_id = tvm.ap_account_id 
		LEFT JOIN trade_volume_yearly tvy 
			ON b1.ap_account_id = tvy.ap_account_id 
		LEFT JOIN aum_yearly ay 
			ON b1.ap_account_id = ay.ap_account_id 
		LEFT JOIN personna p
			ON b1.ap_account_id = p.ap_account_id  
	ORDER BY vip_tier 
;



-- Matic campaign 2203
WITH user_pii AS (
    SELECT   um.user_id 
            ,um.signup_hostcountry 
            , um.ap_account_id 
           , pii.email AS pii_email 
    FROM 
        analytics.users_master um
        LEFT JOIN analytics_pii.users_pii pii 
        ON um.user_id = pii.user_id 
)
,wordpress_email AS (
    SELECT 
        DISTINCT lower(form_value ->> 'email') AS email
        ,MAX(form_date) AS form_date
    FROM data_imports.wordpress_campaigns
        WHERE form_post_title like 'Matic-Trading-Campaign-0322%'
    GROUP BY 1 
 ) 
,user_trade AS ( 
    SELECT  um.pii_email 
            ,tm.ap_account_id
            ,um.user_id 
            ,COALESCE (SUM(CASE WHEN side = 'Buy' THEN tm.quantity END), 0) AS buy_amount   
            ,COALESCE (SUM(CASE WHEN side = 'Sell' THEN tm.quantity END), 0) AS sell_amount                             
            ,COALESCE (SUM(CASE WHEN side = 'Buy' THEN fm.fee_amount END), 0) AS fee_buy_amount                                 
            ,COALESCE (SUM(CASE WHEN side = 'Sell' THEN fm.fee_amount END), 0) AS fee_sell_amount 
    FROM 
        analytics.trades_master tm 
        LEFT JOIN user_pii um 
            ON tm.ap_account_id = um.ap_account_id 
        LEFT JOIN analytics.fees_master fm                                  
            ON tm.execution_id = fm.fee_reference_id 
            AND fm.fee_type = 'Trade'
    WHERE 
    um.pii_email IN (SELECT DISTINCT email FROM wordpress_email)
        AND product_1_symbol = 'MATIC'
        AND tm.created_at + '7h' >= '2022/03/01 00:00:00'
        AND tm.created_at + '7h' <= '2022/03/14 23:59:59'
        AND um.pii_email NOT LIKE '%zipmex%'
        AND tm.ap_account_id NOT IN 
        (SELECT ap_account_id FROM mappings.users_mapping)
    GROUP BY 1,2,3 
)
,check_withdraw AS (
    SELECT ut.pii_email 
           ,ut.user_id 
           ,ut.ap_account_id 
           ,buy_amount 
           ,sell_amount
           ,fee_buy_amount 
           ,fee_sell_amount 
           ,COALESCE(SUM(wtm.amount),0) AS withdraw_amount 
           ,COALESCE(SUM(fm.fee_amount),0) AS fee_withdraw_amount 
    FROM 
        user_trade ut 
	    LEFT JOIN 
	    	analytics.withdraw_tickets_master wtm 
	        ON ut.ap_account_id = wtm.ap_account_id 
	        AND wtm.created_at + '7h' >= '2022/03/01 00:00:00'
	        AND wtm.created_at + '7h' <= '2022/03/14 23:59:59' 
	        AND wtm.status = 'FullyProcessed'
	        AND wtm.product_symbol = 'MATIC'
	    LEFT JOIN 
	    	analytics.fees_master fm 
	        ON wtm.ticket_id = fm.fee_reference_id
            AND fm.fee_type = 'Withdraw'
    GROUP BY 1,2,3,4,5,6,7  
)
,net_matic AS (
    SELECT cw.* 
           ,buy_amount - (sell_amount + fee_buy_amount + fee_sell_amount + withdraw_amount + fee_withdraw_amount) AS net_buy_matic 
    FROM check_withdraw cw 
)
,eligible_users AS (
    SELECT 
    	nm.* 
    	, SUM(net_buy_matic) OVER() total_matic_pool
    FROM net_matic nm 
    WHERE net_buy_matic >= 20 
)
,total_pool_matic AS (
    SELECT SUM(net_buy_matic) AS total_matic 
    FROM eligible_users 
)
,airdrop_amount AS (
    SELECT eu.* 
           ,CASE WHEN (net_buy_matic/matic.total_matic)*8000:: NUMERIC >= 60 THEN 60 
            ELSE (net_buy_matic/matic.total_matic)*8000 ::NUMERIC END AS amount_earn 
     FROM 
        eligible_users eu
     CROSS JOIN total_pool_matic AS matic 
)
,airdrop AS ( 
    SELECT 116 AS product_id 
          ,round(amount_earn,8) AS amount 
          ,'AIRDROP_2203XX_TH_GROWTH_ACQUI_MATICTRADINGCAMPAIGN_MATIC' as notes
          , 27308 AS from_account_id 
          , pii_email AS to_email 
    FROM 
        airdrop_amount 
)
SELECT * FROM airdrop 
;


--Campaign Period 20 March to 20 April 
--Referees deposit 1THB in z-wallet referers recieve 200 THB airdrop 
WITH referee AS (
SELECT um.user_id AS referee_user_id 
      ,um.ap_account_id AS referee_ap_account_id 
      ,pii.email AS referee_email 
      ,um.invitation_code 
FROM 
    analytics.users_master um 
    LEFT JOIN analytics_pii.users_pii pii 
        ON um.user_id = pii.user_id 
    WHERE um.invitation_code NOT IN (SELECT DISTINCT(referral_code) FROM mappings.growth_referral_code)
    AND um.invitation_code IN (SELECT DISTINCT referral_code FROM analytics.users_master)
    AND um.signup_hostcountry = 'TH'
    AND um.created_at + INTERVAL '7h' >= '2022-03-20'
    AND um.created_at + INTERVAL '7h' <= '2022-04-20'
    AND um.verification_approved_at + INTERVAL '7h' >= '2022-03-20 00:00:00'
    AND um.verification_approved_at + INTERVAL '7h' <= '2022-04-20 23:59:59'
    AND um.has_deposited IS TRUE 
)
,new_zipup AS (
SELECT r.* 
       , COALESCE(SUM(amount * rm.price),0) AS new_amount_zipup_usd 
       , COALESCE(SUM(amount * thb.price),0) AS new_amount_zipup_thb 
FROM referee r 
    LEFT JOIN asset_manager_public.deposit_transactions dt 
        ON r.referee_user_id = dt.account_id 
        AND SOURCE = 'alpha_point'
        AND source_ref = 'accounts+zipmexbalance@zipmex.com'
        AND service_id = 'main_wallet'
        AND upper(SPLIT_PART(product_id,'.',1)) IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
        AND dt.created_at + INTERVAL '7h' >= '2022-03-20 00:00:00'
        AND dt.created_at + INTERVAL '7h' <= '2022-04-20 23:59:59'
    LEFT JOIN analytics.rates_master rm 
        ON upper(SPLIT_PART(product_id,'.',1)) = rm.product_1_symbol
        AND DATE_TRUNC('day', dt.created_at) = DATE_TRUNC('day', rm.created_at)
-- incorrect crypto price in THB
    LEFT JOIN analytics.rates_master thb
        ON DATE_TRUNC('day', dt.created_at) = DATE_TRUNC('day', thb.created_at)                                     
        AND thb.product_1_symbol = 'THB'
    GROUP BY 1,2,3,4
)
,add_referrer AS (
SELECT nz.*
      ,referring_user_id AS referrer_user_id 
      ,pii.email AS referrer_email 
FROM new_zipup nz 
    LEFT JOIN user_app_public.user_referrals 
        ON nz.referee_user_id = invited_user_id 
    LEFT JOIN analytics_pii.users_pii pii 
        ON referring_user_id = pii.user_id 
)
SELECT ar.*
    ,CASE 
        WHEN new_amount_zipup_thb >= 1 THEN TRUE 
        ELSE FALSE END AS eligible_status 
FROM add_referrer ar 
;


-- token price minutes
WITH base AS (
SELECT date_trunc('minute',"time_stamp") AS time_, avg(rate) AS rate_usd, product FROM mm_prod_public.market_rates mr 
WHERE product = 'ETH'
GROUP BY 1,3 
)
SELECT date_trunc('minute',tm.created_at + INTERVAL '11hr') AS gmt_11 
      , tm.ap_account_id 
      ,pii.email 
      , side 
      , tm.product_1_symbol 
      ,quantity 
      , base.time_ + INTERVAL '11hr' AS time_rate  
      ,rate_usd 
FROM analytics.trades_master tm 
    LEFT JOIN analytics.rates_master rm 
        ON date_trunc('day',tm.created_at + INTERVAL '11hr') = date_trunc('day',rm.created_at + INTERVAL '11hr')
        AND rm.product_1_symbol = 'AUD'
    LEFT JOIN analytics_pii.users_pii pii 
        ON tm.ap_account_id = pii.ap_account_id 
    LEFT JOIN base 
        ON date_trunc('minute',tm.created_at) = time_ 
        AND product = tm.product_1_symbol  
WHERE date_trunc('day',tm.created_at+INTERVAL '11hr') = '2022/04/01'
AND tm.ap_account_id = 1268672


-- consent under 20
SELECT
    DISTINCT u.email,
    oa.level_increase_status,
    akyd.kyc_notes
    --al.inserted_at,
  --al.updated_at
FROM
    user_app_public.users u
    LEFT JOIN user_app_public.onfido_applicants oa ON oa.user_id = u.id
    LEFT JOIN exchange_admin_public.audit_logs al ON u.id = REPLACE(al. "object", 'user:', '')
    JOIN user_app_public.additional_kyc_details akyd ON oa.user_id = akyd.user_id
WHERE
    al._fivetran_deleted IS NULL
    AND u.signup_hostname = 'trade.zipmex.co.th'
    AND al.inserted_at + INTERVAL '7 HOURS' >= '2022-02-01 00:00:00'
    AND al.inserted_at + INTERVAL '7 HOURS' < '2022-03-01 00:00:00'
    AND al.action = 'edit_kyc_notes'
    and (akyd.kyc_notes like '%consent%') or (akyd.kyc_notes like '%under 20%')
;



-- zipup 2% cashback
WITH user_pii AS (
    SELECT  um.*
           , pii.email AS pii_email 
    FROM 
        analytics.users_master um
        LEFT JOIN analytics_pii.users_pii pii 
        ON um.user_id = pii.user_id 
)
,wordpress_email AS (
    SELECT 
        DISTINCT lower(form_value ->> 'email') AS email
        ,MAX(form_date) AS form_date
    FROM data_imports.wordpress_campaigns
    WHERE form_post_title IN ('ZipUp-Campaign-0322-TH','ZipUp-Campaign-0322-EN') --> FILTER 
    GROUP BY 1 
 ) 
,user_list AS ( 
    SELECT  
          um.pii_email 
          ,um.user_id         
    FROM user_pii um 
        WHERE um.pii_email IN (SELECT DISTINCT email FROM wordpress_email)
        AND pii_email NOT LIKE '%zipmex%'
        AND ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
        AND pii_email NOT IN (SELECT email FROM mappings.list_of_corporate_account)
        --AND pii_email NOT IN (SELECT * FROM mappings.commercial_emp_fee_rebate) 
        AND um.user_id = '01FFZ9TSKRZ5V3TWFENED07M3Z'
    AND signup_hostcountry = 'TH'
)
,zipup AS (
SELECT  ul.* 
            ,COALESCE(SUM(amount * rm.price),0) AS new_amount_zipup_usd 
            ,COALESCE(SUM(amount * rm.price * thb.price),0) AS new_amount_zipup_thb 
    FROM user_list ul 
        LEFT JOIN asset_manager_public.deposit_transactions dt 
            ON ul.user_id = dt.account_id 
            AND SOURCE = 'alpha_point'
            AND source_ref = 'accounts+zipmexbalance@zipmex.com'
            AND service_id = 'main_wallet'
            AND upper(SPLIT_PART(product_id,'.',1)) IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
            --campaign period 31st march to 15th april 
            AND dt.created_at + INTERVAL '7h' >= '2022-03-31 00:00:00' --> FILTER 
            AND dt.created_at + INTERVAL '7h' <= '2022-04-15 23:59:59' --> FILTER 
        LEFT JOIN analytics.rates_master rm 
            ON upper(SPLIT_PART(product_id,'.',1)) = rm.product_1_symbol
            AND DATE_TRUNC('day', dt.created_at) = DATE_TRUNC('day', rm.created_at)
        LEFT JOIN analytics.rates_master thb 
            ON DATE_TRUNC('day', dt.created_at) = DATE_TRUNC('day', thb.created_at)                                     
            AND thb.product_1_symbol = 'THB'
         where user_id in (select distinct user_id from zip_up_service_tnc.acceptances )
    GROUP BY 1,2
)
,check_trade AS (
SELECT z.* 
            ,COALESCE(SUM(amount * rm.price),0) AS zipup_to_trade_usd 
            ,COALESCE(SUM(amount * rm.price * thb.price),0) AS zipup_to_trade_thb 
    FROM zipup z 
        LEFT JOIN asset_manager_public.withdrawal_transactions wt 
            ON z.user_id = wt.account_id 
            AND destination = 'alpha_point'
            AND destination_ref = 'accounts+zipmexbalance@zipmex.com'
            AND service_id = 'main_wallet'
            AND upper(SPLIT_PART(product_id,'.',1)) IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
            AND wt.created_at + INTERVAL '7h' >= '2022-03-31 00:00:00' --> FILTER 
            AND wt.created_at + INTERVAL '7h' <= '2022-04-15 23:59:59' --> FILTER 
        LEFT JOIN analytics.rates_master rm 
            ON upper(SPLIT_PART(product_id,'.',1)) = rm.product_1_symbol
            AND DATE_TRUNC('day', wt.created_at) = DATE_TRUNC('day', rm.created_at)
        LEFT JOIN analytics.rates_master thb 
            ON DATE_TRUNC('day', wt.created_at) = DATE_TRUNC('day', thb.created_at)                                     
            AND thb.product_1_symbol = 'THB'
    GROUP BY 1,2,3,4
)
,check_ziplock AS (
SELECT ct.* 
            ,COALESCE(SUM(amount * rm.price),0) AS zipup_to_ziplock_usd 
            ,COALESCE(SUM(amount * rm.price * thb.price),0) AS zipup_to_ziplock_thb 
    FROM check_trade ct 
        LEFT JOIN asset_manager_public.transfer_transactions tt 
            ON ct.user_id = tt.to_account_id 
            AND from_service_id = 'main_wallet'
            AND to_service_id = 'zip_lock'
            AND upper(SPLIT_PART(product_id,'.',1)) IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
            AND tt.created_at + INTERVAL '7h' >= '2022-03-31 00:00:00' --> FILTER 
            AND tt.created_at + INTERVAL '7h' <= '2022-04-15 23:59:59' --> FILTER 
        LEFT JOIN analytics.rates_master rm 
            ON upper(SPLIT_PART(product_id,'.',1)) = rm.product_1_symbol
            AND DATE_TRUNC('day', tt.created_at) = DATE_TRUNC('day', rm.created_at)
        LEFT JOIN analytics.rates_master thb 
            ON DATE_TRUNC('day', tt.created_at) = DATE_TRUNC('day', thb.created_at)                                     
            AND thb.product_1_symbol = 'THB'
    GROUP BY 1,2,3,4,5,6
)
,net_zipup as (
SELECT cz.*
       ,new_amount_zipup_usd - (zipup_to_trade_usd + zipup_to_ziplock_usd) AS net_zipup_usd 
       ,new_amount_zipup_thb - (zipup_to_trade_thb + zipup_to_ziplock_thb) AS net_zipup_thb 
FROM check_ziplock cz 
)
,eligible as (
select nz.* 
    -- must zipup a minimum of 1000 thb to be eligible 
    ,case when net_zipup_thb >= {{condition_trade}} then 'eligible'
        else 'noteligible' end as eligible_status 
from net_zipup nz 
)
SELECT * FROM eligible 
;



-- pcs au list 
WITH base AS (
    SELECT 
        a.created_at
        , u.signup_hostcountry
        , a.ap_account_id  
        , a.symbol 
        , CASE WHEN a.symbol = 'ZMT' THEN COALESCE(ziplock_amount,0) 
                END AS zmt_locked_amount 
        , (COALESCE(trade_wallet_amount,0) + COALESCE(z_wallet_amount,0) + COALESCE(ziplock_amount,0) + COALESCE(zlaunch_amount,0)) total_unit
        , (COALESCE(trade_wallet_amount,0) + COALESCE(z_wallet_amount,0) + COALESCE(ziplock_amount,0) + COALESCE(zlaunch_amount,0))*r.price total_unit_usd 
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
        a.created_at >= NOW()::DATE - '12 Week' :: INTERVAL AND a.created_at < NOW()::DATE
        AND a.created_at = DATE_TRUNC('week', a.created_at) 
        AND u.signup_hostcountry IN ('AU')
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
        , COALESCE(SUM( CASE WHEN symbol = 'ZMT' THEN total_unit_usd END),0) zmt_amount_usd 
        , COALESCE(SUM( CASE WHEN symbol NOT IN ('ZMT') THEN total_aum_usd END),0) other_coins_usd 
        , SUM( COALESCE (total_aum_usd, 0)) total_aum_usd
    FROM base 
    GROUP BY 1,2,3
    ORDER BY 1 
)
,avg_3m_aum AS (
    SELECT 
    ap_account_id 
    ,avg(total_aum_usd) AS avg_aum_3months_usd 
    FROM 
        aum_snapshot
    GROUP BY 1 
)
,corp_account AS (
    SELECT 
        uf.user_id 
    FROM user_app_public.features f
    INNER JOIN user_app_public.user_features uf 
        ON f.id = uf.feature_id
    LEFT JOIN analytics.users_master um 
        ON um.user_id = uf.user_id 
    LEFT JOIN analytics_pii.users_pii pii 
        ON uf.user_id = pii.user_id 
    WHERE f.code = 'PCS'
    AND signup_hostcountry = 'AU'
)   
,pcs_users AS (
    SELECT 
        a.created_at 
        ,a.signup_hostcountry
        ,a.ap_account_id 
        ,a.zmt_locked_amount 
        ,a.zmt_amount 
        ,a.zmt_amount_usd 
        ,a.other_coins_usd 
        ,a.total_aum_usd 
        ,avg_aum_3months_usd 
        ,CASE WHEN zmt_locked_amount >= 20000 OR avg_aum_3months_usd >= 50000 
                OR um.user_id IN (SELECT DISTINCT user_id FROM corp_account)
                THEN 'pcs'
              WHEN zmt_locked_amount >= 5000 OR avg_aum_3months_usd  >= 10000
                THEN 'near_pcs'
         ELSE NULL END AS status 
    FROM aum_snapshot a 
    LEFT JOIN avg_3m_aum b
        ON a.ap_account_id = b.ap_account_id 
    LEFT JOIN analytics.users_master um
        ON a.ap_account_id = um.ap_account_id 
    WHERE (zmt_locked_amount >= 5000
        OR avg_aum_3months_usd  >= 10000
        OR um.user_id IN 
        (SELECT DISTINCT user_id FROM corp_account)
        )
    AND a.created_at >= NOW()::DATE - '1 week'::INTERVAL
)
SELECT 
    pu.created_at balance_at,
    pu.ap_account_id,
    up.first_name,
    up.last_name,
    up.email,
    up.mobile_number,
    pu.status,
    zmt_amount AS total_zmt_balance,
    zmt_amount_usd AS total_zmt_balance_usd ,
    other_coins_usd AS other_coins_balance_usd, 
    total_aum_usd,
    avg_aum_3months_usd,
    zmt_locked_amount AS total_zmt_lock_balance,
    SUM(COALESCE(tm.amount_usd,0)) AS total_trade_volume_usd,
    SUM(COALESCE(dtm.amount_usd,0)) AS total_deposit_volume_usd,
    COALESCE(SUM(CASE WHEN dtm.product_symbol IN ('USD','AUD') THEN dtm.amount_usd END),0) AS total_deposit_fiat_usd,
    COALESCE(SUM(CASE WHEN dtm.product_symbol NOT IN ('USD','AUD') THEN dtm.amount_usd END),0) AS total_deposit_crypto_usd, 
    SUM(COALESCE(wtm.amount_usd,0)) AS total_withdraw_volume_usd,
    COALESCE(SUM(CASE WHEN wtm.product_symbol IN ('USD','AUD') THEN wtm.amount_usd END),0) AS total_withdraw_fiat_usd, 
    COALESCE(SUM(CASE WHEN wtm.product_symbol NOT IN ('USD','AUD') THEN wtm.amount_usd END),0) AS total_withdraw_crypto_usd,
    (SUM(COALESCE(dtm.amount_usd,0)) - SUM(COALESCE(wtm.amount_usd,0))) AS net_money_usd 
FROM 
    pcs_users pu
    LEFT JOIN analytics_pii.users_pii up 
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
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13 
;



-- TH - PCS list -- Run monthly at the start of month -- not exclude users_mapping 
WITH user_pii AS (
    SELECT um.*, pii.email AS pii_email ,pii.first_name AS pii_firstname ,pii.last_name  AS pii_lastname ,pii.mobile_number AS pii_mobilenumber 
    FROM analytics.users_master um 
        LEFT JOIN analytics_pii.users_pii pii 
        ON um.user_id = pii.user_id 
    WHERE um.ap_account_id = 143639
)
,aum_base AS (
    SELECT 
        a.created_at
        , u.signup_hostcountry
        , a.ap_account_id  
        , a.symbol 
        , CASE WHEN a.symbol = 'ZMT' THEN COALESCE(ziplock_amount,0) 
                END AS zmt_locked_amount 
        , CASE  WHEN r.product_type = 1 
            THEN (COALESCE(trade_wallet_amount,0) + COALESCE(z_wallet_amount,0) + COALESCE(ziplock_amount,0) + COALESCE(zlaunch_amount,0)) * 1/r.price 
                WHEN r.product_type = 2 
            THEN (COALESCE(trade_wallet_amount,0) + COALESCE(z_wallet_amount,0) + COALESCE(ziplock_amount,0) + COALESCE(zlaunch_amount,0)) * r.price
                END AS total_aum_usd
    FROM 
        analytics.wallets_balance_eod a 
        LEFT JOIN 
            analytics.users_master u 
            ON a.ap_account_id = u.ap_account_id 
        LEFT JOIN 
            analytics.rates_master r 
            ON a.symbol = r.product_1_symbol
            AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
    WHERE 
        a.created_at >= NOW()::DATE - '3 MONTH' :: INTERVAL AND a.created_at < NOW()::DATE
        AND u.signup_hostcountry IN ('TH')
        AND a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '3 day'::INTERVAL 
        AND a.symbol NOT IN ('TST1','TST2')
        AND a.ap_account_id NOT IN (SELECT ap_account_id FROM mappings.users_mapping um2)
    ORDER BY 1 DESC 
) 
,aum_snapshot1 AS (
    SELECT 
        DATE_TRUNC('day', created_at)::DATE created_at
        , signup_hostcountry
        , ap_account_id
        , SUM( COALESCE (zmt_locked_amount ,0)) zmt_locked_amount 
        , SUM( COALESCE (total_aum_usd, 0)) total_aum_usd
    FROM aum_base 
        WHERE 
            DATE_TRUNC('MONTH',created_at) >= DATE_TRUNC('MONTH', NOW() ) - '3 MONTH' :: INTERVAL 
    GROUP BY 1,2,3
    ORDER BY 1 
)
,avg_3m_aum AS (
    SELECT 
    ap_account_id 
    ,avg(total_aum_usd) AS avg_aum_3months_usd 
    FROM 
        aum_snapshot1 
    GROUP BY 1 
)
,pcs_users AS (
    SELECT 
         a.created_at 
        ,a.signup_hostcountry
        , a.ap_account_id 
        , zmt_locked_amount
        , total_aum_usd AS eom_aum_usd
        , avg_aum_3months_usd
        ,CASE WHEN zmt_locked_amount >= 20000 THEN 'vip4'
              WHEN avg_aum_3months_usd > 100000 AND zmt_locked_amount < 20000 THEN 'pcs'
              WHEN zmt_locked_amount >= 5000 AND avg_aum_3months_usd <= 100000 THEN 'vip3'
         ELSE 'near_pcs' END AS vip_tier
    FROM aum_snapshot1 a
    LEFT JOIN avg_3m_aum aa  
        ON aa.ap_account_id = a.ap_account_id 
    WHERE 
--    	(zmt_locked_amount >= 5000 OR avg_aum_3months_usd >= 50000)
    	a.created_at >= NOW()::DATE - '1 month'::INTERVAL
)
SELECT signup_hostcountry , created_at , SUM(eom_aum_usd) FROM pcs_users 
GROUP BY 1,2
;



-- zipworld 2% - 4% bonus
WITH aum_base AS (
    SELECT 
        a.created_at
        , u.signup_hostcountry
        , a.ap_account_id  
        ,u.user_id 
        , a.symbol 
        , CASE WHEN a.symbol = 'BTC' THEN COALESCE(z_wallet_amount,0) 
                END AS btc_zwallet_amount
    FROM 
        analytics.wallets_balance_eod a 
        LEFT JOIN 
            analytics.users_master u 
            ON a.ap_account_id = u.ap_account_id 
        LEFT JOIN 
            analytics.rates_master r 
            ON a.symbol = r.product_1_symbol
            AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
    WHERE 
        --balance at 1st may to 31st may 
        a.created_at >= '2022-05-01'
        AND a.created_at <= '2022-05-31'
        AND a.symbol IN ('BTC')
        AND a.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
        AND u.user_id IN ('01FXFD65WFYY0SGTT6HCZT02VR',
'01FB7H7SMVYWDRSXNPNNFME6Z9',
'01FD8XAA3NY146XJXTW9WR3BNW',
'01FTAX08FHS4CQ2M0ZASNJRV89',
'01FVJBZZVS4ZBPH0T1YQXT56HR',
'01FBSR57V9YK7GGJ4S1B2W1TZ5',
'01FC5PXDDCSP93RW613YQ0W03Y',
'01F9WZTMNXEPKWHPZGYPC64XGY',
'01FJHMMDSJH0XGXVB7HGQE6BM0',
'01F4F5KJPFVNHR5TJMKRDG6HRB',
'01FSHBK4XGC9ZGQ6DH3EZEJ1K4',
'01FDWNF4M8QBCR2SKCMDX9NP8M',
'01FD50WRXNFA973KEK919B6RAB',
'01FATEFPMC26Q2QH9K5J02C9K7',
'01FBMRA2Z0CBH3RZFXG8XADH2A',
'01F7G0DQ7GER39428TRYMEQ6BT',
'01EW3C3A8KW5RCK8HEYTYVF5EE',
'01FAMWQNNCEPMZZP5CGPBHQBCA',
'01FNABNFNRKW7JAAAK6TQNG1J1',
'01FBNY3SDANG7S32JW1GP15GA0',
'01FQZJC78BHP6VDAY2ADN9F9Y2',
'01F9K7FW8WFFJNMTQQRRB0ZJXN',
'01EDX1H84Q6AMCRC5TPW87HC40',
'01FT285RTZZ2BCQ735CNVFF3Z2',
'01FZ5PYJ3ST39X0WZMGHJXFHJS',
'01FPFWBXPBEHKP5XK3A8QFHD3Z',
'01ESNBCSQCFNEGMX8J60XZKJ4Y',
'01FNE8WRV6JJ49P9Y6N220663Q',
'01FRAW19SAKAEXVDK264PKQTRF',
'01F8CKGHY74MSJXPFYKTJ9T9JK',
'01EC47YHS4ZT6APQRK7VTP0M6A',
'01FWC7XBJGCR72VHNM90JZXTGG',
'01FPXB088HY2F10NXZE2DM97JE',
'01FSAAJ4A1RVQ9Z8AQGRP24X3H',
'01EXR9PPM1GCFG69R06TPWY4YT',
'01ESSNRQZFPEVPCRTP3702GHQ9',
'01FBDXV2WBP9KQQ3MB46PMP11X',
'01FZHY26K7Y08RCZM9ZKEWH9NW',
'01ERHVZR7WW6KQ2V055HCCMX48',
'01EVHBX99H9JVABDHT2NCG2XCW',
'01EC47YHS40BT91XP8GAX9720G',
'01FCQTTEXWNHTBBP9QD2E7ESSY',
'01F88TVD42KPYP8V1HFQ9Q26FW',
'01FW8988ZWXW9HGTT8Y2R81Q4A',
'01FSWWSACFSDFSG1E2EQ32G1YB',
'01EJ5HBH19VJY64D1W8YRDKK20',
'01FDA315B4AA6S9E6Q8CS7A5FM',
'01FG13FBF9FGJY9ZPZA3TYEFM1',
'01FC02REG427ZTSZ4Q4Z6Z1GZJ',
'01FRDBAXWW33DGR0135K8YR4QW',
'01EDRA0106QKFHBPD3H9X0JT26',
'01F6RP2E96KQ4F53882DVRGS9E',
'01G08B7FGW80Z4759RSYMY2V5C',
'01FBYCSYM8N84QVGFK2WSPBS97',
'01EZQRN17H7FZ12D2GHHQHCK0K',
'01EC47YPJYAR5VS8K8P0VDXJ2K',
'01EC47YAM43BDD6BB1ERQDTBS4',
'01F23K25GRTP6WSWJMM01FJH2E',
'01FBFNP31R57S8BS0E1BSYARG5',
'01FDCCDTRRFJP71DX2J33PKJH2',
'01FE6AT0H88TP7B05AYYWBHCWX',
'01ES88XQ21XNSWM56EKVVSY2NW',
'01EXYBJPDF40D9SQMNA9TTPAYS',
'01FAKY3PDSBC6FFJFVJ277J6XV',
'01FWDSCPQEMAEERHVW10DY4BJY',
'01FA8DTWDWX7YJVVTNHPXZGH1N',
'01FE9KDYSRVXZQWQVFPC5ZHK5Z',
'01FS729NY625KEQBB6HXNAGC4J',
'01FAVNZV8AJZMGRRDQ4JBMR40J',
'01FAPST1H594ZW280G6G8KVTWK',
'01FZM2BZBKNJA9PJQ330VP4H43',
'01FASDPX9QCFAMW6R1Q8PGJ6W3',
'01FMYRZ50VSBBZEPZ2A1KM9RH9',
'01FYDMPRRS9BJB26XMW1HBXRRW',
'01EV3X8039YA3KVW7FCD3S8XPH',
'01F2719AERR51M2KVP9ZJY1QPP',
'01FBQ65CVQWQVYYDP91PMSJAH9',
'01FVNMGG9WZKXRQJWD4DHT0Z45',
'01G0GMN8FTTQWP8KW8D31K4CDC',
'01F8QATW5Y08X86EK4ZNK8X7KY',
'01FAMG62YP5HR29J9TKZN8D5HH',
'01FZZW5HXRQ5B30CYA6V2RMBTY',
'01FM080RYPH6VEERSPN6CEQR7Q',
'01FYERSQJXEJX77N2MK1SBS18M',
'01EV2BRPHQ4WPBWVRTRZHM7K39',
'01FSAJN3HS67T8VX9NGM57A7PX',
'01EX948VKP7X9JX7B11QK6096T',
'01FWR6PRPP32VZ9TBQYS5X2KHC',
'01EC47Y7DDSXDMJDB1Z6BDJZYE',
'01FXP7HJKXWFPHHQN7C41ZTB56',
'01ESK0A26R62E8MTWNQQMCE8Y0',
'01FV7P8GCM98Q8JQ68YZ3Q18YR',
'01FCJF6HEEZCQP6VWRYZ37JFPE',
'01FR084TDK7R2WANA5QM74ZRJ2',
'01FSGV0W27MPYQG8KSEME3DBMT',
'01FSZ7G2J8CTAVJAC4FPPEKAWB',
'01FZXRY3M2AC23G7HW0V0M33R5',
'01FAG97YH6SPG78FJPNDN8972Q',
'01FWRNXAG4JT0NDJABCGH48PMQ',
'01FDC0JDDH6S7CC8KCVHFJC0TQ',
'01FV7QPN5ZXQAXDAMGM9QGPXQZ',
'01FSRQKBQZ6JGFM69G8CC8V4MZ',
'01FTKCGS5G0BX276QBRFCMZF4K',
'01FR0Q9VVAGC1M95YD2AEHTS73',
'01FS4JJ4AXC491X3E1XS6T8TV6',
'01FWT7C6YEGF90SJ87NGK4NX40',
'01EPCHG49EDHX1A9ZWRSR3NG0N')
        ORDER BY 1 DESC 
)
,aum_snapshot AS (
    SELECT 
        DATE_TRUNC('day',created_at)::DATE created_at
        , ap_account_id
        ,user_id 
        , SUM( COALESCE (btc_zwallet_amount ,0)) btc_zwallet_amount 
    FROM aum_base 
    GROUP BY 1,2,3
    ORDER BY 1 
)
,btc_amount AS (
    SELECT 
            * 
    FROM aum_snapshot 
    WHERE btc_zwallet_amount!= 0 
)
,avg_btc AS (
    SELECT ap_account_id 
           ,user_id 
           ,AVG(btc_zwallet_amount) AS avg_btc_zwallet_amount 
           ,COUNT(ap_account_id) AS n_days 
    FROM 
        btc_amount 
    GROUP BY 1,2 
)
,cal_apy AS (
SELECT user_id 
       ,avg_btc_zwallet_amount 
       ,0.02*(n_days::NUMERIC/365)* avg_btc_zwallet_amount ::NUMERIC AS btc_amount_bonus 
FROM 
    avg_btc 
)
,latest_btc AS (
    SELECT 
         created_at 
        ,average_high_low 
    FROM 
        warehouse.oms_data_public.cryptocurrency_prices 
    WHERE product_1_symbol = 'BTC'
    AND product_2_symbol = 'THB'
    ORDER BY created_at DESC 
    LIMIT 1 
)
,cal_thb AS (
SELECT user_id 
       , avg_btc_zwallet_amount 
       ,btc_amount_bonus 
       ,btc.average_high_low AS btc_price 
       --maximum reward 3300 thb 
       ,CASE WHEN (btc_amount_bonus*btc.average_high_low)::NUMERIC >= 3300 THEN 3300
        ELSE (btc_amount_bonus*btc.average_high_low)::NUMERIC 
        END AS airdrop_amount_thb 
FROM 
    cal_apy 
CROSS JOIN latest_btc AS btc 
) 
SELECT user_id 
       ,avg_btc_zwallet_amount 
       ,btc_amount_bonus 
       ,airdrop_amount_thb 
       ,round(airdrop_amount_thb/btc.average_high_low::NUMERIC,8) AS airdrop_btc 
FROM cal_thb
CROSS JOIN latest_btc AS btc
;



-- booster user info
WITH pii_data AS (
    SELECT up.user_id 
           ,up.first_name 
           ,up.last_name 
           ,up.mobile_number 
           ,CASE WHEN tier_name IS NULL THEN 'vip0'
                ELSE tier_name END AS vip_level 
    FROM 
        analytics_pii.users_pii up 
    LEFT JOIN zip_lock_service_public.user_loyalty_tiers ult 
        ON up.user_id = ult.user_id 
    WHERE up.user_id IN 
('01FTFZF7D969N9FQRDCWKM82WV',
'01FXSW6KV3AKTS33BS8MNK34RW',
'01FJJZ1BEKXF3RGEWFR1ZHQWZN',
'01EQ96G1FF38Y5V6YWREE8F36A')
)
,base AS (
    SELECT 
        a.created_at
        , pd.user_id 
        , pd.first_name 
        , pd.last_name 
        , pd.mobile_number 
        , pd.vip_level 
        , a.symbol 
        , CASE WHEN a.symbol = 'BTC' THEN COALESCE(z_wallet_amount*r.price,0) 
                END AS btc_zwallet_usd 
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
        INNER JOIN pii_data pd 
            ON pd.user_id = u.user_id 
    WHERE 
        a.created_at >= DATE_TRUNC('month', NOW())  AND a.created_at < DATE_TRUNC('day', NOW()) 
        AND ((a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
    -- filter accounts from users_mapping
        AND a.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
    -- exclude test products
        AND a.symbol NOT IN ('TST1','TST2')
        AND a.symbol IN ('BTC')
    ORDER BY 1 DESC 
) 
, aum_snapshot AS (
    SELECT 
        DATE_TRUNC('day', created_at)::DATE created_at
        , user_id 
        , first_name 
        , last_name 
        , mobile_number 
        , vip_level 
        , SUM( COALESCE (btc_zwallet_usd ,0)) btc_zwallet_usd 
        , SUM( COALESCE (total_aum_usd, 0)) btc_total_aum_usd
    FROM base 
    GROUP BY 1,2,3,4,5,6 
    ORDER BY 1 
)
SELECT * FROM aum_snapshot 
;


-- deposit campaigns
WITH user_pii AS (
    SELECT  um.*
           , pii.email AS pii_email 
    FROM 
        analytics.users_master um
        LEFT JOIN analytics_pii.users_pii pii 
        ON um.user_id = pii.user_id 
)
,wordpress_email AS (
    SELECT 
        DISTINCT lower(form_value ->> 'email') AS email
        ,MAX(form_date) AS form_date
    FROM data_imports.wordpress_campaigns
    WHERE form_post_title IN ('Deposit Campaign_20_05_2022_TH_copy','Deposit Campaign_20_05_2022_EN_copy')
    GROUP BY 1 
) 
,user_list AS (
    SELECT  um.user_id 
            ,um.pii_email 
    FROM user_pii um 
        --include only user who signed up 
        WHERE um.pii_email IN (SELECT DISTINCT email FROM wordpress_email)
        AND pii_email NOT LIKE '%zipmex%'
        AND um.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
        AND um.pii_email NOT IN (SELECT DISTINCT zip_acc_email FROM mappings.commercial_emp_fee_rebate)
        AND signup_hostcountry = 'TH'
)
,deposit AS (
    SELECT ul.user_id 
          ,ul.pii_email 
          ,um.ap_account_id 
          ,COALESCE(SUM(amount_usd),0) AS deposit_volume_usd 
          ,COALESCE(SUM(amount_base_fiat),0) AS deposit_volume_thb 
    FROM 
        user_list ul 
    LEFT JOIN analytics.users_master um
        ON ul.user_id = um.user_id 
    LEFT JOIN analytics.deposit_tickets_master d 
        ON um.ap_account_id = d.ap_account_id 
        --campaign period 20 may - 2nd june 
        AND d.created_at + INTERVAL '7h' >= '2022-05-20 00:00:00'
        AND d.created_at + INTERVAL '7h' <= '2022-06-02 23:59:59'
        AND status = 'FullyProcessed'
    GROUP BY 1,2,3 
) 
,withdraw AS (
    SELECT d.* 
          ,COALESCE(SUM(amount_usd),0) AS withdraw_volume_usd 
          ,COALESCE(SUM(amount_base_fiat),0) AS withdraw_volume_thb 
          ,COALESCE(SUM(fm.fee_usd_amount),0) AS fee_withdraw_volume_usd 
          ,COALESCE(SUM(fm.fee_base_fiat_amount),0) AS fee_withdraw_volume_thb 
    FROM 
        deposit d 
    LEFT JOIN analytics.withdraw_tickets_master w 
        ON d.ap_account_id = w.ap_account_id 
        --user must hold until 9th june 
        AND w.created_at + INTERVAL '7h' >= '2022-05-20 00:00:00'
        AND w.created_at + INTERVAL '7h' <= '2022-06-02 23:59:59'
        AND status = 'FullyProcessed'
    LEFT JOIN analytics.fees_master fm 
        ON w.ticket_id = fm.fee_reference_id
        AND fee_type = 'Withdraw'
    GROUP BY 1,2,3,4,5 
) 
,net_deposit AS (
    SELECT w.* 
          ,deposit_volume_usd - (withdraw_volume_usd + fee_withdraw_volume_usd) AS net_deposit_volume_usd 
          ,deposit_volume_thb - (withdraw_volume_thb + fee_withdraw_volume_thb) AS net_deposit_volume_thb 
    FROM 
        withdraw w 
)
,eligible_users_campaign AS (
    SELECT nd.* 
            --net deposit must be >= 2000 thb 
           ,CASE WHEN net_deposit_volume_thb >= 2000 THEN TRUE 
                 ELSE FALSE END AS eligible_status_during  
    FROM net_deposit nd 
)
,deposit_after AS (
    SELECT ul.user_id 
          ,ul.pii_email 
          ,um.ap_account_id 
          ,COALESCE(SUM(amount_usd),0) AS deposit_volume_usd 
          ,COALESCE(SUM(amount_base_fiat),0) AS deposit_volume_thb 
    FROM 
        user_list ul 
    LEFT JOIN analytics.users_master um
        ON ul.user_id = um.user_id 
    LEFT JOIN analytics.deposit_tickets_master d 
        ON um.ap_account_id = d.ap_account_id 
        AND d.created_at + INTERVAL '7h' >= '2022-05-20 00:00:00'
        AND d.created_at + INTERVAL '7h' <= '2022-06-09 23:59:59'
        AND status = 'FullyProcessed'
    GROUP BY 1,2,3 
) 
,withdraw_after AS (
    SELECT d.* 
          ,COALESCE(SUM(amount_usd),0) AS withdraw_volume_usd
          ,COALESCE(SUM(amount_base_fiat),0) AS withdraw_volume_thb 
          ,COALESCE(SUM(fm.fee_usd_amount),0) AS fee_withdraw_volume_usd 
          ,COALESCE(SUM(fm.fee_base_fiat_amount),0) AS fee_withdraw_volume_thb 
    FROM 
        deposit_after d 
    LEFT JOIN analytics.withdraw_tickets_master w 
        ON d.ap_account_id = w.ap_account_id 
        --user must hold until 9th june 
        AND w.created_at + INTERVAL '7h' >= '2022-05-20 00:00:00'
        AND w.created_at + INTERVAL '7h' <= '2022-06-09 23:59:59'
        AND status = 'FullyProcessed'
    LEFT JOIN analytics.fees_master fm 
        ON w.ticket_id = fm.fee_reference_id
        AND fee_type = 'Withdraw'
    GROUP BY 1,2,3,4,5 
) 
,net_deposit_after AS (
    SELECT w.* 
          ,deposit_volume_usd - (withdraw_volume_usd + fee_withdraw_volume_usd) AS net_deposit_volume_usd
          ,deposit_volume_thb - (withdraw_volume_thb + fee_withdraw_volume_thb) AS net_deposit_volume_thb
    FROM 
        withdraw_after w 
)
,eligible_users_campaign_after AS (
    SELECT nd.* 
            --net deposit must be > 0 at 9 june 
           ,CASE WHEN net_deposit_volume_thb > 0 THEN TRUE 
                 ELSE FALSE END AS eligible_status_after 
    FROM net_deposit_after nd 
)
,amount AS (
    SELECT a.pii_email 
            --capped amount at 5000 thb 
           ,CASE WHEN (a.net_deposit_volume_thb::NUMERIC/ 2000) * 100 >= 5000 THEN 5000
                ELSE (a.net_deposit_volume_thb::NUMERIC/ 2000) * 100 END AS amount_earn_thb 
           , ROW_NUMBER () OVER(PARTITION BY a.pii_email) row_ 
    FROM 
        eligible_users_campaign a 
    LEFT JOIN eligible_users_campaign_after b 
        ON a.user_id = b.user_id 
    WHERE eligible_status_during IS TRUE 
    AND eligible_status_after IS TRUE 
    ORDER BY 3 DESC 
)
--airdrop in USDC 
,latest_USDC AS (
    SELECT 
        average_high_low 
    FROM warehouse.oms_data_public.cryptocurrency_prices cp
    WHERE product_2_symbol = 'THB'
    AND product_1_symbol = 'USDC'
    ORDER BY created_at DESC 
    LIMIT 1 
)
,airdrop_calculation AS (
    SELECT pii_email 
           ,round(amount_earn_thb/usdc.average_high_low :: NUMERIC,8) AS airdrop_amount 
    FROM amount 
    CROSS JOIN latest_USDC AS usdc 
)
,airdrop AS (
    SELECT 34 AS product_id 
            ,airdrop_amount AS amount   
           ,27308 AS from_account_id 
           ,pii_email AS to_email 
    FROM 
        airdrop_calculation 
)
SELECT SUM(amount) FROM airdrop 
;


--Campaign Period 1 June to 31 July --Referees deposit 1THB in trade-wallet 
WITH referee AS (
SELECT um.user_id AS referee_user_id 
      ,um.ap_account_id AS referee_ap_account_id 
      ,pii.email AS referee_email 
      ,um.invitation_code 
      ,first_deposit_at + INTERVAL '7h' AS first_deposited_gmt7 
      , CASE WHEN first_deposit_at + INTERVAL '7h' <= '2022-06-15 23:59:59' THEN 'batch_1'
      		WHEN first_deposit_at + INTERVAL '7h' <= '2022-06-30 23:59:59' THEN 'batch_2'
      		WHEN first_deposit_at + INTERVAL '7h' <= '2022-07-15 23:59:59' THEN 'batch_3'
      		WHEN first_deposit_at + INTERVAL '7h' <= '2022-07-31 23:59:59' THEN 'batch_4'
      		END AS airdrop_batch
FROM 
    analytics.users_master um 
    LEFT JOIN analytics_pii.users_pii pii 
        ON um.user_id = pii.user_id 
    WHERE um.invitation_code NOT IN (SELECT DISTINCT(referral_code) FROM mappings.growth_referral_code)
    AND um.invitation_code IN (SELECT DISTINCT referral_code FROM analytics.users_master)
    AND um.signup_hostcountry = 'TH'
    AND um.created_at + INTERVAL '7h' >= '2022-06-01 00:00:00'
    AND um.created_at + INTERVAL '7h' <= '2022-07-31 23:59:59'
    --Change these period 
    AND first_deposit_at + INTERVAL '7h' >= '2022-06-01 00:00:00'
    AND first_deposit_at + INTERVAL '7h' <= '2022-07-31 23:59:59'
    AND pii.email NOT LIKE '%zipmex%'
)
,add_referrer AS (
SELECT r.* 
      ,referring_user_id AS referrer_user_id 
      ,pii.email AS referrer_email 
FROM referee r 
    LEFT JOIN user_app_public.user_referrals 
        ON r.referee_user_id = invited_user_id 
    LEFT JOIN analytics_pii.users_pii pii 
        ON referring_user_id = pii.user_id 
WHERE pii.email NOT LIKE '%zipmex%'
)
,latest_usdt AS (
select 
    created_at 
    , average_high_low 
    from warehouse.oms_data_public.cryptocurrency_prices cp
        where product_2_symbol ='THB'
        and product_1_symbol = 'USDC'
    order by
        created_at desc
    limit 1
) 
,referee_airdrop AS (
    SELECT referee_email 
           ,200 AS amount_thb 
           ,average_high_low AS btc_price 
           ,round((200/average_high_low::NUMERIC),8) AS btc_amount 
    FROM 
    add_referrer  
    CROSS JOIN latest_usdt 
)
,referrer_airdrop AS (
    SELECT referrer_email 
           ,200 AS amount_thb 
           ,average_high_low AS btc_price 
           ,round((200/average_high_low::NUMERIC),8) AS btc_amount 
    FROM 
    add_referrer  
    CROSS JOIN latest_usdt  
)
,full_list AS(
SELECT * FROM referee_airdrop 
UNION ALL 
SELECT * FROM referrer_airdrop 
)
SELECT 
    34 AS product_id 
    ,SUM(btc_amount) AS amount 
    ,27308 AS from_account_id 
    ,referee_email AS to_email 
FROM full_list 
GROUP BY 1,3,4 
ORDER BY amount DESC 
;



-- pcs behaviors, balance
WITH aum_base AS (
    SELECT 
        a.created_at
        , u.signup_hostcountry
        , a.ap_account_id  
        , a.symbol 
        , CASE  
            WHEN r.product_type = 1 THEN trade_wallet_amount * 1/r.price 
            WHEN r.product_type = 2 THEN trade_wallet_amount * r.price 
            END AS trade_wallet_amount_usd
        , z_wallet_amount * r.price z_wallet_amount_usd
        , ziplock_amount * r.price ziplock_amount_usd
        , CASE WHEN a.symbol = 'ZMT' THEN COALESCE(ziplock_amount,0) 
                END AS zmt_locked_amount 
        , (COALESCE(trade_wallet_amount,0) + COALESCE(z_wallet_amount,0) + COALESCE(ziplock_amount,0) + COALESCE(zlaunch_amount,0)) total_unit
        , (COALESCE(trade_wallet_amount*r.price,0) + COALESCE(z_wallet_amount*r.price,0) + COALESCE(ziplock_amount*r.price,0) + COALESCE(zlaunch_amount*r.price,0)) total_unit_usd 
        , CASE  WHEN r.product_type = 1 
            THEN (COALESCE(trade_wallet_amount,0) + COALESCE(z_wallet_amount,0) + COALESCE(ziplock_amount,0) + COALESCE(zlaunch_amount,0)) * 1/r.price 
                WHEN r.product_type = 2 
            THEN (COALESCE(trade_wallet_amount,0) + COALESCE(z_wallet_amount,0) + COALESCE(ziplock_amount,0) + COALESCE(zlaunch_amount,0)) * r.price
                END AS total_aum_usd
    FROM 
        analytics.wallets_balance_eod a 
        LEFT JOIN 
            analytics.users_master u 
            ON a.ap_account_id = u.ap_account_id 
        LEFT JOIN 
            analytics.rates_master r 
            ON a.symbol = r.product_1_symbol
            AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
    WHERE 
        a.created_at >= NOW()::DATE - '7 MONTH' :: INTERVAL AND a.created_at < NOW()::DATE - '4 MONTH'::INTERVAL  
        AND u.signup_hostcountry IN ('TH')
        AND a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL 
        AND a.symbol NOT IN ('TST1','TST2')
        AND a.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
    ORDER BY 1 DESC 
) 
,aum_snapshot1 AS (
    SELECT 
        DATE_TRUNC('day', created_at)::DATE created_at
        , signup_hostcountry
        , ap_account_id
        , SUM(COALESCE(trade_wallet_amount_usd,0)) AS trade_wallet_amount_usd 
        , SUM(COALESCE(z_wallet_amount_usd,0)) AS z_wallet_amount_usd
        , SUM(COALESCE(ziplock_amount_usd,0)) AS ziplock_amount_usd
        , SUM( COALESCE (zmt_locked_amount ,0)) zmt_locked_amount 
        , COALESCE( SUM( CASE WHEN symbol = 'ZMT' THEN total_unit END), 0) total_zmt_amount
        , COALESCE( SUM( CASE WHEN symbol = 'ZMT' THEN total_unit_usd END), 0) total_zmt_usd 
        , COALESCE( SUM( CASE WHEN symbol != 'ZMT' THEN total_aum_usd END), 0) other_coins_all_wallet_usd 
        , SUM( COALESCE (total_aum_usd, 0)) total_aum_usd
    FROM aum_base 
    GROUP BY 1,2,3
    ORDER BY 1 
)
,avg_3m_aum AS (
    SELECT 
    ap_account_id 
    ,avg(total_aum_usd) AS avg_aum_3months_usd 
    FROM 
        aum_snapshot1 
    GROUP BY 1 
)
SELECT a.created_at 
      ,a.signup_hostcountry 
      ,a.ap_account_id
      ,pii.email 
      ,CASE WHEN zmt_locked_amount >= 20000 THEN 'vip4'
            WHEN avg_aum_3months_usd > 100000 AND zmt_locked_amount < 20000 THEN 'pcs'
            WHEN zmt_locked_amount >= 5000 AND avg_aum_3months_usd <= 100000 THEN 'vip3'
            ELSE 'near_pcs' END AS status 
      ,trade_wallet_amount_usd 
      ,z_wallet_amount_usd
      ,ziplock_amount_usd
      ,zmt_locked_amount 
      ,total_zmt_amount
      ,total_zmt_usd 
      ,other_coins_all_wallet_usd
      ,total_aum_usd
      ,avg_aum_3months_usd 
      , SUM(d.usd_buy_amount) usd_buy_amount 
      , SUM(d.usd_buy_amount - d.usd_net_buy_amount) usd_sell_amount 
      , SUM(d.sum_usd_trade_amount) usd_trade_amount 
      , SUM(d.sum_usd_deposit_amount) sum_usd_deposit_amount 
      , SUM(d.sum_usd_withdraw_amount) sum_usd_withdraw_amount 
--      ,COALESCE(SUM(CASE WHEN tm.side = 'Buy' THEN tm.amount_usd END),0) buy_volume_usd 
--      ,COALESCE(SUM(CASE WHEN tm.side = 'Sell' THEN tm.amount_usd END),0) sell_volume_usd 
--      ,SUM(COALESCE(tm.amount_usd,0)) AS total_trade_volume_usd
--      ,SUM(COALESCE(dtm.amount_usd,0)) AS total_deposit_volume_usd
--      ,COALESCE(SUM(CASE WHEN dtm.product_symbol IN ('THB') THEN dtm.amount_usd END),0) AS total_deposit_fiat_usd
--      ,COALESCE(SUM(CASE WHEN dtm.product_symbol NOT IN ('THB') THEN dtm.amount_usd END),0) AS total_deposit_crypto_usd
--      ,SUM(COALESCE(wtm.amount_usd,0)) AS total_withdraw_volume_usd
--      ,COALESCE(SUM(CASE WHEN wtm.product_symbol IN ('THB') THEN wtm.amount_usd END),0) AS total_withdraw_fiat_usd
--      ,COALESCE(SUM(CASE WHEN wtm.product_symbol NOT IN ('THB') THEN wtm.amount_usd END),0) AS total_withdraw_crypto_usd
FROM 
    aum_snapshot1 a 
	    LEFT JOIN avg_3m_aum 
	        ON a.ap_account_id = avg_3m_aum.ap_account_id 
	    LEFT JOIN analytics.users_master um 
	        ON a.ap_account_id = um.ap_account_id 
	    LEFT JOIN analytics_pii.users_pii pii 
	        ON a.ap_account_id = pii.ap_account_id 
	    LEFT JOIN reportings_data.dm_user_transactions_dwt_daily d
	    	ON a.ap_account_id = d.ap_account_id 
	        AND DATE_TRUNC('month',a.created_at)::DATE = DATE_TRUNC('month',d.created_at)::DATE
    WHERE a.created_at >= '2022-01-31'
    	AND a.ap_account_id = 199904
    	AND (zmt_locked_amount >= 5000 OR avg_aum_3months_usd >= 50000)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14 
;