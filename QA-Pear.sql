-- TRUE SMS campaign
-- 11 Feb-3 Mar
WITH user_pii AS (
	SELECT 
		um.*
		, pii.email AS	 pii_email
	FROM analytics.users_master um
		LEFT JOIN analytics_pii.users_pii pii 
		ON um.user_id = pii.user_id 
)
,user_submitted_kyc AS (
	SELECT 
		ss.inserted_at
		,up.pii_email
		,up.user_id
		,up.level_increase_status 
		,up.invitation_code 
	FROM user_app_public.suitability_surveys ss 
		LEFT JOIN user_pii up 
			ON ss.user_id = up.user_id
	WHERE ss.inserted_at + INTERVAL '7 HOURS' >= '2022-02-11 00:00:00'
		AND ss.inserted_at + INTERVAL '7 HOURS' <= '2022-03-07 23:59:59'
		AND lower(up.invitation_code)  = 'truexzipmex2'
)
SELECT
	pii_email, level_increase_status, invitation_code
FROM user_submitted_kyc
WHERE level_increase_status = 'pass'


-- FINNOMENA
with user_list as(
SELECT um.*
, pii.email as pii_email
, pii.mobile_number as pii_mobile_number 
, pii.document_number as id_card_number
FROM analytics.users_master um
LEFT JOIN analytics_pii.users_pii pii ON pii.user_id = um.user_id
)
,trade_vol as (
SELECT ap_account_id
, execution_id
, SUM(amount_usd) as tradevol_usd
, SUM(amount_base_fiat) as tradevol_thb
FROM analytics.trades_master
WHERE (created_at + INTERVAL '7 HOURS') >= (DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL)
AND (created_at + INTERVAL '7 HOURS' ) < (DATE_TRUNC('day', NOW()))
GROUP BY 1,2
ORDER BY 1 DESC
)
,trade_fee as (
SELECT ap_account_id
, fee_reference_id
, SUM(fee_usd_amount) as trade_fee_usd
, SUM(fee_base_fiat_amount) as trade_fee_thb
FROM analytics.fees_master
WHERE (created_at + INTERVAL '7 HOURS') >= (DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL)
AND (created_at + INTERVAL '7 HOURS' ) < (DATE_TRUNC('day', NOW()))
AND fee_type = 'Trade'
GROUP BY 1,2
ORDER BY 1 DESC
)
SELECT a.invitation_code
, pii_email AS email
, a.id_card_number
, pii_mobile_number AS mobile_number
, a.created_at AS register_date
, a.level_increase_status AS kyc_status
, a.verification_approved_at AS passed_kyc_date
, SUM(t.tradevol_thb) trade_vol_thb
, SUM(f.trade_fee_thb) trade_fee_thb
FROM user_list a
LEFT JOIN trade_vol t ON t.ap_account_id = a.ap_account_id
LEFT JOIN trade_fee f ON f.fee_reference_id = t.execution_id
WHERE a.invitation_code = 'FINNOMENA'
--AND a.signup_hostname = 'TH'
GROUP BY 1,2,3,4,5,6,7
;


-- MOMMEJZM
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
,um.created_at + INTERVAL '7H' AS created_at_gm7
,um.invitation_code
,um.signup_hostcountry
,um.level_increase_status
,um.verification_approved_at
FROM
analytics.users_master um
LEFT JOIN user_pii up
ON up.user_id = um.user_id
WHERE
um.invitation_code in ('MOMMEJZM')
AND um.ap_account_id not in (select distinct ap_account_id from mappings.users_mapping)
--kyc passed during 20 Mar-20 Apr
AND um.verification_approved_at + interval '7h' >= '2022/03/12 00:00:00'
AND um.verification_approved_at + interval '7h'<= '2022/05/12 23:59:59'
AND pii_email not like '%zipmex%'
AND um.signup_hostcountry = 'TH'
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
SELECT a.user_id
,a.ap_account_id
,a.pii_email
,round(200/btc.average_high_low:: NUMERIC,8) as airdrop_amount
FROM eligible_users a
CROSS JOIN latest_btc as btc
)
,airdrop AS (
SELECT 25 AS product_id
, ac.airdrop_amount AS amount
, 'AIRDROP_220XXX_TH_GROWTH_ACQUI_MOMMEJZM_BTC' as notes
, 27308 as from_account_id
, ac.pii_email as to_email
FROM airdrop_calculation ac
left join airdrop_status a on a.receiver_ap_account_id = ac.ap_account_id
)
SELECT * FROM airdrop
;



-- risk type 3
with user_pii as(
SELECT um.*
, pii.email as pii_email
, adk.risk_type as risk_type
FROM analytics.users_master um
LEFT JOIN analytics_pii.users_pii pii ON pii.user_id = um.user_id
LEFT JOIN user_app_public.additional_kyc_details adk ON adk.user_id = um.user_id
)
--,aum as (
SELECT			
		up.signup_hostcountry	
		, up.ap_account_id	
		, up.pii_email
		, up.risk_type
--		, a.symbol	
		, SUM( case when a.risk_type = 3 THEN coalesce(trade_wallet_amount*r.price,0) END) trade_wallet_usd 
		, SUM( case when a.risk_type = 3 THEN COALESCE(z_wallet_amount*r.price,0) END ) z_wallet_usd
		, SUM( case when a.risk_type = 3 THEN COALESCE(ziplock_amount*r.price,0) END ) ziplock_wallet_usd
		, SUM( case when a.risk_type = 3 THEN COALESCE(zlaunch_amount*r.price,0) END ) zlaunch_wallet_usd
	FROM		
		analytics.wallets_balance_eod a	
		RIGHT JOIN 
			user_pii up 
			ON a.ap_account_id = up.ap_account_id 
		LEFT JOIN	
			analytics.rates_master r
			ON a.symbol = r.product_1_symbol
			AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
	WHERE		
		a.created_at = '2022-03-26'-- DATE_TRUNC('day', a.created_at) - '1 day'::INTERVAL	
		AND a.symbol NOT IN ('TST1','TST2')	
		AND up.signup_hostcountry = 'TH'
		and up.level_increase_status = 'pass'
	group by 1,2,3,4
	ORDER BY 1 DESC		
)
SELECT pii_email AS email
, a.level_increase_status AS kyc_status
, a.verification_approved_at AS passed_kyc_date
, a.risk_type
, coalesce(sum(case when a.risk_type = 3 then u.trade_wallet_usd END), 0) total_trade_wallet_usd 
, coalesce(sum(case when a.risk_type = 3 then u.z_wallet_usd END), 0) total_z_wallet_usd
, coalesce(sum(case when a.risk_type = 3 then u.ziplock_wallet_usd END), 0) total_ziplock_wallet_usd
, coalesce(sum(case when a.risk_type = 3 then u.zlaunch_wallet_usd END), 0) total_zlaunch_wallet_usd
FROM user_pii a
LEFT JOIN aum u ON u.ap_account_id = a.ap_account_id
where a.signup_hostcountry = 'TH'
and a.level_increase_status = 'pass'
GROUP BY 1,2,3,4
;



-- CNX camaign
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
        ,um.created_at + INTERVAL '7H' AS created_at_gm7
        ,um.invitation_code
        ,um.signup_hostcountry
        ,um.level_increase_status
        ,um.verification_approved_at + INTERVAL '7H' AS kyc_approved_at_gm7
    FROM
        analytics.users_master um
        LEFT JOIN user_pii up
        ON up.user_id = um.user_id
    WHERE
        um.invitation_code in ('BMCNX22','CAMPCNX22')
        AND um.ap_account_id not in (select distinct ap_account_id from mappings.users_mapping)
        --kyc passed during 20 Mar-20 Apr
        AND um.verification_approved_at + interval '7h' >= '2022/04/07 00:00:00'
        AND um.verification_approved_at + interval '7h'<= '2022/04/30 23:59:59'
        AND pii_email not like '%zipmex%'
        AND um.signup_hostcountry = 'TH'
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
    SELECT a.user_id
        ,a.ap_account_id
        ,a.pii_email
    ,round(300/btc.average_high_low:: NUMERIC,8) as airdrop_amount
    FROM eligible_users a
    CROSS JOIN latest_btc as btc
)
,airdrop AS (
SELECT 25 AS product_id
    , ac.airdrop_amount AS amount
    , 'AIRDROP_2204XX_TH_GROWTH_ACQUI_CNX_BTC' as notes
    , 27308 as from_account_id
    , ac.pii_email as to_email
FROM airdrop_calculation ac
)
SELECT * FROM airdrop


-- mtu acitivity
WITH base AS (
SELECT 	um.user_id
		, pii.age
		, um.created_at::DATE AS register_date
		, um.verification_approved_at::DATE AS verified_date
FROM analytics.users_master um 
	LEFT JOIN analytics_pii.users_pii pii 
			ON pii.user_id = um.user_id 
WHERE 	um.created_at + INTERVAL '7 HOURS' >= '2022-03-01 00:00:00'
		AND um.created_at + INTERVAL '7 HOURS' <= '2022-04-30 23:59:59'
		AND um.signup_hostcountry = 'ID'
		AND um.verification_approved_at IS NOT NULL 
)
, activities AS (
SELECT 	user_id
		, SUM(sum_usd_deposit_amount) AS sum_usd_deposit_amount
		, SUM(sum_usd_withdraw_amount) AS sum_usd_withdraw_amount
		, SUM(sum_usd_trade_amount) AS sum_usd_trade_amount
FROM reportings_data.dm_user_transactions_dwt_daily
WHERE signup_hostcountry = 'ID'	
GROUP BY 1
)
SELECT b.*
		, rcm.source_group 
		, sum_usd_deposit_amount
		, sum_usd_withdraw_amount
		, sum_usd_trade_amount
FROM base b
	LEFT JOIN activities a
			ON b.user_id = a.user_id
	LEFT JOIN analytics.registration_channel_master rcm 
			ON rcm.user_id = b.user_id 
;


--mtu behavior ID
WITH base AS (
SELECT  um.user_id
        , um.ap_account_id 
        , pii.age
        , um.created_at::DATE AS register_date
        , um.verification_approved_at::DATE AS verified_date
FROM analytics.users_master um 
    LEFT JOIN analytics_pii.users_pii pii 
            ON pii.user_id = um.user_id 
WHERE   um.created_at + INTERVAL '7 HOURS' >= '2022-03-01 00:00:00'
        AND um.created_at + INTERVAL '7 HOURS' <= '2022-04-30 23:59:59'
        AND um.signup_hostcountry = 'ID'
        AND um.verification_approved_at IS NOT NULL 
)
, aum_base AS (
SELECT  a.created_at 
        , a.ap_account_id 
        , b.user_id
        , a.symbol 
        , trade_wallet_amount * r.price AS trade_wallet_amount_usd
        , z_wallet_amount * r.price AS z_wallet_amount_usd
        , ziplock_amount * r.price AS ziplock_amount_usd
FROM analytics.wallets_balance_eod a 
    LEFT JOIN base b
            ON a.ap_account_id = b.ap_account_id
    LEFT JOIN analytics.rates_master r 
            ON a.symbol = r.product_1_symbol
            AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at) 
WHERE   a.created_at = '2022-04-30'
        AND a.symbol NOT IN ('TST1','TST2')
)
, aum_snapshot AS (
SELECT user_id 
        , SUM(COALESCE(trade_wallet_amount_usd,0)) AS trade_wallet_amount_usd
        , SUM(COALESCE(z_wallet_amount_usd,0)) AS z_wallet_amount_usd 
        , SUM(COALESCE(ziplock_amount_usd,0)) AS ziplock_amount_usd
FROM aum_base
GROUP BY 1
)
, activities AS (
SELECT  user_id
        , SUM(sum_usd_deposit_amount) AS sum_usd_deposit_amount
        , SUM(sum_usd_withdraw_amount) AS sum_usd_withdraw_amount
        , SUM(sum_usd_trade_amount) AS sum_usd_trade_amount
FROM reportings_data.dm_user_transactions_dwt_daily
WHERE signup_hostcountry = 'ID' 
GROUP BY 1
)
SELECT b.*
        , rcm.source_group 
        , sum_usd_deposit_amount
        , sum_usd_withdraw_amount
        , sum_usd_trade_amount
        , trade_wallet_amount_usd AS trade_wallet_amount_usd
        , z_wallet_amount_usd AS z_wallet_amount_usd
        , ziplock_amount_usd AS ziplock_amount_usd
FROM base b
    LEFT JOIN aum_snapshot ab
            ON ab.user_id = b.user_id
    LEFT JOIN activities a
            ON b.user_id = a.user_id 
    LEFT JOIN analytics.registration_channel_master rcm 
            ON rcm.user_id = b.user_id 
;


-- ID potential PCS
WITH deposit_vol AS (
	SELECT 
		dm.ap_account_id 
		, COUNT(DISTINCT dm.ticket_id) count_deposits 
		, SUM(dm.amount_usd) sum_deposit_amount_usd
	FROM 
		analytics.deposit_tickets_master dm
	WHERE dm.status = 'FullyProcessed'
	GROUP BY 1
)	, withdraw_vol AS (
	SELECT 
		wm.ap_account_id 
		, COUNT(DISTINCT wm.ticket_id) count_withdraws
		, SUM(wm.amount_usd) sum_withdraw_amount_usd
	FROM 
		analytics.withdraw_tickets_master wm
	WHERE wm.status = 'FullyProcessed'
	GROUP BY 1
)
	, trade_vol AS (
	SELECT 
		tm.ap_account_id 
		, COUNT(DISTINCT tm.trade_id) count_trades 
		, SUM(tm.amount_usd) sum_trade_volume_usd
	FROM 
		analytics.trades_master tm 
	GROUP BY 1
)
SELECT 
	um.ap_account_id 
	, up.email
	, up.mobile_number 
	, um.level_increase_status 
	, um.base_fiat 
	, d.sum_deposit_amount_usd 
	, w.sum_withdraw_amount_usd  
	, (d.sum_deposit_amount_usd - w.sum_withdraw_amount_usd) AS net_usd
	, t.sum_trade_volume_usd 
FROM analytics.users_master um 
    LEFT JOIN analytics_pii.users_pii up 
    	ON up.user_id = um.user_id 
	LEFT JOIN trade_vol t 
		ON um.ap_account_id = t.ap_account_id 
	LEFT JOIN deposit_vol d 
		ON um.ap_account_id = d.ap_account_id 
	LEFT JOIN withdraw_vol w 
		ON um.ap_account_id = w.ap_account_id 
WHERE 
	up.mobile_number LIKE '+61433990033'
--	up.mobile_number::TEXT IN (SELECT mobile_number FROM mappings.commercial_id_potential_pcs WHERE mobile_number LIKE '%6%')
;


