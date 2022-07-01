with first_deposit_date as (
SELECT
	ap_account_id
	, min(created_at) as first_deposit_date 
FROM
	analytics.deposit_tickets_master
where
	status = 'FullyProcessed'
	and ap_account_id not in (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
GROUP BY 1
), first_deposit_date_and_amount as (
SELECT
	f.ap_account_id
	, date_trunc('day', f.first_deposit_date) as first_deposit_date
	, sum(amount_usd) as first_deposit_amount_usd
FROM
	first_deposit_date f
left JOIN
	analytics.deposit_tickets_master d
	on f.ap_account_id = d.ap_account_id
	and f.first_deposit_date = d.created_at
GROUP BY 1,2
ORDER BY 1
),first_trade_at as (
SELECT
	ap_account_id
	, min(created_at) as first_lock_at
FROM
	analytics.trades_master
WHERE
	ap_account_id not in (SELECT DISTINCT ap_account_id from mappings.users_mapping)
GROUP BY 1)
, first_trade_at_and_amount as (
SELECT
	ft.ap_account_id
	, date_trunc('day',ft.first_lock_at) as first_trade_at
	, sum(t.amount_usd) as first_trade_amount_usd
FROM
	first_trade_at ft
LEFT JOIN
	analytics.trades_master t
	on ft.ap_account_id = t.ap_account_id
	and ft.first_lock_at = t.created_at
group by 1,2
ORDER by ft.ap_account_id asc
), first_lock_at as (
SELECT
	a.ap_account_id
	, min(a.created_at) as first_lock_at
from analytics.wallets_balance_eod a
where 
	ziplock_amount > 0
	and date_trunc('day', created_at) >= '2021-10-01' 
	and ap_account_id not in (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
GROUP by 1
)
, firt_lock_at_and_amount as (
select 
	a.ap_account_id
	, a.first_lock_at
	, SUM( COALESCE (a2.ziplock_amount * r.price,0)) first_ziplock_amount_usd
from first_lock_at a
left join analytics.wallets_balance_eod a2
on a.ap_account_id = a2.ap_account_id 
and a.first_lock_at = a2.created_at 
LEFT JOIN analytics.rates_master r 
ON a2.symbol = r.product_1_symbol
AND DATE_TRUNC('day', a2.created_at) = DATE_TRUNC('day', r.created_at)
group by 1, 2
ORDER by a.ap_account_id asc
),registration_channel as (
SELECT
	r.user_id
	, u.ap_account_id
	, source_group
	, medium_group
	, channel_group
FROM
	analytics.registration_channel_master r
LEFT JOIN
	analytics.users_master u
	on r.user_id = u.user_id
WHERE
	u.ap_account_id not in (SELECT DISTINCT ap_account_id from mappings.users_mapping)
	and u.is_verified = TRUE
), fee as (
select 
	t.ap_account_id
    , u.user_id
    , date_trunc('month',t.created_at) created_at
	,case	when coalesce(tm.product_2_symbol,case when t.fee_product in ('THB','USD','SGD','AUD','IDR') THEN t.fee_product ELSE t.base_fiat end) = 'IDR' then 'ID' 
			when coalesce(tm.product_2_symbol,case when t.fee_product in ('THB','USD','SGD','AUD','IDR') THEN t.fee_product ELSE t.base_fiat end) = 'THB' then 'TH'
			when coalesce(tm.product_2_symbol,case when t.fee_product in ('THB','USD','SGD','AUD','IDR') THEN t.fee_product ELSE t.base_fiat end) = 'AUD' then 'AU'
			when coalesce(tm.product_2_symbol,case when t.fee_product in ('THB','USD','SGD','AUD','IDR') THEN t.fee_product ELSE t.base_fiat end) = 'SGD' then 'global'
			when coalesce(tm.product_2_symbol,case when t.fee_product in ('THB','USD','SGD','AUD','IDR') THEN t.fee_product ELSE t.base_fiat end) = 'USD' then 'global'
			else t.signup_hostcountry end reporting_country
--	,t.fee_type 
--	,t.fee_product
	,sum(case when t.ap_account_id = '317029' and t.fee_type = 'Trade' then 0 else coalesce(t.fee_usd_amount,case when t.fee_product in ('THB','USD','SGD','AUD','IDR') THEN t.fee_amount/p.price else t.fee_amount*p.price end) end) "fee_usd_amount"
	,sum(case when t.ap_account_id = '317029' and t.fee_type = 'Trade' then 0 else case when t.fee_type = 'Trade' then coalesce(t.fee_usd_amount,case when t.fee_product in ('THB','USD','SGD','AUD','IDR') THEN t.fee_amount/p.price else t.fee_amount*p.price end) else 0 end end) "Trade Fee"
	,sum(case when t.fee_type = 'Deposit' then coalesce(t.fee_usd_amount,case when t.fee_product in ('THB','USD','SGD','AUD','IDR') THEN t.fee_amount/p.price else t.fee_amount*p.price end) else 0 end) "Deposit Fee"
	,sum(case when t.fee_type = 'Withdraw' then coalesce(t.fee_usd_amount,case when t.fee_product in ('THB','USD','SGD','AUD','IDR') THEN t.fee_amount/p.price else t.fee_amount*p.price end) else 0 end) "Withdraw Fee"
from analytics.fees_master t
left join analytics.users_master u on t.ap_account_id = u.ap_account_id 
left join analytics.trades_master tm on t.fee_reference_id = tm.execution_id and tm.created_at >= '2021-09-01' and tm.created_at < '2021-10-01'
left join analytics.rates_master p on date_trunc('day',t.created_at) = date_trunc('day',p.created_at) and t.fee_product = p.product_1_symbol and p.product_2_symbol = 'USD'
where 
	t.created_at >= date_trunc('month',cast(now() - interval '6 month' as date)) and t.created_at < cast(now() as date)
	and u.signup_hostcountry in ('TH','AU','ID','global')
	AND t.ap_account_id NOT IN (select ap_account_id from warehouse.mappings.users_mapping um)
group by 
	1,2,3,4
--)
--, trading_margin as (
--select
--	ap_account_id
--    , date_trunc('month',"timestamp") created_at
--	,case	when CCY = 'THB' then 'TH'
--			when CCY = 'IDR' then 'ID'
--			when CCY = 'AUD' then 'AU'
--			when CCY = 'SGD' then 'global'
--			when CCY = 'USD' then 'global'
--			when CCY = 'USDT' then 'global' end reporting_country
--	,sum(pnl) Trading_Margin
--from 
--(select 
--	tm.ap_account_id
--    ,m.source
--	--,m.bucket_id 
--	,t.id 
--	--,m.trade_id 
--	--,t.instrument 
--	,case when right(t.instrument,4) = 'USDT' then 'USDT' else right(t.instrument ,3) end CCY
--	,t.hedgeinstrument 
--	,t.way 
--	,t.price 
--	,t.quantity 
--	,t.fx
--	,t.hedgeway 
--	,t.hedgeprice 
--	,t.hedgequantity 
--	,m."timestamp"
--	,t.pnl 
--	--,t."time" 
--	--,t."insertedAt" 
--from mm_prod_public.reports_bucket t
--	left join (select distinct trade_id,"timestamp",bucket_id, source source
--    from mm_prod_public.bucket_trade_map t
--    where t.source not in ('AP')
--    ) m 
--    on t.id = cast(m.bucket_id as int)
--    left join analytics.trades_master tm 
--    on m.trade_id = cast(tm.trade_id as varchar)
--where --m."source" <> 'AP'
--	--and t."date" >= '2021-07-31 17:00:00' and t."date" < '2021-08-24 17:00:00'
--	cast(t.id as varchar) in 
--	(	select bucket_id
--		from mm_prod_public.bucket_trade_map
--		where "timestamp" >= date_trunc('month',cast(now() - interval '6 month' as date)) and "timestamp" < date_trunc('day',cast(now() as date)))) t
--group by 1,2,3
--)
--, base AS (
--	SELECT 
--		DATE_TRUNC('month', a.created_at) created_at
--		, CASE WHEN u.signup_hostcountry IS NULL THEN 'unknown' ELSE u.signup_hostcountry END AS signup_hostcountry
--		, a.ap_account_id 
--		, up.email 
--		, u.user_id 
--	 filter nominee accounts from users_mapping
--		, CASE WHEN a.ap_account_id IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id <> 496001)
--				THEN TRUE ELSE FALSE END AS is_nominee 
--	 filter asset_manager account
--		, CASE WHEN a.ap_account_id = 496001 THEN TRUE ELSE FALSE END AS is_asset_manager
--	 zipup subscribe status to identify zipup amount
--		, u.zipup_subscribed_at , u.is_zipup_subscribed 
--		, a.symbol 
--		, r.price usd_rate 
--		, trade_wallet_amount
--		, z_wallet_amount
--		, ziplock_amount
--		, CASE	WHEN r.product_type = 1 THEN trade_wallet_amount * 1/r.price 
--				WHEN r.product_type = 2 THEN trade_wallet_amount * r.price
--				END AS trade_wallet_amount_usd
--		, z_wallet_amount * r.price z_wallet_amount_usd
--		, ziplock_amount * r.price ziplock_amount_usd
--	FROM 
--		analytics.wallets_balance_eod a 
--	 get country and join with pii data
--		LEFT JOIN 
--			analytics.users_master u 
--			ON a.ap_account_id = u.ap_account_id 
--	 get pii data 
--		LEFT JOIN 
--			analytics_pii.users_pii up 
--			ON u.user_id = up.user_id 
--	 coin prices and exchange rates (USD)
--		LEFT JOIN 
--			analytics.rates_master r 
--			ON a.symbol = r.product_1_symbol
--			AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
--	WHERE 
--		a.created_at >= DATE_TRUNC('month', NOW() - '6 month'::INTERVAL) AND a.created_at < DATE_TRUNC('day', NOW())::DATE -- DATE_TRUNC('month', NOW() - '1 month'::INTERVAL)
--		AND 
--		u.signup_hostcountry IN ('TH','ID','AU','global')
--	 snapshot by end of month or yesterday
--		AND ((a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
--	 exclude test products
--		AND a.symbol NOT IN ('TST1','TST2')
--	ORDER BY 1 DESC 
--)
--, aum AS (
--	SELECT 
--		DATE_TRUNC('month', created_at) created_at
--		, signup_hostcountry
--		, ap_account_id
--		, symbol
--        , CASE WHEN symbol IN ('BTC','ETH','GOLD','LTC','USDC','USDT') THEN symbol -- 'zipup_coin' 
--				WHEN symbol = 'ZMT' THEN 'ZMT' 
--				ELSE 'other' END AS asset_group
--		, SUM( COALESCE (trade_wallet_amount, 0)) trade_wallet_amount
--		, SUM( COALESCE (z_wallet_amount, 0)) z_wallet_amount
--		, SUM( COALESCE (ziplock_amount, 0)) ziplock_amount
--		, SUM( COALESCE (trade_wallet_amount_usd, 0)) trade_wallet_amount_usd
--		, SUM( COALESCE (z_wallet_amount_usd, 0)) z_wallet_amount_usd
--		, SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
--		, SUM( COALESCE (CASE WHEN is_zipup_subscribed = TRUE AND created_at >= DATE_TRUNC('day', zipup_subscribed_at) AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
--					THEN
--						(CASE 	WHEN created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
--								WHEN created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
--					END, 0)) AS zwallet_subscribed_usd
--	FROM 
--		base 
--	WHERE 
--		is_asset_manager = FALSE AND is_nominee = FALSE
--	GROUP BY 
--		1,2,3,4
--	ORDER BY 
--		1 
--)
--, growth_interest_rates as (
--select 
--	cast(year as integer) as year
--	, cast(case 
--		when length(month) <2 then concat('0',month)
--		else month end as integer) as month_fix
--	, case when "signup_hostcountry " = 'SG' then 'global' 
--		else "signup_hostcountry " end as "signup_hostcountry "
--	, (replace(gross_interest_pa,'%','') :: numeric)/100 as gross_interest_pa
--	, symbol
--from mappings.growth_interest_rates
--)
--, aum_interest as (
--select
--    ap_account_id
--    , created_at
--    , signup_hostcountry
--    , gross_interest_pa
--    , SUM(z_wallet_amount_usd) as z_wallet_amount_usd
--    , SUM(ziplock_amount_usd) as ziplock_amount_usd
--    , SUM(z_wallet_amount_usd + ziplock_amount_usd) as zipup_ziplock_amount_usd
--    , SUM(z_wallet_amount_usd*gross_interest_pa/12.00) as zipup_interest_earned
--    , SUM(ziplock_amount_usd*gross_interest_pa/12.00) as ziplock_interest_earned
--    , SUM((z_wallet_amount_usd + ziplock_amount_usd)*gross_interest_pa/12.00) as zipup_ziplock_interest_earned
--from aum a
--left join growth_interest_rates gir
--on EXTRACT(YEAR FROM a.created_at) = gir.year
--    and EXTRACT(MONTH FROM a.created_at) = gir.month_fix
--	and a.symbol = gir.symbol
--where gir.symbol IS NOT NULL
--group by 1,2,3,4)
--, revenue as (
--select
--    f.ap_account_id
--    , f.user_id
--    , f.created_at
--    , EXTRACT(YEAR FROM f.created_at) as year
--    , EXTRACT(MONTH FROM f.created_at) as month
--    , f.reporting_country
--    , coalesce("fee_usd_amount",0) as fee_usd_amount
--    , coalesce("trading_margin",0) as trading_margin
--    , coalesce(zipup_interest_earned,0) as zipup_interest_earned
--    , coalesce(ziplock_interest_earned,0) as ziplock_interest_earned
--    , coalesce(zipup_ziplock_interest_earned,0) as zipup_ziplock_interest_earned
--    , coalesce(fee_usd_amount,0)+coalesce(trading_margin,0)+coalesce(zipup_ziplock_interest_earned,0) as revenue
--	, coalesce(zipup_ziplock_amount_usd,0)+coalesce(ziplock_amount_usd,0) as zipup_ziplock_amount_usd
--	, coalesce(amount_usd,0) as trade_amount_usd
--from fee f
--left join trading_margin tm
--on f.created_at = tm.created_at
--    and f.ap_account_id = tm.ap_account_id
--left join aum_interest aum
--on f.created_at = aum.created_at
--    and f.ap_account_id = aum.ap_account_id
--left join trade tr
--on f.created_at = tr.created_at
--and f.ap_account_id = tr.ap_account_id
--), ltv_cal as (
--select 
--    r.ap_account_id
--    , platform
--	, geo_continent
--	, geo_country
--	, geo_region
--	, geo_city
--	, app_identifier
--	, source_raw
--	, medium_raw
--	, campaign_raw
--	, content_raw
--	, landing_page
--	, referrer_page
--	, landing_page_affliate_code
--	, exsist_in_appsflyer
--	, appsflyer_media_source
--	, appsflyer_attributed_touch_type
--	, agency
--	, source_group
--	, medium_group
--	, campaign_group
--	, appsflyer_gp_referrer
--	, channel_group
--	, internal_campaign
--	, SUM(fee_usd_amount) as fee_usd_amount
--	, SUM(trading_margin) as trading_margin
--	, SUM(zipup_ziplock_amount_usd) as zipup_ziplock_amount_usd
--	, SUM(zipup_ziplock_interest_earned) as zipup_ziplock_interest_earned
--	, SUM(COALESCE(revenue,0)) as revenue
--	, SUM(trade_amount_usd) as trade_amount_usd
--from revenue r
--left join analytics.registration_channel_master rc
--on r.user_id = rc.user_id
--where source_group is not null
--group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
), user_pii as (
select 
	um.*
	, pii.email as pii_email
from 
	analytics.users_master um
left join 
	analytics_pii.users_pii pii
	on um.user_id = pii.user_id
), all_users as (
SELECT
	ur.invited_user_id
	, pii.pii_email as referee_email
	, pii.ap_account_id as referee_ap_account_id
	, pii.signup_hostcountry
	, ur.referring_user_id 
	, pii_referrer.pii_email as referrer_email
	, pii_referrer.ap_account_id as referrer_ap_account_id
	, pii_referrer.signup_hostcountry
FROM
	oms_data_user_app_public.user_referrals ur
LEFT JOIN
	user_pii pii
	on ur.invited_user_id = pii.user_id 
LEFT JOIN
	user_pii pii_referrer
	on ur.referring_user_id = pii_referrer.user_id
), transfer_check as (
select 
	tm.transfer_id
	, tm.created_at + interval '7h' as airdropped_date
	, tm.receiver_ap_account_id
	, tm.receiver_signup_hostcountry as signup_hostcountry
	, pii.pii_email
	, tm.notes
	, cac.campaign_name
	, tm.usd_amount as airdrop_reward_usd
from 
	analytics.transfers_master tm
left join 
	user_pii pii
	on tm.receiver_ap_account_id = pii.ap_account_id
left join
	mappings.commercial_airdrop_campaign cac
	on tm.notes = cac.airdrop_notes
-- Mapping with user_funnel in mappings_table
WHERE 
	notes in (
		select 
			distinct airdrop_notes 
		from
			mappings.commercial_airdrop_campaign c 						 --1) Campaign name
		WHERE
			user_funnel in ('Referral')
			--(airdrop_notes like '%TRADEFEEREFERRAL%' or airdrop_notes like lower('%TRADEFEEREFERRAL%')))
	and tm.created_at >= '2022-01-01')
), referee_user as (
-- Find referee user
SELECT
	receiver_ap_account_id
	, pii_email
FROM
	transfer_check tc
WHERE
	pii_email in (select DISTINCT referee_email from all_users)
	and receiver_ap_account_id in (select DISTINCT referee_ap_account_id from all_users) --select referee users from all_users
), referrer_user as (
-- Find referrer user
SELECT
	receiver_ap_account_id
	, pii_email
FROM
	transfer_check tc
WHERE
	pii_email in (select DISTINCT referrer_email from all_users)
	and receiver_ap_account_id in (select DISTINCT referrer_ap_account_id from all_users) --select referrer users from all_users
), total_user as (
-- Find total user
	select 
		receiver_ap_account_id
		, pii_email
	from 
		transfer_check
group by 1,2
), campaign_with_ltv_and_airdrop as (
-- Find total users from campaigns with ltv and airdrop reward
SELECT 
	date_trunc('month', airdropped_date) airdropped_date
	, u.user_id 														 -- 2) user_id
	, receiver_ap_account_id as ap_account_id
	, tc.signup_hostcountry
	, pii_email
	, u.created_at 														 -- 3)  register_date
	, u.verification_approved_at 										 -- 4)  verified_date
	, u.first_deposit_at 												 -- 5)  first_deposit_at
	, coalesce(d.first_deposit_amount_usd,0) as first_deposit_amount_usd -- 6)  first-_deposit_amount
	, u.first_traded_at 												 -- 7)  first_trade_at
	, coalesce(t.first_trade_amount_usd,0) as first_trade_amount_usd	 -- 8)  first_trade_amount
	, w.first_lock_at													 -- 9)  first_lock_at
	, coalesce(w.first_ziplock_amount_usd,0) as first_ziplock_amount_usd -- 10) first_lock_amount
	, rc.source_group													 -- 11) source_group 
	, rc.medium_group													 -- 12) medium_group
	, rc.channel_group													 -- 13) channel_group
	, notes
	, campaign_name
--	, coalesce(ltv.revenue,0) as ltv_amount_usd							 -- 15) ltv_amount_usd 
	, coalesce(airdrop_reward_usd,0) as airdrop_reward_amount_usd		 -- 14) airdrop_reward_amount
FROM 
	transfer_check tc
--left join
--	ltv_cal ltv
--	on ltv.ap_account_id = tc.receiver_ap_account_id
left join
	analytics.users_master u
	on tc.receiver_ap_account_id = u.ap_account_id
left join
	first_deposit_date_and_amount d
	on tc.receiver_ap_account_id = d.ap_account_id
left join
	first_trade_at_and_amount t
	on tc.receiver_ap_account_id = t.ap_account_id
left join
	firt_lock_at_and_amount w
	on tc.receiver_ap_account_id = w.ap_account_id
left join
	registration_channel rc
	on tc.receiver_ap_account_id = rc.ap_account_id
WHERE 
	receiver_ap_account_id in (select DISTINCT receiver_ap_account_id from total_user)
)
-- Find referee users from campaigns with ltv and airdrop reward
SELECT 
	* 
FROM 
	campaign_with_ltv_and_airdrop
WHERE 
	ap_account_id in (select DISTINCT receiver_ap_account_id from referee_user)

-- Find referrer users from campaigns with ltv and airdrop reward

-- SELECT 	
-- 	* 
-- FROM 
-- 	campaign_with_ltv_and_airdrop
-- WHERE 
-- 	ap_account_id in (select DISTINCT receiver_ap_account_id from referrer_user)