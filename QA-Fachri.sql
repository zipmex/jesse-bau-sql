--go_for_gold_campaign
WITH user_master AS 
(
    SELECT 
        date(u.created_at + interval '7h') "register_date"
        ,date(u.verification_approved_at + interval '7h') "verified_at"
        ,date(u.zipup_subscribed_at + interval '7h') "zipup_subscribed_at"
        ,u.user_id
        ,c.user_id "parent_user_id"
        ,u.ap_account_id 
        ,b.email 
        ,d.email "parent_email"
        ,u.signup_hostcountry
        ,TRUE "is_register"
        ,u.is_verified 
        ,u.is_zipup_subscribed
        ,u.invitation_code 
        ,u.referral_code 
    FROM 
        analytics.users_master u
    LEFT JOIN 
        analytics_pii.users_pii b ON u.user_id = b.user_id 
    LEFT JOIN 
        analytics.users_master c on u.referring_user_id = c.user_id
    LEFT JOIN 
        analytics_pii.users_pii d ON d.user_id = c.user_id
    WHERE 
        u.signup_hostcountry = 'ID'
)
, referred_by as (
	SELECT
	    a.invitation_code
	, count(a.email) "refer_count"
	from user_master a 
	where a.invitation_code <> ''
	group BY
	    1
)
, sus_email as (
	SELECT
	    a.email
	,   regexp_replace(split_part(a.email, '@', 1), '[^a-zA-Z]', '', 'g') as email_grouping
	, a.invitation_code
	from user_master a
)
, sus_email_count as (
	SELECT
	    a.email_grouping
	, count(a.email) as total_users
	from sus_email a
	where a.email_grouping <> ''
	group BY
	    1
)
, sus_pool as (
	SELECT
	    a.receiver_user_id
	, 'pool' as sus_type
	from wallet_app_public.transfer_tickets a
	where a."state" = 'fully_processed'
	and a.ap_receiver_account_id is not null
	group BY
	    1
-- having count(distinct a.ap_account_id) > 2
	having count(distinct a.ap_account_id) > 10
)
, sus_smurf as (
	SELECT
	    DISTINCT
	    a.user_id
	, 'smurf' as sus_type
	from wallet_app_public.transfer_tickets a
	where a."state" = 'fully_processed'
	and EXISTS(
	SELECT
	1
	from sus_pool b
	where a.receiver_user_id = b.receiver_user_id
	)
)
, sus_trf as (
	SELECT
	    coalesce(a.receiver_user_id, b.user_id) as user_id
	, coalesce(a.sus_type, b.sus_type) as sus_type
	from sus_pool a
	full outer join sus_smurf b
	on a.receiver_user_id = b.user_id
)
, "final_table" AS 
(
	SELECT 
	    a.*
	    ,CASE   WHEN a.invitation_code is not null THEN 'reff' ELSE 'not_reff' END "referral_status"
	    ,CASE   WHEN a.parent_email LIKE ('%campaigns%') THEN 'campaign' 
	            WHEN a.parent_email IS NULL THEN 'organic'
	            ELSE 'friend_referral' 
	     END "reff_source"
	    ,b.email_grouping
	FROM 
	    user_master a 
	LEFT JOIN 
	    sus_email b ON a.email = b.email
)
, "dashboard" AS
(
	SELECT 
	a.*
	,d.refer_count
	,b.total_users
	,CASE 
	        WHEN b.total_users >= 20 THEN 'high_sus'
	        WHEN b.total_users >= 1 THEN 'not_sus'
	        ELSE 'suspect'
	     END "user_sus_status"
	FROM 
	    final_table a
	LEFT JOIN 
	    sus_email_count b ON a.email_grouping = b.email_grouping
	LEFT JOIN 
	    referred_by d ON d.invitation_code = a.referral_code 
)
, dashboard_final AS 
(
	SELECT 
	    a.*
	    ,b.user_sus_status "parent_sus_status"
	    ,c.sus_type
	    ,d.sus_type "parent_sus_type"
	FROM 
	    dashboard a
	LEFT JOIN 
	    dashboard b ON a.parent_email = b.email
	LEFT JOIN 
	    sus_trf c on c.user_id = a.user_id
	LEFT JOIN 
	    sus_trf d on d.user_id = a.parent_user_id
)
, user_master_suspect as 
(
	SELECT
	    a.*
	    , left(split_part(a.email, '@', 1), 3)||'***'||right(split_part(a.email, '@', 1), 3)||'@'||split_part(a.email, '@', 2) as email_mask
	    ,CASE 
	        WHEN a.sus_type in ('smurf','pool') or a.parent_sus_type in ('smurf','pool') THEN 'abuser'
	        WHEN a.user_sus_status in ('high_sus') or a.parent_sus_status in ('high_sus') THEN 'abuser'
	        ELSE 'normal_user'
	     END "user_final_status"
	FROM
	    dashboard_final a
)
, gold_trade as 
(
-- Jesse - eligible users to be defined by cumulative trade volume over the period
    SELECT 
    -- this structure gives trade amount by day, not cumulative
        date_trunc('day',t.created_at + interval '7h') ::DATE "tday"
        ,t.ap_account_id 
        ,t.symbol
        ,ROW_NUMBER () OVER (PARTITION BY t.ap_account_id order by date(t.created_at + INTERVAL '7h') ) AS "occurance" 
        ,t.amount_usd * 14500 "amount_idr"
    FROM 
        analytics.trades_master t
    WHERE 
        t.signup_hostcountry = 'ID'
    AND 
    -- this conditions, at this point, filter trade by trade_id, not by day, not by the whole period
        t.quantity >= 9
    AND
    -- there are users traded with GOLD USDT , do we need to count them?
        t.symbol = 'GOLDIDR'
    AND 
        date(t.created_at + interval '7h') >= '2022-02-21' AND date(t.created_at + interval '7h') <= '2022-02-28'  
    ORDER BY 1 ASC 
)
, campiagn_ticket AS 
(
SELECT 
    t.ap_account_id 
    ,min(t.tday) "tday"
    ,'pass' "ticket"
FROM 
    gold_trade t 
WHERE 
    t.occurance = 1
GROUP BY 1
)
, gold_volume as 
(
    SELECT 
         t.ap_account_id 
        ,t.symbol
        ,sum(t.amount_usd) * 14500 "amount_idr"
    FROM 
        analytics.trades_master t
    WHERE 
        t.signup_hostcountry = 'ID'
    AND
        t.symbol = 'GOLDIDR'
    AND 
        date(t.created_at + interval '7h') >= '2022-02-21' AND date(t.created_at + interval '7h') <= '2022-02-28'  
    GROUP BY 1,2
    ORDER BY 1 ASC 
)
 , withdrawal_master AS 
(
    SELECT
        d.ap_account_id 
        ,max(date(d.created_at + interval '7h')):: DATE "last_withdrawal"
        ,sum(d.amount_base_fiat) "withdraw_idr"
    FROM 
        analytics.withdraw_tickets_master d
    WHERE 
        d.signup_hostcountry = 'ID'
    AND 
        d.product_symbol = 'IDR'
    AND 
        date(d.created_at + interval '7h') >= '2022-02-21' AND date(d.created_at + interval '7h') <= '2022-02-28'
    GROUP BY 1
    ORDER BY 2 ASC
 )
 , transfer_zmt AS 
(
	SELECT
	a.ap_account_id  
	,sum(a.amount) "transfer_amount"
	from 
	    wallet_app_public.transfer_tickets a
	WHERE state = 'fully_processed'
	AND date(a.completed_at) >= '2022-02-21' AND date(a.completed_at) <= '2022-02-28'
	GROUP BY 1
)  
, received_zmt AS 
(
	SELECT
	a.ap_receiver_account_id  
	,sum(a.amount) "received_amount"
	from 
	    wallet_app_public.transfer_tickets a
	WHERE state = 'fully_processed'
	AND date(a.completed_at) >= '2022-02-21' AND date(a.completed_at) <= '2022-02-28'
	GROUP BY 1
)  
 , draft_tok_campaign AS 
(
	SELECT   d.register_date
	        ,a.ap_account_id
	        ,d.email_mask
	        ,b.tday "pass_time"
	        ,c.last_withdrawal
	        ,b.ticket
	        ,a.symbol
	        ,a.amount_idr "trade_volume"
	        ,c.withdraw_idr
	        ,d.reff_source
	        ,d.user_final_status
	        ,d.is_verified
	FROM 
	        gold_volume a  
	LEFT JOIN 
	        campiagn_ticket b on a.ap_account_id = b.ap_account_id
	LEFT JOIN 
	        withdrawal_master c on a.ap_account_id = c.ap_account_id
	LEFT JOIN 
	        user_master_suspect d on a.ap_account_id = d.ap_account_id
)
, gold_trade_final as 
(
	SELECT
	    a.*
	    ,b.transfer_amount
	    ,c.received_amount
	    ,CASE
	        WHEN a.register_date >= '2022-02-21' THEN 'new_user'
	        ELSE 'existing_user'
	     END "user_status"
	    ,CASE 
	        WHEN a.ticket = 'pass' THEN TRUE
	        ELSE FALSE
	     END "is_campaign_eligible"
	FROM
	    draft_tok_campaign a 
	LEFT JOIN 
	    transfer_zmt b on a.ap_account_id = b.ap_account_id
	LEFT JOIN 
	    received_zmt c on a.ap_account_id = c.ap_receiver_account_id
	order by 6,8 DESC
)
, tok_volume_before as 
(
    SELECT 
         t.ap_account_id 
        ,t.symbol
        ,sum(t.amount_usd) * 14500 "amount_before"
    FROM 
        analytics.trades_master t
    WHERE 
        t.signup_hostcountry = 'ID'
    AND
        t.symbol = 'GOLDIDR'
    AND 
        date(t.created_at + interval '7h') >= '2022-02-13' AND date(t.created_at + interval '7h') <= '2022-02-20'  
    GROUP BY 1,2
    ORDER BY 1 ASC 
)
, tok_volume_after as 
(
    SELECT 
         t.ap_account_id 
        ,t.symbol
        ,sum(t.amount_usd) * 14500 "amount_after"
    FROM 
        analytics.trades_master t
    WHERE 
        t.signup_hostcountry = 'ID'
    AND
        t.symbol = 'GOLDIDR'
    AND 
        date(t.created_at + interval '7h') >= '2022-03-01' AND date(t.created_at + interval '7h') <= '2022-03-08'  
    GROUP BY 1,2
    ORDER BY 1 ASC 
)
SELECT
a.*
,b.amount_before
,c.amount_after
FROM
gold_trade_final a 
LEFT JOIN tok_volume_before b on a.ap_account_id = b.ap_account_id
LEFT JOIN tok_volume_after c on a.ap_account_id = c.ap_account_id
;



-- gold trade during campaign
WITH base AS (
	SELECT 
		(tm.created_at + '7 hour'::INTERVAL)::DATE created_at_gmt7
		, (um2.created_at + '7 hour'::INTERVAL)::DATE register_gmt7
		, tm.signup_hostcountry 
		, tm.ap_account_id 
		, tm.product_1_symbol 
		, tm.product_2_symbol 
		, SUM(tm.quantity) trade_during_unit
		, SUM(tm.amount_usd) trade_during_usd
	FROM analytics.trades_master tm 
		LEFT JOIN analytics.users_master um2 
			ON tm.ap_account_id = um2.ap_account_id 
	WHERE 
		tm.created_at + '7 hour'::INTERVAL >= '2022-02-21'
		AND tm.created_at + '7 hour'::INTERVAL < '2022-03-01'
		AND tm.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping um)
		AND tm.symbol IN ('GOLDTHB','GOLDIDR')
		AND tm.signup_hostcountry IN ('TH','ID')
	GROUP BY 1,2,3,4,5,6
)	, eligible_user AS (
	SELECT 
		created_at_gmt7
		, register_gmt7
		, signup_hostcountry 
		, ap_account_id 
		, product_1_symbol 
		, trade_during_unit
		, trade_during_usd
		, SUM(trade_during_unit) OVER(PARTITION BY ap_account_id) cumulative_trade_amount_unit
		, CASE WHEN (SUM(trade_during_unit) OVER(PARTITION BY ap_account_id)) >= 9 THEN TRUE ELSE FALSE END AS is_eligible
	FROM base 
)	, active_mtu AS (
	SELECT 
		dmd.mtu_day  
		, dmd.ap_account_id 
		, ROW_NUMBER() OVER(PARTITION BY dmd.ap_account_id ORDER BY dmd.mtu_day DESC) row_ 
	FROM analytics.dm_mtu_daily dmd  
	WHERE 
		dmd.mtu = TRUE
		AND dmd.mtu_day >= '2022-01-20'
		AND dmd.mtu_day < '2022-02-21'
)	, final_list AS (
	SELECT 
		eu.*
		, am.mtu_day::DATE 
		, '2022-02-21'::DATE - am.mtu_day::DATE inactive_time
	FROM eligible_user eu
		LEFT JOIN active_mtu am 
			ON eu.ap_account_id = am.ap_account_id
			AND am.row_ = 1
)	, trade_before AS (
	SELECT 
		tm.ap_account_id 
		, SUM(tm.quantity) trade_2w_before_unit
		, SUM(tm.amount_usd) trade_2w_before_usd
	FROM analytics.trades_master tm 
		RIGHT JOIN (SELECT DISTINCT ap_account_id FROM eligible_user) eu 
			ON tm.ap_account_id = eu.ap_account_id
	WHERE tm.created_at BETWEEN '2022-02-07' AND '2022-02-21'
		AND tm.symbol IN ('GOLDTHB','GOLDIDR')
	GROUP BY 1
)
SELECT 
	fl.*
	, CASE WHEN DATE_TRUNC('month', fl.register_gmt7) = '2022-02-01' THEN 'new_user'
			ELSE 
			( CASE WHEN fl.inactive_time >= 1 AND fl.inactive_time < 31 THEN 'active_user'
					WHEN fl.inactive_time >= 31 THEN 'inactive_user'
					WHEN fl.inactive_time IS NULL THEN 'dormant'
					END)
			END AS segment_mtu
	, tb.trade_2w_before_unit
	, tb.trade_2w_before_usd
FROM final_list fl
	LEFT JOIN trade_before tb 
		ON fl.ap_account_id = tb.ap_account_id
ORDER BY 8 DESC, 1
;


SELECT 
	(tm.created_at + '7 hour'::INTERVAL)::DATE created_at_gmt7
	, tm.signup_hostcountry 
	, tm.ap_account_id 
	, tm.product_1_symbol 
	, tm.product_2_symbol 
	, SUM(tm.quantity) trade_1w_before_unit
	, SUM(tm.amount_usd) trade_1w_before_usd
FROM analytics.trades_master tm 
	LEFT JOIN analytics.users_master um2 
		ON tm.ap_account_id = um2.ap_account_id 
WHERE 
	tm.created_at + '7 hour'::INTERVAL >= '2022-02-13'
	AND tm.created_at + '7 hour'::INTERVAL < '2022-02-21'
	AND tm.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping um)
	AND tm.symbol IN ('GOLDTHB','GOLDIDR')
	AND tm.signup_hostcountry IN ('TH','ID')
GROUP BY 1,2,3,4,5
;



--Monthly_Incentive_Amount 
WITH 
  "user_master" AS 
 (
	SELECT 
		 date(a.created_at + interval '7h') "register_date"
		,date(a.verification_approved_at + INTERVAL '7H') "verified_at"
		,date(a.zipup_subscribed_at + INTERVAL '7H') "zipup_subscribed_at"
		,a.ap_account_id 
		,c.ap_account_id "parent_account_id"
		,a.user_id
		,a.referring_user_id 
		,b.email 
		,d.email "parent_email"
		,a.is_verified 
		,a.invitation_code 
	FROM 
		analytics.users_master a
	LEFT JOIN 
	    analytics_pii.users_pii b ON a.user_id = b.user_id 
	LEFT JOIN 
	    	analytics.users_master c ON c.user_id = a.referring_user_id
	LEFT JOIN 
	    analytics_pii.users_pii d ON d.user_id = c.user_id 
	WHERE 
		a.signup_hostcountry = 'ID'
)
, "influencer_list" AS 
(
	SELECT 
		a.*
	FROM 
		mappings.commercial_indo_referral_code a 
	WHERE 
		a.referral_group = 'Influencer'
)
, trade_master AS 
(
	SELECT
		a.ap_account_id 
		,b.email 
		,c.email "parent_email"
		,ROUND(SUM(a.amount_usd * 14500),2) AS "trade_amount"
	FROM 
		analytics.trades_master a  
	LEFT JOIN 
		user_master b ON a.ap_account_id = b.ap_account_id 
	LEFT JOIN 
		influencer_list c ON c.user_id = b.referring_user_id 
	WHERE 
		a.signup_hostcountry = 'ID'
	AND 
		c.email IS NOT NULL 
	AND
	    date(a.created_at + INTERVAL '7h') >= date(b.register_date) 
	AND 
	    date(a.created_at + INTERVAL '7h') <= date(b.register_date + INTERVAL '90d')  
	AND 
		c.referral_group = 'Influencer'
-- 	AND 
-- 	    c.email in ('andinata@yahoo.com','andysenjaya7@gmail.com','ghalih081092@gmail.com','hotasimikha@gmail.com','rejive.d@gmail.com')
	GROUP BY 1,2,3
) 
, "transfer_master" AS 
(
	SELECT 
		a.email 
		,TRUE "airdrop_status"
		,t.notes 
	FROM 
    	analytics.transfers_master t
    LEFT JOIN 
    	user_master a ON a.ap_account_id = t.receiver_ap_account_id 
	WHERE 
		left(t.notes,50) = 'AIRDROP_220317_ID_PARTNERSHIP_ACQUI_REFERRALBONUS_'		
)
    SELECT
        a.*
        ,case 
            when a.trade_amount >= 15000000 then 150000
            when a.trade_amount >= 5000000 then 100000
            when a.trade_amount >= 150000 then 50000
            else 0 
         end "bonus_incentive"
        ,b.airdrop_status
        ,b.notes
    FROM
        trade_master a
    LEFT JOIN 
    	transfer_master b ON a.ap_account_id = substring(b.notes,51,99)::int
;


--2204_Acti_ZMTAmazing
WITH 
-- campaign info
campaigns as (
SELECT
	'2205_Acti_ZMTAmazing' as campaign_name
, '2022-05-11'::date as start_date
, '2022-05-31'::date as end_date
, '2022-06-30'::date as after_campaign_date
, CASE
	when date(now() + INTERVAL '7h') > '2022-05-31'
		then 'campaign ended'
	else 'campaign active'
	end as campaign_status
, 1000 as target_participant
, 500  as target_eligible
, 'Nabila' as campaign_pic
)
-- users
, blacklist as (
SELECT
	DISTINCT
	a.email
from mappings.growth_blacklist_and_whitelist_users a
where a.signup_hostcountry = 'ID'
and a.user_type = 'blacklist'
)
, campaign_users as (
SELECT
	coalesce(b.user_id, a.user_id) as user_id
, coalesce(b.referral_group, a.referral_group) as referral_group
from (
SELECT
	a.user_id
, 'campaign' as referral_group
from analytics.users_master a
where a.signup_hostcountry = 'ID'
and a.email ~* '(campaign)'
) a
full outer join (
SELECT
	c.user_id
, b.referral_group
from mappings.growth_referral_code b
left join analytics.users_master c
on b.referral_code = c.referral_code
where b.team = 'ID'
) b
on a.user_id = b.user_id
)
 , users as (
SELECT
  a.user_id
, a.referring_user_id
, a.ap_account_id
, aa.email
, a.signup_hostcountry
, CASE
	when date(a.created_at + INTERVAL '7h') between b.start_date and b.end_date
		then 'new'
	else 'existing'
	end as is_new_user
, CASE
	when c.user_id is not NULL
		then 'campaign_referral'
	when a.referring_user_id is not NULL
		then 'user_referral'
	ELSE 'organic'
	end as user_referral_type
, a.invitation_code
, date(a.created_at + INTERVAL '7h') "register_at"
, date(a.verification_approved_at + INTERVAL '7h') "verified_at"
, date(a.zipup_subscribed_at + INTERVAL '7h') "zipup_subscribed_at"
from analytics.users_master a
join analytics_pii.users_pii aa on a.user_id = aa.user_id
CROSS JOIN campaigns b
left join campaign_users c
on a.referring_user_id = c.user_id
where a.signup_hostcountry = 'ID'
 )
, sus_email as (
SELECT
	a.email
,	regexp_replace(split_part(a.email, '@', 1), '[^a-zA-Z]', '', 'g') as email_grouping
from users a
)
, sus_email_count as (
SELECT
	a.email_grouping
, count(a.email) as total_users
from sus_email a
where a.email_grouping <> ''
group BY
	1
)
, sus_pool as (
SELECT
	a.receiver_user_id
, 'pool' as sus_type
from wallet_app_public.transfer_tickets a
where a."state" = 'fully_processed'
and a.ap_receiver_account_id is not null
group BY
	1
having count(distinct a.ap_account_id) > 10
)
, sus_smurf as (
SELECT
	DISTINCT
	a.user_id
, 'smurf' as sus_type
from wallet_app_public.transfer_tickets a
where a."state" = 'fully_processed'
and EXISTS(
SELECT
1
from sus_pool b
where a.receiver_user_id = b.receiver_user_id
)
)
, sus_trf as (
SELECT
	coalesce(a.receiver_user_id, b.user_id) as user_id
, coalesce(a.sus_type, b.sus_type) as sus_type
from sus_pool a
full outer join sus_smurf b
on a.receiver_user_id = b.user_id
)
, new_ziplock AS 
 (
SELECT 
	DISTINCT 
	z.user_id 
	,min(z.locked_at + INTERVAL '7h') as first_lock_dt_ict
FROM 
	zip_lock_service_public.lock_transactions z
LEFT JOIN 
	analytics.users_master um ON um.user_id = z.user_id 
left join 
	analytics_pii.users_pii u ON u.user_id = z.user_id 
WHERE 
	um.signup_hostcountry = 'ID'
GROUP BY 1
)
, user_summary as (
select 
	a.*
, h.first_lock_dt_ict
, CASE
	when g.ap_account_id is not NULL
		then 'is_pcs'
	else 'not_pcs'
	end as is_pcs
, c.total_users as sus_email_group
, d.sus_type as user_sus_type
, e.sus_type as referring_user_sus_type
, CASE
	when (
	c.total_users is not NULL 
	or d.sus_type is not NULL 
	or e.sus_type is not NULL
	)
		then 'abuser'
	else 'normal'
	end as is_abuser
, case
	when f.email is not NULL
		then 'prev blacklist'
	else 'normal'
	end as is_prev_blacklist
, CASE
	when (
	c.total_users is not NULL 
	or d.sus_type is not NULL 
	or e.sus_type is not NULL
	or f.email is not NULL
	)
		then FALSE
	else TRUE
	end as is_user_eli
from users a
left join sus_email b
on a.email = b.email
left join sus_email_count c
on b.email_grouping = c.email_grouping
and c.total_users > 20
left join sus_trf d
on a.user_id = d.user_id
left join sus_trf e
on a.referring_user_id = e.user_id
left join blacklist f
on a.email = f.email
left join mappings.commercial_pcs_id_account_id g
on a.ap_account_id = g.ap_account_id::int
LEFT JOIN new_ziplock h 
ON a.user_id = h.user_id 
)
, "deposit_master" AS (
	SELECT
		a.ap_account_id 
		,max(date(a.created_at + interval '7h')):: DATE "last_deposit"
		,sum(a.amount_usd * 14500) "cumm_deposit_idr"
    FROM analytics.deposit_tickets_master a
    join campaigns b
    on date(a.created_at + INTERVAL '7h') BETWEEN b.start_date and b.end_date
    WHERE 
   	signup_hostcountry = 'ID'
    GROUP BY 1
    ORDER BY 2 ASC
 )
  , "withdraw_master" AS (
	SELECT
		a.ap_account_id 
		,max(date(a.created_at + interval '7h')):: DATE "last_withdraw_at"
		,sum(a.amount_usd * 14500) "cumm_withdraw_idr"
    FROM analytics.withdraw_tickets_master a
    join campaigns b
    on date(a.created_at + INTERVAL '7h') BETWEEN b.start_date - INTERVAL '1d' and b.end_date
    WHERE 
   	a.signup_hostcountry = 'ID'
    GROUP BY 1
    ORDER BY 2 ASC
 )
, trade_master  AS (
	SELECT 
		a.ap_account_id 
		,min(date(a.created_at + interval '7h')):: DATE "first_trade_at"
		,count(a.order_id) "trade_freq"
		,COALESCE(SUM(CASE WHEN a.side = 'Buy' THEN a.quantity END), 0) AS buy
        ,COALESCE(SUM(CASE WHEN a.side = 'Sell' THEN a.quantity END), 0) AS sell
		,SUM(COALESCE(a.quantity, 0)) AS cumm_trade
		,COALESCE(SUM(CASE WHEN a.side = 'Buy' THEN a.amount_usd * 14500 END), 0) AS buy_idr 
        ,COALESCE(SUM(CASE WHEN a.side = 'Sell' THEN a.amount_usd * 14500 END), 0) AS sell_idr
		,SUM(COALESCE(a.amount_usd * 14500, 0)) AS cumm_trade_idr 
	FROM analytics.trades_master a
	join campaigns b
    on date(a.created_at + INTERVAL '7h') BETWEEN b.start_date and b.end_date
	WHERE a.signup_hostcountry = 'ID'
	and a.product_1_symbol = 'ZMT'
	GROUP BY 
		1
)
, campaign_row_data AS (
    SELECT
         a.user_id
        ,a.referring_user_id
        ,a.ap_account_id
        ,a.register_at
        ,a.verified_at
        ,f.first_trade_at
        ,d.last_withdraw_at
        ,a.email
        ,a.signup_hostcountry
        ,a.invitation_code "raw_inv_code"
        ,a.is_new_user
        ,a.user_referral_type
        ,a.is_pcs
        ,a.is_user_eli
        ,c.cumm_deposit_idr
        ,d.cumm_withdraw_idr
        ,f.buy
        ,f.sell
        ,f.cumm_trade
        ,f.buy_idr
        ,f.sell_idr
        ,f.cumm_trade_idr
        ,CASE WHEN f.buy >= 10 THEN TRUE ELSE FALSE END "is_buy_eli"
    FROM user_summary a 
    LEFT JOIN deposit_master c 
    on a.ap_account_id = c.ap_account_id
    LEFT JOIN withdraw_master d 
    on a.ap_account_id = d.ap_account_id
    LEFT JOIN trade_master f
    on a.ap_account_id = f.ap_account_id
)
, user_list AS (
SELECT 
	a.mobile_number
 , a.user_id
 , a.email 
 , initcap(trim(a.first_name || ' ' || a.last_name)) as user_name
 , u.signup_hostcountry  
 , u.is_zipup_subscribed 
FROM analytics.users_master u
LEFT JOIN analytics_pii.users_pii a 
ON a.user_id = u.user_id 
WHERE u.signup_hostcountry = 'ID'
)
,zipup AS (
SELECT  ul.* 
            ,COALESCE(SUM(amount),0) AS new_amount_zipup
            ,COALESCE(SUM(amount * rm.price),0) AS zipup_idr 
    FROM user_list ul 
        LEFT JOIN asset_manager_public.deposit_transactions dt 
            ON ul.user_id = dt.account_id 
            AND SOURCE = 'alpha_point' 
            AND source_ref = 'accounts+zipmexbalance@zipmex.com'
            AND service_id = 'main_wallet'
            AND upper(SPLIT_PART(product_id,'.',1)) IN ('ZMT')
            --campaign period 31st march to 15th april 
            AND dt.created_at + INTERVAL '7h' >= '2022-05-11 00:00:00'
            AND dt.created_at + INTERVAL '7h' <= '2022-05-31 23:59:59'
        LEFT JOIN analytics.rates_master rm 
            ON upper(SPLIT_PART(product_id,'.',1)) = rm.product_1_symbol
            AND DATE_TRUNC('day', dt.created_at) = DATE_TRUNC('day', rm.created_at)
         where user_id in (select distinct user_id from zip_up_service_tnc.acceptances )
    GROUP BY 1,2,3,4,5,6
)
    SELECT
         a.*
        ,c.new_amount_zipup
        ,c.new_amount_zipup                                                                                                                                                                                                                                      _usd
	    ,CASE when is_user_eli = TRUE and is_buy_eli = TRUE AND c.new_amount_zipup >= 10 THEN TRUE ELSE FALSE end "is_campaign_eli"
	    ,CASE when is_user_eli = TRUE and is_buy_eli = TRUE AND c.new_amount_zipup >= 10 THEN 1 end "cost_zmt"
	    ,case 
	        when is_user_eli = FALSE then 'abuser'
	        when is_buy_eli = TRUE AND c.new_amount_zipup < 10 then 'zipup_not_reach'
	        when is_buy_eli = FALSE then 'not_buy_zmt'
	        when is_user_eli = TRUE and is_buy_eli = TRUE AND c.new_amount_zipup >= 10 THEN 'eligible'
	        else ''
	      end "ineligible_reason" 
	    ,b.*
    FROM campaign_row_data a 
    LEFT JOIN zipup c ON a.user_id = c.user_id
    CROSS join campaigns b
    WHERE a.buy IS NOT NULL 
;


--zipup&ziplock tf gmt7

	
--dm_zw_daily_transactions_gmt7	
SELECT 
	date_trunc('week', a.created_at_gmt7)::DATE "week"
	,COUNT(DISTINCT CASE WHEN a.zw_deposit_count > 0 THEN a.ap_account_id ELSE NULL END) "user_zipup_count"
	,sum(a.transfer_to_zwallet_usd) "total_zipup"
	,COUNT(DISTINCT CASE WHEN a.count_ziplock_transactions > 0 THEN a.ap_account_id ELSE NULL END) "user_lock_count"
	,sum(a.ziplock_usd) "total_ziplock"
FROM 
	reportings_data.dm_zw_daily_transations_gmt7 a
		LEFT JOIN analytics.users_master um 
		ON a.ap_account_id = um.ap_account_id 
WHERE 
	a.created_at_gmt7 >= '2022-06-01'
	AND um.signup_hostcountry = 'ID'
--	AND um.is_zipup_subscribed = TRUE 
GROUP BY 1
;

WITH base AS (
SELECT 
	(dt.created_at + '7 hour'::INTERVAL)::DATE created_at_gmt7
	, dt.account_id 
	, upper(SPLIT_PART(dt.product_id,'.',1)) symbol
	, SUM(dt.amount) transfer_to_zw
FROM 
	asset_manager_public.deposit_transactions dt 
WHERE 
	ref_action = 'deposit'
	AND service_id = 'main_wallet'
	AND dt.created_at + '7 hour'::INTERVAL >= '2022-06-01'
	AND dt.created_at + '7 hour'::INTERVAL < NOW()::DATE 
GROUP BY 1,2,3
)	, base_usd AS (
SELECT 
	b.*
	, um.signup_hostcountry 
	, b.transfer_to_zw * rm.price transfer_to_zw_usd
FROM base b
	LEFT JOIN analytics.users_master um 
		ON b.account_id = um.user_id 
	LEFT JOIN 
		analytics.rates_master rm 
		ON b.symbol = rm.product_1_symbol 
		AND b.created_at_gmt7 = rm.created_at 
WHERE um.is_zipup_subscribed = TRUE
)
SELECT 
	DATE_TRUNC('week', created_at_gmt7)::DATE created_at_gmt7
	, signup_hostcountry
	, COUNT( DISTINCT account_id) user_count
--	, SUM(transfer_to_zw) transfer_to_zw
	, SUM(transfer_to_zw_usd) transfer_to_zw_usd
FROM base_usd
WHERE 
	signup_hostcountry = 'ID'
GROUP BY 1,2