WITH date_serie AS 
    (
    SELECT DISTINCT 
            p.created_at::date
            , u.user_id  
    FROM  analytics.period_master p 
        CROSS JOIN (SELECT DISTINCT account_id as user_id 
        FROM warehouse.asset_manager_public.ledgers) u
    WHERE 
        p."period" = 'day'
        --date selection here needs to be wider so that we capture all lock activity; then can fill the balance amt when users are not active locking
        AND p.created_at >= '2021-10-06 00:00:00' 
        AND p.created_at < '2021-10-13 00:00:00'
	--  AND u.user_id = '01F67663GD1K5PT8HE2GGMD3RM' ------ TEST ACCOUNT HERE
    ) --select * from date_serie;
 ,base_lock as (
    select 
        l.updated_at + interval '7 hour' updated_at_bkk_time
        ,l.account_id as user_id
        ,u.signup_hostcountry
        ,u.ap_account_id
        ,u.email
        ,coalesce (SUM( CASE WHEN l.service_id = 'zip_lock' THEN credit - debit END),0) ziplock_amnt
    FROM warehouse.asset_manager_public.ledgers l
        LEFT JOIN warehouse.analytics.users_master u 
                ON l.account_id = u.user_id 
    WHERE u.signup_hostcountry in ('TH', 'ID','AU','global')
        and l.account_id IS NOT NULL 
        and UPPER(SPLIT_PART(l.product_id,'.',1)) = 'ZMT'
        and l.updated_at + interval '7 hour' < '2021-10-13 00:00:00'
--      and l.account_id = '01EXBXGTG0F6W8VHEN84YTK92H'
    GROUP BY 1,2,3,4,5
    )	, tempt AS ( --select * from base_lock;
	select 
	    d.created_at::date bkk_date
	    ,d.user_id
	    ,l.ap_account_id 
	    ,l.email
	    ,l.signup_hostcountry
	    ,sum(ziplock_amnt) ziplock_amnt
	from date_serie d
	left join base_lock l on l.user_id = d.user_id
	    and l.updated_at_bkk_time::date <= d.created_at::date
	where d.created_at < '2021-10-13 00:00:00'
	group by 1,2,3,4,5
)
SELECT 
	ap_account_id
	, SUM(ziplock_amnt) / COUNT(ap_account_id) avg_zmt_lock
FROM tempt
GROUP BY 1

;
