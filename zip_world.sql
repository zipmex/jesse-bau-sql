--- Total ZMT spent on ZipWorld week-by-week (by users' account age)
-->> note: similar as ZX_ZW_Reg_Purchase_wk but without the "users' first login" filter
with date_serie as
 	(select extract('week' from created_at) wk_num
	,created_at::date
	,min (created_at::date) over (partition by extract('week' from created_at) order by created_at ) date_week_start
	,(min (created_at::date) over (partition by extract('week' from created_at) order by created_at ) + interval '6 day')::date date_week_end
	from analytics.period_master p
	where "period" = 'day' 
	and created_at >= '2021-08-01'
 	) 
 ,user_zw as (
 	select z.id
 		,z.zipmex_user_id 
		,analytics.users_master.ap_account_id 
		,analytics.users_master.email
		,extract(week from z.inserted_at)+1 as wk
		--,week(z.inserted_at::date, 3)
		, z.inserted_at as zw_reg_date
		,analytics.users_master.created_at as zm_reg_date
		,date_part('day', analytics.users_master.created_at-z.inserted_at ) gap
		,analytics.users_master.level_increase_status
		,analytics.users_master.signup_hostcountry 
	from zipworld_public.users z
	left join analytics.users_master	
		on analytics.users_master.user_id = z.zipmex_user_id
	--	where {{signup_hostcountry}}
	) --select * from user_zw;
,user_zw_user_group as (
	select *
	,case when gap >=-1 then 'new_users_1'
		when gap >= -3 and gap <-1 then 'new_users_1to3'
		when gap >= -7 and gap <-3 then 'new_users_3to7'
		else 'current_users'
		end user_group
	from user_zw
	)
,purchase as (
	 select 
	 completed_at::date
	 ,user_id
	 ,sum(p.purchase_price) tot_zw_purchase
	 from zipworld_public.purchases p
	 where step = 'completed'
	 group by 1,2
	 )
--,ready as (
	SELECT
    p.*
    , tot_zw_purchase*r.price tot_zw_purchase_usd
    , user_zw_user_group.ap_account_id, user_zw_user_group.signup_hostcountry
	, COALESCE (case when u.age < 30  then 'below30'
			when u.age >= 30 and u.age <=40 then '30-40'
			when u.age >= 41 and u.age <=55 then '41-55'
			when u.age >= 56 then 'over55'
		else null
		end
		,s."age"
		) as age_grp
    , row_number() over (partition by p.user_id order by completed_at asc) rr
	FROM purchase p
	left join user_zw_user_group on user_zw_user_group.id = p.user_id
	LEFT JOIN analytics.users_master u
		ON u.user_id = user_zw_user_group.zipmex_user_id
	LEFT JOIN analytics.rates_master r
	ON p.completed_at = r.created_at::date
	AND r.product_1_symbol = 'ZMT'
	LEFT JOIN (				
		SELECT 
			DISTINCT
			s.user_id 			
			,cast (s.survey ->> 'gender' as text) as gender		
			,cast (s.survey ->> 'age' as text) as "age"		
			,cast (s.survey ->> 'total_estimate_monthly_income' as text) as income
			, s.survey ->> 'occupation' occupation
			, s.survey ->> 'education' education
		FROM
			user_app_public.suitability_surveys s 			
		WHERE
			archived_at IS NULL --taking the latest survey submission			
		)s 
		ON s.user_id  = user_zw_user_group.zipmex_user_id


---- zipworld inventory
WITH base_stock AS (
	SELECT
		p2."name" product_name
		, price 
		, region 
		, pc."name" category
		, DATE_TRUNC('week', p2.inserted_at)::DATE listed_week
		, NOW()::DATE - p2.inserted_at::DATE product_age
		, CASE WHEN drop_end_at::DATE < NOW()::DATE THEN 'expired' ELSE 'live' END AS live_status
		, CASE WHEN (NOW()::DATE - p2.inserted_at::DATE) <= 7 THEN 'A_<_7D'
				WHEN (NOW()::DATE - p2.inserted_at::DATE) BETWEEN 8 AND 30 THEN 'B_7_30D'
				WHEN (NOW()::DATE - p2.inserted_at::DATE) BETWEEN 31 AND 60 THEN 'C_30_60D'
				WHEN (NOW()::DATE - p2.inserted_at::DATE) BETWEEN 61 AND 180 THEN 'D_60_180D'
				WHEN (NOW()::DATE - p2.inserted_at::DATE) > 180 THEN 'E_>_180D'
				END AS age_group
		, available_stock 
		, sold_quantity 
		, available_stock + sold_quantity total_quantity
		, available_stock / (available_stock + sold_quantity)::float avail_percentage
		, COUNT(DISTINCT p2.id) product_count
	FROM 
		zipworld_public.products p2 
		LEFT JOIN zipworld_public.product_categories pc 
			ON p2.category_id = pc.id 
	WHERE 
		lower(short_name) NOT LIKE '%test%'
	GROUP BY 
	    1,2,3,4,5,6,7,8,9,10,11
	ORDER BY 9 DESC, 3 DESC 
)--	, base_stock_rank AS (
SELECT 
	*
	, CASE WHEN avail_percentage < 0.1 THEN 'A_<_10%'
			WHEN avail_percentage BETWEEN 0.1 AND 0.3 THEN 'B_10_30%'
			WHEN avail_percentage BETWEEN 0.3001 AND 0.5 THEN 'C_30_50%'
			WHEN avail_percentage BETWEEN 0.5001 AND 0.7 THEN 'D_50_70%'
			WHEN avail_percentage > 0.7 THEN 'E_70_100%'
			END AS stock_group
FROM base_stock
;


-- Zipworld User base
WITH first_purchase AS (
-- getting first purchased product: why users came to zipworld
	SELECT 
		user_id 
		, purchase_price 
		, price_before_discount 
		, purchase_quantity 
		, product_id 
		, p2."name" product_name
		, pc."name" product_category
		, completed_at::date 
		, p.region 
		, voucher_id 
		, voucher_code 
		, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY completed_at) first_purchased
	FROM 
	-- all purchased order (completed, filled_order, initial)
		zipworld_public.purchases p 
	-- join to get product name
		LEFT JOIN zipworld_public.products p2 
			ON p.product_id = p2.id 
	-- join to get product category
		LEFT JOIN zipworld_public.product_categories pc 
			ON p2.category_id = pc.id 
	WHERE 
	-- first completed orders
		step = 'completed'
)	, spending_report AS (
-- spending behavior from purchase report (completed, incompleted)
	SELECT
		user_id 
		, SUM( CASE WHEN step = 'completed' THEN COALESCE(price_before_discount, purchase_price) END) total_zmt_spent
		, COUNT( CASE WHEN step = 'completed' THEN product_id END) completed_order
		, COUNT( CASE WHEN step = 'filled_user_information' THEN product_id END) filled_user_info_order
		, COUNT( CASE WHEN step = 'initial' THEN product_id END) initial_order
		, SUM(purchase_quantity) purchase_quantity 
	FROM 
		zipworld_public.purchases p 
	GROUP BY 1
)	, login_count AS (
-- total login of each user, table was created after Nov 8, data before that is not available
	SELECT 
		id
		, COUNT(id) total_login_count
		, COUNT(DISTINCT id) unique_login_count
	FROM 
		analytics.login_users_report lur 
	GROUP BY 1
)
SELECT 
	u.id zw_login_id
	, zipmex_user_id 
	, um.ap_account_id
	, 1 user_count
	, um.signup_hostcountry
	, um.gender
	, up.age 
	, CASE WHEN up.age < 26 THEN 'A_<_25' 
			WHEN up.age BETWEEN 26 AND 30 THEN 'B_26_30'
			WHEN up.age BETWEEN 31 AND 35 THEN 'C_31_35'
			WHEN up.age BETWEEN 36 AND 40 THEN 'D_36_40'
			WHEN up.age BETWEEN 41 AND 45 THEN 'E_41_45'
			WHEN up.age BETWEEN 46 AND 50 THEN 'F_46_50'
			WHEN up.age > 50 THEN 'G_>_50'
			ELSE 'N/A'
		END AS age_group
	, u.inserted_at::date first_login_at
	, COALESCE (zts.vip_tier, 'no_tier') first_login_tier
--	, COALESCE (zts.ziplock_amount, 0) first_login_zmtlock_amount
	, COALESCE (zts2.vip_tier, 'no_tier') current_tier
--	, COALESCE (zts2.ziplock_amount, 0) current_zmtlock_amount
	, um.created_at::date register_at
	, (u.inserted_at::date - um.created_at::date) from_reg_to_first_login
	, CASE WHEN (u.inserted_at::date - um.created_at::date) < 8 THEN 'A_<_8D'
			WHEN (u.inserted_at::date - um.created_at::date) BETWEEN 8 AND 14 THEN 'B_8_14D'
			WHEN (u.inserted_at::date - um.created_at::date) BETWEEN 15 AND 30 THEN 'C_15_30D'
			WHEN (u.inserted_at::date - um.created_at::date) BETWEEN 31 AND 60 THEN 'D_30_60D'
			WHEN (u.inserted_at::date - um.created_at::date) > 60 THEN 'D_>_60D'
		END AS first_login_group
	, um.onfido_completed_at::date verified_at
	, p.completed_at::date first_purchased_at
	, product_name first_sold_product
	, product_category first_product_category
	, UPPER(p.region) product_region
	, CASE WHEN p.completed_at IS NOT NULL THEN (p.completed_at::date - um.created_at::date) END AS from_reg_to_purchase
	, CASE WHEN p.completed_at IS NOT NULL THEN 
			(CASE WHEN (p.completed_at::date - um.created_at::date) < 8 THEN 'A_<_8D'
					WHEN (p.completed_at::date - um.created_at::date) BETWEEN 8 AND 14 THEN 'B_8_14D'
					WHEN (p.completed_at::date - um.created_at::date) BETWEEN 15 AND 30 THEN 'C_15_30D'
					WHEN (p.completed_at::date - um.created_at::date) BETWEEN 31 AND 60 THEN 'D_30_60D'
					WHEN (p.completed_at::date - um.created_at::date) > 60 THEN 'E_>_60D'
				END)
		END AS first_purchase_group
	, l.total_login_count
	, l.unique_login_count
	, COALESCE (s.total_zmt_spent, 0) total_zmt_spent
	, CASE WHEN s.total_zmt_spent < 50 THEN 'A_<_50ZMT'
			WHEN s.total_zmt_spent BETWEEN 51 AND 100 THEN 'B_50-100ZMT'
			WHEN s.total_zmt_spent BETWEEN 101 AND 200 THEN 'C_101-200ZMT'
			WHEN s.total_zmt_spent BETWEEN 201 AND 500 THEN 'D_201-500ZMT'
			WHEN s.total_zmt_spent BETWEEN 501 AND 1000 THEN 'E_501-1000ZMT'
			WHEN s.total_zmt_spent > 1000 THEN 'F_>_1000ZMT'
			ELSE 'N/A'
			END AS spending_group
	, s.purchase_quantity total_sold_product
	, s.completed_order	
	, CASE WHEN s.completed_order = 1 THEN 'A_single_purchase'
			WHEN s.completed_order BETWEEN 2 AND 5 THEN 'B_2-5_items'
			WHEN s.completed_order BETWEEN 5 AND 10 THEN 'C_5-10_items'
			WHEN s.completed_order BETWEEN 10 AND 20 THEN 'D_10-20_items'
			WHEN s.completed_order BETWEEN 20 AND 50 THEN 'E_20-50_items'
			WHEN s.completed_order > 50 THEN 'F_>_50_items'
			ELSE '0_purchase'
			END AS repeating_group
	, s.filled_user_info_order
	, s.initial_order
FROM 
-- all users logged in to Zipworld
	zipworld_public.users u 
	-- get account_id, country, gender
	LEFT JOIN analytics.users_master um 
		ON u.zipmex_user_id = um.user_id 
	-- get pii: age, email
	LEFT JOIN analytics_pii.users_pii up 
		ON u.zipmex_user_id = up.user_id 
	-- get user VIP tier on the first_login month
	LEFT JOIN analytics.zmt_tier_1stofmonth zts 
		ON um.ap_account_id = zts.ap_account_id 
		AND DATE_TRUNC('month', u.inserted_at)::date = zts.created_at::date
	-- get user VIP tier on the current month
	LEFT JOIN analytics.zmt_tier_1stofmonth zts2
		ON um.ap_account_id = zts2.ap_account_id 
		AND DATE_TRUNC('month', NOW()::date - '1 day'::INTERVAL)::date = zts2.created_at::date
	-- get total login count of each user
	LEFT JOIN login_count l 
		ON u.id = l.id
	-- get first purchase info: why users came to zipworld
	LEFT JOIN first_purchase p 
		ON u.id = p.user_id 
		AND p.first_purchased = 1
	-- get spending behavior
	LEFT JOIN spending_report s 
		ON u.id = s.user_id 
WHERE 
-- only user in Zipworld
	um.ap_account_id IS NOT NULL 
-- data before today
	AND u.inserted_at < NOW()::DATE
ORDER BY 1
;



SELECT *
FROM zipworld_public.purchases p 
WHERE product_id IN (289,290)


-- weekly redemption
--- Total ZMT spent on ZipWorld week-by-week (by users' account age)
-->> note: similar as ZX_ZW_Reg_Purchase_wk but without the "users' first login" filter
with date_serie as
 	(select extract('week' from created_at) wk_num
	,created_at::date
	,min (created_at::date) over (partition by extract('week' from created_at) order by created_at ) date_week_start
	,(min (created_at::date) over (partition by extract('week' from created_at) order by created_at ) + interval '6 day')::date date_week_end
	from analytics.period_master p
	where "period" = 'day' 
	and created_at >= '2021-08-01'
 	)
 ,user_zw as (
 	select z.id
		,u.ap_account_id 
		,u.email
		,extract(week from z.inserted_at)+1 as wk
		--,week(z.inserted_at::date, 3)
		, z.inserted_at as zw_reg_date
		,u.created_at as zm_reg_date
		,date_part('day', u.created_at-z.inserted_at ) gap
		,u.level_increase_status
		,u.signup_hostcountry 
		,CASE WHEN sd.persona IS NULL THEN 'unknown' ELSE sd.persona END AS persona
	from zipworld_public.users z
	left join analytics.users_master u 
		on u.user_id = z.zipmex_user_id
	LEFT JOIN bo_testing.sample_demo_20211118 sd 
		ON u.ap_account_id = sd.ap_account_id 
	) --select * from user_zw;
,user_zw_user_group as (
	select *
	,case when gap >=-1 then 'new_users_1'
		when gap >= -3 and gap <-1 then 'new_users_1to3'
		when gap >= -7 and gap <-3 then 'new_users_3to7'
		else 'current_users'
		end user_group
	from user_zw
	)-- select * from user_zw_user_group;
,purchase as (
	 select 
	 completed_at::date
	 ,user_id , p2."name" product_name
	 ,sum(p.purchase_quantity) purchase_quantity
	 ,sum(p.purchase_price) tot_zmt_spent
	 from zipworld_public.purchases p
	 	LEFT JOIN zipworld_public.products p2 
	 		ON p.product_id = p2.id 
	 where step = 'completed'
	 group by 1,2,3
	 )
,ready as (
	select 
	d.wk_num, date_week_start
	,p.*, user_zw_user_group.ap_account_id, user_group, signup_hostcountry, persona 
	from purchase p
	left join user_zw_user_group on user_zw_user_group.id = p.user_id
	left join date_serie d on d.created_at = p.completed_at
	)
select r.wk_num, r.date_week_start
, r.signup_hostcountry
, r.ap_account_id
, r.user_group
, r.persona
, count(distinct product_name) product_sold
, count(distinct ap_account_id) buyer_count
,sum(purchase_quantity) purchase_quantity
,sum(tot_zmt_spent) tot_zmt_spent
from ready r
where signup_hostcountry is not null
group by 1,2,3,4,5,6
order by 2 desc, 1 desc,  3
;

 