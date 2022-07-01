-- User_demographic
SELECT 
	CAST(u.created_at as date) reg_date
	, CAST(u.onfido_completed_at as date) ver_date
	, u.ap_account_id
	, u.user_id 
	, u.email
	, u.signup_hostcountry
	, u.level_increase_status 
--	, lower(u.gender) gender_onfido		,s.gender gender_suitability	
	, COALESCE (lower(u.gender),s.gender) as gender --gender onfido is priority selection
	, DATE_TRUNC('day', u.first_traded_at) first_traded_date
	, (DATE_TRUNC('day', NOW()) - DATE_TRUNC('day', u.first_traded_at)) trade_with_zipmex
	-------------			
--	,u.age as age_o	
--		,case when u.age < 30 then 'below30' 
--			when u.age >= 30 and u.age <=40 then '30-40'
--			when u.age >= 41 and u.age <=55 then '41-55'
--			when u.age >= 56 then 'over55'
--		else null
--		end as age_o_grp
--	,s.age as age_s	
	, o.dob
	, EXTRACT( YEAR FROM age(NOW(), o.dob)) age_ 
	, COALESCE (CASE WHEN u.age < 30  THEN 'below30' 	
					WHEN u.age >= 30 AND u.age <= 40 THEN '30-40'	
					WHEN u.age >= 41 AND u.age <= 55 THEN '41-55'	
					WHEN u.age >= 56 THEN 'over55' 
				ELSE NULL
				END
		,s.age) AS age_grp		
	,s.income
	,s.occupation
	,s.education
	,is_zipup_subscribed
	,u.sum_trade_volume_usd 
FROM 
	analytics.users_master u 
LEFT JOIN (				
	SELECT 
		DISTINCT
		s.user_id 			
		,cast (s.survey ->> 'gender' as text) as gender		
		,cast (s.survey ->> 'age' as text) as age		
		,cast (s.survey ->> 'total_estimate_monthly_income' as text) as income
		, s.survey ->> 'occupation' occupation
		, s.survey ->> 'education' education
	FROM
		user_app_public.suitability_surveys s 			
	WHERE
		archived_at IS NULL --taking the latest survey submission			
	)s 
	ON s.user_id  = u.user_id
LEFT JOIN user_app_public.onfido_documents o 
	ON u.user_id = o.user_id 
	AND o.archived_at IS NULL
WHERE
	u.signup_hostcountry IN ('TH','ID','AU','global')				
	AND u.ap_account_id IS NOT NULL
	AND u.level_increase_status = 'pass'
	AND u.is_verified = TRUE 
	AND u.created_at < '2021-10-01 00:00:00'
--	AND u.ap_account_id = 16



SELECT 
	dob
	, EXTRACT( YEAR FROM age(NOW(),dob)) age_ 
from user_app_public.onfido_documents s 			
WHERE archived_at IS NULL
AND user_id = '01EC47XSEC5JTDM0X6D24RAD3Z'
;


--wallet		
select cast(a.created_at as date) balance_date		
		, a.account_id
		,sum(amount) amount
FROM oms_data.public.accounts_positions_daily a		
left join analytics.users_master u on u.ap_account_id  = a.account_id 		
where u.signup_hostcountry in ('TH','ID','AU','global')		
	and a.product_id in (16,50)	
	and a.account_id not in (63312,63313,161347,40706,38260,37955,37807,38263,40683,38262,38121,27308,48870,48948)	
	and a.created_at >= date_trunc('day', now()) - interval '1 day'	
	and a.created_at < date_trunc('day', now())	
group by 1, 2		


--combine 1 and 2
with users_m as (				
	select cast(u.created_at as date) reg_date, u.ap_account_id,u.user_id , u.email, u.signup_hostcountry			
		, lower(u.gender) gender_onfido		,s.gender gender_suitability
		, coalesce (lower(u.gender),s.gender) as gender --gender onfido is priority selection		
		-------------		
		,u.age as age_o		
		,s.age as age_s		
		, case when u.age < 30 then 'A_below30' 	
				when u.age >= 30 and u.age <=40 then 'B_30-40'
				when u.age >= 41 and u.age <=55 then 'C_41-55'
				when u.age >= 56 then 'D_over55' 
			else 'no_age'	
			end	AS age_grp 
		,coalesce (case when u.age < 30 then 'below30' 	
				when u.age >= 30 and u.age <=40 then '30-40'
				when u.age >= 41 and u.age <=55 then '41-55'
				when u.age >= 56 then 'over55' 
			else NULL 
			end	
			, s.age	
			) as age	
		,s.income		
		,is_zipup_subscribed		
	from analytics.users_master u			
	left join (			
		select distinct s.user_id 		
			,cast (s.survey ->> 'gender' as text) as gender	
			,cast (s.survey ->> 'age' as text) as age	
			,cast (s.survey ->> 'total_estimate_monthly_income' as text) as income	
		from user_app_public.suitability_surveys s 		
		where archived_at is not null --taking the latest survey submission		
		)s on s.user_id  = u.user_id		
	where u.signup_hostcountry in ('TH','ID','AU','global')			
)	,wallet_zipup as (				
	select cast(a.created_at as date) balance_date			
		, a.account_id ,u.email ,u.signup_hostcountry		
		,u.gender 		
		,u.age	
		,u.income		
		,u.is_zipup_subscribed		
		,sum(amount) amount		
	FROM oms_data.public.accounts_positions_daily a			
	LEFT join users_m u ON u.ap_account_id = a.account_id			
	where signup_hostcountry is not null			
		and a.product_id in (16,50)		
		and a.account_id not in (63312,63313,161347,40706,38260,37955,37807,38263,40683,38262,38121,27308,48870,48948)		
		and a.created_at >= date_trunc('day', now()) - interval '1 day'		
		and a.created_at < date_trunc('day', now())		
--		and is_zipup_subscribed = TRUE		
	group by 1,2,3,4,5,6,7,8			
)			
select w.age 			
	,count(distinct w.account_id) user_with_balance
--	,count(DISTINCT u.user_id) all_user 
	,sum(amount) as "amount in wallet"			
from wallet_zipup w 
--	LEFT JOIN users_m u ON w.age_grp = u.age_grp
group by 1
	;			


--User master plus Suitability survey
with users_master as (
	select cast(u.created_at as date) reg_date, u.ap_account_id,u.user_id , u.email, u.signup_hostcountry
		, lower(u.gender) gender_onfido		,s.gender gender_suitability
		, coalesce (lower(u.gender),s.gender) as gender --gender onfido is priority selection
		-------------
		,u.age as age_o
			,case when u.age < 30 				 then 'below30' 
				when u.age >= 30 and u.age <=40 then '30-40'
				when u.age >= 41 and u.age <=55 then '41-55'
				when u.age >= 56 				then 'over55'
			else null 
			end as age_o_grp
		,s.age as age_s
		,coalesce (case when u.age < 30 				 then 'below30' 
				when u.age >= 30 and u.age <=40 then '30-40'
				when u.age >= 41 and u.age <=55 then '41-55'
				when u.age >= 56 				then 'over55'
			else null 
			end
			,s.age
			) as age
		,s.income
		,is_zipup_subscribed
	from analytics.users_master u
	left join (
		select distinct s.user_id 
			,cast (s.survey ->> 'gender' as text) as gender
			,cast (s.survey ->> 'age' as text) as age
			,cast (s.survey ->> 'total_estimate_monthly_income' as text) as income
		from user_app_public.suitability_surveys s 
		where archived_at is not null --taking the latest survey submission
		)s on s.user_id  = u.user_id
	where u.signup_hostcountry in ('TH','ID','AU','global')
)
,wallet_zipup as (
	select cast(a.created_at as date) balance_date
		, a.account_id ,u.email ,u.signup_hostcountry
		,amount
		,u.gender 
		,u.age
		,u.income
		,u.is_zipup_subscribed
	FROM oms_data.public.accounts_positions_daily a
	LEFT join users_master u ON u.ap_account_id = a.account_id
	where signup_hostcountry is not null
		and a.product_id in (16,50)
		and a.account_id not in (63312,63313,161347,40706,38260,37955,37807,38263,40683,38262,38121,27308,48870,48948)
		and a.created_at >= date_trunc('day', now()) - interval '1 day'
		and a.created_at < date_trunc('day', now())
--		and is_zipup_subscribed = TRUE
	)
select case when gender in ('male', 'female') then gender else 'no_gender' end as gender
	,count(distinct account_id)
	,sum(amount) as "amount in wallet"
from wallet_zipup
group by 1
	; 
	
WITH gender_temp AS (
SELECT DISTINCT 
	ap_account_id 
	, signup_hostcountry 
--	, CASE WHEN LOWER(u.gender) IN ('male','female') THEN LOWER(u.gender) ELSE 'no_gender' END AS gender 
	, COALESCE (lower(u.gender), s.gender) gender 
FROM analytics.users_master u
	LEFT JOIN (
		SELECT DISTINCT s.user_id 
			,cast (s.survey ->> 'gender' AS text) AS gender
			,cast (s.survey ->> 'age' AS text) AS age
			,cast (s.survey ->> 'total_estimate_monthly_income' as text) as income
		FROM user_app_public.suitability_surveys s 
		WHERE archived_at IS NOT NULL --taking the latest survey submission
		)s ON s.user_id  = u.user_id
		WHERE u.signup_hostcountry in ('TH','ID','AU','global')
), wallet_zipup AS (
	SELECT cast(a.created_at AS date) balance_date
		, a.account_id 
		, u.gender 
		, SUM(amount) zmt_balance
	FROM oms_data.public.accounts_positions_daily a
	LEFT join gender_temp u ON u.ap_account_id = a.account_id
	where signup_hostcountry is not null
		and a.product_id in (16,50)
		and a.account_id not in (63312,63313,161347,40706,38260,37955,37807,38263,40683,38262,38121,27308,48870,48948)
		and a.created_at >= date_trunc('day', now()) - interval '1 day'
		and a.created_at < date_trunc('day', now())
--		and is_zipup_subscribed = TRUE
	GROUP BY 1,2,3
)
SELECT 
	CASE WHEN gender IN ('male','female') THEN gender ELSE 'no_gender' END AS gender 
	, COUNT(DISTINCT account_id)
	, SUM(zmt_balance)
FROM 
	wallet_zipup
GROUP BY 1 


--User master plus Suitability survey
with users_m as (
	select 
		cast(u.created_at as date) reg_date, u.ap_account_id,u.user_id , u.email, u.signup_hostcountry
		, lower(u.gender) gender_onfido		,s.gender gender_suitability
		, coalesce (lower(u.gender),s.gender) as gender --gender onfido is priority selection
		-------------
		,u.age as age_o
			,case when u.age < 30 				 then 'below30' 
				when u.age >= 30 and u.age <=40 then '30-40'
				when u.age >= 41 and u.age <=55 then '41-55'
				when u.age >= 56 				then 'over55'
			else null 
			end as age_o_grp
		,s.age as age_s
		,coalesce (case when u.age < 30 				 then 'below30' 
				when u.age >= 30 and u.age <=40 then '30-40'
				when u.age >= 41 and u.age <=55 then '41-55'
				when u.age >= 56 				then 'over55'
			else null 
			end
			,s.age
			) as age
		,s.income
		,is_zipup_subscribed
	from analytics.users_master u
	left join (
		select distinct s.user_id 
			,cast (s.survey ->> 'gender' as text) as gender
			,cast (s.survey ->> 'age' as text) as age
			,cast (s.survey ->> 'total_estimate_monthly_income' as text) as income
		from user_app_public.suitability_surveys s 
		where archived_at is not null --taking the latest survey submission
		)s on s.user_id  = u.user_id
	where u.signup_hostcountry in ('TH','ID','AU','global')
)
,trade as (
	select t.ap_account_id
	, u.gender
	, sum(amount_usd) amount_usd
	from analytics.trades_master t
	left join users_m u on u.ap_account_id = t.ap_account_id 
	where t.signup_hostcountry not in ('test', 'error', 'xbullion')
	and t.product_1_symbol = 'ZMT'
	and t.ap_account_id not in ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225',27443
		,'25226','25227','38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','49659'
		,'49658','52018','52019','44057','161347'
		)
	and t.created_at < date_trunc('day', now())
	group by 1, 2
	)
select case when gender in ('male', 'female') then gender else 'no_gender' end as gender
	,count(distinct ap_account_id)
	,sum(amount_usd) as trade_vol_usd
from trade
group by 1
;



with users_master as (
select cast(u.created_at as date) reg_date, u.ap_account_id,u.user_id , u.email, u.signup_hostcountry
, lower(u.gender) gender_onfido,s.gender gender_suitability
, coalesce (lower(u.gender),s.gender) as gender --gender onfido is priority selection
-------------
,u.age as age_o
,case when u.age >0 and u.age < 30  then 'below30' 
when u.age >= 30 and u.age <=40 then '30-40'
when u.age >= 41 and u.age <=55 then '41-55'
when u.age >= 56 then 'over55'
else null 
end as age_o_grp
,s.age as age_s
,coalesce (case when u.age >0 and u.age < 30  then 'below30' 
when u.age >= 30 and u.age <=40 then '30-40'
when u.age >= 41 and u.age <=55 then '41-55'
when u.age >= 56 then 'over55'
else null 
end
,s.age
) as age
,s.income
,is_zipup_subscribed
from analytics.users_master u
left join (
select distinct s.user_id 
,cast (s.survey ->> 'gender' as text) as gender
,cast (s.survey ->> 'age' as text) as age
,cast (s.survey ->> 'total_estimate_monthly_income' as text) as income
from user_app_public.suitability_surveys s 
where archived_at is not null --taking the latest survey submission
)s on s.user_id  = u.user_id
where u.signup_hostcountry in ('TH','ID','AU','global')
)
,wallet_zipup as (
select cast(a.created_at as date) balance_date
, a.account_id ,u.email ,u.signup_hostcountry
,amount
,u.gender 
,u.age
,u.income
,u.is_zipup_subscribed
FROM oms_data.public.accounts_positions_daily a
LEFT join users_master u ON u.ap_account_id = a.account_id
where signup_hostcountry is not null
and a.product_id in (16,50)
and a.account_id not in (63312,63313,161347,40706,38260,37955,37807,38263,40683,38262,38121,27308,48870,48948)
and a.created_at >= date_trunc('day', now()) - interval '1 day'
and a.created_at < date_trunc('day', now())
--and is_zipup_subscribed = TRUE
)
,wallet_zipup_agg as (
select 
account_id 
,case when gender in ('male', 'female') then gender else 'no_gender' end as gender
,count(distinct account_id)
,sum(amount) amount
from wallet_zipup
group by 1,2
)
,wallet_zipup_agg_wallet_grp as (
select *,
case when amount >= 0 and amount <= 100 then 'A_0_100'
when amount > 100 and amount <= 500 then 'B_101_500'
when amount > 500 and amount <= 1000 then 'C_501_1000'
when amount > 1000 and amount <= 5000 then 'D_1001_5000'
when amount > 5000 and amount <= 10000 then 'E_5001_10000'
when amount > 10000 and amount <= 20000 then 'F_10001_20000'
when amount > 20000 then 'G_above_20k'
else 'G_null'
end wallet_group
from wallet_zipup_agg
)
select wallet_group, count(account_id), sum(amount) as "amt in wallet"
from wallet_zipup_agg_wallet_grp
group by 1
ORDER BY 1
;


WITH base AS (
SELECT u.signup_hostcountry 
	, a.account_id 
	, SUM(a.amount) coin_amount
FROM public.accounts_positions_daily a 
	LEFT JOIN analytics.users_master u
	ON a.account_id = u.ap_account_id 
WHERE u.signup_hostcountry NOT IN ('test','error','xbullion')
and a.account_id not in (63312,63313,161347,40706,38260,37955,37807,38263,40683,38262,38121,27308,48870,48948)
and a.created_at >= date_trunc('day', now()) - interval '1 day'
and a.created_at < date_trunc('day', now())
AND a.product_id IN (16,50)
GROUP BY 1,2
)
SELECT 
		case when coin_amount >= 0 and coin_amount <= 100 then 'A_0_100'
		when coin_amount > 100 and coin_amount <= 500 then 'B_101_500'
		when coin_amount > 500 and coin_amount <= 1000 then 'C_501_1000'
		when coin_amount > 1000 and coin_amount <= 5000 then 'D_1001_5000'
		when coin_amount > 5000 and coin_amount <= 10000 then 'E_5001_10000'
		when coin_amount > 10000 and coin_amount <= 20000 then 'F_10001_20000'
		when coin_amount > 20000 then 'G_above_20k'
		END AS wallet_group
	, COUNT(DISTINCT account_id) user_count 
	, SUM(coin_amount) wallet_balance
FROM base 
GROUP BY 1