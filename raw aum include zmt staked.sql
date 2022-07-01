------------- generate date - interval 1 day with user id from zip crew stake 
with "date_series" as 
(	select distinct date as date_
		,u.user_id
	FROM GENERATE_SERIES('2020-01-01'::DATE, NOW()::DATE, '1 day') "date"
	CROSS join (SELECT DISTINCT user_id FROM oms_data.user_app_public.zip_crew_stakes) u
	ORDER by 1 ASC
)
-- join with zip crew stake to calculate usd_amount
, zmt_staked as 
(	
	SELECT
	d.date_
	,u.account_id ,u.zip_user_id ,u.email, u.signup_hostcountry 
	,SUM(s.amount) "zmt_staked_amount"
	,SUM(s.amount* c.price) "zmt_staked_usd_amount"
from date_series d
LEFT join oms_data.user_app_public.zip_crew_stakes s
	ON d.user_id = s.user_id AND d.date_ >= DATE_TRUNC('day', s.staked_at) AND d.date_ < COALESCE(DATE_TRUNC('day', s.released_at), NOW())
LEFT join oms_data.analytics.users_master u ON s.user_id = u.zip_user_id
LEFT join oms_data.mysql_replica_apex.products p ON s.product_id = p.product_id
-- join crypto usd prices
LEFT join oms_data.public.prices_eod_gmt0 c
	ON p.symbol = c.product_1_symbol
	AND c.product_2_symbol = 'USD'AND d.date_ = DATE_TRUNC('day', c.actual_timestamp) AND p."type" = 2
where u.account_id IS NOT null 
GROUP by 1, 2, 3,4,5
)
-- join AUM daily
,  aum_eod as 
(	
select --a.balanced_at::date as datadate
	DATE_TRUNC('day', a.balanced_at) datadate --+ INTERVAL '1 MONTH - 1 day' "month"
	,a.account_id ,u.signup_hostcountry ,u.zip_user_id , a.product_id , a.total_balance 
	,COALESCE(SUM(ROUND(CASE 	WHEN p.product_id = 6 THEN a.total_balance * 1
					WHEN p.type = 2 THEN a.total_balance * c.price
					WHEN p.type = 1 THEN a.total_balance / e.exchange_rate
					ELSE 0 END, 4)), 0) "usd_amount"
from oms_data.data_imports.account_balance_eod_gmt0 a
LEFT join oms_data.mysql_replica_apex.products p ON a.product_id = p.product_id
LEFT join oms_data.analytics.users_master u ON a.account_id = u.account_id
-- join crypto usd prices
LEFT join oms_data.public.prices_eod_gmt0 c
	ON p.symbol = c.product_1_symbol AND c.product_2_symbol = 'USD'
	-- if you want rate from specific date replace DATE_TRUNC('day', a.balanced_at)
	AND DATE_TRUNC('day', a.balanced_at) = DATE_TRUNC('day', c.actual_timestamp)
	AND p."type" = 2
LEFT join oms_data.public.exchange_rates e
	ON p.symbol = e.product_2_symbol AND e.product_1_symbol = 'USD'
	-- if you want rate from specific date replace DATE_TRUNC('day', a.balanced_at)
	AND DATE_TRUNC('day', a.balanced_at) = DATE_TRUNC('day', e.created_at)
	AND e."source" = 'coinmarketcap'
	AND p."type" = 1
where 
	--DATE_TRUNC('day', a.balanced_at) = DATE_TRUNC('month', a.balanced_at) + INTERVAL '1 MONTH - 1 day' --use this if calculate AUM eom
	 u.signup_hostcountry IN ('AU') --('TH','AU','ID','global')
	--and date_trunc('day',a.balanced_at) = '2021-05-24 00:00:00'
	AND a.account_id NOT IN (0, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63312, 63313, 161347)
	/* list of accounts to exclude
	account_id
	0		remarketer account
	37807	raphael.ghislain999@gmail.com
	37955	pipshunter330@gmail.com
	38121	tangmo82@gmail.com
	38260	makemarket.id@gmail.com
	38262	whenyousorich@gmail.com
	38263	andreas.rellstab135@gmail.com
	40683	jack.napier7888@gmail.com
	40706	arthur.crypto789@gmail.com
	63312	zipmexasia+zmt@zipmex.com
	63313	accounts+zmt@zipmex.com
	161347	zmt.trader@zipmex.com
	27308	accounts+zipmktth@zipmex.com
	48870	accounts+zipup@zipmex.com
	48948	zipmexasia@zipmex.com
	*/
GROUP by 1,2,3,4,5,6
)
, aum_final as  (
select z.date_
	, z.account_id , z.zip_user_id , a.email 
	, z.signup_hostcountry 
	, EXTRACT(year from z.date_) as year_ 
	, EXTRACT(month from z.date_) as month_ 
	, z.zmt_staked_amount 
	, z.zmt_staked_usd_amount
	, a.usd_amount aum_without_zmt_staked 
--	, (z.zmt_staked_usd_amount + a.usd_amount) as aum_amount
from zmt_staked z 
	left join aum_eod a on z.account_id = a.account_id and z.date_ = a.datadate 
where signup
--a.email = 'dawei_oit@hotmail.com'
--and z.date_ = '2021-05-20'
--group by 1,2,3,4,5,6,7
)
select signup_hostcountry
	, date_trunc('week',date_) + '6 days'::interval end_of_week  
	, SUM(zmt_staked_amount) zmt_staked_amount
	, SUM(zmt_staked_usd_amount) zmt_staked_usd_amount
	, SUM(aum_without_zmt_staked) aum_without_staked
from aum_final 
--where date_ = '2021-05-20'
group by 1,2;
	

------------- for AUM end of month 
SELECT
	DATE_TRUNC('month', a.balanced_at) + INTERVAL '1 MONTH - 1 day' "month"
	, a.product_id --,a.account_id
--	,u.signup_hostcountry 
	,COALESCE(SUM(ROUND(CASE 	WHEN p.product_id = 6 THEN a.total_balance * 1
					WHEN p.type = 2 THEN a.total_balance * c.price
					WHEN p.type = 1 THEN a.total_balance / e.exchange_rate
					ELSE 0
	END, 2)), 0) "usd_amount"
FROM
	oms_data.data_imports.account_balance_eod_gmt0 a
LEFT JOIN
	oms_data.mysql_replica_apex.products p
	ON a.product_id = p.product_id
LEFT JOIN
	oms_data.analytics.users_master	u
	ON a.account_id = u.account_id
-- join crypto usd prices
LEFT JOIN
	oms_data.public.prices_eod_gmt0 c
	ON p.symbol = c.product_1_symbol
	AND c.product_2_symbol = 'USD'
	-- if you want rate from specific date replace DATE_TRUNC('day', a.balanced_at)
	AND DATE_TRUNC('day', a.balanced_at) = DATE_TRUNC('day', c.actual_timestamp)
	AND p."type" = 2
LEFT JOIN
	oms_data.public.exchange_rates e
	ON p.symbol = e.product_2_symbol
	AND e.product_1_symbol = 'USD'
	-- if you want rate from specific date replace DATE_TRUNC('day', a.balanced_at)
	AND DATE_TRUNC('day', a.balanced_at) = DATE_TRUNC('day', e.created_at)
	AND e."source" = 'coinmarketcap'
	AND p."type" = 1
WHERE
	DATE_TRUNC('day', a.balanced_at) = DATE_TRUNC('month', a.balanced_at) + INTERVAL '1 MONTH - 1 day'
	AND u.signup_hostcountry IN ('TH','AU','ID','global')
	AND p.product_id IN (1, 2, 3, 14, 15, 25, 26, 27, 30, 33, 34, 35)
	--and a.account_id = '48300'
	AND a.account_id NOT IN (0 , 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63312, 63313, 161347)
	/* list of accounts to exclude
	account_id
	0		remarketer account
	37807	raphael.ghislain999@gmail.com
	37955	pipshunter330@gmail.com
	38121	tangmo82@gmail.com
	38260	makemarket.id@gmail.com
	38262	whenyousorich@gmail.com
	38263	andreas.rellstab135@gmail.com
	40683	jack.napier7888@gmail.com
	40706	arthur.crypto789@gmail.com
	63312	zipmexasia+zmt@zipmex.com
	63313	accounts+zmt@zipmex.com
	161347	zmt.trader@zipmex.com
	*/
GROUP BY
	1, 2
ORDER by 1
;

select date_trunc('month', balanced_at) month_
	, product_id 
	, sum(product_amount)
from oms_data.data_imports.account_balance_eod_gmt0 a
where product_id IN (1, 2, 3, 14, 15, 25, 26, 27, 30, 33, 34, 35)
group by 1,2
order by 1  

select created_at 
	, account_id 
	, sum(amount)
from oms_data.public.accounts_positions_daily a 
where account_id = 143639
group by 1,2
order by 1 desc 

------------- holding ZMT only as balance
with "date_series" as 
(	select distinct date as date_
		,u.user_id
	FROM GENERATE_SERIES('2020-01-01'::DATE, NOW()::DATE, '1 day') "date"
	CROSS join (SELECT DISTINCT user_id FROM oms_data.user_app_public.zip_crew_stakes) u
	ORDER by 1 ASC
)
-- join with zip crew stake to calculate zmt_staked_usd_amount
, zmt_staked as (	
SELECT
	d.date_
	, u.account_id --,u.zip_user_id ,u.email
	, u.signup_hostcountry 
	,SUM(s.amount) "zmt_staked_amount"
from date_series d
	LEFT join oms_data.user_app_public.zip_crew_stakes s
	ON d.user_id = s.user_id AND d.date_ >= DATE_TRUNC('day', s.staked_at) AND d.date_ < COALESCE(DATE_TRUNC('day', s.released_at), NOW())
	LEFT join oms_data.analytics.users_master u ON s.user_id = u.zip_user_id and u.is_verified = true
where u.account_id IS NOT null 
and u.signup_hostcountry IN ('TH','AU','ID','global')
AND u.account_id NOT IN (0, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63312, 63313, 161347)
GROUP by 1,2,3
order by 1
)
-- join AUM daily
, aum_eod as 
(	
select --a.balanced_at::date as datadate
	DATE_TRUNC('day',a.balanced_at) as datadate --+ INTERVAL '1 MONTH - 1 day' "month"
	,a.account_id --,u.email ,u.zip_user_id , a.product_id 
	, SUM(a.total_balance) zmt_balance 
from oms_data.data_imports.account_balance_eod_gmt0 a
LEFT join oms_data.analytics.users_master u ON a.account_id = u.account_id
where u.signup_hostcountry IN ('TH','AU','ID','global')
	AND a.account_id NOT IN (0 , 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63312, 63313, 161347)
	and a.product_id in ('16','50')
GROUP by 1,2--,3,4,5
ORDER by 1 
)--, aum_final as  (
select coalesce (z.date_, a.datadate) date_ 
	, coalesce (z.account_id,a.account_id) account_id 
	, z.signup_hostcountry 
	, z.zmt_staked_amount 
	, a.zmt_balance 
from zmt_staked z 
	left join aum_eod a on z.account_id = a.account_id and z.date_ = a.datadate 
--where z.zmt_staked_amount < 1 --z.account_id = 38675
and z.signup_hostcountry = 'AU'
)
select signup_hostcountry
	, DATE_TRUNC('day', date_) date_ 
	, COUNT(distinct account_id) as zmt_holder
	, SUM(zmt_staked_amount) zmt_staked_amount
	, SUM(zmt_balance) zmt_balance 
from aum_final 
group by 1,2;

