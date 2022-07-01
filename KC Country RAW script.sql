--this is for KIN CHEN ==reg to kyc to zipsubscribe
with base as (
	select 
	u.signup_hostcountry ,u.user_id ,u.zip_user_id,u.account_id 
	, u.created_at as register_date
	,u.is_verified
	,u.is_zipup_subscribed
	FROM analytics.users_master u
	where signup_hostcountry not in ( 'test','error','xbullion') and created_at < '2021-05-07 00:00:00' --<<<<<<<<CHANGE DATE HERE
	) --select * from base where is_zipup_subscribed = true and onfido_completed_at is null;
,base_month as (
	select signup_hostcountry, cast(date_trunc('month', register_date) as date) as register_month
	, count(distinct user_id) as user_id_c
	,count(distinct case when  register_date is not null and is_verified =TRUE
			then user_id end) as user_id_kyc ---> this one only count the status. meaning everytime we report, number will change and cannot capture true monthly performance
	,count(distinct case when  register_date is not null 
					and is_zipup_subscribed = true then user_id end) as user_id_z_up_sub
	from base
	group by 1, 2
	)
select *, sum(user_id_c) over (partition by signup_hostcountry order by register_month ) as user_id_c_cum
,sum(user_id_kyc) over (partition by signup_hostcountry order by register_month) as user_id_kyc_cum
,sum(user_id_z_up_sub) over (partition by signup_hostcountry order by register_month ) as user_id_z_up_sub_cum
from base_month
--where signup_hostcountry= 'AU'
;

---COMPELTE SCRIPT for all asset holding and ZMT staked. need to run twice for #of ZMT balance and staked --to QA use this account_id = 38675
with daily_user_balance as (
	select created_at, account_id , sum(usd_amount) as usd_amount, avg(price) as zmt_usd
	from (
		select date_trunc('day',a.created_at)as created_at ,a.account_id , a.product_id, p.symbol
			, amount , c.average_high_low , g.mid_price , z.price, 1/e.exchange_rate as exchange_rate
			,SUM(a.amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate)) usd_amount
		from oms_data.public.accounts_positions_daily a
			left join oms_data.mysql_replica_apex.products p
				ON a.product_id = p.product_id
			LEFT JOIN oms_data.public.cryptocurrency_prices c
			    ON CONCAT(p.symbol, 'USD') = c.instrument_symbol
			    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', c.created_at)
			LEFT join oms_data.public.daily_closing_gold_prices g
				ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', g.created_at)
				AND a.product_id IN (15, 35)
			LEFT join oms_data.public.daily_ap_prices z
				ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', z.created_at)
				and z.instrument_symbol  = 'ZMTUSD'
				and a.product_id in (16, 50)
			left join public.exchange_rates e
				on date_trunc('day', e.created_at) = date_trunc('day', a.created_at)
				and e.product_2_symbol  = p.symbol
				and e.source = 'coinmarketcap'
		where a.created_at >='2019-01-01' and a.created_at <'2021-05-07 00:00:00' --<<<<<<<<CHANGE DATE HERE
		and a.account_id not in (63312,63313,161347,40706,38260,37955,37807,38263,40683,38262,38121,27308,48870,48948)
	--add this filter for 2nd run on script for ZMT amount
and a.product_id in (16, 50) --<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
		--===================
		group by 1,2,3,4,5,6,7,8,9
		) a
	group by 1,2
	)
,staked_cum as ( --ZMT staked cumulative
		with staked_monthly as (
			select u.account_id 
				,date_trunc ('month', staked_at) as staked_at
				,sum(amount) amount
			FROM oms_data.user_app_public.zip_crew_stakes s
		 		left join analytics.users_master u on u.zip_user_id = s.user_id
		 	where  signup_hostcountry not in ('test', 'error','xbullion')
			and staked_at >= '2020-12-17 00:00:00' and staked_at < '2021-05-07 00:00:00' --<<<<<<<<CHANGE DATE HERE
			group by 1,2
		)
		,end_of_months_dates AS (
		SELECT (d + '1 month'::interval - '1 day'::interval )::date end_date
		FROM generate_series('2020-12-01'::date, '2021-05-07'::date, '1 month'::interval) d --<<<<<<<<CHANGE DATE HERE
		)
		,staked_cum AS ( --magic is in this section DO NOT ALTER TILL UNDERSTAND
	    select t.account_id,
	      t.sum_amount as amount_staked_cum,
		  date_trunc('month',t.end_date) as staked_at
	    FROM (
	      SELECT
	        u.account_id,
	        SUM(amount) AS sum_amount,
			eomd.end_date AS end_date
	      FROM user_app_public.zip_crew_stakes s
	      	left join analytics.users_master u on u.zip_user_id = s.user_id 
		  JOIN end_of_months_dates eomd ON 1=1
	      where signup_hostcountry not in ('test', 'error','xbullion')
	      	and staked_at >= '2020-12-17 00:00:00'
		  -- filter only stakes before specified timestamp
	        and staked_at < eomd.end_date::timestamp
	      -- and haven't been released yet
		--AND (released_at IS NULL OR released_at > eomd.end_date::timestamp)
	      GROUP BY
	        account_id,
			eomd.end_date
	    ) AS t
	)
	SELECT b.staked_at, b.account_id
		, a.amount
		, b.amount_staked_cum
		,zmt_usd
	FROM staked_cum b 
		left join staked_monthly a on b.account_id = a.account_id and b.staked_at = a.staked_at
		right join ( --mapping ZMT amount with average USD
			select date_trunc('month', created_at) as created_at, avg(price) as zmt_usd
			from oms_data.public.daily_ap_prices z
			where z.instrument_symbol  = 'ZMTUSD'
			group by 1
			) z ON cast(b.staked_at as date) = cast(z.created_at as date)
	   	left join analytics.users_master u on u.account_id = b.account_id
		--where b.account_id in (669,11,30) --test account for cumulative metric.
	)
,monthly_user_balance as (
	select date_trunc ('month' , created_at) created_at
	,account_id
	,count(account_id) account_id_c
	, sum(usd_amount) usd_amount
	,avg(zmt_usd) as zmt_usd
	from daily_user_balance
	group by 1,2
	)	
,asset_holding as (--calculating balance by end of month or MTD
	select a.*, sum(l1y.account_id_c) account_id_c_l1y, sum(l1y.usd_amount) usd_amount_l1y
	from monthly_user_balance a
	left join monthly_user_balance l1y on l1y.account_id = a.account_id
		and l1y.created_at <a.created_at
		and l1y.created_at >= a.created_at - interval '1 year'
	group by 1, 2, 3, 4,5
	)
--run this for holding any asset including ZMT	
select 
coalesce (a.created_at, b.staked_at) as end_of_month_report
,coalesce(a.account_id, b.account_id) as account_id
,u.signup_hostcountry 
,account_id_c, usd_amount
,account_id_c_l1y, usd_amount_l1y
,amount_staked_cum
,coalesce (a.zmt_usd, b.zmt_usd) as zmt_usd
,amount as zmt_monthly_staked
from asset_holding a
	full outer join --this join zmt staked to user balance
		(select distinct * 
		from staked_cum) 
			b on b.staked_at = a.created_at
			and b.account_id = a.account_id
	left join analytics.users_master u on u.account_id  = a.account_id
--where a.account_id = 92584
--limit 500
;



-- ZIP UP non Zero balance. To test use account_id 11 and 13 one of them is not subscribe
with daily_user_balance as (
	select created_at, account_id , sum(usd_amount) as usd_amount, avg(price) as zmt_usd
	from (
		select date_trunc('day',a.created_at)as created_at ,a.account_id , a.product_id, p.symbol
			, amount , c.average_high_low , g.mid_price , z.price, 1/e.exchange_rate as exchange_rate
			,SUM(a.amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate)) usd_amount
		from oms_data.public.accounts_positions_daily a
			left join oms_data.mysql_replica_apex.products p
				ON a.product_id = p.product_id
			LEFT JOIN oms_data.public.cryptocurrency_prices c
			    ON CONCAT(p.symbol, 'USD') = c.instrument_symbol
			    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', c.created_at)
			LEFT join oms_data.public.daily_closing_gold_prices g
				ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', g.created_at)
				AND a.product_id IN (15, 35)
			LEFT join oms_data.public.daily_ap_prices z
				ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', z.created_at)
				and z.instrument_symbol  = 'ZMTUSD'
				and a.product_id in (16, 50)
			left join public.exchange_rates e
				on date_trunc('day', e.created_at) = date_trunc('day', a.created_at)
				and e.product_2_symbol  = p.symbol
				and e.source = 'coinmarketcap'
		where a.created_at >='2019-01-01 00:00:00' and a.created_at <'2021-06-07 00:00:00' --<<<<<<<<CHANGE DATE HERE
		and a.account_id not in (63312,63313,161347,40706,38260,37955,37807,38263,40683,38262,38121,27308,48870,48948)
		--Total Users with ZipUp assets -----<<<<<<<<<<<<<<<<<<<<<<<<<<<<
		AND a.product_id IN (1, 2, 3, 14, 15, 25, 26, 27, 30, 33, 34, 35) --<<<<<<<<<<<<=========
		--===================
		group by 1,2,3,4,5,6,7,8,9
		) a
	group by 1,2
	)
, staked_monthly as ( select u.account_id 
			,date_trunc ('month', staked_at) as staked_at
			,sum(amount) amount
			,avg(z.price) as zmt_usd
			FROM oms_data.user_app_public.zip_crew_stakes s
		 		left join analytics.users_master u on u.zip_user_id = s.user_id
		 		LEFT join oms_data.public.daily_ap_prices z
		 			ON DATE_TRUNC('day', s.staked_at) = DATE_TRUNC('day', z.created_at)
					and z.instrument_symbol  = 'ZMTUSD'
			where  signup_hostcountry not in ('test', 'error','xbullion')
			and staked_at >= '2020-12-17 00:00:00' and staked_at < '2021-06-07 00:00:00'--<<<<<<<<CHANGE DATE HERE
			group by 1,2
			)
,staked_cum as (--this section has last 1 year metric
		with staked_monthly as (
			select u.account_id 
				,date_trunc ('month', staked_at) as staked_at
				,sum(amount) amount
			FROM oms_data.user_app_public.zip_crew_stakes s
		 		left join analytics.users_master u on u.zip_user_id = s.user_id
		 	where  signup_hostcountry not in ('test', 'error','xbullion')
			and staked_at >= '2020-12-17 00:00:00' and staked_at < '2021-06-07 00:00:00' --<<<<<<<<CHANGE DATE HERE
			group by 1,2
		)
	,end_of_months_dates AS (
		SELECT (d + '1 month'::interval - '1 day'::interval )::date end_date
		FROM generate_series('2020-12-01'::date, '2021-05-07'::date, '1 month'::interval) d --<<<<<<<<CHANGE DATE HERE
		)
	,staked_cum AS ( --magic is in this section DO NOT ALTER TILL UNDERSTAND
	    select t.account_id,
	      t.sum_amount as amount_staked_cum,
		  date_trunc('month',t.end_date) as staked_at
	    FROM (
	      SELECT
	        u.account_id,
	        SUM(amount) AS sum_amount,
			eomd.end_date AS end_date
	      FROM user_app_public.zip_crew_stakes s
	      	left join analytics.users_master u on u.zip_user_id = s.user_id 
		  JOIN end_of_months_dates eomd ON 1=1
	      where signup_hostcountry not in ('test', 'error','xbullion')
	      	and staked_at >= '2020-12-17 00:00:00'
		  -- filter only stakes before specified timestamp
	        and staked_at < eomd.end_date::timestamp
	      -- and haven't been released yet
			--AND (released_at IS NULL OR released_at > eomd.end_date::timestamp)
	      GROUP BY
	        account_id,
			eomd.end_date
	    ) AS t
	)
	SELECT b.staked_at, b.account_id
		, a.amount
		, b.amount_staked_cum
		,zmt_usd
		,sum(r.amount) as amount_staked_l1y --last 1 year metric
	FROM staked_cum b 
		left join staked_monthly a on b.account_id = a.account_id and b.staked_at = a.staked_at
		left join staked_monthly r on r.account_id = b.account_id
					and r.staked_at <b.staked_at and b.staked_at >=b.staked_at-interval '1 year' --last 1 year metric
		right join (
			select date_trunc('month', created_at) as created_at, avg(price) as zmt_usd
			from oms_data.public.daily_ap_prices z
			where z.instrument_symbol  = 'ZMTUSD'
			group by 1
			) z ON cast(b.staked_at as date) = cast(z.created_at as date)
	   	left join analytics.users_master u on u.account_id = b.account_id
	group by 1,2,3,4,5
	)	
,monthly_user_balance as (
	select date_trunc ('month' , created_at) created_at
	,account_id
	,count(account_id) account_id_c
	, sum(usd_amount) usd_amount
	,avg(zmt_usd) as zmt_usd
	from daily_user_balance
	group by 1,2
	)	
,asset_holding as (--calculating balance by end of month or MTD
	select a.*, sum(l1y.account_id_c) account_id_c_l1y, sum(l1y.usd_amount) usd_amount_l1y
	from monthly_user_balance a
	left join monthly_user_balance l1y on l1y.account_id = a.account_id
		and l1y.created_at <a.created_at
		and l1y.created_at >= a.created_at - interval '1 year'
	group by 1, 2, 3, 4,5
	)
select 
coalesce (a.created_at, b.staked_at) as end_of_month_report
,coalesce(a.account_id, b.account_id) as account_id
,u.signup_hostcountry 
,account_id_c, usd_amount
,account_id_c_l1y, usd_amount_l1y
,amount_staked_cum
,coalesce (a.zmt_usd, b.zmt_usd) as zmt_usd
,amount_staked_l1y
from asset_holding a
	full outer join --this join zmt staked to user balance
		(select distinct * 
		from staked_cum) 
			b on b.staked_at = a.created_at
			and b.account_id = a.account_id
	left join analytics.users_master u on u.account_id  = a.account_id
where u.is_zipup_subscribed = true --<<<<<<<<<<<<=====================
--and a.account_id = 11
;

--having at least 1 trade ------------------
with 
base as (
	select t.signup_hostcountry
	,t.user_id
	,cast(t.created_at as date) as created_at
	,sum(amount_usd) amount_usd
	,count(trade_id) num_transaction
	from oms_data.analytics.trades_master t
		left join analytics.users_master u on u.user_id = t.user_id
	where t.signup_hostcountry  not in ('test', 'error')
	and u.account_id not in (63312,63313,161347,40706,38260,37955,37807,40683,38262,38121,27308,63611,38263)
	and t.created_at >= '2020-12-01 00:00:00' and t.created_at < '2021-05-07 00:00:00' --<<<<< CHANGE DATE HERE
	group by 1,2,3
	)
,base_m as (	
	select b.user_id
		,cast(DATE_TRUNC('month', b.created_at) as date) as mon
		,signup_hostcountry 
		,sum(amount_usd) amount_usd
		,sum(num_transaction) num_transaction
	from base b
--	where user_id = 38268
	group by 1,2,3
	)
select a.* 
,sum(b.amount_usd) as amount_usd_l30d
,sum(b.num_transaction) as num_transaction_l30d
,amount_usd_l365d
,num_transaction_l365d
from base_m a
	left join base b on b.user_id = a.user_id
		and b.created_at < mon
		and b.created_at >= mon - interval '1 month'
	left join (select a.* 
		,sum(c.amount_usd) as amount_usd_l365d
		,sum(c.num_transaction) as num_transaction_l365d
		from base_m a
		left join base c on c.user_id = a.user_id
		and c.created_at < mon
		and c.created_at >= mon - interval '1 year'
		group by 1,2,3,4,5
		) c on c.user_id = a.user_id and c.mon = a.mon
			and c.signup_hostcountry = a.signup_hostcountry
			and c.amount_usd = a.amount_usd
			and c.num_transaction = a.num_transaction
	group by 1,2,3,4,5,8,9
	;

--full user
select account_id , user_id, signup_hostcountry , cast(date_trunc('month', created_at) as date)
from analytics.users_master
where signup_hostcountry not in ('test', 'error', 'xbullion') and created_at < '2021-05-07 00:00:00' --<<<<< CHANGE DATE HERE
and is_verified  = TRUE
;


--ZMT tab -- ZMT in CIRCULATION --ZMT stake_at
with staked as (
	select u.signup_hostcountry
	,date_trunc('month', s.staked_at) as staked_at
	,account_id
	,sum (amount) as amount
	FROM oms_data.user_app_public.zip_crew_stakes s
 		left join analytics.users_master u on u.zip_user_id = s.user_id
	where 	signup_hostcountry not in ('test', 'error', 'xbullion')
	and staked_at >= '2020-12-17 00:00:00'
	and staked_at < '2021-05-07 00:00:00' --<<<<< CHANGE DATE HERE
	group by 1,2,3
	) 
,releasing as (
	select u.signup_hostcountry
	,date_trunc('month', s.releasing_at) as releasing_at
	,account_id
	,sum (amount) as amount
	FROM oms_data.user_app_public.zip_crew_stakes s
 		left join analytics.users_master u on u.zip_user_id = s.user_id
	where 	signup_hostcountry not in ('test', 'error', 'xbullion')
	and releasing_at < '2021-05-07 00:00:00' --<<<<< CHANGE DATE HERE
	group by 1,2,3
	)
--select * from 	staked;
select * from 	releasing;
------------------------======================
--ZMT category--this section is not main, can just add "amount" as zmt_monthly_staked to above script
with daily_user_balance as (
	select created_at, account_id , sum(usd_amount) as usd_amount
	from (
		select date_trunc('day',a.created_at)as created_at ,a.account_id , a.product_id, p.symbol
			, amount , c.average_high_low , g.mid_price , z.price, 1/e.exchange_rate as exchange_rate
			,SUM(a.amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate)) usd_amount
		from oms_data.public.accounts_positions_daily a
			left join oms_data.mysql_replica_apex.products p
				ON a.product_id = p.product_id
			LEFT JOIN oms_data.public.cryptocurrency_prices c
			    ON CONCAT(p.symbol, 'USD') = c.instrument_symbol
			    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', c.created_at)
			LEFT join oms_data.public.daily_closing_gold_prices g
				ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', g.created_at)
				AND a.product_id IN (15, 35)
			LEFT join oms_data.public.daily_ap_prices z
				ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', z.created_at)
				and z.instrument_symbol  = 'ZMTUSD'
				and a.product_id in (16, 50)
			left join public.exchange_rates e
				on date_trunc('day', e.created_at) = date_trunc('day', a.created_at)
				and e.product_2_symbol  = p.symbol
				and e.source = 'coinmarketcap'
		where a.created_at >='2019-01-01' and a.account_id not in (40683,37807,37955,38121,38260,38262,38263,40706)
--add this filter for 2nd run on script for ZMT amount
--and a.product_id in (16, 50) --<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
		--===================
		group by 1,2,3,4,5,6,7,8,9
		) a
	group by 1,2
	)
,staked_cum as (
	select --cummulative amount staked
		staked_at
		,s.account_id
		,amount
		, sum(amount) over(partition by account_id order by staked_at asc) as amount_staked_cum
		,s.zmt_usd
		from ( --daily staked
			select u.account_id 
			,date_trunc ('month', staked_at) as staked_at
			,sum(amount) amount
			,avg(z.price) as zmt_usd
			FROM oms_data.user_app_public.zip_crew_stakes s
		 		left join analytics.users_master u on u.zip_user_id = s.user_id
		 		LEFT join oms_data.public.daily_ap_prices z
		 			ON DATE_TRUNC('day', s.staked_at) = DATE_TRUNC('day', z.created_at)
					and z.instrument_symbol  = 'ZMTUSD'
			where  signup_hostcountry not in ('test', 'error','xbullion')
			and staked_at >= '2020-12-17 00:00:00'
			group by 1,2
			) s
	)	
,monthly_user_balance as (
	select date_trunc ('month' , created_at) created_at
	,account_id
	,count(account_id) account_id_c
	, sum(usd_amount) usd_amount
	from daily_user_balance
	group by 1,2
	)	
,asset_holding as (--calculating balance by end of month or MTD
	select a.*, sum(l1y.account_id_c) account_id_c_l1y, sum(l1y.usd_amount) usd_amount_l1y
	from monthly_user_balance a
	left join monthly_user_balance l1y on l1y.account_id = a.account_id
		and l1y.created_at <a.created_at
		and l1y.created_at >= a.created_at - interval '1 year'
	group by 1, 2, 3, 4
	)
--run this for holding any asset including ZMT	
select 
coalesce (a.created_at, b.staked_at) as end_of_month_report
,coalesce(a.account_id, b.account_id) as account_id
,u.signup_hostcountry 
,account_id_c, usd_amount
,account_id_c_l1y, usd_amount_l1y
,amount_staked_cum
,zmt_usd
,amount as zmt_monthly_staked
from asset_holding a
	full outer join --this join zmt staked to user balance
		(select distinct * 
		from staked_cum) 
			b on b.staked_at = a.created_at
			and b.account_id = a.account_id
	left join analytics.users_master u on u.account_id  = a.account_id
--where a.account_id = 38675
--limit 500
;




--Users holding ZMT as balance only, does not count staked amount
select created_at, count(distinct account_id) as account_id_c
,count(distinct case when usd_amount>0 then account_id end)	as account_id_c_positive_bal
,sum(usd_amount) as usd_amount_daily_balance
from (
	select date_trunc('day',a.created_at) as created_at,a.account_id , a.product_id, p.symbol
		, amount , c.average_high_low , g.mid_price , z.price, 1/e.exchange_rate as exchange_rate
		,SUM(a.amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate)) usd_amount
	from oms_data.public.accounts_positions_daily a
		left join oms_data.mysql_replica_apex.products p
			ON a.product_id = p.product_id
		LEFT JOIN oms_data.public.cryptocurrency_prices c
		    ON CONCAT(p.symbol, 'USD') = c.instrument_symbol
		    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', c.created_at)
		LEFT join oms_data.public.daily_closing_gold_prices g
			ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', g.created_at)
			AND a.product_id IN (15, 35)
		LEFT join oms_data.public.daily_ap_prices z
			ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', z.created_at)
			and z.instrument_symbol  = 'ZMTUSD'
			and a.product_id in (16, 50)
		left join public.exchange_rates e
			on date_trunc('day', e.created_at) = date_trunc('day', a.created_at)
			and e.product_2_symbol  = p.symbol
			and e.source = 'coinmarketcap'
	where a.created_at >='2021-03-10' 
--		and account_id = 99961 --25882
	group by 1,2,3,4,5,6,7,8,9
	) a
group by 1
;

--
--WITH "aum_summary" AS
--	(SELECT
--		DATE_TRUNC('month', a.balanced_at) "balance_date"
--		,(EXTRACT('year' FROM AGE(DATE_TRUNC('month', NOW()), DATE_TRUNC('month', a.balanced_at)))*12 + EXTRACT('month' FROM AGE(DATE_TRUNC('month', NOW()), DATE_TRUNC('month', a.balanced_at)))) + 1 "period_counter"
--		,u.signup_hostcountry
--		,p.symbol "product"
--		,a.account_id
--		,COALESCE(SUM(ROUND(a.product_amount * CASE WHEN p.product_id IN (14, 30, 33, 34) THEN 1 ELSE c.average_high_low END, 2)), 0) "usd_amount"
--	from oms_data.user_app_public.savings_plan_subscriptions s
--	LEFT join oms_data.analytics.users_master u
--		ON s.user_id = u.zip_user_id
--	LEFT join oms_data.data_imports.account_balance_eod_gmt0 a
--		ON u.account_id = a.account_id
--		AND a.product_id IN (1, 2, 3, 14, 15, 25, 26, 27, 30, 33, 34, 35)
--		/*
--		1	BTC		Bitcoin 		2	LTC		Litecoin 		3	ETH		Ethereum
--		14	USDT	Tether USD 		15	GOLD	XBullion 		25	BTC		Bitcoin
--		26	LTC		Litecoin 		27	ETH		Ethereum 		30	USDT	Tether USD
--		33	USDC	USD Coin 		34	USDC	USD Coin 		35	GOLD	XBullion
--		 */
--		-- AND a.balanced_at > s.subscribed_at
--		AND DATE_TRUNC('day', a.balanced_at) = DATE_TRUNC('month', a.balanced_at) + INTERVAL '1 MONTH - 1 day'
----		AND DATE_TRUNC('week', a.balanced_at) = DATE_TRUNC('day', a.balanced_at)
--	LEFT JOIN
--		oms_data.mysql_replica_apex.products p
--		ON a.product_id = p.product_id
--	LEFT JOIN
--	    oms_data.public.cryptocurrency_prices c
--	    ON CONCAT(p.symbol, 'USD') = c.instrument_symbol
--	    -- AUM valued at date of record
--	    AND DATE_TRUNC('day', c.last_updated) = DATE_TRUNC('day', NOW()) - INTERVAL '1 DAY' 
--	WHERE
--		s.user_id NOT IN ('01EC47YAM6SQE1AB67TGT36TC1','01EC47Y4Y3Q2HYJZXKT7FBJKQA','01EC47XTNMR2XT13EXNJ39363S','01EKFBKZWVEVPNZFJRGNMDSEP1','01EKFBDV7C7VFGZ9TWX0A6KJ0C','01EKFCGFSPZ4G64PAXM9N8E92Q','01EFK1CJN83EQK84Q4ZJ1BDE4T','01EDC2TH79S484ZAQEVB7Y2HQA','01EDN3YPPFD6NN75FX9W01M172')
--	GROUP BY
--		1, 2, 3, 4,5
--	ORDER by 1 desc
--	)
--	select * from aum_summary where account_id = 84319;
--	
--select *,  ntile(10) over (partition by signup_hostcountry, balance_date order by usd_amount asc) as percentile
--from (
--		select cast (balance_date as date) as balance_date, signup_hostcountry, account_id, sum(usd_amount) as usd_amount
--		from aum_summary 
--		where balance_date is not null  and signup_hostcountry not in ('error', 'test')
--		group by 1,2,3
--	) percentile
--		;

--===AUM low mid high for KC Monthly KPI report-- number look ok
--
WITH "aum_summary" AS
(
	SELECT
 		DATE_TRUNC('day', a.balanced_at) "balance_date"
		,u.signup_hostcountry
		,u.user_id 
--		,p.symbol "product"
		,COALESCE(SUM(ROUND(a.product_amount * CASE WHEN p.product_id IN (14, 30, 33, 34) THEN 1 ELSE c.average_high_low END, 2)), 0) "usd_amount"
	FROM		oms_data.user_app_public.savings_plan_subscriptions s
	LEFT JOIN		oms_data.analytics.users_master u
		ON s.user_id = u.zip_user_id
	LEFT JOIN		oms_data.data_imports.account_balance_eod_gmt0 a
		ON u.account_id = a.account_id
		AND a.product_id IN (1, 2, 3, 14, 15, 25, 26, 27, 30, 33, 34, 35)
		AND a.balanced_at > s.subscribed_at
	LEFT JOIN		oms_data.mysql_replica_apex.products p
		ON a.product_id = p.product_id
	LEFT JOIN	    oms_data.public.cryptocurrency_prices c
	    ON CONCAT(p.symbol, 'USD') = c.instrument_symbol
	    -- AUM valued at date of record
	    -- AND DATE_TRUNC('day', a.balanced_at) - INTERVAL '1 DAY' = DATE_TRUNC('day', c.last_updated)
	    -- AUM valued at today's price
	    AND DATE_TRUNC('day', c.last_updated) = DATE_TRUNC('day', NOW()) - INTERVAL '1 DAY' 
	WHERE		s.user_id NOT IN ('01EC47YAM6SQE1AB67TGT36TC1','01EC47Y4Y3Q2HYJZXKT7FBJKQA','01EC47XTNMR2XT13EXNJ39363S','01EKFBKZWVEVPNZFJRGNMDSEP1','01EKFBDV7C7VFGZ9TWX0A6KJ0C','01EKFCGFSPZ4G64PAXM9N8E92Q','01EFK1CJN83EQK84Q4ZJ1BDE4T','01EDC2TH79S484ZAQEVB7Y2HQA','01EDN3YPPFD6NN75FX9W01M172')
	GROUP BY		1, 2, 3
	ORDER BY		1 DESC, 2
)
,max_aum as (
	select 	a.*
	,max (usd_amount) over (partition by signup_hostcountry, user_id, DATE_TRUNC('month', a.balance_date)) as highest_month_bal
	from aum_summary a
--	where user_id in (9, 100019, 141584)
	)
select distinct DATE_TRUNC('month', balance_date) as highest_bal_month
	, signup_hostcountry, user_id, highest_month_bal
	,case when highest_month_bal <=5000 then 'low'
		when highest_month_bal>5000 and highest_month_bal<=1000000 then 'mid'
		when highest_month_bal>1000000 then 'high'
		else 'lol'
		end as mm_grp
from max_aum 
where usd_amount = highest_month_bal and usd_amount >0 
--and user_id = 106
;



---this side is for QA AUM at detail level
WITH "aum_summary" AS
(
	SELECT
 		DATE_TRUNC('day', a.balanced_at) "balance_date"
		,u.signup_hostcountry
		,u.user_id 
--		,p.symbol "product"
		,COALESCE(SUM(ROUND(a.product_amount * CASE WHEN p.product_id IN (14, 30, 33, 34) THEN 1 ELSE c.average_high_low END, 2)), 0) "usd_amount"
	FROM		oms_data.user_app_public.savings_plan_subscriptions s
	LEFT JOIN		oms_data.analytics.users_master u
		ON s.user_id = u.zip_user_id
	LEFT JOIN		oms_data.data_imports.account_balance_eod_gmt0 a
		ON u.account_id = a.account_id
		AND a.product_id IN (1, 2, 3, 14, 15, 25, 26, 27, 30, 33, 34, 35)
		AND a.balanced_at > s.subscribed_at
	LEFT JOIN		oms_data.mysql_replica_apex.products p
		ON a.product_id = p.product_id
	LEFT JOIN	    oms_data.public.cryptocurrency_prices c
	    ON CONCAT(p.symbol, 'USD') = c.instrument_symbol
	    -- AUM valued at date of record
	    -- AND DATE_TRUNC('day', a.balanced_at) - INTERVAL '1 DAY' = DATE_TRUNC('day', c.last_updated)
	    -- AUM valued at today's price
	    AND DATE_TRUNC('day', c.last_updated) = DATE_TRUNC('day', NOW()) - INTERVAL '1 DAY' 
	WHERE		s.user_id NOT IN ('01EC47YAM6SQE1AB67TGT36TC1','01EC47Y4Y3Q2HYJZXKT7FBJKQA','01EC47XTNMR2XT13EXNJ39363S','01EKFBKZWVEVPNZFJRGNMDSEP1','01EKFBDV7C7VFGZ9TWX0A6KJ0C','01EKFCGFSPZ4G64PAXM9N8E92Q','01EFK1CJN83EQK84Q4ZJ1BDE4T','01EDC2TH79S484ZAQEVB7Y2HQA','01EDN3YPPFD6NN75FX9W01M172')
	GROUP BY		1, 2, 3
	ORDER BY		1 DESC, 2
)
select *
from aum_summary
where signup_hostcountry = 'global' and user_id in (48308, 1369)

--====-AUM bal >0 MAPU==================
WITH "aum_summary" AS
	(SELECT
		DATE_TRUNC('month', a.balanced_at) "balance_date"
		,(EXTRACT('year' FROM AGE(DATE_TRUNC('month', NOW()), DATE_TRUNC('month', a.balanced_at)))*12 + EXTRACT('month' FROM AGE(DATE_TRUNC('month', NOW()), DATE_TRUNC('month', a.balanced_at)))) + 1 "period_counter"
		,u.signup_hostcountry
		,u.user_id 
		,COALESCE(SUM(ROUND(a.product_amount * CASE WHEN p.product_id IN (14, 30, 33, 34) THEN 1 ELSE c.average_high_low END, 2)), 0) "usd_amount"
	from 		oms_data.user_app_public.savings_plan_subscriptions s
	LEFT JOIN		oms_data.analytics.users_master u
		ON s.user_id = u.zip_user_id
	LEFT JOIN		oms_data.data_imports.account_balance_eod_gmt0 a
		ON u.account_id = a.account_id
		AND a.product_id IN (1, 2, 3, 14, 15, 25, 26, 27, 30, 33, 34, 35)
		AND DATE_TRUNC('day', a.balanced_at) = DATE_TRUNC('month', a.balanced_at) + INTERVAL '1 MONTH - 1 day'
	LEFT JOIN		oms_data.mysql_replica_apex.products p
		ON a.product_id = p.product_id
	LEFT JOIN	    oms_data.public.cryptocurrency_prices c
	    ON CONCAT(p.symbol, 'USD') = c.instrument_symbol
	    -- AUM valued at date of record
	    -- AND DATE_TRUNC('day', a.balanced_at) - INTERVAL '1 DAY' = DATE_TRUNC('day', c.last_updated)
	    -- AUM valued at today's price
	    AND DATE_TRUNC('day', c.last_updated) = DATE_TRUNC('day', NOW()) - INTERVAL '1 DAY' 
	WHERE		s.user_id NOT IN ('01EC47YAM6SQE1AB67TGT36TC1','01EC47Y4Y3Q2HYJZXKT7FBJKQA','01EC47XTNMR2XT13EXNJ39363S','01EKFBKZWVEVPNZFJRGNMDSEP1','01EKFBDV7C7VFGZ9TWX0A6KJ0C','01EKFCGFSPZ4G64PAXM9N8E92Q','01EFK1CJN83EQK84Q4ZJ1BDE4T','01EDC2TH79S484ZAQEVB7Y2HQA','01EDN3YPPFD6NN75FX9W01M172')
	GROUP BY		1, 2, 3,4
	ORDER BY		1 DESC, 2
)
select signup_hostcountry, balance_date, count(case when usd_amount >0 then user_id end) as aum_positive
from 	aum_summary a
where signup_hostcountry in ('TH', 'ID', 'AU', 'global') and balance_date is not null
group by 1,2

--this script is for KC country KPI =report number of product id each user own along with their total balance and EOM balance
 WITH "aum_summary" AS
	(SELECT
	DATE_TRUNC('day', a.balanced_at) "balance_date"
	,u.signup_hostcountry
	,u.user_id
	,a.product_id
	,COALESCE(SUM(ROUND(a.product_amount * CASE WHEN p.product_id IN (14, 30, 33, 34) THEN 1 ELSE c.average_high_low END, 2)), 0) "usd_amount"
	FROM oms_data.user_app_public.savings_plan_subscriptions s
	LEFT JOIN oms_data.analytics.users_master u
		ON s.user_id = u.zip_user_id
	LEFT JOIN oms_data.data_imports.account_balance_eod_gmt0 a
		ON u.account_id = a.account_id
		AND a.product_id IN (1, 2, 3, 14, 15, 25, 26, 27, 30, 33, 34, 35)
		AND a.balanced_at > s.subscribed_at
	LEFT JOIN oms_data.mysql_replica_apex.products p
		ON a.product_id = p.product_id
	LEFT JOIN oms_data.public.cryptocurrency_prices c
	    ON CONCAT(p.symbol, 'USD') = c.instrument_symbol
	    AND DATE_TRUNC('day', c.last_updated) = DATE_TRUNC('day', NOW()) - INTERVAL '1 DAY' 
	WHERE s.user_id NOT IN ('01EC47YAM6SQE1AB67TGT36TC1','01EC47Y4Y3Q2HYJZXKT7FBJKQA','01EC47XTNMR2XT13EXNJ39363S','01EKFBKZWVEVPNZFJRGNMDSEP1','01EKFBDV7C7VFGZ9TWX0A6KJ0C','01EKFCGFSPZ4G64PAXM9N8E92Q','01EFK1CJN83EQK84Q4ZJ1BDE4T','01EDC2TH79S484ZAQEVB7Y2HQA','01EDN3YPPFD6NN75FX9W01M172')
	GROUP BY 1, 2, 3,4
	ORDER BY 1 DESC, 2
	)
,eom as (
	select a.signup_hostcountry, e.user_id, date_trunc('month', e.balance_date_eom) as balance_date_eom
		, count(distinct a.product_id) as product_id_d
		, sum(a.usd_amount) as usd_amount
	from (
		select distinct user_id, max(balance_date) over (partition by DATE_TRUNC('month', balance_date)) as balance_date_eom	
		FROM 	aum_summary a
		--WHERE user_id in (38604,40524,2931)-----user testing
		) e
	left join aum_summary a on a.user_id = e.user_id and a.balance_date = e.balance_date_eom -- this join is to take last day of month
	group by 1,2,3
	)
,aum_prep as (
	SELECT 	
	case when balance_date = DATE_TRUNC('day', NOW()) - INTERVAL '1 DAY' then DATE_TRUNC('day', NOW()) - INTERVAL '1 DAY'
		else DATE_TRUNC('month', balance_date) end as balance_date_avg
	, user_id, signup_hostcountry
	, count(distinct product_id) as product_id_d
	, sum(usd_amount) as usd_amount
	, count(product_id) as product_id_c
	FROM 	aum_summary a
		--WHERE user_id in (38604,40524,2931)
	group by 1,2,3
	)
select 	a.balance_date_avg, a.user_id, a.signup_hostcountry, a.product_id_d as product_id_own, a.usd_amount as usd_amount_total
	,a.product_id_c --if own 2 product, count can be greater than 30
	,e.product_id_d as product_id_own_eom
	,e.usd_amount as usd_amount_eom
from aum_prep a
	left join eom e on e.user_id =a.user_id and e.balance_date_eom = a.balance_date_avg
where e.user_id is not null
 ;
 




---number of user having ZMT balance and  ZMT staked in usd dollar
--with daily_user_balance as (
--	select created_at, account_id , sum(usd_amount) as usd_amount
--	from (
--		select date_trunc('day',a.created_at)as created_at ,a.account_id , a.product_id, p.symbol
--			, amount , c.average_high_low , g.mid_price , z.price, 1/e.exchange_rate as exchange_rate
--			,SUM(a.amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate)) usd_amount
--		from oms_data.public.accounts_positions_daily a
--			left join oms_data.mysql_replica_apex.products p
--				ON a.product_id = p.product_id
--			LEFT JOIN oms_data.public.cryptocurrency_prices c
--			    ON CONCAT(p.symbol, 'USD') = c.instrument_symbol
--			    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', c.created_at)
--			LEFT join oms_data.public.daily_closing_gold_prices g
--				ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', g.created_at)
--				AND a.product_id IN (15, 35)
--			LEFT join oms_data.public.daily_ap_prices z
--				ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', z.created_at)
--				and z.instrument_symbol  = 'ZMTUSD'
--				and a.product_id in (16, 50)
--			left join public.exchange_rates e
--				on date_trunc('day', e.created_at) = date_trunc('day', a.created_at)
--				and e.product_2_symbol  = p.symbol
--				and e.source = 'coinmarketcap'
--		where a.created_at >='2020-01-01' and a.account_id not in (83822,57145,97321,47351,95471,68654,65476,67448,74213,112192,87004,60786,58132,106684,88967,52826,83821,78111,84684,83109,54687,71617,81334,49340,73680,123314,90150,136168,101733,67810,64567,97019,81743,73926,39246,50940,80581,72147,64802,117338,101261,93185,83467,103594,81516,63989,69205,95302,57996,99702,100182,103773,125847,88379,70922,87096,107757,58588,65450,60855,71577,71634,134288,11414,77178,110919,106664,100555,91928,92680,53414,78389,86967,39445,90429,139172,145632,82464,77336,118085,95254,77371,138342,70494,82055,101545,89359,92234)
--			and a.product_id in (16, 50)---->ZMT only
--		group by 1,2,3,4,5,6,7,8,9
--		) a
--	group by 1,2
--	)
--,staked_cum as (
--	select * ,max(amount_staked_cum) over(partition by account_id, end_of_month_report) as amount_staked_cum_eom
--	from (select 
--		case when cast(staked_at as date) >= cast(date_trunc('month', now () - interval '1 day') as date) 
--			then cast(max(staked_at) over (partition by account_id) as date)
--			else cast(date_trunc('month', staked_at ) +interval '1 month -1' as date)  end as end_of_month_report
--		,s.staked_at
--		,s.account_id
--		,s.zmt_usd
--		, sum(amount) over(partition by account_id order by staked_at asc) as amount_staked_cum
--		from (
--			select u.account_id 
--			,cast (staked_at as date) as staked_at
--			,z.price as zmt_usd
--			,sum(amount) amount
--			FROM oms_data.user_app_public.zip_crew_stakes s
--		 		left join analytics.users_master u on u.zip_user_id = s.user_id
--		 		LEFT join oms_data.public.daily_ap_prices z
--		 			ON DATE_TRUNC('day', s.staked_at) = DATE_TRUNC('day', z.created_at)
--					and z.instrument_symbol  = 'ZMTUSD'
----					and a.product_id in (16, 50)
--			where  signup_hostcountry not in ('test', 'error','xbullion')
--			and staked_at >= '2020-12-17 00:00:00'
--			and account_id not in (83822,57145,97321,47351,95471,68654,65476,67448,74213,112192,87004,60786,58132,106684,88967,52826,83821,78111,84684,83109,54687,71617,81334,49340,73680,123314,90150,136168,101733,67810,64567,97019,81743,73926,39246,50940,80581,72147,64802,117338,101261,93185,83467,103594,81516,63989,69205,95302,57996,99702,100182,103773,125847,88379,70922,87096,107757,58588,65450,60855,71577,71634,134288,11414,77178,110919,106664,100555,91928,92680,53414,78389,86967,39445,90429,139172,145632,82464,77336,118085,95254,77371,138342,70494,82055,101545,89359,92234)
--			group by 1,2,3
--			) s
--		) a
--	)
--,asset_holding as (
--	select a.end_of_month_report, a.account_id, usd_amount as usd_amount_eom, usd_amount_l30d, usd_amount_l365d
--	from (
--		select 
--		case when cast(d.created_at as date) = cast(now () - interval '1 day' as date) then cast(d.created_at as date)
--			else cast(date_trunc('month', d.created_at ) +interval '1 month -1' as date)  end as end_of_month_report
--		,cast (d.created_at as date) as day_matching
--		,d.*, avg(l30d.usd_amount) as usd_amount_l30d, avg(l365d.usd_amount) as usd_amount_l365d
--		from daily_user_balance d
--			left join daily_user_balance l30d on  l30d.account_id = d.account_id
--				and l30d.created_at <d.created_at
--				and l30d.created_at >= d.created_at - interval '1 month'
--			left join daily_user_balance l365d on  l365d.account_id = d.account_id
--				and l365d.created_at <d.created_at
--				and l365d.created_at >= d.created_at - interval '1 year'
--		group by 1,2,3,4,5
--		) a
--	where a.end_of_month_report = cast(a.created_at as date)
--	)
--	
----run this for holding any asset including ZMT	
--select 
--coalesce (a.end_of_month_report, b.end_of_month_report) as end_of_month_report
--,coalesce(a.account_id, b.account_id) as account_id
--,u.signup_hostcountry 
--,usd_amount_eom, usd_amount_l30d, usd_amount_l365d
--,amount_staked_cum_eom
--,zmt_usd
--from asset_holding a
--	full outer join 
--		(select distinct end_of_month_report,account_id, amount_staked_cum_eom, zmt_usd
--		from staked_cum) b on b.end_of_month_report = a.end_of_month_report
--			and b.account_id = a.account_id
--	left join analytics.users_master u on u.account_id  = a.account_id
----limit 200
--;