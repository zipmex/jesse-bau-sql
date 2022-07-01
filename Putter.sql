------ user with deposit >= 5 mil THB, aum and staked
WITH daily_user_balance as (
	select created_at, account_id, signup_hostcountry --, symbol
	, sum(usd_amount) as usd_amount, avg(price) as zmt_usd 
	from (
		select date_trunc('day',a.created_at)as created_at ,a.account_id , a.product_id, p.symbol, u.signup_hostcountry 
			, amount , c.average_high_low , g.mid_price , z.price, 1/e.exchange_rate as exchange_rate
			,SUM(a.amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate)) usd_amount
		from oms_data.public.accounts_positions_daily a
			left join analytics.users_master u on a.account_id = u.account_id 
			left join oms_data.mysql_replica_apex.products p
				ON a.product_id = p.product_id
			LEFT JOIN oms_data.public.cryptocurrency_prices c 
			    ON CONCAT(p.symbol, 'USD') = c.instrument_symbol
			    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', c.created_at)
			LEFT join oms_data.public.daily_closing_gold_prices g
				ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', g.created_at)
				AND a.product_id IN (15,	 35)
			LEFT join oms_data.public.daily_ap_prices z
				ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', z.created_at)
				and z.instrument_symbol  = 'ZMTUSD'
				and a.product_id in (16, 50)
			left join public.exchange_rates e
				on date_trunc('day', e.created_at) = date_trunc('day', a.created_at)
				and e.product_2_symbol  = p.symbol
				and e.source = 'coinmarketcap'
		where a.created_at >='2021-06-01 00:00:00' and a.created_at < DATE_TRUNC('day',NOW()) --<<<<<<<<CHANGE DATE HERE
		and u.signup_hostcountry  not in ('test', 'error','xbullion')
		and a.account_id not in (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347) 
		group by 1,2,3,4,5,6,7,8,9,10
		) a
	group by 1,2,3
	order by 1 desc 
),monthly_user_balance as (
	select date_trunc ('month' , d.created_at) created_at
	,d.account_id, d.signup_hostcountry --d.symbol
	,coalesce (e.usd_amount,y.usd_amount) eom_aum 
	,count(d.account_id) account_id_c
	,sum(d.usd_amount) usd_amount
	,avg(d.zmt_usd) as zmt_usd 
	from daily_user_balance d 
		---- add end of month aum
		left join (select date_trunc('month',created_at) + '1 month - 1 day'::interval month_
		, account_id , signup_hostcountry , sum(usd_amount) usd_amount 
		from daily_user_balance where created_at = date_trunc('month',created_at) + '1 month - 1 day'::interval
		group by 1,2,3) e  
		on d.account_id = e.account_id and d.signup_hostcountry = e.signup_hostcountry
		and date_trunc('month', d.created_at) = date_trunc('month',e.month_) 
		---- add yesterday aum 
		left join (select created_at , account_id , signup_hostcountry , sum(usd_amount) usd_amount 
		from daily_user_balance where created_at = '2021-06-22 00:00:00' --date_trunc('day',NOW()) - '2 day'::interval --<<<<<<<<<<<<<<<<< CHANGE DATE HERE
		group by 1,2,3) y   
		on d.account_id = y.account_id and d.signup_hostcountry = y.signup_hostcountry
		and date_trunc('month', d.created_at) = date_trunc('month',y.created_at)
	group by 1,2,3,4
),asset_holding as (--calculating balance by end of month or MTD
	select a.*, sum(l1y.account_id_c) account_id_c_l1y, sum(l1y.usd_amount) usd_amount_l1y
	from monthly_user_balance a
	left join monthly_user_balance l1y on l1y.account_id = a.account_id
		and l1y.created_at < a.created_at
		and l1y.created_at >= a.created_at - interval '1 year'
	group by 1,2,3,4,5,6,7
	order by 1 
), staked_eom as ( ----- this section provide end of month zmt staked
		SELECT
			d.date ,u.account_id ,u.signup_hostcountry
			,SUM(s.amount) "zmt_staked_amount"
			,SUM(s.amount* c.price) "zmt_staked_usd_amount"
		FROM (
			SELECT DISTINCT date--(DATE_TRUNC('month', date)) + INTERVAL '1 MONTH - 1 day' "month"
				, u.user_id			FROM  GENERATE_SERIES('2020-12-01'::DATE, NOW()::DATE, '1 day') "date"
			CROSS JOIN (SELECT DISTINCT user_id FROM oms_data.user_app_public.zip_crew_stakes) u
			ORDER BY 1 ASC
			) d --date_series
			LEFT JOIN oms_data.user_app_public.zip_crew_stakes s
				ON d.user_id = s.user_id
				AND DATE_TRUNC('day', d.date) >= DATE_TRUNC('day', s.staked_at)
				AND DATE_TRUNC('day', d.date) < COALESCE(DATE_TRUNC('day', s.released_at), DATE_TRUNC('day', s.releasing_at)) 
			LEFT JOIN oms_data.analytics.users_master u
				ON s.user_id = u.zip_user_id
			LEFT JOIN oms_data.mysql_replica_apex.products p
				ON s.product_id = p.product_id
			-- join crypto usd prices
			LEFT JOIN oms_data.public.prices_eod_gmt0 c
				ON p.symbol = c.product_1_symbol
				AND c.product_2_symbol = 'USD'
				AND d.date = DATE_TRUNC('day', c.actual_timestamp)
				AND p."type" = 2
		WHERE u.account_id IS NOT null
			and u.account_id NOT IN (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347)
			and u.signup_hostcountry in ('TH','ID','AU','global') 
			AND d.date = NOW()::date - '1 day'::INTERVAL 
		GROUP BY 1,2,3
), base AS (
SELECT date_trunc('month', created_at) month_
	, account_id 
	, signup_hostcountry 
	, SUM(amount_usd) sum_deposit_usd 
FROM analytics.deposit_tickets_master d 
WHERE created_at >=  date_trunc('month', NOW()) - '1 month'::INTERVAL --'2021-01-01 00:00:00' --
AND signup_hostcountry NOT IN ('test','error','xbullion')
AND status = 'FullyProcessed' 
GROUP BY 1,2,3 
), deposit_ AS (
SELECT month_ 
	, COUNT(DISTINCT account_id) AS user_count
FROM base 
WHERE sum_deposit_usd >= 157549.8
GROUP BY 1
)
SELECT d.month_, d.signup_hostcountry
	, CASE WHEN d.account_id IS NOT NULL THEN 'user' END AS user_name
	, d.sum_deposit_usd 
	, d.user_count 
	, a.account_id_c 
	, a.usd_amount aum_cumulative 
	, a.eom_aum aum_balance 
	, s.zmt_staked_usd_amount 
FROM deposit_ d 
	LEFT JOIN asset_holding a 
		ON a.account_id = d.account_id AND a.created_at = d.month_ 
	LEFT JOIN staked_eom s 
		ON d.account_id = s.account_id AND d.month_ = DATE_TRUNC('month',s.date)
--WHERE d.account_id IN (114423,167164)
;



with daily_user_balance as (
	select created_at, account_id, signup_hostcountry --, symbol
	, sum(usd_amount) as usd_amount, avg(price) as zmt_usd 
	from (
		select date_trunc('day',a.created_at)as created_at ,a.account_id , a.product_id, p.symbol, u.signup_hostcountry 
			, amount , c.average_high_low , g.mid_price , z.price, 1/e.exchange_rate as exchange_rate
			,SUM(a.amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate)) usd_amount
		from oms_data.public.accounts_positions_daily a
			left join analytics.users_master u on a.account_id = u.account_id 
			left join oms_data.mysql_replica_apex.products p
				ON a.product_id = p.product_id
			LEFT JOIN oms_data.public.cryptocurrency_prices c 
			    ON CONCAT(p.symbol, 'USD') = c.instrument_symbol
			    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', c.created_at)
			LEFT join oms_data.public.daily_closing_gold_prices g
				ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', g.created_at)
				AND a.product_id IN (15,	 35)
			LEFT join oms_data.public.daily_ap_prices z
				ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', z.created_at)
				and z.instrument_symbol  = 'ZMTUSD'
				and a.product_id in (16, 50)
			left join public.exchange_rates e
				on date_trunc('day', e.created_at) = date_trunc('day', a.created_at)
				and e.product_2_symbol  = p.symbol
				and e.source = 'coinmarketcap'
		where a.created_at >='2019-01-01 00:00:00' and a.created_at < '2021-06-24 00:00:00' -- DATE_TRUNC('day',NOW()) --<<<<<<<<CHANGE DATE HERE
		and u.signup_hostcountry  not in ('test', 'error','xbullion')
		and a.account_id not in (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347) 
		group by 1,2,3,4,5,6,7,8,9,10
		) a
	group by 1,2,3
	order by 1 desc 
),monthly_user_balance as (
	select date_trunc ('month' , d.created_at) created_at
	,d.account_id, d.signup_hostcountry --, d.symbol
	,coalesce (e.usd_amount,y.usd_amount) eom_aum 
	,count(d.account_id) account_id_c
	,sum(d.usd_amount) usd_amount
	,avg(d.zmt_usd) as zmt_usd 
	from daily_user_balance d 
		---- add end of month aum
		left join (select date_trunc('month',created_at) + '1 month - 1 day'::interval month_
		, account_id , signup_hostcountry , sum(usd_amount) usd_amount 
		from daily_user_balance where created_at = date_trunc('month',created_at) + '1 month - 1 day'::interval
		group by 1,2,3) e  
		on d.account_id = e.account_id and d.signup_hostcountry = e.signup_hostcountry
		and date_trunc('month', d.created_at) = date_trunc('month',e.month_) 
		---- add yesterday aum 
		left join (select created_at , account_id , signup_hostcountry , sum(usd_amount) usd_amount 
		from daily_user_balance where created_at = DATE_TRUNC('day',NOW()) - '1 day'::interval --<<<<<<<<<<<<<<<<< CHANGE DATE HERE
		group by 1,2,3) y   
		on d.account_id = y.account_id and d.signup_hostcountry = y.signup_hostcountry
		and date_trunc('month', d.created_at) = date_trunc('month',y.created_at)
	group by 1,2,3,4
),asset_holding as (--calculating balance by end of month or MTD
	select a.*, sum(l1y.account_id_c) account_id_c_l1y, sum(l1y.usd_amount) usd_amount_l1y
	from monthly_user_balance a
	left join monthly_user_balance l1y on l1y.account_id = a.account_id
		and l1y.created_at < a.created_at
		and l1y.created_at >= a.created_at - interval '1 year'
	group by 1,2,3,4,5,6,7
	order by 1 
), staked_monthly as ( select u.account_id 
			,date_trunc ('day', staked_at) as staked_at
			,sum(amount) amount
			,avg(z.price) as zmt_usd
			FROM oms_data.user_app_public.zip_crew_stakes s
		 		left join analytics.users_master u on u.zip_user_id = s.user_id
		 		LEFT join oms_data.public.daily_ap_prices z
		 			ON DATE_TRUNC('day', s.staked_at) = DATE_TRUNC('day', z.created_at)
					and z.instrument_symbol  = 'ZMTUSD'
			where  signup_hostcountry not in ('test', 'error','xbullion')
			and staked_at >= '2020-12-17 00:00:00' and staked_at < '2021-06-24 00:00:00' --DATE_TRUNC('day',NOW())--<<<<<<<<CHANGE DATE HERE
			group by 1,2
			), staked_eom as ( ----- this section provide end of month zmt staked
				SELECT
					d.date ,u.account_id ,u.signup_hostcountry
					,SUM(s.amount) "zmt_staked_amount"
					,SUM(s.amount* c.price) "zmt_staked_usd_amount"
				FROM (
					SELECT DISTINCT date--(DATE_TRUNC('month', date)) + INTERVAL '1 MONTH - 1 day' "month"
						,u.user_id
					FROM  GENERATE_SERIES('2020-12-01'::DATE, '2021-07-01'::DATE, '1 day') "date"
					CROSS JOIN (SELECT DISTINCT user_id FROM oms_data.user_app_public.zip_crew_stakes) u
					ORDER BY 1 ASC
					) d --date_series
				LEFT JOIN oms_data.user_app_public.zip_crew_stakes s
					ON d.user_id = s.user_id
					AND DATE_TRUNC('day', d.date) >= DATE_TRUNC('day', s.staked_at)
					AND DATE_TRUNC('day', d.date) < COALESCE(DATE_TRUNC('day', s.released_at), DATE_TRUNC('day', s.releasing_at)) 
				LEFT JOIN oms_data.analytics.users_master u
					ON s.user_id = u.zip_user_id
				LEFT JOIN oms_data.mysql_replica_apex.products p
					ON s.product_id = p.product_id
				-- join crypto usd prices
				LEFT JOIN oms_data.public.prices_eod_gmt0 c
					ON p.symbol = c.product_1_symbol
					AND c.product_2_symbol = 'USD'
					AND d.date = DATE_TRUNC('day', c.actual_timestamp)
					AND p."type" = 2
				WHERE u.account_id IS NOT NULL
					AND u.account_id NOT IN (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347)
					AND u.signup_hostcountry IN ('TH','ID','AU','global')
					AND d.date = NOW()::date - '1 day'::INTERVAL 
				GROUP BY 1,2,3
	),staked_cum as (--this section has last 1 year metric
	with staked_monthly as (
		select u.account_id 
			,date_trunc ('month', staked_at) as staked_at
			,sum(amount) amount
		FROM oms_data.user_app_public.zip_crew_stakes s
	 		left join analytics.users_master u on u.zip_user_id = s.user_id
	 	where  signup_hostcountry not in ('test', 'error','xbullion')
		and staked_at >= '2020-12-17 00:00:00' and staked_at < '2021-06-24 00:00:00' --DATE_TRUNC('day',NOW()) --<<<<<<<<CHANGE DATE HERE
		group by 1,2
	)
	,end_of_months_dates AS (
		SELECT (d + '1 month'::interval - '1 day'::interval )::date end_date
		FROM generate_series('2020-12-01'::date, '2021-06-24'::date, '1 month'::interval) d --<<<<<<<<CHANGE DATE HERE
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
	group by 1,2,3,4
	)	
select coalesce (a.created_at, date_trunc('month', b.staked_at)) as end_of_month_report
	,coalesce(a.account_id, b.account_id) as account_id
	--, a.symbol --,u.email 
	,coalesce(a.signup_hostcountry, u.signup_hostcountry) signup_hostcountry
	,case when a.account_id in ('15',	'221',	'634',	'746',	'1002',	'1182',	'1202',	'1272',	'1708',	'6074',	'6828',	'11284',	'16293',	'19763',	'24108',	'24315',	'25431',	'37276',	'38526',	'39858',	'40119',	'40438',	'40890',	'48300',	'51313',	'51333',	'52266',	'54172',	'54231',	'54644',	'55224',	'55660',	'57262',	'58998',	'59049',	'59693',	'62663',	'63292',	'63314',	'63914',	'66402',	'67813',	'82129',	'84431',	'84461',	'84799',	'91297',	'92285',	'93791',	'94663',	'94993',	'96434',	'96535',	'101654',	'101786',	'103488',	'103855',	'104832',	'106308',	'108014',	'127491',	'128405',	'131484',	'139503',	'141711',	'146194',	'146356',	'147984',	'157600',	'159685',	'161863',	'180376',	'183004')
		or b.account_id in ('15',	'221',	'634',	'746',	'1002',	'1182',	'1202',	'1272',	'1708',	'6074',	'6828',	'11284',	'16293',	'19763',	'24108',	'24315',	'25431',	'37276',	'38526',	'39858',	'40119',	'40438',	'40890',	'48300',	'51313',	'51333',	'52266',	'54172',	'54231',	'54644',	'55224',	'55660',	'57262',	'58998',	'59049',	'59693',	'62663',	'63292',	'63314',	'63914',	'66402',	'67813',	'82129',	'84431',	'84461',	'84799',	'91297',	'92285',	'93791',	'94663',	'94993',	'96434',	'96535',	'101654',	'101786',	'103488',	'103855',	'104832',	'106308',	'108014',	'127491',	'128405',	'131484',	'139503',	'141711',	'146194',	'146356',	'147984',	'157600',	'159685',	'161863',	'180376',	'183004')
			then true else false 
			end as is_pcs
	,account_id_c, usd_amount, eom_aum 
--	,account_id_c_l1y, usd_amount_l1y
	,e.zmt_staked_amount--, e.zmt_staked_usd_amount
	,amount_staked_cum
	,coalesce (a.zmt_usd, b.zmt_usd) as zmt_usd
	--,amount_staked_l1y
	--,zmt_staked_amount eom_staked
from asset_holding a 
	full outer join (select distinct * from staked_cum) b on b.staked_at = a.created_at and b.account_id = a.account_id
	LEFT JOIN staked_eom e on e.account_id = b.account_id --AND date_trunc('month',e.date) = b.staked_at 
	left join analytics.users_master u 
		on u.account_id  = a.account_id  and u.account_id = a.account_id
--where coalesce(a.account_id, b.account_id) in (143639) --u.is_zipup_subscribed = true --<<<<<<<<<<<<===================== 
order by 1
;
