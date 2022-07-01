----- kyc user with monthly trade vol
with date_series as (
	SELECT
		DISTINCT
		date(DATE_TRUNC('month', date)) "month"
		,u.user_id
		,u.account_id 
		,u.signup_hostcountry 
	FROM 
		GENERATE_SERIES('2020-12-01'::DATE, NOW()::DATE, '1 month') "date"
	CROSS JOIN
		(SELECT DISTINCT user_id, account_id, signup_hostcountry FROM analytics.users_master 
		where (is_verified = true or level_increase_status = 'pass') and signup_hostcountry  not in ('test', 'error','xbullion')) u
	ORDER BY
		1 ASC
), base as (
select t.signup_hostcountry
	,t.user_id
	,t.account_id
	,cast(t.created_at as date) as created_at
	,sum(amount_usd) amount_usd
	,count(trade_id) num_transaction
	from oms_data.analytics.trades_master t
		left join analytics.users_master u on u.user_id = t.user_id
	where t.signup_hostcountry  not in ('test', 'error','xbullion')
	and u.account_id not in ('0','186','187','869','634','870','1356','1357','4344','18866','25041','25224','25225','25226','25227','38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','49659','49658','52018','52019','44057','161347') -- Minh 2021-05-31
--	and u.account_id not in (1373,1432,13266,16211,16308,22576,34535,48900,80871,84319) -- whales 
	and t.created_at >= '2020-12-01 00:00:00' and t.created_at < '2021-06-04 00:00:00' --<<<<< CHANGE DATE HERE
--	and t.product_1_id not in ('16','50') -- excl ZMT 
	group by 1,2,3,4 
	)
,base_m as (	
	select b.user_id, b.account_id 
		,cast(DATE_TRUNC('month', b.created_at) as date) as mon
		,signup_hostcountry 
		,sum(amount_usd) amount_usd
		,sum(num_transaction) num_transaction
	from base b
	group by 1,2,3,4 
	), daily_user_balance as (
	select created_at, account_id , sum(usd_amount) as usd_amount, avg(price) as zmt_usd
	from (
		select date_trunc('day',a.created_at)as created_at ,a.account_id , a.product_id, p.symbol
			, amount , c.average_high_low , g.mid_price , z.price, 1/e.exchange_rate as exchange_rate
			,SUM(a.amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate)) usd_amount
			 	,COALESCE(SUM(ROUND(CASE 	WHEN p.product_id = 6 THEN a.amount * 1
					WHEN p.type = 2 THEN a.amount * c1.price
					WHEN p.type = 1 THEN a.amount / e.exchange_rate
					ELSE 0 END, 10)), 0) "usd_amount1"
		from oms_data.public.accounts_positions_daily a
			left join oms_data.mysql_replica_apex.products p
				ON a.product_id = p.product_id 
			LEFT join oms_data.public.prices_eod_gmt0 c1 
	ON p.symbol = c1.product_1_symbol AND c1.product_2_symbol = 'USD'
	-- if you want rate from specific date replace DATE_TRUNC('day', a.balanced_at)
	AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', c1.actual_timestamp) 
	AND p."type" = 2
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
		where a.created_at >='2019-01-01 00:00:00' and a.created_at <'2021-06-04 00:00:00' --<<<<<<<<CHANGE DATE HERE
		and a.account_id not in (63312,63313,161347,40706,38260,37955,37807,38263,40683,38262,38121,27308,48870,48948,0)
		--Total Users with ZipUp assets -----<<<<<<<<<<<<<<<<<<<<<<<<<<<<
		-- BTC, USDT, USDC, GOLD, LTC, ETH only 
--		1	BTC		Bitcoin 		2	LTC		Litecoin 		3	ETH		Ethereum
--		14	USDT	Tether USD 		15	GOLD	XBullion 		25	BTC		Bitcoin
--		26	LTC		Litecoin 		27	ETH		Ethereum 		30	USDT	Tether USD
--		33	USDC	USD Coin 		34	USDC	USD Coin 		35	GOLD	XBullion
		--AND a.product_id IN (16,50) -- <<<<<<<< run this for ZMT only 
		AND a.product_id IN (1, 2, 3, 14, 15, 25, 26, 27, 30, 33, 34, 35) -- <<<<<<<<<<<<========= run this for without ZMT  
		group by 1,2,3,4,5,6,7,8,9
		) a
	group by 1,2 
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
select d.month
	, d.user_id
	, d.account_id
	, d.signup_hostcountry
	, coalesce (a.amount_usd,0) trade_amount_usd
	, coalesce (a.num_transaction,0) num_transaction 
	, coalesce (sum(b.amount_usd),0) as trade_amount_usd_l30d
	, coalesce (sum(b.num_transaction),0) as num_transaction_l30d
	, coalesce (h.account_id_c,0) account_id_c
	, coalesce (h.usd_amount ,0) aum_usd_amount 
--,coalesce (amount_usd_l365d,0) amount_usd_l365d
--,coalesce (num_transaction_l365d,0) num_transaction_l365d
from date_series d 
	left join base_m a on d.user_id = a.user_id and d.month = a.mon and d.account_id = a.account_id and d.signup_hostcountry = a.signup_hostcountry
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
		group by 1,2,3,4,5,6
		) c on c.user_id = a.user_id and c.mon = a.mon
			and c.signup_hostcountry = a.signup_hostcountry
			and c.amount_usd = a.amount_usd
			and c.num_transaction = a.num_transaction
	left join asset_holding h on d.account_id = h.account_id and d.month = h.created_at 
--	where d.account_id = 1708
	group by 1,2,3,4,5,6,9,10 
	

----- kyc user with monthly zipup aum
with date_series as (
	SELECT
		DISTINCT
		date(DATE_TRUNC('month', date)) "month"
		,u.user_id
		,u.account_id 
		,u.signup_hostcountry 
	FROM 
		GENERATE_SERIES('2020-12-01'::DATE, NOW()::DATE, '1 month') "date"
	CROSS JOIN
		(SELECT DISTINCT user_id, account_id, signup_hostcountry FROM analytics.users_master 
		where (is_verified = true or level_increase_status = 'pass') and signup_hostcountry  not in ('test', 'error','xbullion')) u
	ORDER BY
		1 ASC
), daily_user_balance as (
	select created_at, account_id , sum(usd_amount) as usd_amount, avg(price) as zmt_usd
	from (
		select date_trunc('day',a.created_at)as created_at ,a.account_id , a.product_id, p.symbol
			, amount , c.average_high_low , g.mid_price , z.price, 1/e.exchange_rate as exchange_rate
			,SUM(a.amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate)) usd_amount
			 	,COALESCE(SUM(ROUND(CASE 	WHEN p.product_id = 6 THEN a.amount * 1
					WHEN p.type = 2 THEN a.amount * c1.price
					WHEN p.type = 1 THEN a.amount / e.exchange_rate
					ELSE 0 END, 10)), 0) "usd_amount1"
		from oms_data.public.accounts_positions_daily a
			left join oms_data.mysql_replica_apex.products p
				ON a.product_id = p.product_id 
			LEFT join oms_data.public.prices_eod_gmt0 c1 
	ON p.symbol = c1.product_1_symbol AND c1.product_2_symbol = 'USD'
	-- if you want rate from specific date replace DATE_TRUNC('day', a.balanced_at)
	AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', c1.actual_timestamp) 
	AND p."type" = 2
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
		where a.created_at >='2019-01-01 00:00:00' and a.created_at <'2021-06-04 00:00:00' --<<<<<<<<CHANGE DATE HERE
		and a.account_id not in (63312,63313,161347,40706,38260,37955,37807,38263,40683,38262,38121,27308,48870,48948,0)
		--Total Users with ZipUp assets -----<<<<<<<<<<<<<<<<<<<<<<<<<<<<
		-- BTC, USDT, USDC, GOLD, LTC, ETH only 
--		1	BTC		Bitcoin 		2	LTC		Litecoin 		3	ETH		Ethereum
--		14	USDT	Tether USD 		15	GOLD	XBullion 		25	BTC		Bitcoin
--		26	LTC		Litecoin 		27	ETH		Ethereum 		30	USDT	Tether USD
--		33	USDC	USD Coin 		34	USDC	USD Coin 		35	GOLD	XBullion
		--AND a.product_id IN (16,50) -- <<<<<<<< run this for ZMT only 
		AND a.product_id IN (1, 2, 3, 14, 15, 25, 26, 27, 30, 33, 34, 35) -- <<<<<<<<<<<<========= run this for without ZMT  
		group by 1,2,3,4,5,6,7,8,9
		) a
	group by 1,2 
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
select d.*
	, coalesce (a.account_id_c,0) account_id_c
	, coalesce (a.usd_amount ,0) usd_amount 
-- coalesce (a.zmt_usd ,0) zmt_usd
-- coalesce (a.account_id_c_l1y ,0) account_id_c_l1y
-- coalesce (a.usd_amount_l1y ,0) usd_amount_l1y
from date_series d 
	left join asset_holding a on d.account_id = a.account_id and d.month = a.created_at 
	


----- total aum and zip staked by countries
with "date_series" as 
(	select DISTINCT
		date(DATE_TRUNC('month', date)) + INTERVAL '1 MONTH - 1 day' "month"
		,u.user_id
	FROM GENERATE_SERIES('2020-01-01'::DATE, NOW()::DATE, '1 month') "date"
	CROSS join (SELECT DISTINCT user_id FROM oms_data.user_app_public.zip_crew_stakes) u
	ORDER by 1 ASC
), zmt_staked as ( -- join with zip crew stake to calculate usd_amount
select 	d.MONTH, u.account_id
	, u.signup_hostcountry --d.user_id ,u.account_id --,u.zip_user_id ,u.email, 
	, SUM(s.amount) "zmt_staked_amount"
	, SUM(s.amount* c.price) "zmt_staked_usd_amount"
from date_series d
	LEFT join oms_data.user_app_public.zip_crew_stakes s
		ON d.user_id = s.user_id 
		AND d.month >= DATE_TRUNC('day', s.staked_at) AND d.month < COALESCE(DATE_TRUNC('day', s.released_at), NOW())
	--LEFT join oms_data.analytics.users_master u ON s.user_id = u.zip_user_id 
	LEFT JOIN oms_data.mysql_replica_apex.products p ON s.product_id = p.product_id
	-- join crypto usd prices
	LEFT JOIN oms_data.public.prices_eod_gmt0 c
		ON p.symbol = c.product_1_symbol
		AND c.product_2_symbol = 'USD' --AND d.date_ = DATE_TRUNC('day', c.actual_timestamp) 
		AND d.month = DATE_TRUNC('day', c.actual_timestamp)
		AND p."type" = 2 
	LEFT JOIN analytics.users_master u ON s.user_id = u.zip_user_id
WHERE u.account_id IS NOT null 
AND u.account_id NOT IN (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63312, 63313, 161347) 
AND u.signup_hostcountry IN ('AU','global','ID','TH')
GROUP BY 1,2,3
	), daily_user_balance AS (
	SELECT created_at, account_id , sum(usd_amount) as usd_amount, avg(price) as zmt_usd
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
		where a.created_at >='2020-01-01' and a.created_at <'2021-06-07 00:00:00' --<<<<<<<<CHANGE DATE HERE
		and a.account_id not in (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347)
		group by 1,2,3,4,5,6,7,8,9
		) a
	GROUP  BY  1,2
	ORDER  BY  1 DESC  
	),monthly_user_balance as (
	SELECT  date_trunc('month' , d.created_at) + INTERVAL '1 MONTH - 1 day' created_at
	, u.signup_hostcountry 
	, d.account_id 
	, sum(d.usd_amount) usd_amount
	, avg(d.zmt_usd) as zmt_usd
	FROM daily_user_balance d 
		LEFT JOIN analytics.users_master u ON d.account_id = u.account_id 
	WHERE d.created_at = date_trunc('month' , d.created_at) + INTERVAL '1 MONTH - 1 day'
	AND u.signup_hostcountry IN ('AU','global','ID','TH')
	GROUP BY 1,2,3
)--, aum_final as (
SELECT  
	COALESCE (b.month, a.created_at) AS  end_of_month
--,coalesce(a.account_id, b.account_id) as account_id
	,COALESCE(a.signup_hostcountry , b.signup_hostcountry) signup_hostcountry 
	,COALESCE(SUM(a.usd_amount),0) usd_amount
--	,COALESCE(a.zmt_usd,0) zmt_usd
	,COALESCE(SUM(b.zmt_staked_amount),0) zmt_staked_amount
	,COALESCE(SUM(b.zmt_staked_usd_amount),0) zmt_staked_usd_amount
--,coalesce(a.usd_amount,0) + coalesce(b.zmt_staked_usd_amount,0) aum_usd 
FROM monthly_user_balance a
	FULL OUTER JOIN  --this join zmt staked to user balance
		(SELECT  DISTINCT * FROM zmt_staked) b on b.month = a.created_at 
		AND b.account_id = a.account_id 
		AND b.signup_hostcountry = a.signup_hostcountry
--WHERE coalesce(a.account_id, b.account_id) = 143639 
GROUP BY 1,2