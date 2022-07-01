
WITH aum_table AS (
SELECT date_trunc('day',a.created_at) datadate
	, signup_hostcountry 
	, account_id 
	, symbol 
	, SUM(amount) quantity 
	, SUM(usd_amount) as usd_amount 
FROM (
	SELECT date_trunc('day',a.created_at) AS created_at ,a.account_id , a.product_id, p.symbol, u.signup_hostcountry 
		, c.average_high_low , g.mid_price , z.price, 1/e.exchange_rate as exchange_rate 
		,SUM(amount) amount 
		,SUM(a.amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate)) usd_amount
	FROM oms_data.public.accounts_positions_daily a
		LEFT JOIN analytics.users_master u on a.account_id = u.account_id 
		LEFT JOIN oms_data.mysql_replica_apex.products p
			ON a.product_id = p.product_id
		LEFT JOIN oms_data.public.cryptocurrency_prices c 
		    ON CONCAT(p.symbol, 'USD') = c.instrument_symbol
		    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', c.last_updated)
		LEFT JOIN oms_data.public.daily_closing_gold_prices g
			ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', g.created_at)
			AND a.product_id IN (15,	 35)
		LEFT JOIN oms_data.public.daily_ap_prices z
			ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', z.created_at)
			AND z.instrument_symbol  = 'ZMTUSD'
			AND a.product_id in (16, 50)
		LEFT JOIN public.exchange_rates e
			ON date_trunc('day', e.created_at) = date_trunc('day', a.created_at)
			AND e.product_2_symbol  = p.symbol
			AND e.source = 'coinmarketcap'
	WHERE a.created_at >= '2021-07-12 00:00:00' AND a.created_at < '2021-07-13 00:00:00' --<<<<<<<<CHANGE DATE HERE
	AND u.signup_hostcountry  NOT IN ('test', 'error','xbullion') 
	AND u.is_zipup_subscribed = TRUE  -- Total Users with ZipUp assets <<<<<<<<<<<<<<<<<<<<<<<<< 
	AND a.created_at >= u.zipup_subscribed_at -- AUM balance starting after subcribed to zipup
	AND a.account_id NOT IN (0, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347) 
	AND a.product_id IN (1, 2, 3, 14, 25, 26, 27, 30, 33, 34, 16, 50) -- BTC, USDT, USDC, LTC, ETH, ZMT 
		/*
		1	BTC		Bitcoin 		2	LTC		Litecoin 		3	ETH		Ethereum
		14	USDT	Tether USD 		15	GOLD	XBullion 		25	BTC		Bitcoin
		26	LTC		Litecoin 		27	ETH		Ethereum 		30	USDT	Tether USD
		33	USDC	USD Coin 		34	USDC	USD Coin 		35	GOLD	XBullion
		 */
	GROUP BY 1,2,3,4,5,6,7,8,9 
	) a
GROUP BY 1,2,3,4
ORDER BY 1 DESC  
), user_table AS (
select 
	u.user_id,
	u.zip_user_id,
	u.account_id,
	u.signup_hostcountry,
	DATE_TRUNC('day',u.created_at) as register_date,
	DATE_TRUNC('day',u.onfido_completed_at) as kyc_date,
	DATE_TRUNC('day',u.zipup_subscribed_at) as zipup_subscribed_date,
	DATE_TRUNC('day',b.is_verified_at) as bank_book_date
from 
	analytics.users_master u
LEFT JOIN
	user_app_public.bank_accounts b
	ON u.zip_user_id = b.user_id
WHERE
	signup_hostcountry not in ('test', 'error', 'xbullion')
	AND zipup_subscribed_at BETWEEN '2021-07-12 00:00:00' and now()
	and account_id not in ('15','221','634','746','1002','1182','1202','1272','1708','6074','6828','11284','16293','19763','24108','24315','25431','37276','38526',
'39858','40119','40438','40890','48300','51313','51333','52266','54172','54231','54644','55224','55660','57262','58998','59049','59693',
'62663','63292','63314','63914','66402','67813','82129','84431','84461','84799','91297','92285','93791','94663','94993','96434','96535',
'101654','101786','103488','103855','104832','106308','108014','127491','128405','131484','139503','141711','146194','146356','147984',
'157600','159685','161863','180376','183004')
)
SELECT u.*
	, a.symbol 
	, a.usd_amount 
FROM user_table u 
	LEFT JOIN aum_table a 
	ON u.account_id = a.account_id 
ORDER BY 1,2,3,4,5 