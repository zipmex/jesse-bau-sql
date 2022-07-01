---- daily user acquisition
with temp_ as (
	select 
	u.signup_hostcountry ,u.user_id ,u.ap_user_id,u.ap_account_id 
	,u.created_at as register_date
	,u.onfido_completed_at as kyc_date 
	,u.zipup_subscribed_at AS zipup_date 
	,u.is_verified 
	,u.level_increase_status 
	FROM analytics.users_master u
	where signup_hostcountry IN ('TH','ID','AU','global')
	and created_at > '2018-01-01 00:00:00' 
),temp_m as (
	select signup_hostcountry, date_trunc('day', register_date) as register_month
	,COUNT(distinct user_id) as user_id_c
	from temp_
	group by 1, 2
),temp_kyc as (
	SELECT signup_hostcountry, date_trunc('day', kyc_date) as kyc_month
	,COUNT(DISTINCT CASE WHEN kyc_date IS NOT NULL AND is_verified = TRUE THEN user_id END) AS user_id_kyc_new 
---> this one count the status by kyc date, number is fixed level_increase_status = 'pass'
	FROM temp_
	GROUP BY 1,2
),temp_zipup as (
	SELECT signup_hostcountry, date_trunc('day', zipup_date) as zipup_month
	,COUNT(DISTINCT CASE WHEN zipup_date IS NOT NULL THEN user_id END) AS user_zipup_new 
	FROM temp_
	GROUP BY 1,2
), final_temp AS (
select b.signup_hostcountry, b.register_month
	, COALESCE(b.user_id_c,0) user_id_c
	, COALESCE(user_id_kyc_new,0) user_id_kyc_new 
	, COALESCE(user_zipup_new,0) user_zipup_new 
from temp_m b
	left join temp_kyc k on k.signup_hostcountry = b.signup_hostcountry and k.kyc_month = b.register_month 
	left join temp_zipup z on z.signup_hostcountry = b.signup_hostcountry and z.zipup_month = b.register_month 
order by 1,2
)
SELECT *
	,sum(user_id_c) over (partition by signup_hostcountry order by register_month ) as total_registered_user
	,sum(user_id_kyc_new) over (partition by signup_hostcountry order by register_month) as total_kyc_user
	,sum(user_zipup_new) over (partition by signup_hostcountry order by register_month) as total_zipup_user
FROM final_temp 
;


----- trade vol by country
WITH pluang_trade_all AS (
	SELECT 
		DATE_TRUNC('day', q.created_at) created_at 
		, 'ID' signup_hostcountry
		, q.user_id
		, UPPER(LEFT(SPLIT_PART(q.instrument_id,'.',1),3)) product_1_symbol  
		, q.quote_id
		, q.order_id
		, q.side
		, CASE WHEN q.side IS NOT NULL THEN TRUE ELSE FALSE END AS is_organic_trade
		, UPPER(SPLIT_PART(q.instrument_id,'.',1)) instrument_symbol 
		, UPPER(RIGHT(SPLIT_PART(q.instrument_id,'.',1),3)) product_2_symbol 
		, q.quoted_quantity 
		, q.quoted_price 
		, SUM(q.quoted_quantity) "quantity"
		, SUM(q.quoted_value) "amount_idr"
		, SUM(q.quoted_value * 1/e.exchange_rate) amount_usd
	FROM 
	-- pluang trade volume from zipmex_otc_public
		zipmex_otc_public.quote_statuses q
		LEFT JOIN 
			oms_data_public.exchange_rates e
			ON DATE_TRUNC('day', e.created_at) = DATE_TRUNC('day', q.created_at)
			AND UPPER(RIGHT(SPLIT_PART(q.instrument_id,'.',1),3))  = e.product_2_symbol
			AND e."source" = 'coinmarketcap'
	WHERE
	-- only completed transaction
		q.status='completed'
	-- pluang user_id
		AND q.user_id IN ('01F14GTKR63YS7QSPGCQDNVJRR')
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
	ORDER BY 1 DESC 
)	, pluang_trade AS (
	SELECT 
		DATE_TRUNC('day', created_at) created_at 
		, signup_hostcountry
	-- using INT value for UNION syntax
		, 0101 ap_account_id 
	-- distinguish pluang and zipmex
		, 'pluang' user_type
		, product_1_symbol
		, side 
		, is_organic_trade 
		, CASE WHEN product_1_symbol = 'ZMT' THEN TRUE ELSE FALSE END AS is_zmt_trade
		, COUNT(DISTINCT order_id) count_orders
		, COUNT(DISTINCT quote_id) count_trades 
		, SUM(quantity) quantity 
		, SUM(amount_usd) amount_usd
	FROM 
		pluang_trade_all
	GROUP BY 1,2,3,4,5,6,7,8
)	, zipmex_trade AS (
	SELECT
		DATE_TRUNC('day', t.created_at) created_at 
		, t.signup_hostcountry 
		, t.ap_account_id 
		-- distinguish pluang and zipmex
		, 'zipmex' user_type
		, t.product_1_symbol
		, t.side 
		, CASE WHEN t.counter_party IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping) THEN FALSE ELSE TRUE END "is_organic_trade"
		, CASE WHEN product_1_id IN (16,50) THEN TRUE ELSE FALSE END AS is_zmt_trade
		, COUNT(DISTINCT t.order_id) "count_orders"
		, COUNT(DISTINCT t.trade_id) "count_trades"
		, SUM(t.quantity) "sum_coin_volume"
		, SUM(t.amount_usd) "sum_usd_trade_volume" 
	FROM 
		analytics.trades_master t
		LEFT JOIN analytics.users_master u
			ON t.ap_account_id = u.ap_account_id
	WHERE 
		t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
		AND t.signup_hostcountry IN ('TH','ID','AU','global')
		AND DATE_TRUNC('day', t.created_at) >= DATE_TRUNC('year', NOW()) 
	GROUP BY 
		1,2,3,4,5,6,7,8
	ORDER BY 1,2,3
)	, all_trade AS (
	SELECT * FROM zipmex_trade
	UNION ALL
	SELECT * FROM pluang_trade
)
SELECT 
	DATE_TRUNC('day', a.created_at) created_at 
	, a.signup_hostcountry 
	, CASE WHEN product_1_symbol = 'ZMT' THEN 'zmt' ELSE 'non-zmt' END AS is_zmt
	, COUNT(DISTINCT ap_account_id) count_traders
	, SUM( COALESCE(sum_coin_volume, 0)) sum_coin_volume 
	, SUM( COALESCE(sum_usd_trade_volume, 0)) sum_usd_trade_volume
FROM 
	all_trade a 
WHERE 
	DATE_TRUNC('day', a.created_at) >= DATE_TRUNC('year', NOW()) 
GROUP BY 
	1,2,3
;


-- trade wallet aum
SELECT 
	DATE_TRUNC('day',a.created_at)::DATE AS created_at 
	, CASE WHEN u.signup_hostcountry IS NULL THEN 'unknown' ELSE u.signup_hostcountry END AS signup_hostcountry
	, CASE WHEN a.account_id IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id <> 496001) 
	THEN TRUE ELSE FALSE END AS is_nominee
	, CASE WHEN a.account_id = 496001 THEN TRUE ELSE FALSE END AS is_asset_manager -- this account holds z_wallet balance
	, p.symbol
	, CASE WHEN u.is_zipup_subscribed = TRUE AND DATE_TRUNC('day',a.created_at) >= DATE_TRUNC('day', u.zipup_subscribed_at)
			AND a.product_id IN (1, 2, 3, 14, 15, 25, 26, 27, 30, 33, 34, 35, 16, 50) 
			THEN TRUE ELSE FALSE END AS is_zipup_amount
	, SUM(amount) amount 
	, SUM( CASE  
			WHEN r.product_type = 1 THEN amount * 1/r.price 
			WHEN r.product_type = 2 THEN amount * r.price 
			END) AS amount_usd
FROM oms_data_public.accounts_positions_daily a
	LEFT JOIN analytics.users_master u 
		ON a.account_id = u.ap_account_id  
	LEFT JOIN apex.products p
		ON a.product_id = p.product_id
	LEFT JOIN 
		analytics.rates_master r 
		ON p.symbol = r.product_1_symbol 
	    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
WHERE
	DATE_TRUNC('day',a.created_at) >= DATE_TRUNC('month', NOW()) -- CHANGE DATE HERE
GROUP BY 1,2,3,4,5,6
;


-- z wallet aum
WITH base AS (
	SELECT 
		a.created_at
		, CASE WHEN u.signup_hostcountry IS NULL THEN 'unknown' ELSE u.signup_hostcountry END AS signup_hostcountry
		, a.ap_account_id 
	-- filter nominee accounts from users_mapping
		, CASE WHEN a.ap_account_id IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id <> 496001)
				THEN TRUE ELSE FALSE END AS is_nominee 
	-- filter asset_manager account
		, CASE WHEN a.ap_account_id = 496001 THEN TRUE ELSE FALSE END AS is_asset_manager
		, a.symbol 
	-- zipup subscribe status to identify zipup amount
		, u.zipup_subscribed_at , u.is_zipup_subscribed 
		, trade_wallet_amount
		, z_wallet_amount
		, ziplock_amount
		, r.price usd_rate 
		, CASE WHEN r.product_type = 1 THEN trade_wallet_amount * 1/r.price 
				WHEN r.product_type = 2 THEN trade_wallet_amount * r.price
				END AS trade_wallet_amount_usd
		, z_wallet_amount * r.price z_wallet_amount_usd
		, ziplock_amount * r.price ziplock_amount_usd
	FROM 
		analytics.wallets_balance_eod a 
	-- get country
		LEFT JOIN 
			analytics.users_master u 
			ON a.ap_account_id = u.ap_account_id 
		LEFT JOIN 
			analytics.rates_master r 
			ON a.symbol = r.product_1_symbol 
		    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
	WHERE 
		a.created_at >= DATE_TRUNC('year', NOW()) AND a.created_at < DATE_TRUNC('day', NOW()) 
		AND u.signup_hostcountry IN ('TH','ID','AU','global')
	-- exclude test products
		AND a.symbol NOT IN ('TST1','TST2')
	ORDER BY 1 DESC 
)
SELECT 
	DATE_TRUNC('day', created_at) created_at 
	, signup_hostcountry
	, symbol 
	, CASE WHEN symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH') THEN 'zipup_coin'
			WHEN symbol IN ('ZMT') THEN 'ZMT'
			ELSE 'non_zipup' END AS asset_group
	, CASE WHEN is_zipup_subscribed = TRUE AND created_at >= DATE_TRUNC('day', zipup_subscribed_at)
			AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT') THEN TRUE 
			ELSE FALSE END AS is_zipup_amount
	, SUM( COALESCE (trade_wallet_amount, 0)) trade_wallet_amount
	, SUM( COALESCE (z_wallet_amount, 0)) z_wallet_amount
	, SUM( COALESCE (ziplock_amount, 0)) ziplock_amount
	, SUM( COALESCE (trade_wallet_amount_usd, 0)) trade_wallet_amount_usd
	, SUM( COALESCE (z_wallet_amount_usd, 0)) z_wallet_amount_usd
	, SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
FROM 
	base 
WHERE
	is_asset_manager = FALSE AND is_nominee = FALSE
GROUP BY 
	1,2,3,4,5
ORDER BY 
	1 DESC 
;