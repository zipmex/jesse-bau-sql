-- monthly cohort by first aum month
WITH coin_base AS (
	SELECT 
		DISTINCT UPPER(SPLIT_PART(product_id,'.',1)) symbol
		, started_at effective_date
		, ended_at expired_date
	FROM zip_up_service_public.interest_rates
	ORDER BY 1
)	, zipup_coin AS (
	SELECT 
		DISTINCT
		symbol
		, (CASE WHEN effective_date < '2022-03-22' THEN '2018-01-01' ELSE effective_date END)::DATE AS effective_date
		, (CASE WHEN expired_date IS NULL THEN COALESCE( LEAD(effective_date) OVER(PARTITION BY symbol),'2999-12-31') ELSE expired_date END)::DATE AS expired_date
	FROM coin_base 
	ORDER BY 3,2
)	, base AS (
	SELECT 
		a.created_at 
		, CASE WHEN u.signup_hostcountry IS NULL THEN 'unknown' ELSE u.signup_hostcountry END AS signup_hostcountry
		, a.ap_account_id 
		, CASE WHEN a.created_at < '2021-11-01 00:00:00' THEN 
				(CASE WHEN a.ap_account_id IN (0, 3, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347, 317029, 11045)
					THEN TRUE ELSE FALSE END)			
				WHEN a.created_at < '2022-05-05' THEN  
				( CASE WHEN a.ap_account_id IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id NOT IN (496001))
					THEN TRUE ELSE FALSE END)
				ELSE
				( CASE WHEN a.ap_account_id IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id NOT IN (38121 ,496001))
					THEN TRUE ELSE FALSE END)
				END AS is_nominee 
		, CASE WHEN a.ap_account_id = 496001 THEN TRUE ELSE FALSE END AS is_asset_manager
		, a.symbol 
		, CASE WHEN a.symbol = 'ZMT' THEN TRUE WHEN zc.symbol IS NOT NULL THEN TRUE ELSE FALSE END AS zipup_coin 
		, CASE WHEN u.signup_hostcountry = 'TH' THEN
			(CASE WHEN a.created_at < '2022-05-08' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
			WHEN u.signup_hostcountry = 'ID' THEN
			(CASE WHEN a.created_at < '2022-07-04' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
			WHEN u.signup_hostcountry IN ('AU','global') THEN
			(CASE WHEN a.created_at < '2022-06-29' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
			END AS zipup_subscribed_at
		, trade_wallet_amount
		, z_wallet_amount
		, ziplock_amount
		, zlaunch_amount
		, COALESCE(c.average_high_low, g.mid_price, z1.price, 1/e.exchange_rate) usd_rate 
		, CASE 
				WHEN a.created_at < '2021-11-01' THEN
					(CASE WHEN a.symbol = 'USD' THEN trade_wallet_amount * 1
							ELSE trade_wallet_amount * COALESCE(c.average_high_low, g.mid_price, z1.price, 1/e.exchange_rate) END)
				ELSE 
					(CASE WHEN r.product_type = 1 THEN trade_wallet_amount * 1/r.price 
						WHEN r.product_type = 2 THEN trade_wallet_amount * r.price END)
				END AS trade_wallet_amount_usd
		, CASE 
				WHEN a.created_at <= '2021-09-15 00:00:00' THEN z_wallet_amount * COALESCE(c.average_high_low, g.mid_price, z.price) 
				WHEN a.created_at < '2021-11-01 00:00:00' THEN z_wallet_amount * COALESCE(c.average_high_low, g.mid_price, z1.price)
				ELSE z_wallet_amount * r.price 
				END AS z_wallet_amount_usd
		, CASE 
				WHEN a.created_at <= '2021-09-15 00:00:00' THEN ziplock_amount * COALESCE(c.average_high_low, g.mid_price, z.price) 
				WHEN a.created_at < '2021-11-01 00:00:00' THEN ziplock_amount * COALESCE(c.average_high_low, g.mid_price, z1.price) 
				ELSE ziplock_amount * r.price 
				END AS ziplock_amount_usd
		, zlaunch_amount * r.price zlaunch_amount_usd
	FROM 
		analytics.wallets_balance_eod a 
		LEFT JOIN 
			zipup_coin zc 
			ON a.symbol = zc.symbol
			AND a.created_at >= zc.effective_date
			AND a.created_at < zc.expired_date
		LEFT JOIN 
			analytics.users_master u 
			ON a.ap_account_id = u.ap_account_id 
		LEFT JOIN oms_data_public.cryptocurrency_prices c 
		    ON ((CONCAT(a.symbol, 'USD') = c.instrument_symbol) 
		    OR (c.instrument_symbol = 'MIOTAUSD' AND a.symbol ='IOTA') 
		    OR (c.instrument_symbol = 'USDPUSD' AND a.symbol ='PAX'))
		    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', c.last_updated) + '1 day'::INTERVAL 
		LEFT JOIN public.daily_closing_gold_prices g 
			ON ((DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', g.created_at)) 
			OR (DATE_TRUNC('day', a.created_at) BETWEEN '2021-07-31 00:00:00' AND '2021-08-01 00:00:00' AND DATE_TRUNC('day', g.created_at) = '2021-07-30 00:00:00'))
			AND a.symbol = 'GOLD'
		LEFT JOIN public.daily_ap_prices z
			ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', z.created_at) + '1 day'::INTERVAL
			AND ((z.instrument_symbol = 'ZMTUSD' AND a.symbol = 'ZMT')
			OR (z.instrument_symbol = 'C8PUSDT' AND a.symbol = 'C8P')
			OR (z.instrument_symbol = 'TOKUSD' AND a.symbol = 'TOK'))
		LEFT JOIN public.daily_ap_prices z1
			ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', z1.created_at)
			AND ((z1.instrument_symbol = 'ZMTUSD' AND a.symbol = 'ZMT')
			OR (z1.instrument_symbol = 'C8PUSDT' AND a.symbol = 'C8P')
			OR (z1.instrument_symbol = 'TOKUSD' AND a.symbol = 'TOK'))
		LEFT JOIN oms_data_public.exchange_rates e
			ON date_trunc('day', e.created_at) = date_trunc('day', a.created_at)
			AND e.product_2_symbol  = a.symbol
			AND e."source" = 'coinmarketcap'
		LEFT JOIN analytics.rates_master r
			ON a.created_at = r.created_at 
			AND a.symbol = r.product_1_symbol 
		LEFT JOIN 
			warehouse.zip_up_service_public.user_settings s
			ON u.user_id = s.user_id 
	WHERE 
		a.created_at >= '2021-01-01' AND a.created_at < '2021-07-01' -- DATE_TRUNC('month', NOW()) 
		AND u.signup_hostcountry IN ('TH','ID','AU','global')
		AND ((a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
		AND a.symbol NOT IN ('TST1','TST2')
	ORDER BY 1 DESC 
)	, aum_snapshot AS (
	SELECT 
		DATE_TRUNC('month', created_at)::DATE monthly_balance 
		, signup_hostcountry
		, ap_account_id 
	--	, symbol 
		, CASE	WHEN symbol <> 'ZMT' AND zipup_coin = TRUE THEN 'zipup_coin' 
				WHEN symbol IN ('ZMT') THEN 'ZMT'
				ELSE 'other' 
				END AS asset_group
--		, SUM( CASE WHEN created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
--						WHEN created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0)
--						END) AS zipup_usd_amount
		, SUM(COALESCE (zlaunch_amount_usd, 0)) zlaunch_amount_usd
		, SUM(COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
		, SUM( COALESCE (CASE WHEN zipup_subscribed_at IS NOT NULL AND created_at >= DATE_TRUNC('day', zipup_subscribed_at) AND zipup_coin = TRUE
					THEN
						(CASE 	WHEN created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
								WHEN created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
					END, 0)) AS zipup_usd_amount
		, SUM( COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0) 
			+ COALESCE (ziplock_amount_usd, 0) + COALESCE (zlaunch_amount_usd, 0)) total_usd_amount
	FROM 
		base 
	WHERE 
		is_asset_manager = FALSE AND is_nominee = FALSE 
	GROUP BY 
		1,2,3,4
	ORDER BY
		1 
)	, aum_zlaunch AS (
	SELECT 
		monthly_balance
		, ap_account_id
		, signup_hostcountry
		, asset_group
		, SUM(COALESCE (zipup_usd_amount, 0) + COALESCE (ziplock_amount_usd, 0) + COALESCE (zlaunch_amount_usd, 0)) total_zipup_usd
		, SUM(COALESCE (total_usd_amount, 0)) total_usd_amount
	FROM 
		aum_snapshot a 
	GROUP BY 1,2,3,4
)	, hourly_accumulated_balances AS (
	SELECT *
	FROM (
		SELECT * , date_trunc('day', created_at) AS thour
		, ROW_NUMBER() OVER(PARTITION BY user_id, product_id , date_trunc('day', created_at) ORDER BY created_at DESC) AS r
		FROM zipmex_otc_prod_public.accumulated_balances
		) t
	WHERE t.r = 1
	AND date_trunc('day', thour) < '2021-08-25'
)	, plaung_aum AS (
	SELECT
		thour, user_id, UPPER(h.product_id) symbol , h.balance, h.created_at, h.id
	--	, h.balance * c.average_high_low usd_amount
		, CASE WHEN UPPER(h.product_id) = 'IDR' THEN h.balance * 1/e.exchange_rate ELSE h.balance * c.average_high_low END usd_amount
		, ROW_NUMBER() OVER(PARTITION BY user_id, UPPER(h.product_id), DATE_TRUNC('month', thour) ORDER BY thour DESC) rank_ 
	FROM 
		hourly_accumulated_balances h 
		LEFT JOIN 
			oms_data_public.cryptocurrency_prices c 
		    ON CONCAT(UPPER(h.product_id), 'USD') = c.instrument_symbol
		    AND DATE_TRUNC('day', thour) = DATE_TRUNC('day', c.last_updated) + '1 day'::INTERVAL 
		LEFT JOIN oms_data_public.exchange_rates e
			ON date_trunc('day', e.created_at) = DATE_TRUNC('day', thour)
			AND e.product_2_symbol  = UPPER(h.product_id)
			AND e."source" = 'coinmarketcap'
	WHERE
		user_id = '01F14GTKR63YS7QSPGCQDNVJRR'
	--	AND extract(day from thour) = 23
	ORDER BY thour DESC, user_id, product_id
)	, pluang_snapshot AS (
	SELECT 
		DATE_TRUNC('month', thour)::DATE monthly_balance 
		, 111111111 ap_account_id 
		, 'ID' signup_hostcountry	
		, CASE	WHEN symbol IN ('BTC','ETH','USDT','USDC','GOLD','LTC') THEN 'zipup_coin'
				WHEN symbol IN ('ZMT') THEN 'ZMT'
				ELSE 'other' 
				END AS asset_group
		, SUM( CASE WHEN symbol <> 'IDR' THEN COALESCE (usd_amount, 0) END) total_zipup_usd 
		, SUM( COALESCE (usd_amount, 0)) total_usd_amount 
	FROM plaung_aum 
	WHERE rank_ = 1 
	GROUP BY 1,2,3,4
)	, total_aum_snapshot AS (
	SELECT * FROM aum_zlaunch
	UNION ALL
	SELECT * FROM pluang_snapshot
)	, first_aum_month AS (
	SELECT
		ap_account_id 
		, DATE_TRUNC('month', MIN(monthly_balance))::DATE first_aum_month
	FROM total_aum_snapshot
	GROUP BY 1
)	, final_table AS (
	SELECT 
		t.*
		, f.first_aum_month
	FROM total_aum_snapshot t
		LEFT JOIN first_aum_month f 
			ON t.ap_account_id = f.ap_account_id
)		
SELECT 
--	monthly_balance
	ap_account_id 
	, first_aum_month
	, CASE WHEN DATE_TRUNC('month', monthly_balance)::DATE = first_aum_month THEN 'm0'
			WHEN DATE_TRUNC('month', monthly_balance)::DATE = first_aum_month + '1 month'::INTERVAL THEN 'm1'
			WHEN DATE_TRUNC('month', monthly_balance)::DATE = first_aum_month + '2 month'::INTERVAL THEN 'm2'
			WHEN DATE_TRUNC('month', monthly_balance)::DATE = first_aum_month + '3 month'::INTERVAL THEN 'm3'
			WHEN DATE_TRUNC('month', monthly_balance)::DATE = first_aum_month + '4 month'::INTERVAL THEN 'm4'
			WHEN DATE_TRUNC('month', monthly_balance)::DATE = first_aum_month + '5 month'::INTERVAL THEN 'm5'
			WHEN DATE_TRUNC('month', monthly_balance)::DATE = first_aum_month + '6 month'::INTERVAL THEN 'm6'
			WHEN DATE_TRUNC('month', monthly_balance)::DATE = first_aum_month + '7 month'::INTERVAL THEN 'm7'
			WHEN DATE_TRUNC('month', monthly_balance)::DATE = first_aum_month + '8 month'::INTERVAL THEN 'm8'
			WHEN DATE_TRUNC('month', monthly_balance)::DATE = first_aum_month + '9 month'::INTERVAL THEN 'm9'
			WHEN DATE_TRUNC('month', monthly_balance)::DATE = first_aum_month + '10 month'::INTERVAL THEN 'm10'
			WHEN DATE_TRUNC('month', monthly_balance)::DATE = first_aum_month + '11 month'::INTERVAL THEN 'm11'
			WHEN DATE_TRUNC('month', monthly_balance)::DATE = first_aum_month + '12 month'::INTERVAL THEN 'm12'
			WHEN DATE_TRUNC('month', monthly_balance)::DATE = first_aum_month + '13 month'::INTERVAL THEN 'm13'
			WHEN DATE_TRUNC('month', monthly_balance)::DATE = first_aum_month + '14 month'::INTERVAL THEN 'm14'
			WHEN DATE_TRUNC('month', monthly_balance)::DATE = first_aum_month + '15 month'::INTERVAL THEN 'm15'
		END AS cohort_group
	, asset_group
	, SUM(total_zipup_usd) total_zipup_usd
	, SUM(total_usd_amount) total_usd_amount
FROM final_table
GROUP BY 1,2,3,4
;



-- using dm_cohort_aum
WITH min_aum AS (
	SELECT 
		ap_account_id 
		, MIN(monthly_balance) first_aum_month
	FROM bo_testing.dm_cohort_aum dca 
	GROUP BY 1
)	, cohort_group AS (
	SELECT 
		dca2.ap_account_id 
		, dca2.asset_group 
		, dca2.total_zipup_usd 
		, dca2.total_usd_amount 
		, m.first_aum_month
		, CASE WHEN dca2.monthly_balance = m.first_aum_month THEN 'm0'
				WHEN dca2.monthly_balance = m.first_aum_month + '1 month'::INTERVAL THEN 'm1'
				WHEN dca2.monthly_balance = m.first_aum_month + '2 month'::INTERVAL THEN 'm2'
				WHEN dca2.monthly_balance = m.first_aum_month + '3 month'::INTERVAL THEN 'm3'
				WHEN dca2.monthly_balance = m.first_aum_month + '4 month'::INTERVAL THEN 'm4'
				WHEN dca2.monthly_balance = m.first_aum_month + '5 month'::INTERVAL THEN 'm5'
				WHEN dca2.monthly_balance = m.first_aum_month + '6 month'::INTERVAL THEN 'm6'
				WHEN dca2.monthly_balance = m.first_aum_month + '7 month'::INTERVAL THEN 'm7'
				WHEN dca2.monthly_balance = m.first_aum_month + '8 month'::INTERVAL THEN 'm8'
				WHEN dca2.monthly_balance = m.first_aum_month + '9 month'::INTERVAL THEN 'm9'
				WHEN dca2.monthly_balance = m.first_aum_month + '10 month'::INTERVAL THEN 'm10'
				WHEN dca2.monthly_balance = m.first_aum_month + '11 month'::INTERVAL THEN 'm11'
				WHEN dca2.monthly_balance = m.first_aum_month + '12 month'::INTERVAL THEN 'm12'
				WHEN dca2.monthly_balance = m.first_aum_month + '13 month'::INTERVAL THEN 'm13'
				WHEN dca2.monthly_balance = m.first_aum_month + '14 month'::INTERVAL THEN 'm14'
				WHEN dca2.monthly_balance = m.first_aum_month + '15 month'::INTERVAL THEN 'm15'
				END AS cohort_group
	FROM 
		bo_testing.dm_cohort_aum dca2 
		LEFT JOIN 
			min_aum m 
			ON dca2.ap_account_id = m.ap_account_id
)
SELECT 
	first_aum_month
	, cohort_group
	, asset_group
	, SUM(total_zipup_usd) total_zipup_usd
	, SUM(total_usd_amount) total_usd_amount
FROM cohort_group
GROUP BY 1,2,3
;

WITH base AS (
SELECT 
	dca.monthly_balance 
	, dca.ap_account_id
	, um.signup_hostcountry 
	, dca.asset_group 
	, dca.total_zipup_usd 
	, dca.total_usd_amount 
FROM 
	bo_testing.dm_cohort_aum dca 
	LEFT JOIN 
		analytics.users_master um 
		ON dca.ap_account_id = um.ap_account_id 
)
SELECT 
	monthly_balance 
	, signup_hostcountry
	, asset_group 
	, SUM(total_zipup_usd) total_zipup_usd
	, SUM(total_usd_amount) total_usd_amount
FROM base 
WHERE 
	monthly_balance >= '2021-11-01'
GROUP BY 1,2,3