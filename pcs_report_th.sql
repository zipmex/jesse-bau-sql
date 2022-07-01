SELECT 
	w.created_at 
	, w.ap_account_id 
	, u.signup_hostcountry 
	, ziplock_amount 
	, CASE WHEN ziplock_amount >= 20000 AND ziplock_amount < 100000 THEN 'ZipCrew'
			WHEN ziplock_amount >= 100000 AND ziplock_amount < 150000 THEN 'ZipCrewVIP'
			WHEN ziplock_amount >= 150000 THEN 'ZipCrewVVIP'
			END AS zip_tier
FROM 
	analytics.wallets_balance_eod w
	LEFT JOIN analytics.users_master u
		ON w.ap_account_id = u.ap_account_id 
WHERE
	ziplock_amount >= 20000
	AND w.created_at = '2021-09-19 00:00:00'
	AND u.signup_hostcountry = 'TH'


WITH yearly_aum AS (
SELECT 
	a.ap_account_id 
	, COUNT(a.ap_account_id) account_id_count
	, SUM(
		(trade_wallet_amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate))
		+ (z_wallet_amount * COALESCE(c.average_high_low, g.mid_price, z.price))
		+ (ziplock_amount * COALESCE(c.average_high_low, z.price))
		) AS total_aum_usd_amount 
FROM 
	analytics.wallets_balance_eod a
	LEFT JOIN analytics.users_master u
		ON a.ap_account_id = u.ap_account_id 
	LEFT JOIN oms_data_public.cryptocurrency_prices c 
		ON ((CONCAT(a.symbol, 'USD') = c.instrument_symbol) OR (c.instrument_symbol = 'MIOTAUSD' AND a.symbol ='IOTA') OR (c.instrument_symbol = 'USDPUSD' AND a.symbol ='PAX'))
	    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', c.last_updated) + '1 day'::INTERVAL 
	LEFT JOIN public.daily_closing_gold_prices g 
		ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', g.created_at)
		AND a.symbol = 'GOLD'
	LEFT JOIN public.daily_ap_prices z
		ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', z.created_at)
		AND ((z.instrument_symbol = 'ZMTUSD' AND a.symbol = 'ZMT')
		OR (z.instrument_symbol = 'C8PUSDT' AND a.symbol = 'C8P'))
	LEFT JOIN oms_data_public.exchange_rates e
		ON date_trunc('day', e.created_at) = date_trunc('day', a.created_at)
		AND e.product_2_symbol  = a.symbol
		AND e."source" = 'coinmarketcap'
WHERE 
--	a.created_at = '2021-09-19 00:00:00'
	a.created_at >= DATE_TRUNC('month', NOW()) - '12 month'::INTERVAL -- START FROM LAST 12 months
	AND a.created_at < DATE_TRUNC('month', NOW())  -- till LAST month
	AND u.signup_hostcountry = 'TH'
GROUP BY 1
), yearly_trade_volume AS (
SELECT
--	DATE_TRUNC('year', t.created_at) created_at 
	ap_account_id 
	, SUM(amount_usd) trade_volume_usd
FROM 
	analytics.trades_master t
WHERE 
	t.created_at >= DATE_TRUNC('month', NOW()) - '12 month'::INTERVAL -- START FROM LAST 12 months
	AND t.created_at < DATE_TRUNC('month', NOW())  -- till LAST month
	AND t.signup_hostcountry = 'TH'
GROUP BY 1
), final_table AS (
SELECT 
	w.created_at 
	, w.ap_account_id 
	, u.signup_hostcountry 
	, w.symbol
	, CASE WHEN symbol = 'ZMT' THEN ziplock_amount END AS zmt_lock_amount
	, total_aum_usd_amount / account_id_count yearly_aum
	, trade_volume_usd 
FROM 
	analytics.wallets_balance_eod w
	LEFT JOIN analytics.users_master u
		ON w.ap_account_id = u.ap_account_id 
	LEFT JOIN yearly_aum a 
		ON w.ap_account_id = a.ap_account_id 
	LEFT JOIN yearly_trade_volume t 
		ON w.ap_account_id = t.ap_account_id 
WHERE
	--ziplock_amount >= 20000
	symbol = 'ZMT'
	AND w.created_at = '2021-09-19 00:00:00'
	AND u.signup_hostcountry = 'TH' 
)
SELECT 
	created_at 
	, ap_account_id 
	, signup_hostcountry
	, zmt_lock_amount 
	, CASE WHEN zmt_lock_amount >= 20000 AND zmt_lock_amount < 100000 THEN 3
			WHEN zmt_lock_amount >= 100000 AND zmt_lock_amount < 150000 THEN 4
			WHEN zmt_lock_amount >= 150000 THEN 5
			ELSE 1
			END AS zmt_stake_tier
	, yearly_aum
	, CASE WHEN yearly_aum >= 500000 AND yearly_aum < 2100000 THEN 3
			ELSE 1
			END AS yearly_aum_tier
	, trade_volume_usd
FROM final_table
;




