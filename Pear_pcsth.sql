WITH yearly_aum AS (
		SELECT 
			a.ap_account_id 
			, COUNT(a.ap_account_id) account_id_count
--			, SUM(
--				(trade_wallet_amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate))
--				+ (z_wallet_amount * COALESCE(c.average_high_low, g.mid_price, z.price))
--				+ (ziplock_amount * COALESCE(c.average_high_low, z.price))
--				) AS total_aum_usd_amount 
			, SUM(
				(CASE WHEN a.symbol = 'USD' THEN (COALESCE (trade_wallet_amount, 0) * 1)
				WHEN r.product_type = 1 THEN (COALESCE (trade_wallet_amount, 0) * 1/r.price)
				WHEN r.product_type = 2 THEN (COALESCE (trade_wallet_amount, 0) * r.price)
				END)
				+ (COALESCE(z_wallet_amount, 0) * r.price)
				+ (COALESCE(ziplock_amount, 0) * price)
				) AS total_aum_usd_amount 
		FROM 
			analytics.wallets_balance_eod a
			LEFT JOIN analytics.users_master u
				ON a.ap_account_id = u.ap_account_id 
			LEFT JOIN 
				data_team_staging.rates_master_staging r
				ON a.symbol = r.product_1_symbol 
				AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
		WHERE 
		--	a.created_at = '2021-09-19 00:00:00'
			a.created_at >= DATE_TRUNC('month', NOW()) - '12 month'::INTERVAL -- START FROM LAST 12 months
			AND a.created_at < DATE_TRUNC('month', NOW()) -- till LAST month
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
			AND t.created_at < DATE_TRUNC('month', NOW()) -- till LAST month
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
			AND w.created_at = '2021-09-30 00:00:00'
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
WHERE ap_account_id = 143639
;