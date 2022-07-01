---- VIP cohort 
WITH tier_base AS (
	SELECT 
		DATE_TRUNC('month', created_at)::DATE created_at 
		, ap_account_id 
		, vip_tier 
		, COUNT(DISTINCT ap_account_id) user_count
	FROM 
		analytics.zmt_tier_endofmonth zte 
	WHERE vip_tier IN ('vip3')
	GROUP BY 1,2,3
)	, m0_base AS (
	SELECT 
		vip_tier 
		, ap_account_id 
		, MIN(created_at) m0 
	FROM tier_base 
	GROUP BY 1,2
)	, cohort_month AS (
	SELECT 
		tb.vip_tier 
		, tb.created_at 
		, tb.ap_account_id 
		, mb.m0 
		, CASE WHEN tb.created_at = mb.m0 THEN 'm0'
				WHEN tb.created_at = mb.m0 + '1 month'::INTERVAL THEN 'm1'
				WHEN tb.created_at = mb.m0 + '2 month'::INTERVAL THEN 'm2'
				WHEN tb.created_at = mb.m0 + '3 month'::INTERVAL THEN 'm3'
				WHEN tb.created_at = mb.m0 + '4 month'::INTERVAL THEN 'm4'
				WHEN tb.created_at = mb.m0 + '5 month'::INTERVAL THEN 'm5'
				WHEN tb.created_at = mb.m0 + '6 month'::INTERVAL THEN 'm6'
				WHEN tb.created_at = mb.m0 + '7 month'::INTERVAL THEN 'm7'
				WHEN tb.created_at = mb.m0 + '8 month'::INTERVAL THEN 'm8'
				WHEN tb.created_at = mb.m0 + '9 month'::INTERVAL THEN 'm9'
				WHEN tb.created_at = mb.m0 + '10 month'::INTERVAL THEN 'm10'
				WHEN tb.created_at = mb.m0 + '11 month'::INTERVAL THEN 'm11'
				WHEN tb.created_at = mb.m0 + '12 month'::INTERVAL THEN 'm12'
				WHEN tb.created_at = mb.m0 + '13 month'::INTERVAL THEN 'm13'
				WHEN tb.created_at = mb.m0 + '14 month'::INTERVAL THEN 'm14'
				WHEN tb.created_at = mb.m0 + '15 month'::INTERVAL THEN 'm15'
				END AS cohort_month
	FROM 
		tier_base tb 
		LEFT JOIN 
			m0_base mb 
			ON tb.ap_account_id = mb.ap_account_id 
)
SELECT 
	vip_tier 
	, m0 
	, cohort_month 
	, COUNT(DISTINCT ap_account_id) user_count
FROM cohort_month 
--WHERE m0 = '2021-02-01'
GROUP BY 1,2,3
;


-- vip avg trade - AUM
WITH tier_base AS (
	SELECT 
		DATE_TRUNC('month', created_at)::DATE created_at 
		, ap_account_id 
		, vip_tier 
	FROM 
		analytics.zmt_tier_endofmonth zte 
	WHERE 
		ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping um)
		AND signup_hostcountry IN ('AU','ID','global','TH')
--		AND vip_tier IN ('vip3','vip4')
--		ap_account_id = 143639
)	, tier_trade AS (
	SELECT 
		tb.*
		, SUM( COALESCE (tm.amount_usd, 0)) sum_trade_vol_usd
	FROM tier_base tb 
		LEFT JOIN 
			analytics.trades_master tm 
			ON tb.ap_account_id = tm.ap_account_id 
			AND tb.created_at = DATE_TRUNC('month', tm.created_at)
	GROUP BY 1,2,3
)	, aum_base AS (
	SELECT 
		tb.created_at::DATE
		, tb.ap_account_id 
		, a.symbol 
		, CASE	WHEN r.product_type = 1 THEN 
					( COALESCE(trade_wallet_amount, 0) + COALESCE(z_wallet_amount, 0)
					+ COALESCE(ziplock_amount, 0) + COALESCE(zlaunch_amount, 0)) * 1/r.price 
				WHEN r.product_type = 2 THEN 
					( COALESCE(trade_wallet_amount, 0) + COALESCE(z_wallet_amount, 0)
					+ COALESCE(ziplock_amount, 0) + COALESCE(zlaunch_amount, 0)) * r.price
				END AS total_usd_balance
	FROM 
		analytics.wallets_balance_eod a 
		RIGHT JOIN tier_base tb 
			ON a.ap_account_id = tb.ap_account_id 
			AND DATE_TRUNC('month', a.created_at)::DATE = tb.created_at 
	-- coin prices and exchange rates (USD)
		LEFT JOIN 
			analytics.rates_master r 
			ON a.symbol = r.product_1_symbol
			AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
	WHERE 
		a.created_at >= '2020-12-01' AND a.created_at < DATE_TRUNC('day', NOW())::DATE
	-- snapshot by end of month or yesterday
		AND (a.created_at = DATE_TRUNC('month', a.created_at) + '1 month - 1 day'::INTERVAL) --OR (a.created_at < '2022-04-01'))--DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
	-- exclude test products
		AND a.symbol NOT IN ('TST1','TST2')
	ORDER BY 1 DESC 
)	, sum_trade_aum AS (
	SELECT 
		tt.*
		, SUM( COALESCE (total_usd_balance, 0)) eom_usd_balance
	FROM tier_trade tt 
		LEFT JOIN aum_base a 
			ON tt.ap_account_id = a.ap_account_id 
			AND tt.created_at = a.created_at 
	GROUP BY 1,2,3,4
)
SELECT 
	created_at 
	, vip_tier 
	, COUNT(DISTINCT ap_account_id) user_count
	, SUM(sum_trade_vol_usd) sum_trade_vol_usd
--	, AVG(sum_trade_vol_usd) avg_trade_vol
	, SUM(eom_usd_balance) sum_eom_aum_usd
--	, AVG(eom_usd_balance) avg_aum_usd
FROM sum_trade_aum 
GROUP BY 1,2
;




