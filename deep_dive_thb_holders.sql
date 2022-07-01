SELECT * FROM warehouse.bo_testing.thb_holder_20220621
;


WITH base_100 AS (
	SELECT 
		*
		, RANK() OVER(ORDER BY trade_wallet_amount DESC) rank_
	FROM bo_testing.thb_holder_20220621 th 
)
SELECT 
	DATE_TRUNC('month', d.created_at)::DATE transactions_month
	, th.rank_
	, th.email
	, th.vip_tier 
	, th.signup_hostcountry 
	, d.product_type
	, th.trade_wallet_amount_usd current_thb_balance_in_usd
	, SUM(sum_usd_deposit_amount) sum_usd_deposit_amount 
	, SUM(usd_net_buy_amount) usd_net_buy_amount 
	, SUM(sum_usd_withdraw_amount) sum_usd_withdraw_amount 
FROM reportings_data.dm_user_transactions_dwt_daily d
	RIGHT JOIN base_100 th 
		ON th.ap_account_id = d.ap_account_id 
		AND th.rank_ < 101
WHERE
	d.created_at >= '2021-10-01'
--	AND th.email = 'cnaviroj@gmail.com'
GROUP BY 1,2,3,4,5,6,7
ORDER BY 7 DESC, 1
;





WITH
	var_account_id AS (VALUES (819548)),
	var_country AS (VALUES ('TH'),('AU'),('ID'),('global')),
	var_date_position AS (VALUES ('2020-01-01'::date))
	, 
query AS (
	WITH deposit AS (
		SELECT 
			DATE_TRUNC('day', d.created_at) created_at
			, d.ap_account_id 
			, SUM(d.amount_usd) dep_amount_usd
			, 0.0 with_amount_usd
			, 0.0 balance_amount_usd
		FROM
			warehouse.analytics.deposit_tickets_master d
		RIGHT JOIN (
					SELECT 
						*
						, RANK() OVER(ORDER BY trade_wallet_amount DESC) rank_
					FROM bo_testing.thb_holder_20220621 th 
					) th 
				ON d.ap_account_id = th.ap_account_id 
				AND th.rank_ < 101
		WHERE 
			d.status = 'FullyProcessed'
			AND d.signup_hostcountry IN (TABLE var_country)
			AND d.created_at::DATE >= (TABLE var_date_position)
		GROUP BY
			1,2,4,5
	)
	-- to isolate all withdrawals (crypto and fiat)
	, withdraw AS (
		SELECT 
			DATE_TRUNC('day', w.created_at) created_at
			, w.ap_account_id 
			, 0.0 dep_amount_usd
			, SUM(w.amount_usd) with_amount_usd
			, 0.0 balance_amount_usd
		FROM
			warehouse.analytics.withdraw_tickets_master w
		RIGHT JOIN (
					SELECT 
						*
						, RANK() OVER(ORDER BY trade_wallet_amount DESC) rank_
					FROM bo_testing.thb_holder_20220621 th 
					) th 
				ON w.ap_account_id = th.ap_account_id 
				AND th.rank_ < 101
		WHERE 
			w.status = 'FullyProcessed'
			AND w.signup_hostcountry IN (TABLE var_country)
			AND w.created_at::DATE >= (TABLE var_date_position)
		GROUP BY
			1,2,3,5
	)
	-- to get crypto and fiat value balance at end of day
	, balance AS (
		SELECT 
			b.created_at created_at
			, b.ap_account_id 
			, 0.0 dep_amount_usd
			, 0.0 with_amount_usd
			-- for crypto amounts
			, SUM(CASE WHEN r.product_type = 2 THEN
						(COALESCE(b.trade_wallet_amount, 0) 
						+ COALESCE(b.z_wallet_amount, 0) 
						+ COALESCE(b.ziplock_amount, 0)
						+ COALESCE(b.zlaunch_amount, 0) ) * r.price
				-- for fiat amounts
					WHEN r.product_type = 1 THEN
						(COALESCE(b.trade_wallet_amount, 0) 
						+ COALESCE(b.z_wallet_amount, 0) 
						+ COALESCE(b.ziplock_amount, 0)
						+ COALESCE(b.zlaunch_amount, 0) ) * 1 / r.price
					END) AS balance_amount_usd
		FROM 
			warehouse.analytics.wallets_balance_eod b
		LEFT JOIN
			-- to crypto/fiat prices
			warehouse.analytics.rates_master r
			-- to link date
			ON r.created_at = b.created_at 
			-- to link cypto/fiat
			AND r.product_1_symbol = b.symbol
		RIGHT JOIN (
					SELECT 
						*
						, RANK() OVER(ORDER BY trade_wallet_amount DESC) rank_
					FROM bo_testing.thb_holder_20220621 th 
					) th 
				ON b.ap_account_id = th.ap_account_id 
				AND th.rank_ < 101
		WHERE 
			b.created_at::DATE >= (TABLE var_date_position)
		GROUP BY
			1,2,3,4
	)
	-- to combine all table values together in the same date, user and crypto row
	, union_table AS ( 
		SELECT * FROM deposit
		UNION ALL
		SELECT * FROM withdraw
		UNION ALL
		SELECT * FROM balance
	)
	-- to sum all lines with the same date, user and crypto
	, raw_table AS (
		SELECT
			u.created_at
			, u.ap_account_id
			, up.email 
			, SUM(u.dep_amount_usd) dep_amount_usd
			, SUM(u.with_amount_usd) with_amount_usd
			, SUM(u.balance_amount_usd) balance_amount_usd
		FROM
			union_table u
			LEFT JOIN analytics_pii.users_pii up 
				ON u.ap_account_id = up.ap_account_id 
		WHERE
			-- accounts to be excluded in all reports
			u.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping um)
--			AND u.ap_account_id = (TABLE var_account_id)
		GROUP BY
			1,2,3
	)
	-- to get cumulative values for all columns
	, total_table AS (
		SELECT 
			*
			, SUM(r.dep_amount_usd)
				OVER(PARTITION BY r.ap_account_id
					ORDER BY r.created_at) AS total_dep_amount_usd
			, SUM(r.with_amount_usd)
				OVER(PARTITION BY r.ap_account_id
					ORDER BY r.created_at) AS total_with_amount_usd
		FROM
			raw_table r
		GROUP BY 
			1,2,3,4,5,6
	)
	-- to convert values into financial terms
	, processed_table AS (
		SELECT
			t.created_at::DATE 
			, t.email
			, u.signup_hostcountry
			, t.total_with_amount_usd total_realised_revenue
			, COALESCE(t.balance_amount_usd, 0) total_unrealised_revenue
			, t.total_dep_amount_usd total_cost_incurred
		FROM
			total_table t
		LEFT JOIN
			warehouse.analytics.users_master u
			ON u.ap_account_id = t.ap_account_id
		WHERE
			u.signup_hostcountry IN ('TH', 'ID', 'AU', 'global')
	)
	-- to calculate user profit
	, revenue_table AS (
		SELECT
			*
			, p.total_realised_revenue + p.total_unrealised_revenue - p.total_cost_incurred total_profit
		FROM 
			processed_table p
	)
	-- to calculate user return on investment (profit over total cost incurred)
	SELECT
		*
		, CASE WHEN r.total_cost_incurred = 0 THEN 0 ELSE (r.total_profit) / r.total_cost_incurred END roi
	FROM
		revenue_table r
	ORDER BY 1 DESC
	)
SELECT * FROM query 
WHERE created_at >= '2021-10-01'
;