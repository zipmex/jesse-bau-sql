-- detailed version - TABLE TO SHOW USERS PROFIT/ROI BREAKDOWN ACCORDING TO DATE, USER, COUNTRY AND COIN
-- to isolate all buy trades
WITH
	var_account_id AS (VALUES (819548)),
	var_country AS (VALUES ('TH'),('AU'),('ID'),('global')),
	var_date_position AS (VALUES ('2020-01-01'::date))
	, 
query AS (
	WITH buy AS (
		SELECT 
			DATE_TRUNC('day', t.created_at)::DATE created_at
			, t.ap_account_id 
			, t.product_1_symbol 
			, AVG(t.price) avg_cost_price
			, SUM(t.amount_usd) buy_amount_usd
			, 0.0 sell_amount_usd
			, 0.0 dep_amount_usd
			, 0.0 with_amount_usd
			, 0.0 market_price
			, 0.0 balance_amount_usd
		FROM
			warehouse.analytics.trades_master t
		WHERE 
			t.side = 'Buy'
			AND t.signup_hostcountry IN (TABLE var_country)
			AND t.created_at::DATE >= (TABLE var_date_position)
		GROUP BY 
			1,2,3,6,7,8,9,10
	)
	-- to isolate all sell trades
	, sell AS (
		SELECT 
			DATE_TRUNC('day', t.created_at)::DATE created_at
			, t.ap_account_id 
			, t.product_1_symbol 
			, AVG(t.price) avg_cost_price
			, 0.0 buy_amount_usd
			, SUM(t.amount_usd) sell_amount_usd
			, 0.0 dep_amount_usd
			, 0.0 with_amount_usd
			, 0.0 market_price
			, 0.0 balance_amount_usd
		FROM
			warehouse.analytics.trades_master t
		WHERE 
			t.side = 'Sell'
			AND t.signup_hostcountry IN (TABLE var_country)
			AND t.created_at::DATE >= (TABLE var_date_position)
		GROUP BY
			1,2,3,5,7,8,9,10
	)
	-- to isolate all deposits (crypto and fiat)
	, deposit AS (
		SELECT 
			DATE_TRUNC('day', d.created_at)::DATE created_at
			, d.ap_account_id 
			, d.product_symbol 
			, 0.0 avg_cost_price
			, 0.0 buy_amount_usd
			, 0.0 sell_amount_usd
			, SUM(d.amount_usd) dep_amount_usd
			, 0.0 with_amount_usd
			, 0.0 market_price
			, 0.0 balance_amount_usd
		FROM
			warehouse.analytics.deposit_tickets_master d
		WHERE 
			d.status = 'FullyProcessed'
			AND d.signup_hostcountry IN (TABLE var_country)
			AND d.created_at::DATE >= (TABLE var_date_position)
		GROUP BY
			1,2,3,4,5,6,8,9,10
	)
	-- to isolate all withdrawals (crypto and fiat)
	, withdraw AS (
		SELECT 
			DATE_TRUNC('day', w.created_at)::DATE created_at
			, w.ap_account_id 
			, w.product_symbol 
			, 0.0 avg_cost_price
			, 0.0 buy_amount_usd
			, 0.0 sell_amount_usd
			, 0.0 dep_amount_usd
			, SUM(w.amount_usd) with_amount_usd
			, 0.0 market_price
			, 0.0 balance_amount_usd
		FROM
			warehouse.analytics.withdraw_tickets_master w
		WHERE 
			w.status = 'FullyProcessed'
			AND w.signup_hostcountry IN (TABLE var_country)
			AND w.created_at::DATE >= (TABLE var_date_position)
		GROUP BY
			1,2,3,4,5,6,7,9,10
	)
	-- to get crypto and fiat value balance at end of day
	, balance AS (
		SELECT 
			DATE_TRUNC('day', b.created_at)::DATE created_at
			, b.ap_account_id 
			, b.symbol 
			, 0.0 avg_cost_price
			, 0.0 buy_amount_usd
			, 0.0 sell_amount_usd
			, 0.0 dep_amount_usd
			, 0.0 with_amount_usd
			-- for crypto amounts
			, r.price market_price
			, CASE WHEN r.product_type = 2 THEN
					(COALESCE(b.trade_wallet_amount, 0) 
					+ COALESCE(b.z_wallet_amount, 0) 
					+ COALESCE(b.ziplock_amount, 0) 
					+ COALESCE(b.zlaunch_amount, 0) ) * r.price
				-- for fiat amounts
				WHEN r.product_type = 1 THEN
					(COALESCE(b.trade_wallet_amount, 0) 
					+ COALESCE(b.z_wallet_amount, 0) 
					+ COALESCE(b.ziplock_amount, 0) 
					+ COALESCE(b.zlaunch_amount, 0) ) * 1/ r.price
				END AS balance_amount_usd
		FROM 
			warehouse.analytics.wallets_balance_eod b
		LEFT JOIN
			-- to crypto/fiat prices
			warehouse.analytics.rates_master r
			-- to link date
			ON r.created_at = b.created_at 
			-- to link cypto/fiat
			AND r.product_1_symbol = b.symbol 
		WHERE 
			b.created_at::DATE >= (TABLE var_date_position)
	)
	-- to combine all table values together in the same date, user and crypto row
	, union_table AS ( 
		SELECT * FROM buy
		UNION ALL
		SELECT * FROM sell
		UNION ALL
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
			, u.product_1_symbol symbol
			, SUM(avg_cost_price) avg_cost_price
			, SUM(u.buy_amount_usd) buy_amount_usd
			, SUM(u.sell_amount_usd) sell_amount_usd
			, SUM(u.dep_amount_usd) dep_amount_usd
			, SUM(u.with_amount_usd) with_amount_usd
			, SUM(market_price) market_price
			, SUM(u.balance_amount_usd) balance_amount_usd
		FROM
			union_table u
			LEFT JOIN analytics_pii.users_pii up 
				ON u.ap_account_id = up.ap_account_id 
		WHERE
			-- accounts to be excluded in all reports
			u.ap_account_id NOT IN (SELECT DISTINCT ap_account_id::NUMERIC FROM mappings.users_mapping um)
			AND u.ap_account_id = (TABLE var_account_id)
		GROUP BY
			1,2,3,4
	)
	-- to get cumulative values for all columns
	, total_table AS (
		SELECT 
			*
			, SUM(r.buy_amount_usd)
				OVER(PARTITION BY r.ap_account_id, r.symbol
					ORDER BY r.created_at) AS total_buy_amount_usd
			, SUM(r.sell_amount_usd)
				OVER(PARTITION BY r.ap_account_id, r.symbol
					ORDER BY r.created_at) AS total_sell_amount_usd
			, SUM(r.dep_amount_usd)
				OVER(PARTITION BY r.ap_account_id, r.symbol
					ORDER BY r.created_at) AS total_dep_amount_usd
			, SUM(r.with_amount_usd)
				OVER(PARTITION BY r.ap_account_id, r.symbol
					ORDER BY r.created_at) AS total_with_amount_usd
--			, AVG(avg_cost_price) OVER(PARTITION BY ap_account_id, symbol ORDER BY created_at) cost_price
		FROM
			raw_table r
	)
	-- to convert values into financial terms
	, processed_table AS (
		SELECT
			t.created_at 
			, t.email
			, u.signup_hostcountry
			, t.symbol
			, avg_cost_price
			, market_price
	--	realised_revenue
			, t.total_sell_amount_usd + t.total_with_amount_usd total_realised_revenue
			, COALESCE(t.balance_amount_usd, 0) total_unrealised_revenue
	--	cost_incurred
			, t.total_buy_amount_usd + t.total_dep_amount_usd total_cost_incurred
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
	ORDER BY 4, 1 DESC
)
SELECT * FROM query 
;


-- simplify version TABLE TO SHOW USERS FULL USER PROFIT/ROI BREAKDOWN ACCORDING TO DATE AND COUNTRY
-- to isolate all deposits (crypto and fiat)
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
			AND u.ap_account_id = (TABLE var_account_id)
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
;


