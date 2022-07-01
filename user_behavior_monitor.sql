/*
 * Distribution of deposits value and count per country (fiat / crypto)
 * Distribution of withdrawal value and count per country (fiat / crypto)
 * Amount and count of withdrawals that are cross border (withdrawn in a different currency to that of the entity the user trades with)
 * Amount and count of deposits that are cross border (deposited from a different currency to that of the entity the user trades with)
 * Distribution of traders (amount traded) and (count of trades)
 * Distribution of users that match on trades (i.e user_a & user_b trade > x times).
**/


--- Distribution of deposits/ withdraw value and count per country (fiat / crypto)
WITH deposit_ AS ( 
		SELECT 
			date_trunc('day', d.updated_at) AS month_  
			, d.ap_account_id 
			, d.signup_hostcountry 
			, d.product_type 
			, u.base_fiat 
			, d.product_symbol 
			, COUNT(d.*) AS deposit_number 
			, SUM(d.amount) AS deposit_amount 
			, SUM( CASE WHEN amount_usd IS NOT NULL THEN amount_usd	
						WHEN product_symbol = 'USD' THEN amount_usd * 1 
						WHEN r.product_type = 1 THEN amount * 1/r.price 
						WHEN r.product_type = 2 THEN amount * r.price 
						END) AS deposit_usd
		--	, SUM(d.amount_usd) deposit_usd
		FROM 
			analytics.deposit_tickets_master d 
			LEFT JOIN 
				data_team_staging.rates_master_staging r 
				ON d.product_symbol = r.product_1_symbol
				AND DATE_TRUNC('day', d.created_at) = DATE_TRUNC('day', r.created_at)
			LEFT JOIN 
				analytics.users_master u
				ON d.ap_account_id = u.ap_account_id 
		WHERE 
			d.status = 'FullyProcessed' 
			AND d.signup_hostcountry IN ('TH','AU','ID','global')
			AND d.updated_at::date >= '2021-01-01' AND d.updated_at::date < NOW()::date 
			AND d.ap_account_id NOT IN (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63312, 63313, 161347, 317029, 496001) 
		GROUP  BY 
			1,2,3,4,5,6
), withdraw_ AS (
		SELECT 
			date_trunc('day', w.updated_at) AS month_  
			, w.ap_account_id 
			, w.signup_hostcountry 
			, w.product_type 
			, w.product_symbol 
			, u.base_fiat 
			, COUNT(w.*) AS withdraw_number 
			, SUM(w.amount) AS withdraw_amount 
			, SUM( CASE WHEN amount_usd IS NOT NULL THEN amount_usd	
						WHEN product_symbol = 'USD' THEN amount_usd * 1 
						WHEN r.product_type = 1 THEN amount * 1/r.price 
						WHEN r.product_type = 2 THEN amount * r.price 
					END) AS withdraw_usd	
		--	, SUM(w.amount_usd) withdraw_usd
		FROM  
			analytics.withdraw_tickets_master w 
			LEFT JOIN 
				data_team_staging.rates_master_staging r 
				ON w.product_symbol = r.product_1_symbol
				AND DATE_TRUNC('day', w.created_at) = DATE_TRUNC('day', r.created_at)
			LEFT JOIN 
				analytics.users_master u
				ON w.ap_account_id = u.ap_account_id 
		WHERE 
			w.status = 'FullyProcessed'
			AND w.signup_hostcountry IN ('TH','AU','ID','global')
			AND w.updated_at::date >= '2021-01-01' AND w.updated_at::date < NOW()::date 
			AND w.ap_account_id NOT IN (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63312, 63313, 161347, 317029, 496001)
		GROUP BY 
			1,2,3,4,5,6
)	, final_t AS (
	SELECT 
		DATE_TRUNC('month', COALESCE(d.month_, w.month_)) datadate  
		, COALESCE(d.product_type, w.product_type) product_type 
		, COALESCE(d.signup_hostcountry, w.signup_hostcountry) signup_hostcountry
	--	, COALESCE(d.ap_account_id, w.ap_account_id) ap_account_id 
		, COALESCE(d.product_symbol, w.product_symbol) product_symbol 
		, COALESCE(d.base_fiat, w.base_fiat) base_fiat 
		, SUM( COALESCE(d.deposit_number, 0)) depost_count 
		, SUM( deposit_amount) deposit_amount
		, SUM( COALESCE(d.deposit_usd, 0)) deposit_usd
		, SUM( COALESCE(w.withdraw_number, 0)) withdraw_count
		, SUM( withdraw_amount) withdraw_amount
		, SUM( COALESCE(w.withdraw_usd, 0)) withdraw_usd
	FROM 
		deposit_ d 
		FULL OUTER JOIN 
			withdraw_ w 
			ON d.ap_account_id = w.ap_account_id 
			AND d.signup_hostcountry = w.signup_hostcountry 
			AND d.product_type = w.product_type 
			AND d.month_ = w.month_ 
			AND d.product_symbol = w.product_symbol 
	WHERE 
		COALESCE(d.month_, w.month_) >= '2021-06-01 00:00:00'
	GROUP BY 
		1,2,3,4,5
	ORDER BY 
		1,2 
)
SELECT 
	CASE WHEN product_type = 'NationalCurrency' AND base_fiat <> product_symbol THEN 1 ELSE 0 END AS cross_border_transaction
	, *
FROM final_t
;


---- cross border deposit/withdraw 
WITH deposit_cb AS (
		SELECT 
			date_trunc('day', d.created_at) created_at 
			, d.ap_account_id
			, d.signup_hostcountry 
			, u.base_fiat 
			, product_symbol
			, COUNT(DISTINCT ticket_id) deposit_count
			, SUM(amount) deposit_amount 
			, SUM(amount_usd) deposit_usd_amount
		FROM 
			analytics.deposit_tickets_master d
			LEFT JOIN analytics.users_master u
				ON d.ap_account_id = u.ap_account_id 
		WHERE 
			product_type = 'NationalCurrency'
			AND d.created_at >= '2021-01-01 00:00:00'
			AND status = 'FullyProcessed'
			AND u.base_fiat <> d.product_symbol 
		GROUP BY 1,2,3,4,5
		ORDER BY 1,2
)	, withdraw_cb AS (
		SELECT 
			date_trunc('day', w.created_at) created_at
			, w.ap_account_id
			, w.signup_hostcountry 
			, u.base_fiat 
			, product_symbol
			, COUNT(DISTINCT ticket_id) withdraw_count
			, SUM(amount) withdraw_amount 
			, SUM(amount_usd) withdraw_usd_amount
		FROM 
			analytics.withdraw_tickets_master w
			LEFT JOIN analytics.users_master u
				ON w.ap_account_id = u.ap_account_id 
		WHERE 
			product_type = 'NationalCurrency'
			AND w.created_at >= '2021-01-01 00:00:00'
			AND status = 'FullyProcessed'
			AND u.base_fiat <> w.product_symbol 
		--	AND w.ap_account_id = 395672
		GROUP BY 1,2,3,4,5
		ORDER BY 1,2
)
SELECT 
	DATE_TRUNC('month', COALESCE(d.created_at, w.created_at)) created_at  
	, COALESCE(d.signup_hostcountry, w.signup_hostcountry) signup_hostcountry
--	, COALESCE(d.ap_account_id, w.ap_account_id) ap_account_id 
	, COALESCE(d.base_fiat, w.base_fiat) base_fiat 
	, COALESCE(d.product_symbol, w.product_symbol) symbol 
	, SUM( COALESCE(d.deposit_count, 0)) deposit_count 
	, SUM( deposit_amount) deposit_amount
	, SUM( COALESCE(d.deposit_usd_amount, 0)) deposit_usd_amount
	, SUM( COALESCE(w.withdraw_count, 0)) withdraw_count
	, SUM( withdraw_amount) withdraw_amount
	, SUM( COALESCE(w.withdraw_usd_amount, 0)) withdraw_usd_amount
FROM 
	deposit_cb d 
	FULL OUTER JOIN withdraw_cb w 
		ON d.ap_account_id = w.ap_account_id 
		AND d.signup_hostcountry = w.signup_hostcountry 
		AND d.created_at = w.created_at 
		AND d.base_fiat = w.base_fiat 
		AND d.product_symbol = w.product_symbol
GROUP BY 1,2,3,4
;


---- distribution of trade volume
WITH base AS (
	SELECT
		DATE_TRUNC('month', created_at) created_at 
		, signup_hostcountry 
		, ap_account_id 
		, counter_party::integer 
		, CASE WHEN t.counter_party IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227','27443'
			,'38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','44057','161347','316078','44056','63152')
			THEN 0 ELSE 1 END AS is_organic_trade
		, COUNT(DISTINCT trade_id) tradeid_count
		, COUNT(DISTINCT order_id) orderid_count
		, SUM(quantity) trade_amount
		, SUM(amount_usd) trade_usd_amount
	FROM
		analytics.trades_master t
	WHERE 
		created_at >= '2021-06-01 00:00:00'
	--	AND created_at < '2021-08-01 00:00:00'
		AND t.ap_account_id NOT IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227','27443'
			,'38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','44057','161347','316078','44056','63152')
		AND t.signup_hostcountry IN ('TH','ID','AU','global')
	GROUP BY 1,2,3,4,5
	ORDER BY 1,2,6 DESC 
)--	, trade_distribution AS (
	SELECT 
		created_at
		, signup_hostcountry
		, COUNT(DISTINCT ap_account_id) trader_count
		, SUM(tradeid_count) tradeid_count
		, SUM(orderid_count) orderid_count
		, SUM(trade_amount) trade_amount
		, SUM(trade_usd_amount) trade_usd_amount
	FROM base
	GROUP BY 1,2
	
)
SELECT 
	created_at
	, signup_hostcountry
	, ap_account_id 
	, counter_party 
	, SUM(tradeid_count) tradeid_count
	, SUM(orderid_count) orderid_count
	, SUM(trade_amount) trade_amount
	, SUM(trade_usd_amount) trade_usd_amount
FROM base
WHERE is_organic_trade = 1
GROUP BY 1,2,3,4