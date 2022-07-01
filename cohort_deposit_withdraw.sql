--- deposit + withdrawl 
WITH deposit_ AS ( 
	SELECT 
		date_trunc('day', d.updated_at) AS month_  
		, d.ap_account_id 
		, d.signup_hostcountry 
		, d.product_type 
		, d.product_symbol 
		,CASE WHEN d.ap_account_id in (1373,1432,13266,16211,16308,22576,34535,48900,53463,80871,84319) THEN TRUE ELSE FALSE END AS is_whale
		, COUNT(d.*) AS deposit_number 
		, SUM(d.amount) AS deposit_amount 
	--	, SUM(d.amount_usd) deposit_usd
		, SUM( CASE WHEN d.amount_usd IS NOT NULL THEN d.amount_usd
					ELSE 
					(CASE WHEN product_symbol = 'USD' THEN amount *1 
						WHEN r.product_type = 1 THEN amount * 1/price 
						WHEN r.product_type = 2 THEN amount * r.price 
						END)
					END) deposit_usd
	FROM 
		analytics.deposit_tickets_master d 
		LEFT JOIN 
			analytics.rates_master r
			ON product_symbol = product_1_symbol
			AND DATE_TRUNC('day', d.created_at) = r.created_at
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
		,CASE WHEN w.ap_account_id IN (1373,1432,13266,16211,16308,22576,34535,48900,53463,80871,84319) THEN TRUE ELSE FALSE END AS is_whale
		, COUNT(w.*) AS withdraw_number 
		, SUM(w.amount) AS withdraw_amount 
	--	, SUM(w.amount_usd) withdraw_usd
		, SUM( CASE WHEN w.amount_usd IS NOT NULL THEN w.amount_usd
					ELSE 
					(CASE WHEN product_symbol = 'USD' THEN amount *1 
						WHEN r.product_type = 1 THEN amount * 1/price 
						WHEN r.product_type = 2 THEN amount * r.price 
						END)
					END) withdraw_usd
	FROM  
		analytics.withdraw_tickets_master w 
		LEFT JOIN 
			analytics.rates_master r
			ON product_symbol = product_1_symbol
			AND DATE_TRUNC('day', w.created_at) = r.created_at
	WHERE 
		w.status = 'FullyProcessed'
		AND w.signup_hostcountry IN ('TH','AU','ID','global')
		AND w.updated_at::date >= '2021-01-01' AND w.updated_at::date < NOW()::date 
		AND w.ap_account_id NOT IN (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63312, 63313, 161347, 317029, 496001)
	GROUP BY 
	1,2,3,4,5,6
)	, base AS (
	SELECT 
		DATE_TRUNC('day', COALESCE(d.month_, w.month_)) created_at  
		, COALESCE(d.signup_hostcountry, w.signup_hostcountry) signup_hostcountry
		, COALESCE (d.ap_account_id, w.ap_account_id) ap_account_id 
		, COALESCE (d.product_type, w.product_type) product_type 
		, COALESCE (d.product_symbol, w.product_symbol) symbol 
	--	, COALESCE(d.is_whale, w.is_whale) is_whale
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
		COALESCE(d.month_, w.month_) >= '2021-01-01 00:00:00' --DATE_TRUNC('month', NOW()) 
		AND COALESCE(d.month_, w.month_) < DATE_TRUNC('day', NOW())
	GROUP BY 
		1,2,3,4,5
	ORDER BY 
		1,2 
)	, verified_users AS (
	SELECT 
		DISTINCT 
		ap_account_id
		, signup_hostcountry 
		, DATE_TRUNC('month', onfido_completed_at) verified_month
	FROM analytics.users_master um 
	WHERE 
		is_verified = TRUE 
		AND DATE_TRUNC('month', onfido_completed_at) >= '2021-03-01 00:00:00'
)
SELECT 
	verified_month
	, u.signup_hostcountry
	, SUM( COALESCE(deposit_usd, 0)) deposit_usd
	, SUM( COALESCE(withdraw_usd, 0)) withdraw_usd
	, COUNT(DISTINCT u.ap_account_id) verified_user_count
	, COUNT(DISTINCT CASE WHEN deposit_usd > 0 THEN u.ap_account_id END) depositor_count
	, COUNT(DISTINCT CASE WHEN withdraw_usd > 0 THEN u.ap_account_id END) withdrawer_count
	, COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month AND deposit_usd > 0 THEN u.ap_account_id END) AS m0_depositor_count
	, COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month + '1 month'::INTERVAL AND deposit_usd > 0 THEN u.ap_account_id END) AS m1_depositor_count
	, COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month + '2 month'::INTERVAL AND deposit_usd > 0 THEN u.ap_account_id END) AS m2_depositor_count
	, COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month + '3 month'::INTERVAL AND deposit_usd > 0 THEN u.ap_account_id END) AS m3_depositor_count
	, COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month + '4 month'::INTERVAL AND deposit_usd > 0 THEN u.ap_account_id END) AS m4_depositor_count
	, COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month + '5 month'::INTERVAL AND deposit_usd > 0 THEN u.ap_account_id END) AS m5_depositor_count
	, COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month + '6 month'::INTERVAL AND deposit_usd > 0 THEN u.ap_account_id END) AS m6_depositor_count
	, COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month + '7 month'::INTERVAL AND deposit_usd > 0 THEN u.ap_account_id END) AS m7_depositor_count
	, COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month AND withdraw_usd > 0 THEN u.ap_account_id END) AS m0_withdrawer_count
	, COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month + '1 month'::INTERVAL AND withdraw_usd > 0 THEN u.ap_account_id END) AS m1_withdrawer_count
	, COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month + '2 month'::INTERVAL AND withdraw_usd > 0 THEN u.ap_account_id END) AS m2_withdrawer_count
	, COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month + '3 month'::INTERVAL AND withdraw_usd > 0 THEN u.ap_account_id END) AS m3_withdrawer_count
	, COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month + '4 month'::INTERVAL AND withdraw_usd > 0 THEN u.ap_account_id END) AS m4_withdrawer_count
	, COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month + '5 month'::INTERVAL AND withdraw_usd > 0 THEN u.ap_account_id END) AS m5_withdrawer_count
	, COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month + '6 month'::INTERVAL AND withdraw_usd > 0 THEN u.ap_account_id END) AS m6_withdrawer_count
	, COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month + '7 month'::INTERVAL AND withdraw_usd > 0 THEN u.ap_account_id END) AS m7_withdrawer_count
	, SUM( CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month THEN COALESCE(deposit_usd, 0) END) m0_deposit_usd
	, SUM( CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month + '1 month'::INTERVAL THEN COALESCE(deposit_usd, 0) END) m1_deposit_usd
	, SUM( CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month + '2 month'::INTERVAL THEN COALESCE(deposit_usd, 0) END) m2_deposit_usd
	, SUM( CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month + '3 month'::INTERVAL THEN COALESCE(deposit_usd, 0) END) m3_deposit_usd
	, SUM( CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month + '4 month'::INTERVAL THEN COALESCE(deposit_usd, 0) END) m4_deposit_usd
	, SUM( CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month + '5 month'::INTERVAL THEN COALESCE(deposit_usd, 0) END) m5_deposit_usd
	, SUM( CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month + '6 month'::INTERVAL THEN COALESCE(deposit_usd, 0) END) m6_deposit_usd
	, SUM( CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month + '7 month'::INTERVAL THEN COALESCE(deposit_usd, 0) END) m7_deposit_usd
	, SUM( CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month THEN COALESCE(withdraw_usd, 0) END) m0_withdraw_usd
	, SUM( CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month + '1 month'::INTERVAL THEN COALESCE(withdraw_usd, 0) END) m1_withdraw_usd
	, SUM( CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month + '2 month'::INTERVAL THEN COALESCE(withdraw_usd, 0) END) m2_withdraw_usd
	, SUM( CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month + '3 month'::INTERVAL THEN COALESCE(withdraw_usd, 0) END) m3_withdraw_usd
	, SUM( CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month + '4 month'::INTERVAL THEN COALESCE(withdraw_usd, 0) END) m4_withdraw_usd
	, SUM( CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month + '5 month'::INTERVAL THEN COALESCE(withdraw_usd, 0) END) m5_withdraw_usd
	, SUM( CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month + '6 month'::INTERVAL THEN COALESCE(withdraw_usd, 0) END) m6_withdraw_usd
	, SUM( CASE WHEN DATE_TRUNC('month', b.created_at) = verified_month + '7 month'::INTERVAL THEN COALESCE(withdraw_usd, 0) END) m7_withdraw_usd
FROM verified_users u 
	LEFT JOIN base b
	ON b.ap_account_id = u.ap_account_id 
	AND b.signup_hostcountry = u.signup_hostcountry 
GROUP BY 1,2
;





