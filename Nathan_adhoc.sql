---- detailed trade report
SELECT
	DATE_TRUNC('month', t.created_at) traded_month
	, t.signup_hostcountry 
	, t.ap_user_id 
	, t.ap_account_id 
	, u.email 
	, t.side 
	, t.instrument_id 
	, i.symbol 
	, t.product_1_symbol 
	, t.execution_id 
	, f.fee_reference_id 
	, f.fee_product 
	, f.fee_amount 
	, SUM( COALESCE (f.fee_usd_amount, f.fee_amount * r.price)) "sum_usd_fee" 
	, SUM(t.quantity) "sum_coin_volume"
	, SUM(t.amount_usd) "sum_usd_volume" 
FROM 
	analytics.trades_master t
	LEFT JOIN analytics.users_master u
		ON t.ap_account_id = u.ap_account_id
	LEFT JOIN analytics.fees_master f
		ON t.execution_id = f.fee_reference_id 
	LEFT JOIN apex.instruments i 
		ON t.instrument_id = i.instrument_id 
	LEFT JOIN analytics.rates_master r
		ON f.fee_product = r.product_1_symbol 
		AND DATE_TRUNC('day', f.created_at) = r.created_at 
WHERE 
	DATE_TRUNC('day', t.created_at) >= '2021-10-01 00:00:00' 
	AND DATE_TRUNC('day', t.created_at) < '2021-11-01 00:00:00' -- DATE_TRUNC('day', NOW())
	AND t.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
	AND t.signup_hostcountry IN ('TH','ID','AU','global')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
ORDER BY 1
;


---- crypto withdraw report
SELECT 
	w.updated_at  
	, w.ticket_id 
	, w.ap_account_id 
	, u.email 
	, w.signup_hostcountry 
	, w.product_type 
	, w.product_symbol 
	, wt.fee_amount::float
	, wt.fee_amount::float * r.price fee_usd
	, COUNT(DISTINCT w.ticket_id) AS withdraw_count 
	, SUM(w.amount) AS withdraw_amount 
	, SUM(w.amount_usd) withdraw_usd
FROM  
	analytics.withdraw_tickets_master w 
	LEFT JOIN analytics.users_master u
		ON w.ap_account_id = u.ap_account_id 
	LEFT JOIN apex.withdraw_tickets wt 
		ON w.ticket_id = wt.withdraw_ticket_id
	LEFT JOIN analytics.rates_master r
		ON w.product_symbol = r.product_1_symbol 
		AND DATE_TRUNC('day', w.created_at) = r.created_at 
WHERE 
	w.status = 'FullyProcessed'
	AND w.signup_hostcountry IN ('TH','AU','ID','global')
	AND w.product_type = 'CryptoCurrency'
	AND w.updated_at::date >= '2021-11-01' AND w.updated_at::date < NOW()::date 
	AND w.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
	AND w.ap_account_id = 812458
GROUP BY 
	1,2,3,4,5,6,7,8,9
ORDER BY 1 DESC 
;


---- crypto deposit report
SELECT 
	d.updated_at  
	, d.ticket_id 
	, d.ap_account_id 
	, u.email 
	, d.signup_hostcountry 
	, d.product_type 
	, d.product_symbol 
	, dt.fee_amount::float
	, dt.fee_amount::float * r.price fee_usd
	, COUNT(DISTINCT d.ticket_id) AS deposit_count 
	, SUM(d.amount) AS deposit_amount 
	, SUM(d.amount_usd) deposit_usd
FROM  
	analytics.deposit_tickets_master d
	LEFT JOIN analytics.users_master u
		ON d.ap_account_id = u.ap_account_id 
	LEFT JOIN apex.deposit_tickets dt 
		ON d.ticket_id = dt.deposit_ticket_id 
	LEFT JOIN analytics.rates_master r
		ON d.product_symbol = r.product_1_symbol 
		AND DATE_TRUNC('day', d.created_at) = r.created_at 
WHERE 
	d.status = 'FullyProcessed'
	AND d.signup_hostcountry IN ('TH','AU','ID','global')
	AND d.product_type = 'CryptoCurrency'
	AND d.updated_at::date >= '2021-10-01' AND d.updated_at::date < NOW()::date 
	AND d.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
GROUP BY 
	1,2,3,4,5,6,7,8,9
;


WITH cp_offline AS (
	SELECT 
		LOWER(cal."Email") email
		, um.created_at 
		, um.verification_approved_at 
	FROM 
		mappings.commercial_au_lashcreative cal 
		LEFT JOIN analytics_pii.users_pii up  
		    ON LOWER(cal."Email") = up.email
		LEFT JOIN analytics.users_master um 
		    ON up.user_id = um.user_id 
    WHERE 
        um.created_at + '11hr' >= '2022-03-09'
        AND um.signup_hostcountry IN ('AU','global')
)
SELECT 
	COUNT(created_at)
	, COUNT(verification_approved_at)
FROM cp_offline f


)	, user_base AS (
	SELECT 
		um.user_id 
		, um.signup_hostcountry 
		, um.created_at + '11hr' register_gmt11
		, um.verification_approved_at + '11hr' verified_gmt11	
		, um.first_deposit_at + '11hr' first_deposit_gmt11
		, um.first_traded_at + '11hr' first_traded_gmt11
		, um.invitation_code 
		, CASE WHEN grc.referral_group IS NOT NULL THEN grc.referral_group 
				WHEN grc.referral_group IS NULL THEN 
				(CASE WHEN cal.email IS NOT NULL THEN 'cp_offline'
						WHEN cal.email IS NULL AND um.invitation_code IS NULL THEN 'organic'
						WHEN cal.email IS NULL AND um.invitation_code IS NOT NULL THEN 'p2p'
						END)
				END AS referral_group 
		, grc.email 
		, um.sum_deposit_amount_usd 
		, um.sum_trade_volume_usd 
		, um.sum_withdraw_amount_usd 
		, um.sum_fee_amount_usd 
	FROM 
		analytics.users_master um 
		LEFT JOIN 
			analytics_pii.users_pii up 
			ON um.user_id = up.user_id 
		LEFT JOIN 
			mappings.growth_referral_code grc 
			ON um.invitation_code = grc.referral_code 
		LEFT JOIN 
			cp_offline cal 
			ON up.email = cal.email 
	WHERE 
		um.signup_hostcountry IN ('AU','global')
		AND um.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
)
SELECT 
	DATE_TRUNC('day', register_gmt11)::DATE register_gmt11
	, signup_hostcountry 
	, referral_group 
	, COUNT(DISTINCT user_id) register_count
	, COUNT(DISTINCT CASE WHEN verified_gmt11 IS NOT NULL THEN user_id END ) verified_count
	, COUNT(DISTINCT CASE WHEN first_deposit_gmt11 IS NOT NULL THEN user_id END ) first_deposit_count
	, COUNT(DISTINCT CASE WHEN first_traded_gmt11 IS NOT NULL THEN user_id END ) first_traded_count
FROM user_base 
GROUP BY 1,2,3
ORDER BY 1 DESC
;




