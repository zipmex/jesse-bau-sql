WITH base AS (
	SELECT 
		wtm.ap_account_id 
		, wtm.signup_hostcountry 
		, up.email 
		, f.code account_type
		, CASE WHEN ult.tier_name IS NULL THEN 'no_zmt' ELSE ult.tier_name END AS vip_tier
		, wtm.product_symbol 
		, SUM( COALESCE (amount_usd, amount * dap.price)) withdraw_usd
		, COUNT(wtm.ticket_id) withdraw_count
	FROM 
		analytics.withdraw_tickets_master wtm 
		LEFT JOIN
			analytics.users_master um 
			ON wtm.ap_account_id = um.ap_account_id 
		LEFT JOIN 
			user_app_public.user_features uf 
			ON um.user_id = uf.user_id 
		LEFT JOIN 
			user_app_public.features f 
			ON uf.feature_id = f.id 
		LEFT JOIN 
			analytics_pii.users_pii up  
			ON wtm.ap_account_id = up.ap_account_id 
		LEFT JOIN 
			zip_lock_service_public.user_loyalty_tiers ult 
			ON um.user_id = ult.user_id 
		LEFT JOIN 
			public.daily_ap_prices dap 
			ON wtm.product_symbol = 'TOK' AND dap.product_1_symbol = 'TOK'
			AND dap.product_2_symbol = 'USD'
	WHERE 
		wtm.status = 'FullyProcessed'
		AND wtm.created_at >= '2022-05-01'
	GROUP BY 1,2,3,4,5,6
)	, withdraw_rank AS (
SELECT 
	*
	, RANK() OVER (PARTITION BY signup_hostcountry ORDER BY withdraw_usd DESC ) rank_withdrawer
FROM 
	base 
)
SELECT 
	*
FROM withdraw_rank
WHERE 
	(CASE WHEN signup_hostcountry = 'TH' THEN rank_withdrawer < 201
		ELSE rank_withdrawer < 101 END)
;


SELECT 
	*
FROM analytics.deposit_tickets_master dtm   
--FROM analytics.withdraw_tickets_master wtm   
WHERE 
	amount_usd IS NULL 
ORDER BY created_at  DESC 
;


-- daily withdraw - threshold 20k USD 
WITH base AS (
	SELECT 
	    wtm.created_at::DATE 
        , wtm.ap_account_id 
		, wtm.signup_hostcountry 
		, up.email 
		, f.code account_type
		, CASE WHEN ult.tier_name IS NULL THEN 'no_zmt' ELSE ult.tier_name END AS vip_tier
		, wtm.product_symbol 
		, SUM( COALESCE (amount_usd, amount * dap.price)) withdraw_usd
		, COUNT(wtm.ticket_id) withdraw_count
	FROM 
		analytics.withdraw_tickets_master wtm 
		LEFT JOIN
			analytics.users_master um 
			ON wtm.ap_account_id = um.ap_account_id 
		LEFT JOIN 
			user_app_public.user_features uf 
			ON um.user_id = uf.user_id 
		LEFT JOIN 
			user_app_public.features f 
			ON uf.feature_id = f.id 
		LEFT JOIN 
			analytics_pii.users_pii up  
			ON wtm.ap_account_id = up.ap_account_id 
		LEFT JOIN 
			zip_lock_service_public.user_loyalty_tiers ult 
			ON um.user_id = ult.user_id 
		LEFT JOIN 
			public.daily_ap_prices dap 
			ON wtm.product_symbol = 'TOK' AND dap.product_1_symbol = 'TOK'
			AND dap.product_2_symbol = 'USD'
	WHERE 
		wtm.status = 'FullyProcessed'
		AND wtm.created_at >= '2022-05-17'
	GROUP BY 1,2,3,4,5,6,7
)	, withdraw_rank AS (
	SELECT 
		*
		, SUM(withdraw_usd) OVER(PARTITION BY ap_account_id) total_withdraw_usd 
	FROM 
		base 
	WHERE 
		withdraw_usd >= 10000 
)
SELECT 
	*
	, withdraw_usd/ withdraw_count avg_withdraw_usd
	, NOW()::DATE updated_at 
	, DENSE_RANK() OVER (PARTITION BY signup_hostcountry ORDER BY total_withdraw_usd DESC ) rank_withdrawer
FROM withdraw_rank
--WHERE 
--	(CASE WHEN signup_hostcountry = 'TH' THEN rank_withdrawer < 201
--		ELSE rank_withdrawer < 101 END)
;


