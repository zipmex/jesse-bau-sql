WITH base AS (
SELECT 
	u.ap_account_id 
	, u.signup_hostcountry 
	, b.code 
	, b.name_en 
FROM analytics.users_master u
	LEFT JOIN (
				SELECT *
				, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY is_verified_at DESC) row_ 
				FROM user_app_public.bank_accounts
				) ba 
		ON u.user_id = ba.user_id 
		AND ba.row_ = 1
	LEFT JOIN user_app_public.banks b 
		ON ba.bank_code = b.code 
)
SELECT 
	name_en
	, COUNT(DISTINCT ap_account_id) user_count
FROM base
GROUP BY 1
ORDER BY 2 DESC

SELECT 
	user_id 
	, COUNT(user_id)
	, COUNT(DISTINCT user_id)
FROM user_app_public.bank_accounts ba 
GROUP BY 1
ORDER BY 2 DESC 

SELECT *, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY is_verified_at DESC) row_ FROM user_app_public.bank_accounts