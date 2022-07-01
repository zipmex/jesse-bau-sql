

WITH base AS (
SELECT 
	um.user_id  
	, um.signup_hostcountry 
	, CASE WHEN u.mfa_enabled_at IS NULL THEN FALSE ELSE TRUE END AS mfa_enable
	, um.is_verified 
	, CASE WHEN dmm.mtu IS NULL THEN FALSE ELSE dmm.mtu END AS is_mtd_mtu
	, CASE WHEN u.signup_platform <> 'web' THEN TRUE ELSE FALSE END AS app_install
FROM 
	analytics.users_master um 
	LEFT JOIN 
		user_app_public.users u 
		ON um.user_id = u.id 
	LEFT JOIN 
		analytics.dm_mtu_monthly dmm 
		ON um.ap_account_id = dmm.ap_account_id 
		AND dmm.mtu_month = DATE_TRUNC('month', NOW()::DATE)::DATE 
WHERE
	um.signup_hostcountry = 'ID'
)
SELECT 
	signup_hostcountry 
	, COUNT(DISTINCT user_id) total_user_count
	, COUNT(DISTINCT CASE WHEN is_verified IS TRUE THEN user_id END) total_verified_user
	, COUNT(DISTINCT CASE WHEN is_mtd_mtu IS TRUE THEN user_id END) mtu_count
	, COUNT(DISTINCT CASE WHEN mfa_enable IS TRUE THEN user_id END) total_2fa_enable
	, COUNT(DISTINCT CASE WHEN app_install IS TRUE THEN user_id END) app_install_count
FROM base 
GROUP BY 1


WITH base AS (
	SELECT 
		user_id 
		, platform 
		, device 
		, session_start_ts 
		, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY session_start_ts DESC) ss_order
	FROM analytics.sessions_master sm2 
	WHERE 
		platform <> 'WEB'
)	, device_user AS (
	SELECT 
		DISTINCT
		b.user_id 
		, um.signup_hostcountry
		, b.platform 
		, b.device 
		, b.session_start_ts::DATE 
	FROM base b
		LEFT JOIN 
			analytics.users_master um 
			ON b.user_id = um.user_id 
	WHERE 
		ss_order = 1
		AND um.signup_hostcountry = 'ID'
	ORDER BY 1
)
SELECT 
	DATE_TRUNC('month', session_start_ts)::DATE session_start_ts
	, signup_hostcountry
	, COUNT(DISTINCT user_id) app_install_count
FROM device_user
GROUP BY 1,2
;

