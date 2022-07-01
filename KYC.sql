---- monthly KYC users by kyc date AND ZipUp user by Subscribed Date
WITH base AS (
	SELECT
		u.created_at AS register_date
		, u.onfido_completed_at kyc_date 
		, u.signup_hostcountry , u.user_id 
		, u.ap_user_id , u.ap_account_id 
		, CASE WHEN u.signup_hostcountry = 'TH' THEN
			(CASE WHEN u.created_at < '2022-05-08' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
			WHEN u.signup_hostcountry = 'ID' THEN
			(CASE WHEN u.created_at < '2022-07-04' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
			WHEN u.signup_hostcountry IN ('AU','global') THEN
			(CASE WHEN u.created_at < '2022-06-29' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
			END AS zip_up_date
		, u.is_verified
		, u.is_zipup_subscribed 
		, u.level_increase_status 
	FROM 
		analytics.users_master u
		LEFT JOIN 
			warehouse.zip_up_service_public.user_settings s
			ON u.user_id = s.user_id 
	WHERE 
		u.signup_hostcountry IN ('TH','ID','AU','global')  
	)	,base_month AS (
	SELECT 
		signup_hostcountry
		, DATE_TRUNC('month', register_date)::DATE AS register_month
		, count(DISTINCT user_id) AS  user_id_c
		, count(DISTINCT CASE WHEN register_date IS  NOT  NULL  AND  is_verified =true THEN  user_id END ) AS  user_id_kyc 
		---> this one only count the status. meaning everytime we report, number will change AND cannot capture true monthly performance
		, count(DISTINCT CASE WHEN register_date IS  NOT  NULL  AND is_zipup_subscribed = TRUE  THEN  user_id END ) AS  user_id_z_up_sub
	FROM 
		base
	GROUP BY 1, 2
	)	,base_month_z_up AS (
	SELECT
		signup_hostcountry, DATE_TRUNC('month', zip_up_date) AS zip_up_month
		,count(DISTINCT CASE WHEN zip_up_date IS NOT NULL THEN user_id end) AS reporting_zipup_subscriber_count 
		---> this one count the status by subscribe date, number is fixed
	FROM 
		base
	GROUP BY 1,2
	)	,base_month_kyc AS (
	SELECT
		signup_hostcountry, DATE_TRUNC('month', kyc_date) AS kyc_month
		,count(DISTINCT CASE WHEN is_verified = TRUE THEN user_id END) AS reporting_verified_user_count 
		---> this one count the status by kyc date, number is fixed level_increase_status = 'pass'
	FROM 
		base
	GROUP BY 1,2
	)
SELECT
	b.* 
	, k.reporting_verified_user_count 
	-- cumulative count over months using Window Function 
	, sum(user_id_c) OVER(PARTITION BY b.signup_hostcountry ORDER BY register_month ) AS  total_registered_user
	, sum(user_id_kyc) OVER(PARTITION BY b.signup_hostcountry ORDER BY register_month) AS  total_kyc
	, sum(reporting_verified_user_count) OVER(PARTITION BY k.signup_hostcountry ORDER BY kyc_month) AS  total_reporting_verified_user
	, sum(user_id_z_up_sub) OVER(PARTITION BY b.signup_hostcountry ORDER BY register_month ) AS  total_zip_up
	, sum(reporting_zipup_subscriber_count) OVER(PARTITION BY z.signup_hostcountry ORDER BY zip_up_month) AS  total_reporting_zipup_subscriber
	, sum(user_id_c) OVER() AS zipmex_registered_user
	, sum(reporting_verified_user_count) OVER() AS zipmex_verified_user
FROM
	base_month b
	LEFT JOIN base_month_kyc k ON  k.signup_hostcountry = b.signup_hostcountry AND k.kyc_month = b.register_month 
	LEFT JOIN base_month_z_up z ON  z.signup_hostcountry = b.signup_hostcountry AND z.zip_up_month = b.register_month 
ORDER BY 
	1 ,2 DESC	 
;




---- daily user funnel
WITH temp_ AS (
	SELECT 
	u.signup_hostcountry ,u.user_id ,u.ap_user_id,u.ap_account_id 
	,u.created_at AS register_date
	,u.onfido_completed_at AS kyc_date 
	,u.zipup_subscribed_at AS zipup_date 
	,u.is_verified 
	,u.level_increase_status 
	FROM 
		analytics.users_master u
	WHERE 
		u.signup_hostcountry IN ('TH','ID','AU','global')  
		AND u.created_at > '2021-01-01 00:00:00' 
),temp_m AS (
	SELECT 
		signup_hostcountry
		, DATE_TRUNC('day', register_date)::DATE AS register_month
		,COUNT(distinct user_id) AS user_id_c
	FROM temp_
	GROUP BY 1, 2
),temp_kyc AS (
	SELECT 
		signup_hostcountry
		, DATE_TRUNC('day', kyc_date)::DATE AS kyc_month
		,COUNT(DISTINCT CASE WHEN kyc_date IS NOT NULL AND is_verified = TRUE THEN user_id END) AS user_id_kyc_new ---> this one count the status by kyc date, number is fixed level_increase_status = 'pass'
	FROM temp_
	GROUP BY 1,2
),temp_zipup AS (
	SELECT 
		signup_hostcountry
		, DATE_TRUNC('day', zipup_date)::DATE AS zipup_month
		,COUNT(DISTINCT CASE WHEN zipup_date IS NOT NULL THEN user_id END) AS user_zipup_new 
	FROM temp_
	GROUP BY 1,2
), final_temp AS (
	SELECT 
		b.signup_hostcountry, b.register_month
		, COALESCE(b.user_id_c,0) user_id_c
		, COALESCE(user_id_kyc_new,0) user_id_kyc_new 
		, COALESCE(user_zipup_new,0) user_zipup_new 
	FROM temp_m b
		LEFT JOIN temp_kyc k ON k.signup_hostcountry = b.signup_hostcountry AND k.kyc_month = b.register_month 
		LEFT JOIN temp_zipup z ON z.signup_hostcountry = b.signup_hostcountry AND z.zipup_month = b.register_month 
	ORDER BY 1,2
)
SELECT 
	*
	,sum(user_id_c) OVER(PARTITION BY signup_hostcountry ORDER BY register_month ) AS total_registered_user
	,sum(user_id_kyc_new) OVER(PARTITION BY signup_hostcountry ORDER BY register_month) AS total_kyc_user
	,sum(user_zipup_new) OVER(PARTITION BY signup_hostcountry ORDER BY register_month) AS total_zipup_user
FROM final_temp 
ORDER BY 2 DESC 
;



---- kyc by signup_platform with register_date
WITH base AS(
SELECT 
	created_at 
	,signup_hostcountry 
	,signup_platform 
	,count(DISTINCT user_id) register_count 
	,count(DISTINCT CASE WHEN is_verified = TRUE THEN user_id END) AS kyc_count 
FROM	
	(
		SELECT 
		um.user_id 
		,signup_hostcountry
		,signup_platform 
		,is_verified 
		,DATE_TRUNC ('month',um.created_at) AS created_at
		FROM analytics.users_master um
		WHERE signup_hostcountry NOT IN ('test','error','xbullion') --AND signup_platform IS NOT NULL
		GROUP BY 1,2,3,4,5
		) a
GROUP BY 1,2,3
ORDER BY 1 
)
SELECT b.created_at 
	, b.signup_hostcountry 
	, b.signup_platform 
	, register_count 
	, kyc_count 
FROM base b 
ORDER BY 2,1 




---- KYC/ register real-time
WITH base AS (
SELECT 
	DATE_TRUNC('day', u.inserted_at) register_date 
	, CASE WHEN o.level_increase_status = 'pass' THEN DATE_TRUNC('day', o.updated_at) END AS kyc_date  
	, CASE	WHEN signup_hostname IN ('au.zipmex.com', 'trade.zipmex.com.au') THEN 'AU'
			WHEN signup_hostname IN ('id.zipmex.com', 'trade.zipmex.co.id') THEN 'ID'
			WHEN signup_hostname IN ('th.zipmex.com', 'trade.zipmex.co.th') THEN 'TH'
			WHEN signup_hostname IN ('sg.zipmex.com', 'exchange.zipmex.com', 'trade.zipmex.com') THEN 'global'
			WHEN signup_hostname IN ('trade.xbullion.io') THEN 'xbullion'
			WHEN signup_hostname IN ('global-staging.zipmex.com', 'localhost')	THEN 'test'
			ELSE 'error'
			END "signup_hostcountry" 
	, u.id 
	, u.email_verified_at 
	, u.mobile_number_verified_at 
	, o.level_increase_status 
FROM 
	user_app_public.users u 
	LEFT JOIN user_app_public.onfido_applicants o 
		ON u.id = o.user_id 
), new_user AS (
SELECT 
	DATE_TRUNC('month', register_date) datamonth 
	, signup_hostcountry 
	, COUNT(DISTINCT id) new_user
	, COUNT(DISTINCT CASE WHEN email_verified_at IS NOT NULL THEN id END) AS email_verified_user
FROM 
	base 
GROUP BY 1,2 
ORDER BY 2,1 DESC
), verified_user AS (
SELECT 
	DATE_TRUNC('month', kyc_date) datamonth 
	, signup_hostcountry
	, COUNT(DISTINCT CASE WHEN level_increase_status = 'pass' AND mobile_number_verified_at IS NOT NULL AND email_verified_at IS NOT NULL THEN id END) AS verified_user
FROM 
	base 
GROUP BY 1,2 
ORDER BY 2,1 DESC 
)
SELECT
	n.datamonth
	, n.signup_hostcountry
	, new_user
	, email_verified_user 
	, verified_user
	, SUM(new_user) OVER(PARTITION BY n.signup_hostcountry ORDER BY n.datamonth) total_new_user
	, SUM(email_verified_user) OVER(PARTITION BY n.signup_hostcountry ORDER BY n.datamonth) total_email_verified_user
	, SUM(verified_user) OVER(PARTITION BY v.signup_hostcountry ORDER BY v.datamonth) total_verified_user
FROM 
	new_user n 
	LEFT JOIN verified_user v 
	ON n.datamonth = v.datamonth AND n.signup_hostcountry = v.signup_hostcountry
WHERE 
	n.signup_hostcountry NOT IN ('test','error','xbullion')
ORDER BY 2,1 DESC 
;



SELECT
	DATE_TRUNC ('month',created_at) "month"
	,signup_hostcountry
	, COUNT(DISTINCT user_id) register_user
	, COUNT(DISTINCT CASE WHEN is_verified = TRUE THEN user_id END) verified_user
FROM
	analytics.users_master
WHERE
	DATE_TRUNC('day',created_at) >= '2021-07-01'
	AND signup_hostcountry IN ('TH','ID')
--AND is_email_verified IN ('TRUE')
--AND is_mobile_verified IN ('TRUE')
--AND is_onfido_verified IN ('TRUE')
--AND is_verified IN ('TRUE')
GROUP BY 1,2
ORDER BY 2,1
