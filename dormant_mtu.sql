WITH base AS (
	SELECT 
		mtu_day 
		, signup_hostcountry 
		, ap_account_id 
		, mtu 
	FROM 
		analytics.dm_mtu_daily dmd 
)	, base_lag AS (
SELECT 
	*
	, LAG(mtu_day) OVER(PARTITION BY ap_account_id ORDER BY mtu_day) mtu_day_lag
FROM base 
)
SELECT 
	*
	, EXTRACT('day' FROM (mtu_day - mtu_day_lag)) inactive_time
	, CASE WHEN mtu IS FALSE THEN 'E_>_60D'
			ELSE 
		(	CASE WHEN EXTRACT('day' FROM (mtu_day - mtu_day_lag)) BETWEEN 1 AND 7 THEN 'A_active'
			WHEN EXTRACT('day' FROM (mtu_day - mtu_day_lag)) BETWEEN 7 AND 14 THEN 'B_7_14D'
			WHEN EXTRACT('day' FROM (mtu_day - mtu_day_lag)) BETWEEN 14 AND 30 THEN 'C_14_30D'
			WHEN EXTRACT('day' FROM (mtu_day - mtu_day_lag)) BETWEEN 30 AND 60 THEN 'D_30_60D'
			WHEN EXTRACT('day' FROM (mtu_day - mtu_day_lag)) > 60 THEN 'E_>_60D'
		END )
		END AS user_group
FROM base_lag
WHERE mtu_day = NOW()::DATE 

SELECT *
FROM analytics.dm_mtu_daily dmd 
WHERE ap_account_id = 986019