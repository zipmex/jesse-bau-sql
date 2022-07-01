/* 
 * this Datamart generate Register User and Verified User count
 * Verified Users tied to Verification_Approved_at 
 * (number will not change for daily reporting)
 * Period: DAY/ WEEK/ MONTH
 * for normal cohort user funnel reporting, pls refer to users_master_country/zipmex_summary tables in analytics schema
 */


DROP TABLE IF EXISTS warehouse.reportings_data.dm_user_funnel_daily;
DROP TABLE IF EXISTS warehouse.reportings_data.dm_user_funnel_monthly;

DROP TABLE IF EXISTS warehouse.reportings_data.dm_cm_reg_ver_user;


CREATE TABLE IF NOT EXISTS warehouse.reportings_data.dm_cm_reg_ver_user
(
    id                                      SERIAL PRIMARY KEY 
    , "period"                              VARCHAR(255)
    , register_date                         DATE
    , signup_hostcountry                    VARCHAR(255)
    , registered_user_count                 INTEGER 
    , verified_user_count                   INTEGER 
    , zipup_subscriber_count                INTEGER 
    , cumulative_registered_user            INTEGER 
    , cumulative_verified_user              INTEGER 
    , cumulative_zipup_subscriber           INTEGER 
    , zipmex_registered_user_count          INTEGER 
    , zipmex_verified_user_count            INTEGER 
);

CREATE INDEX IF NOT EXISTS idx_dm_user_funnel_daily ON warehouse.reportings_data.dm_cm_reg_ver_user 
(register_date, signup_hostcountry);


-- DAILY
DROP TABLE IF EXISTS tmp_user_funnel_daily;

CREATE TEMP TABLE tmp_user_funnel_daily AS 
(
    WITH monthly_base AS 
    (
        SELECT
            u.created_at AS register_date
            , u.onfido_completed_at verified_date 
            , u.zipup_subscribed_at zip_up_date  
            , u.signup_hostcountry 
            , u.user_id 
            , u.is_verified
            , u.is_zipup_subscribed 
        FROM 
    -- consolidation from users_master 
            analytics.users_master u
        WHERE 
    -- only 4 instances
            u.signup_hostcountry IN ('TH','ID','AU','global')  
    )   
        ,base_month AS 
    (
        SELECT 
            register_date::DATE AS register_date
            , signup_hostcountry
            , count(DISTINCT user_id) AS  registered_user_count
        FROM 
            monthly_base
        GROUP BY 1, 2
    )   
        ,base_month_z_up AS 
    (
        SELECT
            zip_up_date::DATE AS zip_up_date
            , signup_hostcountry
---> this one count the status by subscribe date, number is fixed
            , count(DISTINCT 
                        CASE WHEN zip_up_date IS NOT NULL 
                            AND is_zipup_subscribed = TRUE 
                    THEN user_id END) AS zipup_subscriber_count 
        FROM 
            monthly_base
        GROUP BY 1,2
    )   
        ,base_month_verified AS 
    (
        SELECT
            verified_date::DATE AS verified_date
            , signup_hostcountry
---> this one count the status by verified date, number is fixed
            , count(DISTINCT 
                        CASE WHEN is_verified = TRUE 
                    THEN user_id END) AS verified_user_count 
        FROM 
            monthly_base
        GROUP BY 1,2
    )
    SELECT
        'day' "period"
        , b.* 
        , k.verified_user_count
        , z.zipup_subscriber_count
    -- cumulative count for each country
        ,sum(b.registered_user_count) OVER(PARTITION BY b.signup_hostcountry ORDER BY register_date ) AS  total_registered_user
        ,sum(k.verified_user_count) OVER(PARTITION BY k.signup_hostcountry ORDER BY verified_date) AS  cumulative_verified_user
        ,sum(z.zipup_subscriber_count) OVER(PARTITION BY z.signup_hostcountry ORDER BY zip_up_date) AS  cumulative_zipup_subscriber
    -- cumulative count for whole zipmex
        ,sum(b.registered_user_count) OVER(ORDER BY register_date ) AS zipmex_registered_user_count
        ,sum(k.verified_user_count) OVER(ORDER BY register_date ) AS zipmex_verified_user_count
    FROM
        base_month b
        LEFT JOIN base_month_verified k ON  k.signup_hostcountry = b.signup_hostcountry AND k.verified_date = b.register_date 
        LEFT JOIN base_month_z_up z ON  z.signup_hostcountry = b.signup_hostcountry AND z.zip_up_date = b.register_date 
    ORDER BY 
        1 DESC ,2 DESC    
);


-- WEEKLY
DROP TABLE IF EXISTS tmp_user_funnel_weekly;

CREATE TEMP TABLE tmp_user_funnel_weekly AS 
(
    WITH monthly_base AS 
    (
        SELECT
            u.created_at AS register_date
            , u.onfido_completed_at verified_date 
            , u.zipup_subscribed_at zip_up_date  
            , u.signup_hostcountry 
            , u.user_id 
            , u.is_verified
            , u.is_zipup_subscribed 
        FROM 
    -- consolidation from users_master 
            analytics.users_master u
        WHERE 
    -- only 4 instances
            u.signup_hostcountry IN ('TH','ID','AU','global')  
    )   
        ,base_month AS 
    (
        SELECT 
            DATE_TRUNC('week', register_date)::DATE AS register_date
            , signup_hostcountry
            , count(DISTINCT user_id) AS  registered_user_count
        FROM 
            monthly_base
        GROUP BY 1, 2
    )   
        ,base_month_z_up AS 
    (
        SELECT
            DATE_TRUNC('week', zip_up_date)::DATE AS zip_up_date
            , signup_hostcountry
            ---> this one count the status by subscribe date, number is fixed
            , count(DISTINCT 
                        CASE WHEN zip_up_date IS NOT NULL 
                            AND is_zipup_subscribed = TRUE 
                    THEN user_id END) AS reporting_zipup_subscriber_count 
        FROM 
            monthly_base
        GROUP BY 1,2
    )   
        ,base_month_verified AS 
    (
        SELECT
            DATE_TRUNC('week', verified_date)::DATE AS verified_date
            , signup_hostcountry
            ---> this one count the status by verified date, number is fixed
            , count(DISTINCT 
                        CASE WHEN is_verified = TRUE 
                    THEN user_id END) AS reporting_verified_user_count 
        FROM 
            monthly_base
        GROUP BY 1,2
    )
    SELECT
        'week' "period"
        , b.* 
        , k.reporting_verified_user_count
        , z.reporting_zipup_subscriber_count
    -- cumulative count for each country
        ,sum(b.registered_user_count) OVER(PARTITION BY b.signup_hostcountry ORDER BY register_date ) AS  total_registered_user
        ,sum(k.reporting_verified_user_count) OVER(PARTITION BY k.signup_hostcountry ORDER BY verified_date) AS  cumulative_verified_user
        ,sum(z.reporting_zipup_subscriber_count) OVER(PARTITION BY z.signup_hostcountry ORDER BY zip_up_date) AS  cumulative_zipup_subscriber
    -- cumulative count for whole zipmex
        ,sum(b.registered_user_count) OVER(ORDER BY register_date ) AS zipmex_registered_user_count
        ,sum(k.reporting_verified_user_count) OVER(ORDER BY register_date ) AS zipmex_verified_user_count
    FROM
        base_month b
        LEFT JOIN base_month_verified k ON  k.signup_hostcountry = b.signup_hostcountry AND k.verified_date = b.register_date 
        LEFT JOIN base_month_z_up z ON  z.signup_hostcountry = b.signup_hostcountry AND z.zip_up_date = b.register_date 
    ORDER BY 
        1 DESC ,2 DESC    
);



-- MONTHLY
DROP TABLE IF EXISTS tmp_user_funnel_monthly;

CREATE TEMP TABLE tmp_user_funnel_monthly AS 
(
    WITH monthly_base AS 
    (
        SELECT
            u.created_at AS register_date
            , u.onfido_completed_at verified_date 
            , u.zipup_subscribed_at zip_up_date  
            , u.signup_hostcountry 
            , u.user_id 
            , u.is_verified
            , u.is_zipup_subscribed 
        FROM 
    -- consolidation from users_master 
            analytics.users_master u
        WHERE 
    -- only 4 instances
            u.signup_hostcountry IN ('TH','ID','AU','global')  
    )   
        ,base_month AS 
    (
        SELECT 
            DATE_TRUNC('month', register_date)::DATE AS register_date
            , signup_hostcountry
            , count(DISTINCT user_id) AS  registered_user_count
        FROM 
            monthly_base
        GROUP BY 1, 2
    )   
        ,base_month_z_up AS 
    (
        SELECT
            DATE_TRUNC('month', zip_up_date)::DATE AS zip_up_date
            , signup_hostcountry
            ---> this one count the status by subscribe date, number is fixed
            , count(DISTINCT 
                        CASE WHEN zip_up_date IS NOT NULL 
                            AND is_zipup_subscribed = TRUE 
                    THEN user_id END) AS reporting_zipup_subscriber_count 
        FROM 
            monthly_base
        GROUP BY 1,2
    )   
        ,base_month_verified AS 
    (
        SELECT
            DATE_TRUNC('month', verified_date)::DATE AS verified_date
            , signup_hostcountry
            ---> this one count the status by verified date, number is fixed
            , count(DISTINCT 
                        CASE WHEN is_verified = TRUE 
                    THEN user_id END) AS reporting_verified_user_count 
        FROM 
            monthly_base
        GROUP BY 1,2
    )
    SELECT
        'month' "period"
        , b.* 
        , k.reporting_verified_user_count
        , z.reporting_zipup_subscriber_count
    -- cumulative count for each country
        ,sum(b.registered_user_count) OVER(PARTITION BY b.signup_hostcountry ORDER BY register_date ) AS  total_registered_user
        ,sum(k.reporting_verified_user_count) OVER(PARTITION BY k.signup_hostcountry ORDER BY verified_date) AS  cumulative_verified_user
        ,sum(z.reporting_zipup_subscriber_count) OVER(PARTITION BY z.signup_hostcountry ORDER BY zip_up_date) AS  cumulative_zipup_subscriber
    -- cumulative count for whole zipmex
        ,sum(b.registered_user_count) OVER(ORDER BY register_date ) AS zipmex_registered_user_count
        ,sum(k.reporting_verified_user_count) OVER(ORDER BY register_date ) AS zipmex_verified_user_count
    FROM
        base_month b
        LEFT JOIN base_month_verified k ON  k.signup_hostcountry = b.signup_hostcountry AND k.verified_date = b.register_date 
        LEFT JOIN base_month_z_up z ON  z.signup_hostcountry = b.signup_hostcountry AND z.zip_up_date = b.register_date 
    ORDER BY 
        1 DESC ,2 DESC    
);


INSERT INTO warehouse.reportings_data.dm_cm_reg_ver_user ( "period", register_date, signup_hostcountry, registered_user_count, verified_user_count, zipup_subscriber_count, cumulative_registered_user, cumulative_verified_user, cumulative_zipup_subscriber, zipmex_registered_user_count, zipmex_verified_user_count)
(
SELECT * FROM tmp_user_funnel_daily
    UNION ALL 
SELECT * FROM tmp_user_funnel_weekly
    UNION ALL
SELECT * FROM tmp_user_funnel_monthly
)
;


DROP TABLE IF EXISTS tmp_user_funnel_daily;
DROP TABLE IF EXISTS tmp_user_funnel_weekly;
DROP TABLE IF EXISTS tmp_user_funnel_monthly;
