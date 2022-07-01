WITH base AS (
    SELECT 
        a.created_at 
        , CASE WHEN u.signup_hostcountry IS NULL THEN 'unknown' ELSE u.signup_hostcountry END AS signup_hostcountry
        , a.ap_account_id , up.email , u.user_id 
    -- filter nominee accounts from users_mapping
        , CASE WHEN a.ap_account_id IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id <> 496001)
                THEN TRUE ELSE FALSE END AS is_nominee 
    -- filter asset_manager account
        , CASE WHEN a.ap_account_id = 496001 THEN TRUE ELSE FALSE END AS is_asset_manager
    -- zipup subscribe status to identify zipup amount
        , u.zipup_subscribed_at , u.is_zipup_subscribed 
        , a.symbol 
        , r.price usd_rate 
        , trade_wallet_amount
        , z_wallet_amount
        , ziplock_amount
        , zlaunch_amount
        , CASE  WHEN r.product_type = 1 THEN trade_wallet_amount * 1/r.price 
                WHEN r.product_type = 2 THEN trade_wallet_amount * r.price
                END AS trade_wallet_amount_usd
        , z_wallet_amount * r.price z_wallet_amount_usd
        , ziplock_amount * r.price ziplock_amount_usd
        , zlaunch_amount * r.price zlaunch_amount_usd
    FROM 
        analytics.wallets_balance_eod a 
    -- get country and join with pii data
        LEFT JOIN 
            analytics.users_master u 
            ON a.ap_account_id = u.ap_account_id 
    -- get pii data 
        LEFT JOIN 
            analytics_pii.users_pii up 
            ON u.user_id = up.user_id 
    -- coin prices and exchange rates (USD)
        LEFT JOIN 
            analytics.rates_master r 
            ON a.symbol = r.product_1_symbol
            AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
    WHERE 
        a.created_at = '2022-04-10' 
        AND a.created_at < DATE_TRUNC('day', NOW())::DATE
    -- snapshot by end of month or yesterday
--      AND ((a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
    -- exclude test products
        AND a.symbol NOT IN ('TST1','TST2')
        AND u.signup_hostcountry IN ('ID') --('AU','ID','global','TH')
--        AND a.symbol = 'ZMT'
    ORDER BY 1 DESC 
) , aum_snapshot AS (
    SELECT 
        DATE_TRUNC('month', b.created_at)::DATE created_at
        , b.signup_hostcountry
        , b.ap_account_id
        , CASE WHEN zts.vip_tier IS NULL THEN 'no_zmt' ELSE zts.vip_tier END vip_tier
        , SUM(CASE WHEN b.symbol = 'ZMT' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0) 
                    + COALESCE (ziplock_amount_usd, 0) + COALESCE (zlaunch_amount_usd, 0) END) total_zmt_usd
        , SUM(COALESCE (trade_wallet_amount_usd, 0)) trade_wallet_amount_usd
        , SUM(COALESCE (z_wallet_amount_usd, 0)) z_wallet_amount_usd
        , SUM(COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
        , SUM((COALESCE (z_wallet_amount_usd, 0) + COALESCE (ziplock_amount_usd, 0))) total_zwallet_usd
        , SUM(COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0) 
                    + COALESCE (ziplock_amount_usd, 0) + COALESCE (zlaunch_amount_usd, 0)) total_aum_usd
      , SUM( COALESCE (CASE WHEN is_zipup_subscribed = TRUE AND b.created_at >= DATE_TRUNC('day', zipup_subscribed_at) 
                            AND b.symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
                  THEN
                      (CASE   WHEN b.created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
                              WHEN b.created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
                  END, 0)) AS zwallet_subscribed_usd
    FROM 
        base b
        LEFT JOIN 
            analytics.zmt_tier_endofmonth zts 
            ON b.ap_account_id = zts.ap_account_id 
            AND DATE_TRUNC('month', b.created_at)::DATE = zts.created_at::DATE + '1 day'::INTERVAL 
    WHERE 
        is_asset_manager = FALSE AND is_nominee = FALSE
    GROUP BY 
        1,2,3,4
    ORDER BY 
        3 DESC 
)--   , temp_final AS (
SELECT 
    created_at 
    , signup_hostcountry
    , vip_tier 
--    , COUNT( CASE WHEN total_aum_usd > 0 THEN created_at END) day_with_active_balance
--    , COUNT( CASE WHEN total_aum_usd > 0 THEN created_at END) / COUNT(DISTINCT ap_account_id) avg_day_with_active_balance
    , SUM(total_aum_usd) total_aum_usd
    , COUNT(DISTINCT CASE WHEN trade_wallet_amount_usd > 0 THEN ap_account_id END) trade_wallet_user_count
        , SUM( COALESCE (trade_wallet_amount_usd, 0) ) trade_wallet_amount_usd
        , AVG( COALESCE (trade_wallet_amount_usd, 0) ) avg_trade_wallet_usd
    , COUNT(DISTINCT CASE WHEN total_zwallet_usd > 0 THEN ap_account_id END) zwallet_user_count
        , SUM(COALESCE (total_zwallet_usd, 0) ) total_zwallet_usd
        , AVG(COALESCE (total_zwallet_usd, 0) ) avg_total_zwallet_usd
    , COUNT(DISTINCT CASE WHEN COALESCE (zwallet_subscribed_usd, 0) > 0 
            THEN ap_account_id END) zipup_user_count
        , SUM( COALESCE (zwallet_subscribed_usd, 0)  ) zipup_sub_usd
        , AVG( COALESCE (zwallet_subscribed_usd, 0)  ) avg_zipup_sub_usd
    , COUNT(DISTINCT CASE WHEN COALESCE (ziplock_amount_usd, 0) > 0 
            THEN ap_account_id END) ziplock_user_count
        , SUM( COALESCE (ziplock_amount_usd, 0) ) ziplock_sub_usd
        , AVG( COALESCE (ziplock_amount_usd, 0) ) avg_ziplock_sub_usd
    , COUNT(DISTINCT CASE WHEN total_zmt_usd > 0 THEN ap_account_id END) zmt_holder_count
        , SUM( COALESCE (total_zmt_usd, 0) ) total_zmt_usd
FROM aum_snapshot
WHERE 
	total_aum_usd >= 1
	AND vip_tier NOT IN ('vip4','vip3','vip2','vip1')
GROUP BY 1,2,3 
;


