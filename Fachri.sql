--USDT NEW 
SELECT
date_trunc('day', u.created_at) AS "Register_Date"
,d.details ->> 'type' AS "Deposit_Via"
,dt.details ->> 'type' AS "Referrer Deposit_Via"
,ur.invited_user_id
,ur.referring_user_id
,u.email
,um.email AS "Referrer Email"
,u.is_email_verified
,u.has_started_mobile_verification 
,u.is_mobile_verified
,u.has_started_onfido 
,u.is_onfido_verified 
,u.is_verified
,CASE WHEN d.amount >= 300000 THEN TRUE ELSE FALSE END "has_deposit > 300K"
,u.has_traded 
,CASE WHEN d.amount >= 300000 AND u.sum_trade_volume_usd * 14450 >= 300000 THEN 'Eligible' ELSE 'Not Eligible' END "Airdrop_Staus"
,um.is_verified AS "Referrer Verified"
,CASE WHEN dt.amount > 0 THEN TRUE ELSE FALSE END "Referrer deposit"
,um.has_traded "Referrer Traded"
,CASE WHEN um.has_deposited IS TRUE AND um.has_traded IS true THEN 'Eligible' ELSE 'Not Eligible' END "Referrer_Staus"
,um.signup_hostcountry "Referrer Signup"
,d.amount AS "Deposit"
,round(u.sum_trade_volume_usd * 14450,0) AS "Sum Trade"
,sum(dt.amount) AS "Referrer Deposit"
,round(um.sum_trade_volume_usd * 14450,0) AS "Referrer Sum Trade"
FROM 
analytics.users_master u
LEFT JOIN 
user_app_public.user_referrals ur
ON u.user_id = ur.invited_user_id
LEFT JOIN analytics.users_master um ON um.user_id = ur.referring_user_id
LEFT JOIN wallets_app_public.deposit_tickets d ON d.user_id = ur.invited_user_id 
LEFT JOIN wallets_app_public.deposit_tickets dt ON dt.user_id = um.user_id 
WHERE u.user_id IN 
(SELECT ur.invited_user_id FROM user_app_public.user_referrals ur WHERE ur.referring_user_id IS NOT NULL)
AND date_trunc('day', u.created_at) >= '2021-07-28' AND date_trunc('day', u.created_at) <= '2021-07-29'
AND u.signup_hostcountry = 'ID'
AND um.signup_hostcountry = 'ID'
AND d.state IN ('fully_processed')
AND d.ap_product_id = 8
AND d.amount >= 300000
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,25