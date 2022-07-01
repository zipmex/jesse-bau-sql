WITH base AS (
	SELECT 
		ap_receiver_account_id 
		, um.signup_hostcountry 
		, tt.product_id 
		, COUNT(DISTINCT tt.ap_account_id) transfer_account
		, SUM(received_amount) received_amount
		, COUNT(DISTINCT ap_transfer_id) transfer_ticket_count
		, SUM(fee_amount) sum_fee_amount
	FROM wallet_app_public.transfer_tickets tt 
		LEFT JOIN analytics.users_master um
			ON tt.ap_receiver_account_id = um.ap_account_id 
	WHERE 
		um.signup_hostcountry = 'ID'
		AND state = 'fully_processed'
	GROUP BY 
		1,2,3
	ORDER BY 
		transfer_account DESC
)	, pool_grouping AS (
	SELECT 
		ap_receiver_account_id 
		, CASE WHEN transfer_account 	< 5 THEN '0_less_than_5_child'
			WHEN transfer_account 	< 10 THEN 'A_less_than_10_child'
			WHEN transfer_account 	< 30 THEN 'B_10_30_child'
			WHEN transfer_account 	< 50 THEN 'C_30_50_child'
			WHEN transfer_account 	< 100 THEN 'D_50_100_child'
			WHEN transfer_account 	< 500 THEN 'E_100_500_child'
			WHEN transfer_account 	>= 500 THEN 'F_>_500_child'
			END AS pool_group
		, COUNT(DISTINCT ap_receiver_account_id) pool_count
	FROM base b
	GROUP BY 1,2
)--	, referral_sender AS (
	SELECT 
		pool_group
--		tt2.ap_receiver_account_id 
--		, tt2.ap_account_id child_account
--		, um2.referring_user_id clone_account
--		, um2.invitation_code 
		, COUNT(DISTINCT tt2.ap_receiver_account_id ) parent_count
		, COUNT(DISTINCT tt2.ap_account_id) child_count
	FROM pool_grouping b
		LEFT JOIN wallet_app_public.transfer_tickets tt2 
			ON b.ap_receiver_account_id = tt2.ap_receiver_account_id 
		LEFT JOIN analytics.users_master um2 
			ON tt2.ap_account_id = um2.ap_account_id 
--	WHERE tt2.ap_receiver_account_id IN (349824,421316)
	GROUP BY 1

	)
SELECT 
	ap_receiver_account_id
	, u.ap_account_id clone_account_id
	, r.invitation_code clone_invitation_code
	, u.email 
	, child_count
	, SUM(child_count) OVER(PARTITION BY ap_receiver_account_id ORDER BY child_count) cumulative_count
	, SUM(child_count) OVER(ORDER BY child_count) cumulative_count
FROM referral_sender r
	LEFT JOIN analytics.users_master u
		ON r.clone_account = u.user_id 
ORDER BY 1, 4, 5 DESC