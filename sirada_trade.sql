select distinct 
    account_id 
    ,email 
    ,tick_to_timestamp("time_stamp") + interval '7 hour' "timestamp"
    ,me.symbol
    ,case   when transaction_type = 1 then 'Fee'
            when transaction_type = 2 then 'Trade'
            when transaction_type = 3 then 'Other'
            when transaction_type = 4 then 'Reverse'
            when transaction_type = 5 then 'Hold'
            when transaction_type = 6 then 'Rebate'
            when transaction_type = 7 then 'MarginAcquisition'
            when transaction_type = 8 then 'MarginRelinquish' end transaction_type 
    ,case   when transaction_reference_type = 1 then 'Trade'
            when transaction_reference_type = 2 then 'Deposit'
            when transaction_reference_type = 3 then 'Withdraw'
            when transaction_reference_type = 4 then 'Transfer'
            when transaction_reference_type = 5 then 'OrderHold'
            when transaction_reference_type = 6 then 'WithdrawHold'
            when transaction_reference_type = 7 then 'DepositHold'
            when transaction_reference_type = 8 then 'MarginHold'
            when transaction_reference_type = 9 then 'ManualHold'
            when transaction_reference_type = 10 then 'ManualEntry'
            when transaction_reference_type = 11 then 'MarginAquisition'
            when transaction_reference_type = 12 then 'MarginRelinquish'
            when transaction_reference_type = 13 then 'MarginQuoteHold' end reference_type
    ,credit_amount
    ,debit_amount
    ,tm.price 
    ,balance
from 
(
    select (t.transaction_id),t.transaction_reference_type,t.account_id
        , t.credit_amount,t.debit_amount ,t.transaction_type,t.product_id
        ,p.symbol, t.balance,t."time_stamp" , u.email 
        ,t.transaction_reference_id
        from apex.account_transactions_vw t
            left join analytics_pii.users_pii u on t.account_id = u.ap_account_id
            left join apex.products p on t.product_id = p.product_id
    where t.account_id = 890293
) as me
    LEFT JOIN 
        analytics.trades_master tm 
        ON me.transaction_reference_id = tm.trade_id 
order by 3 desc
;


SELECT * FROM analytics.trades_master tm 