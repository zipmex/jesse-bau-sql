--Trades--
select distinct
	date_trunc('month',t.created_at) 
	--,t.trade_id 
	--,t.account_id 
	--,t.counter_party 
	,case when t.ap_account_id = '1356' then 'seedfive' when t.ap_account_id = '1357' then 'seedsix' when t.ap_account_id = '161347' then 'zmt.trader' else 'customers' end "Account"
	,case	when t.product_2_symbol = 'IDR' then 'ID' 
			when t.product_2_symbol = 'THB' then 'TH'
			when t.product_2_symbol = 'AUD' then 'AU'
			when t.product_2_symbol = 'SGD' then 'global'
			when t.product_2_symbol = 'USD' then 'global'
			when t.product_2_symbol = 'USDT' then 'global'
			else u.signup_hostcountry end "Reporting Country"
	,t.signup_hostcountry 
	--,t.side 
	,t.product_1_symbol 
	,t.product_2_symbol 
	,sum(t.amount_usd) "USD Value"
	,sum(case when t.counter_party in ('0', '40706', '40683', '38263', '38262', '38260', '38121', '37955', '37807','161347') then t.amount_usd else 0 end) "non-organic USD"
	,sum(case when t.counter_party not in ('0', '40706', '40683', '38263', '38262', '38260', '38121', '37955', '37807','161347') then t.amount_usd else 0 end) "organic USD"
	,sum(case when t.product_1_symbol = 'ZMT' then 0 else t.amount_usd end) "non-ZMT"
	,sum(t.quantity) "Quantity"
	,sum(case when t.created_at < '2021-07-01' then t.amount_usd*0.0005 else t.amount_usd*0.0004 end) "AP Cost"
from analytics.trades_master t
	left join analytics.users_master u on t.ap_account_id = u.ap_account_id 
where 
	date_trunc('day',t.created_at) >= '2021-07-01' and date_trunc('day',t.created_at) < '2021-08-25'
	--and t.counter_party in ('0', '40706', '40683', '38263', '38262', '38260', '38121', '37955', '37807','161347')
	--and t.account_id not in ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227','38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','49659','49658','52018','52019','44057','161347')
	--and pp.symbol in ('BABA','TSLA','AAPL','AMZN','GOOGL','FB','NFLX','PYPL','TWTR','ZM','ABNB')
	and u.signup_hostcountry not in ('test','error')
	and t.counter_party not in ('0')
	--and t.account_id in ('0')
	and t.side = 'Sell'
	and t.ap_account_id not in ('53073')
	and t.product_2_symbol not in ('BTC','TST2')
	--and t.account_id = '0'
	--and counter_party = '53073'
	group by 
	1,2,3,4,5,6
union 
select distinct
	date_trunc('month',t.created_at) 
	--,t.trade_id 
	--,t.account_id 
	--,t.counter_party 
	,case when t.ap_account_id = '1356' then 'seedfive' when t.ap_account_id = '1357' then 'seedsix' when t.ap_account_id = '161347' then 'zmt.trader' else 'customers' end "Account"
	,case	when t.product_2_symbol = 'IDR' then 'ID' 
			when t.product_2_symbol = 'THB' then 'TH'
			when t.product_2_symbol = 'AUD' then 'AU'
			when t.product_2_symbol = 'SGD' then 'global'
			when t.product_2_symbol = 'USD' then 'global'
			when t.product_2_symbol = 'USDT' then 'global'
			else u.signup_hostcountry end "Reporting Country"
	,t.signup_hostcountry 
	--,t.side 
	,t.product_1_symbol 
	,t.product_2_symbol 
	,sum(t.amount_usd) "USD Value"
	,sum(case when t.counter_party in ('0', '40706', '40683', '38263', '38262', '38260', '38121', '37955', '37807','161347') then t.amount_usd else 0 end) "non-organic USD"
	,sum(case when t.counter_party not in ('0', '40706', '40683', '38263', '38262', '38260', '38121', '37955', '37807','161347') then t.amount_usd else 0 end) "organic USD"
	,sum(case when t.product_1_symbol = 'ZMT' then 0 else t.amount_usd end) "non-ZMT"
	,sum(t.quantity) "Quantity"
	,sum(case when t.created_at < '2021-07-01' then t.amount_usd*0.0005 else t.amount_usd*0.0004 end) "AP Cost"
from analytics.trades_master t
	left join analytics.users_master u on t.ap_account_id = u.ap_account_id 
where 
	date_trunc('day',t.created_at) >= '2021-07-01' and date_trunc('day',t.created_at) < '2021-08-25'
	--and t.counter_party in ('0', '40706', '40683', '38263', '38262', '38260', '38121', '37955', '37807','161347')
	--and t.account_id not in ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227','38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','49659','49658','52018','52019','44057','161347')
	--and pp.symbol in ('BABA','TSLA','AAPL','AMZN','GOOGL','FB','NFLX','PYPL','TWTR','ZM','ABNB')
	and u.signup_hostcountry not in ('test','error')
	and t.counter_party in ('0')
	--and t.account_id in ('0')
	--and t.side = 'Sell'
	and t.ap_account_id not in ('53073')
	and t.product_2_symbol not in ('BTC','TST2')
	--and t.account_id = '0'
	--and counter_party = '53073'
	group by 
	1,2,3,4,5,6
	order by 1 asc
;