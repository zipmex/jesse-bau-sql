with base as (
	select t.signup_hostcountry
	,t.user_id
	,t.account_id
--	,CASE WHEN t.counter_party IN ('0','37807','37955','38121','38260','38262','38263','40683','40706','161347') THEN FALSE ELSE TRUE END "is_organic_trade"
--	,case when t.product_1_id in (16,50) then true else false end as is_zmt_trade 
	,case when t.account_id in ('15',	'221',	'634',	'746',	'1002',	'1182',	'1202',	'1272',	'1708',	'6074',	'6828',	'11284',	'16293',	'19763',	'24108',	'24315',	'25431',	'37276',	'38526',	'39858',	'40119',	'40438',	'40890',	'48300',	'51313',	'51333',	'52266',	'54172',	'54231',	'54644',	'55224',	'55660',	'57262',	'58998',	'59049',	'59693',	'62663',	'63292',	'63314',	'63914',	'66402',	'67813',	'82129',	'84431',	'84461',	'84799',	'91297',	'92285',	'93791',	'94663',	'94993',	'96434',	'96535',	'101654',	'101786',	'103488',	'103855',	'104832',	'106308',	'108014',	'127491',	'128405',	'131484',	'139503',	'141711',	'146194',	'146356',	'147984',	'157600',	'159685',	'161863',	'180376',	'183004')
			then true else false end as is_pcs
	,case when t.account_id in (1373,1432,13266,16211,16308,22576,34535,48900,53463,80871,84319) then true else false end as is_whale
	,cast(t.created_at as date) as created_at
	,sum(amount_usd) amount_usd
	,count(trade_id) num_transaction
	from oms_data.analytics.trades_master t
		left join analytics.users_master u on u.user_id = t.user_id
	where t.signup_hostcountry  not in ('test', 'error','xbullion')
	and u.account_id not in ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227',27443
,'38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','49659','49658','52018','52019','44057','161347') -- Minh 2021-05-31 ,'634'
	and t.created_at >= '2020-01-01 00:00:00' and t.created_at < '2021-06-09 00:00:00' --<<<<< CHANGE DATE HERE
	and t.product_1_id in ('16','50') -- incl/excl ZMT 
	group by 1,2,3,4,5,6 
	)
,base_m as (	
	select b.user_id, b.account_id 
		,cast(DATE_TRUNC('month', b.created_at) as date) as mon
		,is_pcs
		,is_whale
--		,is_zmt_trade 
		,signup_hostcountry 
		,sum(amount_usd) amount_usd
		,sum(num_transaction) num_transaction
	from base b
	group by 1,2,3,4,5,6
	)
select a.* 
,sum(b.amount_usd) as amount_usd_l30d
,sum(b.num_transaction) as num_transaction_l30d
,amount_usd_l365d
,num_transaction_l365d
from base_m a
	left join base b on b.user_id = a.user_id
		and b.created_at < mon
		and b.created_at >= mon - interval '1 month'
	left join (select a.* 
		,sum(c.amount_usd) as amount_usd_l365d
		,sum(c.num_transaction) as num_transaction_l365d
		from base_m a
		left join base c on c.user_id = a.user_id
		and c.created_at < mon 
		and c.created_at >= mon - interval '1 year'
		group by 1,2,3,4,5,6,7,8
		) c on c.user_id = a.user_id and c.mon = a.mon
			and c.signup_hostcountry = a.signup_hostcountry
			and c.amount_usd = a.amount_usd
			and c.num_transaction = a.num_transaction
--	where a.account_id = 143639
	group by 1,2,3,4,5,6,7,8,11,12 
	;