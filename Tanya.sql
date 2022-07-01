--user info: name, age, email
--VIP tier: calculate from ziplock amount (wallets_master)
--Trade volume: total life time (trades_master)
--Last active: open app
--Location: provinces in Thailand (suitability_surveys)
-- scope: TH

WITH user_base AS (
	SELECT
		um.user_id 
		, um.ap_account_id 
		, up.email 
		, up.mobile_number 
		, up.first_name 
		, up.last_name 
		, um.created_at::DATE registered_date
		, um.verification_approved_at::DATE verified_date
		, um.sum_trade_volume_usd life_time_trade_volume_usd
	FROM analytics.users_master um 
		LEFT JOIN analytics_pii.users_pii up 
			ON um.user_id = up.user_id 
	WHERE 
		up.email IN ('miranda.alexa.ng@gmail.com',	'akalarp@zipmex.com',	'james.tippett@gmail.com',	'mlim.marcus@gmail.com',	'nicolas.keravec@me.com',	'pav@zipmex.com',	'araya.hutasuwan@gmail.com',	'calvin.ng@aura.co',	'parkinspire@gmail.com',	'v.pillay@bluehill.com.sg',	'azipvest@gmail.com',	'chaipromprasithpi@gmail.com',	'chayanit.seesan+1@gmail.com',	'francois.monteleon+zipmex@gmail.com',	'kulvaree.trader@gmail.com',	'mook43395@gmail.com',	'pimkanokpiamjariyakul@gmail.com',	'rycrypto@protonmail.com',	'sohliyin@gmail.com',	'tanneishappy@gmail.com',	'tannophotos@gmail.com',	'trikun.srihongse@gmail.com',	'archie.hong+card@zipmex.com',	'kulvaree.trader@gmail.com',	'tthipmart@gmail.com',	'c.kittisowan@outlook.com',	'crystal.ma.lee@icloud.com',	'talamiram@gmail.com',	'alanjnchua@gmail.com',	'cckklam@gmail.com',	'daniel.scinto@gmail.com',	'dwad.lane@gmail.com',	'gabriele.bandi@gmail.com',	'jlxy1988@gmail.com',	'laurinburg@hotmail.com',	'lxx.sam@gmail.com',	'nagendra.iitms@gmail.com',	'rfpchua@gmail.com',	'rwbchwee@gmail.com',	'ryan@cryptogrinders.com',	'sagar.sambrani@gmail.com',	'sim.jian.hong@gmail.com',	'watson.christopher6@gmail.com',	'wcmwongmei@gmail.com',	'yesheg@gmail.com',	'aruehrig@outlook.com',	'aslamghouse@gmail.com',	'chong.alvin@gmail.com',	'chu.daniel88@gmail.com',	'cmhchoi@gmail.com',	'georg@griesemann.com',	'jun@ptrk.com',	'kenzo@bluetouch.xyz',	'lawrenceh2004@yahoo.de',	'lee.ilin@gmail.com',	'limweide1987@gmail.com',	'm-nakayama@maripoza.com',	'mankk8271@gmail.com',	'mingteck.kong@gmail.com',	'ongchoonpeng@hotmail.com',	'pathom@pyi.co.th',	'paul.kewell@thalesgroup.com',	'peter-phun@runbox.com',	'sayuzbasak@gmail.com',	'shaun@dreamcore.com.sg',	'takahs@gmail.com')
--		AND um.has_traded IS TRUE
)	, user_province AS (
	SELECT 
		pi2.user_id 
		, info ->> 'work_address_province' work_address_province
		, info ->> 'present_address_province' present_address_province
		, info ->> 'address_in_id_card_province' address_in_id_card_province
	FROM user_app_public.personal_infos pi2 
	WHERE pi2.archived_at IS NULL
)	, session_base AS (
	SELECT 
		user_id 
		, session_start_ts last_login_at
		, RANK() OVER(PARTITION BY user_id ORDER BY session_start_ts DESC) rank_
	FROM analytics.sessions_master sm 
)
SELECT 
	um.*
	, up.work_address_province
	, up.present_address_province
	, up.address_in_id_card_province
	, sb.last_login_at
	, CASE WHEN NOW()::DATE - last_login_at::DATE BETWEEN 0 AND 30 THEN 'A_30_day'
			WHEN NOW()::DATE - last_login_at::DATE BETWEEN 31 AND 60 THEN 'B_30-60_day'
			WHEN NOW()::DATE - last_login_at::DATE BETWEEN 61 AND 90 THEN 'C_60-90_day'
			WHEN NOW()::DATE - last_login_at::DATE BETWEEN 91 AND 180 THEN 'D_90-180_day'
			ELSE 'E_>_180_day'
			END AS last_active_period
	, CASE WHEN ult.tier_name IS NULL THEN 'vip0' ELSE ult.tier_name END AS vip_tier
	, ult.zmt_balance zmt_lock_balance
FROM 
	user_base um 
	LEFT JOIN user_province up 
		ON um.user_id = up.user_id 
	LEFT JOIN session_base sb 
		ON um.user_id = sb.user_id
		AND sb.rank_ = 1
	LEFT JOIN zip_lock_service_public.user_loyalty_tiers ult 
		ON um.user_id = ult.user_id 
ORDER BY 1 DESC 
;


, session_base AS (
SELECT 
	user_id 
	, session_start_ts last_login_at
	, RANK() OVER(PARTITION BY user_id ORDER BY session_start_ts DESC) rank_
FROM analytics.sessions_master sm 
)	, 
SELECT 
	*
FROM session_base
WHERE rank_ = 1
WHERE user_id IS NOT NULL 
;


SELECT 
	up.email 
	, up.ap_account_id 
	, up.mobile_number 
	, pi2.info ->> 'address_in_id_card_country' address_in_id_card_country
	, pi2.info ->> 'work_address_country' work_address_country
	, pi2.info ->> 'present_address_country' present_address_country
	, ss.survey ->> 'present_address_postal_code' present_address_postal_code
	, ss.survey ->> 'permanent_address' permanent_address
FROM analytics_pii.users_pii up 
	LEFT JOIN user_app_public.personal_infos pi2 
		ON up.user_id = pi2.user_id 
		AND pi2.archived_at IS NULL
	LEFT JOIN user_app_public.suitability_surveys ss 
		ON up.user_id = ss.user_id 
		AND ss.archived_at IS NULL
WHERE email IN ('akalarp@zipmex.com',	'alanjnchua@gmail.com',	'araya.hutasuwan@gmail.com',	'archie.hong+card@zipmex.com',	'aruehrig@outlook.com',	'ashley.guo@live.com.au',	'aslamghouse@gmail.com',	'azipvest@gmail.com',	'calvin.ng@aura.co',	'cckklam@gmail.com',	'chaipromprasithpi@gmail.com',	'chayanit.seesan+1@gmail.com',	'chong.alvin@gmail.com',	'chu.daniel88@gmail.com',	'cmhchoi@gmail.com',	'daniel.scinto@gmail.com',	'dwad.lane@gmail.com',	'francois.monteleon+zipmex@gmail.com',	'gabriele.bandi@gmail.com',	'georg@griesemann.com',	'james.tippett@gmail.com',	'james924604@gmail.com',	'jlxy1988@gmail.com',	'jonathanyclow@gmail.com',	'joshuapancakes@gmail.com',	'jun@ptrk.com',	'kentabuki+030587@gmail.com',	'kenzo@bluetouch.xyz',	'kiufung33@gmail.com',	'klsielecki@outlook.com',	'kulvaree.trader@gmail.com',	'kulvaree.trader@gmail.com',	'laurinburg@hotmail.com',	'lawrenceh2004@yahoo.de',	'lee.ilin@gmail.com',	'limweide1987@gmail.com',	'lxx.sam@gmail.com',	'm-nakayama@maripoza.com',	'mankk8271@gmail.com',	'mingteck.kong@gmail.com',	'mlim.marcus@gmail.com',	'mook43395@gmail.com',	'nagendra.iitms@gmail.com',	'nicolas.keravec@me.com',	'np.itzstein@gmail.com',	'olivier.tang@outlook.com',	'ongchoonpeng@hotmail.com',	'parkinspire@gmail.com',	'pathom@pyi.co.th',	'paul.kewell@thalesgroup.com',	'peter-phun@runbox.com',	'pimkanokpiamjariyakul@gmail.com',	'proud.limpongpan@gmail.com',	'rfpchua@gmail.com',	'rwbchwee@gmail.com',	'ryan@cryptogrinders.com',	'rycrypto@protonmail.com',	'sagar.sambrani@gmail.com',	'sanchitjn@gmail.com',	'sayuzbasak@gmail.com',	'scot.cheung@gmail.com',	'shaun@dreamcore.com.sg',	'sim.jian.hong@gmail.com',	'sohliyin@gmail.com',	'takahs@gmail.com',	'tanneishappy@gmail.com',	'tannophotos@gmail.com',	'tanyaluckt@gmail.com',	'trikun.srihongse@gmail.com',	'v.pillay@bluehill.com.sg',	'watson.christopher6@gmail.com',	'wcmwongmei@gmail.com',	'yesheg@gmail.com')
;



SELECT 
	u.email 
	, u.id 
FROM user_app_public.users u 
WHERE
	u.email IN ('miranda.alexa.ng@gmail.com',	'akalarp@zipmex.com',	'james.tippett@gmail.com',	'mlim.marcus@gmail.com',	'nicolas.keravec@me.com',	'pav@zipmex.com',	'araya.hutasuwan@gmail.com',	'calvin.ng@aura.co',	'parkinspire@gmail.com',	'v.pillay@bluehill.com.sg',	'azipvest@gmail.com',	'chaipromprasithpi@gmail.com',	'chayanit.seesan+1@gmail.com',	'francois.monteleon+zipmex@gmail.com',	'kulvaree.trader@gmail.com',	'mook43395@gmail.com',	'pimkanokpiamjariyakul@gmail.com',	'rycrypto@protonmail.com',	'sohliyin@gmail.com',	'tanneishappy@gmail.com',	'tannophotos@gmail.com',	'trikun.srihongse@gmail.com',	'archie.hong+card@zipmex.com',	'kulvaree.trader@gmail.com',	'tthipmart@gmail.com',	'c.kittisowan@outlook.com',	'crystal.ma.lee@icloud.com',	'talamiram@gmail.com',	'alanjnchua@gmail.com',	'cckklam@gmail.com',	'daniel.scinto@gmail.com',	'dwad.lane@gmail.com',	'gabriele.bandi@gmail.com',	'jlxy1988@gmail.com',	'laurinburg@hotmail.com',	'lxx.sam@gmail.com',	'nagendra.iitms@gmail.com',	'rfpchua@gmail.com',	'rwbchwee@gmail.com',	'ryan@cryptogrinders.com',	'sagar.sambrani@gmail.com',	'sim.jian.hong@gmail.com',	'watson.christopher6@gmail.com',	'wcmwongmei@gmail.com',	'yesheg@gmail.com',	'aruehrig@outlook.com',	'aslamghouse@gmail.com',	'chong.alvin@gmail.com',	'chu.daniel88@gmail.com',	'cmhchoi@gmail.com',	'georg@griesemann.com',	'jun@ptrk.com',	'kenzo@bluetouch.xyz',	'lawrenceh2004@yahoo.de',	'lee.ilin@gmail.com',	'limweide1987@gmail.com',	'm-nakayama@maripoza.com',	'mankk8271@gmail.com',	'mingteck.kong@gmail.com',	'ongchoonpeng@hotmail.com',	'pathom@pyi.co.th',	'paul.kewell@thalesgroup.com',	'peter-phun@runbox.com',	'sayuzbasak@gmail.com',	'shaun@dreamcore.com.sg',	'takahs@gmail.com')
