SELECT 
	user_id 
	, info ->> 'permanent_address' permanent_address
	, info ->> 'permanent_address_postal_code' permanent_address_postal_code
	, info ->> 'present_address' present_address
	, info ->> 'present_address_postal_code' present_address_postal_code
	, info ->> 'address_in_id_card' address_in_id_card
	, info ->> 'address_in_id_card_postal_code' address_in_id_card_postal_code
FROM user_app_public.personal_infos pi2 
WHERE archived_at IS NULL 
;


SELECT
	user_id 
	, first_name 
	, last_name 
	, document_type 
	, document_number 
	, dob 
	, country document_country
FROM user_app_public.onfido_documents od 
WHERE archived_at IS NULL 
;


SELECT
	user_id 
	, frankie_entity_id 
	, updated_at 
	, inserted_at 
	, COUNT(*) duplicate_check
FROM user_app_public.applicant_data ad 
GROUP BY 1,2,3,4
ORDER BY 5 DESC 
;


WITH survey_info AS (
	SELECT 
		user_id 
		, info ->> 'permanent_address' permanent_address
		, info ->> 'permanent_address_postal_code' permanent_address_postal_code
		, info ->> 'present_address' present_address
		, info ->> 'present_address_postal_code' present_address_postal_code
		, info ->> 'address_in_id_card' address_in_id_card
		, info ->> 'address_in_id_card_postal_code' address_in_id_card_postal_code
	FROM user_app_public.personal_infos pi2 
	WHERE archived_at IS NULL 
)	, frankie_info AS (
	SELECT DISTINCT 
		user_id 
		, frankie_entity_id 
	FROM user_app_public.applicant_data ad 
)	, doc_info AS (
	SELECT
		user_id 
		, first_name 
		, last_name 
		, document_type 
		, document_number 
		, dob 
		, country document_country
	FROM user_app_public.onfido_documents od 
	WHERE archived_at IS NULL 
)
SELECT
	d.*
	, f.frankie_entity_id
	, s.permanent_address
	, s.permanent_address_postal_code
	, s.present_address
	, s.present_address_postal_code
	, s.address_in_id_card
	, s.address_in_id_card_postal_code
	, um.signup_hostcountry 
FROM doc_info d
	LEFT JOIN frankie_info f 
		ON d.user_id = f.user_id
	LEFT JOIN survey_info s 
		ON d.user_id = s.user_id
	LEFT JOIN analytics.users_master um 
		ON d.user_id = um.user_id 
WHERE um.signup_hostcountry IN ('AU','global')
AND d.user_id = '01EC47XSE43E03B2D2NGA7HAWA'