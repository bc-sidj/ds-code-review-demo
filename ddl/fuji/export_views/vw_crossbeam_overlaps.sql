CREATE OR REPLACE VIEW EXPORT_VIEWS.VW_CROSSBEAM_OVERLAPS (
    ENTITY_ID COMMENT 'CMR ID of the account/lead.',
    ENTITY_TYPE COMMENT 'Type of entity (account or lead).',
    POPULATION_NAME COMMENT 'Name of population.',
    PARTNER_NAME COMMENT 'Partner of the organization.',
    PARTNER_POPULATION_NAME COMMENT 'The name of the partner''s population.',
    PARTNER_DOMAIN COMMENT 'Domain of partner org based on the website from registration.',
    PARTNER_AE_EMAIL COMMENT 'The partner''s AE email.',
    PARTNER_AE_NAME COMMENT 'The partner''s AE name.',
    PARTNER_AE_PHONE COMMENT 'The partner''s AE phone.',
    MATCHED_AT COMMENT 'The time the overlap was first visible in Crossbeam.',
    UPDATED_AT COMMENT 'Last time the overlap record was updated in Snowflake.',
    CREATED_AT COMMENT 'When the overlap records were created in Snowflake.',
    PARTNER_AE_TITLE COMMENT 'The partner''s AE title.',
    PARTNER_RECORD_NAME COMMENT 'The name of the partner record.',
    PARTNER_RECORD_WEBSITE COMMENT 'The website associated with the partner record.',
    PARTNER_RECORD_TYPE COMMENT 'The type of the partner record (e.g., organization, individual).',
    PARTNER_RECORD_COUNTRY COMMENT 'The country associated with the partner record.',
    PARTNER_RECORD_INDUSTRY COMMENT 'The industry associated with the partner record.',
    PARTNER_RECORD_EMPLOYEES  COMMENT 'The number of employees at the partner''s organization.'
) COMMENT = 'Crossbeam overlaps data to export to Actively. Created as per DS-7058.'
AS
SELECT
    entity_id,
    entity_type,
    population_name,
    partner_name,
    partner_population_name,
    partner_domain,
    partner_ae_email,
    partner_ae_name,
    partner_ae_phone,
    matched_at::TIMESTAMP_NTZ,
    updated_at::TIMESTAMP_NTZ,
    created_at::TIMESTAMP_NTZ,
    partner_ae_title,
    partner_record_name,
    partner_record_website,
    partner_record_type,
    partner_record_country,
    partner_record_industry,
    partner_record_employees
FROM crossbeam.crossbeam.overlaps
WHERE GREATEST(created_at, updated_at, matched_at) >= CURRENT_DATE() - 7;
