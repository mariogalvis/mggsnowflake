/*Entrene un modelo llamado DOC_AI_QS_CO_BRANDING en DOC_AI_SCHEMA con los files de training

effective_date
What is the effective date of the agreement?
parties
Who are the parties involved in the agreement?
duration
What is the duration of the agreement?
notice_period
What is the notice period for termination?
indemnification_clause
Is there an indemnification clause?
renewal_options
Are there any renewal options or conditions mentioned?
force_majeure
Is there a force majeure clause?
payment_terms
What are the payment terms, including amounts, timing, and conditions?
*/

USE DATABASE TB_DOC_AI;
USE SCHEMA DOC_AI_SCHEMA;
USE WAREHOUSE VW_DOCAI;

ALTER WAREHOUSE VW_DOCAI
SET WAREHOUSE_SIZE = XLARGE 
    WAIT_FOR_COMPLETION = TRUE;

-- Create a table with all values and scores
CREATE OR REPLACE TABLE tb_doc_ai.doc_ai_schema.CO_BRANDING_AGREEMENTS
AS
WITH 
-- First part gets the result from applying the model on the pdf documents as a JSON with additional metadata
temp as(
    SELECT 
        Relative_path as file_name
        , size as file_size
        , last_modified
        , file_url as snowflake_file_url
        -- VERIFY THAT BELOW IS USING THE SAME NAME AND NUMER AS THE MODEL INSTRUCTIONS YOU COPIED IN THE PREVIOUS STEP!
        ,  tb_doc_ai.doc_ai_schema.DOC_AI_QS_CO_BRANDING!PREDICT(get_presigned_url('@doc_ai_stage', RELATIVE_PATH ), 1) as json
    from directory(@doc_ai_stage)
)
-- Second part extract the values and the scores from the JSON into columns
SELECT
file_name
, file_size
, last_modified
, snowflake_file_url
, json:__documentMetadata.ocrScore::FLOAT AS ocrScore
, json:parties::ARRAY as parties_array
, ARRAY_SIZE(parties_array) AS identified_parties
, json:effective_date[0]:score::FLOAT AS effective_date_score
, json:effective_date[0]:value::STRING AS effective_date_value
, json:duration[0]:score::FLOAT AS agreement_duration_score
, json:duration[0]:value::STRING AS agreement_duration_value
, json:notice_period[0]:score::FLOAT AS notice_period_score
, json:notice_period[0]:value::STRING AS notice_period_value
, json:payment_terms[0]:score::FLOAT AS payment_terms_score
, json:payment_terms[0]:value::STRING AS payment_terms_value
, json:force_majeure[0]:score::FLOAT AS have_force_majeure_score
, json:force_majeure[0]:value::STRING AS have_force_majeure_value
, json:indemnification_clause[0]:score::FLOAT AS have_indemnification_clause_score
, json:indemnification_clause[0]:value::STRING AS have_indemnification_clause_value
, json:renewal_options[0]:score::FLOAT AS have_renewal_options_score
, json:renewal_options[0]:value::STRING AS have_renewal_options_value
FROM temp;
-- Scale down the WH
ALTER WAREHOUSE VW_DOCAI
SET WAREHOUSE_SIZE = XSMALL 
    WAIT_FOR_COMPLETION = TRUE;


select * from tb_doc_ai.doc_ai_schema.CO_BRANDING_AGREEMENTS;
