//https://docs.snowflake.com/en/user-guide/tutorials/sensitive-data-auto-classification#introduction

USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS governance_db;
CREATE SCHEMA IF NOT EXISTS governance_db.sch;
CREATE WAREHOUSE IF NOT EXISTS tutorial_wh;

CREATE DATABASE IF NOT EXISTS data_db;
CREATE SCHEMA IF NOT EXISTS data_db.sch;

CREATE TABLE data_db.sch.customers (
  account_number NUMBER(38,0),
  first_name VARCHAR(16777216),
  last_name VARCHAR(16777216),
  email VARCHAR(16777216)
);

USE WAREHOUSE tutorial_wh;

INSERT INTO data_db.sch.customers (account_number, first_name, last_name, email)
  VALUES
    (1589420, 'john', 'doe', 'john.doe@example.com'),
    (2834123, 'jane', 'doe', 'jane.doe@example.com'),
    (4829381, 'jim', 'doe', 'jim.doe@example.com'),
    (9821802, 'susan', 'smith', 'susan.smith@example.com'),
    (8028387, 'bart', 'simpson', 'bart.barber@example.com');

CREATE OR REPLACE SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE
  governance_db.sch.my_classification_profile(
      {
        'minimum_object_age_for_classification_days': 0,
        'maximum_classification_validity_days': 30,
        'auto_tag': true
      });

CREATE TAG governance_db.sch.tutorial_pii;

CALL governance_db.sch.my_classification_profile!SET_TAG_MAP(
  {'column_tag_map':[
    {
      'tag_name':'governance_db.sch.tutorial_pii',
      'tag_value':'sensitive_name',
      'semantic_categories':['NAME']
    }]});

ALTER SCHEMA data_db.sch
  SET CLASSIFICATION_PROFILE = 'governance_db.sch.my_classification_profile';

//wait for 1 hour
 
CALL SYSTEM$GET_CLASSIFICATION_RESULT('data_db.sch.customers');

//To Drop
/*
DROP TAG governance_db.sch.tutorial_pii;
DROP DATABASE governance_db;
DROP DATABASE data_db;
DROP WAREHOUSE tutorial_wh;
*/
