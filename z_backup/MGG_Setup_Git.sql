CREATE OR REPLACE DATABASE MGG_SETUP;

create or replace api integration mggsnowflake_git
    api_provider = git_https_api
    api_allowed_prefixes = ('https://github.com/mariogalvis/mggsnowflake')
    enabled = true
    allowed_authentication_secrets = all;

CREATE or replace GIT REPOSITORY mggsnowflake_git 
	ORIGIN = 'https://github.com/mariogalvis/mggsnowflake' 
	API_INTEGRATION = 'MGGSNOWFLAKE_GIT';
