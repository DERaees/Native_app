USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS ITS_NATIVEAPPS;

USE ITS_NATIVEAPPS;

CREATE SCHEMA IF NOT EXISTS SNOPTIMIZER;

CREATE OR REPLACE STAGE ITS_NATIVEAPPS.SNOPTIMIZER.SNOPTIMIZER_APP_STAGE
  file_format = (type = 'CSV' field_delimiter = '|' skip_header = 1);
