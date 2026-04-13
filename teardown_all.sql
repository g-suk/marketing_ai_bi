/*=============================================================================
  Summit Gear Co. -- Marketing AI+BI Lab
  teardown_all.sql
  
  Cleanly removes all lab resources. Run as ACCOUNTADMIN.
=============================================================================*/

USE ROLE ACCOUNTADMIN;

DROP DATABASE IF EXISTS MARKETING_AI_BI;
DROP ROLE IF EXISTS MARKETING_LAB_ROLE;
