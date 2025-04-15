------------------------------------------------------
-- Create free here.com accont and get API key
------------------------------------------------------
/*
Go to https://account.here.com/ create free trial account
Once logged in go to REST APIs > Go To Access Manager or just click link
Click Create new app > give it a name > click Create app
Click Create API key
Copy newly created API key it will be used further down 
*/
------------------------------------------------------


------------------------------------------------------
-- Create DB and Schema
------------------------------------------------------
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE DATABASE DEMO_EXTERNAL_ACCESS;
CREATE SCHEMA DEMO_EXTERNAL_ACCESS.GEO;

USE DATABASE DEMO_EXTERNAL_ACCESS;
USE SCHEMA GEO;


------------------------------------------------------------------------------------------------
-- Create Network Rule
--    Network rules are used to help you control network traffic to and from your Snowflake account.  
------------------------------------------------------------------------------------------------
CREATE OR REPLACE NETWORK RULE here_gis_rule_demo
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('geocode.search.hereapi.com');


------------------------------------------------------------------------------------------------
-- Create External Access Integration
--   External access integrations allow access to external network locations from a User 
--   Defined Function (UDF) or procedure handler; they  rely on network rules.   External 
--   access integrations are securable objects and can be enabled/disabled as needed.
------------------------------------------------------------------------------------------------
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION here_gis_access_integration_demo
  ALLOWED_NETWORK_RULES = (here_gis_rule_demo)
  ENABLED = true;


------------------------------------------------------------------------------------------------
-- Create Python UDF
--   Re-usable function to call the here.com API, relies on the External Access Integration 
--   created in the step above. The function makes an HTTP request to the URL and returns the 
--   response text.
------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_url(url string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER = 'get_url'
EXTERNAL_ACCESS_INTEGRATIONS = (here_gis_access_integration_demo)
PACKAGES = ('snowflake-snowpark-python','requests')
AS
$$
import _snowflake
import requests
import json
session = requests.Session()
def get_url(url):
    response = session.get(url)
    return response.text
$$;


------------------------------------------------------------------------------------------------
-- Test the get_url UDF
--   Pass an address in the function to make sure all is working. The result will be is in 
--   json format, we are casting it to a variant  datatype
------------------------------------------------------------------------------------------------
select parse_json(get_url('https://geocode.search.hereapi.com/v1/geocode?q=4601+Collins+Ave,+Miami+Beach,+FL+33140&apiKey=replace_with_your_api_key'))::variant data;
/*
Expected JSON output:
{ "items": [ { "access": [ { "lat": 25.82012, "lng": -80.12271 } ], "address": { "city": "Miami Beach", "countryCode": "USA", "countryName": "United States", "county": "Miami-Dade", "district": "Oceanfront", "houseNumber": "4601", "label": "4601 Collins Ave, Miami Beach, FL 33140, United States", "postalCode": "33140", "state": "Florida", "stateCode": "FL", "street": "Collins Ave" }, "houseNumberType": "interpolated", "id": "here:af:streetsection:d-9hzgEw06CuHqEFvQbJwD:EAIaBDQ2MDE", "mapView": { "east": -80.12156, "north": 25.821, "south": 25.8192, "west": -80.12356 }, "position": { "lat": 25.8201, "lng": -80.12256 }, "resultType": "houseNumber", "scoring": { "fieldScore": { "city": 1, "houseNumber": 1, "postalCode": 1, "state": 1, "streets": [ 1 ] }, "queryScore": 1 }, "title": "4601 Collins Ave, Miami Beach, FL 33140, United States" } ] }
*/


-----------------------------------------------------------------------
-- Create a secret to store the API key
-----------------------------------------------------------------------
CREATE OR REPLACE SECRET here_maps_api 
TYPE = GENERIC_STRING 
SECRET_STRING = 'replace_with_your_api_key';


-----------------------------------------------------------------------
-- Update External Access Integration to leverage the secret
-----------------------------------------------------------------------
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION here_gis_access_integration_demo
  ALLOWED_NETWORK_RULES = (here_gis_rule_demo)  -- Replace with your actual network rule name
  ALLOWED_AUTHENTICATION_SECRETS = (DEMO_EXTERNAL_ACCESS.GEO.HERE_MAPS_API) -- Add this line
  ENABLED = true;


-----------------------------------------------------------------------
-- Update the UDF to use the Secret
-----------------------------------------------------------------------  
CREATE OR REPLACE FUNCTION get_url(url string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER = 'get_url'
EXTERNAL_ACCESS_INTEGRATIONS = (here_gis_access_integration_demo)
PACKAGES = ('snowflake-snowpark-python','requests')
SECRETS = ('here_api_key' = here_maps_api)  -- Add this line
AS
$$
import _snowflake
import requests
import json
session = requests.Session()
def get_url(url):
    api_key = _snowflake.get_generic_secret_string('here_api_key')  # Get the secret
    url_with_key = f"{url}&apiKey={api_key}"  # Construct the full URL
    response = session.get(url_with_key)
    return response.text
$$;


-----------------------------------------------------------------------  
-- Test the get_url UDF with Secret incorporated
-----------------------------------------------------------------------  
select parse_json(get_url('https://geocode.search.hereapi.com/v1/geocode?q=4601+Collins+Ave,+Miami+Beach,+FL+33140'))::variant data;
/*
Expected JSON output:
{ "items": [ { "access": [ { "lat": 25.82012, "lng": -80.12271 } ], "address": { "city": "Miami Beach", "countryCode": "USA", "countryName": "United States", "county": "Miami-Dade", "district": "Oceanfront", "houseNumber": "4601", "label": "4601 Collins Ave, Miami Beach, FL 33140, United States", "postalCode": "33140", "state": "Florida", "stateCode": "FL", "street": "Collins Ave" }, "houseNumberType": "interpolated", "id": "here:af:streetsection:d-9hzgEw06CuHqEFvQbJwD:EAIaBDQ2MDE", "mapView": { "east": -80.12156, "north": 25.821, "south": 25.8192, "west": -80.12356 }, "position": { "lat": 25.8201, "lng": -80.12256 }, "resultType": "houseNumber", "scoring": { "fieldScore": { "city": 1, "houseNumber": 1, "postalCode": 1, "state": 1, "streets": [ 1 ] }, "queryScore": 1 }, "title": "4601 Collins Ave, Miami Beach, FL 33140, United States" } ] }
*/


-----------------------------------------------------------------------  
-- Format json returned by get_url,
--   Pass an address in the function to make sure all is working. The result will 
--   be is in json format, we are casting it to a variant datatype
----------------------------------------------------------------------- 
with parse_json as (select parse_json(get_url('https://geocode.search.hereapi.com/v1/geocode?q=4601+Collins+Ave,+Miami+Beach,+FL+33140'))::variant data)
SELECT
    f.value:access[0].lat::float AS access_lat,
    f.value:access[0].lng::float AS access_lng,
    f.value:address.houseNumber::STRING AS address_house_number,
    f.value:address.street::STRING AS address_street,
    f.value:address.city::STRING AS address_city,
    f.value:address.state::STRING AS address_state,
    f.value:address.stateCode::STRING AS address_state_code,
    f.value:address.postalCode::STRING AS address_postalcode,
    f.value:address.county::STRING AS address_county
FROM
    parse_json, LATERAL FLATTEN(input => data:items) f;


----------------------------------------------------------------------- 
-- Some possibilities from here include:
--   Write the returned data to a table and store it in a single variant column
--   Write the returned data to a table and store it in individual fields
----------------------------------------------------------------------- 




------------------------------------------------------------------------------
------------------------------------------------------------------------------
------------------------------------------------------------------------------
--  Write the returned data to a table and store it in a single variant column
------------------------------------------------------------------------------
------------------------------------------------------------------------------
------------------------------------------------------------------------------

-------------------------------------
-- create table to store variant data
-------------------------------------
create or replace table address_variant (
address_variant_data variant);


-------------------------------------------------------------------
-- call the api insert data into address_variant table
-------------------------------------------------------------------
insert into address_variant select parse_json(get_url('https://geocode.search.hereapi.com/v1/geocode?q=4601+Collins+Ave,+Miami+Beach,+FL+33140'))::variant data;


-------------------------------------------------------------------
-- query the  address_variant table (or create a view on it)
-------------------------------------------------------------------
with parse_json as (select address_variant_data from address_variant)
 SELECT
    f.value:access[0].lat::float AS access_lat,
    f.value:access[0].lng::float AS access_lng,
    f.value:address.houseNumber::STRING AS address_house_number,
    f.value:address.street::STRING AS address_street,
    f.value:address.city::STRING AS address_city,
    f.value:address.state::STRING AS address_state,
    f.value:address.stateCode::STRING AS address_state_code,
    f.value:address.postalCode::STRING AS address_postalcode,
    f.value:address.county::STRING AS address_county
FROM
     parse_json, LATERAL FLATTEN(input => address_variant_data:items) f;




------------------------------------------------------------------------------
------------------------------------------------------------------------------
------------------------------------------------------------------------------
--  Write the returned data to a table and store it in individual fields
------------------------------------------------------------------------------
------------------------------------------------------------------------------
------------------------------------------------------------------------------

----------------------------------------
-- create table to store address fields
----------------------------------------
create or replace TABLE  address_fields (
    ACCESS_LAT FLOAT,
    ACCESS_LNG FLOAT,
    ADDRESS_HOUSE_NUMBER VARCHAR(16777216),
    ADDRESS_STREET VARCHAR(16777216),
    ADDRESS_CITY VARCHAR(16777216),
    ADDRESS_STATE VARCHAR(16777216),
    ADDRESS_STATE_CODE VARCHAR(16777216),
    ADDRESS_POSTALCODE VARCHAR(16777216),
    ADDRESS_COUNTY VARCHAR(16777216)
);

-----------------------------------------------------
-- call the api insert data into address_fields table
-----------------------------------------------------
insert into address_fields (access_lat, access_lng, address_house_number, address_street, address_city, address_state, address_state_code, address_postalcode, address_county)
with parse_json as (select parse_json(get_url('https://geocode.search.hereapi.com/v1/geocode?q=4601+Collins+Ave,+Miami+Beach,+FL+33140'))::variant data)
SELECT
    f.value:access[0].lat::float AS access_lat,
    f.value:access[0].lng::float AS access_lng,
    f.value:address.houseNumber::STRING AS address_house_number,
    f.value:address.street::STRING AS address_street,
    f.value:address.city::STRING AS address_city,
    f.value:address.state::STRING AS address_state,
    f.value:address.stateCode::STRING AS address_state_code,
    f.value:address.postalCode::STRING AS address_postalcode,
    f.value:address.county::STRING AS address_county
FROM
    parse_json, LATERAL FLATTEN(input => data:items) f;


-----------------------------------------------------
-- simple select
-----------------------------------------------------
select * from address_fields;
