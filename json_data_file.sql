create or replace database my_db;

CREATE OR REPLACE SCHEMA IOT_SCHEMA;

create or replace file format iot_csv
type='csv'
compression='none'
field_delimiter=','
field_optionally_enclosed_by='\042' -- double quotes ASCII value
skip_header=1;


create or replace TABLE my_db.IOT_SCHEMA.LOAD_IOTV2_EUEXPERIENCE00320055 
(
	ATOMICCONSENTS VARCHAR(16777216),
	DATA VARIANT,
	ORIGINREGION VARCHAR(16777216),
	REQUESTID VARCHAR(16777216),
	SERIALNUMBER VARCHAR(16777216),
	SOURCE_FILE_NAME VARCHAR(16777216),
	EVENT_LOCAL_TIMESTAMP VARCHAR(16777216)
);

create or replace TABLE LOAD_IOTV2_EUEXPERIENCE00320055_COPY as select * from LOAD_IOTV2_EUEXPERIENCE00320055;

create or replace TABLE my_db.IOT_SCHEMA.LOAD_IOTV2_JPEXPERIENCE00320055 (
	ATOMICCONSENTS VARCHAR(16777216),
	DATA VARIANT,
	ORIGINREGION VARCHAR(16777216),
	REQUESTID VARCHAR(16777216),
	SERIALNUMBER VARCHAR(16777216),
	SOURCE_FILE_NAME VARCHAR(16777216),
	EVENT_LOCAL_TIMESTAMP VARCHAR(16777216)
);

create or replace TABLE LOAD_IOTV2_JPEXPERIENCE00320055_COPY as select * from LOAD_IOTV2_JPEXPERIENCE00320055;
select * from IOT_DB.IOT_SCHEMA.LOAD_IOTV2_EUEXPERIENCE00320055_COPY;

--endMCUtemperature
--startMCUtemperature

create or replace TABLE my_db.IOT_SCHEMA.LOAD_IOTV2_NZEXPERIENCE002F0052 (
	ATOMICCONSENTS VARCHAR(16777216),
	DATA VARIANT,
	ORIGINREGION VARCHAR(16777216),
	REQUESTID VARCHAR(16777216),
	SERIALNUMBER VARCHAR(16777216),
	SOURCE_FILE_NAME VARCHAR(16777216),
	EVENT_LOCAL_TIMESTAMP VARCHAR(16777216)
);

create or replace TABLE LOAD_IOTV2_NZEXPERIENCE002F0052_COPY as select * from LOAD_IOTV2_NZEXPERIENCE002F0052;

----------------------------------------------------AWS (S3) INTEGRATION------------------------------------------------------------------------
create or replace file format iot_csv
type='csv'
compression='none'
field_delimiter=','
field_optionally_enclosed_by='\042' -- double quotes ASCII value
skip_header=1;

CREATE OR REPLACE STORAGE integration iot_si
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN ='arn:aws:iam::767179190646:role/iot_role' 
STORAGE_ALLOWED_LOCATIONS =('s3://myiotdf/');

DESC integration iot_si;

CREATE OR REPLACE STAGE iot_stage
URL ='s3://myiotdf'
file_format = iot_csv
storage_integration = iot_si;

SHOW STAGES;

LIST @iot_stage;

-------------------------------------------------iotv2_snowpipe--------------------------------------------------------------------

CREATE OR REPLACE PIPE iotv2_snowpipe_EUEXPERIENCE00320055 AUTO_INGEST = TRUE AS
COPY INTO my_db.IOT_SCHEMA.LOAD_IOTV2_EUEXPERIENCE00320055 --yourdatabase -- your schema ---your table
FROM '@iot_stage/iot3/' --s3 bucket subfolde4r name
FILE_FORMAT = iot_csv; --YOUR CSV FILE FORMAT NAME

CREATE OR REPLACE PIPE iotv2_snowpipe_JPEXPERIENCE00320055 AUTO_INGEST = TRUE AS
COPY INTO my_db.IOT_SCHEMA.LOAD_IOTV2_JPEXPERIENCE00320055
FROM '@iot_stage/iot1/' 
FILE_FORMAT = iot_csv;

CREATE OR REPLACE PIPE iotv2_snowpipe_NZEXPERIENCE002F0052 AUTO_INGEST = TRUE AS
COPY INTO my_db.IOT_SCHEMA.LOAD_IOTV2_NZEXPERIENCE002F0052
FROM '@iot_stage/iot2/' 
FILE_FORMAT = iot_csv;


----------------------------------------------------------PIPEREFRESH-----------------------------------------------------------------

ALTER PIPE iotv2_snowpipe_EUEXPERIENCE00320055 refresh;
ALTER PIPE  iotv2_snowpipe_JPEXPERIENCE00320055 refresh;
ALTER PIPE  iotv2_snowpipe_NZEXPERIENCE002F0052 refresh;

SELECT COUNT(*) FROM LOAD_IOTV2_EUEXPERIENCE00320055;
SELECT COUNT(*) FROM LOAD_IOTV2_JPEXPERIENCE00320055;
SELECT COUNT(*) FROM LOAD_IOTV2_NZEXPERIENCE002F0052;

SELECT * FROM LOAD_IOTV2_EUEXPERIENCE00320055;
SELECT * FROM LOAD_IOTV2_JPEXPERIENCE00320055;
SELECT * FROM LOAD_IOTV2_NZEXPERIENCE002F0052;






CREATE OR REPLACE table parsing_json_data
( 
  src variant
)
AS
SELECT PARSE_JSON(column1) AS src
FROM VALUES
('{ 
    "date" : "2017-04-28", 
    "dealership" : "Valley View Auto Sales",
    "salesperson" : {
      "id": "55",
      "name": "Frank Beasley"
    },
    "customer" : [
      {"name": "Joyce Ridgely", "phone": "16504378889", "address": "San Francisco, CA"}
    ],
    "vehicle" : [
      {"make": "Honda", "model": "Civic", "year": "2017", "price": "20275", "extras":["ext warranty", "paint protection"]}
    ]
}'),
('{ 
    "date" : "2017-04-28", 
    "dealership" : "Tindel Toyota",
    "salesperson" : {
      "id": "274",
      "name": "Greg Northrup"
    },
    "customer" : [
      {"name": "Bradley Greenbloom", "phone": "12127593751", "address": "New York, NY"}
    ],
    "vehicle" : [
      {"make": "Toyota", "model": "Camry", "year": "2017", "price": "23500", "extras":["ext warranty", "rust proofing", "fabric protection"]}  
    ]
}') v;


SELECT * FROM parsing_json_data;

---- Traversing Semi-structured Data
--- Insert a colon : between the VARIANT column name and any first-level element: <column>:<level1_element>.


/* Note
In the following examples, the query output is enclosed in double quotes because the query output is VARIANT, not VARCHAR. (The VARIANT values are not strings; the VARIANT values contain strings.) Operators : and subsequent . and [] always return VARIANT values containing strings. */



SELECT src:dealership
FROM parsing_json_data
ORDER BY 1;


There are two ways to access elements in a JSON object:
Dot Notation (in this topic).
Bracket Notation (in this topic).

Important

Regardless of which notation you use, the column name is case-insensitive but element names are case-sensitive. 
For example, in the following list, the first two paths are equivalent, but the third is not:

src:salesperson.name
SRC:salesperson.name
SRC:Salesperson.Name


Dot Notation
Use dot notation to traverse a path in a JSON object: <column>:<level1_element>.<level2_element>.<level3_element>. 
Optionally enclose element names in double quotes: <column>:"<level1_element>"."<level2_element>"."<level3_element>".;

--Get the names of all salespeople who sold cars:;

SELECT src:salesperson.name
FROM parsing_json_data
ORDER BY 1;

Bracket Notation
Alternatively, use bracket notation to traverse the path in an object: <column>['<level1_element>']['<level2_element>']. Enclose element names in single quotes. Values are retrieved as strings.

Get the names of all salespeople who sold cars:;

SELECT src['salesperson']['name']
FROM parsing_json_data
ORDER BY 1;



Retrieving a Single Instance of a Repeating Element
Retrieve a specific numbered instance of a child element in a repeating array by adding a numbered predicate (starting from 0) to the array reference.

Note that to retrieve all instances of a child element in a repeating array, it is necessary to flatten the array. 


Get the vehicle details for each sale:;

SELECT src:customer[0].name, src:vehicle[0]
 FROM parsing_json_data
ORDER BY 1;

Get the price of each car sold:;
SELECT src:customer[0].name, src:vehicle[0].price
FROM parsing_json_data
ORDER BY 1;


Explicitly Casting Values
When you extract values from a VARIANT, you can explicitly cast the values to the desired data type. 
For example, you can extract the prices as numeric values and perform calculations on them:;

SELECT src:vehicle[0].price::NUMBER * 0.10 AS tax
FROM parsing_json_data
ORDER BY tax;

By default, when VARCHARs, DATEs, TIMEs, and TIMESTAMPs are retrieved from a VARIANT column, the values are surrounded by double quotes. 
You can eliminate the double quotes by explicitly casting the values. For example:;

SELECT src:dealership, src:dealership::VARCHAR
FROM parsing_json_data
ORDER BY 2;


Using the FLATTEN Function to Parse Arrays
Parse an array using the FLATTEN function. FLATTEN is a table function that produces a lateral view of a VARIANT, OBJECT, or ARRAY column. The function returns a row for each object, and the LATERAL modifier joins the data with any information outside of the object.

Get the names and addresses of all customers. Cast the VARIANT output to string values:;

SELECT
  value:name::string as "Customer Name",
  value:address::string as "Address"
  FROM parsing_json_data, LATERAL FLATTEN(INPUT => SRC:customer);

Using the FLATTEN Function to Parse Nested Arrays¶
The extras array is nested within the vehicle array in the sample data:

"vehicle" : [
     {"make": "Honda", "model": "Civic", "year": "2017", "price": "20275", "extras":["ext warranty", "paint protection"]}
   ]
Add a second FLATTEN clause to flatten the extras array within the flattened vehicle array and retrieve the “extras” purchased for each car sold:;

SELECT
  vm.value:make::string as make,
  vm.value:model::string as model,
  ve.value::string as "Extras Purchased"
  FROM
    parsing_json_data
    , LATERAL FLATTEN(INPUT => SRC:vehicle) vm
    , LATERAL FLATTEN(INPUT => vm.value:extras) ve
  ORDER BY make, model, "Extras Purchased";








create or replace view my_db.IOT_SCHEMA.V_LOAD_IOTV2_JSON_IQOS4_HOLDER_EXP
(
	ATOMICCONSENTS,
	RECORDINDEX,
	RECORDFORMATVERSION,
	RECORDSIZE,
	STARTTIME,
	EXPCREDIT,
	STARTBATTERYGAUGELEVEL,
	ENDBATTERYGAUGELEVEL,
	CONTROLSTARTBATTERYVOLTAGE,
	CONTROLSTARTTEMPERATURE2,
	CONTROLINTERNALRESISTORINDICATOR,
	CONTROLENDBATTERYVOLTAGE,
	CONTROLENDTEMPERATURE2,
	CONTROLSTOPREASON,
	STARTREASON,
	SKU,
	STARTTEMP1,
	STARTTEMP2,
    START_MCU_TEMPERATURE,
    END_MCU_TEMPERATURE,
	STARTDCDCVOLTAGE,
	ENDTEMP1,
	ENDTEMP2,
	ENDDCDCVOLTAGE,
	DCDCVOLTAGEVARIATION,
	INTERNALRESISTORINDICATOR,
	STARTCONDUCTANCE,
	FIRSTVALLEYCONDUCTANCE,
	FIRSTDELTASCURVECONDUCTANCE,
	LASTDELTASCURVECONDUCTANCE,
	PREHEATSLOPETIME,
	OUTOFRANGEREGULATION,
	DRIFTCOMPENSATIONERROR,
	HEATINGDURATION,
	PAUSEDURATION,
	PAUSETIMESTAMP,
	PAUSEENERGY,
	HEATINGENERGY,
	ENGINESTOPREASON,
	HEATINGPROFILE,
	STARTBATTERYVOLTAGE,
	ENDBATTERYVOLTAGE,
	BATTERYVOLTAGEVARIATION,
	LASTVALLEYCONDUCTANCE,
	FIRSTHILLCONDUCTANCE,
	CALIBRATIONDURATION,
	HOTALARMTREATED,
	MAXIMUMDELTASVARIATION,
	VALIDSCURVEDETECTED,
	COOLINGSEQUENCEFAILURE,
	CALIBRATIONPULSEFAILURE,
	APPLICATIONVERSION,
	PUFFCOUNT,
	PUFFS,
	INDEX,
	PUFFCOUNTBEFOREPAUSE,
	PUFFCOUNTAFTERPAUSE,
	PUFFVOLUMEAFTER14PUFFS,
	TOTALPUFFVOLUME,
    PUFFDURATION,
	PAUSEPROFILE,
	ENERGYTOFIRSTVALLEY,
	STICKEXTRACTIONDURATION,
	ENGINEFLAGS,
	CONTROLFLAGS,
	ORIGINREGION,
	REQUESTID,
	SERIALNUMBER,
	SOURCE_FILE_NAME,
	EVENT_LOCAL_TIMESTAMP
) as
SELECT 
EXP3.ATOMICCONSENTS,
parse_json(DATA):recordIndex,
parse_json(DATA):recordFormatVersion,
parse_json(DATA):recordSize,
parse_json(DATA):startTime,
parse_json(DATA):expCredit,
parse_json(DATA):startBatteryGaugeLevel,
parse_json(DATA):endBatteryGaugeLevel,
parse_json(DATA):controlStartBatteryVoltage,
parse_json(DATA):controlStartTemperature2,
parse_json(DATA):controlInternalResistorIndicator,
parse_json(DATA):controlEndBatteryVoltage,
parse_json(DATA):controlEndTemperature2,
parse_json(DATA):controlStopReason,
parse_json(DATA):startReason,
parse_json(DATA):SKU,
parse_json(DATA):startTemp1,
parse_json(DATA):startTemp2,
parse_json(DATA):startMCUtemperature,
parse_json(DATA):endMCUtemperature,
parse_json(DATA):startDCDCVoltage,
parse_json(DATA):endTemp1,
parse_json(DATA):endTemp2,
parse_json(DATA):endDCDCVoltage,
parse_json(DATA):DCDCVoltageVariation,
parse_json(DATA):internalResistorIndicator,
parse_json(DATA):startConductance,
parse_json(DATA):firstValleyConductance,
parse_json(DATA):firstDeltaScurveConductance,
parse_json(DATA):lastDeltaScurveConductance,
parse_json(DATA):preheatSlopeTime,
parse_json(DATA):outOfRangeRegulation,
parse_json(DATA):driftCompensationError,
parse_json(DATA):heatingDuration,
parse_json(DATA):pauseDuration,
parse_json(DATA):pauseTimeStamp,
parse_json(DATA):pauseEnergy,
parse_json(DATA):heatingEnergy,
parse_json(DATA):engineStopReason,
parse_json(DATA):heatingProfile,
parse_json(DATA):startBatteryVoltage,
parse_json(DATA):endBatteryVoltage,
parse_json(DATA):batteryVoltageVariation,
parse_json(DATA):lastValleyConductance,
parse_json(DATA):firstHillConductance,
parse_json(DATA):calibrationDuration,
parse_json(DATA):hotAlarmTreated,
parse_json(DATA):maximumDeltaSvariation,
parse_json(DATA):validScurveDetected,
parse_json(DATA):coolingSequenceFailure,
parse_json(DATA):calibrationPulseFailure,
parse_json(DATA):application version,
parse_json(DATA):puffCount,
parse_json(DATA):puffs,
parse_json(DATA):index,
NULL,
NULL,
NULL,
NULL,
parse_json(DATA):puffDuration,
NULL,
NULL,
NULL,
NULL,
NULL,
EXP3.ORIGINREGION,
REPLACE(EXP3.REQUESTID,'__USAGE_DATA',''),
EXP3.SERIALNUMBER,
EXP3.SOURCE_FILE_NAME,
EXP3.EVENT_LOCAL_TIMESTAMP
FROM (SELECT ATOMICCONSENTS,DATA,ORIGINREGION,REQUESTID,SERIALNUMBER,SOURCE_FILE_NAME,EVENT_LOCAL_TIMESTAMP
FROM my_db.IOT_SCHEMA.LOAD_IOTV2_EUEXPERIENCE00320055
UNION
SELECT ATOMICCONSENTS,DATA,ORIGINREGION,REQUESTID,SERIALNUMBER,SOURCE_FILE_NAME,EVENT_LOCAL_TIMESTAMP
FROM my_db.IOT_SCHEMA.LOAD_IOTV2_JPEXPERIENCE00320055
UNION
SELECT ATOMICCONSENTS,DATA,ORIGINREGION,REQUESTID,SERIALNUMBER,SOURCE_FILE_NAME,EVENT_LOCAL_TIMESTAMP
FROM my_db.IOT_SCHEMA.LOAD_IOTV2_NZEXPERIENCE002F0052
) EXP3;

SELECT * FROM my_db.IOT_SCHEMA.V_LOAD_IOTV2_JSON_IQOS4_HOLDER_EXP;

SELECT * FROM my_db.IOT_SCHEMA.V_LOAD_IOTV2_JSON_IQOS4_HOLDER_EXP
WHERE START_MCU_TEMPERATURE IS NOT NULL AND END_MCU_TEMPERATURE IS NOT NULL;

SELECT * FROM my_db.IOT_SCHEMA.V_LOAD_IOTV2_JSON_IQOS4_HOLDER_EXP
WHERE START_MCU_TEMPERATURE <> 'null'
AND END_MCU_TEMPERATURE <> 'null';
--PUFFDURATION <> 'null'

SELECT  data:startMCUtemperature::VARCHAR as "Starting MCU Temperature Value",
data:endMCUtemperature::VARCHAR as "Ending MCU Temperature Value"
FROM LOAD_IOTV2_EUEXPERIENCE00320055
WHERE data:startMCUtemperature <> 'null'
AND data:startMCUtemperature <> 'null';

SELECT 
data:puffs::VARCHAR as "Puffs"
--data:endMCUtemperature::VARCHAR as "Ending MCU Temperature Value"
FROM LOAD_IOTV2_EUEXPERIENCE00320055

NULL Values
Snowflake supports two types of NULL values in semi-structured data:

SQL NULL: SQL NULL means the same thing for semi-structured data types as it means for structured data types: the value is missing or unknown.

JSON null (sometimes called “VARIANT NULL”): 
In a VARIANT column, JSON null values are stored as a string containing the word “null” to distinguish them from SQL NULL values.

The following example contrasts SQL NULL and JSON null:;

select 
    parse_json(NULL) AS "SQL NULL", 
    parse_json('null') AS "JSON NULL", 
    parse_json('[ null ]') AS "JSON NULL",
    parse_json('{ "a": null }'):a AS "JSON NULL",
    parse_json('{ "a": null }'):b AS "ABSENT VALUE";

To convert a VARIANT "null" value to SQL NULL, cast it as a string. For example:;

select 
    parse_json('{ "a": null }'):a,
    to_char(parse_json('{ "a": null }'):a);


