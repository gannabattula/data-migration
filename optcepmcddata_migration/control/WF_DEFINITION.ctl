options  ( skip=1 ) 
LOAD DATA
INFILE WF_DEFINITION.data
TRUNCATE
INTO TABLE WF_DEFINITION
FIELDS TERMINATED BY "~"   optionally enclosed by "'" 
 TRAILING NULLCOLS
( WF_DEFINITION_ID CHAR(36) 
,WF_NAME CHAR(200)
,ACTIVE  "decode(:ACTIVE ,'true',1,'false',0, 0)"
,ACTIVATEDDATE TIMESTAMP "YYYY-MM-DD HH24:MI:SS.FF" 
,ACTIVIATEDBY CHAR(200)
,CURRENT_VERSION DECIMAL EXTERNAL 
)