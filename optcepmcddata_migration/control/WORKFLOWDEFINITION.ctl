options  ( skip=1 ) 
LOAD DATA
INFILE WORKFLOWDEFINITION.data
TRUNCATE
INTO TABLE WORKFLOWDEFINITION
FIELDS TERMINATED BY "~"   optionally enclosed by "'" 
 TRAILING NULLCOLS
( WORKFLOWDEFINITIONID DECIMAL EXTERNAL 
,ENTITYTYPEID DECIMAL EXTERNAL 
,PROCESSSTEPTYPEID DECIMAL EXTERNAL 
,ROLE CHAR(36)
,SEQUENCE DECIMAL EXTERNAL 
,CREATEDATE CHAR(200)
,ENABLED DECIMAL EXTERNAL "decode(:ENABLED ,'true',1,'false',0, 0)"
,DOCUMENTKEY CHAR(36)
,LASTUPDATEDBY CHAR(36)
)