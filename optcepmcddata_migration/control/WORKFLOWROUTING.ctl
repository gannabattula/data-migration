options  ( skip=1 ) 
LOAD DATA
INFILE WORKFLOWROUTING.data
TRUNCATE
INTO TABLE WORKFLOWROUTING
FIELDS TERMINATED BY "~"   optionally enclosed by "'" 
 TRAILING NULLCOLS
( WORKFLOWROUTINGID DECIMAL EXTERNAL 
,PROCESSSTEPTYPEID DECIMAL EXTERNAL 
,STATE CHAR(15)
,STATUSID DECIMAL EXTERNAL 
,STATEPROCESSSTEPTYPEID DECIMAL EXTERNAL 
,ENTITYTYPEID DECIMAL EXTERNAL 
,NOTIFY DECIMAL EXTERNAL "decode(:NOTIFY,'true',1,'false',0, 0)"
,DOCUMENTKEY CHAR(36)
,LASTUPDATEDBY CHAR(36)
)