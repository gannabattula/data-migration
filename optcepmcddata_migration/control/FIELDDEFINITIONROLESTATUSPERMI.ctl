options  ( skip=1 ) 
LOAD DATA
INFILE FIELDDEFINITIONROLESTATUSPERMI.data
TRUNCATE
INTO TABLE FIELDDEFINITIONROLESTATUSPERMI
FIELDS TERMINATED BY "~"   optionally enclosed by "'" 
 TRAILING NULLCOLS
( ID DECIMAL EXTERNAL 
,FIELDDEFINITIONID DECIMAL EXTERNAL 
,ROLE CHAR(36)
,WORKFLOWSTATUS DECIMAL EXTERNAL 
,READONLY DECIMAL EXTERNAL "decode(:READONLY ,'true',1,'false',0, 0)"
)