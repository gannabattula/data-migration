options  ( skip=1 ) 
LOAD DATA
INFILE INPUTSTATUS.data
TRUNCATE
INTO TABLE INPUTSTATUS
FIELDS TERMINATED BY "~"   optionally enclosed by "'" 
 TRAILING NULLCOLS
( INPUTDATASTATUSID DECIMAL EXTERNAL 
,STATUSTEXTID CHAR(36)
,STATUSDEFINITIONTEXTID CHAR(36)
,STATUSORDER DECIMAL EXTERNAL 
)