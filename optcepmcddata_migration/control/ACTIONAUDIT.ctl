options  ( skip=1 ) 
LOAD DATA
INFILE ACTIONAUDIT.data
TRUNCATE
INTO TABLE ACTIONAUDIT
FIELDS TERMINATED BY "~"   optionally enclosed by "'" 
 TRAILING NULLCOLS
( ACTIONAUDITID DECIMAL EXTERNAL 
,ACTIONAUDITTYPE CHAR(50)
,ACTIONBY CHAR(36)
,ACTIONTIMEUTC DATE "YYYY-MM-DD HH24:MI:SS" 
,ENTITYID DECIMAL EXTERNAL 
,ENTITYTYPEID DECIMAL EXTERNAL 
,HISTORYTOKEN CHAR(36)
)