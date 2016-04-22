options  ( skip=1 ) 
LOAD DATA
INFILE EMAILS.data
TRUNCATE
INTO TABLE EMAILS
FIELDS TERMINATED BY "~"   optionally enclosed by "'" 
 TRAILING NULLCOLS
( ID DECIMAL EXTERNAL 
,EMAILADDRESS CHAR(100)
,TYPE CHAR(50)
,LANGUAGE CHAR(5)
,SENT DECIMAL EXTERNAL "decode(:SENT ,'true',1,'false',0, 0)"
,SENTUTC DATE "YYYY-MM-DD HH24:MI:SS" 
,MAXATTEMPTSREACHED DECIMAL EXTERNAL 
,MAXATTEMPTSREACHEDUTC DATE "YYYY-MM-DD HH24:MI:SS" 
,GENERATEDBY CHAR(20)
,CREATEDUTC DATE "YYYY-MM-DD HH24:MI:SS" 
,ENTITYID DECIMAL EXTERNAL 
,PROCSTEPTYPEID DECIMAL EXTERNAL 
,STATUSID DECIMAL EXTERNAL 
,ENTITYTYPEID DECIMAL EXTERNAL 
,CHILDENTITYIDS CHAR(100000)
)