options  ( skip=1 ) 
LOAD DATA
INFILE MINORITYTYPE.data
TRUNCATE
INTO TABLE MINORITYTYPE
FIELDS TERMINATED BY "~"   optionally enclosed by "'" 
 TRAILING NULLCOLS
( MINORITYTYPEID DECIMAL EXTERNAL 
,MINORITYTYPETEXTID CHAR(36)
,DESCRIPTIONTEXTID CHAR(36)
)