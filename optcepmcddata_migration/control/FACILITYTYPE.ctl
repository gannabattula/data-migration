options  ( skip=1 ) 
LOAD DATA
INFILE FACILITYTYPE.data
TRUNCATE
INTO TABLE FACILITYTYPE
FIELDS TERMINATED BY "~"   optionally enclosed by "'" 
 TRAILING NULLCOLS
( FACILITYTYPEID DECIMAL EXTERNAL 
,FACILITYTYPETEXTID CHAR(36)
,FACILITYTYPEDESCRIPTIONTEXTID CHAR(36)
,ACTIVE "decode(:ACTIVE,'true','T','false','F','F')"
)