options  ( skip=1 ) 
LOAD DATA
INFILE WFFACILITYREADYSTEP.data
TRUNCATE
INTO TABLE WFFACILITYREADYSTEP
FIELDS TERMINATED BY "~"   optionally enclosed by "'" 
 TRAILING NULLCOLS
( ROLE CHAR(36)
,STATUS DECIMAL EXTERNAL 
)