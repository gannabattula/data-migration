options  ( skip=1 ) 
LOAD DATA
INFILE GTINGENERATORSEEDS.data
TRUNCATE
INTO TABLE GTINGENERATORSEEDS
FIELDS TERMINATED BY "~"   optionally enclosed by "'" 
 TRAILING NULLCOLS
( GTINPREFIX CHAR(13)
,COMMENT_ CHAR(100)
,LASTASSIGNEDSEED CHAR(13)
,INCREMENT_ DECIMAL EXTERNAL 
,LASTASSIGNEDDATE CHAR(200)
,FLOOR CHAR(13)
,CEILING CHAR(13)
)