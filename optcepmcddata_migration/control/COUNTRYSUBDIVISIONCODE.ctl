options  ( skip=1 ) 
LOAD DATA
INFILE COUNTRYSUBDIVISIONCODE.data
TRUNCATE
INTO TABLE COUNTRYSUBDIVISIONCODE
FIELDS TERMINATED BY "~"   optionally enclosed by "'" 
 TRAILING NULLCOLS
( COUNTRYSUBDIVISIONCODEID DECIMAL EXTERNAL 
,COUNTRYSUBDIVISIONCODETEXTID CHAR(36)
,COUNTRYSUBDIVISIONNAMETEXTID CHAR(36)
,TRADINGAREAID DECIMAL EXTERNAL 
,Active FILLER
)