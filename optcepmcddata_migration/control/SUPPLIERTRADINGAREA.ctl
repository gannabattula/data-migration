options  ( skip=1 ) 
LOAD DATA
INFILE SUPPLIERTRADINGAREA.data
TRUNCATE
INTO TABLE SUPPLIERTRADINGAREA
FIELDS TERMINATED BY "~"   optionally enclosed by "'" 
 TRAILING NULLCOLS
( SUPPLIERTRADINGAREAID DECIMAL EXTERNAL 
,TRADINGAREA DECIMAL EXTERNAL 
,TRADINGAREASTATUS DECIMAL EXTERNAL 
,SUPPLIERFACILITYPROFILEID DECIMAL EXTERNAL 
,SUPPLIERCOMPANYPROFILEID DECIMAL EXTERNAL 
,DOCUMENTKEY CHAR(36)
,LASTUPDATEDBY CHAR(36)
,AREALEVEL DECIMAL EXTERNAL 
)