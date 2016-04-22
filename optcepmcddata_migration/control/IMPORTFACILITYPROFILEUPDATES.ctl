options  ( skip=1 ) 
LOAD DATA
INFILE IMPORTFACILITYPROFILEUPDATES.data
TRUNCATE
INTO TABLE IMPORTFACILITYPROFILEUPDATES
FIELDS TERMINATED BY "~"   optionally enclosed by "'" 
 TRAILING NULLCOLS
( FACILITYNAME CHAR(255)
,MERLINFACILITYID CHAR(255)
,STETONFACILITYID CHAR(255)
,IENABLEFACILITYID CHAR(255)
,OWS_ID CHAR(255)
,MERLINPARENTID CHAR(255)
,STETONPARENTID CHAR(255)
,IENABLEPARENTID CHAR(255)
,OWS_PARENTID CHAR(255)
,PARTYRESPONSIBLEFORRELATIONSHI CHAR(255)
,FACILITYGLN CHAR(255)
,FACILITYWSINUMBER CHAR(255)
,FACILITYWSISTATUS CHAR(255)
,FACILITYWSINOTES CHAR(255)
,SWASTATUS CHAR(255)
,SWADATE CHAR(255)
,FACILITYTYPE CHAR(255)
,FACILITYSTARTDATE CHAR(255)
,FACILITYCLOSEDATE CHAR(255)
,FACILITYCATEGORY CHAR(255)
,WEBSITE CHAR(255)
,PHONE CHAR(255)
,FAX CHAR(255)
,PHYSADDRESSLINE1 CHAR(255)
,PHYSADDRESSLINE2 CHAR(255)
,PHYSCITY CHAR(255)
,PHYISOSUBDIVISION CHAR(255)
,PHYSPOSTALCODE CHAR(255)
,PHYSCOUNTRY CHAR(255)
,POSTALADDRESSLINE1 CHAR(255)
,POSTALADDRESSLINE2 CHAR(255)
,POSTALCITY CHAR(255)
,POSTALISOSUBDIVISION CHAR(255)
,POSTALPOSTALCODE CHAR(255)
,POSTALCOUNTRY CHAR(255)
,BILLINGADDRESSLINE1 CHAR(255)
,BILLINGADDRESSLINE2 CHAR(255)
,BILLINGCITY CHAR(255)
,BILLINGISOSUBDIVISION CHAR(255)
,BILLINGPOSTALCODE CHAR(255)
,BILLINGCOUNTRY CHAR(255)
,OWS_WORKFLOW_STATUS CHAR(255)
,GENERATEGLN CHAR(255)
)