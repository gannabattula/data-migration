options  ( skip=1 ) 
LOAD DATA
INFILE IMPORTSUPPLIERPROFILEADDS.data
TRUNCATE
INTO TABLE IMPORTSUPPLIERPROFILEADDS
FIELDS TERMINATED BY "~"   optionally enclosed by "'" 
 TRAILING NULLCOLS
( COMPANYNAME CHAR(255)
,MERLINCOMPANYID CHAR(255)
,STETONCOMPANYID CHAR(255)
,IENABLECOMPANYID CHAR(255)
,OWS_ID CHAR(255)
,COMPANYGLN CHAR(255)
,COMPANYWSINUMBER CHAR(255)
,COMPANYWSISTATUS CHAR(255)
,COMPANYWSINOTES CHAR(255)
,SWASTATUS CHAR(255)
,SWADATE CHAR(255)
,SUPPLIERSTARTDATE CHAR(255)
,NOTIFICATIONCONTACTCATEGORY CHAR(255)
,WEBSITE CHAR(255)
,PHONE CHAR(255)
,FAX CHAR(255)
,PHYSADDRESSLINE1 CHAR(255)
,PHYSADDRESSLINE2 CHAR(255)
,PHYSCITY CHAR(255)
,PHYSISOSUBDIVISION CHAR(255)
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