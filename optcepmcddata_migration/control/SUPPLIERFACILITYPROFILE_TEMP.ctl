options  ( skip=1 ) 
LOAD DATA
INFILE SUPPLIERFACILITYPROFILE_TEMP.data
TRUNCATE
INTO TABLE SUPPLIERFACILITYPROFILE_TEMP
FIELDS TERMINATED BY "~"   optionally enclosed by "'" 
 TRAILING NULLCOLS
( SUPPLIERFACILITYPROFILEID DECIMAL EXTERNAL 
,SUPPLIERCOMPANYPROFILEID DECIMAL EXTERNAL 
,FACILITYMDMID DECIMAL EXTERNAL 
,FACILITYNAME CHAR(256)
,FACILITYNAMEALIAS CHAR(256)
,FACILITYGLN CHAR(13)
,FACILITYWSINUMBER CHAR(5)
,SWASTATUS DECIMAL EXTERNAL 
,SWADATE DATE "YYYY-MM-DD HH24:MI:SS" 
,FACILITYWSISTATUS DECIMAL EXTERNAL 
,FACILITYWSINOTES CHAR(4000)
,LEGACYREFERENCECODE CHAR(256)
,LEGACYREFERENCESYSTEMNAME CHAR(256)
,PARTYRESPONSIBLEFORRELATIONSHI DECIMAL EXTERNAL 
,FACILITYTYPE DECIMAL EXTERNAL 
,SUPPLIERCATEGORY DECIMAL EXTERNAL 
,FACILITYSTARTDATE DATE "YYYY-MM-DD HH24:MI:SS" 
,FACILITYCLOSEDATE DATE "YYYY-MM-DD HH24:MI:SS" 
,FACILITYINPUTDATASTATUS DECIMAL EXTERNAL 
,WEBSITE CHAR(256)
,PHONE CHAR(25)
,FAX CHAR(25)
,PHYSICALADDRESSID DECIMAL EXTERNAL 
,POSTALADDRESSID DECIMAL EXTERNAL 
,BILLINGADDRESSID DECIMAL EXTERNAL 
,APPROVALCOMMENTS CHAR(4000)
,HASOWNGLN DECIMAL EXTERNAL "decode(:HASOWNGLN ,'true',1,'false',0, 0)"
,DOCUMENTKEY CHAR(36)
,LASTUPDATEDBY CHAR(36)
,REMOVALREASONID DECIMAL EXTERNAL 
,REMOVALREASONCOMMENTS CHAR(4000)
,RESPONSIBLEMANAGES DECIMAL EXTERNAL "decode(:RESPONSIBLEMANAGES,'true',1,'false',0, 0)"
,STETONFACILITYID CHAR(255)
,IENABLEFACILITYID CHAR(255)
,LOCALSUPPLIERID CHAR(20)
,FACILITYLEADTYPEID DECIMAL EXTERNAL 
,SUPPLIERCONTACTEMAIL CHAR(256)
,SUPPLIERCONTACTFIRSTNAME CHAR(100)
,SUPPLIERCONTACTLASTNAME CHAR(100)
,SUPPLIERCONTACTTITLE CHAR(100)
,SUPPLIERCONTACTID CHAR(36)
)