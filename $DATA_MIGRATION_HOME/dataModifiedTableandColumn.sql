update doc_Document t1 set t1.documentException =  (select REGEXP_REPLACE(t2.documentException, '@&@','~') value from doc_Document t2  WHERE REGEXP_LIKE(t2.documentException, '@&@') and  t2.doc_DocumentID =  t1.doc_DocumentID);
update ProposedItem t1 set t1.COMMENT_ =  (select REGEXP_REPLACE(t2.COMMENT_, '@&@','~') value from ProposedItem t2  WHERE REGEXP_LIKE(t2.COMMENT_, '@&@') and  t2.ID =  t1.ID);
update ProposedItem t1 set t1.QualityContactEmail =  (select REGEXP_REPLACE(t2.QualityContactEmail, '@&@','~') value from ProposedItem t2  WHERE REGEXP_LIKE(t2.QualityContactEmail, '@&@') and  t2.ID =  t1.ID);
update ProposedItem t1 set t1.PurchasingContactName =  (select REGEXP_REPLACE(t2.PurchasingContactName, '@&@','~') value from ProposedItem t2  WHERE REGEXP_LIKE(t2.PurchasingContactName, '@&@') and  t2.ID =  t1.ID);
update ProposedItem t1 set t1.SupplierContactName =  (select REGEXP_REPLACE(t2.SupplierContactName, '@&@','~') value from ProposedItem t2  WHERE REGEXP_LIKE(t2.SupplierContactName, '@&@') and  t2.ID =  t1.ID);
update ProposedItem t1 set t1.QualityContactName =  (select REGEXP_REPLACE(t2.QualityContactName, '@&@','~') value from ProposedItem t2  WHERE REGEXP_LIKE(t2.QualityContactName, '@&@') and  t2.ID =  t1.ID);
update ProposedItem t1 set t1.SupplierContactEmail =  (select REGEXP_REPLACE(t2.SupplierContactEmail, '@&@','~') value from ProposedItem t2  WHERE REGEXP_LIKE(t2.SupplierContactEmail, '@&@') and  t2.ID =  t1.ID);
update ProposedItem t1 set t1.PurchasingContactEmail =  (select REGEXP_REPLACE(t2.PurchasingContactEmail, '@&@','~') value from ProposedItem t2  WHERE REGEXP_LIKE(t2.PurchasingContactEmail, '@&@') and  t2.ID =  t1.ID);
update SupplierCompanyProfile_History t1 set t1.companyWSINumber =  (select REGEXP_REPLACE(t2.companyWSINumber, '@&@','~') value from SupplierCompanyProfile_History t2  WHERE REGEXP_LIKE(t2.companyWSINumber, '@&@') and  t2.ID =  t1.ID);
update WorkFlowNotification t1 set t1.FirstName =  (select REGEXP_REPLACE(t2.FirstName, '@&@','~') value from WorkFlowNotification t2  WHERE REGEXP_LIKE(t2.FirstName, '@&@') and  t2.WorkFlowNotificationID =  t1.WorkFlowNotificationID);
update WorkFlowNotification t1 set t1.EmailAddress =  (select REGEXP_REPLACE(t2.EmailAddress, '@&@','~') value from WorkFlowNotification t2  WHERE REGEXP_LIKE(t2.EmailAddress, '@&@') and  t2.WorkFlowNotificationID =  t1.WorkFlowNotificationID);
update WorkFlowNotificationDefinition t1 set t1.Email =  (select REGEXP_REPLACE(t2.Email, '@&@','~') value from WorkFlowNotificationDefinition t2  WHERE REGEXP_LIKE(t2.Email, '@&@') and  t2.WORKFLOWNOTIFICATIONDEFINITION =  t1.WORKFLOWNOTIFICATIONDEFINITION);
update WorkFlowNotificationDefinition t1 set t1.FirstName =  (select REGEXP_REPLACE(t2.FirstName, '@&@','~') value from WorkFlowNotificationDefinition t2  WHERE REGEXP_LIKE(t2.FirstName, '@&@') and  t2.WORKFLOWNOTIFICATIONDEFINITION =  t1.WORKFLOWNOTIFICATIONDEFINITION);
update WORKFLOWNOTIFICATIONDEFINITI_1 t1 set t1.Email =  (select REGEXP_REPLACE(t2.Email, '@&@','~') value from WORKFLOWNOTIFICATIONDEFINITI_1 t2  WHERE REGEXP_LIKE(t2.Email, '@&@') and  t2.ID =  t1.ID);
update WORKFLOWNOTIFICATIONDEFINITI_1 t1 set t1.FirstName =  (select REGEXP_REPLACE(t2.FirstName, '@&@','~') value from WORKFLOWNOTIFICATIONDEFINITI_1 t2  WHERE REGEXP_LIKE(t2.FirstName, '@&@') and  t2.ID =  t1.ID);
commit;
exit;