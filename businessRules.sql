insert into `google.com:ml-baguette-demos.bq_results_kyc_kb_geo_demo.business_rules` (name, query, trigger ) VALUES
( "test" ,      "SELECT input_file_name, 'address_is_null' as name  FROM `bq_results_kyc_kb_geo_demo.doc_ai_extracted_entities` where address is null", "every_insert" ),
( "CI_expired" ,"SELECT input_file_name, 'ID_expired' as name       FROM `bq_results_kyc_kb_geo_demo.doc_ai_extracted_entities` where expiration_date >  DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR)" , "every_insert" )
