clear all
version 13
set more off

******************************************************
**** MASTER DO FILE TO RUN ALL OTHER DOFILES *********
******************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_code "S:/Fiona/backup/Ag-EE"
global dirpath_code "S:/Louis/backup/AgEE/AgEE_code"

** additional directory paths
global dirpath_data "$dirpath/data"

*******************************************************************************

*************** BUILD ****************
*** 1: IMPORT DATA TO STATA
do "$dirpath_code/build/BUILD_import_data.do"

*** 2: CREATE LIST OF NAICS CODES
do "$dirpath_code/build/BUILD_naics_descr.do"

*** 3: CLEAN CUSTOMER DATA
do "$dirpath_code/build/BUILD_clean_customer_data.do"
	// calls auxilary GIS scripts "BUILD_gis_climate_zone_20180322.R" 
	//                        and "BUILD_gis_climate_zone_20180719.R"
	//                        and "BUILD_gis_climate_zone_20180827.R"

*** 4: CLEAN BILLING DATA
do "$dirpath_code/build/BUILD_clean_bill_data.do"

*** 5: CLEAN ENERGY EFFICIENCY DATA
do "$dirpath_code/build/BUILD_clean_energy_efficiency_data.do"

*** 6: CLEAN INTERVAL DATA
do "$dirpath_code/build/BUILD_clean_interval_data.do"

*** 7: CLEAN METER BADGE NUMBER DATA
do "$dirpath_code/build/BUILD_clean_badge_number_data.do"

*** 8: CROSS-VALIDATE BILLING VS. INTERVAL DATA
do "$dirpath_code/build/BUILD_compare_billing_interval.do" 

*** 9: CROSS-VALIDATE BILLING VS. CUSTOMER DATA
do "$dirpath_code/build/BUILD_compare_billing_customer.do"

*** 10: CROSS-VALIDATE CUSTOMER VS. BILLING/INTERVAL DATA
do "$dirpath_code/build/BUILD_compare_customer_usage.do"

*** 11: CROSS-VALIDATE CUSTOMER VS. EE DATA
do "$dirpath_code/build/BUILD_compare_customer_ee.do"

*** 12: CROSS-VALIDATE CUSTOMER VS. METER HISTORY DATA, CREATE XWALK
do "$dirpath_code/build/BUILD_compare_customer_meter.do"

*** 13: MONTHIFY BILLING DATA
do "$dirpath_code/build/BUILD_monthify_billing_data.do"

*** 14: CLEAN PUMP TEST DATA
do "$dirpath_code/build/BUILD_clean_pump_test_data.do"

*** 15: CLEAN PUMP TEST PROJECT DATA
do "$dirpath_code/build/BUILD_clean_pump_test_project_data.do"

*** 16: CLEAN PGE RATE DATA
do "$dirpath_code/build/BUILD_clean_rate_data.do"

*** 17: BUILD EVENT DAY DATASET
do "$dirpath_code/build/BUILD_event_days.do"

*** 18: CLEAN CA DWR GROUNDWATER DATA
do "$dirpath_code/build/BUILD_clean_cadwr_groundwater.do"



*************** MERGE ****************

*** 1: MERGE BILLING/INTERVAL DATA WITH RATE DATA
do "$dirpath_code/merge/MERGE_bills_interval_rates.do"

*** 2: ASSIGN AVERAGE PRICES TO BILLS WITHOUT AMI DATA (EXTRAPOLATION)
do "$dirpath_code/merge/MERGE_bills_noninterval_rates.do"

*** 3: ASSIGN AVERAGE PRICES TO ALL (MONTHIFIED) BILLS (INTERNALLY CONSISTENT)
do "$dirpath_code/merge/MERGE_bills_rate_nomerge.do"

*** 4: MERGE CUSTOMER DETAILS & APEP DATASETS TO CONSTRUCT MASTER XSECTION(S) FOR EVENTUAL PANEL(S)
do "$dirpath_code/merge/MERGE_customer_apep_units.do"









*************** ANALYZE ****************
