clear all
version 13
set more off

*******************************************************************************
**** MASTER DO FILE TO RUN ALL OTHER DOFILES                          *********
*******************************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_code "S:/Fiona/backup/Ag-EE"

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
	// calls auxilary GIS script "BUILD_gis_climate_zone.R"

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

*** 11: MONTHIFY BILLING DATA
do "$dirpath_code/buld/BUILD_monthify_billing_data.do"

*** 12: CLEAN PUMP TEST DATA
do "$dirpath_code/build/BUILD_clean_pump_test_data.do"

*** 13: CLEAN PGE Rate DATA
do "$dirpath_code/build/BUILD_clean_rate_data.do"








*************** MERGE ****************











*************** ANALYZE ****************
