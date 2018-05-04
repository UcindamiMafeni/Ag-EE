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

*** 4: CLEAN BILLING DATA
do "$dirpath_code/build/BUILD_clean_bill_data.do"

*** 5: CLEAN PUMP TEST DATA
do "$dirpath_code/build/BUILD_clean_pump_test_data.do"









*************** MERGE ****************











*************** ANALYZE ****************
