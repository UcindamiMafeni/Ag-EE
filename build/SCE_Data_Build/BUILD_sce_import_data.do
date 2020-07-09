************************************************
* Import SCE data from excel / csv.
************************************************
**** SETUP:
clear all
memory clear
set more off, perm
set excelxlsxlargefile on
version 12

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

************************************************
************************************************

** SEPTEMBER 16 2019 DATA
global dirpath_raw "T:/Raw Data/PumpData/Data09132019"

*** load customer data and save as dta file
insheet using "$dirpath_raw/ADHQ1376_EDRP/adhq1376_1.csv", comma clear
tostring *, replace
save "$dirpath_data/sce_raw/customer_data_20190916.dta", replace

*** load monthly billing data and save as dta file
insheet using "$dirpath_raw/ADHQ1376_EDRP/adhq1376_4_mnthly_bill.csv", comma clear
tostring *, replace
save "$dirpath_data/sce_raw/bill_data_20190916.dta", replace

*** load energy efficiency data and save as dta file
insheet using "$dirpath_raw/ADHQ1376_EDRP/adhq1376_5_ee_2014_18.csv", comma clear
tostring *, replace
save "$dirpath_data/sce_raw/energy_efficiency_data_20190916.dta", replace

*** load demand response data and save as dta file
insheet using "$dirpath_raw/ADHQ1376_EDRP/adhq1376_6_dr.csv", comma clear
tostring *, replace
save "$dirpath_data/sce_raw/demand_resposne_data_20190916.dta", replace

*** load pump test project data and save as dta file
import excel "$dirpath_raw/Pump Overhauls 2011 to 2018_Rebate.xlsx", firstrow allstring clear
save "$dirpath_data/sce_raw/pump_test_project_data_20190916.dta", replace

*** load pump test data and save as dta file
import excel using "$dirpath_raw/Pump Test Data Extract.xlsx", firstrow allstring clear
tostring *, replace
save "$dirpath_data/sce_raw/pump_test_data_20190916.dta", replace


*** load interval data and save as dta file
foreach year in "2016" "2017" "2018" "2019" {
  foreach quarter in "1" "2" "3" "4" {
    foreach week in "1" "2" "3" "4" {
     foreach month in "1" "2" "3" "4" "5" "6" "7" "8" "9" "10" "11" "12" {
	   clear
	   cap confirm file "$dirpath_raw/ADHQ1376_INTRVL_`year'Q`quarter'/ADHQ1376_intrvl_`year'0`month'wk`week'.csv"
	   if _rc == 0 {
	    insheet using "$dirpath_raw/ADHQ1376_INTRVL_`year'Q`quarter'/ADHQ1376_intrvl_`year'0`month'wk`week'.csv", comma clear
       save "$dirpath_data/sce_raw/interval_data_`year'_Q`quarter'_m`month'_w`week'_20190916.dta", replace
	   }
	   else {
	     di "The file does not exist"
	   }
     }
	}
  }
}

************************************************
************************************************

** MAY 4 2020 DATA
global dirpath_raw "T:/Raw Data/PumpData/SCE05042020"

** Extract raw xwalk
import excel "$dirpath_raw/SA to CA Table SCE29202610522.xlsx", clear sheet("ADHQ1376_1_updated") firstrow
dropmiss, force
compress
save "$dirpath_data/sce_raw/customer_id_xwalk_20200504.dta", replace

************************************************
************************************************
