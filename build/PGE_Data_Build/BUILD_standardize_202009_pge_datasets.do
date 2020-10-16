************************************************
* Rename variables in new PGE data to match prior data.
************************************************
**** SETUP:
clear all
memory clear
set more off, perm
set excelxlsxlargefile on
version 12

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"
global dirpath_raw "T:/Raw Data/PumpData"

** additional directory paths to make things easier

************************************************
************************************************


*** Customer data
use "$dirpath_data/pge_raw/customer_data_202009.dta", clear

rename (msk_cust_id msk_sa_id msk_sp_id prsn_naics) ///
       (prsn_uuid sa_uuid sp_uuid prsn_naics_cd)

replace dr_program = "NOT ENROLLED" if dr_program == "N"
save "$dirpath_data/temp/customer_data_202009_rename.dta", replace

/*
NEW DATA IS MISSING: 
-- SA/SP START DATE AND STOP DATE
-- NET METER INDICATOR (NOTE THAT THIS IS ACTUALLY IN THE BILLING DATA)

NEW DATA HAS, IN ADDITION TO OLD DATA:
-- METER ID, SA NAICS CODE

*/



*** Billing data
use "$dirpath_data/pge_raw/bill_data_202009.dta", clear

rename (msk_sa_id start_dt end_dt kwh revn_amt) ///
       (sa_uuid bseg_start_dt bseg_end_dt total_electric_usage total_bill_amount)

/*
NEW DATA IS MISSING: 
-- MAX DEMAND
-- PEAK DEMAND
-- PARTIAL PEAK DEMAND

NEW DATA HAS, IN ADDITION TO OLD DATA:
-- ON-PEAK KWH
-- PARTIAL PEAK KWH
-- OFF-PEAK KWH
-- NET METER ID (HERE, RATHER THAN IN THE CUSTOMER DATA)

(OTHER THAN MAX DEMAND, MAY BE ABLE TO CONFORM THESE BY SCALING BY HOURS)

CLARIFYING QUESTION: IS "REVN_AMT" == "TOTAL_BILL_AMT"? OR IS THERE SOMETHING ELSE NEW?

*/
save "$dirpath_data/temp/bill_data_202009_rename.dta", replace
	   
	  
	  
	  
use "$dirpath_data/pge_raw/bill_data_20180827.dta", clear	  
gen bill_start_stata = date(bseg_start, "DMY")
format bill_start %td
gen count = 1
collapse(sum) count, by(bill_start)

twoway line count bill_start
	  
	   
******
use "$dirpath_data/temp/bill_data_202009_rename.dta", clear
gen bill_start_stata = date(bseg_start, "DMY")
format bill_start %td
gen count = 1
collapse(sum) count, by(bill_start)

twoway line count bill_start



append using "$dirpath_data/pge_raw/bill_data_20180827.dta"	   

gen bill_start_stata = date(bseg_start, "DMY")
format bill_start %td

sort sa_uuid bill_start

destring total_electric_usage, replace

gen count = 1
collapse(sum) count, by(bill_start)

twoway line count bill_start

twoway line total_electric_usage bill_start if sa_uuid == "4424020455"
	

*** energy efficiency data 
use "$dirpath_data/pge_raw/energy_efficiency_data_202009.dta", clear
rename (msk_sa_id) (sa_uuid)

/*
NEW DATA IS MISSING:
-- eega_code
-- eega_description
-- chk_issue_date (is this different from measure_date?)

OLD DATA IS MISSING:
- application_code
- program_code
- program_name
- program_desc
-program_subcat

CLARIFYING QUESTION: 
-- application_code looks very different from eega_code. are these supposed to be the same?

there is overlap: measure code, measure desc.

*/

save "$dirpath_data/temp/energy_efficiency_data_202009_rename.dta", replace





*** badge number data
use "$dirpath_data/pge_raw/meter_badge_number_data_202009.dta", clear
/*
 NEW DATA HAS: mtr_id
 OLD DATA HAS: sp_uuid
 I believe these are NOT the same? Otherwise data are the same, but dates are strings in new data
*/



*** pump data is missing a LOT of variables. will need to see whether these are problematic
use "$dirpath_data/pge_raw/pump_test_data_202009.dta", clear


*** excluded CCA data is new, and only at ZIP code level. 

