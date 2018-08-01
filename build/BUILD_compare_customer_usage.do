clear all
version 13
set more off

***********************************************************************
**** Script to compare customer data with billing and interval data ***
***********************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

*******************************************************************************
*******************************************************************************

** 1. March 2018 data pull
if 1==0{

** Load cleaned PGE bililng data
use "$dirpath_data/pge_cleaned/pge_cust_detail_20180322.dta", clear
unique sp_uuid sa_uuid
assert r(unique)==r(N)

** Merge with billing data
joinby sa_uuid using "$dirpath_data/pge_cleaned/billing_data_20180322.dta", unmatched(both)
assert _merge!=2

** Create flag for presence in billing data
egen temp_max_merge1 = max(_merge) if (sp_uuid==sp_uuid1 | sp_uuid==sp_uuid2 | sp_uuid==sp_uuid3), ///
	by(sp_uuid sa_uuid)
egen temp_max_merge2 = mean(temp_max_merge1), by(sp_uuid sa_uuid)
gen in_billing = 0
replace in_billing = 1 if temp_max_merge2==3

** Store first/last merged bills
replace bill_start_dt = . if !(sp_uuid==sp_uuid1 | sp_uuid==sp_uuid2 | sp_uuid==sp_uuid3)
replace bill_end_dt = . if !(sp_uuid==sp_uuid1 | sp_uuid==sp_uuid2 | sp_uuid==sp_uuid3)
egen bill_dt_first = min(bill_start_dt), by(sp_uuid sa_uuid)
egen bill_dt_last = max(bill_end_dt), by(sp_uuid sa_uuid)
format %td bill_dt_first bill_dt_last

** Drop billing data and collapse back to customer data
drop _merge-temp_max_merge2
duplicates drop
unique sp_uuid sa_uuid
assert r(unique)==r(N)
tab in_billing

** Label
la var in_billing "Dummy=1 if SP/SA appears in our billing data"
la var bill_dt_first "Earliest bill start date in our billing data"
la var bill_dt_last "Latest bill end date in our billing data"

** Merge with daily interval data
joinby sa_uuid using "$dirpath_data/pge_cleaned/interval_data_daily_20180322.dta", unmatched(both)
assert _merge!=2

** Create flag for presence in interval data
egen temp_max_merge1 = max(_merge) if in_billing==1, ///
	by(sp_uuid sa_uuid)
egen temp_max_merge2 = mean(temp_max_merge1), by(sp_uuid sa_uuid)
gen in_interval = 0
replace in_interval = 1 if temp_max_merge2==3
 
** Store first/last days in interval data
egen interval_dt_first = min(date) if in_billing==1, by(sp_uuid sa_uuid)
egen interval_dt_last = max(date) if in_billing==1, by(sp_uuid sa_uuid)
format %td interval_dt_first interval_dt_last

** Drop interval data and collapse back to customer data
drop _merge-temp_max_merge2
duplicates drop
unique sp_uuid sa_uuid
assert r(unique)==r(N)
tab in_billing in_interval

** Label
la var in_interval "Dummy=1 if SP/SA appears in our interval data"
la var interval_dt_first "Earliest date in our interval data"
la var interval_dt_last "Latest date in our interval data"

** Save updated version of customer data
compress
sort prsn_uuid sp_uuid sa_uuid
save "$dirpath_data/pge_cleaned/pge_cust_detail_20180322.dta", replace

}

*******************************************************************************
*******************************************************************************

** 2. July 2018 data pull
{
** Load cleaned PGE bililng data
use "$dirpath_data/pge_cleaned/pge_cust_detail_20180719.dta", clear
unique sp_uuid sa_uuid
assert r(unique)==r(N)

** Merge with billing data
joinby sa_uuid using "$dirpath_data/pge_cleaned/billing_data_20180719.dta", unmatched(both)
assert _merge!=2

** Create flag for presence in billing data
egen temp_max_merge1 = max(_merge) if (sp_uuid==sp_uuid1 | sp_uuid==sp_uuid2 | sp_uuid==sp_uuid3), ///
	by(sp_uuid sa_uuid)
egen temp_max_merge2 = mean(temp_max_merge1), by(sp_uuid sa_uuid)
gen in_billing = 0
replace in_billing = 1 if temp_max_merge2==3

** Store first/last merged bills
replace bill_start_dt = . if !(sp_uuid==sp_uuid1 | sp_uuid==sp_uuid2 | sp_uuid==sp_uuid3)
replace bill_end_dt = . if !(sp_uuid==sp_uuid1 | sp_uuid==sp_uuid2 | sp_uuid==sp_uuid3)
egen bill_dt_first = min(bill_start_dt), by(sp_uuid sa_uuid)
egen bill_dt_last = max(bill_end_dt), by(sp_uuid sa_uuid)
format %td bill_dt_first bill_dt_last

** Drop billing data and collapse back to customer data
drop _merge-temp_max_merge2
duplicates drop
unique sp_uuid sa_uuid
assert r(unique)==r(N)
tab in_billing

** Label
la var in_billing "Dummy=1 if SP/SA appears in our billing data"
la var bill_dt_first "Earliest bill start date in our billing data"
la var bill_dt_last "Latest bill end date in our billing data"

** Merge with daily interval data
joinby sa_uuid using "$dirpath_data/pge_cleaned/interval_data_daily_20180719.dta", unmatched(both)
assert _merge!=2

** Create flag for presence in interval data
egen temp_max_merge1 = max(_merge) if in_billing==1, ///
	by(sp_uuid sa_uuid)
egen temp_max_merge2 = mean(temp_max_merge1), by(sp_uuid sa_uuid)
gen in_interval = 0
replace in_interval = 1 if temp_max_merge2==3
 
** Store first/last days in interval data
egen interval_dt_first = min(date) if in_billing==1, by(sp_uuid sa_uuid)
egen interval_dt_last = max(date) if in_billing==1, by(sp_uuid sa_uuid)
format %td interval_dt_first interval_dt_last

** Drop interval data and collapse back to customer data
drop _merge-temp_max_merge2
duplicates drop
unique sp_uuid sa_uuid
assert r(unique)==r(N)
tab in_billing in_interval

** Label
la var in_interval "Dummy=1 if SP/SA appears in our interval data"
la var interval_dt_first "Earliest date in our interval data"
la var interval_dt_last "Latest date in our interval data"

** Save updated version of customer data
compress
sort prsn_uuid sp_uuid sa_uuid
save "$dirpath_data/pge_cleaned/pge_cust_detail_20180719.dta", replace

}

*******************************************************************************
*******************************************************************************

