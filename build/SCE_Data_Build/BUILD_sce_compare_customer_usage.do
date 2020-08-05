clear all
version 13
set more off

***********************************************************************
**** Script to compare customer data with billing and interval data ***
***********************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

*******************************************************************************
*******************************************************************************

** 1. SCE 2019 and 2020 data pulls

** Load cleaned SCE bililng data
use "$dirpath_data/sce_cleaned/sce_cust_detail_20190916.dta", clear
unique sa_uuid
assert r(unique)==r(N)

** Merge with billing data
merge 1:m sa_uuid using "$dirpath_data/sce_cleaned/billing_data.dta"
assert _merge!=2

** Create flag for presence in billing data
gen in_billing = 0
replace in_billing = 1 if _merge==3

** Store first/last merged bills
egen bill_dt_first = min(bill_start_dt), by(sa_uuid)
egen bill_dt_last = max(bill_end_dt), by(sa_uuid)
format %td bill_dt_first bill_dt_last

** Drop billing data and collapse back to customer data
drop bill_start_dt-_merge
duplicates drop
unique sa_uuid
assert r(unique)==r(N)
tab in_billing

** Label
la var in_billing "Dummy=1 if SP/SA appears in our billing data"
la var bill_dt_first "Earliest bill start date in our billing data"
la var bill_dt_last "Latest bill end date in our billing data"

** Merge with daily interval data
preserve
use "$dirpath_data/sce_cleaned/interval_data_daily_20190916.dta", clear
gen pull = 2019
append using "$dirpath_data/sce_cleaned/interval_data_daily_20200722.dta"
replace pull = 2020 if pull==.
duplicates drop sa_uuid date kwh, force
duplicates t sa_uuid date, gen(dup)
sort sa_uuid date pull
br if dup>0
unique sa_uuid date
local uniq = r(unique)
drop if dup>0 & pull==2019
unique sa_uuid date
assert r(unique)==r(N)
tempfile daily
save `daily'
restore
merge 1:m sa_uuid using `daily'
assert _merge!=2

** Create flag for presence in interval data
gen in_interval = 0
replace in_interval = 1 if _merge==3
 
** Store first/last days in interval data
egen interval_dt_first = min(date) if in_billing==1, by(sa_uuid)
egen interval_dt_last = max(date) if in_billing==1, by(sa_uuid)
format %td interval_dt_first interval_dt_last

** Drop interval data and collapse back to customer data
drop _merge date kwh pull dup
duplicates drop
unique sa_uuid
assert r(unique)==r(N)
tab in_billing in_interval

** Label
la var in_interval "Dummy=1 if SP/SA appears in our interval data"
la var interval_dt_first "Earliest date in our interval data"
la var interval_dt_last "Latest date in our interval data"

** Save updated version of customer data
compress
sort sa_uuid
save "$dirpath_data/sce_cleaned/sce_cust_detail.dta", replace

** Remove date-tagged customer details, to avoid confusion
erase "$dirpath_data/sce_cleaned/sce_cust_detail_20190916.dta"

*******************************************************************************
*******************************************************************************

