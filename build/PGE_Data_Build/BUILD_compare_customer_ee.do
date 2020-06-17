clear all
version 13
set more off

*********************************************************************
**** Script to compare customer data with energy efficiency data ****
*********************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

*******************************************************************************
*******************************************************************************

** 1. March 2018 data pull
if 1==0{

** Make EE data unique by SA, to prep for merge
use  "$dirpath_data/pge_cleaned/pge_ee_programs_20180322.dta", clear
keep sa_uuid eega_code measure_code ee_measure_date 
unique sa_uuid eega_code measure_code ee_measure_date
assert r(unique)==r(N)
gen ee_measure_count = 1
collapse (sum) ee_measure_count, by(sa_uuid)
tab ee_measure_count
tempfile ee_unique
save `ee_unique'

** Load cleaned PGE bililng data
use "$dirpath_data/pge_cleaned/pge_cust_detail_20180322.dta", clear
unique sp_uuid sa_uuid
assert r(unique)==r(N)

** Merge in EE measure counts
merge m:1 sa_uuid using `ee_unique'
assert _merge!=2
drop _merge

** Label
la var ee_measure_count "Count of EE measures for SA, over 2010-2017"

** Save
compress
save "$dirpath_data/pge_cleaned/pge_cust_detail_20180322.dta", replace

}

*******************************************************************************
*******************************************************************************

** 2. July 2018 data pull
if 1==1{

** Make EE data unique by SA, to prep for merge
use  "$dirpath_data/pge_cleaned/pge_ee_programs_20180719.dta", clear
keep sa_uuid eega_code measure_code ee_measure_date 
unique sa_uuid eega_code measure_code ee_measure_date
assert r(unique)==r(N)
gen ee_measure_count = 1
collapse (sum) ee_measure_count, by(sa_uuid)
tab ee_measure_count
tempfile ee_unique
save `ee_unique'

** Load cleaned PGE bililng data
use "$dirpath_data/pge_cleaned/pge_cust_detail_20180719.dta", clear
unique sp_uuid sa_uuid
assert r(unique)==r(N)

** Merge in EE measure counts
merge m:1 sa_uuid using `ee_unique'
assert _merge!=2
drop _merge

** Label
la var ee_measure_count "Count of EE measures for SA, over 2010-2017"

** Save
compress
save "$dirpath_data/pge_cleaned/pge_cust_detail_20180719.dta", replace

}

*******************************************************************************
*******************************************************************************

** 3. August 2018 data pull
if 1==1{

** Make EE data unique by SA, to prep for merge
use  "$dirpath_data/pge_cleaned/pge_ee_programs_20180827.dta", clear
keep sa_uuid eega_code measure_code ee_measure_date 
unique sa_uuid eega_code measure_code ee_measure_date
assert r(unique)==r(N)
gen ee_measure_count = 1
collapse (sum) ee_measure_count, by(sa_uuid)
tab ee_measure_count
tempfile ee_unique
save `ee_unique'

** Load cleaned PGE bililng data
use "$dirpath_data/pge_cleaned/pge_cust_detail_20180827.dta", clear
unique sp_uuid sa_uuid
assert r(unique)==r(N)

** Merge in EE measure counts
merge m:1 sa_uuid using `ee_unique'
assert _merge!=2
drop _merge

** Label
la var ee_measure_count "Count of EE measures for SA, over 2010-2017"

** Save
compress
save "$dirpath_data/pge_cleaned/pge_cust_detail_20180827.dta", replace

}

*******************************************************************************
*******************************************************************************


