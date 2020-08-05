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

** 1. 2019 and 2020 SCE data pulls

** Make EE data unique by SA, to prep for merge
use "$dirpath_data/sce_cleaned/sce_ee_programs_20190916.dta", clear
gen pull = 2019
append using "$dirpath_data/sce_cleaned/sce_ee_programs_20200722.dta"
replace pull = 2020 if pull==.
order pull
sort sa_uuid-savings_kwh
duplicates drop sa_uuid-savings_kwh, force
br // 2020 observations are actually extra

hist ee_install_date
keep sa_uuid eega_code ee_enduse ee_install_date 
gen ee_measure_count = 1
gen agpump_measure_count = ee_enduse=="Pumping" & eega_code=="Ag & Water Pumping"
tab ee_enduse agpump_measure_count
tab eega_code agpump_measure_count
collapse (sum) ee_measure_count agpump_measure_count, by(sa_uuid)
tab ee_measure_count agpump_measure_count
tempfile ee_unique
save `ee_unique'

** Load cleaned SCE customer data
use "$dirpath_data/sce_cleaned/sce_cust_detail.dta", clear
unique sa_uuid
assert r(unique)==r(N)

** Merge in EE measure counts
merge m:1 sa_uuid using `ee_unique'
count if _merge==2
drop if _merge==2
drop _merge

** Label
la var ee_measure_count "Count of EE measures for SA, over 2012-2018"
la var agpump_measure_count "Count of ag pumping EE measures for SA, over 2012-2018"

** Save
compress
save "$dirpath_data/sce_cleaned/sce_cust_detail.dta", replace

