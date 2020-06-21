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

** 1. 2019 SCE data pull

** Make EE data unique by SA, to prep for merge
use "$dirpath_data/sce_cleaned/sce_ee_programs_20190916.dta", clear
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
use "$dirpath_data/sce_cleaned/sce_cust_detail_20190916.dta", clear
unique sa_uuid
assert r(unique)==r(N)

** Merge in EE measure counts
merge m:1 sa_uuid using `ee_unique'
assert _merge!=2
drop _merge

** Label
la var ee_measure_count "Count of EE measures for SA, over 2012-2018"
la var agpump_measure_count "Count of ag pumping EE measures for SA, over 2012-2018"

** Save
compress
save "$dirpath_data/sce_cleaned/sce_cust_detail_20190916.dta", replace

