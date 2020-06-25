clear all
version 13
set more off

*****************************************************************
**** Script to create simple summary statistics on SCE rates ****
*****************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

	// start with billing data
use "$dirpath_data/sce_cleaned/billing_data_20190916.dta", replace

	// merge with pump test data
joinby sa_uuid using "$dirpath_data/sce_cleaned/sce_pump_test_data.dta", unmatched(both)
tab _merge

	// assess unmatched pump tests
unique uniq_id if _merge==3
unique uniq_id if _merge==2
br if _merge==2
	// unmatched pump tests are mostly non-ag (municipal, Exxon, water districts, etc.)

	// assess unmatched SAs
unique sa_uuid if _merge!=2 // 42,984 SAs have bills
unique sa_uuid if _merge==3	// 10,081 SAs match to a pump test
unique sa_uuid if _merge==3 & booster_pump==0 // 8,049 SAs match to a non-booster pump test

	// assess unmatched SPs
unique sp_uuid if _merge!=2	// 31,238 SPs have bills
unique sp_uuid if _merge==3 // 9,581 SPs match to a pump test
unique sp_uuid if _merge==3 & booster_pump==0 // 7,627 SPs match to a non-booster pump test

	// see if unmatched pump tests happen to be in customer details?
preserve
keep if _merge==2
keep sa_uuid
duplicates drop
merge 1:m sa_uuid using  "$dirpath_data/sce_cleaned/sce_cust_detail_20190916.dta",	
tab _merge // only 21 matches
restore	
drop if _merge==2

	// collapse by tariff-year
gen count_bills = 1
gen year = year(bill_start_dt)
collapse (sum) count_bills total_bill_kwh, by(tariff_sched year _merge) fast
gen pump_match = _merge==3
drop _merge
reshape wide count_bills total_bill_kwh, i(tariff_sched_text year) j(pump_match)
gen count_bills_all = count_bills0 + count_bills1
gen total_kwh_all = total_bill_kwh0 + total_bill_kwh1
rename count_bills1 count_bills_pumpers
rename total_bill_kwh1 total_kwh_pumpers
drop count_bills0 total_bill_kwh0
gsort year -total_kwh_pumper
foreach v of varlist count* total* {
	egen temp = sum(`v'), by(year)
	local v2 = subinstr(subinstr("`v'","count","pct",1),"total","pct",1)
	gen `v2' = `v'/temp
	drop temp
}
order tariff_sched_text year count_bills_pumpers pct_bills_pumpers total_kwh_pumpers ///
	pct_kwh_pumpers count_bills_all pct_bills_all total_kwh_all pct_kwh_all
egen temp = rmax(count* total* pct*)	
drop if temp==.
drop temp
	
compress
save "$dirpath_data/sce_cleaned/sce_rate_diagnostics.dta", replace	

