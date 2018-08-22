clear all
version 13
set more off

*******************************************************************************
**** DIAGNOSTICS TO ESTIMATE HOW MUCH AG PUMPING DATA WE'RE MISSING   *********
*******************************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_code "S:/Louis/backup/AgEE/AgEE_code"

** additional directory paths
global dirpath_data "$dirpath/data"

*******************************************************************************

** Percentage of APEP data with misclassified NAICS

use "$dirpath_data/pge_cleaned/billing_data_20180719.dta", clear
tabstat total_bill_kwh, by(rt_sched_cd) s(sum)
gen ag_rate = substr(rt_sched_cd,1,2)=="AG" | substr(rt_sched_cd,1,3)=="HAG"
tabstat total_bill_kwh, by(ag_rate) s(sum)
di 1.77e10/2.06e10 // 86% of usage is on an AG tariff
tabstat total_bill_kwh if ag_rate==0, by(rt_sched_cd) s(sum)
collapse (sum) total_bill_kwh, by(sa_uuid ag_rate sp_uuid?) fast

joinby sa_uuid using "$dirpath_data/pge_cleaned/pge_cust_detail_20180719.dta", unmatched(both)
tab _merge
assert _merge!=1
drop if _merge==2
drop if sp_uuid!=sp_uuid1 & sp_uuid!=sp_uuid2 & sp_uuid!=sp_uuid3
drop sp_uuid1 sp_uuid2 sp_uuid3
duplicates drop
duplicates t sa_uuid sp_uuid ag_rate, gen(dup)
tab dup
br if dup>0
egen temp_max = max(total_bill_kwh), by(sp_uuid sa_uuid ag_rate)
drop if total_bill_kwh<temp_max & dup>0
unique sa_uuid sp_uuid ag_rate
assert r(unique)==r(N)
drop dup temp_max

gen naics111 = substr(prsn_naics,1,3)=="111"

tabstat total_bill_kwh if naics111, by(ag_rate) s(sum)
di 7.58e9/7.59e9 // 99.8% of NAICS=111 consmption is on an AG rate

tabstat total_bill_kwh, by(naics111) s(sum)
di 7.59e9/2.06e10 // 37% of TOTAL APEP consumption is coded as NAICS=111

tabstat total_bill_kwh if ag_rate, by(naics111) s(sum)
di 7.58e9/1.77e10 // 43% of APEP consumption on an AG rate is coded as NAICS=111

unique sp_uuid if ag_rate & naics111  // 7815 SPs on AG rate coded as NAICS=111
unique sp_uuid if ag_rate & !naics111 // 6469 SPs on AG rate NOT coded as NAICS=111
unique sa_uuid if ag_rate & naics111  // 16728 SAs on AG rate coded as NAICS=111
unique sa_uuid if ag_rate & !naics111 // 14049 SAs on AG rate NOT coded as NAICS=111


*******************************************************************************

** APEP AG 111 as a share of total AG 111

use "$dirpath_data/pge_cleaned/billing_data_20180719.dta", clear
tabstat total_bill_kwh, by(rt_sched_cd) s(sum)
gen ag_rate = substr(rt_sched_cd,1,2)=="AG" | substr(rt_sched_cd,1,3)=="HAG"
tabstat total_bill_kwh, by(ag_rate) s(sum)
di 1.77e10/2.06e10 // 86% of usage is on an AG tariff
tabstat total_bill_kwh if ag_rate==0, by(rt_sched_cd) s(sum)
collapse (sum) total_bill_kwh, by(sa_uuid ag_rate sp_uuid?) fast

joinby sa_uuid using "$dirpath_data/pge_cleaned/pge_cust_detail_20180719.dta", unmatched(both)
tab _merge
assert _merge!=1
drop if _merge==2
drop if sp_uuid!=sp_uuid1 & sp_uuid!=sp_uuid2 & sp_uuid!=sp_uuid3
drop sp_uuid1 sp_uuid2 sp_uuid3
duplicates drop
duplicates t sa_uuid sp_uuid ag_rate, gen(dup)
tab dup
br if dup>0
egen temp_max = max(total_bill_kwh), by(sp_uuid sa_uuid ag_rate)
drop if total_bill_kwh<temp_max & dup>0
unique sa_uuid sp_uuid ag_rate
assert r(unique)==r(N)
drop dup temp_max

gen naics111 = substr(prsn_naics,1,3)=="111"

keep if naics111==1
tempfile apep111
drop _merge
rename total_bill_kwh total_bill_kwh_july
save `apep111'


use "$dirpath_data/pge_cleaned/billing_data_20180322.dta", clear
tabstat total_bill_kwh, by(rt_sched_cd) s(sum)
gen ag_rate = substr(rt_sched_cd,1,2)=="AG" | substr(rt_sched_cd,1,3)=="HAG"
tabstat total_bill_kwh, by(ag_rate) s(sum)
di (2.41e10-82084.6)/2.41e10 // >99.99% of usage is on an AG tariff
tabstat total_bill_kwh if ag_rate==0, by(rt_sched_cd) s(sum)
collapse (sum) total_bill_kwh, by(sa_uuid ag_rate sp_uuid?) fast

joinby sa_uuid using "$dirpath_data/pge_cleaned/pge_cust_detail_20180322.dta", unmatched(both)
tab _merge
assert _merge!=1
drop if _merge==2
drop if sp_uuid!=sp_uuid1 & sp_uuid!=sp_uuid2 & sp_uuid!=sp_uuid3
drop sp_uuid1 sp_uuid2 sp_uuid3
duplicates drop
duplicates t sa_uuid sp_uuid ag_rate, gen(dup)
tab dup
br if dup>0
egen temp_max = max(total_bill_kwh), by(sp_uuid sa_uuid ag_rate)
drop if total_bill_kwh<temp_max & dup>0
unique sa_uuid sp_uuid ag_rate
assert r(unique)==r(N)
drop dup temp_max

assert substr(prsn_naics,1,3)=="111"
drop _merge

merge 1:1 sa_uuid sp_uuid ag_rate using `apep111'


correlate total_bill_kwh total_bill_kwh_july if _merge==3
gen pct_diff = (total_bill_kwh_july - total_bill_kwh)/total_bill_kwh
br total_bill_kwh* pct_diff if _merge==3 & !(abs(pct_diff)<1e-10 | pct_diff==.)
	// 37 bad SP/SAs out of 16,784


sum total_bill_kwh if _merge==3 & ag_rate==1
local rsum = r(sum)
sum total_bill_kwh if (_merge==1 | _merge==3) & ag_rate==1
di `rsum'/r(sum) // APEP AG-111 is getting us 31% of Total AG-111

sum total_bill_kwh if _merge==3 & ag_rate==1
di r(sum) // 7.6 TWh in APEP, AG, 111

sum total_bill_kwh if _merge==1 & ag_rate==1
di r(sum) // 16.5 TWh in not-APEP, AG, 111


unique sp_uuid if _merge==3 & ag_rate==1 // 7803 SPs in APEP, AG, 111
unique sp_uuid if _merge==1 & ag_rate==1 // 53346 SPs in not-APEP, AG, 111

unique sa_uuid if _merge==3 & ag_rate==1 // 16666 SAs in APEP, AG, 111
unique sa_uuid if _merge==1 & ag_rate==1 // 109921 SAs in not-APEP, AG, 111


*******************************************************************************

use "$dirpath_data/pge_cleaned/billing_data_20180719.dta", clear
tabstat total_bill_kwh, by(rt_sched_cd) s(sum)
gen ag_rate = substr(rt_sched_cd,1,2)=="AG" | substr(rt_sched_cd,1,3)=="HAG"
tabstat total_bill_kwh, by(ag_rate) s(sum)
di 1.77e10/2.06e10 // 86% of usage is on an AG tariff
tabstat total_bill_kwh if ag_rate==0, by(rt_sched_cd) s(sum)
collapse (sum) total_bill_kwh (min) min_dt=bill_start_dt (max) max_dt=bill_start_dt ///
	, by(sa_uuid ag_rate sp_uuid?) fast

joinby sa_uuid using "$dirpath_data/pge_cleaned/pge_cust_detail_20180719.dta", unmatched(both)
tab _merge
assert _merge!=1
drop if _merge==2
drop if sp_uuid!=sp_uuid1 & sp_uuid!=sp_uuid2 & sp_uuid!=sp_uuid3
drop sp_uuid1 sp_uuid2 sp_uuid3
duplicates drop
duplicates t sa_uuid sp_uuid ag_rate, gen(dup)
tab dup
br if dup>0
egen temp_max = max(total_bill_kwh), by(sp_uuid sa_uuid ag_rate)
drop if total_bill_kwh<temp_max & dup>0
unique sa_uuid sp_uuid ag_rate
assert r(unique)==r(N)
drop dup temp_max

gen naics111 = substr(prsn_naics,1,3)=="111"

tabstat total_bill_kwh if naics111, by(ag_rate) s(sum)
di 7.58e9/7.59e9 // 99.8% of NAICS=111 consmption is on an AG rate

tabstat total_bill_kwh, by(naics111) s(sum)
di 7.59e9/2.06e10 // 37% of TOTAL APEP consumption is coded as NAICS=111

tabstat total_bill_kwh if ag_rate, by(naics111) s(sum)
di 7.58e9/1.77e10 // 43% of APEP consumption on an AG rate is coded as NAICS=111


egen max_ag_rate = max(ag_rate), by(sp_uuid)
egen min_ag_rate = min(ag_rate), by(sp_uuid)
tabstat total_bill_kwh if min_ag_rate<max_ag_rate, by(ag_rate) s(sum)
tabstat total_bill_kwh if min_ag_rate==max_ag_rate, by(ag_rate) s(sum)


