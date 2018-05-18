clear all
version 13
set more off

***************************************************************************
**** Script to import and clean raw PGE data -- billing data file *********
***************************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

** Load raw PGE billing data
use "$dirpath_data/pge_raw/bill_data.dta", clear

** Service agreement ID
assert sa_uuid!=""
assert length(sa_uuid)==10
unique sa_uuid // 162809 unique service agreements
unique sa_uuid bseg_start_dt // 163160 unique customer-service point-service aggrements
duplicates r sa_uuid bseg_start_dt
duplicates t sa_uuid bseg_start_dt, gen(dup)
*br if dup>0 // a BUNCH of dups (almost 1% of account-bills), which will be tricky to parse!
drop dup
la var sa_uuid "Service Agreement ID (anonymized, 10-digit)"

** Start and end dates
assert bseg_start_dt!="" & bseg_end_dt!=""
gen bill_start_dt = date(bseg_start_dt,"DMY")
gen bill_end_dt = date(bseg_end_dt,"DMY")
format %td bill_start_dt bill_end_dt
assert bill_start_dt!=. & bill_end_dt!=. // no missings
assert bill_start_dt<=bill_end_dt // end date never prior to start date
gen bill_length = bill_end_dt-bill_start_dt+1
tab bill_length // 0.5% of observations have bill period > 34 days! (97% b/tw 28-34 days)
drop bseg_start_dt bseg_end_dt
order sa_uuid bill_start_dt bill_end_dt bill_length
la var bill_start_dt "Bill period start date"
la var bill_end_dt "Bill period end date"
la var bill_length "Length of bill period (in days)"

** Rate schedule
count if rt_sched_cd=="" // only 1 observation missing
sort sa_uuid bill_start_dt
egen temp1 = mode(rt_sched_cd), by(sa_uuid)
egen temp2 = max(rt_sched_cd==""), by(sa_uuid)
assert (rt_sched_cd==temp1 | rt_sched_cd=="") if temp2==1 // missing is unambiguous
replace rt_sched_cd = temp1 if rt_sched_cd=="" // populate missing
drop temp1 temp2
la var rt_sched_cd "Rate schedule at end of billing cycle"

** Usage and demand
destring total_electric_usage max_demand peak_demand partial_peak_demand, replace
count if total_electric_usage==. // 813 missings out of 5.27M
count if max_demand==. // 2.97M missings out of 5.27M
count if peak_demand==. // 5.13M missings out of 5.72M
count if partial_peak_demand==. // 5.17M missings out of 5.27M

** Bill amount
destring total_bill_amount, replace
assert total_bill_amount!=. // never missing!
gen perkwh = total_bill_amount/total_electric_usage
sum perkwh, detail // p5 = 11 cents/kwh, p95 = 8.5 cents/kWh
drop perkwh

** Labels
rename total_electric_usage total_bill_kwh
la var total_bill_kwh "Total billed electric usage (kWh)"
la var max_demand "Max demand in bill period (kWh)" 
la var peak_demand "Peak demand in bill period (kWh)"
la var partial_peak_demand "Partial peak demand in bill period (kWh)"
la var total_bill_amount "Total bill amount ($)"

** Duplicate bills: everything matches except bill amount
duplicates t sa_uuid bill_start_dt bill_end_dt rt_sched_cd total_bill_kwh, gen(dup)
foreach v of varlist total_bill_amount max_demand peak_demand partial_peak_demand {
	egen double temp = mean(`v'), by(sa_uuid bill_start_dt bill_end_dt rt_sched_cd total_bill_kwh)
	replace `v' = temp if dup>0
	drop temp
}
drop dup
duplicates drop
unique sa_uuid bill_start_dt bill_end_dt rt_sched_cd total_bill_kwh
assert r(unique)==r(N)
	
** Duplicate bills: everything matches except bill amount and usage, and there's a zero	
duplicates t sa_uuid bill_start_dt bill_end rt_sched_cd, gen(dup)
tab dup
sort sa_uuid bill_start_dt bill_end_dt rt_sched_cd
br if dup>0
unique sa_uuid bill_start_dt bill_end_dt rt_sched_cd if dup>0 // 23814 dups
unique sa_uuid bill_start_dt bill_end_dt rt_sched_cd if dup>0 & total_bill_kwh==0
	// 2/3 of dups have a 0 kWh as at least 1 of the dups
egen temp = max(total_bill_kwh==0), by(sa_uuid bill_start_dt bill_end_dt rt_sched_cd)
foreach v of varlist total_bill_kwh total_bill_amount {
	egen double temp2 = sum(`v'), by(sa_uuid bill_start_dt bill_end_dt rt_sched_cd)
	replace `v' = temp2 if dup>0 & temp==1
	drop temp2
}
foreach v of varlist max_demand peak_demand partial_peak_demand {
	egen double temp2 = max(`v'), by(sa_uuid bill_start_dt bill_end_dt rt_sched_cd)
	replace `v' = temp2 if dup>0 & temp==1
	drop temp2
}
drop dup temp
duplicates drop

** Duplicate bills: everything matches except bill amount and usage, and there's not a zero	
duplicates t sa_uuid bill_start_dt bill_end rt_sched_cd, gen(dup)
tab dup
sort sa_uuid bill_start_dt bill_end_dt rt_sched_cd
br if dup>0
unique sa_uuid bill_start_dt bill_end_dt rt_sched_cd if dup>0 // 7706 dups
foreach v of varlist total_bill_kwh total_bill_amount {
	egen double temp = sum(`v'), by(sa_uuid bill_start_dt bill_end_dt rt_sched_cd)
	replace `v' = temp if dup>0
	drop temp
}
foreach v of varlist max_demand peak_demand partial_peak_demand {
	egen double temp = max(`v'), by(sa_uuid bill_start_dt bill_end_dt rt_sched_cd)
	replace `v' = temp if dup>0 
	drop temp
}
drop dup
duplicates drop
unique sa_uuid bill_start_dt bill_end_dt rt_sched_cd
assert r(unique)==r(N)

** Duplicate bills: same account, same start/end dates, same kwh, different tariffs
duplicates t sa_uuid bill_start_dt bill_end total_bill_kwh, gen(dup)
tab dup //  <0.2% of total observations 
sort sa_uuid bill_start_dt bill_end_dt total_bill_kwh
gen temp1 = sa_uuid==sa_uuid[_n+1] & dup==0 & dup[_n+1]>0 // to grab tariff of following bill
gen temp2 = sa_uuid==sa_uuid[_n-1] & dup==0 & dup[_n-1]>0 // to grab tariff of preceding bill
br if dup>0 | temp1==1 | temp2==1
	
	// reshape duplicate rate schedules
gen temp_keep = .
replace temp_keep = 1 if dup>0 & !(sa_uuid==sa_uuid[_n-1] & bill_start_dt==bill_start_dt[_n-1] ///
	& bill_end_dt==bill_end_dt[_n-1] & total_bill_kwh==total_bill_kwh[_n-1])
gen rt_sched1 = ""
replace rt_sched1 = rt_sched_cd if dup>0 & temp_keep==1
gen rt_sched2 = ""
replace rt_sched2 = rt_sched_cd[_n+1] if sa_uuid==sa_uuid[_n+1] & bill_start_dt==bill_start_dt[_n+1] ///
	& bill_end_dt==bill_end_dt[_n+1] & total_bill_kwh==total_bill_kwh[_n+1] & temp_keep==1
gen rt_sched3 = ""
replace rt_sched3 = rt_sched_cd[_n+2] if sa_uuid==sa_uuid[_n+2] & bill_start_dt==bill_start_dt[_n+2] ///
	& bill_end_dt==bill_end_dt[_n+2] & total_bill_kwh==total_bill_kwh[_n+2] & temp_keep==1
foreach v of varlist max_demand peak_demand partial_peak_demand total_bill_amount {
	egen double temp_`v' = mean(`v'), by(sa_uuid bill_start_dt bill_end_dt total_bill_kwh)
}

	// drop duplicates
unique sa_uuid bill_start_dt bill_end_dt total_bill_kwh
local uniq = r(unique)
keep if temp_keep==1 | dup==0
unique sa_uuid bill_start_dt bill_end_dt total_bill_kwh
assert r(unique)==`uniq'
replace rt_sched_cd = "" if dup>0 & temp_keep==1
	
	// assign the (matching) tariff of the Bill After the Last Dup (BALD)
sort sa_uuid bill_start_dt bill_end_dt total_bill_kwh
gen temp_BALD = 0 if dup==0
forvalues i = 1/40 {
	replace temp_BALD = `i' if temp_BALD==. & temp_BALD[_n+`i']==0 & ///
		dup>0 & temp_keep==1 & sa_uuid==sa_uuid[_n+`i']
}
forvalues i = 1/40 {
	replace rt_sched_cd = rt_sched_cd[_n+`i'] if rt_sched_cd=="" & dup>0 & temp_keep==1 & ///
		sa_uuid==sa_uuid[_n+`i'] & rt_sched_cd[_n+`i']!="" & (rt_sched_cd[_n+`i']==rt_sched1 | ///
		rt_sched_cd[_n+`i']==rt_sched2 | rt_sched_cd[_n+`i']==rt_sched3) & temp_BALD==`i'
}	
	
	// for remaining missings, assign the (matching) tariff of Bill Before the Last Dup (BBLD) 
gen temp_BBLD = 0 if dup==0
forvalues i = 1/40 {
	replace temp_BBLD = `i' if temp_BBLD==. & temp_BBLD[_n-`i']==0 & ///
		dup>0 & temp_keep==1 & sa_uuid==sa_uuid[_n-`i']
}
forvalues i = 1/40 {
	replace rt_sched_cd = rt_sched_cd[_n-`i'] if rt_sched_cd=="" & dup>0 & temp_keep==1 & ///
		sa_uuid==sa_uuid[_n-`i'] & rt_sched_cd[_n-`i']!="" & (rt_sched_cd[_n-`i']==rt_sched1 | ///
		rt_sched_cd[_n-`i']==rt_sched2 | rt_sched_cd[_n-`i']==rt_sched3) & temp_BBLD==`i'
}	
	
	// for remaining missings, assing the tariff of the Bill After the Last Dup (BALD)
forvalues i = 1/40 {
	replace rt_sched_cd = rt_sched_cd[_n+`i'] if rt_sched_cd=="" & dup>0 & temp_keep==1 & ///
		sa_uuid==sa_uuid[_n+`i'] & rt_sched_cd[_n+`i']!="" & temp_BALD==`i'
}	
	
	// for remaining missings, assign the tariff of Bill Before the Last Dup (BBLD) 
forvalues i = 1/40 {
	replace rt_sched_cd = rt_sched_cd[_n-`i'] if rt_sched_cd=="" & dup>0 & temp_keep==1 & ///
		sa_uuid==sa_uuid[_n-`i'] & rt_sched_cd[_n-`i']!="" & temp_BBLD==`i'
}	
	
	// for remaining missings, pick a tariff at random (only 29 SAs, and only 3 have 10+ bills)
replace rt_sched_cd = rt_sched1 if rt_sched_cd=="" & dup>0 & temp_keep==1
assert rt_sched_cd!=""
	
	// populate averaged variables from before collapse
foreach v of varlist max_demand peak_demand partial_peak_demand total_bill_amount {
	replace `v' = temp_`v' if dup>0 & temp_keep==1
}
	// flag observations with multiple tariffs
gen flag_multi_tariff = temp_keep==1
la var flag_multi_tariff "Flag for bills with duplicate (alternative) tariffs"

	// clean up
drop dup temp* rt_sched?

** Duplicate bills: same account, same start/end dates, different kwh, different tariffs
duplicates t sa_uuid bill_start_dt bill_end, gen(dup)
tab dup //  <0.02% of total observations 
sort sa_uuid bill_start_dt bill_end_dt total_bill_kwh
gen temp1 = sa_uuid==sa_uuid[_n+1] & dup==0 & dup[_n+1]>0 // to grab tariff of following bill
gen temp2 = sa_uuid==sa_uuid[_n-1] & dup==0 & dup[_n-1]>0 // to grab tariff of preceding bill
br if dup>0 | temp1==1 | temp2==1
	
	// start by dropping zeros (64% of all dups)
egen temp_max_zero = max(total_bill_kwh==0), by(sa_uuid bill_start_dt bill_end)
egen temp_min_zero = min(total_bill_kwh==0), by(sa_uuid bill_start_dt bill_end)
unique sa_uuid bill_start_dt bill_end_dt
local uniq = r(unique)
drop if dup>0 & temp_max_zero==1 & temp_min_zero==0 & total_bill_kwh==0
unique sa_uuid bill_start_dt bill_end_dt
assert r(unique)==`uniq'

	// flag remaining dups
duplicates t sa_uuid bill_start_dt bill_end, gen(dup2)
gsort sa_uuid bill_start_dt bill_end_dt -total_bill_kwh
drop temp1 temp2
gen temp1 = sa_uuid==sa_uuid[_n+1] & dup2==0 & dup2[_n+1]>0 // to grab tariff of preceding bill
gen temp2 = sa_uuid==sa_uuid[_n-1] & dup2==0 & dup2[_n-1]>0 // to grab tariff of following bill
br if dup2>0 | temp1==1 | temp2==1
	
	// reshape duplicate rate schedules
gen temp_keep = .
replace temp_keep = 1 if dup2>0 & !(sa_uuid==sa_uuid[_n-1] & bill_start_dt==bill_start_dt[_n-1] ///
	& bill_end_dt==bill_end_dt[_n-1])
gen rt_sched1 = ""
replace rt_sched1 = rt_sched_cd if dup2>0 & temp_keep==1
gen rt_sched2 = ""
replace rt_sched2 = rt_sched_cd[_n+1] if sa_uuid==sa_uuid[_n+1] & bill_start_dt==bill_start_dt[_n+1] ///
	& bill_end_dt==bill_end_dt[_n+1] & temp_keep==1
foreach v of varlist max_demand peak_demand partial_peak_demand {
	egen double temp_`v' = mean(`v'), by(sa_uuid bill_start_dt bill_end_dt)
}
foreach v of varlist total_bill_kwh total_bill_amount {
	egen double temp_`v' = sum(`v'), by(sa_uuid bill_start_dt bill_end_dt)
}

	// drop duplicates
unique sa_uuid bill_start_dt bill_end_dt
local uniq = r(unique)
keep if temp_keep==1 | dup2==0
unique sa_uuid bill_start_dt bill_end_dt
assert r(unique)==`uniq'
replace rt_sched_cd = "" if dup2>0 & temp_keep==1
	
	// tag Bill After the Last Dup (BALD) and Bill Before the Last Dup (BBLD)
sort sa_uuid bill_start_dt bill_end_dt
gen temp_BALD = 0 if dup2==0
forvalues i = 1/30 {
	replace temp_BALD = `i' if temp_BALD==. & temp_BALD[_n+`i']==0 & ///
		dup2>0 & temp_keep==1 & sa_uuid==sa_uuid[_n+`i']
}
gen temp_BBLD = 0 if dup2==0
forvalues i = 1/30 {
	replace temp_BBLD = `i' if temp_BBLD==. & temp_BBLD[_n-`i']==0 & ///
		dup2>0 & temp_keep==1 & sa_uuid==sa_uuid[_n-`i']
}

	// if rate appears to have *CHANGED*, collapse and sum
gen temp_to_sum = 0
replace temp_to_sum = 1 if temp_keep==1 & dup2>0 & temp_BALD==1 & temp_BBLD==1 & ///
	((rt_sched1==rt_sched_cd[_n-1])+(rt_sched2==rt_sched_cd[_n-1]))==1 & ///
	((rt_sched1==rt_sched_cd[_n+1])+(rt_sched2==rt_sched_cd[_n+1]))==1 & ///
	rt_sched_cd[_n-1]!=rt_sched_cd[_n+1] & sa_uuid==sa_uuid[_n-1] & sa_uuid==sa_uuid[_n+1]
tab temp_to_sum if temp_keep==1 // only 14 out of 410 dups, so this is not a good strategy

	// sum dups and assign the rate wihh the larger kWh
replace rt_sched_cd = rt_sched1 if temp_keep & dup2>0	
assert rt_sched_cd!=""
	
	// populate averaged/summed variables from before collapse
foreach v of varlist total_bill_kwh max_demand peak_demand partial_peak_demand total_bill_amount {
	replace `v' = temp_`v' if dup2>0 & temp_keep==1
}
	// flag observations with multiple tariffs
gen flag_bad_tariff = temp_keep==1
la var flag_bad_tariff "Flag for duplicate bills with multiple, unresolvable tariffs that are summed"

	// clean up
drop dup dup2 temp* rt_sched?

** Confirm bill uniqueness for SA, start date, end date
unique sa_uuid bill_start_dt bill_end_dt
assert r(unique)==r(N)

** Duplicate bills: same account, same start date, different end dates, same kWh
duplicates t sa_uuid bill_start_dt total_bill_kwh, gen(dup)	
tab dup // 0.2% of bills
duplicates t sa_uuid bill_start_dt total_bill_kwh rt_sched, gen(dup2)	
tab dup dup2 // almost all don't have a tariff conflict
sort sa_uuid bill_start_dt bill_end_dt
gen temp = sa_uuid==sa_uuid[_n-1] & dup==0 & dup[_n-1]>0 // to flag following bill
br if dup>0 | temp==1 

	// how close is end date to next start date?
gen temp_date_min = .
replace temp_date_min = bill_end_dt-bill_start_dt[_n+1] if dup>0 & temp[_n+1]==1 & ///
	sa_uuid==sa_uuid[_n+1]
replace temp_date_min = bill_end_dt-bill_start_dt[_n+2] if dup>0 & temp[_n+2]==1 & ///
	sa_uuid==sa_uuid[_n+2] & dup==dup[_n+1] & temp[_n+1]==0 & sa_uuid==sa_uuid[_n+1] & ///
	bill_start_dt==bill_start_dt[_n+1] & total_bill_kwh==total_bill_kwh[_n+1]
replace temp_date_min = bill_end_dt-bill_start_dt[_n+3] if dup>0 & temp[_n+3]==1 & ///
	sa_uuid==sa_uuid[_n+3] & dup==dup[_n+1] & temp[_n+1]==0 & sa_uuid==sa_uuid[_n+1] & ///
	bill_start_dt==bill_start_dt[_n+1] & total_bill_kwh==total_bill_kwh[_n+1] & dup==dup[_n+2] & ///
	temp[_n+2]==0 & sa_uuid==sa_uuid[_n+2] & bill_start_dt==bill_start_dt[_n+2] & ///
	total_bill_kwh==total_bill_kwh[_n+2]
egen temp_date_min2 = min(abs(temp_date_min)), by(sa_uuid bill_start_dt total_bill_kwh)

	// consolidate if kWh matches (averaging cost, picking end date closest to next start date)
 foreach v of varlist max_demand peak_demand partial_peak_demand flag* {
	egen double temp_`v' = max(`v'), by(sa_uuid bill_start_dt total_bill_kwh)
}
foreach v of varlist total_bill_amount {
	egen double temp_`v' = mean(`v'), by(sa_uuid bill_start_dt total_bill_kwh)
}
unique sa_uuid bill_start_dt
local uniq = r(unique)
drop if dup>0 & abs(temp_date_min)>temp_date_min2 & dup==dup2 // where thre aren't multiple tariffs
unique sa_uuid bill_start_dt
assert r(unique)==`uniq'
foreach v of varlist max_demand peak_demand partial_peak_demand flag_multi_tariff flag_bad_tariff total_bill_amount {
	replace `v' = temp_`v' if dup>0 & dup==dup2 & abs(temp_date_min)==temp_date_min2
}

	// for tariff conflicts, take the tariff of the following bill and flag
gen temp_rt_sched_cd = ""
replace temp_rt_sched_cd = rt_sched[_n+1] if dup>0 & temp[_n+1]==1 & ///
	sa_uuid==sa_uuid[_n+1] & dup!=dup2
replace temp_rt_sched_cd = rt_sched[_n+2] if dup>0 & temp[_n+2]==1 & ///
	sa_uuid==sa_uuid[_n+2] & dup==dup[_n+1] & temp[_n+1]==0 & sa_uuid==sa_uuid[_n+1] & ///
	bill_start_dt==bill_start_dt[_n+1] & total_bill_kwh==total_bill_kwh[_n+1] & dup!=dup2
replace temp_rt_sched_cd = rt_sched[_n+3] if dup>0 & temp[_n+3]==1 & ///
	sa_uuid==sa_uuid[_n+3] & dup==dup[_n+1] & temp[_n+1]==0 & sa_uuid==sa_uuid[_n+1] & ///
	bill_start_dt==bill_start_dt[_n+1] & total_bill_kwh==total_bill_kwh[_n+1] & dup==dup[_n+2] & ///
	temp[_n+2]==0 & sa_uuid==sa_uuid[_n+2] & bill_start_dt==bill_start_dt[_n+2] & ///
	total_bill_kwh==total_bill_kwh[_n+2] & dup!=dup2
gen temp_rt_match = rt_sched_cd==temp_rt_sched_cd
egen temp_rt_match2 = max(temp_rt_match), by(sa_uuid bill_start_dt total_bill_kwh)	
unique sa_uuid bill_start_dt
local uniq = r(unique)
drop if dup>0 & abs(temp_date_min)>temp_date_min2 & dup!=dup2 // where thre aren't multiple tariffs
unique sa_uuid bill_start_dt
assert r(unique)==`uniq'
foreach v of varlist max_demand peak_demand partial_peak_demand total_bill_amount flag_bad_tariff rt_sched_cd {
	replace `v' = temp_`v' if dup>0 & dup!=dup2 & abs(temp_date_min)==temp_date_min2
}
replace flag_multi_tariff = 1 if dup>0 & abs(temp_date_min)==temp_date_min2 & dup!=dup2 // multiple tariffs

	// remaining dups don't have a following month, so take end date with bill length closest to 30.4 days (365/12=30.4)
duplicates t sa_uuid bill_start_dt total_bill_kwh, gen(dup3)
duplicates t sa_uuid bill_start_dt total_bill_kwh rt_sched_cd, gen(dup4)
gen temp_full_month = abs(bill_length-30.4)
egen temp_full_month2 = min(temp_full_month), by(sa_uuid bill_start_dt total_bill_kwh)
gen temp_to_drop = dup3>0 & temp_full_month>temp_full_month2 & dup3==dup4
unique sa_uuid bill_start_dt
local uniq = r(unique)
drop if temp_to_drop==1 // appear to be account closings
unique sa_uuid bill_start_dt
assert r(unique)==`uniq'
foreach v of varlist max_demand peak_demand partial_peak_demand flag_multi_tariff flag_bad_tariff total_bill_amount {
	replace `v' = temp_`v' if dup3>0 & dup3==dup4 & temp_full_month==temp_full_month2
}
gen temp_to_drop2 = dup3>0 & temp_full_month>temp_full_month2 & dup3!=dup4
unique sa_uuid bill_start_dt
local uniq = r(unique)
drop if temp_to_drop2 // where thre aren't multiple tariffs
unique sa_uuid bill_start_dt
assert r(unique)==`uniq'
foreach v of varlist max_demand peak_demand partial_peak_demand total_bill_amount flag_bad_tariff rt_sched_cd {
	replace `v' = temp_`v' if dup3>0 & dup3!=dup4 & temp_full_month==temp_full_month2
}
replace flag_multi_tariff = 1 if dup3>0 & temp_full_month==temp_full_month2 & dup3!=dup4 // multiple tariffs

	// confirm uniqueness
unique sa_uuid bill_start_dt total_bill_kwh
assert r(unique)==r(N)	
	
	// clean up
drop dup* temp*	
	
	
** Duplicate bills: same account, different start dates, same end date, same kWh
duplicates t sa_uuid bill_end_dt total_bill_kwh, gen(dup)	
tab dup // 0.2% of bills
duplicates t sa_uuid bill_end_dt total_bill_kwh rt_sched, gen(dup2)	
tab dup dup2 // almost all don't have a tariff conflict
sort sa_uuid bill_start_dt bill_end_dt
gen temp = sa_uuid==sa_uuid[_n+1] & dup==0 & dup[_n+1]>0 // to flag preceding bill
br if dup>0 | temp==1 

	// how close is start date to previous end date?
gen temp_date_min = .
replace temp_date_min = bill_start_dt-bill_end_dt[_n-1] if dup>0 & temp[_n-1]==1 & ///
	sa_uuid==sa_uuid[_n-1]
replace temp_date_min = bill_start_dt-bill_end_dt[_n-2] if dup>0 & temp[_n-2]==1 & ///
	sa_uuid==sa_uuid[_n-2] & dup==dup[_n-1] & temp[_n-1]==0 & sa_uuid==sa_uuid[_n-1] & ///
	bill_end_dt==bill_end_dt[_n-1] & total_bill_kwh==total_bill_kwh[_n-1]
egen temp_date_min2 = min(abs(temp_date_min)), by(sa_uuid bill_end_dt total_bill_kwh)

	// consolidate if kWh matches (averaging cost, picking start date closest to previous end date)
 foreach v of varlist max_demand peak_demand partial_peak_demand flag* {
	egen double temp_`v' = max(`v'), by(sa_uuid bill_end_dt total_bill_kwh)
}
foreach v of varlist total_bill_amount {
	egen double temp_`v' = mean(`v'), by(sa_uuid bill_end_dt total_bill_kwh)
}
unique sa_uuid bill_end_dt
local uniq = r(unique)
drop if dup>0 & abs(temp_date_min)>temp_date_min2 & dup==dup2 // where thre aren't multiple tariffs
unique sa_uuid bill_end_dt
assert r(unique)==`uniq'
foreach v of varlist max_demand peak_demand partial_peak_demand flag_multi_tariff flag_bad_tariff total_bill_amount {
	replace `v' = temp_`v' if dup>0 & dup==dup2 & abs(temp_date_min)==temp_date_min2
}

	// for tariff conflicts, take the tariff of the previous bill and flag
gen temp_rt_sched_cd = ""
replace temp_rt_sched_cd = rt_sched[_n-1] if dup>0 & temp[_n-1]==1 & ///
	sa_uuid==sa_uuid[_n-1] & dup!=dup2
replace temp_rt_sched_cd = rt_sched[_n-2] if dup>0 & temp[_n-2]==1 & ///
	sa_uuid==sa_uuid[_n-2] & dup==dup[_n-1] & temp[_n-1]==0 & sa_uuid==sa_uuid[_n-1] & ///
	bill_end_dt==bill_end_dt[_n-1] & total_bill_kwh==total_bill_kwh[_n-1] & dup!=dup2
gen temp_rt_match = rt_sched_cd==temp_rt_sched_cd
egen temp_rt_match2 = max(temp_rt_match), by(sa_uuid bill_end_dt total_bill_kwh)	
unique sa_uuid bill_end_dt
local uniq = r(unique)
drop if dup>0 & abs(temp_date_min)>temp_date_min2 & dup!=dup2 // where thre aren't multiple tariffs
unique sa_uuid bill_end_dt
assert r(unique)==`uniq'
foreach v of varlist max_demand peak_demand partial_peak_demand total_bill_amount flag_bad_tariff rt_sched_cd {
	replace `v' = temp_`v' if dup>0 & dup!=dup2 & abs(temp_date_min)==temp_date_min2
}
replace flag_multi_tariff = 1 if dup>0 & abs(temp_date_min)==temp_date_min2 & dup!=dup2 // multiple tariffs

	// remaining dups don't have a preceding month, so take end date with bill length closest to 30.4 days (365/12=30.4)
duplicates t sa_uuid bill_end_dt total_bill_kwh, gen(dup3)
duplicates t sa_uuid bill_end_dt total_bill_kwh rt_sched_cd, gen(dup4)
gen temp_full_month = abs(bill_length-30.4)
egen temp_full_month2 = min(temp_full_month), by(sa_uuid bill_end_dt total_bill_kwh)
gen temp_to_drop = dup3>0 & temp_full_month>temp_full_month2 & dup3==dup4
unique sa_uuid bill_end_dt
local uniq = r(unique)
drop if temp_to_drop==1 // appear to be account closings
unique sa_uuid bill_end_dt
assert r(unique)==`uniq'
foreach v of varlist max_demand peak_demand partial_peak_demand flag_multi_tariff flag_bad_tariff total_bill_amount {
	replace `v' = temp_`v' if dup3>0 & dup3==dup4 & temp_full_month==temp_full_month2
}
gen temp_to_drop2 = dup3>0 & temp_full_month>temp_full_month2 & dup3!=dup4
unique sa_uuid bill_end_dt
local uniq = r(unique)
drop if temp_to_drop2 // where thre aren't multiple tariffs
unique sa_uuid bill_end_dt
assert r(unique)==`uniq'
foreach v of varlist max_demand peak_demand partial_peak_demand total_bill_amount flag_bad_tariff rt_sched_cd {
	replace `v' = temp_`v' if dup3>0 & dup3!=dup4 & temp_full_month==temp_full_month2
}
replace flag_multi_tariff = 1 if dup3>0 & temp_full_month==temp_full_month2 & dup3!=dup4 // multiple tariffs

	// confirm uniqueness
unique sa_uuid bill_end_dt total_bill_kwh
assert r(unique)==r(N)	
	
	// clean up
drop dup* temp*		
	
** Duplicate bills: same account, same start date, different end dates
duplicates t sa_uuid bill_start_dt, gen(dup)	
tab dup // 0.1% of bills
duplicates t sa_uuid bill_start_dt rt_sched_cd, gen(dup2)	
sort sa_uuid bill_start_dt bill_end_dt
gen temp = sa_uuid==sa_uuid[_n-1] & dup==0 & dup[_n-1]>0 // to flag following bill
br if dup>0 | temp==1 

	// drop zero/missings if there's a nonzero/nonmissing dup
gen temp_zm = dup>0 & (total_bill_kwh==0 | total_bill_kwh==.)
egen temp_zm_min = min(temp_zm), by(sa_uuid bill_start_dt)
egen temp_zm_max = max(temp_zm), by(sa_uuid bill_start_dt)
gen temp_zm2 = sa_uuid==sa_uuid[_n-1] & dup==0 & dup[_n-1]>0 & temp_zm_min[_n-1]<temp_zm_max[_n-1] // to flag following bill
br if (dup>0 & temp_zm_min<temp_zm_max) | temp_zm2 
unique sa_uuid bill_start_dt
local uniq = r(unique)
drop if temp_zm==1 & temp_zm_min==0 // drop zero/missing where the other dup is not
unique sa_uuid bill_start_dt 
assert r(unique)==`uniq'
duplicates t sa_uuid bill_start_dt, gen(temp_zm_dup)
gen flag_maybe_adjust_end_date = temp_zm_min<temp_zm_max & temp_zm_dup==0
replace flag_multi_tariff = 1 if temp_zm_min<temp_zm_max & temp_zm_dup==0 & dup!=dup2

	// tag remaining dups
duplicates t sa_uuid bill_start_dt, gen(dup3)	
tab dup3 // 0.08% of bills
duplicates t sa_uuid bill_start_dt rt_sched_cd, gen(dup4)	
sort sa_uuid bill_start_dt bill_end_dt
gen temp2 = sa_uuid==sa_uuid[_n-1] & dup3==0 & dup3[_n-1]>0 // to flag following bill
br sa_uuid-bill_length total_bill_kwh dup3 dup4 temp2 if dup3>0 | temp2==1 

	// for dup3==1: calculate short/long ratios (SLR) and gap between end_dt and next start_dt
gen temp_SLR_days = bill_length[_n-1]/bill_length if dup3==1 & sa_uuid==sa_uuid[_n-1] & ///
	bill_start_dt==bill_start_dt[_n-1]
gen temp_SLR_kwh = total_bill_kwh[_n-1]/total_bill_kwh if dup3==1 & sa_uuid==sa_uuid[_n-1] & ///
	bill_start_dt==bill_start_dt[_n-1]
gen temp_SLR_diff = abs(temp_SLR_days - temp_SLR_kwh)
gen temp_end_gap_long = bill_start_dt[_n+1]-bill_end_dt if dup3==1 & sa_uuid==sa_uuid[_n-1] & ///
	bill_start_dt==bill_start_dt[_n-1] & sa_uuid==sa_uuid[_n+1] & temp2[_n+1]==1
gen temp_end_gap_short = bill_start_dt[_n+2]-bill_end_dt if dup3==1 & sa_uuid==sa_uuid[_n+1] & ///
	bill_start_dt==bill_start_dt[_n+1] & sa_uuid==sa_uuid[_n+2] & temp2[_n+2]==1

	// for dup3==1: if SLRs are close, and if end_gap_long is 0 or -1, drop short dup
unique sa_uuid bill_start_dt
local uniq = r(unique)
gen temp_to_drop_SLR = temp_SLR_diff[_n+1]<0.2 & temp_SLR_kwh[_n+1]<1 & /// (using a 20% threshold)
	inrange(temp_end_gap_long[_n+1],-1,0) & temp_end_gap_long==. & ///
	temp_end_gap_short[_n+1]==. & temp_end_gap_short!=. & temp_end_gap_short>=1 & ///
	temp2[_n+2]==1 & sa_uuid==sa_uuid[_n+1] & sa_uuid==sa_uuid[_n+2] & dup3==1
gen flag_dup_partial_overlap = temp_to_drop_SLR[_n-1]==1
drop if temp_to_drop_SLR==1	
unique sa_uuid bill_start_dt 
assert r(unique)==`uniq'
	
	// for dup3==1: if short dup's end date matches next non-dup's start date,
	// drop long dup and flag both short bills
unique sa_uuid bill_start_dt
local uniq = r(unique)
gen temp_to_drop_SLS = dup3==1 & temp_end_gap_long<0 & inrange(temp_end_gap_short[_n-1],0,1) & ///
	dup3[_n-1]==1 & temp2[_n+1]==1 & sa_uuid==sa_uuid[_n-1] & sa_uuid==sa_uuid[_n+1] & ///
	bill_start_dt==bill_start_dt[_n-1]
gen flag_dup_double_overlap = 0
replace flag_dup_double_overlap = 1 if temp_to_drop_SLS[_n-1]==1 & temp2==1 & ///
	sa_uuid==sa_uuid[_n-1]
replace flag_dup_double_overlap = 1 if temp_to_drop_SLS[_n+1]==1 & dup3==1 & ///
	sa_uuid==sa_uuid[_n+1] & bill_start_dt==bill_start_dt[_n+1]
drop if temp_to_drop_SLS==1
unique sa_uuid bill_start_dt 
assert r(unique)==`uniq'

	// circle back and flag rate conflicts
replace flag_multi_tariff = 1 if (flag_dup_partial_overlap | flag_dup_double_overlap) & ///
	dup3!=dup4

	// tag remaining dups
duplicates t sa_uuid bill_start_dt, gen(dup5)	
tab dup5 // 0.06% of bills
duplicates t sa_uuid bill_start_dt rt_sched_cd, gen(dup6)	
sort sa_uuid bill_start_dt bill_end_dt
gen temp3 = sa_uuid==sa_uuid[_n-1] & dup5==0 & dup5[_n-1]>0 // to flag following bill
br sa_uuid-bill_length total_bill_kwh dup5 dup6 temp3 if dup5>0 | temp3==1 
	
	// for dup5==1: calculate short/long ratios (SLR) and gap between end_dt and next start_dt
gen temp_SLR_days2 = bill_length[_n-1]/bill_length if dup5==1 & sa_uuid==sa_uuid[_n-1] & ///
	bill_start_dt==bill_start_dt[_n-1]
gen temp_SLR_kwh2 = total_bill_kwh[_n-1]/total_bill_kwh if dup5==1 & sa_uuid==sa_uuid[_n-1] & ///
	bill_start_dt==bill_start_dt[_n-1]
gen temp_SLR_diff2 = abs(temp_SLR_days2 - temp_SLR_kwh2)
	
	// for dup5==1: if SLRs are close, drop short dup (same as above, but no end_gap because no follow-on bill)
unique sa_uuid bill_start_dt
local uniq = r(unique)
gen temp_to_drop_SLR2 = temp_SLR_diff2[_n+1]<0.2 & temp_SLR_kwh2[_n+1]<1 & /// (using a 20% threshold)
	temp3[_n+2]==0 & sa_uuid==sa_uuid[_n+1] & dup5==1 	
replace flag_dup_partial_overlap = 2 if temp_to_drop_SLR2[_n-1]==1
drop if temp_to_drop_SLR2==1	
unique sa_uuid bill_start_dt 
assert r(unique)==`uniq'

	// circle back and flag rate conflicts
replace flag_multi_tariff = 1 if flag_dup_partial_overlap==2 & dup5!=dup6

	// tag remaining dups
duplicates t sa_uuid bill_start_dt, gen(dup7)	
tab dup7 // 0.02% of bills
duplicates t sa_uuid bill_start_dt rt_sched_cd, gen(dup8)	
sort sa_uuid bill_start_dt bill_end_dt
gen temp4 = sa_uuid==sa_uuid[_n-1] & dup7==0 & dup7[_n-1]>0 // to flag following bill
br sa_uuid-bill_length total_bill_kwh dup7 dup8 temp4 if dup7>0 | temp4==1 

	// for dup7==1: calculate short/long ratios (SLR) and gap between end_dt and next start_dt
gen temp_SLR_days3 = bill_length[_n-1]/bill_length if dup7==1 & sa_uuid==sa_uuid[_n-1] & ///
	bill_start_dt==bill_start_dt[_n-1]
gen temp_SLR_kwh3 = total_bill_kwh[_n-1]/total_bill_kwh if dup7==1 & sa_uuid==sa_uuid[_n-1] & ///
	bill_start_dt==bill_start_dt[_n-1]
gen temp_SLR_diff3 = abs(temp_SLR_days3 - temp_SLR_kwh3)
gen temp_end_gap_long3 = bill_start_dt[_n+1]-bill_end_dt if dup7==1 & sa_uuid==sa_uuid[_n-1] & ///
	bill_start_dt==bill_start_dt[_n-1] & sa_uuid==sa_uuid[_n+1] & temp4[_n+1]==1
gen temp_end_gap_short3 = bill_start_dt[_n+2]-bill_end_dt if dup3==1 & sa_uuid==sa_uuid[_n+1] & ///
	bill_start_dt==bill_start_dt[_n+1] & sa_uuid==sa_uuid[_n+2] & temp4[_n+2]==1
	
	// for dup7==1: if end_gap_long is 0 or -1, drop short dup
unique sa_uuid bill_start_dt
local uniq = r(unique)
gen temp_to_drop_SLR3 = temp_SLR_kwh3[_n+1]<1 & /// (not applying a threshold)
	inrange(temp_end_gap_long3[_n+1],-1,0) & temp_end_gap_long3==. & ///
	temp_end_gap_short3[_n+1]==. & temp_end_gap_short3!=. & temp_end_gap_short3>=1 & ///
	temp4[_n+2]==1 & sa_uuid==sa_uuid[_n+1] & sa_uuid==sa_uuid[_n+2] & dup7==1
replace flag_dup_partial_overlap = 3 if temp_to_drop_SLR3[_n-1]==1
drop if temp_to_drop_SLR3==1	
unique sa_uuid bill_start_dt 
assert r(unique)==`uniq'
	
	// circle back and flag rate conflicts
replace flag_multi_tariff = 1 if flag_dup_partial_overlap==3 & dup7!=dup8

	// tag remaining dups
duplicates t sa_uuid bill_start_dt, gen(dup9)	
tab dup9 // 0.01% of bills
duplicates t sa_uuid bill_start_dt rt_sched_cd, gen(dup10)	
sort sa_uuid bill_start_dt bill_end_dt
gen temp5 = sa_uuid==sa_uuid[_n-1] & dup9==0 & dup9[_n-1]>0 // to flag following bill
br sa_uuid-bill_length total_bill_kwh dup9 dup10 temp5 if dup9>0 | temp5==1 

	// for dup9==1: calculate short/long ratios (SLR)
gen temp_SLR_days4 = bill_length[_n-1]/bill_length if dup9==1 & sa_uuid==sa_uuid[_n-1] & ///
	bill_start_dt==bill_start_dt[_n-1]
gen temp_SLR_kwh4 = total_bill_kwh[_n-1]/total_bill_kwh if dup9==1 & sa_uuid==sa_uuid[_n-1] & ///
	bill_start_dt==bill_start_dt[_n-1]
gen temp_SLR_diff4 = abs(temp_SLR_days4 - temp_SLR_kwh4)
	
	// for dup9==1: drop short dup (same as above, but no end_gap because no follow-on bill)
unique sa_uuid bill_start_dt
local uniq = r(unique)
gen temp_to_drop_SLR4 = temp_SLR_kwh4[_n+1]<1 & /// (not applying a threshold)
	temp5[_n+2]==0 & sa_uuid==sa_uuid[_n+1] & dup9==1 	
replace flag_dup_partial_overlap = 4 if temp_to_drop_SLR4[_n-1]==1
drop if temp_to_drop_SLR4==1	
unique sa_uuid bill_start_dt 
assert r(unique)==`uniq'

	// circle back and flag rate conflicts
replace flag_multi_tariff = 1 if flag_dup_partial_overlap==4 & dup9!=dup10

	// tag remaining dups
duplicates t sa_uuid bill_start_dt, gen(dup11)	
tab dup11 // 0.008% of bills
duplicates t sa_uuid bill_start_dt rt_sched_cd, gen(dup12)	
sort sa_uuid bill_start_dt bill_end_dt
gen temp6 = sa_uuid==sa_uuid[_n-1] & dup11==0 & dup11[_n-1]>0 // to flag following bill
br sa_uuid-bill_length total_bill_kwh dup11 dup12 temp6 if dup11>0 | temp6==1 
	
	// for dup11==1: calculate gap between end_dt and next start_dt
gen temp_SLR_kwh5 = total_bill_kwh[_n-1]/total_bill_kwh if dup11==1 & sa_uuid==sa_uuid[_n-1] & ///
	bill_start_dt==bill_start_dt[_n-1]
gen temp_end_gap_long5 = bill_start_dt[_n+1]-bill_end_dt if dup11==1 & sa_uuid==sa_uuid[_n-1] & ///
	bill_start_dt==bill_start_dt[_n-1] & sa_uuid==sa_uuid[_n+1] & temp6[_n+1]==1
gen temp_end_gap_short5 = bill_start_dt[_n+2]-bill_end_dt if dup11==1 & sa_uuid==sa_uuid[_n+1] & ///
	bill_start_dt==bill_start_dt[_n+1] & sa_uuid==sa_uuid[_n+2] & temp6[_n+2]==1
	
	// for dup11==1: if end_gap_long is 0 or -1, drop short dup
unique sa_uuid bill_start_dt
local uniq = r(unique)
gen temp_to_drop_SLR5 =  /// (allowing short bill to have MORE kwh!!!)
	inrange(temp_end_gap_long5[_n+1],-1,0) & temp_end_gap_long5==. & ///
	temp_end_gap_short5[_n+1]==. & temp_end_gap_short5!=. & temp_end_gap_short5>=1 & ///
	temp6[_n+2]==1 & sa_uuid==sa_uuid[_n+1] & sa_uuid==sa_uuid[_n+2] & dup11==1
replace flag_dup_partial_overlap = 5 if temp_to_drop_SLR5[_n-1]==1
drop if temp_to_drop_SLR5==1	
unique sa_uuid bill_start_dt 
assert r(unique)==`uniq'
gen flag_dup_bad_kwh = flag_dup_partial_overlap==5

	// circle back and flag rate conflicts
replace flag_multi_tariff = 1 if flag_dup_partial_overlap==5 & dup11!=dup12

	// tag remaining dups
duplicates t sa_uuid bill_start_dt, gen(dup13)	
tab dup13 // 0.002% of bills
duplicates t sa_uuid bill_start_dt rt_sched_cd, gen(dup14)	
sort sa_uuid bill_start_dt bill_end_dt
gen temp7 = sa_uuid==sa_uuid[_n-1] & dup13==0 & dup13[_n-1]>0 // to flag following bill
br sa_uuid-bill_length total_bill_kwh dup13 dup14 temp7 if dup13>0 | temp7==1 

	// tag if 1 day, and drop if other dup is more than 1 day
gen temp_dup_1day = inrange(bill_length,0,1) & dup13>0
egen temp_dup_1day_max = max(temp_dup_1day), by(sa_uuid bill_start_dt)
egen temp_dup_1day_min = min(temp_dup_1day), by(sa_uuid bill_start_dt)
unique sa_uuid bill_start_dt
local uniq = r(unique)
gen temp_to_drop_1day = temp_dup_1day==1 & temp_dup_1day_min<temp_dup_1day_max
drop if temp_to_drop_1day==1	
unique sa_uuid bill_start_dt 
assert r(unique)==`uniq'

	// tag if kwh is missing, and drop if the other dup is not missing
gen temp_dup_kwhmissing = total_bill_kwh==. & dup13>0
egen temp_dup_kwhmissing_max = max(temp_dup_kwhmissing), by(sa_uuid bill_start_dt)
egen temp_dup_kwhmissing_min = min(temp_dup_kwhmissing), by(sa_uuid bill_start_dt)
unique sa_uuid bill_start_dt
local uniq = r(unique)
gen temp_to_drop_missing = temp_dup_kwhmissing==1 & temp_dup_kwhmissing_min<temp_dup_kwhmissing_max
drop if temp_to_drop_missing==1	
unique sa_uuid bill_start_dt 
assert r(unique)==`uniq'
assert total_bill_kwh!=. if dup13>0

	// circle back and flag rate conflicts
replace flag_multi_tariff = 1 if (temp_dup_kwhmissing_min<temp_dup_kwhmissing_max | ///
	temp_dup_1day_min<temp_dup_1day_max) & dup13!=dup14

	// tag remaining dups
duplicates t sa_uuid bill_start_dt, gen(dup15)	
tab dup15 // 0.001% of bills
duplicates t sa_uuid bill_start_dt rt_sched_cd, gen(dup16)	
sort sa_uuid bill_start_dt bill_end_dt
gen temp8 = sa_uuid==sa_uuid[_n-1] & dup15==0 & dup15[_n-1]>0 // to flag following bill
br sa_uuid-bill_length total_bill_kwh dup15 dup16 temp8 if dup15>0 | temp8==1 

	// keep dup that's closest to 30.4 days, flag as bad_kwh
gen temp_full_month = abs(bill_length-30.4) if dup15>0
egen temp_full_month2 = min(temp_full_month), by(sa_uuid bill_start_dt)
unique sa_uuid bill_start_dt
local uniq = r(unique)
gen temp_to_drop_30 = dup15>0 & temp_full_month>temp_full_month2
drop if temp_to_drop_30==1	
unique sa_uuid bill_start_dt 
assert r(unique)==`uniq'
replace flag_dup_bad_kwh = 1 if dup15>0

	// circle back and flag rate conflicts
replace flag_multi_tariff = 1 if dup15>0 & dup15!=dup16
	
	// confirm uniqueness, at long last
unique sa_uuid bill_start_dt 
assert r(unique)==r(N)

	// label flags
la var flag_maybe_adjust_end_date "Flag for formerly dup bills with questionable end dates"
la var flag_dup_partial_overlap "1=partial bill; 2=no next bill; 3=not proportionate; 4=2&3; 5=partial more kwh"
la var flag_dup_double_overlap "Flag for bills formerly spanned by overlapping double bill"
la var flag_dup_bad_kwh "Flag for resolved duplicates with unresolvable kWh"
	
	// clean up
drop dup* temp*	
	
** Duplicate bills: same account, different start dates, same end date
duplicates t sa_uuid bill_end_dt, gen(dup)	
tab dup // 0.02% of bills
duplicates t sa_uuid bill_end_dt rt_sched_cd, gen(dup2)	
sort sa_uuid bill_start_dt bill_end_dt
gen temp = sa_uuid==sa_uuid[_n+1] & dup==0 & dup[_n+1]>0 // to flag preceding bill
br sa_uuid-bill_length total_bill_kwh dup dup2 temp if dup>0 | temp==1 

	// drop longer dup if zero kwh, and shorter dup has same start date as previous bill's end date
gen temp_zero = total_bill_kwh==0 & dup==1
egen temp_zero_max = max(temp_zero), by(sa_uuid bill_end_dt)
egen temp_zero_min = min(temp_zero), by(sa_uuid bill_end_dt)
gen temp_date_squeeze = bill_end_dt[_n-1]==bill_start_dt[_n+1] & dup==1 & ///
	dup[_n+1]==1 & temp[_n-1]==1
unique sa_uuid bill_end_dt
local uniq = r(unique)
gen temp_to_drop_zero_squeeze = temp_zero & temp_zero_min<temp_zero_max & ///
	dup==1 & temp_date_squeeze==1
egen temp_to_drop_zero_squeeze_max = max(temp_to_drop_zero_squeeze), by(sa_uuid bill_end_dt)
drop if temp_to_drop_zero_squeeze==1	
unique sa_uuid bill_end_dt 
assert r(unique)==`uniq'

	// circle back and flag rate conflicts
replace flag_multi_tariff = 1 if dup>0 & temp_to_drop_zero_squeeze_max==1 ///
	& dup!=dup2

	// tag remaining dups
duplicates t sa_uuid bill_end_dt, gen(dup3)	
tab dup3 // 0.01% of bills
duplicates t sa_uuid bill_end_dt rt_sched_cd, gen(dup4)	
sort sa_uuid bill_start_dt bill_end_dt
gen temp2 = sa_uuid==sa_uuid[_n+1] & dup3==0 & dup3[_n+1]>0 // to flag preceding bill
br sa_uuid-bill_length total_bill_kwh dup3 dup4 temp2 if dup3>0 | temp2==1 

	// drop shorter dup if zero kwh, and longer dup has same start date as previous bill's end date
gen temp_zero2 = total_bill_kwh==0 & dup3==1
egen temp_zero2_max = max(temp_zero2), by(sa_uuid bill_end_dt)
egen temp_zero2_min = min(temp_zero2), by(sa_uuid bill_end_dt)
gen temp_date_gap = bill_end_dt[_n-2]==bill_start_dt[_n-1] & dup3==1 & ///
	dup3[_n-1]==1 & temp[_n-2]==1
unique sa_uuid bill_end_dt
local uniq = r(unique)
gen temp_to_drop_zero2_gap = temp_zero2 & temp_zero2_min<temp_zero2_max & ///
	dup3==1 & temp_date_gap==1
egen temp_to_drop_zero2_gap_max = max(temp_to_drop_zero2_gap), by(sa_uuid bill_end_dt)
drop if temp_to_drop_zero2_gap==1	
unique sa_uuid bill_end_dt 
assert r(unique)==`uniq'

	// circle back and flag rate conflicts
replace flag_multi_tariff = 1 if dup3>0 & temp_to_drop_zero2_gap_max==1 ///
	& dup3!=dup4
	
	// tag remaining dups
duplicates t sa_uuid bill_end_dt, gen(dup5)	
tab dup5 // 0.007% of bills
duplicates t sa_uuid bill_end_dt rt_sched_cd, gen(dup6)	
sort sa_uuid bill_start_dt bill_end_dt
gen temp3 = sa_uuid==sa_uuid[_n+1] & dup5==0 & dup5[_n+1]>0 // to flag preceding bill
br sa_uuid-bill_length total_bill_kwh dup5 dup6 temp3 if dup5>0 | temp3==1 
	
	// for dup5==1: calculate short/long ratios (SLR) and gap between previous end_dt and start_dt
gen temp_SLR_days = bill_length/bill_length[_n-1] if dup5==1 & sa_uuid==sa_uuid[_n-1] & ///
	bill_end_dt==bill_end_dt[_n-1]
gen temp_SLR_kwh = total_bill_kwh/total_bill_kwh[_n-1] if dup5==1 & sa_uuid==sa_uuid[_n-1] & ///
	bill_end_dt==bill_end_dt[_n-1]
gen temp_SLR_diff = abs(temp_SLR_days - temp_SLR_kwh)
gen temp_end_gap_long = bill_start_dt-bill_end_dt[_n-1] if dup5==1 & sa_uuid==sa_uuid[_n-1] & ///
	bill_end_dt==bill_end_dt[_n+1] & sa_uuid==sa_uuid[_n+1] & temp3[_n-1]==1
gen temp_end_gap_short = bill_start_dt-bill_end_dt[_n-2] if dup5==1 & sa_uuid==sa_uuid[_n-1] & ///
	bill_end_dt==bill_end_dt[_n-1] & sa_uuid==sa_uuid[_n-2] & temp3[_n-2]==1

	// for dup5==1: if SLRs are close, and if end_gap_short is 0 or -1, drop long dup
unique sa_uuid bill_end_dt
local uniq = r(unique)
gen temp_to_drop_SLR = temp_SLR_diff<0.2 & temp_SLR_kwh<1 & /// (using a 20% threshold)
	inrange(temp_end_gap_long[_n-1],-1,0) & temp_end_gap_long==. & ///
	temp_end_gap_short[_n-1]==. & temp_end_gap_short!=. & temp_end_gap_short>=1 & ///
	temp3[_n-2]==1 & sa_uuid==sa_uuid[_n-1] & sa_uuid==sa_uuid[_n-2] & dup5==1
replace flag_dup_partial_overlap = 1 if temp_to_drop_SLR[_n+1]==1
drop if temp_to_drop_SLR==1	
unique sa_uuid bill_end_dt 
assert r(unique)==`uniq'

	// circle back and flag rate conflicts
replace flag_multi_tariff = 1 if dup5>0 & flag_dup_partial_overlap==1 & dup5!=dup6 ///
	& temp3[_n-1]==1 & !(sa_uuid==sa_uuid[_n+1] & bill_end_dt==bill_end_dt[_n+1])
	
	// tag remaining dups
duplicates t sa_uuid bill_end_dt, gen(dup7)	
tab dup7 // .006% of bills
duplicates t sa_uuid bill_end_dt rt_sched_cd, gen(dup8)	
sort sa_uuid bill_start_dt bill_end_dt
gen temp4 = sa_uuid==sa_uuid[_n+1] & dup7==0 & dup7[_n+1]>0 // to flag preceding bill
br sa_uuid-bill_length total_bill_kwh dup7 dup8 temp4 if dup7>0 | temp4==1 

	// for dup7==1: calcualte gaps from previous end date, and dup closest to 30.4 days
gen temp_end_gap_long2 = bill_start_dt-bill_end_dt[_n-1] if dup7==1 & sa_uuid==sa_uuid[_n-1] & ///
	bill_end_dt==bill_end_dt[_n+1] & sa_uuid==sa_uuid[_n+1] & temp4[_n-1]==1
gen temp_end_gap_short2 = bill_start_dt-bill_end_dt[_n-2] if dup7==1 & sa_uuid==sa_uuid[_n-1] & ///
	bill_end_dt==bill_end_dt[_n-1] & sa_uuid==sa_uuid[_n-2] & temp4[_n-2]==1
gen temp_full_month = abs(bill_length-30.4) if dup7>0
egen temp_full_month2 = min(temp_full_month), by(sa_uuid bill_end_dt)

	// if dup that BETTER matches dates is also closest to 30.4 days, drop the other dup
unique sa_uuid bill_end_dt
local uniq = r(unique)
gen temp_to_drop_gap304 = 0
replace temp_to_drop_gap304 = 1 if dup7==1 & temp4[_n-1]==1 & sa_uuid==sa_uuid[_n-1] & ///
	sa_uuid==sa_uuid[_n+1] & bill_end_dt==bill_end_dt[_n+1] & temp_end_gap_long2<0 & ///
	inrange(temp_end_gap_short2[_n+1],0,1) & temp_full_month>temp_full_month2
replace temp_to_drop_gap304 = 1 if dup7==1 & temp4[_n-2]==1 & sa_uuid==sa_uuid[_n-1] & ///
	sa_uuid==sa_uuid[_n-2] & bill_end_dt==bill_end_dt[_n-1] & temp_end_gap_short2>1 & ///
	inrange(temp_end_gap_long2[_n-1],0,1) & temp_full_month>temp_full_month2
replace temp_to_drop_gap304 = 1 if dup7==1 & temp4[_n-2]==1 & sa_uuid==sa_uuid[_n-1] & ///
	sa_uuid==sa_uuid[_n-2] & bill_end_dt==bill_end_dt[_n-1] & temp_end_gap_short2>0 & ///
	inrange(temp_end_gap_long2[_n-1],0,0) & temp_full_month>temp_full_month2
egen temp_to_drop_gap304_max = max(temp_to_drop_gap304), by(sa_uuid bill_end_dt)
drop if temp_to_drop_gap304==1	
unique sa_uuid bill_end_dt 
assert r(unique)==`uniq'
replace flag_dup_double_overlap = 1 if temp_to_drop_gap304_max==1
replace flag_dup_bad_kwh = 1 if temp_to_drop_gap304_max==1	

	// circle back and flag rate conflicts
replace flag_multi_tariff = 1 if dup7>0 & temp_to_drop_gap304_max==1 & dup7!=dup8
	
	// tag remaining dups
duplicates t sa_uuid bill_end_dt, gen(dup9)	
tab dup9 // .004% of bills
duplicates t sa_uuid bill_end_dt rt_sched_cd, gen(dup10)	
sort sa_uuid bill_start_dt bill_end_dt
gen temp5 = sa_uuid==sa_uuid[_n+1] & dup9==0 & dup9[_n+1]>0 // to flag preceding bill
br sa_uuid-bill_length total_bill_kwh dup9 dup10 temp5 if dup9>0 | temp5==1 
	
	// for dup9==1: calculate gaps from previous end date
gen temp_end_gap_long3 = bill_start_dt-bill_end_dt[_n-1] if dup9==1 & sa_uuid==sa_uuid[_n-1] & ///
	bill_end_dt==bill_end_dt[_n+1] & sa_uuid==sa_uuid[_n+1] & temp5[_n-1]==1
gen temp_end_gap_short3 = bill_start_dt-bill_end_dt[_n-2] if dup9==1 & sa_uuid==sa_uuid[_n-1] & ///
	bill_end_dt==bill_end_dt[_n-1] & sa_uuid==sa_uuid[_n-2] & temp5[_n-2]==1
	
	// drop dup with the larger gap
unique sa_uuid bill_end_dt
local uniq = r(unique)
gen temp_to_drop_gap = 0
replace temp_to_drop_gap = 1 if dup9==1 & temp5[_n-1]==1 & sa_uuid==sa_uuid[_n-1] & ///
	sa_uuid==sa_uuid[_n+1] & bill_end_dt==bill_end_dt[_n+1] & temp_end_gap_long3<0 & ///
	inrange(temp_end_gap_short3[_n+1],0,1)
replace temp_to_drop_gap = 1 if dup9==1 & temp5[_n-2]==1 & sa_uuid==sa_uuid[_n-1] & ///
	sa_uuid==sa_uuid[_n-2] & bill_end_dt==bill_end_dt[_n-1] & temp_end_gap_short2>0 & ///
	inrange(temp_end_gap_long2[_n-1],0,0) 
replace temp_to_drop_gap = 1 if dup9==1 & temp5[_n-2]==1 & sa_uuid==sa_uuid[_n-1] & ///
	sa_uuid==sa_uuid[_n-2] & bill_end_dt==bill_end_dt[_n-1] & temp_end_gap_short2>1 & ///
	inrange(temp_end_gap_long2[_n-1],1,1) 
egen temp_to_drop_gap_max = max(temp_to_drop_gap), by(sa_uuid bill_end_dt)
drop if temp_to_drop_gap==1	
unique sa_uuid bill_end_dt 
assert r(unique)==`uniq'
replace flag_dup_double_overlap = 1 if temp_to_drop_gap_max==1
replace flag_dup_bad_kwh = 1 if temp_to_drop_gap_max==1	
	
	// circle back and flag rate conflicts
replace flag_multi_tariff = 1 if dup9>0 & temp_to_drop_gap_max==1 & dup9!=dup10

	// tag remaining dups
duplicates t sa_uuid bill_end_dt, gen(dup11)	
tab dup11 // .004% of bills
duplicates t sa_uuid bill_end_dt rt_sched_cd, gen(dup12)	
sort sa_uuid bill_start_dt bill_end_dt
gen temp6 = sa_uuid==sa_uuid[_n+1] & dup11==0 & dup11[_n+1]>0 // to flag preceding bill
br sa_uuid-bill_length total_bill_kwh dup11 dup12 temp6 if dup11>0 | temp6==1 

	// keep dup that's closest to 30.4 days, flag as bad_kwh
gen temp_full_month3 = abs(bill_length-30.4) if dup11>0
egen temp_full_month4 = min(temp_full_month3), by(sa_uuid bill_end_dt)
unique sa_uuid bill_end_dt
local uniq = r(unique)
gen temp_to_drop_30 = dup11>0 & temp_full_month3>temp_full_month4
drop if temp_to_drop_30==1	
unique sa_uuid bill_end_dt 
assert r(unique)==`uniq'
replace flag_dup_bad_kwh = 1 if dup11>0

	// circle back and flag rate conflicts
replace flag_multi_tariff = 1 if dup11>0 & dup11!=dup12
	
	// confirm uniqueness, at long last
unique sa_uuid bill_end_dt 
assert r(unique)==r(N)

	// clean up
drop dup* temp*	
			
** Duplicate bills: same account, overlapping bill periods
sort sa_uuid bill_start_dt bill_end_dt
gen temp_date_overlap = (bill_start_dt<bill_end_dt[_n-1] & sa_uuid==sa_uuid[_n-1]) | ///
	(bill_end_dt>bill_start_dt[_n+1] & sa_uuid==sa_uuid[_n+1])
tab temp_date_overlap // 0.04% of bills
br sa_uuid-bill_length total_bill_kwh temp_date_overlap flag_maybe_adjust_end_date ///
	if temp_date_overlap

	// drop if zero/missing is sandwiched by bills that are flush
gen temp_to_drop = (total_bill_kwh==0 | total_bill_kwh==.) & temp_date_overlap==1 & ///
	temp_date_overlap[_n-1]==1 & temp_date_overlap[_n+1]==1 & sa_uuid==sa_uuid[_n-1] & ///
	sa_uuid==sa_uuid[_n-1] & bill_end_dt[_n-1]==bill_start_dt[_n+1]
assert temp_date_overlap==1 if temp_to_drop==1
drop if temp_to_drop==1 & temp_to_drop[_n-1]==0 & temp_to_drop[_n+1]==0 
	
	// tag remaining dups
sort sa_uuid bill_start_dt bill_end_dt
gen temp_date_overlap2 = (bill_start_dt<bill_end_dt[_n-1] & sa_uuid==sa_uuid[_n-1]) | ///
	(bill_end_dt>bill_start_dt[_n+1] & sa_uuid==sa_uuid[_n+1])
tab temp_date_overlap2 // 0.02% of bills
br sa_uuid-bill_length total_bill_kwh temp_date_overlap2 flag_maybe_adjust_end_date ///
	if temp_date_overlap2

	// drop if zero/missing is sandwiched by bills that are flush (strings of overlaping dups)
gen temp_to_drop2 = (total_bill_kwh==0 | total_bill_kwh==.) & temp_date_overlap2==1 & ///
	temp_date_overlap2[_n-1]==1 & temp_date_overlap2[_n+1]==1 & sa_uuid==sa_uuid[_n-1] & ///
	sa_uuid==sa_uuid[_n-1] & bill_end_dt[_n-1]==bill_start_dt[_n+1]
assert temp_date_overlap2==1 if temp_to_drop2==1
br sa_uuid-bill_length total_bill_kwh temp_date_overlap2 temp_to_drop2 flag_maybe_adjust_end_date ///
	if temp_date_overlap2 & (temp_to_drop2==1 | temp_to_drop2[_n-1]==1 | temp_to_drop2[_n+1]==1)
	// count odd/evens
gen temp_to_drop2_odd = 0
replace temp_to_drop2_odd = 1 if temp_to_drop2==1 & (temp_to_drop2[_n-1]==0 | temp_to_drop2[_n+1]==0)
replace temp_to_drop2_odd = 1 if temp_to_drop2==1 & (temp_to_drop2[_n-1]==1 & temp_to_drop2[_n+1]==1) & ///
	(temp_to_drop2[_n-2]==1 & temp_to_drop2[_n+2]==1) & (temp_to_drop2[_n-3]==0 | temp_to_drop2[_n+3]==0) & ///
	(temp_to_drop2_odd[_n-1]==0 & temp_to_drop2_odd[_n+1]==0) & sa_uuid==sa_uuid[_n-1] & sa_uuid==sa_uuid[_n+1] & ///
	(temp_to_drop2_odd[_n-2]==1 & temp_to_drop2_odd[_n+2]==1) & sa_uuid==sa_uuid[_n-2] & sa_uuid==sa_uuid[_n+2] & ///
	(temp_to_drop2_odd[_n-3]==0 & temp_to_drop2_odd[_n+3]==0) & sa_uuid==sa_uuid[_n-3] & sa_uuid==sa_uuid[_n+3] 
	//drop odds
drop if temp_to_drop2==1 & temp_to_drop2_odd==1 & temp_to_drop2_odd[_n-1]==0 & temp_to_drop2_odd[_n+1]==0
	
	// tag remaining dups
sort sa_uuid bill_start_dt bill_end_dt
gen temp_date_overlap3 = (bill_start_dt<bill_end_dt[_n-1] & sa_uuid==sa_uuid[_n-1]) | ///
	(bill_end_dt>bill_start_dt[_n+1] & sa_uuid==sa_uuid[_n+1])
tab temp_date_overlap3 // 0.01% of bills
br sa_uuid-bill_length total_bill_kwh temp_date_overlap3 flag_maybe_adjust_end_date ///
	if temp_date_overlap3

	// drop if zero/missing is sandwiched by bills that are flush (pairs that are both zeros and same gap)
gen temp_to_drop3 = (total_bill_kwh==0 | total_bill_kwh==.) & temp_date_overlap3==1 & ///
	temp_date_overlap3[_n-1]==1 & temp_date_overlap3[_n+1]==1 & sa_uuid==sa_uuid[_n-1] & ///
	sa_uuid==sa_uuid[_n-1] & bill_end_dt[_n-1]==bill_start_dt[_n+1]
assert temp_date_overlap3==1 if temp_to_drop3==1
br sa_uuid-bill_length total_bill_kwh temp_date_overlap3 temp_to_drop3 flag_maybe_adjust_end_date ///
	if temp_date_overlap3 & (temp_to_drop3==1 | temp_to_drop3[_n-1]==1 | temp_to_drop3[_n+1]==1)
gen temp_date_gap = .
replace temp_date_gap = bill_start_dt[_n+2]-bill_end_dt[_n-1] if temp_to_drop3==1 & temp_to_drop3[_n+1]==1 & ///
	temp_to_drop[_n-1]==0 & temp_to_drop[_n+2]==0 & sa_uuid==sa_uuid[_n+1] & sa_uuid==sa_uuid[_n+2] & ///
	sa_uuid==sa_uuid[_n-1]
replace temp_date_gap = bill_start_dt[_n+1]-bill_end_dt[_n-2] if temp_to_drop3==1 & temp_to_drop3[_n-1]==1 & ///
	temp_to_drop[_n-2]==0 & temp_to_drop[_n+1]==0 & sa_uuid==sa_uuid[_n-1] & sa_uuid==sa_uuid[_n-2] & ///
	sa_uuid==sa_uuid[_n+1]
gen temp_date_gap_equal = (temp_date_gap==temp_date_gap[_n-1] | temp_date_gap==temp_date_gap[_n+1]) & ///
	temp_date_gap!=.
drop if temp_date_gap_equal==1 & temp_date_gap<=5 & temp_to_drop3==1  

	// tag remaining dups
sort sa_uuid bill_start_dt bill_end_dt
gen temp_date_overlap4 = (bill_start_dt<bill_end_dt[_n-1] & sa_uuid==sa_uuid[_n-1]) | ///
	(bill_end_dt>bill_start_dt[_n+1] & sa_uuid==sa_uuid[_n+1])
tab temp_date_overlap4 // 0.01% of bills
br sa_uuid-bill_length total_bill_kwh temp_date_overlap4 flag_maybe_adjust_end_date ///
	if temp_date_overlap4 | temp_date_overlap4[_n-1] | temp_date_overlap4[_n+1]

	// prepare to collapse dups that are both zero kWh, with (close to) flush dates to non-dups
gen temp_to_collapse = 0
replace temp_to_collapse = 1 if temp_date_overlap4==1 & temp_date_overlap4[_n-1]==0 & ///
	temp_date_overlap4[_n+1]==1 & temp_date_overlap4[_n+2]==0 & sa_uuid==sa_uuid[_n-1] & ///
	sa_uuid==sa_uuid[_n+1] & sa_uuid==sa_uuid[_n+2] & bill_start_dt-bill_end_dt[_n-1]<=3
replace temp_to_collapse = 1 if temp_date_overlap4==1 & temp_date_overlap4[_n-2]==0 & ///
	temp_date_overlap4[_n-1]==1 & temp_date_overlap4[_n+1]==0 & sa_uuid==sa_uuid[_n-2] & ///
	sa_uuid==sa_uuid[_n-1] & sa_uuid==sa_uuid[_n+1] & bill_start_dt[_n+1]-bill_end_dt<=3
gen temp_to_collapse_id = _n if temp_to_collapse==1 & temp_to_collapse[_n-1]==0 & temp_to_collapse[_n+1]==1
replace temp_to_collapse_id = _n-1 if temp_to_collapse==1 & temp_to_collapse[_n-1]==1 & temp_to_collapse[_n+1]==0
foreach v of varlist flag* max_demand peak_demand partial_peak_demand {
	egen double temp_`v' = max(`v') if temp_to_collapse_id!=., by(temp_to_collapse_id)
	replace temp_`v' = `v' if temp_to_collapse_id==temp_to_collapse_id[_n+1] & total_bill_kwh!=. & ///
		total_bill_kwh[_n+1]==. & temp_to_collapse_id!=.
	replace temp_`v' = `v' if temp_to_collapse_id==temp_to_collapse_id[_n+1] & total_bill_kwh!=0 & ///
		total_bill_kwh[_n+1]==0 & temp_to_collapse_id!=.
	replace temp_`v' = `v'[_n-1] if temp_to_collapse_id==temp_to_collapse_id[_n-1] & total_bill_kwh[_n-1]!=. & ///
		total_bill_kwh==. & temp_to_collapse_id!=.
	replace temp_`v' = `v'[_n-1] if temp_to_collapse_id==temp_to_collapse_id[_n-1] & total_bill_kwh[_n-1]!=0 & ///
		total_bill_kwh==0 & temp_to_collapse_id!=.
	replace temp_`v' = `v' if temp_to_collapse_id==temp_to_collapse_id[_n-1] & total_bill_kwh!=. & ///
		total_bill_kwh[_n-1]==. & temp_to_collapse_id!=.
	replace temp_`v' = `v' if temp_to_collapse_id==temp_to_collapse_id[_n-1] & total_bill_kwh!=0 & ///
		total_bill_kwh[_n-1]==0 & temp_to_collapse_id!=.
	replace temp_`v' = `v'[_n+1] if temp_to_collapse_id==temp_to_collapse_id[_n+1] & total_bill_kwh[_n+1]!=. & ///
		total_bill_kwh==. & temp_to_collapse_id!=.
	replace temp_`v' = `v'[_n+1] if temp_to_collapse_id==temp_to_collapse_id[_n+1] & total_bill_kwh[_n+1]!=0 & ///
		total_bill_kwh==0 & temp_to_collapse_id!=.
}
foreach v of varlist total_bill_kwh total_bill_amount {
	egen double temp_`v' = mean(`v') if temp_to_collapse_id!=., by(temp_to_collapse_id)
	replace temp_`v' = `v' if temp_to_collapse_id==temp_to_collapse_id[_n+1] & total_bill_kwh!=. & ///
		total_bill_kwh[_n+1]==. & temp_to_collapse_id!=.
	replace temp_`v' = `v' if temp_to_collapse_id==temp_to_collapse_id[_n+1] & total_bill_kwh!=0 & ///
		total_bill_kwh[_n+1]==0 & temp_to_collapse_id!=.
	replace temp_`v' = `v'[_n-1] if temp_to_collapse_id==temp_to_collapse_id[_n-1] & total_bill_kwh[_n-1]!=. & ///
		total_bill_kwh==. & temp_to_collapse_id!=.
	replace temp_`v' = `v'[_n-1] if temp_to_collapse_id==temp_to_collapse_id[_n-1] & total_bill_kwh[_n-1]!=0 & ///
		total_bill_kwh==0 & temp_to_collapse_id!=.
	replace temp_`v' = `v' if temp_to_collapse_id==temp_to_collapse_id[_n-1] & total_bill_kwh!=. & ///
		total_bill_kwh[_n-1]==. & temp_to_collapse_id!=.
	replace temp_`v' = `v' if temp_to_collapse_id==temp_to_collapse_id[_n-1] & total_bill_kwh!=0 & ///
		total_bill_kwh[_n-1]==0 & temp_to_collapse_id!=.
	replace temp_`v' = `v'[_n+1] if temp_to_collapse_id==temp_to_collapse_id[_n+1] & total_bill_kwh[_n+1]!=. & ///
		total_bill_kwh==. & temp_to_collapse_id!=.
	replace temp_`v' = `v'[_n+1] if temp_to_collapse_id==temp_to_collapse_id[_n+1] & total_bill_kwh[_n+1]!=0 & ///
		total_bill_kwh==0 & temp_to_collapse_id!=.
}
replace temp_flag_dup_bad_kwh = 1 if (temp_to_collapse_id!=. & temp_to_collapse_id==temp_to_collapse_id[_n+1] & ///
	temp_total_bill_kwh!=total_bill_kwh & temp_total_bill_kwh!=total_bill_kwh[_n+1]) | ///
	(temp_to_collapse_id!=. & temp_to_collapse_id==temp_to_collapse_id[_n-1] & ///
	temp_total_bill_kwh!=total_bill_kwh & temp_total_bill_kwh!=total_bill_kwh[_n-1])
replace bill_start_dt = bill_start_dt[_n-1] if temp_to_collapse_id!=. & ///
	temp_to_collapse_id==temp_to_collapse_id[_n-1]
replace bill_end_dt = bill_end_dt[_n+1] if temp_to_collapse_id!=. & ///
	temp_to_collapse_id==temp_to_collapse_id[_n+1]
replace bill_length = bill_end_dt-bill_start_dt+1 if temp_to_collapse_id!=.
egen temp_rt_sched_cd = mode(rt_sched_cd) if temp_to_collapse_id!=., by(temp_to_collapse_id)
replace temp_flag_multi_tariff = 1 if temp_to_collapse_id!=. & temp_to_collapse_id==temp_to_collapse_id[_n+1] & ///
	(temp_rt_sched_cd!=rt_sched_cd | temp_rt_sched_cd!=rt_sched_cd[_n+1])
replace temp_flag_multi_tariff = 1 if temp_to_collapse_id!=. & temp_to_collapse_id==temp_to_collapse_id[_n-1] & ///
	(temp_rt_sched_cd!=rt_sched_cd | temp_rt_sched_cd!=rt_sched_cd[_n-1])
replace temp_rt_sched_cd = rt_sched_cd if temp_to_collapse_id!=. & temp_to_collapse_id==temp_to_collapse_id[_n+1] & ///
	temp_rt_sched_cd=="" & rt_sched_cd==rt_sched_cd[_n-1] & sa_uuid==sa_uuid[_n-1] & ///
	rt_sched_cd==rt_sched_cd[_n+2] & sa_uuid==sa_uuid[_n+2]
replace temp_rt_sched_cd = rt_sched_cd[_n-1] if temp_to_collapse_id!=. & temp_to_collapse_id==temp_to_collapse_id[_n-1] & ///
	temp_rt_sched_cd=="" & rt_sched_cd[_n-1]==rt_sched_cd[_n-2] & sa_uuid[_n-1]==sa_uuid[_n-2] & ///
	rt_sched_cd[_n-1]==rt_sched_cd[_n+1] & sa_uuid[_n-1]==sa_uuid[_n+1]
replace temp_rt_sched_cd = rt_sched_cd if temp_to_collapse_id!=. & temp_to_collapse_id==temp_to_collapse_id[_n-1] & ///
	temp_rt_sched_cd=="" & rt_sched_cd==rt_sched_cd[_n-2] & sa_uuid==sa_uuid[_n-2] & ///
	rt_sched_cd==rt_sched_cd[_n+1] & sa_uuid==sa_uuid[_n+1]
replace temp_rt_sched_cd = rt_sched_cd[_n+1] if temp_to_collapse_id!=. & temp_to_collapse_id==temp_to_collapse_id[_n+1] & ///
	temp_rt_sched_cd=="" & rt_sched_cd[_n+1]==rt_sched_cd[_n-1] & sa_uuid[_n+1]==sa_uuid[_n-1] & ///
	rt_sched_cd[_n+1]==rt_sched_cd[_n+2] & sa_uuid[_n+1]==sa_uuid[_n+2]
replace temp_rt_sched_cd = rt_sched_cd[_n+2] if temp_to_collapse_id!=. & temp_to_collapse_id==temp_to_collapse_id[_n+1] & ///
	temp_rt_sched_cd=="" & sa_uuid==sa_uuid[_n+2] 
replace temp_rt_sched_cd = rt_sched_cd[_n+1] if temp_to_collapse_id!=. & temp_to_collapse_id==temp_to_collapse_id[_n-1] & ///
	temp_rt_sched_cd=="" & sa_uuid==sa_uuid[_n+1] 
assert rt_sched_cd!="" if temp_to_collapse_id!=.

	// collapse overlapping dups
foreach v of varlist temp_flag* temp_max_demand temp_peak_demand temp_partial_peak_demand temp_total_bill_kwh temp_total_bill_amount temp_rt_sched_cd {
	local v2 = subinstr("`v'","temp_","",1)
	replace `v2' = `v' if temp_to_collapse_id!=.
}	
duplicates drop
unique temp_to_collapse_id if temp_to_collapse_id!=.
assert r(unique)==r(N)

	// clean up
drop temp*	

	// tag remaining dups
sort sa_uuid bill_start_dt bill_end_dt
gen temp_date_overlap = (bill_start_dt<bill_end_dt[_n-1] & sa_uuid==sa_uuid[_n-1]) | ///
	(bill_end_dt>bill_start_dt[_n+1] & sa_uuid==sa_uuid[_n+1])
tab temp_date_overlap // 0.01% of bills
br sa_uuid-bill_length total_bill_kwh temp_date_overlap flag_maybe_adjust_end_date ///
	if temp_date_overlap==1 | temp_date_overlap[_n-1]==1 | temp_date_overlap[_n+1]==1
	
	// I see no consistent patterns or room for resolving these remaining dups.
	// So, I'm opting to set all kwh/$ variables as missings and collapse to a single
	// missing observation per set of overlapping dups (and flagging as such).
	
	// prepare to collapse
gen temp_to_collapse = temp_date_overlap
gen temp_to_collapse_id = _n if temp_to_collapse==1 & temp_to_collapse[_n-1]==0 & temp_to_collapse[_n+1]==1
replace temp_to_collapse_id = _n-1 if temp_to_collapse==1 & temp_to_collapse[_n-1]==1 & ///
	temp_to_collapse[_n-2]==0
replace temp_to_collapse_id = _n-2 if temp_to_collapse==1 & temp_to_collapse[_n-1]==1 & ///
	temp_to_collapse[_n-2]==1 & temp_to_collapse[_n-3]==0 
replace temp_to_collapse_id = _n-3 if temp_to_collapse==1 & temp_to_collapse[_n-1]==1 & ///
	temp_to_collapse[_n-2]==1 & temp_to_collapse[_n-3]==1 & temp_to_collapse[_n-4]==0
replace temp_to_collapse_id = _n-4 if temp_to_collapse==1 & temp_to_collapse[_n-1]==1 & ///
	temp_to_collapse[_n-2]==1 & temp_to_collapse[_n-3]==1 & temp_to_collapse[_n-4]==1 & ///
	temp_to_collapse[_n-5]==0
replace temp_to_collapse_id = _n-5 if temp_to_collapse==1 & temp_to_collapse[_n-1]==1 & ///
	temp_to_collapse[_n-2]==1 & temp_to_collapse[_n-3]==1 & temp_to_collapse[_n-4]==1 & ///
	temp_to_collapse[_n-5]==1 & temp_to_collapse[_n-6]==0
replace temp_to_collapse_id = _n-6 if temp_to_collapse==1 & temp_to_collapse[_n-1]==1 & ///
	temp_to_collapse[_n-2]==1 & temp_to_collapse[_n-3]==1 & temp_to_collapse[_n-4]==1 & ///
	temp_to_collapse[_n-5]==1 & temp_to_collapse[_n-6]==1 & temp_to_collapse[_n-7]==0
replace temp_to_collapse_id = _n-7 if temp_to_collapse==1 & temp_to_collapse[_n-1]==1 & ///
	temp_to_collapse[_n-2]==1 & temp_to_collapse[_n-3]==1 & temp_to_collapse[_n-4]==1 & ///
	temp_to_collapse[_n-5]==1 & temp_to_collapse[_n-6]==1 & temp_to_collapse[_n-7]==1 & ///
	temp_to_collapse[_n-8]==0 
replace temp_to_collapse_id = _n-8 if temp_to_collapse==1 & temp_to_collapse[_n-1]==1 & ///
	temp_to_collapse[_n-2]==1 & temp_to_collapse[_n-3]==1 & temp_to_collapse[_n-4]==1 & ///
	temp_to_collapse[_n-5]==1 & temp_to_collapse[_n-6]==1 & temp_to_collapse[_n-7]==1 & ///
	temp_to_collapse[_n-8]==1 & temp_to_collapse[_n-9]==0
replace temp_to_collapse_id = _n-9 if temp_to_collapse==1 & temp_to_collapse[_n-1]==1 & ///
	temp_to_collapse[_n-2]==1 & temp_to_collapse[_n-3]==1 & temp_to_collapse[_n-4]==1 & ///
	temp_to_collapse[_n-5]==1 & temp_to_collapse[_n-6]==1 & temp_to_collapse[_n-7]==1 & ///
	temp_to_collapse[_n-8]==1 & temp_to_collapse[_n-9]==1 & temp_to_collapse[_n-10]==0
replace temp_to_collapse_id = _n-10 if temp_to_collapse==1 & temp_to_collapse[_n-1]==1 & ///
	temp_to_collapse[_n-2]==1 & temp_to_collapse[_n-3]==1 & temp_to_collapse[_n-4]==1 & ///
	temp_to_collapse[_n-5]==1 & temp_to_collapse[_n-6]==1 & temp_to_collapse[_n-7]==1 & ///
	temp_to_collapse[_n-8]==1 & temp_to_collapse[_n-9]==1 & temp_to_collapse[_n-10]==1 & ///
	temp_to_collapse[_n-11]==0
replace temp_to_collapse_id = _n-11 if temp_to_collapse==1 & temp_to_collapse[_n-1]==1 & ///
	temp_to_collapse[_n-2]==1 & temp_to_collapse[_n-3]==1 & temp_to_collapse[_n-4]==1 & ///
	temp_to_collapse[_n-5]==1 & temp_to_collapse[_n-6]==1 & temp_to_collapse[_n-7]==1 & ///
	temp_to_collapse[_n-8]==1 & temp_to_collapse[_n-9]==1 & temp_to_collapse[_n-10]==1 & ///
	temp_to_collapse[_n-11]==1 & temp_to_collapse[_n-12]==0
replace temp_to_collapse_id = _n-12 if temp_to_collapse==1 & temp_to_collapse[_n-1]==1 & ///
	temp_to_collapse[_n-2]==1 & temp_to_collapse[_n-3]==1 & temp_to_collapse[_n-4]==1 & ///
	temp_to_collapse[_n-5]==1 & temp_to_collapse[_n-6]==1 & temp_to_collapse[_n-7]==1 & ///
	temp_to_collapse[_n-8]==1 & temp_to_collapse[_n-9]==1 & temp_to_collapse[_n-10]==1 & ///
	temp_to_collapse[_n-11]==1 & temp_to_collapse[_n-12]==1 & temp_to_collapse[_n-13]==0
replace temp_to_collapse_id = _n-13 if temp_to_collapse==1 & temp_to_collapse[_n-1]==1 & ///
	temp_to_collapse[_n-2]==1 & temp_to_collapse[_n-3]==1 & temp_to_collapse[_n-4]==1 & ///
	temp_to_collapse[_n-5]==1 & temp_to_collapse[_n-6]==1 & temp_to_collapse[_n-7]==1 & ///
	temp_to_collapse[_n-8]==1 & temp_to_collapse[_n-9]==1 & temp_to_collapse[_n-10]==1 & ///
	temp_to_collapse[_n-11]==1 & temp_to_collapse[_n-12]==1 & temp_to_collapse[_n-13]==1 & ///
	temp_to_collapse[_n-14]==0
replace temp_to_collapse_id = _n-14 if temp_to_collapse==1 & temp_to_collapse[_n-1]==1 & ///
	temp_to_collapse[_n-2]==1 & temp_to_collapse[_n-3]==1 & temp_to_collapse[_n-4]==1 & ///
	temp_to_collapse[_n-5]==1 & temp_to_collapse[_n-6]==1 & temp_to_collapse[_n-7]==1 & ///
	temp_to_collapse[_n-8]==1 & temp_to_collapse[_n-9]==1 & temp_to_collapse[_n-10]==1 & ///
	temp_to_collapse[_n-11]==1 & temp_to_collapse[_n-12]==1 & temp_to_collapse[_n-13]==1 & ///
	temp_to_collapse[_n-14]==1 & temp_to_collapse[_n-15]==0
replace temp_to_collapse_id = _n-15 if temp_to_collapse==1 & temp_to_collapse[_n-1]==1 & ///
	temp_to_collapse[_n-2]==1 & temp_to_collapse[_n-3]==1 & temp_to_collapse[_n-4]==1 & ///
	temp_to_collapse[_n-5]==1 & temp_to_collapse[_n-6]==1 & temp_to_collapse[_n-7]==1 & ///
	temp_to_collapse[_n-8]==1 & temp_to_collapse[_n-9]==1 & temp_to_collapse[_n-10]==1 & ///
	temp_to_collapse[_n-11]==1 & temp_to_collapse[_n-12]==1 & temp_to_collapse[_n-13]==1 & ///
	temp_to_collapse[_n-14]==1 & temp_to_collapse[_n-15]==1 & temp_to_collapse[_n-16]==0
replace temp_to_collapse_id = _n-16 if temp_to_collapse==1 & temp_to_collapse[_n-1]==1 & ///
	temp_to_collapse[_n-2]==1 & temp_to_collapse[_n-3]==1 & temp_to_collapse[_n-4]==1 & ///
	temp_to_collapse[_n-5]==1 & temp_to_collapse[_n-6]==1 & temp_to_collapse[_n-7]==1 & ///
	temp_to_collapse[_n-8]==1 & temp_to_collapse[_n-9]==1 & temp_to_collapse[_n-10]==1 & ///
	temp_to_collapse[_n-11]==1 & temp_to_collapse[_n-12]==1 & temp_to_collapse[_n-13]==1 & ///
	temp_to_collapse[_n-14]==1 & temp_to_collapse[_n-15]==1 & temp_to_collapse[_n-16]==1 & ///
	temp_to_collapse[_n-17]==0
replace temp_to_collapse_id = _n-17 if temp_to_collapse==1 & temp_to_collapse[_n-1]==1 & ///
	temp_to_collapse[_n-2]==1 & temp_to_collapse[_n-3]==1 & temp_to_collapse[_n-4]==1 & ///
	temp_to_collapse[_n-5]==1 & temp_to_collapse[_n-6]==1 & temp_to_collapse[_n-7]==1 & ///
	temp_to_collapse[_n-8]==1 & temp_to_collapse[_n-9]==1 & temp_to_collapse[_n-10]==1 & ///
	temp_to_collapse[_n-11]==1 & temp_to_collapse[_n-12]==1 & temp_to_collapse[_n-13]==1 & ///
	temp_to_collapse[_n-14]==1 & temp_to_collapse[_n-15]==1 & temp_to_collapse[_n-16]==1 & ///
	temp_to_collapse[_n-17]==1 & temp_to_collapse[_n-18]==0
replace temp_to_collapse_id = _n-18 if temp_to_collapse==1 & temp_to_collapse[_n-1]==1 & ///
	temp_to_collapse[_n-2]==1 & temp_to_collapse[_n-3]==1 & temp_to_collapse[_n-4]==1 & ///
	temp_to_collapse[_n-5]==1 & temp_to_collapse[_n-6]==1 & temp_to_collapse[_n-7]==1 & ///
	temp_to_collapse[_n-8]==1 & temp_to_collapse[_n-9]==1 & temp_to_collapse[_n-10]==1 & ///
	temp_to_collapse[_n-11]==1 & temp_to_collapse[_n-12]==1 & temp_to_collapse[_n-13]==1 & ///
	temp_to_collapse[_n-14]==1 & temp_to_collapse[_n-15]==1 & temp_to_collapse[_n-16]==1 & ///
	temp_to_collapse[_n-17]==1 & temp_to_collapse[_n-18]==1 & temp_to_collapse[_n-19]==0
replace temp_to_collapse_id = _n-19 if temp_to_collapse==1 & temp_to_collapse[_n-1]==1 & ///
	temp_to_collapse[_n-2]==1 & temp_to_collapse[_n-3]==1 & temp_to_collapse[_n-4]==1 & ///
	temp_to_collapse[_n-5]==1 & temp_to_collapse[_n-6]==1 & temp_to_collapse[_n-7]==1 & ///
	temp_to_collapse[_n-8]==1 & temp_to_collapse[_n-9]==1 & temp_to_collapse[_n-10]==1 & ///
	temp_to_collapse[_n-11]==1 & temp_to_collapse[_n-12]==1 & temp_to_collapse[_n-13]==1 & ///
	temp_to_collapse[_n-14]==1 & temp_to_collapse[_n-15]==1 & temp_to_collapse[_n-16]==1 & ///
	temp_to_collapse[_n-17]==1 & temp_to_collapse[_n-18]==1 & temp_to_collapse[_n-19]==1 & ///
	temp_to_collapse[_n-20]==0
assert temp_to_collapse_id!=. if temp_to_collapse==1	
foreach v of varlist total_bill_kwh max_demand peak_demand partial_peak_demand total_bill_amount {
	replace `v' = . if temp_to_collapse_id!=.
}
foreach v of varlist flag* {
	egen temp = max(`v') if temp_to_collapse_id!=., by(temp_to_collapse_id)
	replace `v' = temp if temp_to_collapse_id!=.
	drop temp
}
egen temp_rt_sched_cd = mode(rt_sched_cd) if temp_to_collapse_id!=., by(temp_to_collapse_id)
gen temp_rt_sched_cd_check = rt_sched_cd==temp_rt_sched_cd if temp_to_collapse_id!=.
egen temp_rt_sched_cd_check2 = min(temp_rt_sched_cd_check) if temp_to_collapse_id!=., by(temp_to_collapse_id)
replace flag_multi_tariff = 1 if temp_to_collapse_id!=. & (temp_rt_sched_cd_check2==0 | temp_rt_sched_cd=="")
replace flag_bad_tariff = 1 if temp_to_collapse_id!=. & temp_rt_sched_cd==""
replace temp_rt_sched_cd = rt_sched_cd[_n-1] if temp_rt_sched_cd=="" & temp_to_collapse_id!=. & ///
	temp_to_collapse_id[_n-1]==.
replace temp_rt_sched_cd = temp_rt_sched_cd[_n-1] if temp_rt_sched_cd=="" & ///
	temp_to_collapse_id[_n-1]==temp_to_collapse_id
assert temp_rt_sched_cd!="" if temp_to_collapse_id!=.
replace rt_sched_cd = temp_rt_sched_cd if temp_to_collapse_id!=.	
drop temp_rt_sched_cd_check*
	
	// collapse overlapping dups
egen temp_start_min = min(bill_start_dt) if temp_to_collapse_id!=., by(temp_to_collapse_id)
egen temp_end_max = min(bill_end_dt) if temp_to_collapse_id!=., by(temp_to_collapse_id)
assert bill_end_dt<=temp_start_min[_n+1] if temp_to_collapse_id==. & temp_to_collapse_id[_n+1]!=. & ///
	sa_uuid==sa_uuid[_n+1]
assert bill_start_dt>=temp_end_max[_n-1] if temp_to_collapse_id==. & temp_to_collapse_id[_n-1]!=. & ///
	sa_uuid==sa_uuid[_n-1]
replace bill_start_dt = temp_start_min if temp_to_collapse_id!=.
replace bill_end_dt = temp_end_max if temp_to_collapse_id!=.
replace bill_length = bill_end_dt-bill_start_dt+1 if temp_to_collapse_id!=.
duplicates drop
unique temp_to_collapse_id if temp_to_collapse_id!=.
assert r(unique)==r(N)

	// flag overlapping bills that I just collapsed and replaced with missings
gen flag_dup_overlap_missing = temp_to_collapse_id!=.
la var flag_dup_overlap_missing "Flag for unresolvable overlapping bills; consolidated and set to all missing"
replace flag_dup_bad_kwh = 1 if flag_dup_overlap_missing==1

	// clean up
drop temp*
drop flag_maybe_adjust_end_date // turns out this was not useful	

** Confirm all bills are unique and non-overlapping
sort sa_uuid bill_start_dt bill_end_dt
assert bill_start_dt>=bill_end_dt[_n-1] if sa_uuid==sa_uuid[_n-1]
assert bill_end_dt<=bill_start_dt[_n+1] if sa_uuid==sa_uuid[_n+1]

** Flag first and last bill for each SA (if account begins after Jan 2008 or ends before Aug 2017)
gen month_st = month(bill_start_dt)
gen year_st = year(bill_start_dt)
gen modate_st = ym(year_st,month_st)
gen month_end = month(bill_end_dt)
gen year_end = year(bill_end_dt)
gen modate_end = ym(year_end,month_end)
format %tm modate*

	// flag first bill
tab modate_st if year_st<=2008
egen temp_first_start = min(bill_start_dt), by(sa_uuid)
format %td temp_first_start
tab temp_first_start if year_st<=2008 & temp_first_start==bill_start_dt
gen flag_first_bill = temp_first_start==bill_start_dt & bill_start_dt>date("01jan2008","DMY")
tab flag_first_bill if temp_first_start==bill_start_dt
preserve 
unique sa_uuid
local uniq = r(unique)
keep if temp_first_start==bill_start_dt
unique sa_uuid
assert r(unique)==`uniq'
collapse (count) count=bill_length, by(modate_st flag_first_bill)
twoway ///
	(scatter count modate_st if flag_first_bill==0, color(blue)) ///
	(scatter count modate_st if flag_first_bill==1, color(red))
restore
	
	// flag last bill
tab modate_end if year_end>=2017
egen temp_last_end = max(bill_end_dt), by(sa_uuid)
format %td temp_last_end
tab temp_last_end if year_end>=2017 & temp_last_end==bill_end_dt
gen flag_last_bill = temp_last_end==bill_end_dt & bill_end_dt<date("30aug2017","DMY")
tab flag_last_bill if temp_last_end==bill_end_dt
preserve 
unique sa_uuid
local uniq = r(unique)
keep if temp_last_end==bill_end_dt
unique sa_uuid
assert r(unique)==`uniq'
collapse (count) count=bill_length, by(modate_end flag_last_bill)
twoway ///
	(scatter count modate_end if flag_last_bill==0, color(blue)) ///
	(scatter count modate_end if flag_last_bill==1, color(red))
restore
	
	// plot first and last bill dates
preserve 
keep if temp_first_start==bill_start_dt | temp_last_end==bill_end_dt
egen starter = max(flag_first_bill), by(sa_uuid)
egen ender = max(flag_last_bill), by(sa_uuid)
unique sa_uuid
unique sa_uuid if starter // 64.7% of SAs start during our sample period
unique sa_uuid if ender // 65.2% of SAs end during our sample period
unique sa_uuid if starter & ender // 31.3% of SAs start AND end during our sample period
gen temp_start_month = modate_st if temp_first_start==bill_start_dt
gen temp_end_month = modate_end if temp_last_end==bill_end_dt
egen start_month = mean(temp_start_month), by(sa_uuid)
egen end_month = mean(temp_end_month), by(sa_uuid)
format %tm start_month end_month
keep sa_uuid start_month end_month starter ender
duplicates drop
unique sa_uuid
assert r(unique)==r(N)
gen group = 1 if starter==1 & ender==1
replace group = 2 if starter==1 & ender==0
replace group = 3 if starter==0 & ender==1
replace group = 4 if starter==0 & ender==0
gen labgroup = ""
replace labgroup = "Openers and closers" if group==1
replace labgroup = "Openers only" if group==2
replace labgroup = "Closers only" if group==3
replace labgroup = "Neither" if group==4
tab group
tab labgroup
gen count = _n
collapse (count) count, by(starter ender start_month end_month)
twoway ///
	(scatter end_month start_month if starter==0 & ender==0 [fw=count], msize(vsmall) mfcolor(none) mlcolor(green) mlwidth(vthin)) ///
	(scatter end_month start_month if starter==1 & ender==0 [fw=count], msize(vsmall) mfcolor(none) mlcolor(blue) mlwidth(vthin)) ///
	(scatter end_month start_month if starter==0 & ender==1 [fw=count], msize(vsmall) mfcolor(none) mlcolor(red) mlwidth(vthin)) ///
	(scatter end_month start_month if starter==1 & ender==1 [fw=count], msize(vsmall) mfcolor(none) mlcolor(black) mlwidth(vthin)) ///
	, ///
	xtitle("Start date of first bill", size(vsmall)) xscale(r(564,695)) ///
	xlabel(576 600 624 648 672 696, labsize(vsmall)) ///
	ytitle("End date of last bill", size(vsmall)) ylabel(, nogrid labsize(vsmall)) ///
	graphr(color(white) lc(white)) plotregion(margin(l=0 r=0 t = 0)) ///
	title("PGE Ag Customer Billing Accounts", size(small) color(black)) ///
	legend(order(4 "Openers and closers" 2 "Openers only" 3 "Closers only" 1 "Neither") size(small) c(4) )

restore
	// only 1.5% of accounts are neither openers nor closers!
	// 33.3% of accounts are openers only
	// 33.9% of accounts are closers only
	// for 31% of accoutns, we observe the opening and closing!
	
	// label flags
la var flag_first_bill "Flag indicating SA's first bill, when first bill is after start of our sample"	
la var flag_last_bill "Flag indicating SA's last bill, when last bill is before the end of our sample"	

	// clean up
drop month* year* modate* temp*

** Deal with bill length
tab bill_length 
tab bill_length if total_bill_kwh!=.
tab bill_length if total_bill_kwh!=. & total_bill_kwh!=0
tab bill_length if total_bill_kwh!=. & total_bill_kwh!=0 & total_bill_kwh>0	
assert bill_length!=.

gen flag_long_bill = bill_length>34
gen flag_short_bill = bill_length<28
la var flag_long_bill "Flag for bills longer than 34 days"
la var flag_short_bill "Flag for bills shorter than 28 days"
tab flag_long_bill flag_short_bill

count if flag_long_bill==0 
di 1 - r(N)/_N // 0.5% of bills are longer than 34 days
count if flag_long_bill==0 & flag_short_bill==0
di 1 - r(N)/_N // 3.0% of bills are shorter than 28 days

count if total_bill_kwh!=. & total_bill_kwh!=0 & total_bill_kwh>0	
local N = r(N)
count if flag_long_bill==0 & total_bill_kwh!=. & total_bill_kwh!=0 & total_bill_kwh>0
di 1 - r(N)/`N' // 0.5% of positive bills are longer than 34 days
count if flag_long_bill==0 & flag_short_bill==0 & total_bill_kwh!=. & total_bill_kwh!=0 & total_bill_kwh>0
di 1 - r(N)/`N' // 2.7% of positive bills are shorter than 28 days

tab flag_long_bill 
tab flag_long_bill if total_bill_kwh==. | total_bill_kwh==0
tab flag_short_bill 
tab flag_short_bill if total_bill_kwh==. | total_bill_kwh==0

	// Compress and save
compress
save "$dirpath_data/pge_cleaned/billing_data.dta", replace	

	


** Potentially questionable choices
// Dups where everything's identical except $ amount: collapse to avg $ amount
// Dups where everything's identical except $ amount and kWh, and 1 dup is 0 kWh: collapse and sum $ amount 
// Dups where everything's identical except $ amount and kWh, and there's not a zero: collapse and sum kWh and $ amount
// Dups where billed kWh is the same, but tarriffs and $ amound differ: assign tariff of next non-duplicate month, take average $ amount 
// Dups where kWh, tarriffs, and $ amound differ, but 1 dup has 0 kWh: drop the zero kWh observation
// Dups where kWh, tarriffs, and $ amound differ, with no zeros: aggregate the dups and assign the modal tariff
// Dups where end date differs but kWh matches: take end date closest to next start date (or closest to 30.4 days), and average $ amount
// Dups where end date differs, and one kWh is zero/missing: keep the non-missing dup, flag to potentially adjust the end date later
// Dups where end date differs, and shorter dup is fraction of longer dup: keep the longer dup, flag (esp if longer dup's end date doesn't match next bill's start date)
// Dups where end date differs, and longer dup spans shorter dup and following bill: drop the longer dup, flag 
// Dups where end date differs, still unresolved: keep bill closest to 30.4 days, and flag as bad_kwh
// Dups where start date differs, shorter (longer) dup's date matches, longer (shorter) dup is zero/missing: drop longer (shorter) dup
// Dups where start date differs, and shorter dup is fraction of longer dup: keep the longer dup if longer dup's start date match previous bill's end date, flag 
// Dups where start date differs, still unresolved; keep bill with smallest date gap and (then or) closet to 30.4 days; flag as bad_kwh
// Dups where bills overlap: drop zero/missing kwh overlapping bills if sandwiched by bills that are flush
// Dups where bills overlap, still unresolved: collapse to a single observation, set everything to missing, flag as such

** Pending
// Monthify bills!
// Cross-check billing data with interval data
