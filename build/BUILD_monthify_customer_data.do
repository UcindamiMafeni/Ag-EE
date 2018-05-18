clear all
version 13
set more off

**************************************************************************
**** Script to build monthified version of (cleaned) PGE billing data ****
**************************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

** Load cleaned PGE billing data
use "$dirpath_data/pge_cleaned/billing_data.dta", clear

** Drop variables I won't be using
drop max_demand peak_demand partial_peak_demand

** Expand whole dataset by bill length variable
expand bill_length, gen(temp_new)
sort sa_uuid bill_start_dt temp_new
tab temp_new

** Construct date variable (duplicated at each bill change-over)
gen date = bill_start_dt if temp_new==0
format %td date
replace date = date[_n-1]+1 if temp_new==1
assert date==bill_start_dt if temp_new==0
assert date==bill_end_dt if temp_new[_n+1]==0
assert date!=.
unique sa_uuid bill_start_dt date
assert r(unique)==r(N)

** Flag duplicate account-dates (bill changeover dates where end=start)
gen temp_wt = 1
replace temp_wt = 0.5 if date==date[_n+1] & date==bill_end_dt & ///
	bill_end_dt==bill_start_dt[_n+1] & temp_new==1 & temp_new[_n+1]==0 & ///
	sa_uuid==sa_uuid[_n+1]
replace temp_wt = 0.5 if date==date[_n-1] & date==bill_end_dt[_n-1] & ///
	bill_end_dt[_n-1]==bill_start_dt & temp_new[_n-1]==1 & temp_new==0 & ///
	sa_uuid==sa_uuid[_n-1]
	// this assigns 50% weight to days that are shared by two bills (i.e. the
	// end_date of the previous bill and the start_date of the current bill)
	
** Distribute kwh and $ evenly across all days within each bill
egen double temp_denom = sum(temp_wt), by(sa_uuid bill_start_dt bill_end_dt)
foreach v of varlist total_bill_kwh total_bill_amount {
	replace `v' = `v'*temp_wt/temp_denom
}
	
** Harmonize variables where temp_wt==0.5
foreach v of varlist *flag* {
	egen double temp = max(`v') if temp_wt==0.5, by(sa_uuid date)
	replace `v' =  temp if temp_wt==0.5
	drop temp
}
replace rt_sched_cd = rt_sched_cd[_n+1] if temp_wt==0.5 & temp_wt[_n+1]==0.5 & ///
	sa_uuid==sa_uuid[_n+1] & temp_new==1 & temp_new[_n+1]==0 
foreach v of varlist total_bill_kwh total_bill_amount {
	egen double temp = sum(`v') if temp_wt==0.5, by(sa_uuid date)
	replace `v' = temp if temp_wt==0.5
	drop temp
}
	
** Drop variables that are no longer necessary and not unique
drop bill_start_dt bill_end_dt bill_length temp*
order sa_uuid date	
	
** Duplicates drop; confirm unique
duplicates drop
unique sa_uuid date
assert r(unique)==r(N)

** Create new time variables
gen modate = ym(year(date),month(date))
format %tm modate	
egen days = count(date), by(sa_uuid modate)
la var modate "Month-Year"
la var days "Number of days in month covered by a bill"
order sa_uuid modate days	
	
** Prepare to collapse to monthly level
foreach v of varlist flag* {
	egen double temp = max(`v'), by(sa_uuid modate)
	replace `v' =  temp
	drop temp
}
foreach v of varlist total_bill_kwh total_bill_amount {
	egen double temp = sum(`v'), by(sa_uuid modate)
	replace `v' = temp
	drop temp
}
egen temp1 = mode(rt_sched_cd), by(sa_uuid modate)
gen temp2 = rt_sched_cd==temp1 & temp1!=""
egen temp3 = min(temp2), by(sa_uuid modate)
egen temp4 = max(temp2), by(sa_uuid modate)
replace flag_multi_tariff = 1 if temp3<temp4 // some disagreement on tariff within month
replace flag_multi_tariff = 1 if temp4==0 // missing --> two modes = some disagreement 
replace rt_sched_cd = temp1 if temp4==1
egen temp5 = max(date), by(sa_uuid modate)
gen temp6 = rt_sched_cd if date==temp5 & rt_sched_cd!=""
egen temp7 = mode(temp6), by(sa_uuid modate)
replace rt_sched_cd = temp7 if temp4==0 & temp7!="" // if two modes --> assign tariff from end of month
gen temp8 = ""
replace temp8 = rt_sched_cd[_n-1] if rt_sched_cd=="" & rt_sched_cd[_n-1]!="" & ///
	sa_uuid==sa_uuid[_n-1] & temp8==""
replace temp8 = rt_sched_cd[_n+1] if rt_sched_cd=="" & rt_sched_cd[_n+1]!="" & ///
	sa_uuid==sa_uuid[_n+1] & temp8==""
egen temp9 = mode(temp8), by(sa_uuid modate)
replace flag_bad_tariff = 1 if rt_sched_cd==""
replace rt_sched_cd = temp9 if temp9!="" & rt_sched_cd==""
assert rt_sched_cd!=""
drop temp*

** Collapse to SA-month level
drop date
duplicates drop
unique sa_uuid modate
assert r(unique)==r(N)

** Compress and save
sort sa_uuid modate
compress
save "$dirpath_data/pge_cleaned/billing_data_monthified.dta", replace
