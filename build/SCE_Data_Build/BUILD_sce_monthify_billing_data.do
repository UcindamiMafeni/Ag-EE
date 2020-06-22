clear all
version 13
set more off

**************************************************************************
**** Script to build monthified version of (cleaned) SCE billing data ****
**************************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

*******************************************************************************
*******************************************************************************

** 1. SCE 2019 data pull

** Load cleaned SCE billing data
use "$dirpath_data/sce_cleaned/billing_data_20190916.dta", clear

** Drop variables I won't be using
drop monthly_max_kw

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
foreach v of varlist flag* interval_bill_corr {
	egen double temp = max(`v') if temp_wt==0.5, by(sa_uuid date)
	replace `v' =  temp if temp_wt==0.5
	drop temp
}
foreach v of varlist tariff_sched_text {
	replace `v' = `v'[_n+1] if temp_wt==0.5 & temp_wt[_n+1]==0.5 & ///
		sa_uuid==sa_uuid[_n+1] & temp_new==1 & temp_new[_n+1]==0 
}
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
gen day = day(date)
egen day_first = min(day), by(sa_uuid modate)
egen day_last = max(day), by(sa_uuid modate)
drop day
la var modate "Month-Year"
la var days "Number of days in month covered by a bill"
la var day_first "First day of month covered by a bill"
la var day_last "Last day of month covered by a bill"
order sa_uuid modate days day_first day_last
	
** Prepare to collapse to monthly level
foreach v of varlist flag* interval_bill_corr {
	egen double temp = max(`v'), by(sa_uuid modate)
	replace `v' =  temp
	drop temp
}
foreach v of varlist total_bill_kwh total_bill_amount {
	egen double temp = sum(`v'), by(sa_uuid modate)
	replace `v' = temp
	drop temp
}
egen temp1 = mode(tariff_sched_text), by(sa_uuid modate)
gen temp2 = tariff_sched_text==temp1 & temp1!=""
egen temp3 = min(temp2), by(sa_uuid modate)
egen temp4 = max(temp2), by(sa_uuid modate)
gen flag_multi_tariff = temp3<temp4 // some disagreement on tariff within month
replace flag_multi_tariff = 1 if temp4==0 // missing --> two modes = some disagreement 
replace tariff_sched_text = temp1 if temp4==1
egen temp5 = max(date), by(sa_uuid modate)
gen temp6 = tariff_sched_text if date==temp5 & tariff_sched_text!=""
egen temp7 = mode(temp6), by(sa_uuid modate)
replace tariff_sched_text = temp7 if temp4==0 & temp7!="" // if two modes --> assign tariff from end of month
gen temp8 = ""
replace temp8 = tariff_sched_text[_n-1] if tariff_sched_text=="" & tariff_sched_text[_n-1]!="" & ///
	sa_uuid==sa_uuid[_n-1] & temp8==""
replace temp8 = tariff_sched_text[_n+1] if tariff_sched_text=="" & tariff_sched_text[_n+1]!="" & ///
	sa_uuid==sa_uuid[_n+1] & temp8==""
egen temp9 = mode(temp8), by(sa_uuid modate)
gen flag_bad_tariff = tariff_sched_text==""
replace tariff_sched_text = temp9 if temp9!="" & tariff_sched_text==""
drop temp*

** Collapse to SA-month level
drop date
duplicates drop
unique sa_uuid modate
assert r(unique)==r(N)

** Rename and relabel
rename total_bill_kwh mnth_bill_kwh
rename total_bill_amount mnth_bill_amount
la var mnth_bill_kwh "Total billed kWh, monthified"
la var mnth_bill_amount "Total billed charges ($), monthified"
la var flag_multi_tariff "Flag for SA-months with multiple tariffs"
assert flag_bad_tariff==0
drop flag_bad_tariff

** Confirm that total monthified kWh and $ add up to the same as in billing data
preserve
collapse (sum) mnth_bill_kwh mnth_bill_amount, by(sa_uuid) fast
tempfile monthified
save `monthified' 
use "$dirpath_data/sce_cleaned/billing_data_20190916.dta", clear
collapse (sum) total_bill_kwh total_bill_amount, by(sa_uuid) fast
merge 1:1 sa_uuid using `monthified'
assert _merge==3
gen pct_diff_kwh = (mnth_bill_kwh-total_bill_kwh)/total_bill_kwh 
gen pct_diff_amount = (mnth_bill_amount-total_bill_amount)/total_bill_amount 
sum pct_diff_kwh pct_diff_amount, detail
restore

** Drop months prior to Dec 2007
drop if modate<ym(2007,12)

** Compress and save
sort sa_uuid modate
compress
save "$dirpath_data/sce_cleaned/billing_data_monthified_20190916.dta", replace

