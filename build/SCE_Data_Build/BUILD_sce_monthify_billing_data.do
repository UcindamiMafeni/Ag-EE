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

** 1. Monthify SCE 2019 and 2020 data pulls
{
** Load cleaned SCE billing data
use "$dirpath_data/sce_cleaned/billing_data.dta", clear

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
sort sa_uuid date
gen temp_wt = 1
replace temp_wt = 0.5 if date==date[_n+1] & sa_uuid==sa_uuid[_n+1]
replace temp_wt = 0.5 if date==date[_n-1] & sa_uuid==sa_uuid[_n-1]
	// this assigns 50% weight to days that are shared by two bills (i.e. the
	// end_date of the previous bill and the start_date of the current bill)
	// Only matters for the 4 SAs with bills that overlap at the 2019/2020 EDRP cusp
	
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
drop bill_start_dt bill_end_dt bill_length temp* pull
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
use "$dirpath_data/sce_cleaned/billing_data.dta", clear
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
save "$dirpath_data/sce_cleaned/billing_data_monthified.dta", replace
}

*******************************************************************************
*******************************************************************************

** 2. Fill gaps using monthly interval data
{
	// Start with monthified billing data
use "$dirpath_data/sce_cleaned/billing_data_monthified.dta", clear
gen month = real(subinstr(substr(string(modate,"%tm"),5,3),"m","",1))
gen year = real(substr(string(modate,"%tm"),1,4))
egen days_max = max(days), by(modate)
tab month days_max
sort sa_uuid modate 
gen flag_end_of_gap = sa_uuid==sa_uuid[_n-1] & flag_disct_bill[_n-1]==1
gen gap_length_months = modate[_n+1] - modate if flag_disct_bill==1

	// Merge in interval data (collapsed to the monthly level)
merge 1:1 sa_uuid modate using "$dirpath_data/sce_cleaned/interval_data_monthly_20190916.dta", gen(_merge2019)
rename kwh kwh2019
rename ndays_interval ndays_interval2019
merge 1:1 sa_uuid modate using "$dirpath_data/sce_cleaned/interval_data_monthly_20200722.dta", gen(_merge2020)
rename kwh kwh2020
rename ndays_interval ndays_interval2020
gen max_merge = _merge2019==3 | _merge2020==3
tab modate max_merge

	// Combine 2019 and 2020 interval data into a single variable
count if _merge2019==3 & _merge2020==3
local rN = r(N)
count if _merge2019==3 & _merge2020==3 & abs(kwh2019-kwh2020)<0.00001
di r(N)/`rN'
tab modate if _merge2019==3 & _merge2020==3 & abs(kwh2019-kwh2020)>0.00001
tab modate if _merge2019==3 & _merge2020==3 // only 1 month of overlap
replace kwh2019 = kwh2020 if kwh2020!=.
replace ndays_interval2019 = ndays_interval2020 if ndays_interval2020!=.
drop kwh2020 ndays_interval2020
rename kwh2019 kwh_interval
rename ndays_interval2019 ndays_interval
assert kwh_interval!=. if max_merge==1
assert kwh_interval!=. if _merge2019==2 | _merge2020==2
drop _merge2019 _merge2020 max_merge

	// Use interval data to fill in missings in billing data
egen temp = max(flag_acct), by(sa_uuid)
replace flag_acct = temp
drop temp	
br sa_uuid modate days_max days day_first day_last mnth_bill_kwh flag_disct_bill flag_acct ///
	flag_interval_merge flag_interval_disp20 interval_bill_corr kwh_interval ndays_interval ///
	if flag_acct==1 & year>=2016 & (days<days_max | days==.)
correlate mnth_bill_kwh kwh_interval
correlate mnth_bill_kwh kwh_interval if interval_bill_corr!=.
correlate mnth_bill_kwh kwh_interval if interval_bill_corr!=. & days>=28
correlate mnth_bill_kwh kwh_interval if interval_bill_corr!=. & days>=28 & day_first==1
egen interval_bill_corr_filled = mode(interval_bill_corr), by(sa_uuid)
assert interval_bill_corr==interval_bill_corr_filled | interval_bill_corr==.

gen mnth_bill_kwh_filled = mnth_bill_kwh	

	//  replace where bill covers part of the month, right before the billing gap
sort sa_uuid modate
replace mnth_bill_kwh_filled = kwh_interval if kwh_interval!=. & days<days_max & ///
	ndays_interval==days_max & interval_bill_corr_filled>0.9 & interval_bill_corr_filled!=. & ///
	flag_disct_bill==1
	
	//  replace months during gap
egen temp = mode(days_max), by(modate)
assert temp==days_max | days_max==.
replace days_max = temp
drop temp	
forvalues i = 1/35 {
	replace mnth_bill_kwh_filled = kwh_interval if mnth_bill_kwh_filled==. & ///
		kwh_interval!=. & days==. & ndays_interval==days_max & ///
		interval_bill_corr_filled>0.9 & interval_bill_corr_filled!=. & ///
		flag_disct_bill[_n-`i']==1 & sa_uuid==sa_uuid[_n-`i'] & `i'<gap_length_months[_n-`i']
}

	//  replace where observed bills resumes, after the billing gap
replace mnth_bill_kwh_filled = kwh_interval if kwh_interval!=. & days<days_max & ///
	ndays_interval==days_max & interval_bill_corr_filled>0.9 & interval_bill_corr_filled!=. & ///
	flag_end_of_gap==1

	// replace cases where bills stop, but AMI continues
egen temp_max = max(modate), by(sa_uuid)
egen temp = max(modate) if days!=., by(sa_uuid)
egen temp_max_bill = mean(temp), by(sa_uuid)
format %tm temp*
gen flag_ami_after_bills_stop = 0

br sa_uuid modate days_max days day_first day_last mnth_bill_kwh flag_disct_bill flag_acct ///
	interval_bill_corr kwh_interval mnth_bill_kwh_filled ndays_interval temp_max temp_max_bill ///
	if year>=2016 & (days<days_max | days==.) & temp_max>temp_max_bill

replace flag_ami_after_bills_stop = 1 if kwh_interval!=. & days<days_max & ///
	ndays_interval==days_max & interval_bill_corr_filled>0.9 & interval_bill_corr_filled!=. & ///
	modate==temp_max_bill 	
replace mnth_bill_kwh_filled = kwh_interval if kwh_interval!=. & days<days_max & ///
	ndays_interval==days_max & interval_bill_corr_filled>0.9 & interval_bill_corr_filled!=. & ///
	modate==temp_max_bill 
	
forvalues i = 1/24 {
	replace flag_ami_after_bills_stop = 1 if mnth_bill_kwh_filled==. & ///
		kwh_interval!=. & days==. & ndays_interval==days_max & ///
		interval_bill_corr_filled>0.9 & interval_bill_corr_filled!=. & ///
		modate==temp_max_bill+`i' & sa_uuid==sa_uuid[_n-`i']
	replace mnth_bill_kwh_filled = kwh_interval if  mnth_bill_kwh_filled==. & ///
		kwh_interval!=. & days==. & ndays_interval==days_max & ///
		interval_bill_corr_filled>0.9 & interval_bill_corr_filled!=. & ///
		modate==temp_max_bill+`i' & sa_uuid==sa_uuid[_n-`i']	
}

drop temp*


	// replace cases where AMI starts beore bills 
egen temp_min = min(modate), by(sa_uuid)
egen temp = min(modate) if days!=., by(sa_uuid)
egen temp_min_bill = mean(temp), by(sa_uuid)
format %tm temp*
gen flag_ami_before_bills_start = 0

br sa_uuid modate days_max days day_first day_last mnth_bill_kwh flag_disct_bill flag_acct ///
	interval_bill_corr kwh_interval mnth_bill_kwh_filled ndays_interval temp_min temp_min_bill ///
	if year>=2016 & (days<days_max | days==.) & temp_min<temp_min_bill

replace flag_ami_before_bills_start = 1 if kwh_interval!=. & days<days_max & ///
	ndays_interval==days_max & interval_bill_corr_filled>0.9 & interval_bill_corr_filled!=. & ///
	modate==temp_min_bill 	
replace mnth_bill_kwh_filled = kwh_interval if kwh_interval!=. & days<days_max & ///
	ndays_interval==days_max & interval_bill_corr_filled>0.9 & interval_bill_corr_filled!=. & ///
	modate==temp_min_bill 

forvalues i = 1/36 {
	replace flag_ami_before_bills_start = 1 if mnth_bill_kwh_filled==. & ///
		kwh_interval!=. & days==. & ndays_interval==days_max & ///
		interval_bill_corr_filled>0.9 & interval_bill_corr_filled!=. & ///
		modate==temp_min_bill-`i' & sa_uuid==sa_uuid[_n+`i']
	replace mnth_bill_kwh_filled = kwh_interval if  mnth_bill_kwh_filled==. & ///
		kwh_interval!=. & days==. & ndays_interval==days_max & ///
		interval_bill_corr_filled>0.9 & interval_bill_corr_filled!=. & ///
		modate==temp_min_bill-`i' & sa_uuid==sa_uuid[_n+`i']	
}
	
drop temp*	
	
	// populate single value that somehow didn't populate above
count if mnth_bill_kwh_filled==. & kwh_interval!=. & interval_bill_corr_filled>0.9 ///
	& interval_bill_corr_filled!=. & ndays_interval==days_max
replace mnth_bill_kwh_filled = kwh_interval if mnth_bill_kwh_filled==. & ///
	kwh_interval!=. & interval_bill_corr_filled>0.9 & interval_bill_corr_filled!=. & /// 
	ndays_interval==days_max
assert mnth_bill_kwh_filled!=. if kwh_interval!=. & interval_bill_corr_filled>0.9 ///
	& interval_bill_corr_filled!=. & ndays_interval==days_max
 	
	
	// populate values where both billing and interval have days missing, but
	// interval has FEWER missing days
replace mnth_bill_kwh_filled = kwh_interval if kwh_interval!=. & ///
	interval_bill_corr_filled>0.9 & interval_bill_corr_filled!=. & /// 
	ndays_interval<days_max & days<days_max & ndays_interval>days
	// 2337 observations
gen temp = ndays_interval - days if kwh_interval!=. & ///
	interval_bill_corr_filled>0.9 & interval_bill_corr_filled!=. & /// 
	ndays_interval<days_max & days<days_max & ndays_interval>days		
tab temp // adding in a fair amount of days here
drop temp

	// populate values where billing is entirely missing and interval 
	// interval is populated but has a few days missing
replace mnth_bill_kwh_filled = kwh_interval if kwh_interval!=. & ///
	interval_bill_corr_filled>0.9 & interval_bill_corr_filled!=. & /// 
	ndays_interval<days_max & days==.
	// 138 observations

	// confirm all observations with high correlaton are popullated
assert mnth_bill_kwh_filled!=. if kwh_interval!=. & interval_bill_corr_filled>0.9 ///
	& interval_bill_corr_filled!=. 
	

	// 3rd option: flat-out replace monthified kwh with interval data (whenever nonmissing)
gen mnth_bill_kwh_interval = mnth_bill_kwh
replace mnth_bill_kwh_interval = kwh_interval if kwh_interval!=.	

	// Flags
gen flag_kwh_filled_from_interval = abs(mnth_bill_kwh_filled-mnth_bill_kwh)>0.001 | ///
	(mnth_bill_kwh==. & mnth_bill_kwh_filled!=.)
count if year>=2016
local rN = r(N)
count if year>=2016 & flag_kwh_filled_from_interval==1 & mnth_bill_kwh!=.
di r(N)/`rN'
count if year>=2016 & flag_kwh_filled_from_interval==1 & mnth_bill_kwh==.
di r(N)/`rN'

	// Clean up and label
replace interval_bill_corr = interval_bill_corr_filled
la var days_max "Number of days in month"
rename days days_billing
rename day_first day_bill_first
rename day_last day_bill_last
order days_max day_bill_first day_bill_last ndays_interval, after(days_billing) 
la var flag_end_of_gap "Flag for end of discontinuous billing gap"
la var mnth_bill_kwh_filled "Total kWh, monthified from bills, filled from interval data (corr>0.9)" 
la var flag_ami_after_bills_stop "Flag for if billing data stops before interval data"
la var flag_ami_before_bills_start "Flag for if billing data starts after interval data"
la var mnth_bill_kwh_interval "Total kWh, interval data (where nonmissing), then monthified billed kWh" 
la var flag_kwh_filled_from_interval "Flag if mnth_bill_kwh_filled comes from AMI, rather than bills"
order mnth_bill_kwh_filled mnth_bill_kwh_interval, after(mnth_bill_kwh)
order flag_end_of_gap gap_length_months, after(flag_disct_bill)
drop interval_bill_corr_filled month year kwh_interval gap_length_months
 
	// Save
sort sa_uuid modate
unique sa_uuid modate
assert r(unique)==r(N)
compress
save "$dirpath_data/sce_cleaned/billing_data_monthified.dta", replace 	
 
}

*******************************************************************************
*******************************************************************************
