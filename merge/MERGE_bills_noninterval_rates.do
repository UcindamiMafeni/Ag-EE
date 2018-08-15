clear all
version 13
set more off

******************************************************************************
**** Script to assign PGE rates to billing data that don't merge into AMI ****
******************************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

	***** COME BACK AND FIX THIS STUFF LATER:
	***** 1. Assign SAs to the proper groups 
	***** 2. Fix rate AG-4B!!
	***** 3. Get AG-ICE rates

*******************************************************************************
*******************************************************************************

** 1. Use AMI-matched bills to calculate average prices by day-rate
{
** Start with merged billing data that actually have prices
use "$dirpath_data/merged/bills_rates_constructed.dta", clear
keep sa_uuid bill_start_dt bill_end_dt bill_length rt_sched_cd p_kw*

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

** Collapse down to 1 day on cusps (keep start date over end date)
assert date==bill_end_dt | date==bill_start_dt if temp_wt==0.5
drop if temp_wt==0.5 & date==bill_end_dt
drop temp_new temp_wt

** Confirm uniqueness
unique sa_uuid date
assert r(unique)==r(N)

** Collapse to rate-date
foreach v of varlist p_kw* {
	egen double temp = mean(`v'), by(rt_sched_cd date)
	replace `v' = temp
	drop temp
}
keep rt_sched_cd date p_kw*
duplicates drop
unique rt_sched_cd date
assert r(unique)==r(N)

** Save temporary file
compress
tempfile avg_prices
save `avg_prices'

** Now merge these average prices into non-AMI-matched bills 

** Load cleaned PGE bililng data (full dataset)
use "$dirpath_data/pge_cleaned/billing_data_20180719.dta", clear
gen pull = "20180719"
merge 1:1 sa_uuid bill_start_dt using "$dirpath_data/pge_cleaned/billing_data_20180322.dta"
replace pull = "20180322" if _merge==2
drop _merge

** Keep subset of bills that haven't already merged into rate data
tab flag_interval_merge
merge 1:1 sa_uuid bill_start_dt using "$dirpath_data/merged/bills_rates_constructed.dta", ///
	keepusing(sa_uuid) keep(1) nogen
tab rt_sched_cd flag_interval_merge

** Drop bill starting prior to 2011
drop if bill_start_dt<date("01jan2011","DMY")

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

** Collapse down to 1 day on cusps (keep start date over end date)
assert date==bill_end_dt | date==bill_start_dt if temp_wt==0.5
drop if temp_wt==0.5 & date==bill_end_dt
drop temp_new temp_wt

** Confirm uniqueness
unique sa_uuid date
assert r(unique)==r(N)

** Prep for merge into merged rate schedule variable
replace rt_sched_cd = subinstr(rt_sched_cd,"H","",1) if substr(rt_sched_cd,1,1)=="H"
replace rt_sched_cd = subinstr(rt_sched_cd,"AG","AG-",1) if substr(rt_sched_cd,1,2)=="AG"
drop if substr(rt_sched_cd,1,2)!="AG"

** Merge into average prices by rate-day
merge m:1 rt_sched_cd date using `avg_prices'
tab rt_sched _merge
keep if _merge==3
drop _merge
assert rt_sched_cd!=""
unique sa_uuid date
assert r(unique)==r(N)

** Collapse back to bill, taking the average of price variables
foreach v of varlist p_k* {
	egen double temp = mean(`v'), by(sa_uuid bill_start_dt)
	replace `v' = temp
	drop temp
}
drop date
duplicates drop

** Confirm uniqueness
unique sa_uuid bill_start_dt
assert r(unique)==r(N)

** Create flag for price taken from averages
gen flag_p_from_ami_avgs = 1
la var flag_p_from_ami_avgs "Flag for non-AMI bills, with prices assigned from rate-day averages"

** Save
la var pull "Which data pull does this SA come from?"
compress
save "$dirpath_data/merged/bills_rates_nonami.dta", replace

}

*******************************************************************************
*******************************************************************************

