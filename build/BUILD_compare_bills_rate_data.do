clear all
version 13
set more off

***********************************************************************************
**** Script to compare customer bills to constructed bills using PGE rate data ****
***********************************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

*******************************************************************************
*******************************************************************************

** 1. Prepare rates for merge
{

** Load rates for both large and small farms
use "$dirpath_data/pge_cleaned/large_ag_rates.dta", clear
append using "$dirpath_data/pge_cleaned/small_ag_rates.dta"
unique rateschedule-peak
assert r(unique)==r(N)
unique rateschedule-minute
assert r(unique)==r(N)

** Populate group variable where missing
assert group==. if tou==0
replace group = 1 if group==. & tou==0
assert group!=.

** Collapse from 30 min to 1 hour (which is the resolution of our AMI data)
foreach v of varlist offpeak partpeak peak {
	egen temp = max(`v'), by(rateschedule-hour)
	replace `v' = temp
	drop temp
}
foreach v of varlist demandcharge-pdpenergycredit {
	egen double temp = mean(`v'), by(rateschedule-hour)
	replace `v' = temp
	drop temp
}
drop minute
duplicates drop
unique rateschedule-hour
assert r(unique)==r(N)

** Expand to the daily level
gen rate_length = rate_end_date-rate_start_date+1
gen year = year(rate_start_date)
assert year==2018 if rate_length==. // all rates starting in 2018 which are after our sample
drop if year==2018 
expand rate_length, gen(temp_new)
sort rateschedule-peak temp_new
	
** Construct date variable (duplicated at each rate change-over)
gen date = rate_start_date if temp_new==0
format %td date
replace date = date[_n-1]+1 if temp_new==1
assert date==rate_start_date if temp_new==0
assert date==rate_end_date if temp_new[_n+1]==0
assert date!=.
unique rateschedule-hour date
assert r(unique)==r(N)
order date, after(rate_end_date)

** Drop redundant summer/winter months 
drop if season=="summer" & inlist(month(date),11,12,1,2,3,4)	
drop if season=="winter" & inlist(month(date),5,6,7,8,9,10)	
drop season // now redundant

** Match day of week, and drop mismatched dates/dow's
gen temp_dow = dow(date)
gen temp_dow_match = temp_dow==dow_num
keep if temp_dow_match==1 | dow_num==.
drop temp_dow temp_dow_match dow_num
sort rateschedule tou group date hour	
*br rateschedule tou group rate_start_date rate_end_date date hour temp_new
	
** Expand and assign hours for non-TOU rates (where hour is missing)
expand 24 if tou==0 & hour==., gen(temp_new2)	
sort rateschedule tou group date hour temp_new2
replace hour = 0 if hour==. & tou==0 & temp_new2==0
replace hour = hour[_n-1]+1 if hour==. & tou==0 & temp_new2==1 & ///
	rateschedule==rateschedule[_n-1] & date==date[_n-1] & tou[_n-1]==0 & ///
	group==group[_n-1]
tab hour
	
** Confirm uniqueness of dates
unique rateschedule tou group date hour	
assert r(unique)==r(N)
drop rate_start_date rate_end_date rate_length year temp_new*

** Merge in Event Days
merge m:1 date using "$dirpath_data/pge_cleaned/event_days.dta"
assert _merge!=2
drop _merge

/*
** Drop groups for now (COME BACK AND FIX THIS)
egen temp_min = min(group), by(rateschedule date)
egen temp_max = max(group), by(rateschedule date)
drop if temp_min<temp_max
drop temp*
*/
	
** Drop pre-2011 rates
drop if date<date("01jan2011","DMY")	

** Drop post-2017 rates
drop if date>date("01nov2017","DMY")	
	
** Save as working file
rename rateschedule rt_sched_cd
unique rt_sched_cd group date hour
assert r(unique)==r(N)
compress
save "$dirpath_data/pge_cleaned/ag_rates_for_merge.dta", replace
		
}

*******************************************************************************
*******************************************************************************

** 2. Merge in billing/interval data, looping over rates
{

** Loop over months of sample
local YM_min = ym(2011,1)
local YM_max = ym(2017,9)
local YM = 650

** Load cleaned PGE bililng data
use "$dirpath_data/pge_cleaned/billing_data.dta", clear

** Prep for merge into rate schedule data
replace rt_sched_cd = subinstr(rt_sched_cd,"H","",1) if substr(rt_sched_cd,1,1)=="H"
replace rt_sched_cd = subinstr(rt_sched_cd,"AG","AG-",1) if substr(rt_sched_cd,1,2)=="AG"
drop if substr(rt_sched_cd,1,2)!="AG"

** Drop observations without good interval data (for purposes of corroborating dollar amounts)
keep if flag_interval_merge==1

** Keep if in month
keep if `YM'==ym(year(bill_start_dt),month(bill_start_dt))

** Drop flags
drop flag* sp_uuid? interval_bill_corr

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

** Store min and max date, for narrowing down the two subsequent merges
sum date
local dmin = r(min)
local dmax = r(max)
	
** Merge in hourly interval data
preserve
clear
use "$dirpath_data/pge_cleaned/interval_data_hourly.dta" if inrange(date,`dmin',`dmax')
tempfile temp_interval
save `temp_interval'
restore
merge 1:m sa_uuid date using `temp_interval', keep(1 3)
assert _merge==3
drop _merge

** Merge in rate data by hour
preserve
clear
use "$dirpath_data/pge_cleaned/ag_rates_for_merge.dta" if inrange(date,`dmin',`dmax')
tempfile temp_rates
save `temp_rates'
restore
joinby rt_sched_cd date hour using `temp_rates', unmatched(master)
assert _merge==3 | (_merge==1 & rt_sched_cd=="AG-ICE")
drop if _merge==1
drop _merge

** Assign marginal price











** Save as working file
compress
save "$dirpath_data/pge_cleaned/bills_hourly_for_rate_merge.dta", replace
	
}


*******************************************************************************
*******************************************************************************





	***** COME BACK AND FIX THIS LATER TO:
	***** 1. Assign SAs to groups!
