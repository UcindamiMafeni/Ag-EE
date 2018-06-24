clear all
version 13
set more off

**********************************************************************
**** Script to assign prices to bills directly from PGE rate data ****
**********************************************************************

	***** COME BACK AND FIX THIS STUFF LATER:
	***** 1. Assign SAs to groups, when we know the groups
	***** 2. Fix rate AG-4B!!
	***** 3. Get AG-ICE rates
	***** 4. DOUBLE CHECK LIST OF EVENT DAYS
	***** 5. Fix monthified to expand to the correct *days* within a month

** This script assigns min/max/average prices to ALL bills, not by merging into
** AMI data or taking averages. Rather, it takes prices *directly* from rate
** data and applies them equally to all bills, without differentiating between
** bills that also have AMI data and bills that don't. A major shortcoming of this
** approach is taking unweighted averages across hours.

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

**********************************************************************
**********************************************************************

** 1. Create daily panel of average prices by rate
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

** Code up marginal price per kWh
gen p_kwh = .
replace p_kwh = energycharge // start with energy charge per kwh (for all non-Event hours)
replace pdpenergycredit = 0 if pdpenergycredit==.
assert pdpenergycredit<=0 & pdpcharge>=0 & pdpcharge!=.
replace p_kwh = pdpcharge + pdpenergycredit if pdpcharge!=0 & inlist(hour,14,15,16,17) & ///
	year(date)>=2013 & event_day_biz==1 & /// 4-hour event windows 2013-2017, based on "business" Event Days
	(pdpcharge+pdpenergycredit)!=0 & (pdpcharge+pdpenergycredit)!=.
replace p_kwh = pdpcharge + pdpenergycredit if pdpcharge!=0 & inlist(hour,14,15,16,17) & ///
	year(date)<2013 & event_day_res==1 & /// 4-hour event windows 2013-2017, based on "residential" Event Days
	(pdpcharge+pdpenergycredit)!=0 & (pdpcharge+pdpenergycredit)!=.
	
** Code up prices per kW
gen p_kw_max = maxdemandcharge
gen p_kw_peak = demandcharge if offpeak==0 & partpeak==0 & peak==1 
gen p_kw_partpeak = demandcharge if offpeak==0 & partpeak==1 & peak==0

gen p_kw_pdp_peak = pdpcredit if offpeak==0 & partpeak==0 & peak==1 & (inlist(month(date),5,6,7,8,9,10))
gen p_kw_pdp_partpeak = pdpcredit if offpeak==0 & partpeak==1 & peak==0 & (inlist(month(date),5,6,7,8,9,10))
replace p_kw_pdp_peak = 0 if p_kw_pdp_peak==.
replace p_kw_pdp_partpeak = 0 if p_kw_pdp_partpeak==.

replace p_kw_peak = p_kw_peak + p_kw_pdp_peak
replace p_kw_partpeak = p_kw_partpeak + p_kw_pdp_partpeak

** Collapse to daily level (TAKING UNWEIGHTED AVERAGES OF HOURS!)
assert p_kw_peak==. if (offpeak==0 & partpeak==0 & peak==1)==0
assert p_kw_partpeak==. if (offpeak==0 & partpeak==1 & peak==0)==0
foreach v of varlist p_kwh p_kw_max p_kw_peak p_kw_partpeak {
	egen double mean_`v' = mean(`v'), by(rateschedule tou group date)
}
egen double min_p_kwh = min(p_kwh), by(rateschedule tou group date)
egen double max_p_kwh = max(p_kwh), by(rateschedule tou group date)

keep rateschedule date tou group mean_p_kw* min_p_kw* max_p_kw*
duplicates drop 

** Confirm uniqueness
unique rateschedule date group tou
assert r(unique)==r(N)

** Confirm TOU is redudnant and drop
unique rateschedule date group
assert r(unique)==r(N)
drop tou

** Collapse groups, taking simple average of all price variables (COME BACK AND REMOVE LATER)
foreach v of varlist *_p_* {
	egen double temp = mean(`v'), by(rateschedule date)
	replace `v' = temp
	drop temp
}
drop group
duplicates drop

** Confirm uniqueness
unique rateschedule date
assert r(unique)==r(N)
	
** Label 
la var mean_p_kwh "Avg daily marginal price ($/kWh) for rate (weighting hours equally)"
la var min_p_kwh "Min daily marginal price ($/kWh) for rate"
la var max_p_kwh "Max daily marginal price ($/kWh) for rate"
la var mean_p_kw_max "Avg max demand charge ($/kW) for rate"
la var mean_p_kw_peak "Avg peak demand charge ($/kW) for rate"	
la var mean_p_kw_partpeak "Avg partial peak demand charge ($/kW) for rate"
	
** Save
rename rateschedule rt_sched_cd
compress
save "$dirpath_data/merged/ag_rates_avg_by_day.dta", replace

}

**********************************************************************
**********************************************************************

** 2. Merge average prices by rate into billing data
{
use "$dirpath_data/pge_cleaned/billing_data.dta", clear

** Prep for merge into rate schedule data
replace rt_sched_cd = subinstr(rt_sched_cd,"H","",1) if substr(rt_sched_cd,1,1)=="H"
replace rt_sched_cd = subinstr(rt_sched_cd,"AG","AG-",1) if substr(rt_sched_cd,1,2)=="AG"
drop if substr(rt_sched_cd,1,2)!="AG"

** Keep only essential variables (for purposes of this script)
keep sa_uuid bill_start_dt bill_end_dt bill_length rt_sched_cd

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

** Merge into averge daily rates data
merge m:1 rt_sched_cd date using "$dirpath_data/merged/ag_rates_avg_by_day.dta"
assert date>date("01sep2017","DMY") if _merge==2
drop if _merge==2

** Collapse back to bills
foreach v of varlist *_p_* {
	local fxn = subinstr(substr("`v'",1,4),"_","",1)
	egen double temp = `fxn'(`v'), by(sa_uuid bill_start_dt)
	replace `v' = temp
	drop temp
}
drop date _merge
duplicates drop

** Confirm uniqueness
unique sa_uuid bill_start_dt
assert r(unique)==r(N)

** Save
compress
save "$dirpath_data/merged/bills_avg_prices_nomerge.dta", replace

}
**********************************************************************
**********************************************************************

** 3. Merge average prices by rate into monthified billing data
{
use "$dirpath_data/pge_cleaned/billing_data_monthified.dta", clear

** Prep for merge into rate schedule data
replace rt_sched_cd = subinstr(rt_sched_cd,"H","",1) if substr(rt_sched_cd,1,1)=="H"
replace rt_sched_cd = subinstr(rt_sched_cd,"AG","AG-",1) if substr(rt_sched_cd,1,2)=="AG"
drop if substr(rt_sched_cd,1,2)!="AG"

** Keep only essential variables (for purposes of this script)
keep sa_uuid modate days day_first day_last rt_sched_cd

** Create flag for the (very few) months where there's a gap
gen flag_date_gap = (day_last-day_first+1)!=days
tab flag_date_gap // 113 out of 5.3M
la var flag_date_gap "Flag for SA-months where expanding by day is fuzzy"

** Expand whole dataset by bill length variable
expand days, gen(temp_new)
sort sa_uuid modate temp_new
tab temp_new

** Construct date variable (duplicated at each bill change-over)
gen date = date(substr(string(modate,"%tm"),6,2) + "/" + string(day_first) + ///
	"/" + substr(string(modate,"%tm"),1,4),"MDY") if temp_new==0
format %td date
replace date = date[_n-1]+1 if temp_new==1
assert day(date)==day_first if temp_new==0
assert day(date)==day_last if temp_new[_n+1]==0 & flag_date_gap==0
assert date!=.
unique sa_uuid modate date
assert r(unique)==r(N)

** Merge into averge daily rates data
merge m:1 rt_sched_cd date using "$dirpath_data/merged/ag_rates_avg_by_day.dta"
assert date>date("01sep2017","DMY") if _merge==2
drop if _merge==2

** Collapse back to bills
foreach v of varlist *_p_* {
	local fxn = subinstr(substr("`v'",1,4),"_","",1)
	egen double temp = `fxn'(`v'), by(sa_uuid modate)
	replace `v' = temp
	drop temp
}
drop date _merge day_first day_last days temp_new
duplicates drop

** Confirm uniqueness
unique sa_uuid modate
assert r(unique)==r(N)

** Save
compress
save "$dirpath_data/merged/monthified_avg_prices_nomerge.dta", replace

}
**********************************************************************
**********************************************************************
