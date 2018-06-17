clear all
version 13
set more off

*****************************************************************************
**** Script to merge customer bills and interval data into PGE rate data ****
*****************************************************************************

** This script does two things, ONLY for SA-bills that merge into AMI data: 
** 		1) Add hourly marginal prices to AMI data
**		2) Add $/kW fixed charges to each bill

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
replace pdpenergycredit = 0 if pdpenergycredit==.
unique rt_sched_cd group date hour
assert r(unique)==r(N)
compress
save "$dirpath_data/merged/ag_rates_for_merge.dta", replace
		
}

*******************************************************************************
*******************************************************************************

** 2. Merge in billing/interval data, looping over rates
{

** Loop over months of sample
local YM_min = ym(2011,1)
local YM_max = ym(2017,9)
forvalues YM = `YM_min'/`YM_max' {

	qui {
	
	** Load cleaned PGE bililng data
	use "$dirpath_data/pge_cleaned/billing_data.dta", clear

	** Keep if in month
	keep if `YM'==ym(year(bill_start_dt),month(bill_start_dt))

	** Prep for merge into rate schedule data
	replace rt_sched_cd = subinstr(rt_sched_cd,"H","",1) if substr(rt_sched_cd,1,1)=="H"
	replace rt_sched_cd = subinstr(rt_sched_cd,"AG","AG-",1) if substr(rt_sched_cd,1,2)=="AG"
	drop if substr(rt_sched_cd,1,2)!="AG"

	** Drop observations without good interval data (for purposes of corroborating dollar amounts)
	keep if flag_interval_merge==1

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
	use "$dirpath_data/merged/ag_rates_for_merge.dta" if inrange(date,`dmin',`dmax')
	tempfile temp_rates
	save `temp_rates'
	restore
	joinby rt_sched_cd date hour using `temp_rates', unmatched(master)
	assert _merge==3 | (_merge==1 & rt_sched_cd=="AG-ICE")
	drop if _merge==1
	drop _merge

	** Assign marginal price per kWh
	gen p_kwh = .
	replace p_kwh = energycharge // start with energy charge per kwh (for all non-Event hours)
	assert pdpenergycredit<=0 & pdpcharge!=. & pdpcharge>=0
	replace p_kwh = pdpcharge + pdpenergycredit if pdpcharge!=0 & inlist(hour,14,15,16,17) & ///
		year(date)>=2013 & event_day_biz==1 // 4-hour event windows 2013-2017, based on "business" Event Days
	replace p_kwh = pdpcharge + pdpenergycredit if pdpcharge!=0 & inlist(hour,14,15,16,17) & ///
		year(date)<2013 & event_day_res==1 // 4-hour event windows 2013-2017, based on "residential" Event Days
		
	** Save hourly data with rates and prices
	preserve
	keep sa_uuid date hour kwh p_kwh bill_start_dt group
	la var p_kwh "Hourly (avg) marginal price ($/kWh)"
	compress
	save "$dirpath_data/merged/hourly_with_prices_`YM'.dta", replace
	restore	
		
	** Check if max kW is missing anywhere where we need it
	count if max_demand==. & maxdemandcharge!=0 & maxdemandcharge!=.
	local Nmiss_demand = r(N)
	if `Nmiss_demand'>0 {
		gen flag_max_demand_constr = max_demand==. & maxdemandcharge!=0 & maxdemandcharge!=.
		la var flag_max_demand_constr "Fixed charge (max) from AMI data, missing in billing"
		egen double temp_max_demand = max(kwh), by(sa_uuid bill_start_dt)
		replace max_demand = temp_max_demand if flag_max_demand_constr==1
		drop temp*
	}

	count if peak_demand==. & demandcharge!=0 & demandcharge!=. & peak==1 & partpeak==0 & offpeak==0
	local Nmiss_peak = r(N)
	if `Nmiss_peak'>0 {
		gen flag_peak_demand_constr = peak_demand==. & demandcharge!=0 & demandcharge!=. ///
			& peak==1 & partpeak==0 & offpeak==0
		la var flag_peak_demand_constr "Fixed charge (peak) from AMI data, missing in billing"
		egen double temp_peak_demand0 = max(kwh) if peak==1 & partpeak==0 & offpeak==0, by(sa_uuid bill_start_dt)
		egen double temp_peak_demand = mean(temp_peak_demand0), by(sa_uuid bill_start_dt)
		replace peak_demand = temp_peak_demand if flag_peak_demand_constr==1
		drop temp*
	}

	count if partial_peak_demand==. & demandcharge!=0 & demandcharge!=. & peak==0 & partpeak==1 & offpeak==0
	local Nmiss_partpeak = r(N)
	if `Nmiss_partpeak'>0 {
		gen flag_partpeak_demand_constr = partial_peak_demand==. & demandcharge!=0 & demandcharge!=. ///
			& peak==0 & partpeak==1 & offpeak==0
		la var flag_partpeak_demand_constr "Fixed charge (partpeak) from AMI data, missing in billing"
		egen double temp_partial_peak_demand0 = max(kwh) if peak==0 & partpeak==1 & offpeak==0, by(sa_uuid bill_start_dt)
		egen double temp_partial_peak_demand = mean(temp_partial_peak_demand0), by(sa_uuid bill_start_dt)
		replace partial_peak_demand = temp_partial_peak_demand if flag_partpeak_demand_constr==1
		drop temp*
	}

	** Calculate min/max/mean of marginal price, before collapsing
	egen double p_kwh_min = min(p_kwh), by(sa_uuid bill_start_dt group)
	egen double p_kwh_max = max(p_kwh), by(sa_uuid bill_start_dt group)
	egen double p_kwh_mean = mean(p_kwh), by(sa_uuid bill_start_dt group)
	la var p_kwh_min "Min marg price ($/kWh) across whole bill"
	la var p_kwh_max "Max marg price ($/kWh) across whole bill"
	la var p_kwh_mean "Mean marg price ($/kWh) across whole bill"

	** Calculate total volumetric portion of bill
	gen temp = kwh*p_kwh
	egen double total_bill_volumetric = sum(kwh*p_kwh), by(sa_uuid bill_start_dt group)
	la var total_bill_volumetric "Total $ of volumetric charges on bill ($/kWh * kWh)"
	drop temp

	** Collapse down from hourly observations, to expedite the next few steps
	drop date hour kwh tou p_kwh energycharge pdpcharge pdpenergycredit event_day* bill_length
	duplicates drop sa_uuid bill_start_dt group offpeak partpeak peak, force
		// all I need here is SA-bill-group-offpeak/partpeak/peak to build up
		// group-specific fixed charges

	** Calculate fixed charge per kW, for whole bill
	gen temp_max1 = maxdemandcharge*max_demand
	egen temp_max2 = max(temp_max1), by(sa_uuid bill_start_dt group)

	gen temp_peak1 = demandcharge*peak_demand if offpeak==0 & partpeak==0 & peak==1
	egen temp_peak2 = max(temp_peak1), by(sa_uuid bill_start_dt group)

	gen temp_partpeak1 = demandcharge*partial_peak_demand if offpeak==0 & partpeak==1 & peak==0
	egen temp_partpeak2 = max(temp_partpeak1), by(sa_uuid bill_start_dt group)

	gen temp_pdp_peak1 = pdpcredit*peak_demand if offpeak==0 & partpeak==0 & peak==1 & ///
		(inlist(month(bill_start_dt),5,6,7,8,9,10) | inlist(month(bill_end_dt),5,6,7,8,9,10))
	egen temp_pdp_peak2 = max(temp_pdp_peak1), by(sa_uuid bill_start_dt group)

	gen temp_pdp_partpeak1 = pdpcredit*partial_peak_demand if offpeak==0 & partpeak==1 & peak==0 & ///
		(inlist(month(bill_start_dt),5,6,7,8,9,10) | inlist(month(bill_end_dt),5,6,7,8,9,10))
	egen temp_pdp_partpeak2 = max(temp_pdp_partpeak1), by(sa_uuid bill_start_dt group)

	gen total_bill_kw = 0
	replace total_bill_kw = total_bill_kw + temp_max2 if temp_max2!=.
	replace total_bill_kw = total_bill_kw + temp_peak2 if temp_peak2!=.
	replace total_bill_kw = total_bill_kw + temp_partpeak2 if temp_partpeak2!=.
	replace total_bill_kw = total_bill_kw + temp_pdp_peak2 if temp_pdp_peak2!=.
	replace total_bill_kw = total_bill_kw + temp_pdp_partpeak2 if temp_pdp_partpeak2!=.
	drop temp*
	la var total_bill_kw "Total $ of per-kW charges on bill ($/kW * max_kW)"

	** Calculate fixed charge per day, for whole bill
	assert customercharge!=. & metercharge!=.
	gen total_bill_fixed = (bill_end_dt - bill_start_dt)*(customercharge + metercharge)
		// I'm assuming that relevant bill length is end_dt-start_dt, not end_dt-start_dt+1
		// Nearly all normal bills have end_dt that matches the next start date, which would
		// mean the billing days by end_dt-start_dt+1 would double-count the cusp day
	la var total_bill_fixed "Total $ of fixed per-day charges on bill"
		
	** Collapse to the SA-bill-group level
	foreach v of varlist *max_demand* *peak_demand* partial_peak_demand {
		egen double temp = max(`v'), by(sa_uuid bill_start_dt group)
		replace `v' = temp if `v'==.
		replace `v' = temp if substr("`v'",1,4)=="flag"
		drop temp
	}
	drop offpeak partpeak peak demandcharge maxdemandcharge customercharge metercharge pdpcredit
	duplicates drop
	unique sa_uuid bill_start_dt group
	if r(unique)!=r(N) { // to fix a weird glitch where total_bill_fixed wasn't unique
		duplicates t sa_uuid bill_start_dt group, gen(temp_dup)
		egen double temp = max(total_bill_fixed), by(sa_uuid bill_start_dt group)
		replace total_bill_fixed = temp if temp_dup>0
		drop temp temp_dup
		duplicates drop
	}
	unique sa_uuid bill_start_dt group
	assert r(unique)==r(N)

	** Add up bill components to get to total estimated bill amount
	assert total_bill_volumetric!=. & total_bill_kw!=. & total_bill_fixed!=.
	gen total_bill_amount_constr = total_bill_volumetric + total_bill_kw + total_bill_fixed
	la var total_bill_amount_constr "Total $ on bill, constructed by summing fixed + marginal components"

	** Save monthly data of constructed bill components
	compress
	save "$dirpath_data/merged/bills_rates_constructed_`YM'.dta", replace
	
	}
	
	di %tm `YM' "  " c(current_time)
}

** Append monthly files (hourly)
clear 
cd "$dirpath_data/merged"
local files_hourly : dir "." files "hourly_with_prices_*.dta"
foreach f in `files_hourly' {
	append using "`f'"
}
duplicates drop // for some reason there are a small number of dups...
sort sa_uuid date hour group bill_start_dt
duplicates t sa_uuid date hour group, gen(dup) // dups occur on that span months bill cusp dates
assert inlist(dup,0,1)
drop if dup==1 & dup[_n+1]==1 & sa_uuid==sa_uuid[_n+1] & date==date[_n+1] & hour==hour[_n+1] & ///
	 group==group[_n+1] & bill_start_dt<bill_start_dt[_n+1] // keep later bill date (everything else is identical)
drop dup
unique sa_uuid date hour group
assert r(unique)==r(N)
compress
save "$dirpath_data/merged/hourly_with_prices.dta", replace

** Append monthly files (bills)
clear 
cd "$dirpath_data/merged"
local files_bills : dir "." files "bills_rates_constructed_*.dta"
foreach f in `files_bills' {
	append using "`f'"
}
duplicates drop
sort sa_uuid bill_start_dt group
unique sa_uuid bill_start_dt group
assert r(unique)==r(N)
compress
save "$dirpath_data/merged/bills_rates_constructed.dta", replace

** Delete monthly files (hourly)
cd "$dirpath_data/merged"
local files_hourly : dir "." files "hourly_with_prices_*.dta"
foreach f in `files_hourly' {
	erase "`f'"
}

** Delete monthly files (bills)
cd "$dirpath_data/merged"
local files_bills : dir "." files "bills_rates_constructed_*.dta"
foreach f in `files_bills' {
	erase "`f'"
}

}

*******************************************************************************
*******************************************************************************

*** USE CORRELATION TO ASSIGN GROUPS



	***** COME BACK AND FIX THIS LATER TO:
	***** 1. Assign SAs to groups!
	***** 2. For flags on peak and partpeak, differentiate between "missing and needed
	*****    to calculate bill" vs. "missing but not relevant"
