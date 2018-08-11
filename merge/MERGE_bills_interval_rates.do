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

	***** COME BACK AND FIX THIS STUFF LATER:
	***** 1. Assign SAs to groups in step 2!
	***** 2. Fix rate AG-4B!! (and maybe also AG-4C)
	***** 3. Get AG-ICE rates
	***** 4. Double check list of Event Days
	
	***** NOTE: fixed charges at the season changeover (Apr/May, Oct/Nov) will be off.
	***** PGE prorates based on max kW & proportion of bill in each season, but the 
	***** billing data report only 1 value for max demand for changeover months, which
	***** we couldn't properly unpack...

*******************************************************************************
*******************************************************************************

** 1. Prepare rates for merge
if 1==1 {

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
	
** Fix holidays so all hours are off-peak (date holiday is observed!)
	// New Year's Day, President's Day, Memorial Day, Independence Day, Labor Day, 
	// Veterans Day, Thanksgiving Day, and Christmas Day (list of holidays from PGE)
gen temp_holiday = 0

	// New Year's Day
replace temp_holiday = 1 if date==date("31jan2010","DMY")
replace temp_holiday = 1 if date==date("02jan2012","DMY")
replace temp_holiday = 1 if date==date("01jan2013","DMY")
replace temp_holiday = 1 if date==date("01jan2014","DMY")
replace temp_holiday = 1 if date==date("01jan2015","DMY")
replace temp_holiday = 1 if date==date("01jan2016","DMY")
replace temp_holiday = 1 if date==date("02jan2017","DMY")

	// President's Day
replace temp_holiday = 1 if date==date("21feb2011","DMY")
replace temp_holiday = 1 if date==date("20feb2012","DMY")
replace temp_holiday = 1 if date==date("18feb2013","DMY")
replace temp_holiday = 1 if date==date("17feb2014","DMY")
replace temp_holiday = 1 if date==date("16feb2015","DMY")
replace temp_holiday = 1 if date==date("15feb2016","DMY")
replace temp_holiday = 1 if date==date("20feb2017","DMY")

	// Memorial Day
replace temp_holiday = 1 if date==date("30may2011","DMY")
replace temp_holiday = 1 if date==date("28may2012","DMY")
replace temp_holiday = 1 if date==date("27may2013","DMY")
replace temp_holiday = 1 if date==date("26may2014","DMY")
replace temp_holiday = 1 if date==date("25may2015","DMY")
replace temp_holiday = 1 if date==date("30may2016","DMY")
replace temp_holiday = 1 if date==date("29may2017","DMY")

	// Independence Day
replace temp_holiday = 1 if date==date("04jul2011","DMY")
replace temp_holiday = 1 if date==date("04jul2012","DMY")
replace temp_holiday = 1 if date==date("04jul2013","DMY")
replace temp_holiday = 1 if date==date("04jul2014","DMY")
replace temp_holiday = 1 if date==date("03jul2015","DMY")
replace temp_holiday = 1 if date==date("04jul2016","DMY")
replace temp_holiday = 1 if date==date("04jul2017","DMY")

	// Labor Day
replace temp_holiday = 1 if date==date("05sep2011","DMY")
replace temp_holiday = 1 if date==date("03sep2012","DMY")
replace temp_holiday = 1 if date==date("02sep2013","DMY")
replace temp_holiday = 1 if date==date("01sep2014","DMY")
replace temp_holiday = 1 if date==date("07sep2015","DMY")
replace temp_holiday = 1 if date==date("05sep2016","DMY")
replace temp_holiday = 1 if date==date("04sep2017","DMY")

	// Veteran's Day
replace temp_holiday = 1 if date==date("11nov2011","DMY")
replace temp_holiday = 1 if date==date("11nov2012","DMY")
replace temp_holiday = 1 if date==date("11nov2013","DMY")
replace temp_holiday = 1 if date==date("11nov2014","DMY")
replace temp_holiday = 1 if date==date("11nov2015","DMY")
replace temp_holiday = 1 if date==date("11nov2016","DMY")
replace temp_holiday = 1 if date==date("10nov2017","DMY")

	// Thanksgiving Day
replace temp_holiday = 1 if date==date("24nov2011","DMY")
replace temp_holiday = 1 if date==date("22nov2012","DMY")
replace temp_holiday = 1 if date==date("28nov2013","DMY")
replace temp_holiday = 1 if date==date("27nov2014","DMY")
replace temp_holiday = 1 if date==date("26nov2015","DMY")
replace temp_holiday = 1 if date==date("24nov2016","DMY")
replace temp_holiday = 1 if date==date("23nov2017","DMY")

	// Christmas Day
replace temp_holiday = 1 if date==date("26dec2011","DMY")
replace temp_holiday = 1 if date==date("25dec2012","DMY")
replace temp_holiday = 1 if date==date("25dec2013","DMY")
replace temp_holiday = 1 if date==date("25dec2014","DMY")
replace temp_holiday = 1 if date==date("25dec2015","DMY")
replace temp_holiday = 1 if date==date("26dec2016","DMY")
replace temp_holiday = 1 if date==date("25dec2017","DMY")

	// Calculate offpeak price per kWh for the day of each holiday
egen double temp1 = mean(energycharge) if offpeak==1 & partpeak==0 & peak==0 & temp_holiday==1, ///
	by(rateschedule tou group date)
egen double temp2 = mean(temp1) if temp_holiday==1, by(rateschedule tou group date)
egen double temp3 = sd(energycharge) if offpeak==1 & partpeak==0 & peak==0 & temp_holiday==1, ///
	by(rateschedule tou group date)
assert round(temp3,1e-6)==0 | temp3==. // confirm no variation in offpeak price within day/rate/group
assert temp2!=. if temp_holiday==1 & tou==1

	// Calculate offpeak price per kW for the day of each holiday
egen double temp4 = mean(demandcharge) if offpeak==1 & partpeak==0 & peak==0 & temp_holiday==1, ///
	by(rateschedule tou group date)
egen double temp5 = mean(temp4) if temp_holiday==1, by(rateschedule tou group date)
egen double temp6 = sd(demandcharge) if offpeak==1 & partpeak==0 & peak==0 & temp_holiday==1, ///
	by(rateschedule tou group date)
assert round(temp6,1e-6)==0 | temp6==. // confirm no variation in offpeak price within day/rate/group
assert temp5!=. if temp_holiday==1 & tou==1 & inlist(substr(rateschedule,-1,1),"B","C","E","F") 
	// only large ag rates have demand charges

	// Assign offpeak prices for all hours of all holidays
replace energycharge = temp2 if temp_holiday==1 & tou==1 & temp2!=.
replace demandcharge = temp5 if temp_holiday==1 & tou==1 & temp5!=. & inlist(substr(rateschedule,-1,1),"B","C","E","F")
replace offpeak = 1 if temp_holiday==1 & tou==1
replace partpeak = 0 if temp_holiday==1 & tou==1
replace peak = 0 if temp_holiday==1 & tou==1

	// Confirm that holidays never coincide with Event Days
assert event_day_biz==. & event_day_res==. if temp_holiday==1	

	// Clean up
rename temp_holiday holiday
la var holiday "Indicator for observed holiday (no peaks or partpeaks)"	
drop temp*	

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

** 2. Merge in billing/interval data, looping over rates (MARCH DATA)
if 1==1 {

local tag = "20180322"

** Loop over months of sample
local YM_min = ym(2011,1)
local YM_max = ym(2017,9)
forvalues YM = `YM_min'/`YM_max' {

	qui {
	
	** Load cleaned PGE bililng data
	use "$dirpath_data/pge_cleaned/billing_data_`tag'.dta", clear

	** Keep if in month
	keep if `YM'==ym(year(bill_start_dt),month(bill_start_dt))

	** Keep if NOT in the July data pull (this speeds up run time and avoid redundancies
	merge 1:1 sa_uuid bill_start_dt using "$dirpath_data/pge_cleaned/billing_data_20180719.dta", ///
		nogen keep(1) keepusing(sa_uuid bill_start_dt)
	
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
	use "$dirpath_data/pge_cleaned/interval_data_hourly_`tag'.dta" if inrange(date,`dmin',`dmax')
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
	save "$dirpath_data/merged/hourly_with_prices_`YM'_`tag'.dta", replace
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
	la var total_bill_volumetric "Total $ of volumetric charges on bill ($/kWh * kWh, constructed)"
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
	la var total_bill_kw "Total $ of per-kW charges on bill ($/kW * max_kW, constructed)"

	** Calculate fixed charge per day, for whole bill
	assert customercharge!=. & metercharge!=.
	gen total_bill_fixed = (bill_end_dt - bill_start_dt)*(customercharge + metercharge)
		// I'm assuming that relevant bill length is end_dt-start_dt, not end_dt-start_dt+1
		// Nearly all normal bills have end_dt that matches the next start date, which would
		// mean the billing days by end_dt-start_dt+1 would double-count the cusp day
	la var total_bill_fixed "Total $ of fixed per-day charges on bill ($/day * days, constructed)"
		
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
	save "$dirpath_data/merged/bills_rates_constructed_`YM'_`tag'.dta", replace
	
	}
	
	di %tm `YM' "  " c(current_time)
}

** Append monthly files (hourly)
clear 
cd "$dirpath_data/merged"
local files_hourly : dir "." files "hourly_with_prices_*_`tag'.dta"
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
save "$dirpath_data/merged/hourly_with_prices_`tag'.dta", replace

** Append monthly files (bills)
clear 
cd "$dirpath_data/merged"
local files_bills : dir "." files "bills_rates_constructed_*_`tag'.dta"
foreach f in `files_bills' {
	append using "`f'"
}
duplicates drop
sort sa_uuid bill_start_dt group
unique sa_uuid bill_start_dt group
assert r(unique)==r(N)
compress
save "$dirpath_data/merged/bills_rates_constructed_`tag'.dta", replace

** Delete monthly files (hourly)
cd "$dirpath_data/merged"
local files_hourly : dir "." files "hourly_with_prices_*_`tag'.dta"
foreach f in `files_hourly' {
	erase "`f'"
}

** Delete monthly files (bills)
cd "$dirpath_data/merged"
local files_bills : dir "." files "bills_rates_constructed_*_`tag'.dta"
foreach f in `files_bills' {
	erase "`f'"
}

}

*******************************************************************************
*******************************************************************************

** 3. Merge in billing/interval data, looping over rates (JULY DATA)
if 1==1 {

local tag = "20180719"

** Loop over months of sample
local YM_min = ym(2011,1)
local YM_max = ym(2017,9)
forvalues YM = `YM_min'/`YM_max' {

	qui {
	
	** Load cleaned PGE bililng data
	use "$dirpath_data/pge_cleaned/billing_data_`tag'.dta", clear

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
	use "$dirpath_data/pge_cleaned/interval_data_hourly_`tag'.dta" if inrange(date,`dmin',`dmax')
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
	save "$dirpath_data/merged/hourly_with_prices_`YM'_`tag'.dta", replace
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
	la var total_bill_volumetric "Total $ of volumetric charges on bill ($/kWh * kWh, constructed)"
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
	la var total_bill_kw "Total $ of per-kW charges on bill ($/kW * max_kW, constructed)"

	** Calculate fixed charge per day, for whole bill
	assert customercharge!=. & metercharge!=.
	gen total_bill_fixed = (bill_end_dt - bill_start_dt)*(customercharge + metercharge)
		// I'm assuming that relevant bill length is end_dt-start_dt, not end_dt-start_dt+1
		// Nearly all normal bills have end_dt that matches the next start date, which would
		// mean the billing days by end_dt-start_dt+1 would double-count the cusp day
	la var total_bill_fixed "Total $ of fixed per-day charges on bill ($/day * days, constructed)"
		
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
	save "$dirpath_data/merged/bills_rates_constructed_`YM'_`tag'.dta", replace
	
	}
	
	di %tm `YM' "  " c(current_time)
}

** Append monthly files (hourly)
clear 
cd "$dirpath_data/merged"
local files_hourly : dir "." files "hourly_with_prices_*_`tag'.dta"
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
save "$dirpath_data/merged/hourly_with_prices_`tag'.dta", replace

** Append monthly files (bills)
clear 
cd "$dirpath_data/merged"
local files_bills : dir "." files "bills_rates_constructed_*_`tag'.dta"
foreach f in `files_bills' {
	append using "`f'"
}
duplicates drop
sort sa_uuid bill_start_dt group
unique sa_uuid bill_start_dt group
assert r(unique)==r(N)
compress
save "$dirpath_data/merged/bills_rates_constructed_`tag'.dta", replace

** Delete monthly files (hourly)
cd "$dirpath_data/merged"
local files_hourly : dir "." files "hourly_with_prices_*_`tag'.dta"
foreach f in `files_hourly' {
	erase "`f'"
}

** Delete monthly files (bills)
cd "$dirpath_data/merged"
local files_bills : dir "." files "bills_rates_constructed_*_`tag'.dta"
foreach f in `files_bills' {
	erase "`f'"
}

}

*******************************************************************************
*******************************************************************************

** 4. Diagnostics on billing data, and using correlations to resolve rate groups
if 1==1{
use "$dirpath_data/merged/bills_rates_constructed_20180719.dta", clear
gen pull = "20180719"
merge 1:1 sa_uuid bill_start_dt group using "$dirpath_data/merged/bills_rates_constructed_20180322.dta"
*assert _merge!=3
replace pull = "20180322" if _merge==2
drop _merge

** Check flags for peak/partial peak demand that were constructed (because missing for bill)
gen month = month(bill_start_dt)
tab flag_max_demand_constr, missing // missing for 54%, which means it was never created in the first place
replace flag_max_demand_constr = 0 if flag_max_demand_constr==.
tab month flag_peak_demand_constr, missing // missing for winter bills where NO bills starting in that month extended into summer
replace flag_peak_demand_constr = 0 if flag_peak_demand_constr==.
tab month flag_partpeak_demand_constr, missing
assert flag_partpeak_demand_constr!=.
drop month

** Resolve rates with multiple groups, using within-SA correlations
egen temp_group_sd = sd(group), by(sa_uuid rt_sched_cd) 
tab rt_sched_cd group if temp_group_sd==0 | temp_group_sd==.
tab rt_sched_cd group if temp_group_sd>0 & temp_group_sd!=.
preserve
keep if temp_group_sd>0 & temp_group_sd!=. // keep only SA-rates with groups that need resolving
sort sa_uuid rt_sched_cd group bill_start_dt
egen temp_corr_group = corr(total_bill_amount total_bill_amount_constr), ///
	by(sa_uuid rt_sched_cd group)
egen temp_corr_group_max = max(temp_corr_group), by(sa_uuid rt_sched_cd)
egen temp_corr_group_min = min(temp_corr_group), by(sa_uuid rt_sched_cd)
gen temp_corr_diff = temp_corr_group_max - temp_corr_group_min
egen temp_tag = tag(sa_uuid rt_sched_cd)
sum temp_corr_diff if temp_tag==1, detail

	// Keep the group with the highest correlation coefficient
unique sa_uuid bill_start_dt rt_sched 
local uniq = r(unique)
drop if temp_corr_group<temp_corr_group_max
unique sa_uuid bill_start_dt rt_sched
assert `uniq'==r(unique)

	// Resolve remaining dups by randomly picking a group
duplicates t sa_uuid bill_start_dt rt_sched, gen(temp_dup)
br sa_uuid bill_start_dt rt_sched group total_bill_kwh total_bill_amount* temp_corr* if temp_dup>0	
	// the vast majority are accounts with (close to) zero consumption
gen temp_random = uniform()	
egen temp_random_max = max(temp_random), by(sa_uuid rt_sched)
gen temp_random_group = group if temp_random==temp_random_max
egen temp_random_group_max = max(temp_random_group), by(sa_uuid rt_sched)
assert temp_random_group_max!=.
unique sa_uuid bill_start_dt rt_sched 
local uniq = r(unique)
drop if temp_dup>0 & group!=temp_random_group_max
unique sa_uuid bill_start_dt rt_sched
assert `uniq'==r(unique)
assert r(unique)==r(N)
	// Save IDs as temp file to merge back into main dataset
keep sa_uuid bill_start_dt rt_sched_cd group
tempfile groups
save `groups'	
restore

	// Merge in tempfile and drop dups
merge 1:1 sa_uuid bill_start_dt rt_sched_cd group using `groups'
assert _merge!=2
egen temp_merge_max = max(_merge), by(sa_uuid bill_start_dt rt_sched_cd)
unique sa_uuid bill_start_dt rt_sched
local uniq = r(unique)
drop if temp_merge_max==3 & _merge==1
unique sa_uuid bill_start_dt rt_sched
assert `uniq'==r(unique)
assert r(unique)==r(N)
drop _merge temp*

** Merge in other variables from billing data
merge 1:1 sa_uuid bill_start_dt using "$dirpath_data/pge_cleaned/billing_data_20180719.dta"
assert _merge!=1 if pull=="20180719"
assert _merge==3 if  pull=="20180719" & flag_interval_merge==1 & ///
	regexm(rt_sched_cd,"AG")==1 & regexm(rt_sched_cd,"AGICE")==0
drop if _merge==2 // keep only bills with interval data (for this merge file)
drop _merge

merge 1:1 sa_uuid bill_start_dt using "$dirpath_data/pge_cleaned/billing_data_20180322.dta", update
assert _merge!=1 if pull=="20180322"
assert _merge>=3 if  pull=="20180322" & flag_interval_merge==1 & ///
	regexm(rt_sched_cd,"AG")==1 & regexm(rt_sched_cd,"AGICE")==0
drop if _merge==2 // keep only bills with interval data (for this merge file)
drop _merge

unique sa_uuid bill_start_dt
assert r(unique)==r(N)

** Diagnostics
gen temp_bad_bill_match = total_bill_amount==. | total_bill_amount_constr==. | ///
	total_bill_amount/total_bill_amount_constr > 5 | ///
	total_bill_amount_constr/total_bill_amount > 5 
tab temp_bad_bill_match // 3.37% (not great, not terrible)
tab temp_bad_bill_match if flag_interval_merge==1 & flag_interval_disp20==0 // 2.98%
tab temp_bad_bill_match if flag_interval_merge==1 & flag_interval_disp20==0 & ///
	flag_nem==0 // 2.93%
tab temp_bad_bill_match if flag_interval_merge==1 & flag_interval_disp20==0 & ///
	flag_nem==0 & flag_bad_tariff==0 // 2.93% (these basically all got dropped in interval merge)
tab temp_bad_bill_match if flag_interval_merge==1 & flag_interval_disp20==0 & ///
	flag_nem==0 & flag_bad_tariff==0 & flag_multi_tariff==0 // 2.94% (5x threshold too big to matter here)
tab temp_bad_bill_match if flag_interval_merge==1 & flag_interval_disp20==0 & ///
	flag_nem==0 & flag_bad_tariff==0 & flag_multi_tariff==0 & flag_dup_bad_kwh==0 & ///
	flag_dup_double_overlap==0 & flag_dup_partial_overlap==0 & flag_dup_overlap_missing==0 
	// 2.94% (very few obs meet these added criteria after interval merge)
tab temp_bad_bill_match if flag_interval_merge==1 & flag_interval_disp20==0 & ///
	flag_nem==0 & flag_bad_tariff==0 & flag_multi_tariff==0 & flag_dup_bad_kwh==0 & ///
	flag_dup_double_overlap==0 & flag_dup_partial_overlap==0 & flag_dup_overlap_missing==0 & ///
	flag_max_demand_constr+flag_peak_demand_const+flag_partpeak_demand_constr==0 // 3.62% (this is surprising!)
tab temp_bad_bill_match if flag_interval_merge==1 & flag_interval_disp20==0 & ///
	flag_nem==0 & flag_bad_tariff==0 & flag_multi_tariff==0 & flag_dup_bad_kwh==0 & ///
	flag_dup_double_overlap==0 & flag_dup_partial_overlap==0 & flag_dup_overlap_missing==0 & ///
	flag_max_demand_constr+flag_peak_demand_const+flag_partpeak_demand_constr==0 & ///
	flag_short_bill==0 & flag_long_bill==0 // 3.56% (not much difference)
tab temp_bad_bill_match if flag_interval_merge==1 & flag_interval_disp20==0 & ///
	flag_nem==0 & flag_bad_tariff==0 & flag_multi_tariff==0 & flag_dup_bad_kwh==0 & ///
	flag_dup_double_overlap==0 & flag_dup_partial_overlap==0 & flag_dup_overlap_missing==0 & ///
	flag_max_demand_constr+flag_peak_demand_const+flag_partpeak_demand_constr==0 & ///
	flag_short_bill==0 & flag_long_bill==0 & flag_first_bill==0 & flag_last_bill==0 
	// 3.56% (not much difference)
tab temp_bad_bill_match if flag_interval_merge==1 & flag_interval_disp20==0 & ///
	flag_nem==0 & flag_bad_tariff==0 & flag_multi_tariff==0 & flag_dup_bad_kwh ==0 & ///
	flag_dup_double_overlap==0 & flag_dup_partial_overlap==0 & flag_dup_overlap_missing==0 & ///
	flag_max_demand_constr+flag_peak_demand_const+flag_partpeak_demand_constr==0 & ///
	flag_short_bill==0 & flag_long_bill==0 & flag_first_bill==0 & flag_last_bill==0 & ///
	total_bill_kwh>0 & total_bill_kwh!=. // 1.09% (zero kWh bills are most of the problem!)

gen temp_regular_bill = flag_interval_merge==1 & flag_interval_disp20==0 & ///
	flag_nem==0 & flag_bad_tariff==0 & flag_multi_tariff==0 & flag_dup_bad_kwh==0 & ///
	flag_dup_double_overlap==0 & flag_dup_partial_overlap==0 & flag_dup_overlap_missing==0 & ///
	flag_max_demand_constr+flag_peak_demand_const+flag_partpeak_demand_constr==0 & ///
	flag_short_bill==0 & flag_long_bill==0 & flag_first_bill==0 & flag_last_bill==0 
tab temp_regular_bill // 67.6% have no reason to suspect a problem
gen temp_year = year(bill_start_dt)
tabstat temp_bad_bill_match, by(temp_year) 
tabstat temp_bad_bill_match if temp_regular_bill==1, by(temp_year) 
tabstat temp_bad_bill_match, by(rt_sched_cd) s(mean count) 
tabstat temp_bad_bill_match if temp_regular_bill==1, by(rt_sched_cd) s(mean count)
tabstat temp_bad_bill_match if temp_regular_bill==1 & total_bill_kwh>0, by(rt_sched_cd) s(mean count)
	
/*
twoway (scatter total_bill_amount_constr  total_bill_amount if temp_regular_bill==1 & total_bill_kwh!=0 ///
			& total_bill_kwh!=., msize(tiny)) ///
	(line total_bill_amount total_bill_amount if temp_regular_bill==1 & total_bill_kwh!=0 & total_bill_kwh!=.)
*/

gen temp_regular_bill2 = flag_interval_merge==1 & flag_interval_disp20==0  & ///
	flag_nem==0 & flag_bad_tariff==0 & flag_multi_tariff==0 & flag_dup_bad_kwh ==0 & ///
	flag_dup_double_overlap==0 & flag_dup_partial_overlap==0 & flag_dup_overlap_missing==0 & ///
	flag_short_bill==0 & flag_long_bill==0 & flag_first_bill==0 & flag_last_bill==0 
	// same as the other flag, but allowing for constructed max/peak/partpeak demand
	
/*
levelsof rt_sched_cd, local(levs)
foreach rt in `levs' {
	twoway (scatter total_bill_amount_constr total_bill_amount ///
			if temp_regular_bill==1 & total_bill_kwh!=0 & total_bill_kwh!=. & rt_sched_cd=="`rt'" ///
			& total_bill_amount<=10000 ///
			, msize(tiny) ///
			) ///
		(line total_bill_amount total_bill_amount if temp_regular_bill==1 & total_bill_kwh!=0 & total_bill_kwh!=. ///
			& rt_sched_cd=="`rt'" ///
			& total_bill_amount<=10000 ///
			, msize(tiny) ///
			), ///
		title("`rt'")	
	sleep 5000 // the problem is "AG-4B"!!
}


correlate total_bill_amount_constr total_bill_amount if ///
	temp_regular_bill2==1 & total_bill_kwh!=0 & total_bill_kwh!=. & rt_sched_cd!="AG-4B"
correlate total_bill_amount_constr total_bill_amount if ///
	temp_regular_bill==1 & total_bill_kwh!=0 & total_bill_kwh!=. & rt_sched_cd!="AG-4B"
local corr = string(r(rho),"%9.3f")	
twoway (scatter total_bill_amount_constr total_bill_amount if temp_regular_bill==1 & total_bill_kwh!=0 ///
		& total_bill_kwh!=. & rt_sched_cd!="AG-4B", msize(vsmall)) ///
	(line total_bill_amount total_bill_amount if temp_regular_bill==1 & total_bill_kwh!=0 & total_bill_kwh!=. ///
		& rt_sched_cd!="AG-4B", msize(tiny)), ///
	title("All rates except AG-4B (rho = `corr')") ytitle("Constructed bill amount ($)") legend(off)

correlate total_bill_amount_constr total_bill_amount if ///
	temp_regular_bill2==1 & total_bill_kwh!=0 & total_bill_kwh!=. & rt_sched_cd!="AG-4B" & total_bill_amount<60000 & total_bill_amount_constr<60000
correlate total_bill_amount_constr total_bill_amount if ///
	temp_regular_bill==1 & total_bill_kwh!=0 & total_bill_kwh!=. & rt_sched_cd!="AG-4B" & total_bill_amount<60000 & total_bill_amount_constr<60000
local corr = string(r(rho),"%9.3f")	
twoway (scatter total_bill_amount_constr total_bill_amount if temp_regular_bill==1 & total_bill_kwh!=0 ///
		& total_bill_kwh!=. & rt_sched_cd!="AG-4B" & total_bill_amount<60000 & total_bill_amount_constr<60000, msize(vsmall)) ///
	(line total_bill_amount total_bill_amount if temp_regular_bill==1 & total_bill_kwh!=0 & total_bill_kwh!=. ///
		& rt_sched_cd!="AG-4B" & total_bill_amount<60000, msize(tiny)), ///
	title("All rates except AG-4B (rho = `corr')") ytitle("Constructed bill amount ($)") legend(off)
 
correlate total_bill_amount_constr total_bill_amount if ///
	temp_regular_bill2==1 & total_bill_kwh!=0 & total_bill_kwh!=. & rt_sched_cd=="AG-4B"
correlate total_bill_amount_constr total_bill_amount if ///
	temp_regular_bill==1 & total_bill_kwh!=0 & total_bill_kwh!=. & rt_sched_cd=="AG-4B"
local corr = string(r(rho),"%9.3f")	
twoway (scatter total_bill_amount_constr total_bill_amount ///
		if temp_regular_bill==1 & total_bill_kwh!=0 & total_bill_kwh!=. & rt_sched_cd=="AG-4B", msize(vsmall)) ///
	(line total_bill_amount total_bill_amount if temp_regular_bill==1 & total_bill_kwh!=0 & total_bill_kwh!=. ///
		& rt_sched_cd=="AG-4B", msize(tiny)), ///
	title("Rate AG-4B (rho = `corr')") ytitle("Constructed bill amount ($)") legend(off)	
 
forvalues y = 2011/2017 {
	twoway (scatter total_bill_amount_constr total_bill_amount ///
			if temp_regular_bill==1 & total_bill_kwh!=0 & total_bill_kwh!=. & rt_sched_cd=="AG-4B" ///
			& year(bill_start_dt)==`y', msize(vsmall)) ///
		/// (line total_bill_amount total_bill_amount if temp_regular_bill==1 & total_bill_kwh!=0 & total_bill_kwh!=. ///
		///	& rt_sched_cd=="AG-4B" & year(bill_start_dt==`y'), msize(vsmall))///
		, ///
		title("AG-4B, Year = `y'")	
	sleep 5000 
}

twoway (scatter total_bill_amount_constr total_bill_amount ///
		if temp_regular_bill==1 & total_bill_kwh!=0 & total_bill_kwh!=. & rt_sched_cd=="AG-4B" ///
		& inlist(month(bill_start_dt),5,6,7,8,9,10) & inrange(year(bill_start_dt),2011,2015), msize(vsmall)) ///
		(line total_bill_amount total_bill_amount if temp_regular_bill==1 & total_bill_kwh!=0 & total_bill_kwh!=. ///
		& rt_sched_cd=="AG-4B" && inrange(year(bill_start_dt),2011,2015), msize(vsmall))///
	, ///
	title("AG-4B, Summer")	
	
twoway (scatter total_bill_amount_constr total_bill_amount ///
		if temp_regular_bill==1 & total_bill_kwh!=0 & total_bill_kwh!=. & rt_sched_cd=="AG-4B" ///
		& inlist(month(bill_start_dt),11,12,1,2) & inrange(year(bill_start_dt),2011,2015), msize(vsmall)) ///
		(line total_bill_amount total_bill_amount if temp_regular_bill==1 & total_bill_kwh!=0 & total_bill_kwh!=. ///
		& rt_sched_cd=="AG-4B" & inrange(year(bill_start_dt),2011,2015), msize(vsmall)) ///
	, ///
	title("AG-4B, Winter")	

twoway (scatter total_bill_amount_constr total_bill_amount ///
		if temp_regular_bill==1 & total_bill_kwh!=0 & total_bill_kwh!=. & rt_sched_cd=="AG-4B" ///
		& inlist(month(bill_start_dt),3,4) & inrange(year(bill_start_dt),2016,2016), msize(vsmall)) ///
		(line total_bill_amount total_bill_amount if temp_regular_bill==1 & total_bill_kwh!=0 & total_bill_kwh!=. ///
		& rt_sched_cd=="AG-4B" & inlist(month(bill_start_dt),3,4) & inrange(year(bill_start_dt),2016,2016), msize(vsmall)) ///
	, ///
	title("AG-4B, March-April")	

preserve
gen temp_modate = ym(year(bill_start_dt),month(bill_start_dt))
gen temp_month = month(bill_start_dt)
gen temp_in_corr = temp_regular_bill2==1 & total_bill_kwh!=0 & total_bill_kwh!=. & rt_sched_cd=="AG-4D"
drop if temp_in_corr==0
*gsort temp_modate total_bill_amount_constr total_bill_amount
*egen temp_corr = corr(total_bill_amount_constr total_bill_amount), by(temp_modate)
*tabstat temp_corr, by(temp_modate) s(mean n)
gsort temp_month total_bill_amount_constr total_bill_amount
egen temp_corr = corr(total_bill_amount_constr total_bill_amount), by(temp_month)
tabstat temp_corr, by(temp_month) s(mean n)
restore	
	
gen month = month(bill_start_dt)
tab rt_sched month
tab rt_sched monthif temp_reglar_bill==1

tab rt_sched month if flag_partpeak-demand_constr==1 // partial_peak_demand only missing for 4C, 4F, 5C, 5F
tab flag_partpeak_demand_constr month if inlist(rt_sched,"AG-4C","AG-4F","AG-5C","AG-5F") // very often missing

tab rt_sched month if flag_peak_demand_constr==1 // peak_demand missing for lots!
tab flag_peak_demand_constr month if inlist(rt_sched,"AG-4B","AG-5B","AG-RB","AG-VB") // very often missing
tab flag_peak_demand_constr month if inlist(rt_sched,"AG-4E","AG-5E","AG-RE","AG-VE") // very often missing
tab flag_peak_demand_constr month if inlist(rt_sched,"AG-4C","AG-4F","AG-5C","AG-5F") // very often missing

*/



 
** Create flag for non-zero bills that are off by more than 100% 
gen flag_bill_constr_error100 = total_bill_kwh>0 & total_bill_kwh!=. & ///
	(total_bill_amount/total_bill_amount_constr>2 | total_bill_amount_constr/total_bill_amount>2)
tab flag_bill_constr_error100
tab flag_bill_constr_error100 if temp_regular_bill==1
tab flag_bill_constr_error100 if temp_regular_bill==1 & rt_sched_cd!="AG_4B" 
la var flag_bill_constr_error100 "Flag = 1 for non-zero kWh bills with 100% discrepancy btw $ and $hat"

** Save
drop temp*
sort sa_uuid bill_start_dt
unique sa_uuid bill_start_dt
assert r(unique)==r(N)
compress
save "$dirpath_data/merged/bills_rates_constructed.dta", replace
 
}

*******************************************************************************
*******************************************************************************
START HERE
** 5. Remove unmatched rate groups from hourly dataset
{
use "$dirpath_data/merged/bills_rates_constructed.dta", clear
keep sa_uuid bill_start_dt group
merge 1:m sa_uuid bill_start_dt group using "$dirpath_data/merged/hourly_with_prices.dta"
assert _merge!=1
unique sa_uuid date hour
local uniq = r(unique)
drop if _merge==2
drop _merge
unique sa_uuid date hour
assert r(unique)==`uniq'
compress
duplicates drop sa_uuid date hour, force // for some reason there are still a few dups?
save "$dirpath_data/merged/hourly_with_prices.dta", replace
}

*******************************************************************************
*******************************************************************************

