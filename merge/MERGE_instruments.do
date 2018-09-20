clear all
version 13
set more off

************************************************************************
**** Script to create datasets of instruments for electricity price ****
************************************************************************


global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"


*******************************************************************************
*******************************************************************************

** 1. Prepare E-1 residential rates (daily/monthly)
if 1==1{

** Load rates
use "$dirpath_data/pge_cleaned/e1_rates.dta", clear

** Create variables for low/middle/high prices
gen p_kwh_e1_lo = energycharge_tier1
gen p_kwh_e1_mi = energycharge_tier3 if energycharge_tier5!=.
replace p_kwh_e1_mi = energycharge_tier2 if energycharge_tier5==.
gen p_kwh_e1_hi = energycharge_tier5
replace p_kwh_e1_hi = energycharge_tier3 if energycharge_tier5==.

** Keep only dates and marginal prices
keep rate_start_date rate_end_date p_kwh_e1_*

** Expand to the daily level
gen rate_length = rate_end_date-rate_start_date+1
gen year = year(rate_start_date)
assert year==2018 if rate_length==. // all rates starting in 2018 which are after our sample
drop if year==2018 
expand rate_length, gen(temp_new)
sort rate_start_date rate_end_date temp_new

** Construct date variable (duplicated at each rate change-over)
gen date = rate_start_date if temp_new==0
format %td date
replace date = date[_n-1]+1 if temp_new==1
assert date==rate_start_date if temp_new==0
assert date==rate_end_date if temp_new[_n+1]==0
assert date!=.
unique date
assert r(unique)==r(N)
order date, after(rate_end_date)
keep date p_kwh_e1*

** Label
la var date "Date"
la var p_kwh_e1_lo "E1 marg price ($/kWh), lowest tier" 
la var p_kwh_e1_mi "E1 marg price ($/kWh), middle tier" 
la var p_kwh_e1_hi "E1 marg price ($/kWh), highest tier" 

** Fill date gap
assert p_kwh_e1_lo!=. & p_kwh_e1_mi!=. & p_kwh_e1_hi!=.
tsset date
tsfill
sort date
replace p_kwh_e1_lo = p_kwh_e1_lo[_n-1] if p_kwh_e1_lo==.
replace p_kwh_e1_mi = p_kwh_e1_mi[_n-1] if p_kwh_e1_mi==.
replace p_kwh_e1_hi = p_kwh_e1_hi[_n-1] if p_kwh_e1_hi==.
assert p_kwh_e1_lo!=. & p_kwh_e1_mi!=. & p_kwh_e1_hi!=.

** Save
sort date
compress
save "$dirpath_data/merged/e1_prices_daily.dta", replace

** Collapse to monthly level and save
gen modate = ym(year(date),month(date))
format %tm modate
drop date
foreach v of varlist p_kwh_e1_* {
	egen double temp = mean(`v'), by(modate)
	replace `v' = temp
	drop temp
}
duplicates drop 
unique modate
assert r(unique)==r(N)
order modate
sort modate
compress
save "$dirpath_data/merged/e1_prices_monthly.dta", replace

}

*******************************************************************************
*******************************************************************************

** 2. Prepare E-20 industrial rates (hourly)
if 1==1{

** Load rates
use "$dirpath_data/pge_cleaned/e20_rates.dta", clear

** Drop rate components I'm not using
drop demandcharge customercharge averageratelim powerfactoradj pdp2credits ufrcredit
duplicates drop

**Eliminate duplicates
egen double temp = min(pdpenergycredit), by(rateschedule-peak)
replace pdpenergycredit = temp
drop temp
duplicates drop
unique rateschedule-peak
assert r(unique)==r(N)
unique rateschedule-minute
assert r(unique)==r(N)

** Collapse from 30 min to 1 hour (which is the resolution of our AMI data)
foreach v of varlist offpeak partpeak peak {
	egen temp = max(`v'), by(rateschedule-hour)
	replace `v' = temp
	drop temp
}
foreach v of varlist energycharge pdp1charges pdpenergycredit {
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
sort rateschedule date hour	
br rateschedule rate_start_date rate_end_date date hour temp_new
		
** Confirm uniqueness of dates
unique rateschedule date hour	
assert r(unique)==r(N)
drop rate_start_date rate_end_date rate_length year temp_new*

** Merge in Event Days
merge m:1 date using "$dirpath_data/pge_cleaned/event_days.dta"
assert _merge!=2
drop _merge

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
	by(rateschedule date)
egen double temp2 = mean(temp1) if temp_holiday==1, by(rateschedule date)
egen double temp3 = sd(energycharge) if offpeak==1 & partpeak==0 & peak==0 & temp_holiday==1, ///
	by(rateschedule date)
assert round(temp3,1e-6)==0 | temp3==. // confirm no variation in offpeak price within day/rate/group
assert temp2!=. if temp_holiday==1

	// Assign offpeak prices for all hours of all holidays
replace energycharge = temp2 if temp_holiday==1 & temp2!=.
replace offpeak = 1 if temp_holiday==1
replace partpeak = 0 if temp_holiday==1
replace peak = 0 if temp_holiday==1

	// Confirm that holidays never coincide with Event Days
assert event_day_biz==. & event_day_res==. if temp_holiday==1	

	// Clean up
rename temp_holiday holiday
la var holiday "Indicator for observed holiday (no peaks or partpeaks)"	
drop temp*	

** Assign hourly (average) marginal price 
gen p_kwh_e20 = energycharge
assert pdpenergycredit<=0 & pdp1charges!=. & pdp1charges>=0
replace p_kwh_e20 = pdp1charges + pdpenergycredit if pdp1charges!=0 & inlist(hour,14,15,16,17) & ///
	year(date)>=2013 & event_day_biz==1 // 4-hour event windows 2013-2017, based on "business" Event Days
replace p_kwh_e20 = pdp1charges + pdpenergycredit if pdp1charges!=0 & inlist(hour,14,15,16,17) & ///
	year(date)<2013 & event_day_res==1 // 4-hour event windows 2013-2017, based on "residential" Event Days
keep date hour p_kwh_e20

** Confirm no gaps in time series	
egen date_hour = group(date hour)
tsset date_hour
tsfill
assert p_kwh_e20!=.
drop date_hour
	
** Label, and save
la var date "Date"
la var hour "Hour of day"
la var p_kwh_e20 "E20 hourly marg price ($/kWh)"
sort date hour
unique date hour
assert r(unique)==r(N)
compress
save "$dirpath_data/merged/e20_prices_hourly.dta", replace
		
}

*******************************************************************************
*******************************************************************************

** 3. Prepare E-20 industrial rates (monthly)
if 1==1{

** Load rates
use "$dirpath_data/pge_cleaned/e20_rates.dta", clear

** Drop rate components I'm not using
drop demandcharge customercharge averageratelim powerfactoradj pdp2credits ufrcredit
duplicates drop

**Eliminate duplicates
egen double temp = min(pdpenergycredit), by(rateschedule-peak)
replace pdpenergycredit = temp
drop temp
duplicates drop
unique rateschedule-peak
assert r(unique)==r(N)
unique rateschedule-minute
assert r(unique)==r(N)

** Collapse from 30 min to 1 hour (which is the resolution of our AMI data)
foreach v of varlist offpeak partpeak peak {
	egen temp = max(`v'), by(rateschedule-hour)
	replace `v' = temp
	drop temp
}
foreach v of varlist energycharge pdp1charges pdpenergycredit {
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
sort rateschedule date hour	
br rateschedule rate_start_date rate_end_date date hour temp_new
		
** Confirm uniqueness of dates
unique rateschedule date hour	
assert r(unique)==r(N)
drop rate_start_date rate_end_date rate_length year temp_new*

** Merge in Event Days
merge m:1 date using "$dirpath_data/pge_cleaned/event_days.dta"
assert _merge!=2
drop _merge

** Fix holidays so all hours are off-peak (date holiday is observed!)
	// New Year's Day, President's Day, Memorial Day, Independence Day, Labor Day, 
	// Veterans Day, Thanksgiving Day, and Christmas Day (list of holidays from PGE)
gen temp_holiday = 0

	// New Year's Day
replace temp_holiday = 1 if date==date("01jan2008","DMY")
replace temp_holiday = 1 if date==date("01jan2009","DMY")
replace temp_holiday = 1 if date==date("01jan2010","DMY")
replace temp_holiday = 1 if date==date("31jan2010","DMY")
replace temp_holiday = 1 if date==date("02jan2012","DMY")
replace temp_holiday = 1 if date==date("01jan2013","DMY")
replace temp_holiday = 1 if date==date("01jan2014","DMY")
replace temp_holiday = 1 if date==date("01jan2015","DMY")
replace temp_holiday = 1 if date==date("01jan2016","DMY")
replace temp_holiday = 1 if date==date("02jan2017","DMY")

	// President's Day
replace temp_holiday = 1 if date==date("18feb2008","DMY")
replace temp_holiday = 1 if date==date("16feb2009","DMY")
replace temp_holiday = 1 if date==date("15feb2010","DMY")
replace temp_holiday = 1 if date==date("21feb2011","DMY")
replace temp_holiday = 1 if date==date("20feb2012","DMY")
replace temp_holiday = 1 if date==date("18feb2013","DMY")
replace temp_holiday = 1 if date==date("17feb2014","DMY")
replace temp_holiday = 1 if date==date("16feb2015","DMY")
replace temp_holiday = 1 if date==date("15feb2016","DMY")
replace temp_holiday = 1 if date==date("20feb2017","DMY")

	// Memorial Day
replace temp_holiday = 1 if date==date("26may2008","DMY")
replace temp_holiday = 1 if date==date("25may2009","DMY")
replace temp_holiday = 1 if date==date("31may2010","DMY")
replace temp_holiday = 1 if date==date("30may2011","DMY")
replace temp_holiday = 1 if date==date("28may2012","DMY")
replace temp_holiday = 1 if date==date("27may2013","DMY")
replace temp_holiday = 1 if date==date("26may2014","DMY")
replace temp_holiday = 1 if date==date("25may2015","DMY")
replace temp_holiday = 1 if date==date("30may2016","DMY")
replace temp_holiday = 1 if date==date("29may2017","DMY")

	// Independence Day
replace temp_holiday = 1 if date==date("04jul2008","DMY")
replace temp_holiday = 1 if date==date("03jul2009","DMY")
replace temp_holiday = 1 if date==date("04jul2010","DMY")
replace temp_holiday = 1 if date==date("04jul2011","DMY")
replace temp_holiday = 1 if date==date("04jul2012","DMY")
replace temp_holiday = 1 if date==date("04jul2013","DMY")
replace temp_holiday = 1 if date==date("04jul2014","DMY")
replace temp_holiday = 1 if date==date("03jul2015","DMY")
replace temp_holiday = 1 if date==date("04jul2016","DMY")
replace temp_holiday = 1 if date==date("04jul2017","DMY")

	// Labor Day
replace temp_holiday = 1 if date==date("01sep2008","DMY")
replace temp_holiday = 1 if date==date("07sep2009","DMY")
replace temp_holiday = 1 if date==date("06sep2010","DMY")
replace temp_holiday = 1 if date==date("05sep2011","DMY")
replace temp_holiday = 1 if date==date("03sep2012","DMY")
replace temp_holiday = 1 if date==date("02sep2013","DMY")
replace temp_holiday = 1 if date==date("01sep2014","DMY")
replace temp_holiday = 1 if date==date("07sep2015","DMY")
replace temp_holiday = 1 if date==date("05sep2016","DMY")
replace temp_holiday = 1 if date==date("04sep2017","DMY")

	// Veteran's Day
replace temp_holiday = 1 if date==date("11nov2008","DMY")
replace temp_holiday = 1 if date==date("11nov2009","DMY")
replace temp_holiday = 1 if date==date("11nov2010","DMY")
replace temp_holiday = 1 if date==date("11nov2011","DMY")
replace temp_holiday = 1 if date==date("11nov2012","DMY")
replace temp_holiday = 1 if date==date("11nov2013","DMY")
replace temp_holiday = 1 if date==date("11nov2014","DMY")
replace temp_holiday = 1 if date==date("11nov2015","DMY")
replace temp_holiday = 1 if date==date("11nov2016","DMY")
replace temp_holiday = 1 if date==date("10nov2017","DMY")

	// Thanksgiving Day
replace temp_holiday = 1 if date==date("27nov2008","DMY")
replace temp_holiday = 1 if date==date("26nov2009","DMY")
replace temp_holiday = 1 if date==date("25nov2010","DMY")
replace temp_holiday = 1 if date==date("24nov2011","DMY")
replace temp_holiday = 1 if date==date("22nov2012","DMY")
replace temp_holiday = 1 if date==date("28nov2013","DMY")
replace temp_holiday = 1 if date==date("27nov2014","DMY")
replace temp_holiday = 1 if date==date("26nov2015","DMY")
replace temp_holiday = 1 if date==date("24nov2016","DMY")
replace temp_holiday = 1 if date==date("23nov2017","DMY")

	// Christmas Day
replace temp_holiday = 1 if date==date("25dec2008","DMY")
replace temp_holiday = 1 if date==date("25dec2009","DMY")
replace temp_holiday = 1 if date==date("24dec2010","DMY")
replace temp_holiday = 1 if date==date("26dec2011","DMY")
replace temp_holiday = 1 if date==date("25dec2012","DMY")
replace temp_holiday = 1 if date==date("25dec2013","DMY")
replace temp_holiday = 1 if date==date("25dec2014","DMY")
replace temp_holiday = 1 if date==date("25dec2015","DMY")
replace temp_holiday = 1 if date==date("26dec2016","DMY")
replace temp_holiday = 1 if date==date("25dec2017","DMY")

	// Calculate offpeak price per kWh for the day of each holiday
egen double temp1 = mean(energycharge) if offpeak==1 & partpeak==0 & peak==0 & temp_holiday==1, ///
	by(rateschedule date)
egen double temp2 = mean(temp1) if temp_holiday==1, by(rateschedule date)
egen double temp3 = sd(energycharge) if offpeak==1 & partpeak==0 & peak==0 & temp_holiday==1, ///
	by(rateschedule date)
assert round(temp3,1e-6)==0 | temp3==. // confirm no variation in offpeak price within day/rate/group
assert temp2!=. if temp_holiday==1

	// Assign offpeak prices for all hours of all holidays
replace energycharge = temp2 if temp_holiday==1 & temp2!=.
replace offpeak = 1 if temp_holiday==1
replace partpeak = 0 if temp_holiday==1
replace peak = 0 if temp_holiday==1

	// Confirm that holidays never coincide with Event Days
assert event_day_biz==. & event_day_res==. if temp_holiday==1	

	// Clean up
rename temp_holiday holiday
la var holiday "Indicator for observed holiday (no peaks or partpeaks)"	
drop temp*	

** Assign hourly (average) marginal price 
gen p_kwh_e20 = energycharge
assert pdpenergycredit<=0 & pdp1charges!=. & pdp1charges>=0
replace p_kwh_e20 = pdp1charges + pdpenergycredit if pdp1charges!=0 & inlist(hour,14,15,16,17) & ///
	year(date)>=2013 & event_day_biz==1 // 4-hour event windows 2013-2017, based on "business" Event Days
replace p_kwh_e20 = pdp1charges + pdpenergycredit if pdp1charges!=0 & inlist(hour,14,15,16,17) & ///
	year(date)<2013 & event_day_res==1 // 4-hour event windows 2013-2017, based on "residential" Event Days

** Confirm no gaps in time series	
egen date_hour = group(date hour)
tsset date_hour
tsfill
assert p_kwh_e20!=.
drop date_hour

** Collapse to monthly level (TAKING UNWEIGHTED AVERAGES OF HOURS!)
gen modate = ym(year(date),month(date))
format %tm modate
egen double mean_p_kwh_e20 = mean(p_kwh_e20), by(rateschedule modate)
egen double min_p_kwh_e20 = min(p_kwh_e20), by(rateschedule modate)
egen double max_p_kwh_e20 = max(p_kwh_e20), by(rateschedule modate)
keep modate mean_p_kwh_e20 min_p_kwh_e20 max_p_kwh_e20
duplicates drop 

** Label, and save
la var modate "Year-Month"
la var mean_p_kwh_e20 "E20 avg marg price ($/kWh)"
la var min_p_kwh_e20 "E20 min marg price ($/kWh)"
la var max_p_kwh_e20 "E20 max marg price ($/kWh)"
sort modate
unique modate
assert r(unique)==r(N)
compress
save "$dirpath_data/merged/e20_prices_monthly.dta", replace
		
}

*******************************************************************************
*******************************************************************************

** 4. Prepare default ag rates (hourly)
if 1==1{

** Start with list of ag rates
use "$dirpath_data/merged/ag_rates_for_merge.dta", clear
keep rt_sched_cd
duplicates drop
sort rt_sched_cd

** Restriction on rate switches
/*

-- AG-1 customers are a distinct group

-- AG-ICE customers are a distinct group

-- AG-4/AG-5/AG-R/AG-V customers can switch between these 4 types of rates

-- A/D customers are small, while B/C/E/F customers are large

-- D/E/F customers are on dumb meters, while A/B/C customers are on smart meters,
   and we can probably assume that PGE meter replacement timing is exogenous

-- C & F are the PDP variants of B & E
 
*/

** Assign the "default" rate for each rate
gen rt_default = ""

replace rt_default = "AG-1A" if rt_sched_cd=="AG-1A"

replace rt_default = "AG-1B" if rt_sched_cd=="AG-1B"

replace rt_default = "AG-ICE" if rt_sched_cd=="AG-ICE"

replace rt_default = "AG-4A" if rt_sched_cd=="AG-4A"
replace rt_default = "AG-4A" if rt_sched_cd=="AG-5A"
replace rt_default = "AG-4A" if rt_sched_cd=="AG-RA"
replace rt_default = "AG-4A" if rt_sched_cd=="AG-VA"

replace rt_default = "AG-4D" if rt_sched_cd=="AG-4D"
replace rt_default = "AG-4D" if rt_sched_cd=="AG-5D"
replace rt_default = "AG-4D" if rt_sched_cd=="AG-RD"
replace rt_default = "AG-4D" if rt_sched_cd=="AG-VD"

replace rt_default = "AG-4B" if rt_sched_cd=="AG-4B"
replace rt_default = "AG-4B" if rt_sched_cd=="AG-4C"
replace rt_default = "AG-4B" if rt_sched_cd=="AG-5B"
replace rt_default = "AG-4B" if rt_sched_cd=="AG-5C"
replace rt_default = "AG-4B" if rt_sched_cd=="AG-RB"
replace rt_default = "AG-4B" if rt_sched_cd=="AG-VB"

replace rt_default = "AG-4E" if rt_sched_cd=="AG-4E"
replace rt_default = "AG-4E" if rt_sched_cd=="AG-4F"
replace rt_default = "AG-4E" if rt_sched_cd=="AG-5E"
replace rt_default = "AG-4E" if rt_sched_cd=="AG-5F"
replace rt_default = "AG-4E" if rt_sched_cd=="AG-RE"
replace rt_default = "AG-4E" if rt_sched_cd=="AG-VE"

** Save temp file
tempfile rt_default
save `rt_default'

** Construct dataset of default rates at the hourly level

	// Isolate only the rates serving as a default
use "$dirpath_data/merged/ag_rates_for_merge.dta", clear
rename rt_sched_cd rt_default
joinby rt_default using `rt_default'
drop rt_sched_cd tou group
duplicates drop
unique rt_default date hour
assert r(unique)==r(N)

	// Construct marginal price variable
gen p_kwh_ag_default = energycharge
assert pdpenergycredit<=0 & pdpcharge!=. & pdpcharge>=0
replace p_kwh_ag_default = pdpcharge + pdpenergycredit if pdpcharge!=0 & inlist(hour,14,15,16,17) & ///
	year(date)>=2013 & event_day_biz==1 // 4-hour event windows 2013-2017, based on "business" Event Days
replace p_kwh_ag_default = pdpcharge + pdpenergycredit if pdpcharge!=0 & inlist(hour,14,15,16,17) & ///
	year(date)<2013 & event_day_res==1 // 4-hour event windows 2013-2017, based on "residential" Event Days
assert p_kwh_ag_default!=.	

	// Clean up and label
keep rt_default date hour p_kwh_ag_default
la var rt_default "Default ag tariff for customer group"
la var p_kwh_ag_default "Default rate's hourly (avg) marg price ($/kWh)"

	// Merge in rates
joinby rt_default using `rt_default', unmatch(both)
assert _merge==3	
drop _merge

	// Clean up and save
order rt_sched_cd date hour rt_default p_kwh_ag_default
sort rt_sched_cd date hour 
unique rt_sched_cd date hour
assert r(unique)==r(N)
compress
save "$dirpath_data/merged/ag_default_prices_hourly.dta", replace

}
*******************************************************************************
*******************************************************************************

** 5. Prepare default ag rates (monthly)
if 1==1{

** Start with list of ag rates
use "$dirpath_data/merged/ag_rates_avg_by_day.dta", clear
keep rt_sched_cd
duplicates drop
sort rt_sched_cd

** Restriction on rate switches
/*

-- AG-1 customers are a distinct group

-- AG-ICE customers are a distinct group

-- AG-4/AG-5/AG-R/AG-V customers can switch between these 4 types of rates

-- A/D customers are small, while B/C/E/F customers are large

-- D/E/F customers are on dumb meters, while A/B/C customers are on smart meters,
   and we can probably assume that PGE meter replacement timing is exogenous

-- C & F are the PDP variants of B & E
 
*/

** Assign the "default" rate for each rate
gen rt_default = ""

replace rt_default = "AG-1A" if rt_sched_cd=="AG-1A"

replace rt_default = "AG-1B" if rt_sched_cd=="AG-1B"

replace rt_default = "AG-ICE" if rt_sched_cd=="AG-ICE"

replace rt_default = "AG-4A" if rt_sched_cd=="AG-4A"
replace rt_default = "AG-4A" if rt_sched_cd=="AG-5A"
replace rt_default = "AG-4A" if rt_sched_cd=="AG-RA"
replace rt_default = "AG-4A" if rt_sched_cd=="AG-VA"

replace rt_default = "AG-4D" if rt_sched_cd=="AG-4D"
replace rt_default = "AG-4D" if rt_sched_cd=="AG-5D"
replace rt_default = "AG-4D" if rt_sched_cd=="AG-RD"
replace rt_default = "AG-4D" if rt_sched_cd=="AG-VD"

replace rt_default = "AG-4B" if rt_sched_cd=="AG-4B"
replace rt_default = "AG-4B" if rt_sched_cd=="AG-4C"
replace rt_default = "AG-4B" if rt_sched_cd=="AG-5B"
replace rt_default = "AG-4B" if rt_sched_cd=="AG-5C"
replace rt_default = "AG-4B" if rt_sched_cd=="AG-RB"
replace rt_default = "AG-4B" if rt_sched_cd=="AG-VB"

replace rt_default = "AG-4E" if rt_sched_cd=="AG-4E"
replace rt_default = "AG-4E" if rt_sched_cd=="AG-4F"
replace rt_default = "AG-4E" if rt_sched_cd=="AG-5E"
replace rt_default = "AG-4E" if rt_sched_cd=="AG-5F"
replace rt_default = "AG-4E" if rt_sched_cd=="AG-RE"
replace rt_default = "AG-4E" if rt_sched_cd=="AG-VE"

** Save temp file
tempfile rt_default
save `rt_default'

** Construct dataset of default rates at the hourly level

	// Isolate only the rates serving as a default
use "$dirpath_data/merged/ag_rates_avg_by_day.dta", clear
rename rt_sched_cd rt_default
joinby rt_default using `rt_default'
drop rt_sched_cd
duplicates drop
unique rt_default date
assert r(unique)==r(N)

	// Collapse to monthly level
gen modate = ym(year(date),month(date))
format %tm modate
foreach v of varlist mean_p_kwh min_p_kwh max_p_kwh {
	local fxn = subinstr(substr("`v'",1,4),"_","",.)
	egen double temp = `fxn'(`v'), by(rt_default modate)
	replace `v' = temp
	drop temp
}
keep rt_default modate mean_p_kwh min_p_kwh max_p_kwh
duplicates drop
unique rt_default modate
assert r(unique)==r(N)

	// Clean up and label
rename *_p_kwh *_p_kwh_ag_default
la var rt_default "Default ag tariff for customer group"
la var mean_p_kwh_ag_default "Default rate's avg hourly marg price ($/kWh)"
la var min_p_kwh_ag_default "Default rate's min hourly marg price ($/kWh)"
la var max_p_kwh_ag_default "Default rate's max hourly marg price ($/kWh)"

	// Merge in rates
joinby rt_default using `rt_default', unmatch(both)
assert _merge==3	
drop _merge

	// Clean up and save
order rt_sched_cd modate rt_default 
sort rt_sched_cd modate
unique rt_sched_cd modate
assert r(unique)==r(N)
compress
save "$dirpath_data/merged/ag_default_prices_monthly.dta", replace

}

*******************************************************************************
*******************************************************************************

** 6. Construct "control function" variable (hourly)
if 1==1{

** Merge ag rates with E-1 and E-20 hourly rates
use "$dirpath_data/merged/ag_rates_for_merge.dta", clear
merge m:1 date using "$dirpath_data/merged/e1_prices_daily.dta"
assert _merge!=1
drop if _merge==2
drop _merge
merge m:1 date hour using "$dirpath_data/merged/e20_prices_hourly.dta"
assert _merge==3
drop _merge

** Construct marginal price variable
gen p_kwh = energycharge
assert pdpenergycredit<=0 & pdpcharge!=. & pdpcharge>=0
replace p_kwh = pdpcharge + pdpenergycredit if pdpcharge!=0 & inlist(hour,14,15,16,17) & ///
	year(date)>=2013 & event_day_biz==1 // 4-hour event windows 2013-2017, based on "business" Event Days
replace p_kwh = pdpcharge + pdpenergycredit if pdpcharge!=0 & inlist(hour,14,15,16,17) & ///
	year(date)<2013 & event_day_res==1 // 4-hour event windows 2013-2017, based on "residential" Event Days
assert p_kwh!=.	

** Construct control function variable
gen ctrl_fxn = .
levelsof rt_sched_cd, local(levs)
foreach rt in `levs' {
	qui reg p_kwh i.group##c.p_kwh_e20 i.group##c.p_kwh_e1_lo ///
		i.group##c.p_kwh_e1_mi i.group##c.p_kwh_e1_hi if rt_sched_cd=="`rt'"
	local r2 = round(e(r2),0.001)
	di "`rt'   `r2'" 
	qui predict temp if rt_sched_cd=="`rt'", residuals
	qui sum temp
	assert abs(r(mean))<1e-7
	qui replace ctrl_fxn = temp if rt_sched_cd=="`rt'"
	drop temp
}

** Clean up, label and save residuals at hourly level
keep rt_sched date hour group ctrl_fxn
sort rt_sched group date hour
unique rt_sched group date hour
assert r(unique)==r(N)
la var date "Date"
la var ctrl_fxn "Residuals from hourly rate-specific time-series reg on E1/E20 prices"
compress
save "$dirpath_data/merged/ag_rates_ctrl_fxn_hourly.dta", replace
}

*******************************************************************************
*******************************************************************************

** 7. Construct "control function" variable (monthly)
if 1==1{

** Collapse avg daily to avg monthly ag rates
use "$dirpath_data/merged/ag_rates_avg_by_day.dta", clear
gen modate = ym(year(date),month(date))
format %tm modate
egen double temp = mean(mean_p_kwh), by(rt_sched_cd modate)
replace mean_p_kwh = temp
keep rt_sched_cd modate mean_p_kwh
duplicates drop
unique rt_sched_cd modate
assert r(unique)==r(N)
drop if modate<ym(2008,1)

** Merge in monthly E-1 and E-20 rates
merge m:1 modate using "$dirpath_data/merged/e1_prices_monthly.dta"
assert _merge==3
drop _merge
merge m:1 modate using "$dirpath_data/merged/e20_prices_monthly.dta"
assert _merge==3
drop _merge

** Construct control function variables
gen ctrl_fxn_mean = .
levelsof rt_sched_cd, local(levs)
foreach rt in `levs' {
	qui reg mean_p_kwh *e1* *e20* if rt_sched_cd=="`rt'"
	local r2 = round(e(r2),0.001)
	di "`rt'   `r2'" 
	qui predict temp if rt_sched_cd=="`rt'", residuals
	qui sum temp
	assert abs(r(mean))<1e-7
	qui replace ctrl_fxn_mean = temp if rt_sched_cd=="`rt'"
	drop temp
}

** Clean up, label and save residuals at hourly level
keep rt_sched modate ctrl_fxn
sort rt_sched modate
unique rt_sched modate
assert r(unique)==r(N)
la var modate "Year-Month"
la var ctrl_fxn "Residuals from monthly rate-specific time-series reg on E1/E20 prices"
compress
save "$dirpath_data/merged/ag_rates_ctrl_fxn_monthly.dta", replace

}

*******************************************************************************
*******************************************************************************
