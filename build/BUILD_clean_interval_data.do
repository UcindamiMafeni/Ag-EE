clear all
version 13
set more off

****************************************************************************
**** Script to import and clean raw PGE data -- interval data file *********
****************************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

** Load raw PGE interval data
use "$dirpath_data/pge_raw/interval_data.dta", clear
*duplicates drop // this takes hours and there are no dups

** Fix date
assert usg_dt!=""
gen date = date(usg_dt,"DMY")
format date %td
assert date!=.
drop usg_dt
compress
la var date "Date"

** Check hour
//tab hr_ending
assert hr_ending!=.
assert inrange(hr_ending,0,23)
assert round(hr_ending)==hr_ending
la var hr_ending "Hour (at end of interval)"
rename hr_ending hour

** Service agreement ID (to string)
assert sa_uuid!=.
tostring sa_uuid, replace
replace sa_uuid = "0" + sa_uuid if length(sa_uuid)<10
replace sa_uuid = "0" + sa_uuid if length(sa_uuid)<10
replace sa_uuid = "0" + sa_uuid if length(sa_uuid)<10
replace sa_uuid = "0" + sa_uuid if length(sa_uuid)<10
assert length(sa_uuid)==10
la var sa_uuid "Service Agreement ID (anonymized, 10-digit)"
order sa_uuid date hour kwh

** kWh
assert kwh!=.
sum kwh, detail
count if kwh<0
di r(N)/_N // 0.3% of hourly observations are negative
la var kwh "kWh consumed in hourly interval"

** Confirm unique
unique sa_uuid date hour
assert r(unique)==r(N)

** Compress and save
compress
save "$dirpath_data/pge_cleaned/interval_data_hourly.dta", replace	

** Collapse to daily level and save
use  "$dirpath_data/pge_cleaned/interval_data_hourly.dta", clear
egen double kwh_daily = sum(kwh), by(sa_uuid date)
drop hour kwh
rename kwh_daily kwh
la var kwh "kWh consumed in daily interval"
duplicates drop
sum kwh, detail
count if kwh<0
di r(N)/_N // ??? of daily observations are negative
unique sa_uuid date
assert r(unique)==r(N)
compress
save "$dirpath_data/pge_cleaned/interval_data_daily.dta", replace	

** Collapse to monthly level and save
use  "$dirpath_data/pge_cleaned/interval_data_daily.dta", clear
gen modate = ym(year(date),month(date))
format %tm modate
egen double kwh_monthly = sum(kwh), by(sa_uuid modate)
drop date kwh
rename kwh_monthly kwh
la var kwh "kWh consumed in monthly interval"
duplicates drop
sum kwh, detail
count if kwh<0
di r(N)/_N // ??? of monthly observations are negative
unique sa_uuid modate
assert r(unique)==r(N)
compress
save "$dirpath_data/pge_cleaned/interval_data_monthly.dta", replace	


