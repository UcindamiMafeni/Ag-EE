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
duplicates drop

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




** Pending
// Check negatives
// Drop missing/duplicates
// Confirm unique
