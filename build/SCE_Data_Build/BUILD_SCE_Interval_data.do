clear all
version 13
set more off

****************************************************************************
**** Script to import and clean raw PGE data -- interval data file *********
****************************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

*******************************************************************************
*******************************************************************************

clear
cd "$dirpath_data/sce_raw"
local files: dir . files "interval*.dta"

foreach file of local files {

** Load raw PGE interval data
use "$dirpath_data/sce_raw/`file'", clear
duplicates drop // this takes hours and there are no dups

** Fix date, hour and time
assert interval_start_dttm!="" & interval_end_dttm!=""
split interval_start_dttm, p(":")
drop interval_start_dttm4
split interval_end_dttm, p(":")
drop interval_end_dttm4
gen date1 = date(interval_start_dttm1,"DMY")
gen date2 = date(interval_end_dttm1,"DMY")
split interval_date, p(":")
rename interval_date1 date3
drop interval_date*
gen date32= date(date3,"DMY")
assert date1==date32
drop date3*

rename (interval_start_dttm2 interval_end_dttm2 interval_start_dttm3 interval_end_dttm3) (hr1 hr2 m1 m2)
destring hr1 hr2 m1 m2, replace
gen flag_nc_interval =0
replace flag_nc_interval=1 if !( hr1==hr2 | ( (hr2==hr1+1) |(hr1==23 & hr2==0) & m1==45 & m2==0))
assert date1==date2 | (hr2==0 & m2==0 & hr1==23 & m1==45) | flag_nc_interval==1
drop date2 hr2 m2
replace m1= m1/15 + 1
rename (date1 hr1 m1)  (date hr qtr)
format date %td
assert date!=.
compress
la var date "Date"
la var hr "Hour 0-23 at the start of the interval"
la var qtr "Quarter of hr; 1: 00-15, 2: 15-30, 3: 30-45, 4: 45-60"

** Check hour
//tab hr_ending
assert hr!=.
assert inrange(hr,0,23)
assert round(hr)==hr

** Service agreement ID (to string)
rename service_account_id sa_uuid
unique sa_uuid
assert sa_uuid!=.
tostring sa_uuid, replace
la var sa_uuid "Service Account ID"
order sa_uuid date hr qtr usage

** units
tab unit_of_measure

** kWh
assert usage!=.
sum usage, detail
count if usage<0
di r(N)/_N // 0.3% of hourly observations are negative
la var usage "kWh consumed in hourly interval"



** Compress and save
compress
save "$dirpath_data/temp/sce/`file'.dta", replace	

}

clear
cd "$dirpath_data/temp/sce"
local files: dir . files "interval*.dta"

set obs 1 
gen delete=.
foreach file of local files {
append using "`file'"
keep sa_uuid date hr qtr usage
}
keep sa_uuid date hr qtr usage
drop if _n==1
** Confirm unique
unique sa_uuid date hr qtr
assert r(unique)==r(N)
save "$dirpath_data/sce_cleaned/interval_data_hourly_20190916.dta", replace

//

collapse (mean) usage, by(sa_uuid date hr qtr)
save "$dirpath_data/sce_cleaned/interval_data_hourly_20190916_2.dta", replace

collapse (mean) usage, by(sa_uuid date)
save "$dirpath_data/sce_cleaned/interval_data_daily_20190916.dta", replace

gen month= month(date)
collapse (mean) usage, by(sa_uuid date)
save "$dirpath_data/sce_cleaned/interval_data_monthly_20190916.dta", replace
