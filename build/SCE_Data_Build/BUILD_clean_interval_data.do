clear all
version 13
set more off

****************************************************************************
**** Script to import and clean raw SCE data -- interval data file *********
****************************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

*******************************************************************************
*******************************************************************************

** 1. Clean raw weekly files and collapse to the hourly level 
{
** Loop over raw (weekly) SCE interval data
clear
cd "$dirpath_data/sce_raw"
local files: dir . files "interval*.dta"
foreach file of local files {

	** Load raw SCE interval data
	use "$dirpath_data/sce_raw/`file'", clear
	assert unit_of_measure=="KWH_DEL"
	drop unit_of_measure

	** Fix date, hour, interval
	drop interval_date interval_end_dttm // redundant
	assert interval_start_dttm!="" 
	replace interval_start_dttm = subinstr(interval_start_dttm,":00.000000000","",1)
	compress
	split interval_start_dttm, p(":")
	gen date = date(interval_start_dttm1,"DMY")
	format %td date
	assert date!=.
	drop interval_start_dttm interval_start_dttm1
	rename interval_start_dttm2 hr
	rename interval_start_dttm3 intv
	destring hr intv, replace
	assert inrange(hr,0,23) & round(hr)==hr
	assert inlist(intv,0,15,30,45)

	** Calculate the total amount of interval time in each SA-hour
	sort service_account_id date hr intv
	gen min = .
	replace min = 15 if intv==0 & intv[_n+1]==15 & service_account_id==service_account_id[_n+1] & date==date[_n+1] & hr==hr[_n+1]
	replace min = 15 if intv==15 & intv[_n+1]==30 & service_account_id==service_account_id[_n+1] & date==date[_n+1] & hr==hr[_n+1]
	replace min = 15 if intv==30 & intv[_n+1]==45 & service_account_id==service_account_id[_n+1] & date==date[_n+1] & hr==hr[_n+1]
	replace min = 15 if intv==45 & intv[_n+1]==0 & service_account_id==service_account_id[_n+1] & date==date[_n+1] & hr==hr[_n+1]-1
	replace min = 15 if intv==45 & intv[_n+1]==0 & service_account_id==service_account_id[_n+1] & date==date[_n+1]-1 & hr==23 & hr[_n+1]==0
	replace min = 30 if intv==0 & intv[_n+1]==30 & service_account_id==service_account_id[_n+1] & date==date[_n+1] & hr==hr[_n+1]
	replace min = 30 if intv==15 & intv[_n+1]==45 & service_account_id==service_account_id[_n+1] & date==date[_n+1] & hr==hr[_n+1]
	replace min = 30 if intv==30 & intv[_n+1]==0 & service_account_id==service_account_id[_n+1] & date==date[_n+1] & hr==hr[_n+1]-1
	replace min = 30 if intv==30 & intv[_n+1]==0 & service_account_id==service_account_id[_n+1] & date==date[_n+1]-1 & hr==23 & hr[_n+1]==0
	replace min = 45 if intv==0 & intv[_n+1]==45 & service_account_id==service_account_id[_n+1] & date==date[_n+1] & hr==hr[_n+1]
	replace min = 45 if intv==15 & intv[_n+1]==0 & service_account_id==service_account_id[_n+1] & date==date[_n+1] & hr==hr[_n+1]-1
	replace min = 45 if intv==15 & intv[_n+1]==0 & service_account_id==service_account_id[_n+1] & date==date[_n+1]-1 & hr==23 & hr[_n+1]==0
	replace min = 60 if intv==0 & intv[_n+1]==0 & service_account_id==service_account_id[_n+1] & date==date[_n+1] & hr==hr[_n+1]-1
	replace min = 60 if intv==0 & intv[_n+1]==0 & service_account_id==service_account_id[_n+1] & date==date[_n+1]-1 & hr==23 & hr[_n+1]==0
	replace min = 60 if intv==0 & min==.
	replace min = 45 if intv==15 & min==.
	replace min = 30 if intv==30 & min==.
	replace min = 15 if intv==45 & min==.
	assert min!=.

	** Collapse to hour, adjusting for non-60-minute hours
	egen tot_min = sum(min), by(service_account_id date hr)
	egen kwh = sum(usage), by(service_account_id date hr)
	replace kwh = kwh/(tot_min/60) // scale or scale down to put all intervals in units of 60 minutes
	drop intv min tot_min usage
	duplicates drop
	unique service_account_id date hr
	assert r(unique)==r(N)
	compress
	la var date "Date"
	la var hr "Hour 0-23 at the start of the interval"

	** Service agreement ID (to string)
	rename service_account_id sa_uuid
	unique sa_uuid
	assert sa_uuid!=.
	tostring sa_uuid, replace
	la var sa_uuid "Service Account ID"

	** kWh
	assert kwh!=.
	la var kwh "kWh consumed in hourly interval"

	** Compress and save
	order sa_uuid date hr kwh
	compress
	save "$dirpath_data/temp/sce/`file'.dta", replace	
}

}

*******************************************************************************
*******************************************************************************

** 2. Append weekly datasets and collapse 
{

** Append all cleaned weekly files
clear
cd "$dirpath_data/temp/sce"
local files: dir . files "interval*.dta"
foreach file of local files {
	append using "`file'"
}

** Confirm uniqueness
unique sa_uuid date hr
assert r(unique)==r(N)

** Save hourly dataset
sort sa_uuid date hr
compress
save "$dirpath_data/sce_cleaned/interval_data_hourly_20190916.dta", replace

** Collapse to daily level and save
use "$dirpath_data/sce_cleaned/interval_data_hourly_20190916.dta", clear
egen double kwh_daily = sum(kwh), by(sa_uuid date)
drop hour kwh
rename kwh_daily kwh
la var kwh "kWh consumed in daily interval"
duplicates drop
sum kwh, detail
count if kwh<0
di r(N)/_N // 0.6% of daily observations are negative
unique sa_uuid date
assert r(unique)==r(N)
compress
save "$dirpath_data/sce_cleaned/interval_data_daily_20190916.dta", replace	

** Collapse to monthly level and save
use "$dirpath_data/sce_cleaned/interval_data_daily_20190916.dta", clear
gen modate = ym(year(date),month(date))
format %tm modate
egen double kwh_monthly = sum(kwh), by(sa_uuid modate)
drop date kwh
rename kwh_monthly kwh
la var kwh "kWh consumed in monthly interval"
duplicates drop
sum kwh, detail
count if kwh<0
di r(N)/_N // 0.5% of monthly observations are negative
preserve
collapse (sum) kwh, by(modate)
twoway line kwh modate
restore
unique sa_uuid modate
assert r(unique)==r(N)
compress
save "$dirpath_data/sce_cleaned/interval_data_monthly_20190916.dta", replace	

}

*******************************************************************************
*******************************************************************************
