clear all
version 13
set more off

**********************************************************************
**** Script to assign SPs and APEP pump daily min/max temperatures ***
**********************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"
global dirpath_code "S:/Louis/backup/AgEE/AgEE_code/build"
*global R_exe_path "C:/PROGRA~1/MIE74D~1/ROPEN~1/bin/x64/R"
*global R_lib_path "C:/Program Files/Microsoft/R Open/R-3.4.4/library"

*******************************************************************************
*******************************************************************************

** 1. Run auxilary GIS script "BUILD_prism_daily_temperature.R"

*******************************************************************************
*******************************************************************************

** 2. Construct daily panel for SP coordinates
if 1==0{

** Import results from GIS script
insheet using "$dirpath_data/misc/pge_prem_coord_daily_temperatures.csv", double comma clear
drop v1 lon lat
duplicates drop // tons of dups and I don't know why!

** Destring temperature
destring degrees, replace force

**Reformat date
gen temp = date(substr(string(date,"%12.0g"),1,4) + "/" + ///
	substr(string(date,"%12.0g"),5,2) + "/" + ///
	substr(string(date,"%12.0g"),7,2),"YMD")
format %td temp
assert temp!=.
drop date
rename temp date

** Confirm uniqueness before reshape
unique sp_uuid date which
assert r(unique)==r(N)
*duplicates t sp_uuid date which, gen(dup)
*tab dup
*br if dup>0

** Reshape
reshape wide degrees, i(sp_uuid date) j(which) string

** Clean SP variable
tostring sp_uuid, replace
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
assert length(sp_uuid)==10 & real(sp_uuid)!=.
unique sp_uuid date
assert r(unique)==r(N)

** Label
la var sp_uuid "Unique service point identifier"	
la var date "Date"	
rename degreestmax degreesC_max 
rename degreestmin degreesC_min 
la var degreesC_max "Daily max temperature (C) at premise"
la var degreesC_min "Daily min temperature (C) at premise"

** Sort and save
sort sp_uuid date
order sp_uuid date
compress
save "$dirpath_data/prism/sp_temperature_daily.dta", replace	

** Collapse to monthly
gen degreesC_mean = (degreesC_max + degreesC_min) / 2
gen modate = ym(year(date),month(date))
format %tm modate
collapse (mean) degreesC_*, by(sp_uuid modate) fast

** Label
la var sp_uuid "Unique service point identifier"	
la var modate "Year-Month"	
la var degreesC_max "Avg daily max temperature (C) at premise"
la var degreesC_min "Avg daily min temperature (C) at premise"
la var degreesC_mean "Avg daily 'mean' temperature (C) at premise"

** Sort and save
sort sp_uuid modate
order sp_uuid modate
compress
save "$dirpath_data/prism/sp_temperature_monthly.dta", replace	

}

*******************************************************************************
*******************************************************************************

** 3. Construct daily panel for APEP coordinates
if 1==1{

** Import results from GIS script
insheet using "$dirpath_data/misc/apep_pump_coord_daily_temperatures.csv", double comma clear
drop v1 lon lat
duplicates drop // tons of dups and I don't know why!

** Destring temperature
destring degrees, replace force

**Reformat date
gen temp = date(substr(string(date,"%12.0g"),1,4) + "/" + ///
	substr(string(date,"%12.0g"),5,2) + "/" + ///
	substr(string(date,"%12.0g"),7,2),"YMD")
format %td temp
assert temp!=.
drop date
rename temp date

** Confirm uniqueness before reshape
unique latlon_group date which
assert r(unique)==r(N)
*duplicates t latlon_group date which, gen(dup)
*tab dup
*br if dup>0

** Reshape
reshape wide degrees, i(latlon_group date) j(which) string

** Clean ID variable
destring latlon_group, replace
assert latlon_group!=.
unique latlon_group date
assert r(unique)==r(N)

** Label
la var latlon_group "APEP pump location identifier"	
la var date "Date"	
rename degreestmax degreesC_max 
rename degreestmin degreesC_min 
la var degreesC_max "Daily max temperature (C) at pump"
la var degreesC_min "Daily min temperature (C) at pump"

** Sort and save
sort latlon_group date
order latlon_group date
compress
save "$dirpath_data/prism/apep_temperature_daily.dta", replace	

** Collapse to monthly
gen degreesC_mean = (degreesC_max + degreesC_min) / 2
gen modate = ym(year(date),month(date))
format %tm modate
collapse (mean) degreesC_*, by(latlon_group modate) fast

** Label
la var latlon_group "APEP pump location identifier"	
la var modate "Year-Month"	
la var degreesC_max "Avg daily max temperature (C) at pump"
la var degreesC_min "Avg daily min temperature (C) at pump"
la var degreesC_mean "Avg daily 'mean' temperature (C) at pump"

** Sort and save
sort latlon_group modate
order latlon_group modate
compress
save "$dirpath_data/prism/apep_temperature_monthly.dta", replace	

}

*******************************************************************************
*******************************************************************************
