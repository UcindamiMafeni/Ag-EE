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
START HERE
** 2. Construct daily panel for SP coordinates
if 1==1{

** Import results from GIS script
insheet using "$dirpath_data/misc/pge_prem_coord_polygon_wdist.txt", double delim("%") clear
drop prem_lat prem_lon longitude latitude bad_geocode_flag

** Clean GIS variables
tostring sp_uuid pull, replace
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
assert length(sp_uuid)==10 & real(sp_uuid)!=.
unique sp_uuid pull
assert r(unique)==r(N)

foreach v of varlist wdist wdist_id wdist_area_km2 {
	replace `v' = "" if `v'=="NA"
}
destring wdist_id wdist_area_km2, replace

foreach v of varlist nearestwdist_id nearestwdist_area_km2 nearestwdist_dist_km {
	replace `v' = . if nearestwdist==""
}

	// Convert from km to miles
replace nearestwdist_dist_km = nearestwdist_dist_km*0.621371
rename nearestwdist_dist_km nearestwdist_miles
replace wdist_area_km2 = wdist_area_km2*0.386102
rename wdist_area_km2 wdist_area_sqmi
replace nearestwdist_area_km2 = nearestwdist_area_km2*0.386102
rename nearestwdist_area_km2 nearestwdist_area_sqmi
sum nearestwdist_miles, detail // p90 = 2.13 miles, not bad
local p90 = r(p90)

	// Assign nearest water districts within 2.13 miles
assert inlist(in_wdist,0,1)
assert nearestwdist_miles!=. if in_wdist==0
assert nearestwdist_miles==. if in_wdist==1
rename in_wdist wdist_dist_miles
replace wdist_dist_miles = 1 - wdist_dist_miles
replace wdist_dist_miles = nearestwdist_miles if nearestwdist_miles!=.	
sum wdist_dist_miles, det
replace wdist = nearestwdist if nearestwdist!="" & wdist_dist_miles<=`p90'
replace wdist_id = nearestwdist_id if nearestwdist_id!=. & wdist_dist_miles<=`p90'
replace wdist_area_sqmi = nearestwdist_area_sqmi if nearestwdist_area_sqmi!=. & wdist_dist_miles<=`p90'

	// Drop nearest water district variables
drop nearestwdist*	

	// Label
la var wdist "Water district (assigned by GIS)"	
la var wdist_dist_miles "Distance to water district (cut off at 2.13 miles)"	
la var wdist_id "Water district ID (from shapefile)"
la var wdist_area_sqmi "GIS-derived area of water district (sq miles)"
	
	// Save
tempfile gis_out
save `gis_out'

** Merge results into merge back into main dataset
use "$dirpath_data/pge_cleaned/sp_premise_gis.dta", clear
merge 1:1 sp_uuid pull using `gis_out'	
assert _merge!=2 // confirm everything merges in
assert _merge==1 if prem_lat==. | prem_lon==. // confirm nothing merges if it has missing lat/lon
assert (prem_lat==. | prem_lon==.) if _merge==1	// confirm that all non-merges have missing lat/lon
drop _merge

** Confirm uniqueness and save
unique sp_uuid pull
assert r(unique)==r(N)
compress
save "$dirpath_data/pge_cleaned/sp_premise_gis.dta", replace	

}

*******************************************************************************
*******************************************************************************

** 3. Construct daily panel for APEP coordinates
if 1==1{

** Import results from GIS script
insheet using "$dirpath_data/misc/apep_pump_coord_polygon_wdist.txt", double delim("%") clear
drop pump_lat pump_lon longitude latitude

** Clean GIS variables
destring latlon_group, replace
assert latlon_group!=.
unique latlon_group
assert r(unique)==r(N)

foreach v of varlist wdist wdist_id wdist_area_km2 {
	replace `v' = "" if `v'=="NA"
}
destring wdist_id wdist_area_km2, replace

foreach v of varlist nearestwdist_id nearestwdist_area_km2 nearestwdist_dist_km {
	replace `v' = . if nearestwdist==""
}

	// Convert from km to miles
replace nearestwdist_dist_km = nearestwdist_dist_km*0.621371
rename nearestwdist_dist_km nearestwdist_miles
replace wdist_area_km2 = wdist_area_km2*0.386102
rename wdist_area_km2 wdist_area_sqmi
replace nearestwdist_area_km2 = nearestwdist_area_km2*0.386102
rename nearestwdist_area_km2 nearestwdist_area_sqmi
sum nearestwdist_miles, detail // p90 = 2.05 miles, not bad
local p90 = r(p90)

	// Assign nearest water districts within 2.05 miles
assert inlist(in_wdist,0,1)
assert nearestwdist_miles!=. if in_wdist==0
assert nearestwdist_miles==. if in_wdist==1
rename in_wdist wdist_dist_miles
replace wdist_dist_miles = 1 - wdist_dist_miles
replace wdist_dist_miles = nearestwdist_miles if nearestwdist_miles!=.	
sum wdist_dist_miles, det
replace wdist = nearestwdist if nearestwdist!="" & wdist_dist_miles<=`p90'
replace wdist_id = nearestwdist_id if nearestwdist_id!=. & wdist_dist_miles<=`p90'
replace wdist_area_sqmi = nearestwdist_area_sqmi if nearestwdist_area_sqmi!=. & wdist_dist_miles<=`p90'

	// Drop nearest water district variables
drop nearestwdist*	
	
	// Label
la var wdist "Water district (assigned by GIS)"	
la var wdist_dist_miles "Distance to water district (cut off at 2.13 miles)"	
la var wdist_id "Water district ID (from shapefile)"
la var wdist_area_sqmi "GIS-derived area of water district (sq miles)"
	
	// Save
tempfile gis_out
save `gis_out'

** Merge results into merge back into main dataset
use "$dirpath_data/pge_cleaned/apep_pump_gis.dta", clear
merge m:1 latlon_group using `gis_out'	
assert _merge==3 // confirm everything merges
drop _merge

** Confirm uniqueness and save
unique apeptestid crop test_date_stata
assert r(unique)==r(N)
compress
save "$dirpath_data/pge_cleaned/apep_pump_gis.dta", replace	

}

*******************************************************************************
*******************************************************************************
