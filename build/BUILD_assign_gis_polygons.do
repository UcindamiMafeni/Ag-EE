clear all
version 13
set more off

******************************************************************************
**** Script to assign SPs and APEP pump coordinates to various CA polygons ***
******************************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"
global dirpath_code "T:/Home/Louis/backup/AgEE/AgEE_code/build"
*global R_exe_path "C:/PROGRA~1/MIE74D~1/ROPEN~1/bin/x64/R"
*global R_lib_path "C:/Program Files/Microsoft/R Open/R-3.4.4/library"

*******************************************************************************
*******************************************************************************

** 1. Save SP coordinates (techincally *premise* coordinates)
if 1==0{

** Load all 3 PGE customer datasets
use "$dirpath_data/pge_cleaned/pge_cust_detail_20180719.dta", clear
gen pull = "20180719"
merge 1:1 sp_uuid sa_uuid using "$dirpath_data/pge_cleaned/pge_cust_detail_20180322.dta"
replace pull = "20180322" if _merge==2
drop _merge
merge 1:1 sp_uuid sa_uuid using "$dirpath_data/pge_cleaned/pge_cust_detail_20180827.dta"
replace pull = "20180827" if _merge==2
drop _merge

** Keep relevant variables
keep pull sp_uuid prem_lat prem_long bad_geocode_flag missing_geocode_flag
duplicates drop 
unique sp_uuid pull prem_lat prem_lon
assert r(unique)==r(N)

** Export coordinates
preserve
drop if missing_geocode_flag==1
outsheet using "$dirpath_data/misc/pge_prem_coord_3pulls.txt", comma replace
restore
	
** Save
la var pull "Which PGE data pull did this SP come from?"
compress
save "$dirpath_data/pge_cleaned/sp_premise_gis.dta", replace

}
	
*******************************************************************************
*******************************************************************************

** 2. Save APEP pump coordinates
if 1==0{

** Load APEP test dataset
use "$dirpath_data/pge_cleaned/pump_test_data.dta", clear

** Group by pump lat/lon
egen latlon_group = group(pumplatnew pumplongnew)

** Establish a unique ID, since there isn't one!
unique apeptestid test_date_stata crop
assert r(unique)==r(N)

** Keep only pump lat/lon
keep apeptestid test_date_stata crop pumplatnew pumplongnew latlon_group

** Export coordinates
preserve
drop if latlon_group==.
keep pumplatnew pumplongnew latlon_group
duplicates drop
rename pumplatnew pump_lat
rename pumplongnew pump_long
outsheet using "$dirpath_data/misc/apep_pump_coord.txt", comma replace
restore
	
** Save
la var latlon_group "APEP lat/lon identifier"
compress
save "$dirpath_data/pge_cleaned/apep_pump_gis.dta", replace

}

*******************************************************************************
*******************************************************************************

** 3. Assign SP coordinates to water districts
if 1==0{

** Run auxilary GIS script "BUILD_gis_water_districts.R"

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

** 4. Assign APEP coordinates to water districts
if 1==0{

** Run auxilary GIS script "BUILD_gis_water_districts.R"

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

** 5. Create unique water district identifier
if 1==0{
	
	// The water district ID in the GIS attributes table is not *quite* unique
	
** Create water district group that's actually unique
use "$dirpath_data/pge_cleaned/sp_premise_gis.dta", clear
append using "$dirpath_data/pge_cleaned/apep_pump_gis.dta"

keep wdist wdist_id wdist_area_sqmi
duplicates drop

unique wdist_id
unique wdist_id wdist
unique wdist_id wdist wdist_area_sqmi
egen wdist_group = group(wdist wdist_id wdist_area_sqmi)	
la var wdist_group "Unique water district ID"

tempfile wdist_group
save `wdist_group'

** Merge this variable into SP GIS dataset
use "$dirpath_data/pge_cleaned/sp_premise_gis.dta", clear
merge m:1 wdist wdist_id wdist_area_sqmi using `wdist_group'
assert _merge!=1
drop if _merge==2
drop _merge
sort sp_uuid pull
unique sp_uuid pull
assert r(unique)==r(N)
compress
save "$dirpath_data/pge_cleaned/sp_premise_gis.dta", replace

** Merge this variable into APEP GIS dataset
use "$dirpath_data/pge_cleaned/apep_pump_gis.dta", clear
merge m:1 wdist wdist_id wdist_area_sqmi using `wdist_group'
assert _merge!=1
drop if _merge==2
drop _merge
sort apeptestid test_date_stata crop
unique apeptestid test_date_stata crop
assert r(unique)==r(N)
compress
save "$dirpath_data/pge_cleaned/apep_pump_gis.dta", replace
}

*******************************************************************************
*******************************************************************************

** 6. Assign SP coordinates to counties
if 1==0{

** Run auxilary GIS script "BUILD_gis_counties.R"

** Import results from GIS script
insheet using "$dirpath_data/misc/pge_prem_coord_polygon_counties.txt", double delim("%") clear
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

foreach v of varlist county county_id {
	replace `v' = "" if `v'=="NA"
}

	// Format county_id as FIPS code
rename county_id county_fips
replace county_fips =  "0" + county_fips if length(county_fips)<3 & in_county==1
replace county_fips =  "0" + county_fips if length(county_fips)<3 & in_county==1
assert length(county_fips)==3 if in_county==1

	// Drop extraneous in_county indicator
assert county=="" & county_fips=="" if in_county==0
assert county!="" & county_fips!="" if in_county==1
drop in_county
	
	// Label
la var county "County name (assigned by GIS)"	
la var county_fips "County FIPS (assigned by GIS)"	
	
	// Confirm uniqueness
preserve
keep county county_fips
duplicates drop
unique county_fips
assert r(unique)==r(N)	
restore
	
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

** 7. Assign APEP coordinates to counties
if 1==0{

** Run auxilary GIS script "BUILD_gis_counties.R"

** Import results from GIS script
insheet using "$dirpath_data/misc/apep_pump_coord_polygon_counties.txt", double delim("%") clear
drop pump_lat pump_lon longitude latitude

** Clean GIS variables
destring latlon_group, replace
assert latlon_group!=.
unique latlon_group
assert r(unique)==r(N)

foreach v of varlist county county_id {
	replace `v' = "" if `v'=="NA"
}

	// Format county_id as FIPS code
rename county_id county_fips
replace county_fips =  "0" + county_fips if length(county_fips)<3 & in_county==1
replace county_fips =  "0" + county_fips if length(county_fips)<3 & in_county==1
assert length(county_fips)==3 if in_county==1

	// Drop extraneous in_county indicator
assert county=="" & county_fips=="" if in_county==0
assert county!="" & county_fips!="" if in_county==1
gen bad_geocode_flag = 1 - in_county
drop in_county
	
	// Label
la var county "County name (assigned by GIS)"	
la var county_fips "County FIPS (assigned by GIS)"	
la var bad_geocode_flag "Flag for APEP pumps with geocodes outside California"
	
	// Confirm uniqueness
preserve
keep county county_fips
duplicates drop
unique county_fips
assert r(unique)==r(N)	
restore

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

** 8. Build dataset of California water basins 
if 1==0{

** Import data table from GIS files of CA Water basins
import delimited "$dirpath_data/misc/ca_water_basins_raw.txt", delimiter("%") clear

** Clean and label variables
rename objectid basin_object_id 
rename basin_numb basin_id
rename basin_subb basin_sub_id
rename basin_su_1 basin_sub_name
rename region_off basin_reg_off
rename area_km2 basin_sub_area_sqmi
replace basin_sub_area_sqmi = 0.386102*basin_sub_area_sqmi

la var basin_object_id "Groundwater basin polygon identifier"
la var basin_id "Groundwater basin identifier"
la var basin_sub_id "Groundwater sub-basin identifier"
la var basin_name "Groundwater basin name"
la var basin_sub_name "Groundwater sub-basin name"
la var basin_reg_off "Groundwater basin region office"
la var basin_sub_area_sqmi "Groundwater sub-basin polygon area (sq miles)"

foreach v of varlist basin_name basin_sub_name {
	replace `v' = upper(trim(itrim(`v')))
}

** Confirm uniqueness
unique basin_object_id
assert r(unique)==r(N)
drop globalid

** Save
sort basin_object_id
compress
save "$dirpath_data/groundwater/ca_water_basins.dta", replace

}

*******************************************************************************
*******************************************************************************

** 9. Assign SP coordinates to water basins
if 1==0{

** Run auxilary GIS script "BUILD_gis_water_basins.R"

** Import results from GIS script
insheet using "$dirpath_data/misc/pge_prem_coord_polygon_wbasn.txt", double delim("%") clear
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

foreach v of varlist wbasn wbasn_id {
	replace `v' = "" if `v'=="NA"
}
destring wbasn_id , replace

foreach v of varlist nearestwbasn_id nearestwbasn_dist_km {
	replace `v' = . if wbasn_id!=.
}

	// Convert from km to miles
replace nearestwbasn_dist_km = nearestwbasn_dist_km*0.621371
rename nearestwbasn_dist_km nearestwbasn_miles
sum nearestwbasn_miles, detail // p90 = 11.75 miles, not great, not terrible
local p90 = r(p90)

	// Assign nearest water districts within 11.75 miles (vast majority not in APEP)
assert inlist(in_wbasn,0,1)
assert nearestwbasn_miles!=. if in_wbasn==0
assert nearestwbasn_miles==. if in_wbasn==1
rename in_wbasn wbasn_dist_miles
replace wbasn_dist_miles = 1 - wbasn_dist_miles
replace wbasn_dist_miles = nearestwbasn_miles if nearestwbasn_miles!=.	
sum wbasn_dist_miles, det
replace wbasn_id = nearestwbasn_id if nearestwbasn_id!=. & wbasn_dist_miles<=`p90'

	// Drop nearest water district variables
drop nearestwbasn*	

	// Merge in water basin variables
rename wbasn_id basin_object_id
joinby basin_object_id using "$dirpath_data/groundwater/ca_water_basins.dta", unmatched(master)
assert _merge==3 if basin_object_id!=.
assert upper(wbasn)==basin_name if wbasn!=""
drop _merge wbasn
order wbasn_dist_miles, after(basin_object_id)
rename wbasn_dist_miles basin_dist_miles

	// Label
la var basin_object_id "Groundwater basin polygon (assigned by GIS)"	
la var basin_dist_miles "Distance to goundwater basin (cut off at 11.75 miles)"	
	
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

** 10. Assign APEP coordinates to water basins
if 1==0{

** Run auxilary GIS script "BUILD_gis_water_basins.R"

** Import results from GIS script
insheet using "$dirpath_data/misc/apep_pump_coord_polygon_wbasn.txt", double delim("%") clear
drop pump_lat pump_lon longitude latitude

** Clean GIS variables
destring latlon_group, replace
assert latlon_group!=.
unique latlon_group
assert r(unique)==r(N)

foreach v of varlist wbasn wbasn_id {
	replace `v' = "" if `v'=="NA"
}
destring wbasn_id, replace

foreach v of varlist nearestwbasn_id nearestwbasn_dist_km {
	replace `v' = . if wbasn_id!=.
}

	// Convert from km to miles
replace nearestwbasn_dist_km = nearestwbasn_dist_km*0.621371
rename nearestwbasn_dist_km nearestwbasn_miles
sum nearestwbasn_miles, detail // p90 = 11.00 miles, not great, not terrible
local p90 = r(p90)

	// Assign nearest water districts within 11.00 miles
assert inlist(in_wbasn,0,1)
assert nearestwbasn_miles!=. if in_wbasn==0
assert nearestwbasn_miles==. if in_wbasn==1
rename in_wbasn wbasn_dist_miles
replace wbasn_dist_miles = 1 - wbasn_dist_miles
replace wbasn_dist_miles = nearestwbasn_miles if nearestwbasn_miles!=.	
sum wbasn_dist_miles, det
replace wbasn_id = nearestwbasn_id if nearestwbasn_id!=. & wbasn_dist_miles<=`p90'

	// Drop nearest water district variables
drop nearestwbasn*	

	// Merge in water basin variables
rename wbasn_id basin_object_id
joinby basin_object_id using "$dirpath_data/groundwater/ca_water_basins.dta", unmatched(master)
assert _merge==3 if basin_object_id!=.
assert upper(wbasn)==basin_name if wbasn!=""
drop _merge wbasn
order wbasn_dist_miles, after(basin_object_id)
rename wbasn_dist_miles basin_dist_miles

	// Label
la var basin_object_id "Groundwater basin polygon (assigned by GIS)"	
la var basin_dist_miles "Distance to goundwater basin (cut off at 11.75 miles)"	
	
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

** 11. Make SP coordinates unique by SP
if 1==0{

use "$dirpath_data/pge_cleaned/sp_premise_gis.dta", clear
duplicates t sp_uuid, gen(dup)
egen temp_max = max(pull=="20180322"), by(sp_uuid)
unique sp_uuid
local uniq = r(unique)
drop if temp_max==1 & pull!="20180322"
unique sp_uuid
assert r(unique)==`uniq'
assert r(unique)==r(N)
compress
save "$dirpath_data/pge_cleaned/sp_premise_gis.dta", replace	

}

*******************************************************************************
*******************************************************************************

** 12. Assign SP coordinates to tax parcels (all parcels)
if 1==0{

** Run auxilary GIS script "BUILD_gis_parcel_assign.R"

** Import results from GIS script
insheet using "$dirpath_data/misc/pge_prem_coord_polygon_parcels.csv", double comma clear
drop prem_lon prem_lat bad_geocode_flag

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

foreach v of varlist parcelid-nearest_parcelacres {
	replace `v' = "" if `v'=="NA"
}
destring parcelacres nearest_dist_km nearest_parcelacres , replace

foreach v of varlist nearest_* {
	assert mi(`v') if mi(parcelid)==0
}

	// Convert from km to miles
replace nearest_dist_km = nearest_dist_km*0.621371
rename nearest_dist_km nearest_miles
sum nearest_miles, detail // p90 = 16.7 but p75 = 0.01!!
_pctile nearest_miles, p(75(1)90)
local cutoff = 1 // some crazy sharp cutoff in the percentiles

	// Fix logical
replace in_parcel = "1" if in_parcel=="TRUE"
replace in_parcel = "0" if in_parcel=="FALSE"
destring in_parcel, replace
assert inlist(in_parcel,0,1)
	
	// Assign nearest parcel within 1 miles (vast majority not in APEP)
assert nearest_miles==. if in_parcel==1
gen parcel_dist_miles = 0 if in_parcel==1
replace parcel_dist_miles = nearest_miles if parcel_dist_miles==.
sum parcel_dist_miles, det
replace parcelid = nearest_parcelid if parcelid=="" & nearest_parcelid!="" & parcel_dist_miles<`cutoff'

	// Transfer county for nearest parcels
gen temp = subinstr(subinstr(nearest_parcelid,"-","",.),".","",.)
forvalues n = 0/9 {
	replace temp = subinstr(temp,"`n'","",.)
}
replace temp = trim(itrim(temp))
gen temp2 = 0
levelsof parcelcounty, local(levs)
foreach lev in `levs'{
	replace temp2 = 1 if temp=="`lev'"
}
assert temp2==1 if temp!=""
replace parcelcounty = temp if parcelid!="" & parcelcounty=="" & parcel_dist_miles<`cutoff'
rename parcelcounty parcel_county

	// Transfer acres for nearest parcels
replace parcelacres = nearest_parcelacres if parcelid!="" & parcelacres==. & parcel_dist_miles<`cutoff'
rename parcelacres parcel_acres

	// Drop nearest parcel variables
drop nearest_* temp*	

	// Label
la var in_parcel "Dummy for SPs properly within parcel polygons, all parcels"
la var parcelid "Unique parcel ID, all parcels (county, area, lon, lat)"	
la var parcel_county "County of assigned parcel, all parcels"	
la var parcel_acres "Area (acres) of assigned parcel, all parcels"
la var parcel_dist_miles "Distance to assigned parcel, all parcels (cut off at 1 mile)"

	// Save
tempfile gis_out
save `gis_out'

** Merge results into merge back into main dataset
use "$dirpath_data/pge_cleaned/sp_premise_gis.dta", clear
merge 1:1 sp_uuid pull using `gis_out'	
egen temp_max_merge = max(_merge), by(sp_uuid)
assert _merge!=2 | temp_max_merge==3 // confirm everything merges in
assert _merge!=3 if prem_lat==. | prem_long==. // confirm nothing merges if it has missing lat/lon
assert (prem_lat==. | prem_long==.) if _merge==1	// confirm that all non-merges have missing lat/lon
drop if _merge==2
drop _merge temp*
cap drop dup

** Confirm uniqueness and save
unique sp_uuid pull
assert r(unique)==r(N)
compress
save "$dirpath_data/pge_cleaned/sp_premise_gis.dta", replace	

}

*******************************************************************************
*******************************************************************************

** 13. Assign SP coordinates to tax parcels (CLU-merged parcels only!)
if 1==0{

** Run auxilary GIS script "BUILD_gis_parcel_assign.R"

** Import results from GIS script
insheet using "$dirpath_data/misc/pge_prem_coord_polygon_parcels_conc.csv", double comma clear
drop prem_lon prem_lat bad_geocode_flag

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

foreach v of varlist parcelid-nearest_parcelacres {
	replace `v' = "" if `v'=="NA"
}
destring parcelacres nearest_dist_km nearest_parcelacres , replace

foreach v of varlist nearest_* {
	assert mi(`v') if mi(parcelid)==0
}

	// Convert from km to miles
replace nearest_dist_km = nearest_dist_km*0.621371
rename nearest_dist_km nearest_miles
sum nearest_miles, detail // p90 = 1.9 but p75 = 0.15
_pctile nearest_miles, p(75(1)90)
return list
local cutoff = 1 // keep the same cutoff as above, which here is 88th pctile

	// Fix logical
replace in_parcel = "1" if in_parcel=="TRUE"
replace in_parcel = "0" if in_parcel=="FALSE"
destring in_parcel, replace
assert inlist(in_parcel,0,1)
	
	// Assign nearest parcel within 1 miles (vast majority not in APEP)
assert nearest_miles==. if in_parcel==1
gen parcel_dist_miles = 0 if in_parcel==1
replace parcel_dist_miles = nearest_miles if parcel_dist_miles==.
sum parcel_dist_miles, det
replace parcelid = nearest_parcelid if parcelid=="" & nearest_parcelid!="" & parcel_dist_miles<`cutoff'

	// Transfer county for nearest parcels
gen temp = subinstr(subinstr(nearest_parcelid,"-","",.),".","",.)
forvalues n = 0/9 {
	replace temp = subinstr(temp,"`n'","",.)
}
replace temp = trim(itrim(temp))
gen temp2 = 0
levelsof parcelcounty, local(levs)
foreach lev in `levs'{
	replace temp2 = 1 if temp=="`lev'"
}
tab temp if temp2!=1 & temp!=""
replace parcelcounty = temp if parcelid!="" & parcelcounty=="" & parcel_dist_miles<`cutoff'
rename parcelcounty parcel_county

	// Transfer acres for nearest parcels
replace parcelacres = nearest_parcelacres if parcelid!="" & parcelacres==. & parcel_dist_miles<`cutoff'
rename parcelacres parcel_acres

	// Drop nearest parcel variables
drop nearest_* temp*	

	// Label
la var in_parcel "Dummy for SPs properly within parcel polygons, CLU-merged parcels"
la var parcelid "Unique parcel ID, CLU-merged parcels (county, area, lon, lat)"	
la var parcel_county "County of assigned parcel, CLU-merged parcels"	
la var parcel_acres "Area (acres) of assigned parcel, CLU-merged parcels"
la var parcel_dist_miles "Distance to assigned parcel, CLU-merged parcels (cut off at 1 mile)"

	// Rename to differentiate from the all-parcel varaibles
rename *parcel_* *parcel_conc_*
rename in_parcel in_parcel_conc
rename parcelid parcelid_conc
	
	// Save
tempfile gis_out
save `gis_out'

** Merge results into merge back into main dataset
use "$dirpath_data/pge_cleaned/sp_premise_gis.dta", clear
merge 1:1 sp_uuid pull using `gis_out'	
egen temp_max_merge = max(_merge), by(sp_uuid)
assert _merge!=2 | temp_max_merge==3 // confirm everything merges in
assert _merge!=3 if prem_lat==. | prem_long==. // confirm nothing merges if it has missing lat/lon
assert (prem_lat==. | prem_long==.) if _merge==1	// confirm that all non-merges have missing lat/lon
drop if _merge==2
drop _merge temp*
cap drop dup

** Confirm uniqueness and save
unique sp_uuid pull
assert r(unique)==r(N)
compress
save "$dirpath_data/pge_cleaned/sp_premise_gis.dta", replace	

}

*******************************************************************************
*******************************************************************************

** 14. Assign SP coordinates to CLUs 
if 1==0{

** Run auxilary GIS script "BUILD_gis_clu_assign.R"

** Import results from GIS script
insheet using "$dirpath_data/misc/pge_prem_coord_polygon_clu.csv", double comma clear
drop prem_lon prem_lat bad_geocode_flag

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

foreach v of varlist clu_id-nearest_clucounty {
	replace `v' = "" if `v'=="NA"
}
destring cluacres nearest_dist_km nearest_cluacres , replace

foreach v of varlist nearest_* {
	assert mi(`v') if mi(clu_id)==0
}

	// Convert from km to miles
replace nearest_dist_km = nearest_dist_km*0.621371
rename nearest_dist_km nearest_miles
sum nearest_miles, detail // p90 = .625
local cutoff = 1 // for consistency with parcles cutoff

	// Fix logical
replace in_clu = "1" if in_clu=="TRUE"
replace in_clu = "0" if in_clu=="FALSE"
destring in_clu, replace
assert inlist(in_clu,0,1)
	
	// Assign nearest CLU within 1 miles (vast majority not in APEP)
assert nearest_miles==. if in_clu==1
gen clu_dist_miles = 0 if in_clu==1
replace clu_dist_miles = nearest_miles if clu_dist_miles==.
sum clu_dist_miles, det
replace clu_id = nearest_clu_id if clu_id=="" & nearest_clu_id!="" & clu_dist_miles<`cutoff'

	// Transfer county for nearest CLUs
replace clucounty = nearest_clucounty if clu_id!="" & clucounty=="" & clu_dist_miles<`cutoff'
rename clucounty clu_county

	// Transfer acres for nearest CLUs
replace cluacres = nearest_cluacres if clu_id!="" & cluacres==. & clu_dist_miles<`cutoff'
rename cluacres clu_acres

	// Drop nearest CLU variables
drop nearest_* 	

	// Label
la var in_clu "Dummy for SPs properly within CLU polygons"
la var clu_id "Unique CLU ID (county, lon, lat, area)"	
la var clu_county "County of assigned CLU"	
la var clu_acres "Area (acres) of assigned CLU"
la var clu_dist_miles "Distance to assigned CLU (cut off at 1 mile)"

	// Save
tempfile gis_out
save `gis_out'

** Merge results into merge back into main dataset
use "$dirpath_data/pge_cleaned/sp_premise_gis.dta", clear
merge 1:1 sp_uuid pull using `gis_out'	
egen temp_max_merge = max(_merge), by(sp_uuid)
assert _merge!=2 | temp_max_merge==3 // confirm everything merges in
assert _merge!=3 if prem_lat==. | prem_long==. // confirm nothing merges if it has missing lat/lon
assert (prem_lat==. | prem_long==.) if _merge==1	// confirm that all non-merges have missing lat/lon
drop if _merge==2
drop _merge temp*
cap drop dup

** Confirm uniqueness and save
unique sp_uuid pull
assert r(unique)==r(N)
compress
save "$dirpath_data/pge_cleaned/sp_premise_gis.dta", replace	

}

*******************************************************************************
*******************************************************************************

** 15. Assign APEP coordinates to tax parcels (all parcels)
if 1==1{

** Run auxilary GIS script "BUILD_gis_parcel_assign.R"

** Import results from GIS script
insheet using "$dirpath_data/misc/apep_pump_coord_polygon_parcels.csv", double comma clear
drop pump_lat pump_lon

** Clean GIS variables
destring latlon_group, replace
assert latlon_group!=.
unique latlon_group
assert r(unique)==r(N)

foreach v of varlist parcelid-nearest_parcelacres {
	replace `v' = "" if `v'=="NA"
}
destring parcelacres nearest_dist_km nearest_parcelacres , replace

foreach v of varlist nearest_* {
	assert mi(`v') if mi(parcelid)==0
}

	// Convert from km to miles
replace nearest_dist_km = nearest_dist_km*0.621371
rename nearest_dist_km nearest_miles
sum nearest_miles, detail // p90 = 16.7 but p75 = 0.01!!
_pctile nearest_miles, p(75(1)90)
local cutoff = 1 // some crazy sharp cutoff in the percentiles

	// Fix logical
replace in_parcel = "1" if in_parcel=="TRUE"
replace in_parcel = "0" if in_parcel=="FALSE"
destring in_parcel, replace
assert inlist(in_parcel,0,1)
	
	// Assign nearest parcel within 1 miles (vast majority not in APEP)
assert nearest_miles==. if in_parcel==1
gen parcel_dist_miles = 0 if in_parcel==1
replace parcel_dist_miles = nearest_miles if parcel_dist_miles==.
sum parcel_dist_miles, det
replace parcelid = nearest_parcelid if parcelid=="" & nearest_parcelid!="" & parcel_dist_miles<`cutoff'

	// Transfer county for nearest parcels
gen temp = subinstr(subinstr(nearest_parcelid,"-","",.),".","",.)
forvalues n = 0/9 {
	replace temp = subinstr(temp,"`n'","",.)
}
replace temp = trim(itrim(temp))
gen temp2 = 0
levelsof parcelcounty, local(levs)
foreach lev in `levs'{
	replace temp2 = 1 if temp=="`lev'"
}
assert temp2==1 if temp!=""
replace parcelcounty = temp if parcelid!="" & parcelcounty=="" & parcel_dist_miles<`cutoff'
rename parcelcounty parcel_county

	// Transfer acres for nearest parcels
replace parcelacres = nearest_parcelacres if parcelid!="" & parcelacres==. & parcel_dist_miles<`cutoff'
rename parcelacres parcel_acres

	// Drop nearest parcel variables
drop nearest_* temp*	

	// Label
la var in_parcel "Dummy for SPs properly within parcel polygons, all parcels"
la var parcelid "Unique parcel ID, all parcels (county, area, lon, lat)"	
la var parcel_county "County of assigned parcel, all parcels"	
la var parcel_acres "Area (acres) of assigned parcel, all parcels"
la var parcel_dist_miles "Distance to assigned parcel, all parcels (cut off at 1 mile)"

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

** 16. Assign APEP coordinates to tax parcels (CLU-merged parcels only!)
if 1==1{

** Run auxilary GIS script "BUILD_gis_parcel_assign.R"

** Import results from GIS script
insheet using "$dirpath_data/misc/apep_pump_coord_polygon_parcels_conc.csv", double comma clear
drop pump_lat pump_lon

** Clean GIS variables
destring latlon_group, replace
assert latlon_group!=.
unique latlon_group
assert r(unique)==r(N)

foreach v of varlist parcelid-nearest_parcelacres {
	replace `v' = "" if `v'=="NA"
}
destring parcelacres nearest_dist_km nearest_parcelacres , replace

foreach v of varlist nearest_* {
	assert mi(`v') if mi(parcelid)==0
}

	// Convert from km to miles
replace nearest_dist_km = nearest_dist_km*0.621371
rename nearest_dist_km nearest_miles
sum nearest_miles, detail // p90 = 1.9 but p75 = 0.15
_pctile nearest_miles, p(75(1)90)
return list
local cutoff = 1 // keep the same cutoff as above, which here is 88th pctile

	// Fix logical
replace in_parcel = "1" if in_parcel=="TRUE"
replace in_parcel = "0" if in_parcel=="FALSE"
destring in_parcel, replace
assert inlist(in_parcel,0,1)
	
	// Assign nearest parcel within 1 miles (vast majority not in APEP)
assert nearest_miles==. if in_parcel==1
gen parcel_dist_miles = 0 if in_parcel==1
replace parcel_dist_miles = nearest_miles if parcel_dist_miles==.
sum parcel_dist_miles, det
replace parcelid = nearest_parcelid if parcelid=="" & nearest_parcelid!="" & parcel_dist_miles<`cutoff'

	// Transfer county for nearest parcels
gen temp = subinstr(subinstr(nearest_parcelid,"-","",.),".","",.)
forvalues n = 0/9 {
	replace temp = subinstr(temp,"`n'","",.)
}
replace temp = trim(itrim(temp))
gen temp2 = 0
levelsof parcelcounty, local(levs)
foreach lev in `levs'{
	replace temp2 = 1 if temp=="`lev'"
}
tab temp if temp2!=1 & temp!=""
replace parcelcounty = temp if parcelid!="" & parcelcounty=="" & parcel_dist_miles<`cutoff'
rename parcelcounty parcel_county

	// Transfer acres for nearest parcels
replace parcelacres = nearest_parcelacres if parcelid!="" & parcelacres==. & parcel_dist_miles<`cutoff'
rename parcelacres parcel_acres

	// Drop nearest parcel variables
drop nearest_* temp*	

	// Label
la var in_parcel "Dummy for SPs properly within parcel polygons, CLU-merged parcels"
la var parcelid "Unique parcel ID, CLU-merged parcels (county, area, lon, lat)"	
la var parcel_county "County of assigned parcel, CLU-merged parcels"	
la var parcel_acres "Area (acres) of assigned parcel, CLU-merged parcels"
la var parcel_dist_miles "Distance to assigned parcel, CLU-merged parcels (cut off at 1 mile)"

	// Rename to differentiate from the all-parcel varaibles
rename *parcel_* *parcel_conc_*
rename in_parcel in_parcel_conc
rename parcelid parcelid_conc
	
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

** 17. Assign APEP coordinates to CLUs 
if 1==1{

** Run auxilary GIS script "BUILD_gis_clu_assign.R"

** Import results from GIS script
insheet using "$dirpath_data/misc/apep_pump_coord_polygon_clu.csv", double comma clear
drop pump_lat pump_lon

** Clean GIS variables
destring latlon_group, replace
assert latlon_group!=.
unique latlon_group
assert r(unique)==r(N)

foreach v of varlist clu_id-nearest_clucounty {
	replace `v' = "" if `v'=="NA"
}
destring cluacres nearest_dist_km nearest_cluacres , replace

foreach v of varlist nearest_* {
	assert mi(`v') if mi(clu_id)==0
}

	// Convert from km to miles
replace nearest_dist_km = nearest_dist_km*0.621371
rename nearest_dist_km nearest_miles
sum nearest_miles, detail // p90 = .625
local cutoff = 1 // for consistency with parcles cutoff

	// Fix logical
replace in_clu = "1" if in_clu=="TRUE"
replace in_clu = "0" if in_clu=="FALSE"
destring in_clu, replace
assert inlist(in_clu,0,1)
	
	// Assign nearest CLU within 1 miles (vast majority not in APEP)
assert nearest_miles==. if in_clu==1
gen clu_dist_miles = 0 if in_clu==1
replace clu_dist_miles = nearest_miles if clu_dist_miles==.
sum clu_dist_miles, det
replace clu_id = nearest_clu_id if clu_id=="" & nearest_clu_id!="" & clu_dist_miles<`cutoff'

	// Transfer county for nearest CLUs
replace clucounty = nearest_clucounty if clu_id!="" & clucounty=="" & clu_dist_miles<`cutoff'
rename clucounty clu_county

	// Transfer acres for nearest CLUs
replace cluacres = nearest_cluacres if clu_id!="" & cluacres==. & clu_dist_miles<`cutoff'
rename cluacres clu_acres

	// Drop nearest CLU variables
drop nearest_* 	

	// Label
la var in_clu "Dummy for SPs properly within CLU polygons"
la var clu_id "Unique CLU ID (county, lon, lat, area)"	
la var clu_county "County of assigned CLU"	
la var clu_acres "Area (acres) of assigned CLU"
la var clu_dist_miles "Distance to assigned CLU (cut off at 1 mile)"

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

