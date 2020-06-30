clear all
version 13
set more off

****************************************************************************************
**** Assign PGE (SPs & pump) coordinates and SCE (SP/SA) coordinates to CLU polygons ***
****************************************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"
global dirpath_code "T:/Home/Louis/backup/AgEE/AgEE_code/build"

*******************************************************************************
*******************************************************************************

** 1. Assign counties to points, based on their assigned CLUs
if 1==0{
** PGE SPs
use "$dirpath_data/pge_cleaned/pge_sp_premise_gis.dta", clear
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_county_conc.dta", keep(1 3) ///
	keepusing(county_name fips statefp countyfp) nogen
foreach v of varlist  county_name fips statefp countyfp {
	rename `v' `v'A
}
rename clu_id clu_idA
rename clu_id_ec clu_id 
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_county_conc.dta", keep(1 3) ///
	keepusing(county_name fips statefp countyfp) nogen
foreach v of varlist county_name fips statefp countyfp {
	rename `v' `v'EC
}
rename clu_id clu_id_ec
rename clu_idA clu_id
gen temp_match = fipsA==fipsEC
tab temp_match
tab temp_match if clu_id!=""
rename *A *
foreach v of varlist county_name fips statefp countyfp {
	replace `v' = "" if temp_match==0
}
count if fips!=""
count if fips=="" & clu_id!=""
count if clu_id==""
drop *EC temp_match
compress
save "$dirpath_data/pge_cleaned/pge_sp_premise_gis.dta", replace

** APEP pumps
use "$dirpath_data/pge_cleaned/apep_pump_gis.dta", clear
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_county_conc.dta", keep(1 3) ///
	keepusing(county_name fips statefp countyfp) nogen
foreach v of varlist  county_name fips statefp countyfp {
	rename `v' `v'A
}
rename clu_id clu_idA
rename clu_id_ec clu_id 
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_county_conc.dta", keep(1 3) ///
	keepusing(county_name fips statefp countyfp) nogen
foreach v of varlist county_name fips statefp countyfp {
	rename `v' `v'EC
}
rename clu_id clu_id_ec
rename clu_idA clu_id
gen temp_match = fipsA==fipsEC
tab temp_match
tab temp_match if clu_id!=""
rename *A *
foreach v of varlist county_name fips statefp countyfp {
	replace `v' = "" if temp_match==0
}
count if fips!=""
count if fips=="" & clu_id!=""
count if clu_id==""
drop *EC temp_match
compress
save "$dirpath_data/pge_cleaned/apep_pump_gis.dta", replace

** SCE SPs
use "$dirpath_data/sce_cleaned/sce_sp_premise_gis.dta", clear
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_county_conc.dta", keep(1 3) ///
	keepusing(county_name fips statefp countyfp) nogen
foreach v of varlist  county_name fips statefp countyfp {
	rename `v' `v'A
}
rename clu_id clu_idA
rename clu_id_ec clu_id 
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_county_conc.dta", keep(1 3) ///
	keepusing(county_name fips statefp countyfp) nogen
foreach v of varlist county_name fips statefp countyfp {
	rename `v' `v'EC
}
rename clu_id clu_id_ec
rename clu_idA clu_id
gen temp_match = fipsA==fipsEC
tab temp_match
tab temp_match if clu_id!=""
rename *A *
foreach v of varlist county_name fips statefp countyfp {
	replace `v' = "" if temp_match==0
}
count if fips!=""
count if fips=="" & clu_id!=""
count if clu_id==""
drop *EC temp_match
compress
save "$dirpath_data/sce_cleaned/sce_sp_premise_gis.dta", replace
}

*******************************************************************************
*******************************************************************************

** 2. Assign counties to points directly, and harmonize
if 1==0{

** Run auxiliary GIS scrpt "BUILD_com_points_in_counties.R", which plunks all
** 3 sets of lat/lons into county polygons

** PGE SPs
{
	// import GIS output
import delimited "$dirpath_data/misc/pge_prem_coord_polygon_counties.txt", delimiter("%") clear 
tostring sp_uuid, replace
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
assert length(sp_uuid)==10 & real(sp_uuid)!=.
rename county countyP
rename in_county in_countyP
rename fips fipsP
replace countyP = "" if countyP=="NA"
tempfile counties
save `counties'
	
	// merge GIS output into master
use "$dirpath_data/pge_cleaned/pge_sp_premise_gis.dta", clear
merge 1:1 sp_uuid using `counties'
assert _merge!=2
assert missing_geocode_flag==1 if _merge==1
drop _merge
tab in_countyP

	// transfer county names if CLU-assignment did not find (settle on) a county
br if county_name!=countyP
gen COUNTY = county_name
replace COUNTY = countyP if COUNTY=="" & in_countyP==1 & countyP!="NA"

	// assess remaining missings
sum clu_nearest_dist_m if COUNTY!=countyP, detail // most are not in a CLU polygon
gen flag_county_ques = COUNTY!=countyP
replace COUNTY = countyP if flag_county_ques==1
la var flag_county_ques "Flag for CLU county assignment that conflicts with unit lat/lon"
assert COUNTY!="" if bad_geocode_flag==0 & missing_geocode_flag==0
assert in_countyP==1 if bad_geocode_flag==0 & missing_geocode_flag==0

	// populate FIPS codes
replace fips = fipsP if COUNTY!=county_name
replace county_name = COUNTY	
replace statefp = substr(fips,1,2)
replace countyfp = substr(fips,3,3)	
assert fips==fipsP if fipsP!="NA"
assert county_name==countyP
drop countyP in_countyP fipsP COUNTY

	// relabel
la var county_name "SP county name"
la var fips "SP state+county FIPS code"
la var statefp "SP state FIPS code"
la var countyfp "SP county FIPS code"

	// save
sort sp_uuid
unique sp_uuid
assert r(unique)==r(N)
compress
save "$dirpath_data/pge_cleaned/pge_sp_premise_gis.dta", replace
}

** APEP pumps
{
	// import GIS output
import delimited "$dirpath_data/misc/apep_pump_coord_polygon_counties.txt", delimiter("%") clear 
destring latlon_group, replace
assert latlon_group!=.
unique latlon_group
assert r(unique)==r(N)
rename county countyP
rename in_county in_countyP
rename fips fipsP
replace countyP = "" if countyP=="NA"
tempfile counties
save `counties'
	
	// merge GIS output into master
use "$dirpath_data/pge_cleaned/apep_pump_gis.dta", clear
merge m:1 latlon_group using `counties'
assert _merge==3
drop _merge
tab in_countyP

	// transfer county names if CLU-assignment did not find (settle on) a county
br if county_name!=countyP
gen COUNTY = county_name
replace COUNTY = countyP if COUNTY=="" & in_countyP==1 & countyP!="NA"

	// assess remaining missings
sum clu_nearest_dist_m if COUNTY!=countyP, detail // most are not in a CLU polygon
gen flag_county_ques = COUNTY!=countyP
replace COUNTY = countyP if flag_county_ques==1
la var flag_county_ques "Flag for CLU county assignment that conflicts with unit lat/lon"
count if COUNTY=="" 

	// populate FIPS codes
replace fips = fipsP if COUNTY!=county_name
replace county_name = COUNTY	
replace statefp = substr(fips,1,2)
replace countyfp = substr(fips,3,3)	
assert fips==fipsP if fipsP!="NA"
assert county_name==countyP
drop countyP in_countyP fipsP COUNTY

	// relabel
la var county_name "APEP pump county name"
la var fips "APEP pump state+county FIPS code"
la var statefp "APEP pump state FIPS code"
la var countyfp "APEP pump county FIPS code"

	// save
sort latlon_group
unique latlon_group apeptestid
assert r(unique)==r(N)
compress
save "$dirpath_data/pge_cleaned/apep_pump_gis.dta", replace
}

** SCE SPs
{
	// import GIS output
import delimited "$dirpath_data/misc/sce_prem_coord_polygon_counties.txt", delimiter("%") clear 
tostring sp_uuid, replace
assert real(sp_uuid)!=.
rename county countyP
rename in_county in_countyP
rename fips fipsP
replace countyP = "" if countyP=="NA"
tempfile counties
save `counties'
	
	// merge GIS output into master
use "$dirpath_data/sce_cleaned/sce_sp_premise_gis.dta", clear
merge 1:1 sp_uuid using `counties'
assert _merge!=2
assert missing_geocode_flag==1 if _merge==1
drop _merge
tab in_countyP

	// transfer county names if CLU-assignment did not find (settle on) a county
br if county_name!=countyP
gen COUNTY = county_name
replace COUNTY = countyP if COUNTY=="" & in_countyP==1 & countyP!="NA"

	// assess remaining missings
sum clu_nearest_dist_m if COUNTY!=countyP, detail // most are not in a CLU polygon
gen flag_county_ques = COUNTY!=countyP
replace COUNTY = countyP if flag_county_ques==1
la var flag_county_ques "Flag for CLU county assignment that conflicts with unit lat/lon"
assert COUNTY!="" if bad_geocode_flag==0 & missing_geocode_flag==0
assert in_countyP==1 if bad_geocode_flag==0 & missing_geocode_flag==0

	// populate FIPS codes
replace fips = fipsP if COUNTY!=county_name
replace county_name = COUNTY	
replace statefp = substr(fips,1,2)
replace countyfp = substr(fips,3,3)	
assert fips==fipsP if fipsP!="NA"
assert county_name==countyP
drop countyP in_countyP fipsP COUNTY

	// relabel
la var county_name "SP county name"
la var fips "SP state+county FIPS code"
la var statefp "SP state FIPS code"
la var countyfp "SP county FIPS code"

	// save
sort sp_uuid
unique sp_uuid
assert r(unique)==r(N)
compress
save "$dirpath_data/sce_cleaned/sce_sp_premise_gis.dta", replace
}

}

*******************************************************************************
*******************************************************************************

LEFTOVER CODE
{

	// Label
la var wdist "Water district (assigned by GIS)"	
la var wdist_dist_miles "Distance to water district (cut off at 2.13 miles)"	
la var wdist_id "Water district ID (from shapefile)"
la var wdist_area_sqmi "GIS-derived area of water district (sq miles)"


	// Label
la var in_parcel "Dummy for SPs properly within parcel polygons, all parcels"
la var parcelid "Unique parcel ID, all parcels (county, area, lon, lat)"	
la var parcel_county "County of assigned parcel, all parcels"	
la var parcel_acres "Area (acres) of assigned parcel, all parcels"
la var parcel_dist_miles "Distance to assigned parcel, all parcels (cut off at 1 mile)"

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
	
}	

