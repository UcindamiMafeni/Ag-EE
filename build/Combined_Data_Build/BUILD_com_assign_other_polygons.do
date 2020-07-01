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
if 1==1{

** 1a. PGE SPs
{
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
}

** 1b. APEP pumps
{
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
}

** 1c. SCE SPs
{
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

}

*******************************************************************************
*******************************************************************************

** 2. Assign counties to points directly, and harmonize
if 1==1{

** Run auxiliary GIS scrpt "BUILD_com_points_in_counties.R", which plunks all
** 3 sets of lat/lons into county polygons

** 2a. PGE SPs
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
replace flag_county_ques = . if fips==""

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

** 2b. APEP pumps
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
replace flag_county_ques = . if fips==""

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

** 2c. SCE SPs
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
replace flag_county_ques = . if fips==""

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

** 3. Assign groundwater basins to points, based on their assigned CLUs
if 1==1{

** 3a. PGE SPs
{
use "$dirpath_data/pge_cleaned/pge_sp_premise_gis.dta", clear
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_basin_conc.dta", keep(1 3) nogen
foreach v of varlist basin_object_id-miles_to_nearest_basin {
	rename `v' `v'A
}
rename clu_id clu_idA
rename clu_id_ec clu_id 
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_basin_conc.dta", keep(1 3) nogen
drop basin_object_id
foreach v of varlist basin_id-miles_to_nearest_basin {
	rename `v' `v'EC
}
rename clu_id clu_id_ec
rename clu_idA clu_id
gen temp_match = basin_sub_idA==basin_sub_idEC
tab temp_match
tab temp_match if clu_id!=""
rename *A *
foreach v of varlist basin_id-miles_to_nearest_basin {
	cap replace `v' = "" if temp_match==0
	cap replace `v' = . if temp_match==0
}
count if basin_sub_id!=""
count if basin_sub_id=="" & clu_id!=""
count if basin_sub_id=="" & clu_id==""
assert basin_sub_id=="" if clu_id==""
drop *EC temp_match
compress
save "$dirpath_data/pge_cleaned/pge_sp_premise_gis.dta", replace
}

** 3b. APEP pumps
{
use "$dirpath_data/pge_cleaned/apep_pump_gis.dta", clear
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_basin_conc.dta", keep(1 3) nogen
foreach v of varlist basin_object_id-miles_to_nearest_basin {
	rename `v' `v'A
}
rename clu_id clu_idA
rename clu_id_ec clu_id 
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_basin_conc.dta", keep(1 3) nogen
drop basin_object_id
foreach v of varlist basin_id-miles_to_nearest_basin {
	rename `v' `v'EC
}
rename clu_id clu_id_ec
rename clu_idA clu_id
gen temp_match = basin_sub_idA==basin_sub_idEC
tab temp_match
tab temp_match if clu_id!=""
rename *A *
foreach v of varlist basin_id-miles_to_nearest_basin {
	cap replace `v' = "" if temp_match==0
	cap replace `v' = . if temp_match==0
}
count if basin_sub_id!=""
count if basin_sub_id=="" & clu_id!=""
count if basin_sub_id=="" & clu_id==""
assert basin_sub_id=="" if clu_id==""
drop *EC temp_match
compress
save "$dirpath_data/pge_cleaned/apep_pump_gis.dta", replace
}

** 3c. SCE SPs
{
use "$dirpath_data/sce_cleaned/sce_sp_premise_gis.dta", clear
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_basin_conc.dta", keep(1 3) nogen
foreach v of varlist basin_object_id-miles_to_nearest_basin {
	rename `v' `v'A
}
rename clu_id clu_idA
rename clu_id_ec clu_id 
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_basin_conc.dta", keep(1 3) nogen
drop basin_object_id
foreach v of varlist basin_id-miles_to_nearest_basin {
	rename `v' `v'EC
}
rename clu_id clu_id_ec
rename clu_idA clu_id
gen temp_match = basin_sub_idA==basin_sub_idEC
tab temp_match
tab temp_match if clu_id!=""
rename *A *
foreach v of varlist basin_id-miles_to_nearest_basin {
	cap replace `v' = "" if temp_match==0
	cap replace `v' = . if temp_match==0
}
count if basin_sub_id!=""
count if basin_sub_id=="" & clu_id!=""
count if basin_sub_id=="" & clu_id==""
assert basin_sub_id=="" if clu_id==""
drop *EC temp_match
compress
save "$dirpath_data/sce_cleaned/sce_sp_premise_gis.dta", replace
}

}

*******************************************************************************
*******************************************************************************

** 4. Assign groundwater basins to points directly, and harmonize
if 1==1{

** Run auxiliary GIS scrpt "BUILD_com_points_in_water_basins.R", which plunks all
** 3 sets of lat/lons into groundwater basin polygons

** 4a. PGE SPs
{
	// import GIS output
import delimited "$dirpath_data/misc/pge_prem_coord_polygon_wbasn.txt", delimiter("%") clear 
tostring sp_uuid, replace
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
assert length(sp_uuid)==10 & real(sp_uuid)!=.
rename wbasn basin_nameP
rename in_wbasn in_wbasnP
rename wbasn_id basin_object_id
rename nearestwbasn_dist_m miles_to_nearest_basinP
replace miles_to_nearest_basinP = miles_to_nearest_basinP*0.000621371 // convert from meters to miles
replace basin_nameP = "" if basin_nameP=="NA"
replace basin_object_id = "" if basin_object_id=="NA"
destring basin_object_id, replace
replace basin_object_id = nearestwbasn_id if basin_object_id==. & nearestwbasn_id!=0
assert basin_object_id!=.
drop nearestwbasn_id
merge m:1 basin_object_id using "$dirpath_data/groundwater/ca_water_basins.dta", keep(1 3)
assert _merge==3
drop _merge basin_reg_off basin_sub_area_sqmi
tab basin_nameP if basin_name!=basin_nameP
drop basin_nameP
foreach v of varlist basin_object_id basin_id basin_sub_id basin_name basin_sub_name {
	rename `v' `v'P
}
tempfile basins
save `basins'
	
	// merge GIS output into master
use "$dirpath_data/pge_cleaned/pge_sp_premise_gis.dta", clear
merge 1:1 sp_uuid using `basins'
assert _merge!=2
assert missing_geocode_flag==1 if _merge==1
drop _merge
tab in_wbasnP

	// transfer basin IDs if CLU-assignment did not find (settle on) a basin
br if basin_object_id!=basin_object_idP
gen BASIN_OBJECT_ID = basin_object_id
replace BASIN_OBJECT_ID = basin_object_idP if BASIN_OBJECT_ID==. & in_wbasnP==1 ///
	& basin_object_idP!=.
replace BASIN_OBJECT_ID = basin_object_idP if BASIN_OBJECT_ID==. & in_wbasnP==0 ///
	& basin_object_idP!=. & miles_to_nearest_basinP<=10

	// assess remaining missings
br if BASIN_OBJECT_ID==. & prem_lat!=. & prem_long!=.
sum clu_nearest_dist_m if BASIN_OBJECT_ID!=basin_object_idP, detail // over 75% not in a CLU polygon
gen flag_basin_sub_ques = BASIN_OBJECT_ID!=basin_object_idP
replace BASIN_OBJECT_ID = basin_object_idP if flag_basin_sub_ques==1
la var flag_basin_sub_ques "Flag for CLU sub-basin assignment that conflicts with unit lat/lon"
gen flag_basin_ques = flag_basin_sub_ques
replace flag_basin_ques = 0 if basin_name==basin_nameP
la var flag_basin_ques "Flag for CLU basin assignment that conflicts with unit lat/lon"

	// revise questionable basin flags when benchmarking against questionable CLU assignemnts
tab flag_multisubbasin_clu flag_basin_sub_ques if miles_to_nearest_basinP<10
tab flag_multibasin_clu flag_basin_sub_ques if miles_to_nearest_basinP<10
tab flag_clu_not_in_basin flag_basin_sub_ques if miles_to_nearest_basinP<10
tab in_clu flag_basin_sub_ques if miles_to_nearest_basinP<10
replace flag_basin_sub_ques = 0 if in_clu==0 
replace flag_basin_ques = 0 if in_clu==0
replace flag_basin_sub_ques = 0 if flag_multisubbasin_clu==1
replace flag_basin_ques = 0 if flag_multibasin_clu==1
drop flag_multisubbasin_clu flag_multibasin_clu flag_clu_not_in_basin

	// populate a single set of basin variables
replace basin_object_id = BASIN_OBJECT_ID
replace basin_id = basin_idP
replace basin_sub_id = basin_sub_idP
replace basin_name = basin_nameP
replace basin_sub_name = basin_sub_nameP
replace miles_to_nearest_basin = miles_to_nearest_basinP
rename in_wbasnP in_basin
assert basin_object_id==basin_object_idP
drop *basin*P BASIN_OBJECT_ID

	// enforce 10-miles cutoff on nearest basin assignments
gen temp = miles_to_nearest_basin>10
tab temp
assert inlist(in_basin,0,.) if temp==1
replace in_basin = 0 if temp==1
foreach v of varlist basin_*id basin_*name miles_to_nearest_basin flag_basin* {
	cap replace `v' = . if temp==1
	cap replace `v' = "" if temp==1
}
drop temp

	// relabel
la var miles_to_nearest_basin "Miles to nearest groundwater basin, cut off at 10"
la var in_basin "Dummy for SPs properly within a groundwater basin"
order miles_to_nearest_basin, after(in_basin)

	// save
sort sp_uuid
unique sp_uuid
assert r(unique)==r(N)
compress
save "$dirpath_data/pge_cleaned/pge_sp_premise_gis.dta", replace
}

** 4b. APEP pumps
{
	// import GIS output
import delimited "$dirpath_data/misc/apep_pump_coord_polygon_wbasn.txt", delimiter("%") clear 
destring latlon_group, replace
assert latlon_group!=.
unique latlon_group
assert r(unique)==r(N)
rename wbasn basin_nameP
rename in_wbasn in_wbasnP
rename wbasn_id basin_object_id
rename nearestwbasn_dist_m miles_to_nearest_basinP
replace miles_to_nearest_basinP = miles_to_nearest_basinP*0.000621371 // convert from meters to miles
replace basin_nameP = "" if basin_nameP=="NA"
replace basin_object_id = "" if basin_object_id=="NA"
destring basin_object_id, replace
replace basin_object_id = nearestwbasn_id if basin_object_id==. & nearestwbasn_id!=0
assert basin_object_id!=.
drop nearestwbasn_id
merge m:1 basin_object_id using "$dirpath_data/groundwater/ca_water_basins.dta", keep(1 3)
assert _merge==3
drop _merge basin_reg_off basin_sub_area_sqmi
tab basin_nameP if basin_name!=basin_nameP
drop basin_nameP
foreach v of varlist basin_object_id basin_id basin_sub_id basin_name basin_sub_name {
	rename `v' `v'P
}
tempfile basins
save `basins'

	// merge GIS output into master
use "$dirpath_data/pge_cleaned/apep_pump_gis.dta", clear
merge m:1 latlon_group using `basins'
assert _merge==3
drop _merge
tab in_wbasnP

	// transfer basin IDs if CLU-assignment did not find (settle on) a basin
br if basin_object_id!=basin_object_idP
gen BASIN_OBJECT_ID = basin_object_id
replace BASIN_OBJECT_ID = basin_object_idP if BASIN_OBJECT_ID==. & in_wbasnP==1 ///
	& basin_object_idP!=.
replace BASIN_OBJECT_ID = basin_object_idP if BASIN_OBJECT_ID==. & in_wbasnP==0 ///
	& basin_object_idP!=. & miles_to_nearest_basinP<=10

	// assess remaining missings
br if BASIN_OBJECT_ID==.
sum clu_nearest_dist_m if BASIN_OBJECT_ID!=basin_object_idP, detail // over 75% not in a CLU polygon
gen flag_basin_sub_ques = BASIN_OBJECT_ID!=basin_object_idP
replace BASIN_OBJECT_ID = basin_object_idP if flag_basin_sub_ques==1
la var flag_basin_sub_ques "Flag for CLU sub-basin assignment that conflicts with unit lat/lon"
gen flag_basin_ques = flag_basin_sub_ques
replace flag_basin_ques = 0 if basin_name==basin_nameP
la var flag_basin_ques "Flag for CLU basin assignment that conflicts with unit lat/lon"

	// revise questionable basin flags when benchmarking against questionable CLU assignemnts
tab flag_multisubbasin_clu flag_basin_sub_ques if miles_to_nearest_basinP<10
tab flag_multibasin_clu flag_basin_sub_ques if miles_to_nearest_basinP<10
tab flag_clu_not_in_basin flag_basin_sub_ques if miles_to_nearest_basinP<10
tab in_clu flag_basin_sub_ques if miles_to_nearest_basinP<10
replace flag_basin_sub_ques = 0 if in_clu==0 
replace flag_basin_ques = 0 if in_clu==0
replace flag_basin_sub_ques = 0 if flag_multisubbasin_clu==1
replace flag_basin_ques = 0 if flag_multibasin_clu==1
drop flag_multisubbasin_clu flag_multibasin_clu flag_clu_not_in_basin

	// populate a single set of basin variables
replace basin_object_id = BASIN_OBJECT_ID
replace basin_id = basin_idP
replace basin_sub_id = basin_sub_idP
replace basin_name = basin_nameP
replace basin_sub_name = basin_sub_nameP
replace miles_to_nearest_basin = miles_to_nearest_basinP
rename in_wbasnP in_basin
assert basin_object_id==basin_object_idP
drop *basin*P BASIN_OBJECT_ID

	// enforce 10-miles cutoff on nearest basin assignments
gen temp = miles_to_nearest_basin>10
tab temp
assert inlist(in_basin,0,.) if temp==1
replace in_basin = 0 if temp==1
foreach v of varlist basin_*id basin_*name miles_to_nearest_basin flag_basin* {
	cap replace `v' = . if temp==1
	cap replace `v' = "" if temp==1
}
drop temp

	// relabel
la var miles_to_nearest_basin "Miles to nearest groundwater basin, cut off at 10"
la var in_basin "Dummy for pumps properly within a groundwater basin"
order miles_to_nearest_basin, after(in_basin)

	// save
sort latlon_group
unique latlon_group apeptestid
assert r(unique)==r(N)
compress
save "$dirpath_data/pge_cleaned/apep_pump_gis.dta", replace
}

** 4c. SCE SPs
{
	// import GIS output
import delimited "$dirpath_data/misc/sce_prem_coord_polygon_wbasn.txt", delimiter("%") clear 
tostring sp_uuid, replace
assert real(sp_uuid)!=.
rename wbasn basin_nameP
rename in_wbasn in_wbasnP
rename wbasn_id basin_object_id
rename nearestwbasn_dist_m miles_to_nearest_basinP
replace miles_to_nearest_basinP = miles_to_nearest_basinP*0.000621371 // convert from meters to miles
replace basin_nameP = "" if basin_nameP=="NA"
replace basin_object_id = "" if basin_object_id=="NA"
destring basin_object_id, replace
replace basin_object_id = nearestwbasn_id if basin_object_id==. & nearestwbasn_id!=0
assert basin_object_id!=.
drop nearestwbasn_id
merge m:1 basin_object_id using "$dirpath_data/groundwater/ca_water_basins.dta", keep(1 3)
assert _merge==3
drop _merge basin_reg_off basin_sub_area_sqmi
tab basin_nameP if basin_name!=basin_nameP
drop basin_nameP
foreach v of varlist basin_object_id basin_id basin_sub_id basin_name basin_sub_name {
	rename `v' `v'P
}
tempfile basins
save `basins'
	
	// merge GIS output into master
use "$dirpath_data/sce_cleaned/sce_sp_premise_gis.dta", clear
merge 1:1 sp_uuid using `basins'
assert _merge!=2
assert missing_geocode_flag==1 if _merge==1
drop _merge
tab in_wbasnP

	// transfer basin IDs if CLU-assignment did not find (settle on) a basin
br if basin_object_id!=basin_object_idP
gen BASIN_OBJECT_ID = basin_object_id
replace BASIN_OBJECT_ID = basin_object_idP if BASIN_OBJECT_ID==. & in_wbasnP==1 ///
	& basin_object_idP!=.
replace BASIN_OBJECT_ID = basin_object_idP if BASIN_OBJECT_ID==. & in_wbasnP==0 ///
	& basin_object_idP!=. & miles_to_nearest_basinP<=10

	// assess remaining missings
br if BASIN_OBJECT_ID==. & prem_lat!=. & prem_long!=.
sum clu_nearest_dist_m if BASIN_OBJECT_ID!=basin_object_idP, detail // over 75% not in a CLU polygon
gen flag_basin_sub_ques = BASIN_OBJECT_ID!=basin_object_idP
replace BASIN_OBJECT_ID = basin_object_idP if flag_basin_sub_ques==1
la var flag_basin_sub_ques "Flag for CLU sub-basin assignment that conflicts with unit lat/lon"
gen flag_basin_ques = flag_basin_sub_ques
replace flag_basin_ques = 0 if basin_name==basin_nameP
la var flag_basin_ques "Flag for CLU basin assignment that conflicts with unit lat/lon"

	// revise questionable basin flags when benchmarking against questionable CLU assignemnts
tab flag_multisubbasin_clu flag_basin_sub_ques if miles_to_nearest_basinP<10
tab flag_multibasin_clu flag_basin_sub_ques if miles_to_nearest_basinP<10
tab flag_clu_not_in_basin flag_basin_sub_ques if miles_to_nearest_basinP<10
tab in_clu flag_basin_sub_ques if miles_to_nearest_basinP<10
replace flag_basin_sub_ques = 0 if in_clu==0 
replace flag_basin_ques = 0 if in_clu==0
replace flag_basin_sub_ques = 0 if flag_multisubbasin_clu==1
replace flag_basin_ques = 0 if flag_multibasin_clu==1
drop flag_multisubbasin_clu flag_multibasin_clu flag_clu_not_in_basin

	// populate a single set of basin variables
replace basin_object_id = BASIN_OBJECT_ID
replace basin_id = basin_idP
replace basin_sub_id = basin_sub_idP
replace basin_name = basin_nameP
replace basin_sub_name = basin_sub_nameP
replace miles_to_nearest_basin = miles_to_nearest_basinP
rename in_wbasnP in_basin
assert basin_object_id==basin_object_idP
drop *basin*P BASIN_OBJECT_ID

	// enforce 10-miles cutoff on nearest basin assignments
gen temp = miles_to_nearest_basin>10
tab temp
assert inlist(in_basin,0,.) if temp==1
replace in_basin = 0 if temp==1
foreach v of varlist basin_*id basin_*name miles_to_nearest_basin flag_basin* {
	cap replace `v' = . if temp==1
	cap replace `v' = "" if temp==1
}
drop temp

	// relabel
la var miles_to_nearest_basin "Miles to nearest groundwater basin, cut off at 10"
la var in_basin "Dummy for SPs properly within a groundwater basin"
order miles_to_nearest_basin, after(in_basin)

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

