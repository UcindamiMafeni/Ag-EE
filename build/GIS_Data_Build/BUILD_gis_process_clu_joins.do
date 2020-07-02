clear all
version 13
set more off

*****************************************************************************
**** Script to create spatial concordance between CLUs and other polygons ***
*****************************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"
global dirpath_code "T:/Home/Louis/backup/AgEE/AgEE_code/build"
*global R_exe_path "C:/PROGRA~1/MIE74D~1/ROPEN~1/bin/x64/R"
*global R_lib_path "C:/Program Files/Microsoft/R Open/R-3.4.4/library"

*******************************************************************************
*******************************************************************************

** 1. Run auxilary GIS script "BUILD_gis_clu_counties.R" to spatially join 
**    CLUs to counties and export to csv

*******************************************************************************
*******************************************************************************

** 2. Process CLU-to-county polygon-to-polygon join
{
insheet using "$dirpath_data/misc/CLU_counties.csv", clear comma names
foreach vy of varlist *y {
	local vx = substr("`vy'",1,length("`vy'")-1) + "x"
	replace `vx' = "" if `vx'=="NA" & `vy'!="NA"
	replace `vy' = "" if `vy'=="NA" & `vx'!="NA"
	replace `vx' = `vy' if `vy'!="" & `vx'==""
	assert inlist(`vy',"",`vx')
	drop `vy'
}
rename *x *
foreach v of varlist * {
	replace `v' = "" if `v'=="NA"
	destring `v', replace
}
assert clu_id!="" & name!=""
 
	// assess county matches
gen county_match = county==name
egen max_county_match = max(county_match), by(clu_id)
tab county_match max_county_match
egen tag_clu = tag(clu_id)
tab max_county_match if tag_clu // not that great?
tab county name if max_county_match==0

	// deal with duplicates: keep if >=80% of matched CLU area is in a single county
gsort clu_id -intacres 
duplicates t clu_id, gen(dup)
unique clu_id if dup>0 //1656 dups
assert d_min==. if dup>0
assert dup==0 if d_min!=.
br clu_id county name county_match max_county_match cluacres intacres tot_int_area dup if dup>0
gen pct_int_area = intacres/tot_int_area
egen max_pct_int_area = max(pct_int_area), by(clu_id)
hist max_pct_int_area if tag_clu & dup>0
unique clu_id
local uniq = r(unique)
drop if dup>0 & max_pct_int_area>0.80 & pct_int_area<max_pct_int_area
unique clu_id
assert r(unique)==`uniq'

	// deal with duplicates: keep if >50% of matched CLU area is in a single county, but flag
duplicates t clu_id, gen(dup2)
unique clu_id if dup2>0 //219 dups
br clu_id county name county_match max_county_match cluacres intacres tot_int_area pct_int_area ///
	max_pct_int_area dup2 if dup2>0
gen flag_multicounty_clu = dup2>0
hist max_pct_int_area if tag_clu & dup2>0
unique clu_id
local uniq = r(unique)
drop if dup2>0 & max_pct_int_area>0.50 & pct_int_area<max_pct_int_area
unique clu_id
assert r(unique)==`uniq'
assert r(unique)==r(N)

	// reconcile acreage mismatches
gen temp = tot_int_area/cluacres	
sum temp, detail // almost systematically off by a factor of 1.6
sort temp // edges of state + very small acreages explain most of the tails of this mismatch
drop cluacres intacres tot_int_area temp

	// deal with unmatched CLUs
gen flag_clu_not_in_county = d_min!=.
tab county_match flag_clu_not_in_county
gen miles_to_nearest_county = d_min*0.000621371 // convert from meters to miles
sum miles_to_nearest_county, detail	
sort miles
br
	// if you're over 3 miles from the nearest county, we'll say that you're not matched to any county
foreach v of varlist statefp-totacres {
	cap replace `v' = . if miles_to_nearest_county>3 & miles_to_nearest_county!=.
	cap replace `v' = "" if miles_to_nearest_county>3 & miles_to_nearest_county!=.
}
replace miles_to_nearest_county = . if miles_to_nearest_county>3	
	
	// clean up
br
keep clu_id county statefp countyfp name county_match flag_multicounty_clu flag_clu_not_in_county miles_to_nearest_county
unique name
unique name statefp countyfp
tostring statefp countyfp, replace
replace statefp = "0" + statefp if length(statefp)<2
replace countyfp = "0" + countyfp if length(countyfp)<3
replace countyfp = "0" + countyfp if length(countyfp)<3
gen fips = statefp + countyfp
assert length(fips)==5
rename county clu_file_county 
rename name county_name 
gen flag_county_mismatch = 1 - county_match
tabstat flag_county_mismatch, by(clu_file_county) s(mean)
drop county_match
foreach v of varlist fips statefp countyfp flag_county_mismatch flag_multicounty_clu {
	cap replace `v' = "" if county_name==""
	cap replace `v' = . if county_name==""
}

	// label
la var clu_id "CLU unique identifier"
la var clu_file_county "County that CLU comes from in raw data"
la var county_name "County that CLU is actually in, geographically speaking"
la var fips "CLU state+county FIPS code"
la var statefp "CLU state FIPS code"
la var countyfp "CLU county FIPS code"
la var flag_county_mismatch "Flag for CLUs in different counties than their raw data file"
la var flag_multicounty_clu "Flag for CLUs where <80% of matched acreage is in a single county"
la var flag_clu_not_in_county "Flag for CLUs that don't spatially overlap with any CA county polygon"
la var miles_to_nearest_county "For unmatched CLUs, miles to the nearest CA county polygon"
order clu_id clu_file_county county_name fips statefp countyfp flag_county_mismatch flag_multicounty_clu

	// create separate variable for nearest county, to avoid ambiguity later on
gen county_name_nearest = county_name if flag_clu_not_in_county==1
gen fips_nearest = fips if flag_clu_not_in_county==1
la var county_name_nearest "Nearest county to CLUs that don't have a proper polygon overlap"
la var fips_nearest "FIPS of nearest county to CLUs that don't have a proper polygon overlap"
foreach v of varlist county_name fips statefp countyfp flag_county_mismatch {
	cap replace `v' = "" if flag_clu_not_in_county==1
	cap replace `v' = . if flag_clu_not_in_county==1
}

	// save
sort clu_id
unique clu_id
assert r(unique)==r(N)
compress
save "$dirpath_data/cleaned_spatial/clu_county_conc.dta", replace

}

*******************************************************************************
*******************************************************************************

** 3. Run auxilary GIS script "BUILD_gis_clu_basins.R" to spatially join 
**    CLUs to basin polygons and export to csv

*******************************************************************************
*******************************************************************************

** 4. Build dataset of California water basins 
{

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

replace basin_name = subinstr(basin_name,"AÃO","AÑO",1)
replace basin_sub_name = subinstr(basin_sub_name,"AÃO","AÑO",1)

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

** 5. Process CLU-to-basin polygon-to-polygon join
{
insheet using "$dirpath_data/misc/CLU_basins.csv", clear comma names
foreach vy of varlist *y {
	local vx = substr("`vy'",1,length("`vy'")-1) + "x"
	replace `vx' = "" if `vx'=="NA" & `vy'!="NA"
	replace `vy' = "" if `vy'=="NA" & `vx'!="NA"
	replace `vx' = `vy' if `vy'!="" & `vx'==""
	assert inlist(`vy',"",`vx')
	drop `vy'
}
rename *x *
foreach v of varlist * {
	replace `v' = "" if `v'=="NA"
	destring `v', replace
}
assert clu_id!="" & objectid!=. & basin_name!=""
egen tag_clu = tag(clu_id)
 
	// deal with duplicates: keep if >=80% of matched CLU area is in a single sub-basin polygon
gsort clu_id -intacres 
duplicates t clu_id, gen(dup)
unique clu_id if dup>0 //1889 dups
br clu_id objectid basin_name basin_su_1 intacres tot_int_area dup if dup>0
gen pct_int_area = intacres/tot_int_area
egen max_pct_int_area = max(pct_int_area), by(clu_id)
hist max_pct_int_area if tag_clu & dup>0
unique clu_id
local uniq = r(unique)
drop if dup>0 & max_pct_int_area>0.80 & pct_int_area<max_pct_int_area
unique clu_id
assert r(unique)==`uniq'

	// deal with duplicates: keep if >50% of matched CLU area is in a single sub-basin polygon, but flag
duplicates t clu_id, gen(dup2)
unique clu_id if dup2>0 //537 dups
br clu_id objectid basin_name basin_su_1 intacres tot_int_area pct_int_area max_pct_int_area dup2 if dup2>0
gen flag_multisubbasin_clu = dup2>0
hist max_pct_int_area if tag_clu & dup2>0
egen temp1 = sum(pct_int_area), by(clu_id basin_name)
assert (temp1>0.999 | temp1<0.8) if dup2>0
gen flag_multibasin_clu = dup2>0 & temp1<0.8
unique clu_id
local uniq = r(unique)
drop if dup2>0 & max_pct_int_area>0.50 & pct_int_area<max_pct_int_area
unique clu_id
assert r(unique)==`uniq'

	// deal with duplicates: keep if plurality of matched CLU area is in a single sub-basin polygon, but flag
duplicates t clu_id, gen(dup3)
unique clu_id if dup3>0 //1 dups
br clu_id objectid basin_name basin_su_1 intacres tot_int_area pct_int_area max_pct_int_area dup3 if dup3>0
unique clu_id
local uniq = r(unique)
drop if dup2>0 & pct_int_area<max_pct_int_area
unique clu_id
assert r(unique)==`uniq'
assert r(unique)==r(N)

	// compare acreages
gen temp = tot_int_area/cluacres	
sum temp, detail // these are clearly in the same units
sort temp // edges of state + very small acreages explain most of the tails of this mismatch
br cluacres tot_int_area temp
gen temp2 = log(cluacres)
twoway scatter temp temp2, msize(vtiny)
drop cluacres intacres tot_int_area temp*

	// deal with unmatched CLUs
gen flag_clu_not_in_basin = d_min!=.
tab flag_clu_not_in_basin
gen miles_to_nearest_basin = d_min*0.000621371 // convert from meters to miles
sum miles_to_nearest_basin, detail	
sort miles
br
	// if you're over 10 miles from the nearest basin, we'll say that you're not matched to basin
foreach v of varlist objectid-d_min{
	cap replace `v' = . if miles_to_nearest_basin>10 & miles_to_nearest_basin!=.
	cap replace `v' = "" if miles_to_nearest_basin>10 & miles_to_nearest_basin!=.
}
replace miles_to_nearest_basin = . if miles_to_nearest_basin>10	

	// clean up and label
br
keep clu_id objectid basin_name basin_su_1 basin_numb basin_subb flag* miles_to_nearest_basin
unique objectid
local uniq = r(unique)
unique objectid basin_name basin_su_1 basin_numb basin_subb
assert r(unique)==`uniq'

rename objectid basin_object_id 
rename basin_numb basin_id
rename basin_subb basin_sub_id
rename basin_su_1 basin_sub_name

replace basin_name = "AÑO NUEVO AREA" if basin_id=="3-020"
replace basin_sub_name = "AÑO NUEVO AREA" if basin_sub_id=="3-020" 
foreach v of varlist basin_name basin_sub_name {
	replace `v' = upper(trim(itrim(`v')))
}

la var clu_id "CLU unique identifier"
la var basin_object_id "Groundwater basin polygon identifier (assigned by GIS)"
la var basin_id "Groundwater basin identifier"
la var basin_sub_id "Groundwater sub-basin identifier"
la var basin_name "Groundwater basin name"
la var basin_sub_name "Groundwater sub-basin name"
la var flag_multisubbasin_clu "Flag for CLUs where <80% of matched acreage is in a single sub-basin"
la var flag_multibasin_clu "Flag for CLUs where <80% of matched acreage is in a single basin"
la var flag_clu_not_in_basin "Flag for CLUs that don't overlap with in a groundwater basin polygon"
la var miles_to_nearest_basin "Miles to nearest groundwater basin, for unmatched CLUs (cut off at 10 miles"

	// Merge in water basin variables to confirm they match
rename basin_id basin_idM
rename basin_sub_id basin_sub_idM
rename basin_name basin_nameM
rename basin_sub_name basin_sub_nameM
joinby basin_object_id using "$dirpath_data/groundwater/ca_water_basins.dta", unmatched(master)
assert _merge==3 if basin_object_id!=.
assert basin_name==basin_nameM if basin_object_id!=.
assert basin_sub_name==basin_sub_nameM if basin_object_id!=.
assert basin_id==basin_idM if basin_object_id!=.
assert basin_sub_id==basin_sub_idM if basin_object_id!=.
drop _merge-basin_sub_area_sqmi
rename *M *

	// save
sort clu_id
unique clu_id
assert r(unique)==r(N)
compress
save "$dirpath_data/cleaned_spatial/clu_basin_conc.dta", replace

}

*******************************************************************************
*******************************************************************************


