clear all
version 13
set more off

****************************************************************************************
**** Assign PGE (SPs & pump) coordinates and SCE (SP/SA) coordinates to CLU polygons ***
****************************************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"
global dirpath_code "T:/Home/Louis/backup/AgEE/AgEE_code/build"
*global R_exe_path "C:/PROGRA~1/MIE74D~1/ROPEN~1/bin/x64/R"
*global R_lib_path "C:/Program Files/Microsoft/R Open/R-3.4.4/library"

*******************************************************************************
*******************************************************************************

** 1. Save PGE SP coordinates (techincally *premise* coordinates)
if 1==1{

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

** Make unique by SP & coordinates (for SPs appearing in multiple pulls)
duplicates t sp_uuid prem_lat prem_long, gen(dup)
egen temp_max = max(pull=="20180322"), by(sp_uuid prem_lat prem_long)
unique sp_uuid prem_lat prem_long
local uniq = r(unique)
drop if temp_max==1 & pull!="20180322"
unique sp_uuid prem_lat prem_long
assert r(unique)==`uniq'
assert r(unique)==r(N)
drop dup temp_max

** Make unique by SP (for SPs appearing in multiple pulls)
duplicates t sp_uuid, gen(dup)
sort sp_uuid pull
br if dup>0
gen temp_lat = prem_lat[_n+1] if sp_uuid==sp_uuid[_n+1]
gen temp_long = prem_long[_n+1] if sp_uuid==sp_uuid[_n+1]
geodist prem_lat prem_long temp_lat temp_long, gen(temp_dist)
sum temp_dist, detail // only 23 discrpancies, and 75% are <0.44km apart
egen temp_max = max(pull=="20180827"), by(sp_uuid)
unique sp_uuid
local uniq = r(unique)
drop if temp_max==1 & pull!="20180827" // keep later data pull
unique sp_uuid
assert r(unique)==`uniq'
assert r(unique)==r(N)
drop dup temp*

** Export coordinates
if 1==0 {
	preserve
	drop if missing_geocode_flag==1
	outsheet using "$dirpath_data/misc/pge_prem_coord_3pulls.txt", comma replace
	restore
}	

** Save
la var pull "Which PGE data pull did this SP come from?"
compress
save "$dirpath_data/pge_cleaned/pge_sp_premise_gis.dta", replace

}
	
*******************************************************************************
*******************************************************************************

** 2. Save APEP pump coordinates
if 1==1{

** Load APEP test dataset
use "$dirpath_data/pge_cleaned/apep_pump_test_data.dta", clear

** Group by pump lat/lon
egen latlon_group = group(pumplatnew pumplongnew)

** Establish a unique ID, since there isn't one!
unique apeptestid test_date_stata crop
assert r(unique)==r(N)

** Keep only pump lat/lon
keep apeptestid test_date_stata crop pumplatnew pumplongnew latlon_group

** Export coordinates
if 1==0 {
	preserve
	drop if latlon_group==.
	keep pumplatnew pumplongnew latlon_group
	duplicates drop
	rename pumplatnew pump_lat
	rename pumplongnew pump_long
	outsheet using "$dirpath_data/misc/apep_pump_coord.txt", comma replace
	restore
}
	
** Save
la var latlon_group "APEP lat/lon identifier"
compress
save "$dirpath_data/pge_cleaned/apep_pump_gis.dta", replace

}

*******************************************************************************
*******************************************************************************

** 3. Save SCE SP/SA coordinates
if 1==1{

** Load all 1 SCE customer datasets
use "$dirpath_data/sce_cleaned/sce_cust_detail_20190916.dta", clear
gen pull = "20190916"

** Keep relevant variables
keep pull sp_uuid sa_uuid prem_lat prem_long bad_geocode_flag missing_geocode_flag ///
	sa_start sa_stop in_billing bill_dt* in_interval interval_dt*
duplicates drop 
unique sp_uuid sa_uuid pull prem_lat prem_lon
assert r(unique)==r(N)
unique sp_uuid sa_uuid
assert r(unique)==r(N)

/*
** Make unique by SP/SA & coordinates (for SPs appearing in multiple pulls)
TURN ON THIS CODE WHEN WE HAVE MULTIPE PULLS
duplicates t sp_uuid sa_uuid prem_lat prem_long, gen(dup)
egen temp_max = max(pull=="20180322"), by(sp_uuid sa_uuid prem_lat prem_long)
unique sp_uuid sa_uuid prem_lat prem_long
local uniq = r(unique)
drop if temp_max==1 & pull!="20190916"
unique sp_uuid sa_uuid prem_lat prem_long
assert r(unique)==`uniq'
assert r(unique)==r(N)
drop dup temp_max
*/

** Confirm consisten lat/lon within SPs
egen latlon_group = group(prem_lat prem_long)
egen temp_min = min(latlon_group), by(sp_uuid)
egen temp_max = max(latlon_group), by(sp_uuid)
assert temp_min==temp_max if sp_uuid!="" // lat/lon never conflict within an SP

** What about lat/lons that are missing SP ID, but have SA ID?
egen temp_min2 = min(sp_uuid==""), by(latlon_group)
egen temp_max2 = max(sp_uuid==""), by(latlon_group)
unique latlon_group if latlon_group!=.
unique latlon_group if latlon_group!=. & sp_uuid==""
unique latlon_group if temp_min2<temp_max2 
	// 1050 of 1162 lat/lons with missing SP share identical lat/lon with a non-missing SP	
unique latlon if latlon_group!=. & temp_min2==1
	// we'll lose 113 lat/lons for SAs with no colocated SP
unique latlon if latlon_group!=. & temp_min2==1 & in_billing==1
	// none of these 113 lat/lons are in our billing data
drop sa_uuid sa_start sa_stop in_billing bill_dt_first bill_dt_last ///
	in_interval interval_dt_first interval_dt_last latlon_group temp*

** Make unique by SP
drop if sp_uuid==""
duplicates drop 
unique sp_uuid
assert r(unique)==r(N)

** Export coordinates
if 1==0 {
	preserve
	drop if missing_geocode_flag==1
	outsheet using "$dirpath_data/misc/sce_prem_coord_1pull.txt", comma replace
	restore
}
	
** Save
la var pull "Which SCE data pull did this SP come from?"
compress
save "$dirpath_data/sce_cleaned/sce_sp_premise_gis.dta", replace

}
	
*******************************************************************************
*******************************************************************************

** 4. Run auxilary GIS script "BUILD_com_clu_assign.R"
**    This plunks lat/lons in CLU polygons, and finds the distance to nearest polygons
**    3 set of lat/lons: PGE SPs, PGE pumps, SCE SPs

*******************************************************************************
*******************************************************************************

** 5. Assign PGE SP coordinates to CLUs (all CLUs)
if 1==1{

** Import results from GIS script
insheet using "$dirpath_data/misc/pge_prem_coord_polygon_clu.csv", double comma clear
drop prem_lon prem_lat bad_geocode_flag

** Clean GIS variables
	// SP identifiers
tostring sp_uuid pull, replace
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
assert length(sp_uuid)==10 & real(sp_uuid)!=.
unique sp_uuid pull
assert r(unique)==r(N)
unique sp_uuid 
assert r(unique)==r(N)

	// Fix logical
replace in_clu = "1" if in_clu=="TRUE"
replace in_clu = "0" if in_clu=="FALSE"
destring in_clu, replace
assert inlist(in_clu,0,1)
tab pull in_clu

	// Remove NAs and destring
foreach v of varlist clu_id-nearest2_cluacres {
	replace `v' = "" if `v'=="NA"
}
destring *_m *acres, replace

foreach v of varlist nearest_* {
	assert mi(`v') if mi(clu_id)==0
}

** Some checks for sanity and internal consistency
foreach v of varlist clu_id cluacres edge_dist_m neighbor* {
	assert nearest_clu_id=="" if !mi(`v')
	assert nearest2_clu_id=="" if !mi(`v')
	assert nearest_clu_id!="" if mi(`v')
	assert nearest2_clu_id!="" if mi(`v')
}
foreach v of varlist nearest* {
	assert clu_id=="" if !mi(`v')
	assert clu_id!="" if mi(`v')
}

	// is 2nd-nearest always different from 1st-nearest?
assert nearest_clu_id!=nearest2_clu_id if nearest_clu_id!="" // yes

	// is 2nd-nearest always weakly further than 1st-nearest?
gen temp = nearest2_dist_m - nearest_dist_m
sort temp
br
sum temp, detail // not always!
sum nearest_dist_m if temp<0, detail
sum nearest_dist_m if temp>=0, detail
sum nearest_dist_m if temp<0 & pull=="20180719", detail // ~100 SPs where we might have an actual issue
sum nearest_dist_m if temp>=0 & pull=="20180719", detail
gen flag_nearest_clu_error = temp<0

	// is neighbor always weakly further than edge?
gen temp2 = neighbor_dist_m - edge_dist_m	
sort temp2
br
sum temp2, detail // almost always, save for 154 SPs
replace flag_nearest_clu_error = 1 if temp2<0


** Diagnostics on distance to edge
sum edge_dist_m, detail // 50% of matches <15m from the edge of their polygons
sum nearest_dist_m, detail // 50% of nonmatches <25m from nearest polygons

** Diagnostics on CLU assignment
sum neighbor_dist_m, detail // 50% of matches <40m from nearest neighboring polygon
sum nearest2_dist_m, detail	// 50% of nonmatches <86m from 2nd-nearest polygon

** Transfer assignmentss
replace clu_id = nearest_clu_id if in_clu==0 & clu_id==""
replace edge_dist_m = nearest_dist_m if in_clu==0 & edge_dist_m==.
replace cluacres = nearest_cluacres if in_clu==0 & cluacres==.
replace neighbor_clu_id = nearest2_clu_id if in_clu==0 & neighbor_clu_id==""
replace neighbor_dist_m = nearest2_dist_m if in_clu==0 & neighbor_dist_m==.
replace neighbor_cluacres = nearest2_cluacres if in_clu==0& neighbor_cluacres==.
drop nearest* temp*
	
** Remove assignments more than 1km in distance
assert edge_dist_m!=.
gen temp = in_clu==0 & edge_dist_m>1000
tab pull temp
replace clu_id = "" if temp==1
replace cluacres = . if temp==1
replace edge_dist_m = . if temp==1
replace neighbor_clu_id = "" if temp==1
replace neighbor_dist_m = . if temp==1
replace neighbor_cluacres = . if temp==1
replace flag_nearest_clu_error = 0 if temp==1
drop temp
gen clu_nearest_dist_m = edge_dist_m
replace clu_nearest_dist_m = 0 if in_clu==1

** Label
rename cluacres clu_acres
rename edge_dist_m clu_edge_dist_m
rename neighbor_dist_m neighbor_clu_dist_m
la var in_clu "Dummy for SPs properly within CLU polygons"
la var clu_id "Unique CLU ID (county, lon, lat, area)"	
la var clu_acres "Area (acres) of assigned CLU"
la var clu_edge_dist_m "Meters to edge of assigned CLU polygon (cut off at 1 km for unmatched)"
la var clu_nearest_dist_m "Meters to assigned CLU polygon (cut off at 1 km, 0 for matched)"
la var neighbor_clu_dist_m "Meters to nearest non-assigned CLU polygon"
la var flag_nearest_clu_error "Flag for errors in GIS nearest feature algorithm"
drop neighbor_clu_id neighbor_cluacres // don't think we actually need these
drop pull // not necessary for merge
order sp_uuid in_clu clu_id clu_acres clu_edge_dist_m clu_nearest_dist_m 

** Store temp file
tempfile gis_out
save `gis_out'

** Merge results into merge back into main dataset
use "$dirpath_data/pge_cleaned/pge_sp_premise_gis.dta", clear
merge 1:1 sp_uuid using `gis_out'	
assert _merge!=2
assert missing_geocode_flag==1 if _merge==1 // confirm that all non-merges have missing lat/lon
assert _merge==1 if missing_geocode_flag==1 // confirm that missing lat/lons don't merge
replace in_clu = 0 if _merge==1 // flag missing lat/lons as non-merges
assert _merge==3 if prem_lat!=. & prem_lon!=.
assert prem_lat!=. & prem_lon!=. if _merge==3
drop if _merge==2
drop _merge 

** Confirm uniqueness and save
unique sp_uuid
assert r(unique)==r(N)
compress
save "$dirpath_data/pge_cleaned/pge_sp_premise_gis.dta", replace	

}

*******************************************************************************
*******************************************************************************

** 6. Assign PGE SP coordinates to CLUs (ever-crop CLUs)
if 1==1{

** Import results from GIS script
insheet using "$dirpath_data/misc/pge_prem_coord_polygon_clu_ever_crop.csv", double comma clear
drop prem_lon prem_lat bad_geocode_flag

** Clean GIS variables
	// SP identifiers
tostring sp_uuid pull, replace
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
assert length(sp_uuid)==10 & real(sp_uuid)!=.
unique sp_uuid pull
assert r(unique)==r(N)
unique sp_uuid 
assert r(unique)==r(N)

	// Fix logical
replace in_clu = "1" if in_clu=="TRUE"
replace in_clu = "0" if in_clu=="FALSE"
destring in_clu, replace
assert inlist(in_clu,0,1)
tab pull in_clu

	// Remove NAs and destring
foreach v of varlist clu_id-nearest2_cluacres {
	replace `v' = "" if `v'=="NA"
}
destring *_m *acres, replace

foreach v of varlist nearest_* {
	assert mi(`v') if mi(clu_id)==0
}

** Some checks for sanity and internal consistency
foreach v of varlist clu_id cluacres edge_dist_m neighbor* {
	assert nearest_clu_id=="" if !mi(`v')
	assert nearest2_clu_id=="" if !mi(`v')
	assert nearest_clu_id!="" if mi(`v')
	assert nearest2_clu_id!="" if mi(`v')
}
foreach v of varlist nearest* {
	assert clu_id=="" if !mi(`v')
	assert clu_id!="" if mi(`v')
}

	// is 2nd-nearest always different from 1st-nearest?
assert nearest_clu_id!=nearest2_clu_id if nearest_clu_id!="" // yes

	// is 2nd-nearest always weakly further than 1st-nearest?
gen temp = nearest2_dist_m - nearest_dist_m
sort temp
br
sum temp, detail // not always!
sum nearest_dist_m if temp<0, detail
sum nearest_dist_m if temp>=0, detail
sum nearest_dist_m if temp<0 & pull=="20180719", detail // ~100 SPs where we might have an actual issue
sum nearest_dist_m if temp>=0 & pull=="20180719", detail
gen flag_nearest_clu_error = temp<0

	// is neighbor always weakly further than edge?
gen temp2 = neighbor_dist_m - edge_dist_m	
sort temp2
br
sum temp2, detail // almost always, save for 152 SPs
replace flag_nearest_clu_error = 1 if temp2<0


** Diagnostics on distance to edge
sum edge_dist_m, detail // 50% of matches <15m from the edge of their polygons
sum nearest_dist_m, detail // 50% of nonmatches <26m from nearest polygons

** Diagnostics on CLU assignment
sum neighbor_dist_m, detail // 50% of matches <40m from nearest neighboring polygon
sum nearest2_dist_m, detail	// 50% of nonmatches <90m from 2nd-nearest polygon

** Transfer assignmentss
replace clu_id = nearest_clu_id if in_clu==0 & clu_id==""
replace edge_dist_m = nearest_dist_m if in_clu==0 & edge_dist_m==.
replace cluacres = nearest_cluacres if in_clu==0 & cluacres==.
replace neighbor_clu_id = nearest2_clu_id if in_clu==0 & neighbor_clu_id==""
replace neighbor_dist_m = nearest2_dist_m if in_clu==0 & neighbor_dist_m==.
replace neighbor_cluacres = nearest2_cluacres if in_clu==0& neighbor_cluacres==.
drop nearest* temp*
	
** Remove assignments more than 1km in distance
assert edge_dist_m!=.
gen temp = in_clu==0 & edge_dist_m>1000
tab pull temp
replace clu_id = "" if temp==1
replace cluacres = . if temp==1
replace edge_dist_m = . if temp==1
replace neighbor_clu_id = "" if temp==1
replace neighbor_dist_m = . if temp==1
replace neighbor_cluacres = . if temp==1
replace flag_nearest_clu_error = 0 if temp==1
drop temp
gen clu_nearest_dist_m = edge_dist_m
replace clu_nearest_dist_m = 0 if in_clu==1

** Label
rename cluacres clu_acres
rename edge_dist_m clu_edge_dist_m
rename neighbor_dist_m neighbor_clu_dist_m
la var in_clu "Dummy for SPs properly within ever-crop CLU polygons"
la var clu_id "Unique CLU ID (county, lon, lat, area; ever-crop)"	
la var clu_acres "Area (acres) of assigned ever-crop CLU"
la var clu_edge_dist_m "Meters to edge of assigned ever-crop CLU polygon (cut off at 1 km for unmatched)"
la var clu_nearest_dist_m "Meters to assigned ever-crop CLU polygon (cut off at 1 km, 0 for matched)"
la var neighbor_clu_dist_m "Meters to nearest non-assigned ever-crop CLU polygon"
la var flag_nearest_clu_error "Flag for errors in GIS nearest feature algorithm (ever-crop)"
drop neighbor_clu_id neighbor_cluacres // don't think we actually need these
drop pull // not necessary for merge
order sp_uuid in_clu clu_id clu_acres clu_edge_dist_m clu_nearest_dist_m 

** Rename to tag _ec for "ever-crop" CLUs
rename in_clu in_clu_ec
rename clu_id clu_id_ec
rename clu_acres clu_ec_acres
rename clu_edge_dist_m clu_ec_edge_dist_m
rename clu_nearest_dist_m clu_ec_nearest_dist_m
rename neighbor_clu_dist_m neighbor_clu_ec_dist_m
rename flag_nearest_clu_error flag_nearest_clu_ec_error

** Store temp file
tempfile gis_out
save `gis_out'

** Merge results into merge back into main dataset
use "$dirpath_data/pge_cleaned/pge_sp_premise_gis.dta", clear
merge 1:1 sp_uuid using `gis_out'	
assert _merge!=2
assert missing_geocode_flag==1 if _merge==1 // confirm that all non-merges have missing lat/lon
assert _merge==1 if missing_geocode_flag==1 // confirm that missing lat/lons don't merge
replace in_clu_ec = 0 if _merge==1 // flag missing lat/lons as non-merges
assert _merge==3 if prem_lat!=. & prem_lon!=.
assert prem_lat!=. & prem_lon!=. if _merge==3
drop if _merge==2
drop _merge 

** Confirm uniqueness and save
unique sp_uuid
assert r(unique)==r(N)
compress
save "$dirpath_data/pge_cleaned/pge_sp_premise_gis.dta", replace	

}

*******************************************************************************
*******************************************************************************

** 7. Assign APEP coordinates to CLUs (all CLUs)
if 1==1{

** Import results from GIS script
insheet using "$dirpath_data/misc/apep_pump_coord_polygon_clu.csv", double comma clear
drop pump_lat pump_lon

** Clean GIS variables
	// Pump lat/lon group identifiers
destring latlon_group, replace
assert latlon_group!=.
unique latlon_group
assert r(unique)==r(N)

	// Fix logical
replace in_clu = "1" if in_clu=="TRUE"
replace in_clu = "0" if in_clu=="FALSE"
destring in_clu, replace
assert inlist(in_clu,0,1)
tab in_clu

	// Remove NAs and destring
foreach v of varlist clu_id-nearest2_cluacres {
	replace `v' = "" if `v'=="NA"
}
destring *_m *acres, replace

foreach v of varlist nearest_* {
	assert mi(`v') if mi(clu_id)==0
}

** Some checks for sanity and internal consistency
foreach v of varlist clu_id cluacres edge_dist_m neighbor* {
	assert nearest_clu_id=="" if !mi(`v')
	assert nearest2_clu_id=="" if !mi(`v')
	assert nearest_clu_id!="" if mi(`v')
	assert nearest2_clu_id!="" if mi(`v')
}
foreach v of varlist nearest* {
	assert clu_id=="" if !mi(`v')
	assert clu_id!="" if mi(`v')
}

	// is 2nd-nearest always different from 1st-nearest?
assert nearest_clu_id!=nearest2_clu_id if nearest_clu_id!="" // yes

	// is 2nd-nearest always weakly further than 1st-nearest?
gen temp = nearest2_dist_m - nearest_dist_m
sort temp
br
sum temp, detail // not always!
sum nearest_dist_m if temp<0, detail // 356 pumps might be problematic
sum nearest_dist_m if temp>=0, detail
gen flag_nearest_clu_error = temp<0

	// is neighbor always weakly further than edge?
gen temp2 = neighbor_dist_m - edge_dist_m	
sort temp2
br
sum temp2, detail // almost always, save for 36 pumps
replace flag_nearest_clu_error = 1 if temp2<0


** Diagnostics on distance to edge
sum edge_dist_m, detail // 50% of matches <48m from the edge of their polygons
sum nearest_dist_m, detail // 50% of nonmatches <60m from nearest polygons

** Diagnostics on CLU assignment
sum neighbor_dist_m, detail // 50% of matches <76m from nearest neighboring polygon
sum nearest2_dist_m, detail	// 50% of nonmatches <184m from 2nd-nearest polygon

** Transfer assignmentss
replace clu_id = nearest_clu_id if in_clu==0 & clu_id==""
replace edge_dist_m = nearest_dist_m if in_clu==0 & edge_dist_m==.
replace cluacres = nearest_cluacres if in_clu==0 & cluacres==.
replace neighbor_clu_id = nearest2_clu_id if in_clu==0 & neighbor_clu_id==""
replace neighbor_dist_m = nearest2_dist_m if in_clu==0 & neighbor_dist_m==.
replace neighbor_cluacres = nearest2_cluacres if in_clu==0& neighbor_cluacres==.
drop nearest* temp*
	
** Remove assignments more than 1km in distance
assert edge_dist_m!=.
gen temp = in_clu==0 & edge_dist_m>1000
tab temp
replace clu_id = "" if temp==1
replace cluacres = . if temp==1
replace edge_dist_m = . if temp==1
replace neighbor_clu_id = "" if temp==1
replace neighbor_dist_m = . if temp==1
replace neighbor_cluacres = . if temp==1
replace flag_nearest_clu_error = 0 if temp==1
drop temp
gen clu_nearest_dist_m = edge_dist_m
replace clu_nearest_dist_m = 0 if in_clu==1

** Label
rename cluacres clu_acres
rename edge_dist_m clu_edge_dist_m
rename neighbor_dist_m neighbor_clu_dist_m
la var in_clu "Dummy for pumps properly within CLU polygons"
la var clu_id "Unique CLU ID (county, lon, lat, area)"	
la var clu_acres "Area (acres) of assigned CLU"
la var clu_edge_dist_m "Meters to edge of assigned CLU polygon (cut off at 1 km for unmatched)"
la var clu_nearest_dist_m "Meters to assigned CLU polygon (cut off at 1 km, 0 for matched)"
la var neighbor_clu_dist_m "Meters to nearest non-assigned CLU polygon"
la var flag_nearest_clu_error "Flag for errors in GIS nearest feature algorithm"
drop neighbor_clu_id neighbor_cluacres // don't think we actually need these
order latlon_group in_clu clu_id clu_acres clu_edge_dist_m clu_nearest_dist_m 

** Store temp file
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

** 8. Assign APEP coordinates to CLUs (ever-crop CLUs)
if 1==1{

** Import results from GIS script
insheet using "$dirpath_data/misc/apep_pump_coord_polygon_clu_ever_crop.csv", double comma clear
drop pump_lat pump_lon

** Clean GIS variables
	// Pump lat/lon group identifiers
destring latlon_group, replace
assert latlon_group!=.
unique latlon_group
assert r(unique)==r(N)

	// Fix logical
replace in_clu = "1" if in_clu=="TRUE"
replace in_clu = "0" if in_clu=="FALSE"
destring in_clu, replace
assert inlist(in_clu,0,1)
tab in_clu

	// Remove NAs and destring
foreach v of varlist clu_id-nearest2_cluacres {
	replace `v' = "" if `v'=="NA"
}
destring *_m *acres, replace

foreach v of varlist nearest_* {
	assert mi(`v') if mi(clu_id)==0
}

** Some checks for sanity and internal consistency
foreach v of varlist clu_id cluacres edge_dist_m neighbor* {
	assert nearest_clu_id=="" if !mi(`v')
	assert nearest2_clu_id=="" if !mi(`v')
	assert nearest_clu_id!="" if mi(`v')
	assert nearest2_clu_id!="" if mi(`v')
}
foreach v of varlist nearest* {
	assert clu_id=="" if !mi(`v')
	assert clu_id!="" if mi(`v')
}

	// is 2nd-nearest always different from 1st-nearest?
assert nearest_clu_id!=nearest2_clu_id if nearest_clu_id!="" // yes

	// is 2nd-nearest always weakly further than 1st-nearest?
gen temp = nearest2_dist_m - nearest_dist_m
sort temp
br
sum temp, detail // not always!
sum nearest_dist_m if temp<0, detail // 387 pumps might be problematic
sum nearest_dist_m if temp>=0, detail
gen flag_nearest_clu_error = temp<0

	// is neighbor always weakly further than edge?
gen temp2 = neighbor_dist_m - edge_dist_m	
sort temp2
br
sum temp2, detail // almost always, save for 36 pumps
replace flag_nearest_clu_error = 1 if temp2<0


** Diagnostics on distance to edge
sum edge_dist_m, detail // 50% of matches <48m from the edge of their polygons
sum nearest_dist_m, detail // 50% of nonmatches <65m from nearest polygons

** Diagnostics on CLU assignment
sum neighbor_dist_m, detail // 50% of matches <76m from nearest neighboring polygon
sum nearest2_dist_m, detail	// 50% of nonmatches <193m from 2nd-nearest polygon

** Transfer assignmentss
replace clu_id = nearest_clu_id if in_clu==0 & clu_id==""
replace edge_dist_m = nearest_dist_m if in_clu==0 & edge_dist_m==.
replace cluacres = nearest_cluacres if in_clu==0 & cluacres==.
replace neighbor_clu_id = nearest2_clu_id if in_clu==0 & neighbor_clu_id==""
replace neighbor_dist_m = nearest2_dist_m if in_clu==0 & neighbor_dist_m==.
replace neighbor_cluacres = nearest2_cluacres if in_clu==0& neighbor_cluacres==.
drop nearest* temp*
	
** Remove assignments more than 1km in distance
assert edge_dist_m!=.
gen temp = in_clu==0 & edge_dist_m>1000
tab temp
replace clu_id = "" if temp==1
replace cluacres = . if temp==1
replace edge_dist_m = . if temp==1
replace neighbor_clu_id = "" if temp==1
replace neighbor_dist_m = . if temp==1
replace neighbor_cluacres = . if temp==1
replace flag_nearest_clu_error = 0 if temp==1
drop temp
gen clu_nearest_dist_m = edge_dist_m
replace clu_nearest_dist_m = 0 if in_clu==1

** Label
rename cluacres clu_acres
rename edge_dist_m clu_edge_dist_m
rename neighbor_dist_m neighbor_clu_dist_m
la var in_clu "Dummy for pumps properly within ever-crop CLU polygons"
la var clu_id "Unique CLU ID (county, lon, lat, area; ever-crop)"	
la var clu_acres "Area (acres) of assigned ever-crop CLU"
la var clu_edge_dist_m "Meters to edge of assigned ever-crop CLU polygon (cut off at 1 km for unmatched)"
la var clu_nearest_dist_m "Meters to assigned ever-crop CLU polygon (cut off at 1 km, 0 for matched)"
la var neighbor_clu_dist_m "Meters to nearest non-assigned ever-crop CLU polygon"
la var flag_nearest_clu_error "Flag for errors in GIS nearest feature algorithm (ever-crop)"
drop neighbor_clu_id neighbor_cluacres // don't think we actually need these
order latlon_group in_clu clu_id clu_acres clu_edge_dist_m clu_nearest_dist_m 

** Rename to tag _ec for "ever-crop" CLUs
rename in_clu in_clu_ec
rename clu_id clu_id_ec
rename clu_acres clu_ec_acres
rename clu_edge_dist_m clu_ec_edge_dist_m
rename clu_nearest_dist_m clu_ec_nearest_dist_m
rename neighbor_clu_dist_m neighbor_clu_ec_dist_m
rename flag_nearest_clu_error flag_nearest_clu_ec_error

** Store temp file
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

** 9. Assign SCE SP coordinates to CLUs (all CLUs)
if 1==1{

** Import results from GIS script
insheet using "$dirpath_data/misc/sce_prem_coord_polygon_clu.csv", double comma clear
drop prem_lon prem_lat bad_geocode_flag

** Clean GIS variables
	// SP identifiers
tostring sp_uuid pull, replace
unique sp_uuid pull
assert r(unique)==r(N)
unique sp_uuid 
assert r(unique)==r(N)

	// Fix logical
replace in_clu = "1" if in_clu=="TRUE"
replace in_clu = "0" if in_clu=="FALSE"
destring in_clu, replace
assert inlist(in_clu,0,1)
tab pull in_clu

	// Remove NAs and destring
foreach v of varlist clu_id-nearest2_cluacres {
	replace `v' = "" if `v'=="NA"
}
destring *_m *acres, replace

foreach v of varlist nearest_* {
	assert mi(`v') if mi(clu_id)==0
}

** Some checks for sanity and internal consistency
foreach v of varlist clu_id cluacres edge_dist_m neighbor* {
	assert nearest_clu_id=="" if !mi(`v')
	assert nearest2_clu_id=="" if !mi(`v')
	assert nearest_clu_id!="" if mi(`v')
	assert nearest2_clu_id!="" if mi(`v')
}
foreach v of varlist nearest* {
	assert clu_id=="" if !mi(`v')
	assert clu_id!="" if mi(`v')
}

	// is 2nd-nearest always different from 1st-nearest?
assert nearest_clu_id!=nearest2_clu_id if nearest_clu_id!="" // yes

	// is 2nd-nearest always weakly further than 1st-nearest?
gen temp = nearest2_dist_m - nearest_dist_m
sort temp
br
sum temp, detail // not always!
sum nearest_dist_m if temp<0, detail  // ~100 SPs where we might have an actual issue
sum nearest_dist_m if temp>=0, detail
gen flag_nearest_clu_error = temp<0

	// is neighbor always weakly further than edge?
gen temp2 = neighbor_dist_m - edge_dist_m	
sort temp2
br
sum temp2, detail // almost always, save for 13 SPs
replace flag_nearest_clu_error = 1 if temp2<0


** Diagnostics on distance to edge
sum edge_dist_m, detail // 50% of matches <18m from the edge of their polygons
sum nearest_dist_m, detail // 50% of nonmatches <566m from nearest polygons

** Diagnostics on CLU assignment
sum neighbor_dist_m, detail // 50% of matches <44m from nearest neighboring polygon
sum nearest2_dist_m, detail	// 50% of nonmatches <859m from 2nd-nearest polygon

** Transfer assignmentss
replace clu_id = nearest_clu_id if in_clu==0 & clu_id==""
replace edge_dist_m = nearest_dist_m if in_clu==0 & edge_dist_m==.
replace cluacres = nearest_cluacres if in_clu==0 & cluacres==.
replace neighbor_clu_id = nearest2_clu_id if in_clu==0 & neighbor_clu_id==""
replace neighbor_dist_m = nearest2_dist_m if in_clu==0 & neighbor_dist_m==.
replace neighbor_cluacres = nearest2_cluacres if in_clu==0& neighbor_cluacres==.
drop nearest* temp*
	
** Remove assignments more than 1km in distance
assert edge_dist_m!=.
gen temp = in_clu==0 & edge_dist_m>1000
tab pull temp
replace clu_id = "" if temp==1
replace cluacres = . if temp==1
replace edge_dist_m = . if temp==1
replace neighbor_clu_id = "" if temp==1
replace neighbor_dist_m = . if temp==1
replace neighbor_cluacres = . if temp==1
replace flag_nearest_clu_error = 0 if temp==1
drop temp
gen clu_nearest_dist_m = edge_dist_m
replace clu_nearest_dist_m = 0 if in_clu==1

** Label
rename cluacres clu_acres
rename edge_dist_m clu_edge_dist_m
rename neighbor_dist_m neighbor_clu_dist_m
la var in_clu "Dummy for SPs properly within CLU polygons"
la var clu_id "Unique CLU ID (county, lon, lat, area)"	
la var clu_acres "Area (acres) of assigned CLU"
la var clu_edge_dist_m "Meters to edge of assigned CLU polygon (cut off at 1 km for unmatched)"
la var clu_nearest_dist_m "Meters to assigned CLU polygon (cut off at 1 km, 0 for matched)"
la var neighbor_clu_dist_m "Meters to nearest non-assigned CLU polygon"
la var flag_nearest_clu_error "Flag for errors in GIS nearest feature algorithm"
drop neighbor_clu_id neighbor_cluacres // don't think we actually need these
drop pull // not necessary for merge
order sp_uuid in_clu clu_id clu_acres clu_edge_dist_m clu_nearest_dist_m 

** Store temp file
tempfile gis_out
save `gis_out'

** Merge results into merge back into main dataset
use "$dirpath_data/sce_cleaned/sce_sp_premise_gis.dta", clear
merge 1:1 sp_uuid using `gis_out'	
assert _merge!=2
assert missing_geocode_flag==1 if _merge==1 // confirm that all non-merges have missing lat/lon
assert _merge==1 if missing_geocode_flag==1 // confirm that missing lat/lons don't merge
replace in_clu = 0 if _merge==1 // flag missing lat/lons as non-merges
assert _merge==3 if prem_lat!=. & prem_lon!=.
assert prem_lat!=. & prem_lon!=. if _merge==3
drop if _merge==2
drop _merge 

** Confirm uniqueness and save
unique sp_uuid
assert r(unique)==r(N)
compress
save "$dirpath_data/sce_cleaned/sce_sp_premise_gis.dta", replace	

}

*******************************************************************************
*******************************************************************************

** 10. Assign SCE SP coordinates to CLUs (ever-crop CLUs)
if 1==1{

** Import results from GIS script
insheet using "$dirpath_data/misc/sce_prem_coord_polygon_clu_ever_crop.csv", double comma clear
drop prem_lon prem_lat bad_geocode_flag

** Clean GIS variables
	// SP identifiers
tostring sp_uuid pull, replace
assert real(sp_uuid)!=.
unique sp_uuid pull
assert r(unique)==r(N)
unique sp_uuid 
assert r(unique)==r(N)

	// Fix logical
replace in_clu = "1" if in_clu=="TRUE"
replace in_clu = "0" if in_clu=="FALSE"
destring in_clu, replace
assert inlist(in_clu,0,1)
tab pull in_clu

	// Remove NAs and destring
foreach v of varlist clu_id-nearest2_cluacres {
	replace `v' = "" if `v'=="NA"
}
destring *_m *acres, replace

foreach v of varlist nearest_* {
	assert mi(`v') if mi(clu_id)==0
}

** Some checks for sanity and internal consistency
foreach v of varlist clu_id cluacres edge_dist_m neighbor* {
	assert nearest_clu_id=="" if !mi(`v')
	assert nearest2_clu_id=="" if !mi(`v')
	assert nearest_clu_id!="" if mi(`v')
	assert nearest2_clu_id!="" if mi(`v')
}
foreach v of varlist nearest* {
	assert clu_id=="" if !mi(`v')
	assert clu_id!="" if mi(`v')
}

	// is 2nd-nearest always different from 1st-nearest?
assert nearest_clu_id!=nearest2_clu_id if nearest_clu_id!="" // yes

	// is 2nd-nearest always weakly further than 1st-nearest?
gen temp = nearest2_dist_m - nearest_dist_m
sort temp
br
sum temp, detail // not always!
sum nearest_dist_m if temp<0, detail // ~100 SPs where we might have an actual issue
sum nearest_dist_m if temp>=0, detail
gen flag_nearest_clu_error = temp<0

	// is neighbor always weakly further than edge?
gen temp2 = neighbor_dist_m - edge_dist_m	
sort temp2
br
sum temp2, detail // almost always, save for 8 SPs
replace flag_nearest_clu_error = 1 if temp2<0


** Diagnostics on distance to edge
sum edge_dist_m, detail // 50% of matches <19m from the edge of their polygons
sum nearest_dist_m, detail // 50% of nonmatches <718m from nearest polygons

** Diagnostics on CLU assignment
sum neighbor_dist_m, detail // 50% of matches <43m from nearest neighboring polygon
sum nearest2_dist_m, detail	// 50% of nonmatches <1087m from 2nd-nearest polygon

** Transfer assignmentss
replace clu_id = nearest_clu_id if in_clu==0 & clu_id==""
replace edge_dist_m = nearest_dist_m if in_clu==0 & edge_dist_m==.
replace cluacres = nearest_cluacres if in_clu==0 & cluacres==.
replace neighbor_clu_id = nearest2_clu_id if in_clu==0 & neighbor_clu_id==""
replace neighbor_dist_m = nearest2_dist_m if in_clu==0 & neighbor_dist_m==.
replace neighbor_cluacres = nearest2_cluacres if in_clu==0& neighbor_cluacres==.
drop nearest* temp*
	
** Remove assignments more than 1km in distance
assert edge_dist_m!=.
gen temp = in_clu==0 & edge_dist_m>1000
tab pull temp
replace clu_id = "" if temp==1
replace cluacres = . if temp==1
replace edge_dist_m = . if temp==1
replace neighbor_clu_id = "" if temp==1
replace neighbor_dist_m = . if temp==1
replace neighbor_cluacres = . if temp==1
replace flag_nearest_clu_error = 0 if temp==1
drop temp
gen clu_nearest_dist_m = edge_dist_m
replace clu_nearest_dist_m = 0 if in_clu==1

** Label
rename cluacres clu_acres
rename edge_dist_m clu_edge_dist_m
rename neighbor_dist_m neighbor_clu_dist_m
la var in_clu "Dummy for SPs properly within ever-crop CLU polygons"
la var clu_id "Unique CLU ID (county, lon, lat, area; ever-crop)"	
la var clu_acres "Area (acres) of assigned ever-crop CLU"
la var clu_edge_dist_m "Meters to edge of assigned ever-crop CLU polygon (cut off at 1 km for unmatched)"
la var clu_nearest_dist_m "Meters to assigned ever-crop CLU polygon (cut off at 1 km, 0 for matched)"
la var neighbor_clu_dist_m "Meters to nearest non-assigned ever-crop CLU polygon"
la var flag_nearest_clu_error "Flag for errors in GIS nearest feature algorithm (ever-crop)"
drop neighbor_clu_id neighbor_cluacres // don't think we actually need these
drop pull // not necessary for merge
order sp_uuid in_clu clu_id clu_acres clu_edge_dist_m clu_nearest_dist_m 

** Rename to tag _ec for "ever-crop" CLUs
rename in_clu in_clu_ec
rename clu_id clu_id_ec
rename clu_acres clu_ec_acres
rename clu_edge_dist_m clu_ec_edge_dist_m
rename clu_nearest_dist_m clu_ec_nearest_dist_m
rename neighbor_clu_dist_m neighbor_clu_ec_dist_m
rename flag_nearest_clu_error flag_nearest_clu_ec_error

** Store temp file
tempfile gis_out
save `gis_out'

** Merge results into merge back into main dataset
use "$dirpath_data/sce_cleaned/sce_sp_premise_gis.dta", clear
merge 1:1 sp_uuid using `gis_out'	
assert _merge!=2
assert missing_geocode_flag==1 if _merge==1 // confirm that all non-merges have missing lat/lon
assert _merge==1 if missing_geocode_flag==1 // confirm that missing lat/lons don't merge
replace in_clu_ec = 0 if _merge==1 // flag missing lat/lons as non-merges
assert _merge==3 if prem_lat!=. & prem_lon!=.
assert prem_lat!=. & prem_lon!=. if _merge==3
drop if _merge==2
drop _merge 

** Confirm uniqueness and save
unique sp_uuid
assert r(unique)==r(N)
compress
save "$dirpath_data/sce_cleaned/sce_sp_premise_gis.dta", replace	

}

*******************************************************************************
*******************************************************************************

