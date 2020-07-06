clear all
version 13
set more off

*************************************************************
**** Script to aggregate CLUs/APEP units up to fields/farms ***
*************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"
global dirpath_code "T:/Home/Louis/backup/AgEE/AgEE_code/build"
*global R_exe_path "C:/PROGRA~1/MIE74D~1/ROPEN~1/bin/x64/R"
*global R_lib_path "C:/Program Files/Microsoft/R Open/R-3.4.4/library"

*******************************************************************************
*******************************************************************************

** 1. Diagnostics: compare assigned CLUs, all vs. ever-crop
if 1==1{

** 1a. PGE SPs
{
use "$dirpath_data/pge_cleaned/pge_sp_premise_gis.dta", clear
unique sp_uuid
assert r(unique)==r(N)
gen temp_clu_all = clu_id!=""
gen temp_clu_ec = clu_id_ec!=""
gen temp_clu_match = clu_id==clu_id_ec & clu_id!=""

	// How many SPs have CLU assignments
tab temp_clu_all // 96%
tab temp_clu_all if pull=="20180719" // 94%
tab temp_clu_all if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 // 97%

tab temp_clu_ec // 95%
tab temp_clu_ec if pull=="20180719" // 94%
tab temp_clu_ec if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 // 97%

	// Denominator
count if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 
local rN = r(N)

	// Has *a* CLU
count if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 & ///
	(clu_id!="" | clu_id_ec!="")
di r(N)/`rN' // 97.4%
	
	// Assigned CLU is consistent
count if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 & ///
	clu_id_ec!="" & temp_clu_match==1
di r(N)/`rN' // 96.7%
	
	// Comparing to non-APEP pulls
count if pull!="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1
local rN = r(N)
count if pull!="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 & ///
	clu_id_ec!="" & temp_clu_match==1
di r(N)/`rN' // 93.9%
}

** 1b. APEP pumps
{
use "$dirpath_data/pge_cleaned/apep_pump_gis.dta", clear
drop crop-test_date_stata
duplicates drop
unique latlon_group
assert r(unique)==r(N)
gen temp_clu_all = clu_id!=""
gen temp_clu_ec = clu_id_ec!=""
gen temp_clu_match = clu_id==clu_id_ec & clu_id!=""

	// How many pumps have CLU assignments
tab temp_clu_all // 92%
tab temp_clu_ec // 91%

	// Has *a* CLU
count if (clu_id!="" | clu_id_ec!="")
di r(N)/_N // 92.1%
	
	// Assigned CLU parcel is consistent
count if clu_id_ec!="" & temp_clu_match==1
di r(N)/_N // 90.0%
}

** 1c. SCE SPs
{
use "$dirpath_data/sce_cleaned/sce_sp_premise_gis.dta", clear
unique sp_uuid
assert r(unique)==r(N)
gen temp_clu_all = clu_id!=""
gen temp_clu_ec = clu_id_ec!=""
gen temp_clu_match = clu_id==clu_id_ec & clu_id!=""

	// How many SPs have parcel assignments
tab temp_clu_all // 55%
tab temp_clu_ec // 53%

	// Denominator
count if bad_geocode_flag!=1 & missing_geocode_flag!=1 
local rN = r(N)

	// Has *a* CLU
count if bad_geocode_flag!=1 & missing_geocode_flag!=1 & (clu_id!="" | clu_id_ec!="")
di r(N)/`rN' // 67.1%
	
	// Assigned CLU is consistent
count if bad_geocode_flag!=1 & missing_geocode_flag!=1 & clu_id_ec!="" & temp_clu_match==1
di r(N)/`rN' // 61.0%
}

}

*******************************************************************************
*******************************************************************************

** 2. Dioagnostics: compare assigned parcels, via all vs. ever-crop CLUs
if 1==1{

** 2a. PGE SPs
{
use "$dirpath_data/pge_cleaned/pge_sp_premise_gis.dta", clear
unique sp_uuid
assert r(unique)==r(N)
gen temp_parcel_all = parcelid!=""
gen temp_parcel_ec = parcelid_ec!=""
gen temp_parcel_match = parcelid==parcelid_ec & parcelid!=""

	// How many SPs have parcel assignments
tab temp_parcel_all // 91%
tab temp_parcel_all if pull=="20180719" // 90%
tab temp_parcel_all if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 // 93%

tab temp_parcel_ec // 90%
tab temp_parcel_ec if pull=="20180719" // 90%
tab temp_parcel_ec if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 // 93%

tab temp_parcel_match if in_clu==1 & in_clu_ec==1 & ///
	bad_geocode_flag!=1 & missing_geocode_flag!=1 // 95.4%
tab temp_parcel_match if in_clu==1 & in_clu_ec==1 & pull=="20180719" & ///
	bad_geocode_flag!=1 & missing_geocode_flag!=1 // 96.2%
	
	// Denominator
count if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 
local rN = r(N)

	// Has *a* parcel
count if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 & ///
	(parcelid!="" | parcelid_ec!="")
di r(N)/`rN' // 93.5%
	
	// Assigned parcel is consistent across both CLU assignments
count if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 & ///
	parcelid_ec!="" & temp_parcel_match==1
di r(N)/`rN' // 92.9%
	
	// Comparing to non-APEP pulls
count if pull!="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1
local rN = r(N)
count if pull!="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 & ///
	parcelid_ec!="" & temp_parcel_match==1
di r(N)/`rN' // 90.7%
}

** 2b. APEP pumps
{
use "$dirpath_data/pge_cleaned/apep_pump_gis.dta", clear
drop crop-test_date_stata
duplicates drop
unique latlon_group
assert r(unique)==r(N)
gen temp_parcel_all = parcelid!=""
gen temp_parcel_ec = parcelid_ec!=""
gen temp_parcel_match = parcelid==parcelid_ec & parcelid!=""

	// How many pumps have parcel assignments
tab temp_parcel_all // 87%
tab temp_parcel_ec // 86%
tab temp_parcel_match if in_clu==1 & in_clu_ec==1 // 94.1%

	// Has *a* parcel
count if (parcelid!="" | parcelid_ec!="")
di r(N)/_N // 86.6%
	
	// Assigned parcel is consistent across both CLU assignments
count if parcelid_ec!="" & temp_parcel_match==1
di r(N)/_N // 84.4%
}

** 2c. SCE SPs
{
use "$dirpath_data/sce_cleaned/sce_sp_premise_gis.dta", clear
unique sp_uuid
assert r(unique)==r(N)
gen temp_parcel_all = parcelid!=""
gen temp_parcel_ec = parcelid_ec!=""
gen temp_parcel_match = parcelid==parcelid_ec & parcelid!=""

	// How many pumps have parcel assignments
tab temp_parcel_all // 54%
tab temp_parcel_ec // 53%
tab temp_parcel_match if in_clu==1 & in_clu_ec==1 // 99%

	// Denominator
count if bad_geocode_flag!=1 & missing_geocode_flag!=1 
local rN = r(N)

	// Has *a* parcel
count if bad_geocode_flag!=1 & missing_geocode_flag!=1 & (parcelid!="" | parcelid_ec!="")
di r(N)/`rN' // 66.4%
	
	// Assigned parcel is consistent across both CLU assignments
count if bad_geocode_flag!=1 & missing_geocode_flag!=1 & parcelid_ec!="" & temp_parcel_match==1
di r(N)/`rN' // 60.8%
}

}

*******************************************************************************
*******************************************************************************

** 2. Merge group identifiers into SP GIS dataset
{
	// Start with SP GIS assignments
use "$dirpath_data/pge_cleaned/sp_premise_gis.dta", clear
cap drop clu_group* parcel_group* polygon_check*

	// Rename unrestricted polygon identifiers
rename clu_id clu_id_unr
rename parcelid parcelid_unr

	// Merge in CLU groups (ever-crop)
rename clu_id_ec clu_id
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_conc_groups.dta", ///
	keep(1 3) nogen keepusing(clu_group*)
rename clu_group* clu_group*_ec
rename clu_id clu_id_ec
	
	// Merge in CLU groups (unrestricted)
rename clu_id_unr clu_id
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_conc_groups.dta", ///
	keep(1 3) nogen keepusing(clu_group*)
rename clu_group* clu_group*_unr
rename *_ec_unr *_ec
rename clu_id clu_id_unr

	// Merge in parcel groups (concorcdance)
rename parcelid_conc parcelid
merge m:1 parcelid using "$dirpath_data/cleaned_spatial/parcel_conc_groups.dta", ///
	keep(1 3) nogen keepusing(parcel_group*)
rename parcel_group* parcel_group*_conc
rename parcelid parcelid_conc
	
	// Merge in parcel groups (unrestricted)
rename parcelid_unr parcelid
merge m:1 parcelid using "$dirpath_data/cleaned_spatial/parcel_conc_groups.dta", ///
	keep(1 3) nogen keepusing(parcel_group*)
rename parcel_group* parcel_group*_unr
rename *_conc_unr *_conc
rename parcelid parcelid_unr

	// Test if assigned CLU groups (EC) and pacel groups (conc) match to each other
foreach s in 0 10 25 50 75 {
	rename clu_group`s'_ec clu_group`s'
	rename parcel_group`s'_conc parcel_group`s'
	merge m:1 clu_group`s' parcel_group`s' using ///
		"$dirpath_data/cleaned_spatial/groups_conc`s'.dta", keep(1 3)
	gen groups_match`s' = _merge==3
	drop _merge
	unique sp_uuid
	assert r(unique)==r(N)
	rename clu_group`s' clu_group`s'_ec
	rename parcel_group`s' parcel_group`s'_conc
}
		
	// Test if assigned CLU (EC) and parcel (conc) match to each other
rename clu_id_ec clu_id
rename parcelid_conc parcelid
merge m:1 clu_id parcelid using "$dirpath_data/cleaned_spatial/clu_parcel_conc.dta", ///
	keep(1 3) keepusing(drop_parcelid_total drop_slivers)
gen polygons_match_slivers = _merge==3
gen polygons_match = _merge==3 & drop_parcelid_total==0 & drop_slivers==0
drop _merge	drop_parcelid_total drop_slivers
unique sp_uuid
assert r(unique)==r(N)
rename clu_id clu_id_ec
rename parcelid parcelid_conc


	// CHECK A: Does clu_id_ec exist, and match county?
gen polygon_check_A = clu_id_ec!="" & clu_ec_county==county
sum polygon_check_A
la var polygon_check_A "Check A: clu_id_ec matches county"
	
	// CHECK B: Does parcelid_conc exist, and match county?
gen polygon_check_B = parcelid_conc!="" & parcel_conc_county==county
sum polygon_check_B
la var polygon_check_B "Check B: parcelid_conc matches county"
	
	// CHECK C: Does clu_id_ec exist, match county, and match the group of clu_id_unr?
foreach s in 0 10 25 50 75 {
	gen polygon_check_C`s' = polygon_check_A==1 & clu_group`s'_ec==clu_group`s'_unr
	sum polygon_check_C`s'
	la var polygon_check_C`s' "Check C: clu_id_ec matches county and group of clu_id_unr"
}

	// CHECK D: Does the group of clu_id_ec match the group of parcelid_conc, with county match?
foreach s in 0 10 25 50 75 {
	gen polygon_check_D`s' = polygon_check_A==1 & groups_match`s'==1
	sum polygon_check_D`s'
	la var polygon_check_D`s' "Check D: clu_id_ec matches county and group of parcelid_conc"
}

	// CHECK E: Does clu_id_ec match with parcelid_conc, with county match, including slivers?
gen polygon_check_E = polygon_check_A==1 & polygons_match_slivers==1
sum polygon_check_E
la var polygon_check_E "Check E: clu_id_ec matches county and parcelid_conc (incl slivers)"

	// CHECK F: Does clu_id_ec match with parcelid_conc, with county match, excluding slivers?
gen polygon_check_F = polygon_check_A==1 & polygons_match==1
sum polygon_check_F
la var polygon_check_F "Check F: clu_id_ec matches county and parcelid_conc (excl slivers)"

	// CHECK G: Does clu_id_ec match clu_id_unr, wih county match?
gen polygon_check_G = polygon_check_A==1 & clu_id_ec==clu_id_unr
sum polygon_check_G
la var polygon_check_G "Check G: clu_id_ec matches county and clu_id_unr"

	// CHECK H: Does parcelid_conc match parcelid_unr, wih county match?
gen polygon_check_H = polygon_check_B==1 & parcelid_conc==parcelid_unr
sum polygon_check_H
la var polygon_check_H "Check H: parcelid_conc matches county and parcelid_unr"

	// Some diagnostics
sum polygon_check* if pull=="20180719"
sum polygon_check* if pull!="20180719"

sum polygon_check_C* if polygon_check_A==1
sum polygon_check_D* if polygon_check_A==1
foreach s in 0 10 25 50 75 {
	sum polygon_check_D`s'
	sum polygon_check_D`s' if polygon_check_C`s'==1
}

sum polygon_check_E
sum polygon_check_E if polygon_check_A==1
foreach s in 0 10 25 50 75 {
	sum polygon_check_E if polygon_check_C`s'==1
	sum polygon_check_E if polygon_check_D`s'==1
}

sum polygon_check_F
sum polygon_check_F if polygon_check_A==1
foreach s in 0 10 25 50 75 {
	sum polygon_check_F if polygon_check_C`s'==1
	sum polygon_check_F if polygon_check_D`s'==1
}
sum polygon_check_F if polygon_check_E==1
assert polygon_check_E==1 if polygon_check_F==1

	// Clean up and save
drop groups_match* polygons_match* // redundant now
sort sp_uuid
unique sp_uuid
assert r(unique)==r(N)
compress
save "$dirpath_data/pge_cleaned/sp_premise_gis.dta", replace

}

*******************************************************************************
*******************************************************************************

** 6. Merge group identifiers into APEP GIS dataset
{
	// Start with SP GIS assignments
use "$dirpath_data/pge_cleaned/apep_pump_gis.dta", clear
cap drop clu_group* parcel_group* polygon_check*

	// Rename unrestricted polygon identifiers
rename clu_id clu_id_unr
rename parcelid parcelid_unr

	// Merge in CLU groups (ever-crop)
rename clu_id_ec clu_id
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_conc_groups.dta", ///
	keep(1 3) nogen keepusing(clu_group*)
rename clu_group* clu_group*_ec
rename clu_id clu_id_ec
	
	// Merge in CLU groups (unrestricted)
rename clu_id_unr clu_id
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_conc_groups.dta", ///
	keep(1 3) nogen keepusing(clu_group*)
rename clu_group* clu_group*_unr
rename *_ec_unr *_ec
rename clu_id clu_id_unr

	// Merge in parcel groups (concorcdance)
rename parcelid_conc parcelid
merge m:1 parcelid using "$dirpath_data/cleaned_spatial/parcel_conc_groups.dta", ///
	keep(1 3) nogen keepusing(parcel_group*)
rename parcel_group* parcel_group*_conc
rename parcelid parcelid_conc
	
	// Merge in parcel groups (unrestricted)
rename parcelid_unr parcelid
merge m:1 parcelid using "$dirpath_data/cleaned_spatial/parcel_conc_groups.dta", ///
	keep(1 3) nogen keepusing(parcel_group*)
rename parcel_group* parcel_group*_unr
rename *_conc_unr *_conc
rename parcelid parcelid_unr

	// Test if assigned CLU groups (EC) and pacel groups (conc) match to each other
foreach s in 0 10 25 50 75 {
	rename clu_group`s'_ec clu_group`s'
	rename parcel_group`s'_conc parcel_group`s'
	merge m:1 clu_group`s' parcel_group`s' using ///
		"$dirpath_data/cleaned_spatial/groups_conc`s'.dta", keep(1 3)
	gen groups_match`s' = _merge==3
	drop _merge
	unique apeptestid crop
	assert r(unique)==r(N)
	rename clu_group`s' clu_group`s'_ec
	rename parcel_group`s' parcel_group`s'_conc
}
		
	// Test if assigned CLU (EC) and parcel (conc) match to each other
rename clu_id_ec clu_id
rename parcelid_conc parcelid
merge m:1 clu_id parcelid using "$dirpath_data/cleaned_spatial/clu_parcel_conc.dta", ///
	keep(1 3) keepusing(drop_parcelid_total drop_slivers)
gen polygons_match_slivers = _merge==3
gen polygons_match = _merge==3 & drop_parcelid_total==0 & drop_slivers==0
drop _merge	drop_parcelid_total drop_slivers
unique apeptestid crop
assert r(unique)==r(N)
rename clu_id clu_id_ec
rename parcelid parcelid_conc


	// CHECK A: Does clu_id_ec exist, and match county?
gen polygon_check_A = clu_id_ec!="" & clu_ec_county==county
sum polygon_check_A
la var polygon_check_A "Check A: clu_id_ec matches county"
	
	// CHECK B: Does parcelid_conc exist, and match county?
gen polygon_check_B = parcelid_conc!="" & parcel_conc_county==county
sum polygon_check_B
la var polygon_check_B "Check B: parcelid_conc matches county"
	
	// CHECK C: Does clu_id_ec exist, match county, and match the group of clu_id_unr?
foreach s in 0 10 25 50 75 {
	gen polygon_check_C`s' = polygon_check_A==1 & clu_group`s'_ec==clu_group`s'_unr
	sum polygon_check_C`s'
	la var polygon_check_C`s' "Check C: clu_id_ec matches county and group of clu_id_unr"
}

	// CHECK D: Does the group of clu_id_ec match the group of parcelid_conc, with county match?
foreach s in 0 10 25 50 75 {
	gen polygon_check_D`s' = polygon_check_A==1 & groups_match`s'==1
	sum polygon_check_D`s'
	la var polygon_check_D`s' "Check D: clu_id_ec matches county and group of parcelid_conc"
}

	// CHECK E: Does clu_id_ec match with parcelid_conc, with county match, including slivers?
gen polygon_check_E = polygon_check_A==1 & polygons_match_slivers==1
sum polygon_check_E
la var polygon_check_E "Check E: clu_id_ec matches county and parcelid_conc (incl slivers)"

	// CHECK F: Does clu_id_ec match with parcelid_conc, with county match, excluding slivers?
gen polygon_check_F = polygon_check_A==1 & polygons_match==1
sum polygon_check_F
la var polygon_check_F "Check F: clu_id_ec matches county and parcelid_conc (excl slivers)"

	// CHECK G: Does clu_id_ec match clu_id_unr, wih county match?
gen polygon_check_G = polygon_check_A==1 & clu_id_ec==clu_id_unr
sum polygon_check_G
la var polygon_check_G "Check G: clu_id_ec matches county and clu_id_unr"

	// CHECK H: Does parcelid_conc match parcelid_unr, wih county match?
gen polygon_check_H = polygon_check_B==1 & parcelid_conc==parcelid_unr
sum polygon_check_H
la var polygon_check_H "Check H: parcelid_conc matches county and parcelid_unr"

	// Some diagnostics
sum polygon_check_C* if polygon_check_A==1
sum polygon_check_D* if polygon_check_A==1
foreach s in 0 10 25 50 75 {
	sum polygon_check_D`s'
	sum polygon_check_D`s' if polygon_check_C`s'==1
}

sum polygon_check_E
sum polygon_check_E if polygon_check_A==1
foreach s in 0 10 25 50 75 {
	sum polygon_check_E if polygon_check_C`s'==1
	sum polygon_check_E if polygon_check_D`s'==1
}

sum polygon_check_F
sum polygon_check_F if polygon_check_A==1
foreach s in 0 10 25 50 75 {
	sum polygon_check_F if polygon_check_C`s'==1
	sum polygon_check_F if polygon_check_D`s'==1
}
sum polygon_check_F if polygon_check_E==1
assert polygon_check_E==1 if polygon_check_F==1

	// Clean up and save
drop groups_match* polygons_match* // redundant now
sort apeptestid crop
unique apeptestid crop
assert r(unique)==r(N)
compress
save "$dirpath_data/pge_cleaned/apep_pump_gis.dta", replace

}

*******************************************************************************
*******************************************************************************

