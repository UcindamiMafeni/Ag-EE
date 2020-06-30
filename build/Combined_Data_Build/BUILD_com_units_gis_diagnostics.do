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

** 1. Compare assigned parcels, all vs. CLU-matched
if 1==0{

** SP premise lat/lons
use "$dirpath_data/pge_cleaned/sp_premise_gis.dta", clear
unique sp_uuid
assert r(unique)==r(N)
gen temp_parcel_any = parcelid!=""
gen temp_parcel_conc_any = parcelid_conc!=""
gen temp_parcel_match = parcelid==parcelid_conc & parcelid!=""

	// How many SPs have parcel assignments
tab temp_parcel_any // 96%
tab temp_parcel_any if pull=="20180719" // 94% in both
tab temp_parcel_any if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 // 98%

tab temp_parcel_conc_any // 94%
tab temp_parcel_conc_any if pull=="20180719" // 94% in both
tab temp_parcel_conc_any if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 // 97%

	// Compare parcels (properly within polygon)
tab in_parcel in_parcel_conc // 64% in both
tab in_parcel in_parcel_conc if pull=="20180719" // 73% in both
tab in_parcel in_parcel_conc if pull=="20180719" & bad_geocode_flag!=1 & ///
	missing_geocode_flag!=1 // 73% in both (barely a sample restriction)
	
tab temp_parcel_match if in_parcel==1 & in_parcel_conc==1 & ///
	bad_geocode_flag!=1 & missing_geocode_flag!=1 & parcel_county==parcel_conc_county // 99.7%
tab temp_parcel_match if in_parcel==1 & in_parcel_conc==1 & pull=="20180719" & ///
	bad_geocode_flag!=1 & missing_geocode_flag!=1 & parcel_county==parcel_conc_county // 99.8%
	
	// Compare parcels (properly within polygon, or < 1 mile away)
tab temp_parcel_match if parcelid!="" & parcelid_conc!="" & ///
	bad_geocode_flag!=1 & missing_geocode_flag!=1 & parcel_county==parcel_conc_county // 77%
tab temp_parcel_match if parcelid!="" & parcelid_conc!="" & pull=="20180719" & ///
	bad_geocode_flag!=1 & missing_geocode_flag!=1 & parcel_county==parcel_conc_county // 85%

	// Formalizing validation checks, from weakest to strongest
	
	// Denominator
count if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 
local rN = r(N)

	// Has *a* parcel
count if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 & ///
	(parcelid!="" | parcelid_conc!="")
di r(N)/`rN' // 98.0%
	
	// Has *a* parcel that matches county
count if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 & ///
	(parcelid!="" | parcelid_conc!="") & (parcel_county==county | parcel_conc_county==county)
di r(N)/`rN' // 96.0%
	
	// Has CLU-matched parcel that matches county
count if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 & ///
	parcelid_conc!="" & parcel_conc_county==county
di r(N)/`rN' // 94.4%
	
	// Has CLU-matched parcel that matches county, and matches unrestricted parcel
count if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 & ///
	parcelid_conc!="" & parcel_conc_county==county & temp_parcel_match==1
di r(N)/`rN' // 79.1%
	
	// Properly within CLU-matched parcel that matches county, and matches unrestricted parcel
count if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 & ///
	parcelid_conc!="" & parcel_conc_county==county & temp_parcel_match==1 & in_parcel_conc==1
di r(N)/`rN' // 70.0%

	// Comparing to non-APEP pulls
count if pull!="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1
local rN = r(N)
count if pull!="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 & ///
	parcelid_conc!="" & parcel_conc_county==county & temp_parcel_match==1 & in_parcel_conc==1
di r(N)/`rN' // 56.0%, not bad!


** APEP pump lat/lons
use "$dirpath_data/pge_cleaned/apep_pump_gis.dta", clear
drop crop-test_date_stata
duplicates drop
unique latlon_group
assert r(unique)==r(N)
gen temp_parcel_any = parcelid!=""
gen temp_parcel_conc_any = parcelid_conc!=""
gen temp_parcel_match = parcelid==parcelid_conc & parcelid!=""

	// How many pumps have parcel assignments
tab temp_parcel_any // 97.5%
tab temp_parcel_conc_any // 92.5%

	// Compare parcels (properly within polygon)
tab in_parcel in_parcel_conc // 70.8% in both
	
tab temp_parcel_match if in_parcel==1 & in_parcel_conc==1 & ///
	parcel_county==parcel_conc_county // 99.8%
	
	// Compare parcels (properly within polygon, or < 1 mile away)
tab temp_parcel_match if parcelid!="" & parcelid_conc!="" & ///
	parcel_county==parcel_conc_county // 80.3%

	// Formalizing validation checks, from weakest to strongest
	
	// Denominator
count
local rN = r(N)

	// Has *a* parcel
count if (parcelid!="" | parcelid_conc!="")
di r(N)/`rN' // 97.7%
	
	// Has *a* parcel that matches county
count if (parcelid!="" | parcelid_conc!="") & (parcel_county==county | parcel_conc_county==county)
di r(N)/`rN' // 96.4%
	
	// Has CLU-matched parcel that matches county
count if parcelid_conc!="" & parcel_conc_county==county
di r(N)/`rN' // 91.0%
	
	// Has CLU-matched parcel that matches county, and matches unrestricted parcel
count if parcelid_conc!="" & parcel_conc_county==county & temp_parcel_match==1
di r(N)/`rN' // 72.0%
	
	// Properly within CLU-matched parcel that matches county, and matches unrestricted parcel
count if parcelid_conc!="" & parcel_conc_county==county & temp_parcel_match==1 & in_parcel_conc==1
di r(N)/`rN' // 68.7%



}

*******************************************************************************
*******************************************************************************

** 2. Compare assigned CLUs, all vs. ever-crop
if 1==0{

** SP premise lat/lons
use "$dirpath_data/pge_cleaned/sp_premise_gis.dta", clear
unique sp_uuid
assert r(unique)==r(N)
gen temp_clu_any = clu_id!=""
gen temp_clu_ec_any = clu_id_ec!=""
gen temp_clu_match = clu_id==clu_id_ec & clu_id!=""

	// How many SPs have CLU assignments
tab temp_clu_any // 96.8%
tab temp_clu_any if pull=="20180719" // 94.9% in both
tab temp_clu_any if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 // 98.2%

tab temp_clu_ec_any // 96.1%
tab temp_clu_ec_any if pull=="20180719" // 94.8% in both
tab temp_clu_ec_any if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 // 98.1%

	// Compare CLUs (properly within polygon)
tab in_clu in_clu_ec // 41.2% in both
tab in_clu in_clu_ec if pull=="20180719" // 54.6% in both
tab in_clu in_clu_ec if pull=="20180719" & bad_geocode_flag!=1 & ///
	missing_geocode_flag!=1 // 54.6% in both (barely a sample restriction)
	
tab temp_clu_match if in_clu==1 & in_clu_ec==1 & ///
	bad_geocode_flag!=1 & missing_geocode_flag!=1 & clu_county==clu_ec_county // 100%
	// No weird overlapping CLU issues thankfully
	
	// Compare CLUs (properly within polygon, or < 1 mile away)
tab temp_clu_match if clu_id!="" & clu_id_ec!="" & ///
	bad_geocode_flag!=1 & missing_geocode_flag!=1 & clu_county==clu_ec_county // 97.7%
tab temp_clu_match if clu_id!="" & clu_id_ec!="" & pull=="20180719" & ///
	bad_geocode_flag!=1 & missing_geocode_flag!=1 & clu_county==clu_ec_county // 99.4%

tab temp_clu_match if clu_id!="" & clu_id_ec!="" & ///
	bad_geocode_flag!=1 & missing_geocode_flag!=1 // 97.7%
tab temp_clu_match if clu_id!="" & clu_id_ec!="" & pull=="20180719" & ///
	bad_geocode_flag!=1 & missing_geocode_flag!=1 // 99.4%

tab temp_clu_match if clu_id!="" & clu_id_ec!="" & ///
	bad_geocode_flag!=1 & missing_geocode_flag!=1 & clu_county==county // 97.7%
tab temp_clu_match if clu_id!="" & clu_id_ec!="" & pull=="20180719" & ///
	bad_geocode_flag!=1 & missing_geocode_flag!=1 & clu_county==county // 99.3%


	// Formalizing validation checks, from weakest to strongest
	
	// Denominator
count if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 
local rN = r(N)

	// Has *a* CLU
count if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 & ///
	(clu_id!="" | clu_id_ec!="")
di r(N)/`rN' // 98.2%
	
	// Has *a* clu that matches county
count if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 & ///
	(clu_id!="" | clu_id_ec!="") & (clu_county==county | clu_ec_county==county)
di r(N)/`rN' // 96.2%
	
	// Has ever-crop CLU that matches county
count if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 & ///
	clu_id_ec!="" & clu_ec_county==county
di r(N)/`rN' // 96.1%
	
	// Has ever-crop CLU that matches county, and matches unrestricted CLU
count if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 & ///
	clu_id_ec!="" & clu_ec_county==county & temp_clu_match==1
di r(N)/`rN' // 95.4%
sum clu_ec_dist_miles if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 & ///
	clu_id_ec!="" & clu_ec_county==county & temp_clu_match==1, detail
	// 95% of these w/in .16 miles, 99% of these w/in .52 miles 
	
	// Properly within ever-crop CLU that matches county, and matches unrestricted CLU
count if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 & ///
	clu_id_ec!="" & clu_ec_county==county & temp_clu_match==1 & in_clu_ec==1
di r(N)/`rN' // 53.7%


	// Comparing to non-APEP pulls
count if pull!="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1
local rN = r(N)
count if pull!="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 & ///
	clu_id_ec!="" & clu_ec_county==county & temp_clu_match==1
di r(N)/`rN' // 93.0%, not bad!
sum clu_ec_dist_miles if pull!="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 & ///
	clu_id_ec!="" & clu_ec_county==county & temp_clu_match==1, detail
	// 95% of these w/in .21 miles, 99% of these w/in .59 miles 
count if pull!="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 & ///
	clu_id_ec!="" & clu_ec_county==county & temp_clu_match==1 & in_clu_ec==1
di r(N)/`rN' // 43.2%, not bad!


** APEP pump lat/lons
use "$dirpath_data/pge_cleaned/apep_pump_gis.dta", clear
drop crop-test_date_stata
duplicates drop
unique latlon_group
assert r(unique)==r(N)
gen temp_clu_any = clu_id!=""
gen temp_clu_ec_any = clu_id_ec!=""
gen temp_clu_match = clu_id==clu_id_ec & clu_id!=""

	// How many pumps have CLU assignments
tab temp_clu_any // 94.5%
tab temp_clu_ec_any // 93.8%

	// Compare CLUs (properly within polygon)
tab in_clu in_clu_ec // 54.1% in both
	
tab temp_clu_match if in_clu==1 & in_clu_ec==1 & ///
	clu_county==clu_ec_county // 100%
	
	// Compare CLUs (properly within polygon, or < 1 mile away)
tab temp_clu_match if clu_id!="" & clu_id_ec!="" & ///
	clu_county==clu_ec_county // 98.3%

	// Formalizing validation checks, from weakest to strongest

	// Denominator
count
local rN = r(N)

	// Has *a* CLU
count if (clu_id!="" | clu_id_ec!="")
di r(N)/`rN' // 94.5%
	
	// Has *a* CLU that matches county
count if (clu_id!="" | clu_id_ec!="") & (clu_county==county | clu_ec_county==county)
di r(N)/`rN' // 91.6%
	
	// Has ever-crop CLU that matches county
count if clu_id_ec!="" & clu_ec_county==county
di r(N)/`rN' // 90.9%
	
	// Has ever-crop CLU that matches county, and matches unrestricted CLU
count if clu_id_ec!="" & clu_ec_county==county & temp_clu_match==1
di r(N)/`rN' // 89.2%
sum clu_ec_dist_miles if clu_id_ec!="" & clu_ec_county==county & temp_clu_match==1, detail 
	// 95% of these w/in .37 miles, 99% of these w/in .85 miles 
	
	// Properly within ever-crop CLU that matches county, and matches unrestricted CLU
count if clu_id_ec!="" & clu_ec_county==county & temp_clu_match==1 & in_clu_ec==1
di r(N)/`rN' // 52.1%


}

*******************************************************************************
*******************************************************************************

** 3. Compare parcel-CLU concordance vs. parcel-CLU assignments
if 1==0{

** SP premise lat/lons, assigned parcels (all)
use "$dirpath_data/pge_cleaned/sp_premise_gis.dta", clear
keep sp_uuid bad_geocode_flag missing_geocode_flag pull county in_parcel parcelid ///
	parcel_county clu_id clu_dist_miles clu_id_ec clu_ec_dist_miles
rename clu_id clu_id_all
joinby parcelid using "$dirpath_data/cleaned_spatial/clu_parcel_conc.dta", unmatched(master)
gen clu_id_all_match = clu_id_all!="" & clu_id_all==clu_id
gen clu_id_ec_match = clu_id_ec!="" & clu_id_ec==clu_id
collapse (sum) pct_int_parcel, by(sp_uuid bad_geocode_flag missing_geocode_flag pull ///
	county in_parcel parcelid parcel_county clu_id_all_match clu_id_ec_match) fast
unique sp_uuid clu_id_all_match clu_id_ec_match
assert r(unique)==r(N)
egen temp_max_all = max(clu_id_all_match), by(sp_uuid)
egen temp_max_ec = max(clu_id_ec_match), by(sp_uuid)
egen temp_tag = tag(sp_uuid)

sum temp_max_all if temp_tag & parcelid!="" // 65.4% of SPs assigned to a parcel
	// also match to a CLU that's concordanced to that same parcel

sum temp_max_ec if temp_tag & parcelid!="" // 64.8% of SPs assigned to a parcel
	// also match to an ever-crop CLU that's concordanced to that same parcel

tabstat temp_max_ec if temp_tag & parcelid!="", by(county) s(mean n)	
tabstat temp_max_ec if temp_tag & parcelid!="" & pull=="20180719", by(county) s(mean n)	

egen temp_denom = sum(pct_int_parcel), by(sp_uuid)
assert round(temp_denom,0.00001)<=1
replace temp_denom = min(temp_denom,1)
replace pct_int_parcel = pct_int_parcel/temp_denom
tabstat pct_int_parcel if parcelid!="" & temp_max_all==1, by(clu_id_all_match) ///
	s(p25 p50 p75 mean n)

	

** SP premise lat/lons, assigned parcels (CLU-matched)
use "$dirpath_data/pge_cleaned/sp_premise_gis.dta", clear
keep sp_uuid bad_geocode_flag missing_geocode_flag pull county in_parcel_conc parcelid_conc ///
	parcel_conc_county clu_id clu_dist_miles clu_id_ec clu_ec_dist_miles
rename clu_id clu_id_all
rename parcelid_conc parcelid
joinby parcelid using "$dirpath_data/cleaned_spatial/clu_parcel_conc.dta", unmatched(master)
gen clu_id_all_match = clu_id_all!="" & clu_id_all==clu_id
gen clu_id_ec_match = clu_id_ec!="" & clu_id_ec==clu_id
collapse (sum) pct_int_parcel, by(sp_uuid bad_geocode_flag missing_geocode_flag pull ///
	county in_parcel parcelid parcel_conc_county clu_id_all_match clu_id_ec_match) fast
unique sp_uuid clu_id_all_match clu_id_ec_match
assert r(unique)==r(N)
egen temp_max_all = max(clu_id_all_match), by(sp_uuid)
egen temp_max_ec = max(clu_id_ec_match), by(sp_uuid)
egen temp_tag = tag(sp_uuid)

sum temp_max_all if temp_tag & parcelid!="" // 82.4% of SPs assigned to a CLU-matched parcel
	// also match to a CLU that's concordanced to that same parcel

sum temp_max_ec if temp_tag & parcelid!="" // 83.0% of SPs assigned to a CLU-matched parcel
	// also match to an ever-crop CLU that's concordanced to that same parcel

tabstat temp_max_ec if temp_tag & parcelid!="", by(county) s(mean n)	
	// only 51% for San Joaquin, but 88% for Fresno...
	
egen temp_denom = sum(pct_int_parcel), by(sp_uuid)
assert round(temp_denom,0.0001)<=1
replace temp_denom = min(temp_denom,1)
replace pct_int_parcel = pct_int_parcel/temp_denom
tabstat pct_int_parcel if parcelid!="" & temp_max_all==1, by(clu_id_all_match) ///
	s(p25 p50 p75 mean n)

	
** SP premise lat/lons, assigned CLUs (all)
use "$dirpath_data/pge_cleaned/sp_premise_gis.dta", clear
keep sp_uuid bad_geocode_flag missing_geocode_flag pull county parcelid parcel_dist_miles ///
	parcelid_conc parcel_conc_dist_miles in_clu clu_id clu_county
rename parcelid parcelid_all
joinby clu_id using "$dirpath_data/cleaned_spatial/clu_parcel_conc.dta", unmatched(master)
gen parcelid_all_match = parcelid_all!="" & parcelid_all==parcelid
gen parcelid_conc_match = parcelid_conc!="" & parcelid_conc==parcelid
collapse (sum) pct_int_clu, by(sp_uuid bad_geocode_flag missing_geocode_flag pull ///
	county in_clu clu_id clu_county parcelid_all_match parcelid_conc_match) fast
unique sp_uuid parcelid_all_match parcelid_conc_match
assert r(unique)==r(N)
egen temp_max_all = max(parcelid_all_match), by(sp_uuid)
egen temp_max_conc = max(parcelid_conc_match), by(sp_uuid)
egen temp_tag = tag(sp_uuid)

sum temp_max_all if temp_tag & clu_id!="" // 65.6% of SPs assigned to a CLU
	// also match to a parcel that's concordanced to that same CLU

sum temp_max_conc if temp_tag & clu_id!="" // 80.4% of SPs assigned to a CLU
	// also match to a concordanced parcel that's concordanced to that same CLU

tabstat temp_max_conc if temp_tag & clu_id!="", by(county) s(mean n)	

egen temp_denom = sum(pct_int_clu), by(sp_uuid)
sum temp_denom, det
replace pct_int_clu = pct_int_clu/temp_denom
tabstat pct_int_clu if clu_id!="" & temp_max_conc==1, by(parcelid_conc_match) ///
	s(p25 p50 p75 mean n)


** SP premise lat/lons, assigned CLUs (ever-crop)
use "$dirpath_data/pge_cleaned/sp_premise_gis.dta", clear
keep sp_uuid bad_geocode_flag missing_geocode_flag pull county parcelid parcel_dist_miles ///
	parcelid_conc parcel_conc_dist_miles in_clu_ec clu_id_ec clu_ec_county
rename parcelid parcelid_all
rename clu_id_ec clu_id
joinby clu_id using "$dirpath_data/cleaned_spatial/clu_parcel_conc.dta", unmatched(master)
gen parcelid_all_match = parcelid_all!="" & parcelid_all==parcelid
gen parcelid_conc_match = parcelid_conc!="" & parcelid_conc==parcelid
collapse (sum) pct_int_clu, by(sp_uuid bad_geocode_flag missing_geocode_flag pull ///
	county in_clu_ec clu_id clu_ec_county parcelid_all_match parcelid_conc_match) fast
unique sp_uuid parcelid_all_match parcelid_conc_match
assert r(unique)==r(N)
egen temp_max_all = max(parcelid_all_match), by(sp_uuid)
egen temp_max_conc = max(parcelid_conc_match), by(sp_uuid)
egen temp_tag = tag(sp_uuid)

sum temp_max_all if temp_tag & clu_id!="" // 64.5% of SPs assigned to an ever-crop CLU
	// also match to a parcel that's concordanced to that same CLU

sum temp_max_conc if temp_tag & clu_id!="" // 81.1% of SPs assigned to an ever-crop CLU
	// also match to a concordanced parcel that's concordanced to that same CLU

sum temp_max_conc if temp_tag & clu_id!="" & pull=="20180719" 
	// 86.3% for APEP-matched SPs
	
tabstat temp_max_conc if temp_tag & clu_id!="", by(county) s(mean n)	
	// only 51% for San Joaquin, but 89% for Fresno...

tabstat temp_max_conc if temp_tag & clu_id!="" & pull=="20180719", by(county) s(mean n)	
	// only 56% for San Joaquin, but 91% for Fresno...

egen temp_denom = sum(pct_int_clu), by(sp_uuid)
sum temp_denom, det
replace pct_int_clu = pct_int_clu/temp_denom
tabstat pct_int_clu if clu_id!="" & temp_max_conc==1, by(parcelid_conc_match) ///
	s(p25 p50 p75 mean n)


** APEP pump lat/lons, assigned parcels (all)
use "$dirpath_data/pge_cleaned/apep_pump_gis.dta", clear
keep latlon_group county in_parcel parcelid ///
	parcel_county clu_id clu_dist_miles clu_id_ec clu_ec_dist_miles
rename clu_id clu_id_all
joinby parcelid using "$dirpath_data/cleaned_spatial/clu_parcel_conc.dta", unmatched(master)
gen clu_id_all_match = clu_id_all!="" & clu_id_all==clu_id
gen clu_id_ec_match = clu_id_ec!="" & clu_id_ec==clu_id
collapse (sum) pct_int_parcel, by(latlon_group ///
	county in_parcel parcelid parcel_county clu_id_all_match clu_id_ec_match) fast
unique latlon_group clu_id_all_match clu_id_ec_match
assert r(unique)==r(N)
egen temp_max_all = max(clu_id_all_match), by(latlon_group)
egen temp_max_ec = max(clu_id_ec_match), by(latlon_group)
egen temp_tag = tag(latlon_group)

sum temp_max_all if temp_tag & parcelid!="" // 71.8% of pumps assigned to a parcel
	// also match to a CLU that's concordanced to that same parcel

sum temp_max_ec if temp_tag & parcelid!="" // 71.3% of pumps assigned to a parcel
	// also match to an ever-crop CLU that's concordanced to that same parcel

tabstat temp_max_ec if temp_tag & parcelid!="", by(county) s(mean n)	

egen temp_denom = sum(pct_int_parcel), by(latlon_group)
replace pct_int_parcel = pct_int_parcel/temp_denom
tabstat pct_int_parcel if parcelid!="" & temp_max_all==1, by(clu_id_all_match) ///
	s(p25 p50 p75 mean n)

	

** APEP pump lat/lons, assigned parcels (CLU-matched)
use "$dirpath_data/pge_cleaned/apep_pump_gis.dta", clear
keep latlon_group county in_parcel_conc parcelid_conc ///
	parcel_conc_county clu_id clu_dist_miles clu_id_ec clu_ec_dist_miles
rename clu_id clu_id_all
rename parcelid_conc parcelid
joinby parcelid using "$dirpath_data/cleaned_spatial/clu_parcel_conc.dta", unmatched(master)
gen clu_id_all_match = clu_id_all!="" & clu_id_all==clu_id
gen clu_id_ec_match = clu_id_ec!="" & clu_id_ec==clu_id
collapse (sum) pct_int_parcel, by(latlon_group ///
	county in_parcel parcelid parcel_conc_county clu_id_all_match clu_id_ec_match) fast
unique latlon_group clu_id_all_match clu_id_ec_match
assert r(unique)==r(N)
egen temp_max_all = max(clu_id_all_match), by(latlon_group)
egen temp_max_ec = max(clu_id_ec_match), by(latlon_group)
egen temp_tag = tag(latlon_group)

sum temp_max_all if temp_tag & parcelid!="" // 88.0% of pumps assigned to a CLU-matched parcel
	// also match to a CLU that's concordanced to that same parcel

sum temp_max_ec if temp_tag & parcelid!="" // 88.8% of pumps assigned to a CLU-matched parcel
	// also match to an ever-crop CLU that's concordanced to that same parcel

tabstat temp_max_ec if temp_tag & parcelid!="", by(county) s(mean n)	
	// only 61% for San Joaquin, but 92% for Fresno...
	
egen temp_denom = sum(pct_int_parcel), by(latlon_group)
replace pct_int_parcel = pct_int_parcel/temp_denom
tabstat pct_int_parcel if parcelid!="" & temp_max_all==1, by(clu_id_all_match) ///
	s(p25 p50 p75 mean n)

	
	
** APEP pump lat/lons, assigned CLUs (all)
use "$dirpath_data/pge_cleaned/apep_pump_gis.dta", clear
keep latlon_group county parcelid parcel_dist_miles ///
	parcelid_conc parcel_conc_dist_miles in_clu clu_id clu_county
rename parcelid parcelid_all
joinby clu_id using "$dirpath_data/cleaned_spatial/clu_parcel_conc.dta", unmatched(master)
gen parcelid_all_match = parcelid_all!="" & parcelid_all==parcelid
gen parcelid_conc_match = parcelid_conc!="" & parcelid_conc==parcelid
collapse (sum) pct_int_clu, by(latlon_group ///
	county in_clu clu_id clu_county parcelid_all_match parcelid_conc_match) fast
unique latlon_group parcelid_all_match parcelid_conc_match
assert r(unique)==r(N)
egen temp_max_all = max(parcelid_all_match), by(latlon_group)
egen temp_max_conc = max(parcelid_conc_match), by(latlon_group)
egen temp_tag = tag(latlon_group)

sum temp_max_all if temp_tag & clu_id!="" // 74.1% of pumps assigned to a CLU
	// also match to a parcel that's concordanced to that same CLU

sum temp_max_conc if temp_tag & clu_id!="" // 86.1% of pumps assigned to a CLU
	// also match to a concordanced parcel that's concordanced to that same CLU

tabstat temp_max_conc if temp_tag & clu_id!="", by(county) s(mean n)	

egen temp_denom = sum(pct_int_clu), by(latlon_group)
sum temp_denom, det
replace pct_int_clu = pct_int_clu/temp_denom
tabstat pct_int_clu if clu_id!="" & temp_max_conc==1, by(parcelid_conc_match) ///
	s(p25 p50 p75 mean n)


** APEP pump lat/lons, assigned CLUs (ever-crop)
use "$dirpath_data/pge_cleaned/apep_pump_gis.dta", clear
keep latlon_group county parcelid parcel_dist_miles ///
	parcelid_conc parcel_conc_dist_miles in_clu_ec clu_id_ec clu_ec_county
rename parcelid parcelid_all
rename clu_id_ec clu_id
joinby clu_id using "$dirpath_data/cleaned_spatial/clu_parcel_conc.dta", unmatched(master)
gen parcelid_all_match = parcelid_all!="" & parcelid_all==parcelid
gen parcelid_conc_match = parcelid_conc!="" & parcelid_conc==parcelid
collapse (sum) pct_int_clu, by(latlon_group ///
	county in_clu_ec clu_id clu_ec_county parcelid_all_match parcelid_conc_match) fast
unique latlon_group parcelid_all_match parcelid_conc_match
assert r(unique)==r(N)
egen temp_max_all = max(parcelid_all_match), by(latlon_group)
egen temp_max_conc = max(parcelid_conc_match), by(latlon_group)
egen temp_tag = tag(latlon_group)

sum temp_max_all if temp_tag & clu_id!="" // 74.1% of pumps assigned to an ever-crop CLU
	// also match to a parcel that's concordanced to that same CLU

sum temp_max_conc if temp_tag & clu_id!="" // 87.6% of pumps assigned to an ever-crop CLU
	// also match to a concordanced parcel that's concordanced to that same CLU
	
tabstat temp_max_conc if temp_tag & clu_id!="", by(county) s(mean n)	
	// only 51% for San Joaquin, but 88% for Fresno...

egen temp_denom = sum(pct_int_clu), by(latlon_group)
sum temp_denom, det
replace pct_int_clu = pct_int_clu/temp_denom
tabstat pct_int_clu if clu_id!="" & temp_max_conc==1, by(parcelid_conc_match) ///
	s(p25 p50 p75 mean n)	
	
	
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
