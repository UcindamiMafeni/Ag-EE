clear all
version 13
set more off

*************************************************************
**** Script to aggregate SP/APEP units up to fields/farms ***
*************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"
global dirpath_code "T:/Home/Louis/backup/AgEE/AgEE_code/build"
*global R_exe_path "C:/PROGRA~1/MIE74D~1/ROPEN~1/bin/x64/R"
*global R_lib_path "C:/Program Files/Microsoft/R Open/R-3.4.4/library"

*******************************************************************************
*******************************************************************************

** 1. Compare assigned parcels, all vs. CLU-matched
{

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
{

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
{

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

** 4. Aggregate parcel & CLU polygons up to "super-polygons"
{
	// Start with full concordance
use "$dirpath_data/cleaned_spatial/clu_parcel_conc.dta", clear
assert parcelid!="" & clu_id!=""
unique parcelid
unique clu_id
unique clu_id if ever_crop_clu==1

	// Drop barely-matched parcels and slivers
drop if drop_parcelid_total==1
drop if drop_slivers==1
unique parcelid
unique clu_id
unique clu_id if ever_crop_clu==1

	// Created numeric IDs, indexed separately for each county (to facilitate joins below)
egen group_p = group(parcelid)	
egen group_c = group(clu_id)
egen temp_p = min(group_p), by(county)
egen temp_c = min(group_c), by(county)
replace group_p = group_p - temp_p + 1
replace group_c = group_c - temp_c + 1

	// Confirm uniqueness
unique group_p county
local uniq = r(unique)
unique parcelid
assert r(unique)==`uniq'
unique group_c county
local uniq = r(unique)
unique clu_id
assert r(unique)==`uniq'

	// Flag duplicates
duplicates t group_p county, gen(dup_p)
duplicates t group_c county, gen(dup_c)

	// Group all like-merged CLUs, looping over counties (removing small intersections)
foreach small in 0 10 25 50 75 { // loop over small intersection cutoffs

	qui gen temp_clu_ratios`small' = .
	qui levelsof county, local(levs)
	foreach cty in `levs' { // loop over counties
		
		// Keep only observations in county
		preserve
		qui keep if county=="`cty'"
		
		// Flag largest intersections for each CLU, and drop small intersections
		qui egen double temp = max(intacres), by(group_c)
		qui unique group_c
		local uniq = r(unique)
		drop if intacres<`small'/100 & intacres<temp & largest_clu==0 & largest_parcel==0 
			// dropping small intersections, but never the major polygon of a merge
		qui unique group_c
		assert r(unique)==`uniq'
			// we don't want two sets of intermerged polygons to be grouped together 
			// because of one tenuous intersection that's very small
		
		// Keep essential variables only
		qui keep group_p group_c
		qui unique group_p group_c
		assert r(unique)==r(N)
		
		// Outerjoin dataset to itself on parcel ID
		tempfile `cty'_temp1
		qui save ``cty'_temp1'
		rename group_c group_cM
		qui joinby group_p using ``cty'_temp1', unmatched(both)
		assert _merge==3
		drop _merge group_p
		qui duplicates drop
		
		// Drop doubled matches (i.e A joins to B, then B also joins to A)
		sort group_cM group_c
		qui drop if group_c>group_cM
		
		// Define group variable for each CLU, as its minimum matched CLU ID
		qui egen c_group = min(group_c), by(group_cM)
		qui unique group_cM
		local uniq1 = r(unique)
		qui unique c_group
		local uniq2 = r(unique)
		di "`cty' `small': `uniq1' unique CLUs"
		di "`cty' `small': `uniq2' non-disjoint groups"
		
		// Drop matched CLU ID (no longer needed)
		drop group_c
		qui duplicates drop
		rename group_cM group_c
		qui unique group_c
		assert r(unique)==r(N)
		sort group_c
		assert group_c==_n 
		
		// Combine non-disjoint groups so they become disjoint
		qui levelsof c_group, local(levs)
		foreach g in `levs' {
			if c_group[`g']!=`g' { 
				qui replace c_group = c_group[`g'] if c_group==`g'
			}
		}

		// Confirm groups are now fully disjoint
		qui levelsof c_group, local(levs)
		foreach g in `levs' {
			assert c_group[`g']==`g'
		}
		// (If A joins to B, and B joins to C, but C doesn't joing to A, then the above
		// step assigned C to B's group, when B doesn't have it's own group. This loop
		// concisely fixes all of these non-disjoint groupings by switching C's group
		// from B to A, in all such cases.)
		
		qui unique c_group
		local uniq3 = r(unique)
		di "`cty' `small': `uniq3' disjoint groups"
		local ratio = string(`uniq1'/`uniq3',"%9.2f")
		di "`cty' `small': `ratio' ratio of polygons to disjoint groups" _n
		
		// Merge back into main dataset
		gen county = "`cty'"
		rename c_group c_group`small'
		tempfile `cty'_temp2
		qui save ``cty'_temp2'
		restore
		qui merge m:1 county group_c using ``cty'_temp2', update
		qui replace temp_clu_ratios`small' = `uniq1'/`uniq3' if county=="`cty'"
		assert _merge>=3 if county=="`cty'"
		drop _merge
	}	
	unique county group_c	
	unique county c_group`small'
	la var c_group`small' "CLU group identifiers, ignoring intersections <0.`small' acres"
	assert c_group`small'!=.

}	

	// Group all like-merged parcels, looping over counties (removing small intersections)
foreach small in 0 10 25 50 75 { // loop over small intersection cutoffs

	qui gen temp_parcel_ratios`small' = .
	qui levelsof county, local(levs)
	foreach cty in `levs' { // loop over counties
		
		// Keep only observations in county
		preserve
		qui keep if county=="`cty'"
		
		// Flag largest intersections for each parcel, and drop small intersections
		qui egen double temp = max(intacres), by(group_p)
		qui unique group_p
		local uniq = r(unique)
		drop if intacres<`small'/100 & intacres<temp & largest_clu==0 & largest_parcel==0 
			// dropping small intersections, but never the major polygon of a merge
		qui unique group_p
		assert r(unique)==`uniq'
			// we don't want two sets of intermerged polygons to be grouped together 
			// because of one tenuous intersection that's very small

		// Keep essential variables only
		qui keep group_p group_c
		qui unique group_p group_c
		assert r(unique)==r(N)
		
		// Outerjoin dataset to itself on CLU ID
		tempfile `cty'_temp1
		qui save ``cty'_temp1'
		rename group_p group_pM
		qui joinby group_c using ``cty'_temp1', unmatched(both)
		assert _merge==3
		drop _merge group_c
		qui duplicates drop
		
		// Drop doubled matches (i.e A joins to B, then B also joins to A)
		sort group_pM group_p
		qui drop if group_p>group_pM
		
		// Define group variable for each parcel, as its minimum matched parcel ID
		qui egen p_group = min(group_p), by(group_pM)
		qui unique group_pM
		local uniq1 = r(unique)
		qui unique p_group
		local uniq2 = r(unique)
		di "`cty' `small': `uniq1' parcels"
		di "`cty' `small': `uniq2' non-disjoint groups"
		
		// Drop matched parcel ID (no longer needed)
		drop group_p
		qui duplicates drop
		rename group_pM group_p
		qui unique group_p
		assert r(unique)==r(N)
		sort group_p
		assert group_p==_n 
		
		// Combine non-disjoint groups so they become disjoint
		qui levelsof p_group, local(levs)
		foreach g in `levs' {
			if p_group[`g']!=`g' { 
				qui replace p_group = p_group[`g'] if p_group==`g'
			}
		}
		
		// Confirm groups are now fully disjoint
		qui levelsof p_group, local(levs)
		foreach g in `levs' {
			assert p_group[`g']==`g'
		}

		// (If A joins to B, and B joins to C, but C doesn't joing to A, then the above
		// step assigned C to B's group, when B doesn't have it's own group. This loop
		// concisely fixes all of these non-disjoint groupings by switching C's group
		// from B to A, in all such cases.)
		
		qui unique p_group
		local uniq3 = r(unique)
		di "`cty' `small': `uniq3' disjoint groups"
		local ratio = string(`uniq1'/`uniq3',"%9.2f")
		di "`cty' `small': `ratio' ratio of polygons to disjoint groups" _n
		
		// Merge back into main dataset
		gen county = "`cty'"
		rename p_group p_group`small'
		tempfile `cty'_temp2
		qui save ``cty'_temp2'
		restore
		qui merge m:1 county group_p using ``cty'_temp2', update
		qui replace temp_parcel_ratios`small' = `uniq1'/`uniq3' if county=="`cty'"
		assert _merge>=3 if county=="`cty'"
		drop _merge
	}	
	unique county group_p	
	unique county p_group`small'
	la var p_group`small' "Parcel group identifiers, ignoring intersections <0.`small' acres"
	assert p_group`small'!=.

}	

preserve
gen uniq_clu = .
levelsof county, local(levs) 
foreach cty in `levs' {
	unique group_c if county=="`cty'"
	replace uniq_clu = r(unique) if county=="`cty'"
}
keep county temp_*_ratios* uniq_clu
duplicates drop
gsort -uniq_clu
list
restore

	// Reindex to non-county-specific IDs
foreach v of varlist c_group* p_group* {
	assert `v'!=.
	unique county `v'
	local uniq = r(unique)
	egen temp = group(county `v')
	replace `v' = temp
	drop temp
	unique `v'
	assert r(unique)==`uniq'
}
	
	// Rename group IDs
rename c_group* clu_group*
rename p_group* parcel_group*

	// Drop unnecessary variables
drop temp* group_p group_c dup_p dup_c	
compress

	// Save CLU groups
preserve
keep county clu_id clu_group*	
duplicates drop
unique clu_id
assert r(unique)==r(N)	
gen temp = _n
foreach v of varlist clu_group* {
	assert `v'!=.
	local vnew = subinstr("`v'","clu_group","clu_count",1)
	egen `vnew' = count(temp), by(`v')
	la var `vnew' "Number of unique CLUs grouped in `v'"
}
foreach v of varlist clu_group* {
	local vnew = subinstr("`v'","clu_group","temp_tag",1)
	egen `vnew' = tag(`v')
}	
foreach v of varlist clu_count* {
	local vnew = subinstr("`v'","clu_count","temp_count",1)
	gen `vnew' = min(`v',10)
}	
twoway ///
	(hist temp_count0  if temp_tag0 , w(1) color(black) fcolor(none) lw(thick)) ///
	(hist temp_count10 if temp_tag10, w(1) color(blue) fcolor(none) lw(thick)) ///
	(hist temp_count25 if temp_tag25, w(1) color(green) fcolor(none) lw(thick)) ///
	(hist temp_count50 if temp_tag50, w(1) color(red) fcolor(none) lw(thick)) ///
	(hist temp_count75 if temp_tag75, w(1) color(yellow) fcolor(none) lw(thick)), ///
	legend(order(1 "0" 2 "10" 3 "25" 4 "50" 5 "75") c(5))
drop temp*
sort clu_id
order county clu_id
compress
save "$dirpath_data/cleaned_spatial/clu_conc_groups.dta", replace
restore

	// Save parcel groups
preserve
keep county parcelid parcel_group*	
duplicates drop
unique parcelid
assert r(unique)==r(N)
gen temp = _n
foreach v of varlist parcel_group* {
	assert `v'!=.
	local vnew = subinstr("`v'","parcel_group","parcel_count",1)
	egen `vnew' = count(temp), by(`v')
	la var `vnew' "Number of unique parcels grouped in `v'"
}	
foreach v of varlist parcel_group* {
	local vnew = subinstr("`v'","parcel_group","temp_tag",1)
	egen `vnew' = tag(`v')
}	
foreach v of varlist parcel_count* {
	local vnew = subinstr("`v'","parcel_count","temp_count",1)
	gen `vnew' = min(`v',10)
}	
twoway ///
	(hist temp_count0  if temp_tag0 , w(1) color(black) fcolor(none) lw(thick)) ///
	(hist temp_count10 if temp_tag10, w(1) color(blue) fcolor(none) lw(thick)) ///
	(hist temp_count25 if temp_tag25, w(1) color(green) fcolor(none) lw(thick)) ///
	(hist temp_count50 if temp_tag50, w(1) color(red) fcolor(none) lw(thick)) ///
	(hist temp_count75 if temp_tag75, w(1) color(yellow) fcolor(none) lw(thick)), ///
	legend(order(1 "0" 2 "10" 3 "25" 4 "50" 5 "75") c(5))
drop temp*
sort parcelid
order county parcelid	
compress
save "$dirpath_data/cleaned_spatial/parcel_conc_groups.dta", replace
restore

	// Save group-wise concordances
foreach small in 0 10 25 50 75 {
	preserve
	keep county clu_group`small' parcel_group`small'
	duplicates drop
	sort *
	compress
	save "$dirpath_data/cleaned_spatial/groups_conc`small'.dta", replace
	restore
}
	
	// Save full conconrdance with both sets of groups
keep county clu_id parcelid
unique clu_id parcelid
assert r(unique)==r(N)
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_conc_groups.dta", nogen
merge m:1 parcelid using "$dirpath_data/cleaned_spatial/parcel_conc_groups.dta", nogen
sort county clu_id parcelid
order county clu_id parcelid
compress
save "$dirpath_data/cleaned_spatial/clu_parcel_conc_groups_full.dta", replace

}

*******************************************************************************
*******************************************************************************

** 5. Merge group identifiers into SP GIS dataset
{
	// Start with SP GIS assignments
use "$dirpath_data/pge_cleaned/sp_premise_gis.dta", clear

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

	// Test whether assigned CLU groups (EC) match to assigned parcelid_conc
	
	
	// Test whether assigned parcel groups (conc) match to assigned clu_id_ec

}

*******************************************************************************
*******************************************************************************

/*	

CHECKS:

A) clu_id_ec matches clu_id

B) parcelid_conc matches parcelid

C) clu_id_ec and clu_id in the same clu_group

D) clu_group matches to assigned parcelid_conc

E) parcel_group matches to assigned clu_id[_ec]

F) SP and APEP coordinates have same clu_group assignments

G) SP and APEP coordinates have same parcel_group assigments
	
*/

*******************************************************************************
*******************************************************************************

