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

tab temp_parcel_conc_any // 95%
tab temp_parcel_conc_any if pull=="20180719" // 94% in both
tab temp_parcel_conc_any if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 // 97%

	// Compare parcels (properly within polygon)
tab in_parcel in_parcel_conc // 65% in both
tab in_parcel in_parcel_conc if pull=="20180719" // 76% in both
tab in_parcel in_parcel_conc if pull=="20180719" & bad_geocode_flag!=1 & ///
	missing_geocode_flag!=1 // 76% in both (barely a sample restriction)
	
tab temp_parcel_match if in_parcel==1 & in_parcel_conc==1 & ///
	bad_geocode_flag!=1 & missing_geocode_flag!=1 & parcel_county==parcel_conc_county // 92%
tab temp_parcel_match if in_parcel==1 & in_parcel_conc==1 & pull=="20180719" & ///
	bad_geocode_flag!=1 & missing_geocode_flag!=1 & parcel_county==parcel_conc_county // 87%
	
tab temp_parcel_match if in_parcel==1 & in_parcel_conc==1 & pull=="20180719" & ///
	bad_geocode_flag!=1 & missing_geocode_flag!=1 & parcel_county==parcel_conc_county & ///
	parcel_county!="Kern" // 99.8%
tab temp_parcel_match if in_parcel==1 & in_parcel_conc==1 & pull=="20180719" & ///
	bad_geocode_flag!=1 & missing_geocode_flag!=1 & parcel_county==parcel_conc_county & ///
	parcel_county=="Kern" // 45% THE PROBLEM IS KERN COUNTY

	// Compare parcels (properly within polygon, or < 1 mile away)
tab temp_parcel_match if parcelid!="" & parcelid_conc!="" & ///
	bad_geocode_flag!=1 & missing_geocode_flag!=1 & parcel_county==parcel_conc_county // 73%
tab temp_parcel_match if parcelid!="" & parcelid_conc!="" & pull=="20180719" & ///
	bad_geocode_flag!=1 & missing_geocode_flag!=1 & parcel_county==parcel_conc_county // 77%

tab temp_parcel_match if parcelid!="" & parcelid_conc!="" & ///
	bad_geocode_flag!=1 & missing_geocode_flag!=1 & parcel_county==parcel_conc_county & ///
	parcel_county!="Kern" // 77%
tab temp_parcel_match if parcelid!="" & parcelid_conc!="" & pull=="20180719" & ///
	bad_geocode_flag!=1 & missing_geocode_flag!=1 & parcel_county==parcel_conc_county & ///
	parcel_county!="Kern" // 84%

tab temp_parcel_match if parcelid!="" & parcelid_conc!="" & ///
	bad_geocode_flag!=1 & missing_geocode_flag!=1 & parcel_county==parcel_conc_county & ///
	parcel_county=="Kern" // 47% KERN KERN KERN
tab temp_parcel_match if parcelid!="" & parcelid_conc!="" & pull=="20180719" & ///
	bad_geocode_flag!=1 & missing_geocode_flag!=1 & parcel_county==parcel_conc_county & ///
	parcel_county=="Kern" // 45% KERN KERN KERN

	// Compare parcels (properly within polygon, or < 1 mile away)
tab temp_parcel_match if parcelid!="" & parcelid_conc!="" & ///
	bad_geocode_flag!=1 & missing_geocode_flag!=1 & parcel_county==parcel_conc_county // 73%
tab temp_parcel_match if parcelid!="" & parcelid_conc!="" & pull=="20180719" & ///
	bad_geocode_flag!=1 & missing_geocode_flag!=1 & parcel_county==parcel_conc_county // 77%

tab temp_parcel_match if parcelid!="" & parcelid_conc!="" & ///
	bad_geocode_flag!=1 & missing_geocode_flag!=1 & parcel_county==parcel_conc_county & ///
	parcel_county!="Kern" // 77%
tab temp_parcel_match if parcelid!="" & parcelid_conc!="" & pull=="20180719" & ///
	bad_geocode_flag!=1 & missing_geocode_flag!=1 & parcel_county==parcel_conc_county & ///
	parcel_county!="Kern" // 84%

tab temp_parcel_match if parcelid!="" & parcelid_conc!="" & ///
	bad_geocode_flag!=1 & missing_geocode_flag!=1 & parcel_county==parcel_conc_county & ///
	parcel_county=="Kern" // 47% KERN KERN KERN
tab temp_parcel_match if parcelid!="" & parcelid_conc!="" & pull=="20180719" & ///
	bad_geocode_flag!=1 & missing_geocode_flag!=1 & parcel_county==parcel_conc_county & ///
	parcel_county=="Kern" // 45% KERN KERN KERN

	// Formalizing validation checks, from weakest to strongest
drop if parcel_county=="Kern" | parcel_conc_county=="Kern"	
	
	// Denominator
count if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 
local rN = r(N)

	// Has *a* parcel
count if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 & ///
	(parcelid!="" | parcelid_conc!="")
di r(N)/`rN' // 97.6%
	
	// Has *a* parcel that matches county
count if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 & ///
	(parcelid!="" | parcelid_conc!="") & (parcel_county==county | parcel_conc_county==county)
di r(N)/`rN' // 95.1%
	
	// Has CLU-matched parcel that matches county
count if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 & ///
	parcelid_conc!="" & parcel_conc_county==county
di r(N)/`rN' // 93.7%
	
	// Has CLU-matched parcel that matches county, and matches unrestricted parcel
count if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 & ///
	parcelid_conc!="" & parcel_conc_county==county & temp_parcel_match==1
di r(N)/`rN' // 77.0%
	
	// Properly within CLU-matched parcel that matches county, and matches unrestricted parcel
count if pull=="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 & ///
	parcelid_conc!="" & parcel_conc_county==county & temp_parcel_match==1 & in_parcel_conc==1
di r(N)/`rN' // 67.4%

	// Comparing to non-APEP pulls
count if pull!="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1
local rN = r(N)
count if pull!="20180719" & bad_geocode_flag!=1 & missing_geocode_flag!=1 & ///
	parcelid_conc!="" & parcel_conc_county==county & temp_parcel_match==1 & in_parcel_conc==1
di r(N)/`rN' // 54.1%, not bad!


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
tab temp_parcel_conc_any // 95.7%

	// Compare parcels (properly within polygon)
tab in_parcel in_parcel_conc // 80.7% in both
	
tab temp_parcel_match if in_parcel==1 & in_parcel_conc==1 & ///
	parcel_county==parcel_conc_county // 77.4%
	
tab temp_parcel_match if in_parcel==1 & in_parcel_conc==1 & ///
	parcel_county==parcel_conc_county & parcel_county!="Kern" // 99.8%
	
tab temp_parcel_match if in_parcel==1 & in_parcel_conc==1 & ///
	parcel_county==parcel_conc_county & parcel_county=="Kern" // 25.8% THE PROBLEM IS KERN COUNTY

	// Compare parcels (properly within polygon, or < 1 mile away)
tab temp_parcel_match if parcelid!="" & parcelid_conc!="" & ///
	parcel_county==parcel_conc_county // 68.8%

tab temp_parcel_match if parcelid!="" & parcelid_conc!="" & ///
	parcel_county==parcel_conc_county & parcel_county!="Kern" // 83.5%

tab temp_parcel_match if parcelid!="" & parcelid_conc!="" & ///
	parcel_county==parcel_conc_county & parcel_county=="Kern" // 28.8%

	// Formalizing validation checks, from weakest to strongest
drop if parcel_county=="Kern" | parcel_conc_county=="Kern"	
	
	// Denominator
count
local rN = r(N)

	// Has *a* parcel
count if (parcelid!="" | parcelid_conc!="")
di r(N)/`rN' // 97.0%
	
	// Has *a* parcel that matches county
count if (parcelid!="" | parcelid_conc!="") & (parcel_county==county | parcel_conc_county==county)
di r(N)/`rN' // 95.3%
	
	// Has CLU-matched parcel that matches county
count if parcelid_conc!="" & parcel_conc_county==county
di r(N)/`rN' // 92.3%
	
	// Has CLU-matched parcel that matches county, and matches unrestricted parcel
count if parcelid_conc!="" & parcel_conc_county==county & temp_parcel_match==1
di r(N)/`rN' // 75.8%
	
	// Properly within CLU-matched parcel that matches county, and matches unrestricted parcel
count if parcelid_conc!="" & parcel_conc_county==county & temp_parcel_match==1 & in_parcel_conc==1
di r(N)/`rN' // 71.9%



}

*******************************************************************************
*******************************************************************************

** 2. Compare assigned CLUs, all vs. ever-crop
{


}

*******************************************************************************
*******************************************************************************

** 3. Compare parcel-CLU concordance vs. parcel-CLU assignments
{


}

*******************************************************************************
*******************************************************************************

** 4. Compare assigned parcels, SP lat/lon vs. APEP lat/lon
{


}

*******************************************************************************
*******************************************************************************

** 5. Compare assigned CLUs, SP lat/lon vs. APEP lat/lon
{


}

*******************************************************************************
*******************************************************************************

