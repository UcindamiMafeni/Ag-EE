clear all
version 13
set more off

*******************************************************************************
**** Script to import and clean raw SCE data -- customer details file *********
*******************************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"
global dirpath_code "C:/Users/clohani/Desktop/Code_Chinmay"
global dirpath_temp "$dirpath_code/Temp"
global R_exe_path "C:/PROGRA~1/MICROS~4/ROPEN~1/R-35~1.1/bin/x64/R"
*global R_lib_path "C:/Program Files/Microsoft/R Open/R-3.4.4/library"

*******************************************************************************
*******************************************************************************

use "$dirpath_data/sce_raw/customer_data_20190916.dta", clear

** NAICS code
assert facility_naics_cd!=""
assert length(facility_naics_cd)==6 | facility_naics_cd=="."
//assert substr(facility_naics_cd,1,3)=="111"
rename facility_naics_cd prsn_naics
la var prsn_naics "6-digit NAICS at customer level"
tab prsn_naics
rename prsn_naics naics
merge m:1 naics using "$dirpath_data/misc/naics_descr.dta", keep(1 3)
keep if _merge==3
drop _merge
rename naics prsn_naics

** No customer ID given to us yet

/*
** Customer ID
assert prsn_uuid!=""
assert length(prsn_uuid)==10
duplicates r prsn_uuid
unique prsn_uuid // 33253 unique customers
la var prsn_uuid "Customer ID (anonymized, 10-digit)"
*/

** Service point ID
rename css_instl_serv_num sp_uuid
assert sp_uuid!=""
duplicates r sp_uuid
unique sp_uuid // 29413 unique service points
la var sp_uuid "Service Point ID (anonymized)"

** Service account ID
rename serv_acct_num sa_uuid
duplicates r sa_uuid // unique
unique sa_uuid //39078 unique service agreements
la var sa_uuid "Service Account ID"

** Service agreement start/stop dates
assert sa_estab_date!=""
gen sa_start = date(sa_estab_date,"DMY")
gen sa_stop = date(sa_close_date,"DMY")
format %td sa_start sa_stop
assert sa_start<=sa_stop // stop not prior to start
count if sa_start==sa_stop 
drop sa_estab_date sa_close_date
la var sa_start "Start date (SA)"
la var sa_stop "Stop date (SA)"

** SCE meter number
count if meter_no=="" // 1,419 missings out of 39078
count if length(meter_no)==12 // >95% of nonmissings have 12 digits
unique meter_no // 28113 unique badge numbers
la var meter_no "SCE meter number"

** Geocoordinates
tostring latitude longitude, generate(lat1 lon1) force
charlist lat1
assert r(chars)==".0123456789" // confirm all numeric (and positive)
charlist lon1
assert r(chars)=="-.0123456789" // confirm all numeric
drop lat1 lon1
//0 lat longs seem to indicate missing information
assert latitude==0 if longitude==0
replace latitude=. if latitude==0
replace longitude=. if longitude==0
assert latitude>0 // latitude always positive
replace longitude=-longitude if longitude>0 // 1 observation seems like a typo
**twoway scatter lat lon, msize(vsmall) // looks mostly good, a few issues tho
count if lat==. | lon==. // 1% missing, deal with below
la var lat "Latitude of premises"
la var lon "Longitude of premises"
	// confirm lat/lon is unique by service point
unique sp_uuid
local uniq = r(unique)
unique sp_uuid lat lon
assert `uniq'==r(unique)

** Net energy metering indicator
tab nem_proatr_id, missing
gen net_mtr_ind=1
assert !missing(net_mtr_ind)
replace net_mtr_ind = 0 if missing(nem_proatr_id)
la var net_mtr_ind "Dummy for NEM participation"

** Presumably no DR information
/*
** Demand response
tab dr_program, missing
assert dr_program!=""
la var dr_program "Demand response program"
gen dr_ind = dr_program!="NOT ENROLLED"
la var dr_ind "Dummy for demand response participation"
*/

** Climate zone
tab climate_zone, missing 
count if climate_zone=="" // no missing
la var climate_zone "CA climate zone code"


** Cross-check lat/lon vs. climate zone, using CA Climate Zones shapefile
	// export coordinates and climate zones
preserve
keep sa_uuid lat lon climate_zone
duplicates drop
drop if lat==. | lon==. // GIS can't get nowhere with missing lat/lon
unique sa_uuid
assert r(unique)==r(N)
replace climate_zone = "Z07" if climate_zone=="" // an obviously wrong Climate Zone, so the R script won't break
outsheet using "$dirpath_data/misc/sce_prem_coord_raw_20190916.txt", comma replace
restore
	
	// run auxilary GIS script "BUILD_gis_sce_climate_zone_20190916.R"
shell "${R_exe_path}" --vanilla <"${dirpath_code}/BUILD_gis_climate_zone.R"
	
	// import results from GIS script
preserve
insheet using "$dirpath_data/misc/sce_prem_coord_polygon_20190916.csv", double comma clear
drop longitude latitude
replace czone_gis = "" if czone_gis=="NA"
replace czone_gis = "Z0" + czone_gis if length(czone_gis)==1
replace czone_gis = "Z" + czone_gis if length(czone_gis)==2
rename czone_gis climate_zone_gis
tostring climate_zone, replace
rename pou pou_name
replace pou_name = "" if pou_name=="NA"
rename bad_geocode bad_geocode_flag
gen bad_cz_flag = climate_zone!=climate_zone_gis  // GIS assigns different climate zone
tab climate* if bad_cz_flag==1, missing
drop climate_zone

unique sa_uuid
assert r(unique)==r(N)
tostring sa_uuid climate_zone, replace
save "$dirpath_temp/gis_out.dta", replace
restore

	// merge back into main dataset
merge m:1 sa_uuid using "$dirpath_temp/gis_out.dta"
assert _merge!=2 // confirm everything merges in
assert _merge==1 if latitude==. | longitude==. // confirm nothing merges if it has missing lat/lon
assert (latitude==. | longitude==.) if _merge==1	// confirm that all non-merges have missing lat/lon
gen missing_geocode_flag = _merge==1
drop _merge
*twoway (scatter prem_lat prem_lon if bad_cz_flag==0, msize(tiny) color(black)) ///
*	   (scatter prem_lat prem_lon if bad_cz_flag==1, msize(tiny) color(red))
	
	// label new variables
la var in_calif	"Dummy=1 if lat/lon are within California"
la var in_sce "Dummy=1 if lat/lon are within SCE service territory proper"
la var in_pou "Dummy=1 if lat/lon are withiin SCE-enveloped POU territory"
la var pou_name "Name of SCE-enveloped POU (or other notes)"
la var bad_geocode "Lat/lon not in SCE territory, and not in PGE-enveloped POU"
la var climate_zone_gis "Climate zone, as assigned by GIS shapefile using lat/lon"
la var bad_cz_flag "Flag for SCE-assigned climate zone that's contradicted by GIS"
la var missing_geocode_flag "SCE geocodes are missing"	

** Confirm uniqueness and save
unique sp_uuid sa_uuid
assert r(unique)==r(N)
compress
save "$dirpath_data/sce_cleaned/sce_cust_detail_20190916.dta", replace
