clear all
version 13
set more off

*******************************************************************************
**** Script to import and clean raw SCE data -- customer details file *********
*******************************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"
global dirpath_code "T:/Home/Louis/backup/AgEE/AgEE_code/build"
global dirpath_temp "$dirpath_code/Temp"
global R_exe_path "C:/PROGRA~1/MICROS~4/ROPEN~1/R-35~1.1/bin/x64/R"
*global R_lib_path "C:/Program Files/Microsoft/R Open/R-3.4.4/library"

*******************************************************************************
*******************************************************************************

** 1. Sept 2019 data pull (the only customer details in the July 2020 data pull is a DR participation file
{

** Start with raw customer data
use "$dirpath_data/sce_raw/customer_data_20190916.dta", clear

** Populate NAICS code
assert facility_naics_cd!=""
assert length(facility_naics_cd)==6 | facility_naics_cd=="."
//assert substr(facility_naics_cd,1,3)=="111"
rename facility_naics_cd prsn_naics
la var prsn_naics "6-digit NAICS at customer level"
tab prsn_naics
rename prsn_naics naics
merge m:1 naics using "$dirpath_data/misc/naics_descr.dta", keep(1 3) nogen
rename naics prsn_naics
replace prsn_naics = "" if prsn_naics=="."

** Service point ID
rename css_instl_serv_num sp_uuid
replace sp_uuid = "" if sp_uuid=="."
count if sp_uuid==""
duplicates r sp_uuid
unique sp_uuid // 29413 unique service points
la var sp_uuid "Service Point ID (anonymized)"

** Service account ID
rename serv_acct_num sa_uuid
assert sa_uuid!=""
unique sa_uuid //45243 unique service agreements
assert r(unique)==r(N)
la var sa_uuid "Service Account ID"

** Merge in customer ID from xwalk
rename sp_uuid sp_uuidM
merge 1:m sa_uuid using "$dirpath_data/sce_cleaned/customer_id_xwalk_20200504.dta"
count if _merge==3
local rN = r(N)
count if _merge==3 & sp_uuid==sp_uuidM
di r(N)/`rN' // sp_uuid matches for 99.8% of merges!
br sa_uuid sp_uuidM sp_uuid if _merge==3 & sp_uuid!=sp_uuidM // non-matches are all half missing
replace sp_uuidM = sp_uuid if sp_uuidM==""
drop sp_uuid
rename sp_uuidM sp_uuid
count if _merge!=2 & sp_uuid==""
	// confirm that no SPs matches exist that didn't match on SA
egen temp_min = min(_merge), by(sp_uuid)
egen temp_max = max(_merge), by(sp_uuid)
tab _merge temp_max if sp_uuid!=""
drop if _merge==2 
drop temp* _merge

** Reshape customer ID to make unique by sa_uuid
duplicates t sa_uuid, gen(dup)
tab dup
sort sa_uuid sp_uuid prsn_uuid
br sa_uuid sp_uuid prsn_uuid dup if dup>0
gen temp_row = _n
egen temp_row_min = min(temp_row), by(sa_uuid sp_uuid)
gen prsn_uuid1 = prsn_uuid if temp_row==temp_row_min
gen prsn_uuid2 = prsn_uuid[_n+1] if dup>0 & temp_row==temp_row_min & sa_uuid[_n+1]==sa_uuid & sp_uuid[_n+1]==sp_uuid
gen prsn_uuid3 = prsn_uuid[_n+2] if dup>1 & temp_row==temp_row_min & sa_uuid[_n+2]==sa_uuid & sp_uuid[_n+2]==sp_uuid
gen prsn_uuid4 = prsn_uuid[_n+3] if dup>2 & temp_row==temp_row_min & sa_uuid[_n+3]==sa_uuid & sp_uuid[_n+3]==sp_uuid
gen prsn_uuid5 = prsn_uuid[_n+4] if dup>3 & temp_row==temp_row_min & sa_uuid[_n+4]==sa_uuid & sp_uuid[_n+4]==sp_uuid
gen prsn_uuid6 = prsn_uuid[_n+5] if dup>4 & temp_row==temp_row_min & sa_uuid[_n+5]==sa_uuid & sp_uuid[_n+5]==sp_uuid
unique sa_uuid 
local uniq = r(unique)
drop if temp_row!=temp_row_min
drop temp* dup prsn_uuid
unique sa_uuid
assert r(unique)==r(N)
la var prsn_uuid1 "Customer ID 1"
la var prsn_uuid2 "Customer ID 2"
la var prsn_uuid3 "Customer ID 3"
la var prsn_uuid4 "Customer ID 4"
la var prsn_uuid5 "Customer ID 5"
la var prsn_uuid6 "Customer ID 6"

** Merge newly populated SPs back into xwalk
preserve
keep sa_uuid sp_uuid
duplicates drop
tempfile for_xwalk
save `for_xwalk'
use "$dirpath_data/sce_cleaned/customer_id_xwalk_20200504.dta", clear
rename sp_uuid sp_uuidM
merge m:1 sa_uuid using `for_xwalk'
replace sp_uuidM = sp_uuid if sp_uuidM==""
assert sp_uuid==sp_uuidM if sp_uuid!=""
drop sp_uuid _merge
rename sp_uuidM sp_uuid
duplicates drop
compress
save "$dirpath_data/sce_cleaned/customer_id_xwalk_20200504_updated.dta", replace
restore

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
duplicates t sp_uuid lat lon, gen(dup)
sort sp_uuid
br if dup>0
	// confirm lat/lon is unique by service point (where non-missing)
unique sp_uuid if sp_uuid!=""
local uniq = r(unique)
unique sp_uuid lat lon if sp_uuid!=""
assert `uniq'==r(unique)
drop dup

** Net energy metering indicator
tab nem_proatr_id, missing
gen net_mtr_ind=1
assert !missing(net_mtr_ind)
replace net_mtr_ind = 0 if missing(nem_proatr_id)
la var net_mtr_ind "Dummy for NEM participation"

** Demand response
preserve
use "$dirpath_data/sce_raw/demand_response_data_20190916.dta" , clear
gen pull = "20190916"
append using "$dirpath_data/sce_raw/demand_response_data_20200722.dta"
gen date_dr_start = date(subinstr(subinstr(lower(start_date),"-","",1),"-","20",1),"DMY")
gen date_dr_end = date(subinstr(subinstr(lower(end_date),"-","",1),"-","20",1),"DMY")
format %td date*
assert date_dr_start!=. & date_dr_end!=.
replace date_dr_start = mdy(month(date_dr_start), day(date_dr_start), year(date_dr_start)-1000) if year(date_dr_start)>2040
gen ndays = date_dr_end - date_dr_start
sort ndays
replace ndays = -ndays if ndays<0
drop prog_name start_date end_date pull proent_value_text
duplicates drop
collapse (sum) ndays, by(serv_acct_num)
rename serv_acct_num sa_uuid
sum ndays, detail
gen dr_ind = ndays>=365
keep if dr_ind==1
keep sa_uuid dr_ind
tempfile dr
save `dr'
restore
merge m:1 sa_uuid using `dr', nogen
replace dr_ind = 0 if dr_ind==.
la var dr_ind "Dummy for demand response participation (>=365 days)"

** Climate zone
tab climate_zone, missing 
replace climate_zone = "" if climate_zone=="."
count if climate_zone==""  // 1667 no missing
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
	
	// run auxilary GIS script "BUILD_sce_gis_sce_climate_zone_20190916.R"
*shell "${R_exe_path}" --vanilla <"${dirpath_code}/SCE_Data_Build/BUILD_sce_gis_climate_zone_20190916.R"
	
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
tempfile gis_out
save `gis_out'
restore

	// merge back into main dataset
merge m:1 sa_uuid using `gis_out'
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
drop longitude1 latitude1 czone

** Clean SCE-specific variables
la var sa_status_code "SA status code"
tab sa_status_code, missing

la var current_tariff "SA's current tariff"
tab current_tariff, missing

la var cust_name "Customer name"
la var service_zip "SP ZIP code"

assert nem_proatr_id=="" if net_mtr_ind==0
assert nem_proatr_id=="NEMTYPE" if net_mtr_ind==1
drop nem_proatr_id

br nem* if net_mtr_ind==1
tab nem_end_date
assert inlist(nem_end_date,"","31DEC2525")
drop nem_end_date
gen temp = date(nem_start_date,"DMY")
format %td temp
assert temp!=. if nem_start_date!=""
drop nem_start_date
rename temp nem_start_date
la var nem_start_date "NEM start date"
tab nem_text
rename nem_text nem_type 
la var nem_type "NEM description"

assert inlist(bcd_rate_grp_desc,"Ag & Pumping","")
assert inlist(puc_group_code,"AG")
tab bcd puc, missing
tab current_tariff bcd, missing
gen flag_non_ag_tariff = bcd_rate_grp_desc==""
la var flag_non_ag_tariff "Flag for tariffs that are not labeled 'Ag & Pumping' "
drop bcd_rate_grp_desc puc_group_code

tab revclass_type_desc, missing
assert revclass_type_desc=="NON DOMESTIC"
drop revclass_type_desc

br facility_sic_cd sic_desc prsn_naics naics_descr
drop facility_sic_cd sic_desc // redundant since we have NAICS

tab cec_sector_desc cec_sec_grp_desc, missing
la var cec_sec_grp_desc "CEC sector: agr, ind, or com"
la var cec_sector_desc "CEC subsector description"
rename cec_sec_grp_desc cec_sector
replace cec_sector = "Agr" if cec_sector=="Agricultural"
replace cec_sector = "Ind" if cec_sector=="Industrial"
replace cec_sector = "Com" if cec_sector=="Commercial"
replace cec_sector = "" if cec_sector=="Other"
assert length(cec_sector)==3 | cec_sector==""
rename cec_sector_desc cec_subsector
tab segment_name cec_sector, missing
tab segment_name cec_subsector
la var segment_name "Customer segment description"
tab ind_subgrp
la var ind_subgrp "Industry subgroup description"

** Rename latitude and longitude for consistency with PGE datasets
rename latitude prem_lat
rename longitude prem_long

** Confirm uniqueness and save
unique sp_uuid sa_uuid
assert r(unique)==r(N)
compress
save "$dirpath_data/sce_cleaned/sce_cust_detail_20190916.dta", replace

}

*******************************************************************************
*******************************************************************************
