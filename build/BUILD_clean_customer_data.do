clear all
version 13
set more off

*******************************************************************************
**** Script to import and clean raw PGE data -- customer details file *********
*******************************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"
global dirpath_code "S:/Louis/backup/AgEE/AgEE_code/build"
*global R_exe_path "C:/PROGRA~1/MIE74D~1/ROPEN~1/bin/x64/R"
*global R_lib_path "C:/Program Files/Microsoft/R Open/R-3.4.4/library"

*******************************************************************************
*******************************************************************************

** 1. March 2018 data pull
{

** Load raw PGE customer data
use "$dirpath_data/pge_raw/customer_data_20180322.dta", clear

** NAICS code
assert prsn_naics_cd!=""
assert length(prsn_naics_cd)==6
assert substr(prsn_naics_cd,1,3)=="111"
rename prsn_naics_cd prsn_naics
la var prsn_naics "6-digit NAICS at customer level"
tab prsn_naics
rename prsn_naics naics
merge m:1 naics using "$dirpath_data/misc/naics_descr.dta", keep(1 3)
assert _merge==3
drop _merge
rename naics prsn_naics

** Customer ID
assert prsn_uuid!=""
assert length(prsn_uuid)==10
duplicates r prsn_uuid
unique prsn_uuid // 33253 unique customers
la var prsn_uuid "Customer ID (anonymized, 10-digit)"

** Service point ID
assert sp_uuid!=""
assert length(sp_uuid)==10
duplicates r sp_uuid
unique sp_uuid // 72114 unique service points
unique prsn_uuid sp_uuid // 96099 unique customer-service points
preserve
keep prsn_uuid sp_uuid
gen count = 1
collapse (sum) count, by(sp_uuid) fast
tab count // 74% of sp_uuid's have multiple customers IDs --> customer ID is not longitudinal??
restore
la var sp_uuid "Service Point ID (anonymized, 10-digit)"

** Service agreement ID
assert sa_uuid!=""
assert length(sa_uuid)==10
duplicates r sa_uuid // almost unique
unique sa_uuid // 162809 unique service agreements
unique prsn_uuid sp_uuid sa_uuid // 163160 unique customer-service point-service aggrements
duplicates r prsn_uuid sp_uuid sa_uuid
duplicates t prsn_uuid sp_uuid sa_uuid, gen(dup)
*br if dup>0 // don't appear to be real dupes, will make unique 
drop dup
la var sa_uuid "Service Agreement ID (anonymized, 10-digit)"

** Service agreement start/stop dates
assert sa_sp_start_dttm!=""
count if !(word(sa_sp_start_dttm,2)=="00:00:00")
	// 2% start at a particular time, but I don't think we care
count if !(word(sa_sp_stop_dttm,2)=="00:00:00" | sa_sp_stop_dttm=="") 
	// 4% stop at a particular time, but I don't think we care
gen sa_sp_start = date(word(sa_sp_start_dttm,1),"DMY")
gen sa_sp_stop = date(word(sa_sp_stop_dttm,1),"DMY")
format %td sa_sp_start sa_sp_stop
assert sa_sp_start<=sa_sp_stop // stop not prior to start
count if sa_sp_start==sa_sp_stop // 2% of obs have stop = start ???
drop sa_sp_start_dttm sa_sp_stop_dttm
la var sa_sp_start "Start date (SA/SP)"
la var sa_sp_stop "Stop date (SA/SP)"

** PGE meter badge number
count if pge_badge_nbr=="" // 756 missings out of 163,821
count if length(pge_badge_nbr)==10 // 94% of nonmissings have 10 digits
unique pge_badge_nbr // 71513 unique badge numbers
la var pge_badge_nbr "PG&E meter badge number"

** Geocoordinates
charlist prem_lat
assert r(chars)==".0123456789" // confirm all numeric (and positive)
charlist prem_lon
assert r(chars)=="-.0123456789" // confirm all numeric
destring prem_lat prem_lon, replace
assert prem_lat>0 // latitude always positive
assert prem_lon<0 | prem_lon==. // longitude always negative (when not missing)
*twoway scatter prem_lat prem_lon, msize(vsmall) // looks mostly good, a few issues tho
count if prem_lat==. | prem_lon==. // 1% missing, deal with below
la var prem_lat "Latitude of premises"
la var prem_lon "Longitude of premises"
	// confirm lat/lon is unique by service point
unique sp_uuid
local uniq = r(unique)
unique sp_uuid prem_lat prem_lon
assert `uniq'==r(unique)	

** Net energy metering indicator
tab net_mtr_ind, missing
assert net_mtr_ind!=""
replace net_mtr_ind = "0" if net_mtr_ind=="N"
replace net_mtr_ind = "1" if net_mtr_ind=="Y"
destring net_mtr_ind, replace
assert net_mtr_ind!=.
la var net_mtr_ind "Dummy for NEM participation"

** Demand response
tab dr_program, missing
assert dr_program!=""
la var dr_program "Demand response program"
gen dr_ind = dr_program!="NOT ENROLLED"
la var dr_ind "Dummy for demand response participation"

** Climate zone
tab climate_zone_cd, missing 
count if climate_zone_cd=="" // 37 missings
la var climate_zone_cd "CA climate zone code"

** Resolve duplicates where one observation stops and the next immediately starts
duplicates t prsn_uuid sp_uuid sa_uuid, gen(dup1)
duplicates t prsn_uuid sp_uuid sa_uuid pge_badge_nbr prem_lat prem_lon net_mtr_ind ///
	dr_program dr_in climate_zone_cd , gen(dup2)
assert dup1==dup2
unique prsn_uuid sp_uuid sa_uuid if dup1>0 // 109 unique dups
local uniq_dups = r(unique)
sort prsn_uuid sp_uuid sa_uuid sa_sp_start sa_sp_stop

	// transpose dates for duplicates
gen temp_first = dup1>0 & !(prsn_uuid==prsn_uuid[_n-1] & sp_uuid==sp_uuid[_n-1] & ///
	sa_uuid==sa_uuid[_n-1])
gen start1 = sa_sp_start if temp_first	
gen stop1 = sa_sp_stop if temp_first	
gen start2 = sa_sp_start[_n+1] if temp_first & ///
	(prsn_uuid==prsn_uuid[_n+1] & sp_uuid==sp_uuid[_n+1] & sa_uuid==sa_uuid[_n+1])
gen stop2 = sa_sp_stop[_n+1] if temp_first & ///
	(prsn_uuid==prsn_uuid[_n+1] & sp_uuid==sp_uuid[_n+1] & sa_uuid==sa_uuid[_n+1])
gen start3 = sa_sp_start[_n+2] if temp_first & ///
	(prsn_uuid==prsn_uuid[_n+2] & sp_uuid==sp_uuid[_n+2] & sa_uuid==sa_uuid[_n+2])
gen stop3 = sa_sp_stop[_n+2] if temp_first & ///
	(prsn_uuid==prsn_uuid[_n+2] & sp_uuid==sp_uuid[_n+2] & sa_uuid==sa_uuid[_n+2])
gen start4 = sa_sp_start[_n+3] if temp_first & ///
	(prsn_uuid==prsn_uuid[_n+3] & sp_uuid==sp_uuid[_n+3] & sa_uuid==sa_uuid[_n+3])
gen stop4 = sa_sp_stop[_n+3] if temp_first & ///
	(prsn_uuid==prsn_uuid[_n+3] & sp_uuid==sp_uuid[_n+3] & sa_uuid==sa_uuid[_n+3])
format %td start* stop*
order start1 stop1 start2 stop2 start3 stop3 start4 stop4, after(sa_sp_stop)	
assert start2>=stop1 & start3>=stop2 & start4>=stop3 if temp_first

	// keep one observation per duplicate
keep if temp_first | dup1==0
unique prsn_uuid sp_uuid sa_uuid if dup1>0 // 109 unique dups
assert `uniq_dups'==r(unique)

	// assign first and last date as start/stop
gen start_first = start1 if dup1>0 & temp_first
gen stop_last = .
replace stop_last = stop2 if dup1==1 & temp_first
replace stop_last = stop3 if dup1==2 & temp_first
replace stop_last = stop4 if dup1==3 & temp_first
format %td  start_first stop_last
assert sa_sp_start==start_first if temp_first
drop start_first
assert sa_sp_stop<=stop_last if temp_first
replace sa_sp_stop = stop_last if temp_first
drop stop_last

	// construct lapse dates
drop start1 stop4
rename stop1 sa_sp_lapse_start1 
rename stop2 sa_sp_lapse_start2 
rename stop3 sa_sp_lapse_start3 
rename start2 sa_sp_lapse_stop1 
rename start3 sa_sp_lapse_stop2 
rename start4 sa_sp_lapse_stop3

	// eliminate open-ended lapses
replace sa_sp_lapse_start3 = . if sa_sp_lapse_stop3==. & temp_first
replace sa_sp_lapse_start2 = . if sa_sp_lapse_stop2==. & temp_first
replace sa_sp_lapse_start1 = . if sa_sp_lapse_stop1==. & temp_first

	// eliminate lapses <= 15 days (most of which are <= 2 days)
gen len1 = sa_sp_lapse_stop1-sa_sp_lapse_start1 if temp_first
gen temp1 = len1<=15 & temp_first
replace sa_sp_lapse_start1 = sa_sp_lapse_start2 if temp1
replace sa_sp_lapse_stop1 = sa_sp_lapse_stop2 if temp1
replace sa_sp_lapse_start2 = sa_sp_lapse_start3 if temp1
replace sa_sp_lapse_stop2 = sa_sp_lapse_stop3 if temp1
replace sa_sp_lapse_start3 = . if temp1
replace sa_sp_lapse_stop3 = . if temp1
assert sa_sp_lapse_start3==. & sa_sp_lapse_stop3==.
drop sa_sp_lapse_start3 sa_sp_lapse_stop3

gen len2 = sa_sp_lapse_stop1-sa_sp_lapse_start1 if temp_first
gen temp2 = len2<=15 & temp_first
replace sa_sp_lapse_start1 = sa_sp_lapse_start2 if temp2
replace sa_sp_lapse_stop1 = sa_sp_lapse_stop2 if temp2
replace sa_sp_lapse_start2 = . if temp2
replace sa_sp_lapse_stop2 = . if temp2

gen len3 = sa_sp_lapse_stop1-sa_sp_lapse_start1 if temp_first
assert len3>15

gen len4 = sa_sp_lapse_stop2-sa_sp_lapse_start2 if temp_first
assert len4>15

	// clean up temp variables and label
drop dup1 dup2 temp* len?	
la var sa_sp_lapse_start1 "Start date of lapse 1 (when SA/SP was not listed as active"
la var sa_sp_lapse_stop1 "Stop date of lapse 1 (when SA/SP was not listed as active"
la var sa_sp_lapse_start2 "Start date of lapse 2 (when SA/SP was not listed as active"
la var sa_sp_lapse_stop2 "Stop date of lapse 2 (when SA/SP was not listed as active"
order dr_ind, before(dr_program)
order prsn_naics, before(naics_descr)

** Cross-check lat/lon vs. climate zone, using CA Climate Zones shapefile
	// export coordinates and climate zones
preserve
keep sp_uuid prem_lat prem_lon climate_zone_cd
duplicates drop
drop if prem_lat==. | prem_lon==. // GIS can't get nowhere with missing lat/lon
unique sp_uuid
assert r(unique)==r(N)
replace climate_zone_cd = "Z07" if climate_zone_cd=="" // an obviously wrong Climate Zone, so the R script won't break
outsheet using "$dirpath_data/misc/pge_prem_coord_raw_20180322.txt", comma replace
restore
	
	// run auxilary GIS script "BUILD_gis_climate_zone_20180322.R"
*shell "${R_exe_path}" --vanilla <"${dirpath_code}/BUILD_gis_climate_zone.R"
	
	// import results from GIS script
preserve
insheet using "$dirpath_data/misc/pge_prem_coord_polygon_20180322.csv", double comma clear
drop prem_lat prem_lon longitude latitude czone 
replace czone_gis = "" if czone_gis=="NA"
replace czone_gis = "Z0" + czone_gis if length(czone_gis)==1
replace czone_gis = "Z" + czone_gis if length(czone_gis)==2
rename czone_gis climate_zone_cd_gis
rename pou pou_name
replace pou_name = "" if pou_name=="NA"
rename bad_geocode bad_geocode_flag
gen bad_cz_flag = climate_zone_cd!=climate_zone_cd_gis  // GIS assigns different climate zone
tab climate* if bad_cz_flag==1, missing
drop climate_zone_cd
tostring sp_uuid, replace
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
assert length(sp_uuid)==10
unique sp_uuid
assert r(unique)==r(N)
tempfile gis_out
save `gis_out'
restore

	// merge back into main dataset
merge m:1 sp_uuid using `gis_out'	
assert _merge!=2 // confirm everything merges in
assert _merge==1 if prem_lat==. | prem_lon==. // confirm nothing merges if it has missing lat/lon
assert (prem_lat==. | prem_lon==.) if _merge==1	// confirm that all non-merges have missing lat/lon
gen missing_geocode_flag = _merge==1
drop _merge
*twoway (scatter prem_lat prem_lon if bad_cz_flag==0, msize(tiny) color(black)) ///
*	   (scatter prem_lat prem_lon if bad_cz_flag==1, msize(tiny) color(red))
	
	// label new variables
la var in_calif	"Dummy=1 if lat/lon are within California"
la var in_pge "Dummy=1 if lat/lon are within PGE service territory proper"
la var in_pou "Dummy=1 if lat/lon are withiin PGE-enveloped POU territory"
la var pou_name "Name of PGE-enveloped POU (or other notes)"
la var bad_geocode "Lat/lon not in PGE territory, and not in PGE-enveloped POU"
la var climate_zone_cd_gis "Climate zone, as assigned by GIS shapefile using lat/lon"
la var bad_cz_flag "Flag for PGE-assigned climate zone that's contradicted by GIS"
la var missing_geocode_flag "PGE geocodes are missing"	


** Confirm uniqueness and save
unique prsn_uuid sp_uuid sa_uuid
assert r(unique)==r(N)
compress
save "$dirpath_data/pge_cleaned/pge_cust_detail_20180322.dta", replace	

}

*******************************************************************************
*******************************************************************************

** 2. July 2018 data pull
{

** Load raw PGE customer data
use "$dirpath_data/pge_raw/customer_data_20180719.dta", clear

** NAICS code
count if prsn_naics_cd=="" // 390 missings out of 40,907
replace prsn_naics_cd = "" if prsn_naics_cd=="0"
assert length(prsn_naics_cd)==6 if prsn_naics_cd!=""
count if substr(prsn_naics_cd,1,3)!="111" // 19,904 out of 40,907!
rename prsn_naics_cd prsn_naics
la var prsn_naics "6-digit NAICS at customer level"
tab prsn_naics
rename prsn_naics naics
merge m:1 naics using "$dirpath_data/misc/naics_descr.dta", keep(1 3)
tab naics if _merge==3
drop _merge
rename naics prsn_naics

** Customer ID
assert prsn_uuid!=""
assert length(prsn_uuid)==10
duplicates r prsn_uuid
unique prsn_uuid // 7764 unique customers
la var prsn_uuid "Customer ID (anonymized, 10-digit)"

** Service point ID
assert sp_uuid!=""
assert length(sp_uuid)==10
duplicates r sp_uuid
unique sp_uuid // 13305 unique service points
unique prsn_uuid sp_uuid // 22508 unique customer-service points
preserve
keep prsn_uuid sp_uuid
gen count = 1
collapse (sum) count, by(sp_uuid) fast
tab count // 96% of sp_uuid's have multiple customers IDs --> customer ID is not longitudinal??
restore
la var sp_uuid "Service Point ID (anonymized, 10-digit)"

** Service agreement ID
assert sa_uuid!=""
assert length(sa_uuid)==10
duplicates r sa_uuid // almost unique
unique sa_uuid // 40857 unique service agreements
unique prsn_uuid sp_uuid sa_uuid // 40871 unique customer-service point-service aggrements
duplicates r prsn_uuid sp_uuid sa_uuid
duplicates t prsn_uuid sp_uuid sa_uuid, gen(dup)
*br if dup>0 // don't appear to be real dupes, will make unique 
drop dup
la var sa_uuid "Service Agreement ID (anonymized, 10-digit)"

** Service agreement start/stop dates
assert sa_sp_start_dttm!=""
count if !(word(sa_sp_start_dttm,2)=="00:00:00")
	// 2% start at a particular time, but I don't think we care
count if !(word(sa_sp_stop_dttm,2)=="00:00:00" | sa_sp_stop_dttm=="") 
	// 2% stop at a particular time, but I don't think we care
gen sa_sp_start = date(word(sa_sp_start_dttm,1),"DMY")
gen sa_sp_stop = date(word(sa_sp_stop_dttm,1),"DMY")
format %td sa_sp_start sa_sp_stop
assert sa_sp_start<=sa_sp_stop // stop not prior to start
count if sa_sp_start==sa_sp_stop // 2% of obs have stop = start ???
drop sa_sp_start_dttm sa_sp_stop_dttm
la var sa_sp_start "Start date (SA/SP)"
la var sa_sp_stop "Stop date (SA/SP)"

** PGE meter badge number
assert pge_badge_nbr!="" // badge number NEVER missing!
count if length(pge_badge_nbr)==10 // 98% have 10 digits ( ~= smart meter)
unique pge_badge_nbr // 13241 unique badge numbers
la var pge_badge_nbr "PG&E meter badge number"

** Geocoordinates
charlist prem_lat
assert r(chars)==".0123456789" // confirm all numeric (and positive)
charlist prem_lon
assert r(chars)=="-.0123456789" // confirm all numeric
destring prem_lat prem_lon, replace
assert prem_lat>0 // latitude always positive
assert prem_lon<0 | prem_lon==. // longitude always negative (when not missing)
*twoway scatter prem_lat prem_lon, msize(vsmall) // looks mostly good, a few issues tho
count if prem_lat==. | prem_lon==. // 3% missing, deal with below
la var prem_lat "Latitude of premises"
la var prem_lon "Longitude of premises"
	// confirm lat/lon is unique by service point
unique sp_uuid
local uniq = r(unique)
unique sp_uuid prem_lat prem_lon
assert `uniq'==r(unique)	

** Net energy metering indicator
tab net_mtr_ind, missing
assert net_mtr_ind!=""
replace net_mtr_ind = "0" if net_mtr_ind=="N"
replace net_mtr_ind = "1" if net_mtr_ind=="Y"
destring net_mtr_ind, replace
assert net_mtr_ind!=.
la var net_mtr_ind "Dummy for NEM participation"

** Demand response
tab dr_program, missing
assert dr_program!=""
la var dr_program "Demand response program"
gen dr_ind = dr_program!="NOT ENROLLED"
la var dr_ind "Dummy for demand response participation"

** Climate zone
tab climate_zone_cd, missing 
count if climate_zone_cd=="" // 13 missings
la var climate_zone_cd "CA climate zone code"

** Resolve duplicates where one observation stops and the next immediately starts
duplicates t prsn_uuid sp_uuid sa_uuid, gen(dup1)
duplicates t prsn_uuid sp_uuid sa_uuid pge_badge_nbr prem_lat prem_lon net_mtr_ind ///
	dr_program dr_in climate_zone_cd , gen(dup2)
assert dup1==dup2
unique prsn_uuid sp_uuid sa_uuid if dup1>0 // 34 unique dups
local uniq_dups = r(unique)
sort prsn_uuid sp_uuid sa_uuid sa_sp_start sa_sp_stop

	// transpose dates for duplicates
gen temp_first = dup1>0 & !(prsn_uuid==prsn_uuid[_n-1] & sp_uuid==sp_uuid[_n-1] & ///
	sa_uuid==sa_uuid[_n-1])
gen start1 = sa_sp_start if temp_first	
gen stop1 = sa_sp_stop if temp_first	
gen start2 = sa_sp_start[_n+1] if temp_first & ///
	(prsn_uuid==prsn_uuid[_n+1] & sp_uuid==sp_uuid[_n+1] & sa_uuid==sa_uuid[_n+1])
gen stop2 = sa_sp_stop[_n+1] if temp_first & ///
	(prsn_uuid==prsn_uuid[_n+1] & sp_uuid==sp_uuid[_n+1] & sa_uuid==sa_uuid[_n+1])
gen start3 = sa_sp_start[_n+2] if temp_first & ///
	(prsn_uuid==prsn_uuid[_n+2] & sp_uuid==sp_uuid[_n+2] & sa_uuid==sa_uuid[_n+2])
gen stop3 = sa_sp_stop[_n+2] if temp_first & ///
	(prsn_uuid==prsn_uuid[_n+2] & sp_uuid==sp_uuid[_n+2] & sa_uuid==sa_uuid[_n+2])
gen start4 = sa_sp_start[_n+3] if temp_first & ///
	(prsn_uuid==prsn_uuid[_n+3] & sp_uuid==sp_uuid[_n+3] & sa_uuid==sa_uuid[_n+3])
gen stop4 = sa_sp_stop[_n+3] if temp_first & ///
	(prsn_uuid==prsn_uuid[_n+3] & sp_uuid==sp_uuid[_n+3] & sa_uuid==sa_uuid[_n+3])
format %td start* stop*
order start1 stop1 start2 stop2 start3 stop3 start4 stop4, after(sa_sp_stop)	
assert start2>=stop1 & start3>=stop2 & start4>=stop3 if temp_first

	// keep one observation per duplicate
keep if temp_first | dup1==0
unique prsn_uuid sp_uuid sa_uuid if dup1>0 // 109 unique dups
assert `uniq_dups'==r(unique)

	// assign first and last date as start/stop
gen start_first = start1 if dup1>0 & temp_first
gen stop_last = .
replace stop_last = stop2 if dup1==1 & temp_first
replace stop_last = stop3 if dup1==2 & temp_first
replace stop_last = stop4 if dup1==3 & temp_first
format %td  start_first stop_last
assert sa_sp_start==start_first if temp_first
drop start_first
assert sa_sp_stop<=stop_last if temp_first
replace sa_sp_stop = stop_last if temp_first
drop stop_last

	// construct lapse dates
drop start1 stop4
rename stop1 sa_sp_lapse_start1 
rename stop2 sa_sp_lapse_start2 
rename stop3 sa_sp_lapse_start3 
rename start2 sa_sp_lapse_stop1 
rename start3 sa_sp_lapse_stop2 
rename start4 sa_sp_lapse_stop3

	// eliminate open-ended lapses
replace sa_sp_lapse_start3 = . if sa_sp_lapse_stop3==. & temp_first
replace sa_sp_lapse_start2 = . if sa_sp_lapse_stop2==. & temp_first
replace sa_sp_lapse_start1 = . if sa_sp_lapse_stop1==. & temp_first

	// eliminate lapses <= 15 days (most of which are <= 2 days)
gen len1 = sa_sp_lapse_stop1-sa_sp_lapse_start1 if temp_first
gen temp1 = len1<=15 & temp_first
replace sa_sp_lapse_start1 = sa_sp_lapse_start2 if temp1
replace sa_sp_lapse_stop1 = sa_sp_lapse_stop2 if temp1
replace sa_sp_lapse_start2 = sa_sp_lapse_start3 if temp1
replace sa_sp_lapse_stop2 = sa_sp_lapse_stop3 if temp1
replace sa_sp_lapse_start3 = . if temp1
replace sa_sp_lapse_stop3 = . if temp1
assert sa_sp_lapse_start3==. & sa_sp_lapse_stop3==.
drop sa_sp_lapse_start3 sa_sp_lapse_stop3

gen len2 = sa_sp_lapse_stop1-sa_sp_lapse_start1 if temp_first
gen temp2 = len2<=15 & temp_first
replace sa_sp_lapse_start1 = sa_sp_lapse_start2 if temp2
replace sa_sp_lapse_stop1 = sa_sp_lapse_stop2 if temp2
replace sa_sp_lapse_start2 = . if temp2
replace sa_sp_lapse_stop2 = . if temp2

gen len3 = sa_sp_lapse_stop1-sa_sp_lapse_start1 if temp_first
assert len3>15

gen len4 = sa_sp_lapse_stop2-sa_sp_lapse_start2 if temp_first
assert len4>15

	// clean up temp variables and label
drop dup1 dup2 temp* len?	
la var sa_sp_lapse_start1 "Start date of lapse 1 (when SA/SP was not listed as active"
la var sa_sp_lapse_stop1 "Stop date of lapse 1 (when SA/SP was not listed as active"
la var sa_sp_lapse_start2 "Start date of lapse 2 (when SA/SP was not listed as active"
la var sa_sp_lapse_stop2 "Stop date of lapse 2 (when SA/SP was not listed as active"
order dr_ind, before(dr_program)
order prsn_naics, before(naics_descr)

	// droop lapse 2 variables, since they're always missing
assert sa_sp_lapse_start2==. & sa_sp_lapse_stop2==.
drop sa_sp_lapse_start2 sa_sp_lapse_stop2

** Cross-check lat/lon vs. climate zone, using CA Climate Zones shapefile
	// export coordinates and climate zones
preserve
keep sp_uuid prem_lat prem_lon climate_zone_cd
duplicates drop
drop if prem_lat==. | prem_lon==. // GIS can't get nowhere with missing lat/lon
unique sp_uuid
assert r(unique)==r(N)
replace climate_zone_cd = "Z07" if climate_zone_cd=="" // an obviously wrong Climate Zone, so the R script won't break
outsheet using "$dirpath_data/misc/pge_prem_coord_raw_20180719.txt", comma replace
restore
	
	// run auxilary GIS script "BUILD_gis_climate_zone_20180719.R"
*shell "${R_exe_path}" --vanilla <"${dirpath_code}/BUILD_gis_climate_zone_20180719.R"
	
	// import results from GIS script
preserve
insheet using "$dirpath_data/misc/pge_prem_coord_polygon_20180719.csv", double comma clear
drop prem_lat prem_lon longitude latitude czone 
replace czone_gis = "" if czone_gis=="NA"
replace czone_gis = "Z0" + czone_gis if length(czone_gis)==1
replace czone_gis = "Z" + czone_gis if length(czone_gis)==2
rename czone_gis climate_zone_cd_gis
rename pou pou_name
replace pou_name = "" if pou_name=="NA"
rename bad_geocode bad_geocode_flag
gen bad_cz_flag = climate_zone_cd!=climate_zone_cd_gis  // GIS assigns different climate zone
tab climate* if bad_cz_flag==1, missing
drop climate_zone_cd
tostring sp_uuid, replace
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
assert length(sp_uuid)==10
unique sp_uuid
assert r(unique)==r(N)
tempfile gis_out
save `gis_out', replace
restore

	// merge back into main dataset
merge m:1 sp_uuid using `gis_out'	
assert _merge!=2 // confirm everything merges in
assert _merge==1 if prem_lat==. | prem_lon==. // confirm nothing merges if it has missing lat/lon
assert (prem_lat==. | prem_lon==.) if _merge==1	// confirm that all non-merges have missing lat/lon
gen missing_geocode_flag = _merge==1
drop _merge
*twoway (scatter prem_lat prem_lon if bad_cz_flag==0, msize(tiny) color(black)) ///
*	   (scatter prem_lat prem_lon if bad_cz_flag==1, msize(tiny) color(red))
	
	// label new variables
la var in_calif	"Dummy=1 if lat/lon are within California"
la var in_pge "Dummy=1 if lat/lon are within PGE service territory proper"
la var in_pou "Dummy=1 if lat/lon are withiin PGE-enveloped POU territory"
la var pou_name "Name of PGE-enveloped POU (or other notes)"
la var bad_geocode "Lat/lon not in PGE territory, and not in PGE-enveloped POU"
la var climate_zone_cd_gis "Climate zone, as assigned by GIS shapefile using lat/lon"
la var bad_cz_flag "Flag for PGE-assigned climate zone that's contradicted by GIS"
la var missing_geocode_flag "PGE geocodes are missing"	


** Confirm uniqueness and save
unique prsn_uuid sp_uuid sa_uuid
assert r(unique)==r(N)
compress
save "$dirpath_data/pge_cleaned/pge_cust_detail_20180719.dta", replace	

}

*******************************************************************************
*******************************************************************************

