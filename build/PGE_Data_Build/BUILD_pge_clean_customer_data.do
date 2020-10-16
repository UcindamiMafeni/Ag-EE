clear all
version 13
set more off

*******************************************************************************
**** Script to import and clean raw PGE data -- customer details file *********
*******************************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"
global dirpath_code "T:/Home/Louis/backup/AgEE/AgEE_code/build"
*global R_exe_path "C:/PROGRA~1/MIE74D~1/ROPEN~1/bin/x64/R"
*global R_lib_path "C:/Program Files/Microsoft/R Open/R-3.4.4/library"

*******************************************************************************
*******************************************************************************

** 1. March 2018 data pull
if 1==0{

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
duplicates drop
gen count = 1
collapse (sum) count, by(sp_uuid) fast
tab count // 25% of sp_uuid's have multiple customers IDs
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
	
	// run auxilary GIS script "BUILD_pge_gis_climate_zone_20180322.R"
*shell "${R_exe_path}" --vanilla <"${dirpath_code}/PGE_Data_Build/BUILD_pge_gis_climate_zone.R"
	
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
if 1==0{

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
duplicates drop
gen count = 1
collapse (sum) count, by(sp_uuid) fast
tab count // 43% of sp_uuid's have multiple customers IDs
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
	
	// run auxilary GIS script "BUILD_pge_gis_climate_zone_20180719.R"
*shell "${R_exe_path}" --vanilla <"${dirpath_code}/PGE_Data_Build/BUILD_pge_gis_climate_zone_20180719.R"
	
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

** 3. Compare March 2018 vs July 2018
if 1==0{

** Append the two datasets
use "$dirpath_data/pge_cleaned/pge_cust_detail_20180322.dta", clear
gen temp_old = 1
append using "$dirpath_data/pge_cleaned/pge_cust_detail_20180719.dta"
replace temp_old = 0 if temp_old==.
unique sp_uuid sa_uuid temp_old
assert r(unique)==r(N)

** Check duplicates
duplicates r
duplicates r prsn_uuid sp_uuid sa_uuid
duplicates r sp_uuid sa_uuid
duplicates t prsn_uuid-missing_geocode_flag, gen(dup_all)
duplicates t sp_uuid sa_uuid, gen(dup_spsa)
tab dup_all dup_spsa
sort sp_uuid sa_uuid temp_old
br if dup_spsa!=dup_all // many discrepancies are badge number only (a variable that's not coded properly!)

order pge_badge_nbr, last
duplicates t prsn_uuid-missing_geocode_flag, gen(dup_all2)
tab dup_all2 dup_spsa
br if dup_spsa!=dup_all2 // many discrepancies in naics (another variable that's coded badly!)

order prsn_naics naics_descr, last
duplicates t prsn_uuid-missing_geocode_flag, gen(dup_all3)
tab dup_all3 dup_spsa
br if dup_spsa!=dup_all3 // many discrepancies in end dates after Feb 2018

order sa_sp_stop, last
duplicates t prsn_uuid-missing_geocode_flag, gen(dup_all4)
tab dup_all4 dup_spsa
br if dup_spsa!=dup_all4 // many discrepancies in NEM and DR

order net_mtr_ind dr_ind dr_program, last
duplicates t prsn_uuid-missing_geocode_flag, gen(dup_all5)
tab dup_all5 dup_spsa
br if dup_spsa!=dup_all5 // a few discrepancies in prsn_uuid

order prsn_uuid, last
duplicates t sp_uuid-missing_geocode_flag, gen(dup_all6)
tab dup_all6 dup_spsa
br if dup_spsa!=dup_all6 // 8 SPs with lat/lon discrepancies (17 SP/SA observations)

}

*******************************************************************************
*******************************************************************************

** 4. August 2018 data pull
if 1==1{

** Load raw PGE customer data
use "$dirpath_data/pge_raw/customer_data_20180827.dta", clear

** NAICS code
count if prsn_naics_cd=="" // 0 missings out of 146,085
replace prsn_naics_cd = "" if prsn_naics_cd=="0"
assert length(prsn_naics_cd)==6 if prsn_naics_cd!=""
count if substr(prsn_naics_cd,1,3)!="111" // 146,085 out of 146,085!
rename prsn_naics_cd prsn_naics
la var prsn_naics "6-digit NAICS at customer level"
tab prsn_naics
rename prsn_naics naics
merge m:1 naics using "$dirpath_data/misc/naics_descr.dta", keep(1 3)
tab naics if _merge==3
tab naics if _merge==1 // 95% are "110000" which is like misc ag
drop _merge
rename naics prsn_naics

** Customer ID
assert prsn_uuid!=""
assert length(prsn_uuid)==10
duplicates r prsn_uuid
unique prsn_uuid // 40900 unique customers
la var prsn_uuid "Customer ID (anonymized, 10-digit)"

** Service point ID
assert sp_uuid!=""
assert length(sp_uuid)==10
duplicates r sp_uuid
unique sp_uuid // 64038 unique service points
unique prsn_uuid sp_uuid // 87782 unique customer-service points
preserve
keep prsn_uuid sp_uuid
duplicates drop
gen count = 1
collapse (sum) count, by(sp_uuid) fast
tab count // 27% of sp_uuid's have multiple customers IDs
restore
la var sp_uuid "Service Point ID (anonymized, 10-digit)"

** Service agreement ID
assert sa_uuid!=""
assert length(sa_uuid)==10
duplicates r sa_uuid // almost unique
unique sa_uuid // 145649 unique service agreements
unique prsn_uuid sp_uuid sa_uuid // 145986 unique customer-service point-service aggrements
duplicates r prsn_uuid sp_uuid sa_uuid
duplicates t prsn_uuid sp_uuid sa_uuid, gen(dup)
*br if dup>0 // don't appear to be real dupes, will make unique 
drop dup
la var sa_uuid "Service Agreement ID (anonymized, 10-digit)"

** Service agreement start/stop dates
assert sa_sp_start_dttm!=""
count if !(word(sa_sp_start_dttm,2)=="00:00:00")
di r(N)/_N // 2% start at a particular time, but I don't think we care
count if !(word(sa_sp_stop_dttm,2)=="00:00:00" | sa_sp_stop_dttm=="") 
di r(N)/_N // 3% stop at a particular time, but I don't think we care
gen sa_sp_start = date(word(sa_sp_start_dttm,1),"DMY")
gen sa_sp_stop = date(word(sa_sp_stop_dttm,1),"DMY")
format %td sa_sp_start sa_sp_stop
assert sa_sp_start<=sa_sp_stop // stop not prior to start
count if sa_sp_start==sa_sp_stop 
di r(N)/_N // 3% of obs have stop = start ???
drop sa_sp_start_dttm sa_sp_stop_dttm
la var sa_sp_start "Start date (SA/SP)"
la var sa_sp_stop "Stop date (SA/SP)"

** PGE meter badge number
count if pge_badge_nbr=="" // 431 meter numbers missing
count if length(pge_badge_nbr)==10 
di r(N)/_N // 93% have 10 digits ( ~= smart meter)
unique pge_badge_nbr // 63497 unique badge numbers
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
count if prem_lat==. | prem_lon==. 
di r(N)/_N // 1% missing, deal with below
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
count if climate_zone_cd=="" // 24 missings
la var climate_zone_cd "CA climate zone code"

** Resolve duplicates where one observation stops and the next immediately starts
duplicates t prsn_uuid sp_uuid sa_uuid, gen(dup1)
duplicates t prsn_uuid sp_uuid sa_uuid pge_badge_nbr prem_lat prem_lon net_mtr_ind ///
	dr_program dr_in climate_zone_cd , gen(dup2)
assert dup1==dup2
unique prsn_uuid sp_uuid sa_uuid if dup1>0 // 85 unique dups
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
unique prsn_uuid sp_uuid sa_uuid if dup1>0 // 85 unique dups
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
gen temp3 = len3<=15 & temp_first
replace sa_sp_lapse_start1 = . if temp3
replace sa_sp_lapse_stop1 = . if temp3

gen len4 = sa_sp_lapse_stop1-sa_sp_lapse_start1 if temp_first
assert len4>15

gen len5 = sa_sp_lapse_stop2-sa_sp_lapse_start2 if temp_first
gen temp5 = len5<=15 & temp_first
replace sa_sp_lapse_start2 = . if temp5
replace sa_sp_lapse_stop2 = . if temp5

gen len6 = sa_sp_lapse_stop2-sa_sp_lapse_start2 if temp_first
assert len6>15

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
outsheet using "$dirpath_data/misc/pge_prem_coord_raw_20180827.txt", comma replace
restore
	
	// run auxilary GIS script "BUILD_pge_gis_climate_zone_20180827.R"
*shell "${R_exe_path}" --vanilla <"${dirpath_code}/PGE_Data_Build/BUILD_pge_gis_climate_zone_20180827.R"
	
	// import results from GIS script
preserve
insheet using "$dirpath_data/misc/pge_prem_coord_polygon_20180827.csv", double comma clear
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
save "$dirpath_data/pge_cleaned/pge_cust_detail_20180827.dta", replace	

}

*******************************************************************************
*******************************************************************************

** 5. Compare August 2018 vs March/July 2018
if 1==1{

** Confirm no overlap between March and August
use "$dirpath_data/pge_cleaned/pge_cust_detail_20180322.dta", clear
gen temp_old = 1
append using "$dirpath_data/pge_cleaned/pge_cust_detail_20180827.dta"
replace temp_old = 0 if temp_old==.
unique sp_uuid sa_uuid temp_old
assert r(unique)==r(N)

duplicates t sp_uuid sa_uuid, gen(dup)
tab dup // 36 dups
sort sp_uuid sa_uuid temp_old
br if dup>0

duplicates t sp_uuid sa_uuid prem_lat prem_long, gen(dup2)
assert dup2==dup
br if dup2!=dup // 1 minor lat/lon discrepancy, nothing else to see here


** Confirm no overlap between March and August
use "$dirpath_data/pge_cleaned/pge_cust_detail_20180719.dta", clear
gen temp_old = 1
append using "$dirpath_data/pge_cleaned/pge_cust_detail_20180827.dta"
replace temp_old = 0 if temp_old==.
unique sp_uuid sa_uuid temp_old
assert r(unique)==r(N)

duplicates t sp_uuid sa_uuid, gen(dup)
tab dup 

unique sp_uuid sa_uuid
assert r(unique)==r(N)
// NO OVERLAP in SP/SA

egen temp_min = min(temp_old), by(sp_uuid)
egen temp_max = max(temp_old), by(sp_uuid)
tab temp_min temp_max
assert temp_min==temp_max
// NO OVERLAP IN SPs

egen temp_min2 = min(temp_old), by(sa_uuid)
egen temp_max2 = max(temp_old), by(sa_uuid)
tab temp_min2 temp_max2 
// very minor overlap for SA


}

*******************************************************************************
*******************************************************************************

** 6. September 2020 data pull
if 1==1{

** Load raw PGE customer data
use "$dirpath_data/pge_raw/customer_data_202009.dta", clear

** NAICS code (customer level)
count if prsn_naics=="" // 1,123 missings out of 125,682
replace prsn_naics = "" if prsn_naics=="0"
assert length(prsn_naics)==6 if prsn_naics!=""
count if substr(prsn_naics,1,3)!="111" // 67,627 out of 125,682
la var prsn_naics "6-digit NAICS at customer level"
tab prsn_naics
rename prsn_naics naics
merge m:1 naics using "$dirpath_data/misc/naics_descr.dta", keep(1 3)
tab naics if _merge==3
tab naics if _merge==1 // 97% are "110000" which is like misc ag
drop _merge
rename naics prsn_naics

** NAICS code (account-level)
count if sa_naics=="" // 4,001 missings out of 125,682
replace sa_naics = "" if sa_naics=="0"
assert length(sa_naics)==6 if sa_naics!=""
count if sa_naics!="" & prsn_naics!="" & sa_naics!=prsn_naics // 55,215 SAs with different NAICS
count if substr(sa_naics,1,3)!="111" // 75,914 out of 125,682
la var sa_naics "6-digit NAICS at service account level"
tab sa_naics
rename sa_naics naics
rename naics_descr Naics_descr
merge m:1 naics using "$dirpath_data/misc/naics_descr.dta", keep(1 3)
tab naics if _merge==3
tab naics if _merge==1 // 99% are "110000" which is like misc ag
drop _merge
rename naics sa_naics
rename naics_descr naics_descr_sa
la var naics_descr_sa "NAICS code description (SA-level)"
rename Naics_descr naics_descr

** Customer ID
rename msk_cust_id prsn_uuid
assert prsn_uuid!=""
assert length(prsn_uuid)==10
duplicates r prsn_uuid
unique prsn_uuid // 37404 unique customers
la var prsn_uuid "Customer ID (anonymized, 10-digit)"

** Service point ID
rename msk_sp_id sp_uuid
assert sp_uuid!=""
assert length(sp_uuid)==10
duplicates r sp_uuid
unique sp_uuid // 86994 unique service points
unique prsn_uuid sp_uuid // 101059 unique customer-service points
preserve
keep prsn_uuid sp_uuid
duplicates drop
gen count = 1
collapse (sum) count, by(sp_uuid) fast
tab count // 15% of sp_uuid's have multiple customers IDs
restore
la var sp_uuid "Service Point ID (anonymized, 10-digit)"

** Service agreement ID
rename msk_sa_id sa_uuid
assert sa_uuid!=""
assert length(sa_uuid)==10
duplicates r sa_uuid // almost unique
unique sa_uuid // 125458 unique service agreements
unique prsn_uuid sp_uuid sa_uuid // 125682 unique customer-service point-service aggrements
assert r(unique)==r(N) // is unique
la var sa_uuid "Service Agreement ID (anonymized, 10-digit)"

** Service agreement start/stop dates
// MISSING
// assert sa_sp_start_dttm!=""
// count if !(word(sa_sp_start_dttm,2)=="00:00:00")
// di r(N)/_N // 2% start at a particular time, but I don't think we care
// count if !(word(sa_sp_stop_dttm,2)=="00:00:00" | sa_sp_stop_dttm=="") 
// di r(N)/_N // 3% stop at a particular time, but I don't think we care
// gen sa_sp_start = date(word(sa_sp_start_dttm,1),"DMY")
// gen sa_sp_stop = date(word(sa_sp_stop_dttm,1),"DMY")
// format %td sa_sp_start sa_sp_stop
// assert sa_sp_start<=sa_sp_stop // stop not prior to start
// count if sa_sp_start==sa_sp_stop 
// di r(N)/_N // 3% of obs have stop = start ???
// drop sa_sp_start_dttm sa_sp_stop_dttm
// la var sa_sp_start "Start date (SA/SP)"
// la var sa_sp_stop "Stop date (SA/SP)"

** PGE meter badge number
count if pge_badge_nbr=="" // 597 meter numbers missing
count if length(pge_badge_nbr)==10 
di r(N)/_N // 98% have 10 digits ( ~= smart meter)
unique pge_badge_nbr // 86377 unique badge numbers
la var pge_badge_nbr "PG&E meter badge number"

** Extra meter ID (unique to this data pull)
count if mtr_id==""
assert pge_badge_nbr=="" if mtr_id==""
assert pge_badge_nbr!="" if mtr_id!=""
egen temp1 = group(pge_badge_nbr)
egen temp2 = group(pge_badge_nbr mtr_id)
assert temp1==temp2 // confirms this is entirely redundant information
drop temp*
assert length(mtr_id)==10 | mtr_id==""
rename mtr_id mtr_uuid
la var mtr_uuid "Anonymized meter idenfitier (fully redundant)"
	
** Geocoordinates
charlist prem_lat
assert r(chars)==".0123456789" // confirm all numeric (and positive)
charlist prem_lon
assert r(chars)=="-.0123456789" // confirm all numeric
destring prem_lat prem_lon, replace
assert prem_lat>0 // latitude always positive
assert prem_lon<0 | prem_lon==. // longitude always negative (when not missing)
twoway scatter prem_lat prem_lon, msize(vsmall) // looks mostly good, a few issues tho
count if prem_lat==. | prem_lon==. 
di r(N)/_N // 1.7% missing, deal with below
la var prem_lat "Latitude of premises"
la var prem_lon "Longitude of premises"
	// confirm lat/lon is unique by service point
unique sp_uuid
local uniq = r(unique)
unique sp_uuid prem_lat prem_lon
assert `uniq'==r(unique)	

** Net energy metering indicator
// MISSING
// tab net_mtr_ind, missing
// assert net_mtr_ind!=""
// replace net_mtr_ind = "0" if net_mtr_ind=="N"
// replace net_mtr_ind = "1" if net_mtr_ind=="Y"
// destring net_mtr_ind, replace
// assert net_mtr_ind!=.
// la var net_mtr_ind "Dummy for NEM participation"

** Demand response
tab dr_program, missing
assert dr_program!=""
// la var dr_program "Demand response program"
gen dr_ind = dr_program=="Y"
la var dr_ind "Dummy for demand response participation"
drop dr_program

** Climate zone
tab climate_zone_cd, missing 
count if climate_zone_cd=="" // 1984 missings
la var climate_zone_cd "CA climate zone code"

// ** Resolve duplicates where one observation stops and the next immediately starts
// duplicates t prsn_uuid sp_uuid sa_uuid, gen(dup1)
// duplicates t prsn_uuid sp_uuid sa_uuid pge_badge_nbr prem_lat prem_lon net_mtr_ind ///
// 	dr_program dr_in climate_zone_cd , gen(dup2)
// assert dup1==dup2
// unique prsn_uuid sp_uuid sa_uuid if dup1>0 // 85 unique dups
// local uniq_dups = r(unique)
// sort prsn_uuid sp_uuid sa_uuid sa_sp_start sa_sp_stop
//
// 	// transpose dates for duplicates
// gen temp_first = dup1>0 & !(prsn_uuid==prsn_uuid[_n-1] & sp_uuid==sp_uuid[_n-1] & ///
// 	sa_uuid==sa_uuid[_n-1])
// gen start1 = sa_sp_start if temp_first	
// gen stop1 = sa_sp_stop if temp_first	
// gen start2 = sa_sp_start[_n+1] if temp_first & ///
// 	(prsn_uuid==prsn_uuid[_n+1] & sp_uuid==sp_uuid[_n+1] & sa_uuid==sa_uuid[_n+1])
// gen stop2 = sa_sp_stop[_n+1] if temp_first & ///
// 	(prsn_uuid==prsn_uuid[_n+1] & sp_uuid==sp_uuid[_n+1] & sa_uuid==sa_uuid[_n+1])
// gen start3 = sa_sp_start[_n+2] if temp_first & ///
// 	(prsn_uuid==prsn_uuid[_n+2] & sp_uuid==sp_uuid[_n+2] & sa_uuid==sa_uuid[_n+2])
// gen stop3 = sa_sp_stop[_n+2] if temp_first & ///
// 	(prsn_uuid==prsn_uuid[_n+2] & sp_uuid==sp_uuid[_n+2] & sa_uuid==sa_uuid[_n+2])
// gen start4 = sa_sp_start[_n+3] if temp_first & ///
// 	(prsn_uuid==prsn_uuid[_n+3] & sp_uuid==sp_uuid[_n+3] & sa_uuid==sa_uuid[_n+3])
// gen stop4 = sa_sp_stop[_n+3] if temp_first & ///
// 	(prsn_uuid==prsn_uuid[_n+3] & sp_uuid==sp_uuid[_n+3] & sa_uuid==sa_uuid[_n+3])
// format %td start* stop*
// order start1 stop1 start2 stop2 start3 stop3 start4 stop4, after(sa_sp_stop)	
// assert start2>=stop1 & start3>=stop2 & start4>=stop3 if temp_first
//
// 	// keep one observation per duplicate
// keep if temp_first | dup1==0
// unique prsn_uuid sp_uuid sa_uuid if dup1>0 // 85 unique dups
// assert `uniq_dups'==r(unique)
//
// 	// assign first and last date as start/stop
// gen start_first = start1 if dup1>0 & temp_first
// gen stop_last = .
// replace stop_last = stop2 if dup1==1 & temp_first
// replace stop_last = stop3 if dup1==2 & temp_first
// replace stop_last = stop4 if dup1==3 & temp_first
// format %td  start_first stop_last
// assert sa_sp_start==start_first if temp_first
// drop start_first
// assert sa_sp_stop<=stop_last if temp_first
// replace sa_sp_stop = stop_last if temp_first
// drop stop_last
//
// 	// construct lapse dates
// drop start1 stop4
// rename stop1 sa_sp_lapse_start1 
// rename stop2 sa_sp_lapse_start2 
// rename stop3 sa_sp_lapse_start3 
// rename start2 sa_sp_lapse_stop1 
// rename start3 sa_sp_lapse_stop2 
// rename start4 sa_sp_lapse_stop3
//
// 	// eliminate open-ended lapses
// replace sa_sp_lapse_start3 = . if sa_sp_lapse_stop3==. & temp_first
// replace sa_sp_lapse_start2 = . if sa_sp_lapse_stop2==. & temp_first
// replace sa_sp_lapse_start1 = . if sa_sp_lapse_stop1==. & temp_first
//
// 	// eliminate lapses <= 15 days (most of which are <= 2 days)
// gen len1 = sa_sp_lapse_stop1-sa_sp_lapse_start1 if temp_first
// gen temp1 = len1<=15 & temp_first
// replace sa_sp_lapse_start1 = sa_sp_lapse_start2 if temp1
// replace sa_sp_lapse_stop1 = sa_sp_lapse_stop2 if temp1
// replace sa_sp_lapse_start2 = sa_sp_lapse_start3 if temp1
// replace sa_sp_lapse_stop2 = sa_sp_lapse_stop3 if temp1
// replace sa_sp_lapse_start3 = . if temp1
// replace sa_sp_lapse_stop3 = . if temp1
// assert sa_sp_lapse_start3==. & sa_sp_lapse_stop3==.
// drop sa_sp_lapse_start3 sa_sp_lapse_stop3
//
// gen len2 = sa_sp_lapse_stop1-sa_sp_lapse_start1 if temp_first
// gen temp2 = len2<=15 & temp_first
// replace sa_sp_lapse_start1 = sa_sp_lapse_start2 if temp2
// replace sa_sp_lapse_stop1 = sa_sp_lapse_stop2 if temp2
// replace sa_sp_lapse_start2 = . if temp2
// replace sa_sp_lapse_stop2 = . if temp2
//
// gen len3 = sa_sp_lapse_stop1-sa_sp_lapse_start1 if temp_first
// gen temp3 = len3<=15 & temp_first
// replace sa_sp_lapse_start1 = . if temp3
// replace sa_sp_lapse_stop1 = . if temp3
//
// gen len4 = sa_sp_lapse_stop1-sa_sp_lapse_start1 if temp_first
// assert len4>15
//
// gen len5 = sa_sp_lapse_stop2-sa_sp_lapse_start2 if temp_first
// gen temp5 = len5<=15 & temp_first
// replace sa_sp_lapse_start2 = . if temp5
// replace sa_sp_lapse_stop2 = . if temp5
//
// gen len6 = sa_sp_lapse_stop2-sa_sp_lapse_start2 if temp_first
// assert len6>15
//
// 	// clean up temp variables and label
// drop dup1 dup2 temp* len?	
// la var sa_sp_lapse_start1 "Start date of lapse 1 (when SA/SP was not listed as active"
// la var sa_sp_lapse_stop1 "Stop date of lapse 1 (when SA/SP was not listed as active"
// la var sa_sp_lapse_start2 "Start date of lapse 2 (when SA/SP was not listed as active"
// la var sa_sp_lapse_stop2 "Stop date of lapse 2 (when SA/SP was not listed as active"
// order dr_ind, before(dr_program)
// order prsn_naics, before(naics_descr)
//
// 	// droop lapse 2 variables, since they're always missing
// assert sa_sp_lapse_start2==. & sa_sp_lapse_stop2==.
// drop sa_sp_lapse_start2 sa_sp_lapse_stop2

** Cross-check lat/lon vs. climate zone, using CA Climate Zones shapefile
	// export coordinates and climate zones
preserve
keep sp_uuid prem_lat prem_lon climate_zone_cd
duplicates drop
drop if prem_lat==. | prem_lon==. // GIS can't get nowhere with missing lat/lon
unique sp_uuid
assert r(unique)==r(N)
replace climate_zone_cd = "Z07" if climate_zone_cd=="" // an obviously wrong Climate Zone, so the R script won't break
outsheet using "$dirpath_data/misc/pge_prem_coord_raw_202009.txt", comma replace
restore
	
	// run auxilary GIS script "BUILD_pge_gis_climate_zone_20180827.R"
*shell "${R_exe_path}" --vanilla <"${dirpath_code}/PGE_Data_Build/BUILD_pge_gis_climate_zone_20180827.R"
	
	// import results from GIS script
preserve
insheet using "$dirpath_data/misc/pge_prem_coord_polygon_202009.csv", double comma clear
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
save "$dirpath_data/pge_cleaned/pge_cust_detail_202009.dta", replace	

}

*******************************************************************************
*******************************************************************************

** 7. Compare September 2020 to 3 2018 pulls
if 1==1{

use "$dirpath_data/pge_cleaned/pge_cust_detail_20180322.dta", clear
gen pull = "20180322"
append using "$dirpath_data/pge_cleaned/pge_cust_detail_20180827.dta"
replace pull = "20180827" if pull==""
append using "$dirpath_data/pge_cleaned/pge_cust_detail_20180719.dta"
replace pull = "20180719" if pull==""

keep sp_uuid sa_uuid prem_lat prem_long pull
duplicates drop sp_uuid sa_uuid, force

rename prem_lat prem_latM
rename prem_long prem_longM

merge 1:m sp_uuid sa_uuid using "$dirpath_data/pge_cleaned/pge_cust_detail_202009.dta"
tab _merge if pull=="20180322"
tab _merge if pull=="20180827"
tab _merge if pull=="20180719"
count if _merge==3 & prem_lat!=. & prem_long!=.
local rN = r(N)
count if _merge==3 & prem_lat!=. & prem_long!=. & prem_latM==prem_lat & prem_longM==prem_long
di r(N)/`rN' // 99.7% exact matches on lat/lon
count if _merge==3 & prem_lat!=. & prem_long!=. & abs(prem_latM-prem_lat)<0.01 & abs(prem_longM-prem_long)<0.01
di r(N)/`rN' // 99.7% exact matches on lat/lon





use "$dirpath_data/pge_cleaned/pge_cust_detail_20180322.dta", clear
gen pull = "20180322"
append using "$dirpath_data/pge_cleaned/pge_cust_detail_20180827.dta"
replace pull = "20180827" if pull==""
append using "$dirpath_data/pge_cleaned/pge_cust_detail_20180719.dta"
replace pull = "20180719" if pull==""

keep sp_uuid prem_lat prem_long pull
duplicates drop sp_uuid, force

merge 1:m sp_uuid using "$dirpath_data/pge_cleaned/pge_cust_detail_202009.dta", ///
	keepusing(sp_uuid)
duplicates drop
unique sp_uuid
assert r(unique)==r(N)
tab _merge
tab _merge if pull=="20180322"
tab _merge if pull=="20180827"
tab _merge if pull=="20180719"


count if _merge==3 & prem_lat!=. & prem_long!=.
local rN = r(N)
count if _merge==3 & prem_lat!=. & prem_long!=. & prem_latM==prem_lat & prem_longM==prem_long
di r(N)/`rN' // 99.7% exact matches on lat/lon
count if _merge==3 & prem_lat!=. & prem_long!=. & abs(prem_latM-prem_lat)<0.01 & abs(prem_longM-prem_long)<0.01
di r(N)/`rN' // 99.7% exact matches on lat/lon



use "$dirpath_data/merged_pge/sp_month_water_panel.dta", clear
keep if year==2017
keep sp_uuid 
duplicates drop
merge 1:m sp_uuid using "$dirpath_data/pge_cleaned/pge_cust_detail_20180719.dta", keepusing(sp_uuid)
duplicates drop
unique sp_uuid
tab _merge
rename _merge _merge_water_panel
merge 1:m sp_uuid using "$dirpath_data/pge_cleaned/pge_cust_detail_202009.dta", keepusing(sp_uuid)
duplicates drop
tab _merge*


use "$dirpath_data/merged_pge/sp_month_elec_panel.dta", clear
keep if year==2017
tab pull
tab modate
keep if modate==ym(2017,8)
tab rt_sched_cd, missing
unique sp_uuid
merge 1:m sp_uuid using "$dirpath_data/pge_cleaned/pge_cust_detail_202009.dta", keepusing(sp_uuid)
duplicates drop
tab _merge pull, missing

tab rt_sched_cd _merge

di 1299/11559










}

*******************************************************************************
*******************************************************************************


