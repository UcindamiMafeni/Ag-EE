clear all
version 13
set more off

*******************************************************************************
**** Script to import and clean raw PGE data -- customer details file *********
*******************************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

** Loadaw PGE customer data
use "$dirpath_data/pge_raw/customer_data.dta", clear

** NAICS code
assert prsn_naics_cd!=""
assert length(prsn_naics_cd)==6
assert substr(prsn_naics_cd,1,3)=="111"
rename prsn_naics_cd prsn_naics
la var prsn_naics "6-digit NAICS at customer level"
tab prsn_naics

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

** Confirm uniqueness and save
unique prsn_uuid sp_uuid sa_uuid
assert r(unique)==r(N)
compress
save "$dirpath_data/pge_cleaned/pge_cust_detail.dta", replace	

/*
twoway ///
(scatter prem_lat prem_lon if cl=="Z01", msize(vsmall) mcolor(blue)) ///
(scatter prem_lat prem_lon if cl=="Z02", msize(vsmall) mcolor(black)) ///
(scatter prem_lat prem_lon if cl=="Z03", msize(vsmall) mcolor(orange)) ///
(scatter prem_lat prem_lon if cl=="Z04", msize(vsmall) mcolor(green)) ///
(scatter prem_lat prem_lon if cl=="Z05", msize(vsmall) mcolor(maroon)) ///
(scatter prem_lat prem_lon if cl=="Z11", msize(vsmall) mcolor(magenta)) ///
(scatter prem_lat prem_lon if cl=="Z12", msize(vsmall) mcolor(red)) ///
(scatter prem_lat prem_lon if cl=="Z13", msize(vsmall) mcolor(yellow)) ///
(scatter prem_lat prem_lon if cl=="Z16", msize(vsmall) mcolor(cyan)), ///
legend(off)
*/


// PENDING TASKS
// Deal with missing badge number and badge numbers shorter than 10 digits
// Deal with bad lat/lon 
// Deal with missing lat/lon
// Assign missing climate zones
// Crosscheck lat/lon against climate zone
// Climate zone crosswalk
// NAICS crosswalk
