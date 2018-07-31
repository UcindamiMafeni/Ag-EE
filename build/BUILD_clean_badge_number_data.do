clear all
version 13
set more off

*******************************************************************************
**** Script to import and clean raw PGE data -- meter badge history file ******
*******************************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

*******************************************************************************
*******************************************************************************

** 1. March 2018 data pull
{

** Load raw PGE customer data
use "$dirpath_data/pge_raw/meter_badge_number_data_20180322.dta", clear
duplicates drop

** Service point ID
assert sp_uuid!=""
assert length(sp_uuid)==10
duplicates r sp_uuid
unique sp_uuid // 71912 unique service points
unique sp_uuid pge_badge_nbr // 173669 unique service point-meters (ALMOST unique)
la var sp_uuid "Service Point ID (anonymized, 10-digit)"

** PGE meter badge number
assert pge_badge_nbr!="" 
count if length(pge_badge_nbr)==10 // 56% have 10 digits
gen len = length(pge_badge_nbr)
tab len
drop len
unique pge_badge_nbr // 172249 unique badge numbers (100,000 more than in customer data!)
la var pge_badge_nbr "PG&E meter badge number"

** Meter install/removal dates and times
assert mtr_install_dttm!=.
gen mtr_install_date = dofc(mtr_install_dttm)
format %td mtr_install_date
gen mtr_install_hour = hh(mtr_install_dttm)
assert mtr_remove_dttm!=.
gen mtr_remove_date = dofc(mtr_remove_dttm)
format %td mtr_remove_date
gen mtr_remove_hour = hh(mtr_remove_dttm)
drop mtr_install_dttm mtr_remove_dttm
replace mtr_remove_date = . if year(mtr_remove_date)==9099
replace mtr_remove_hour = . if mtr_remove_date==.
la var mtr_install_date "Meter install date"
la var mtr_install_hour "Meter install hour"
la var mtr_remove_date "Meter remove date (if missing, never removed)"
la var mtr_remove_hour "Meter remove hour (if missing, never removed)"

** Resolve duplicates (when the same meter starts in the same hour it just stopped)
duplicates t sp_uuid pge_badge_nbr, gen(dup)
sort sp_uuid pge_badge_nbr mtr_install_date mtr_install_hour mtr_remove_date mtr_remove_hour
//br if dup>0
gen to_collapse = .
replace to_collapse = 1 if mtr_remove_date==mtr_install_date[_n+1] & ///
	mtr_remove_hour==mtr_install_hour[_n+1] & sp_uuid==sp_uuid[_n+1] & ///
	pge_badge_nbr==pge_badge_nbr[_n+1] & dup>0
gen to_drop = .
replace to_drop = 1 if sp_uuid==sp_uuid[_n-1] & pge_badge_nbr==pge_badge_nbr[_n-1] & ///
	dup>0 & to_collapse==. & to_collapse[_n-1]==1
replace mtr_remove_date = mtr_remove_date[_n+1] if to_collapse==1 & to_drop==. & ///
		to_collapse[_n+1]==. & to_drop[_n+1]==1
replace mtr_remove_hour = mtr_remove_hour[_n+1] if to_collapse==1 & to_drop==. & ///
		to_collapse[_n+1]==. & to_drop[_n+1]==1
unique sp_uuid pge_badge_nbr
local uniq = r(unique)
drop if to_drop==1
unique sp_uuid pge_badge_nbr
assert r(unique)==`uniq'		
drop to_collapse to_drop dup	

** Resolve duplicates (when the same meter starts the hour after it just stopped)
duplicates t sp_uuid pge_badge_nbr, gen(dup)
sort sp_uuid pge_badge_nbr mtr_install_date mtr_install_hour mtr_remove_date mtr_remove_hour
//br if dup>0
gen to_collapse = .
replace to_collapse = 1 if mtr_remove_date==mtr_install_date[_n+1] & ///
	mtr_remove_hour+1==mtr_install_hour[_n+1] & sp_uuid==sp_uuid[_n+1] & ///
	pge_badge_nbr==pge_badge_nbr[_n+1] & dup>0
gen to_drop = .
replace to_drop = 1 if sp_uuid==sp_uuid[_n-1] & pge_badge_nbr==pge_badge_nbr[_n-1] & ///
	dup>0 & to_collapse==. & to_collapse[_n-1]==1
replace mtr_remove_date = mtr_remove_date[_n+1] if to_collapse==1 & to_drop==. & ///
		to_collapse[_n+1]==. & to_drop[_n+1]==1
replace mtr_remove_hour = mtr_remove_hour[_n+1] if to_collapse==1 & to_drop==. & ///
		to_collapse[_n+1]==. & to_drop[_n+1]==1
unique sp_uuid pge_badge_nbr
local uniq = r(unique)
drop if to_drop==1
unique sp_uuid pge_badge_nbr
assert r(unique)==`uniq'		
drop to_collapse to_drop dup	

duplicates t sp_uuid pge_badge_nbr, gen(dup)
sort sp_uuid pge_badge_nbr mtr_install_date mtr_install_hour mtr_remove_date mtr_remove_hour
//br if dup>0
gen to_collapse = .
replace to_collapse = 1 if mtr_remove_date+1==mtr_install_date[_n+1] & ///
	mtr_remove_hour==23 & mtr_install_hour[_n+1]==0 & sp_uuid==sp_uuid[_n+1] & ///
	pge_badge_nbr==pge_badge_nbr[_n+1] & dup>0
gen to_drop = .
replace to_drop = 1 if sp_uuid==sp_uuid[_n-1] & pge_badge_nbr==pge_badge_nbr[_n-1] & ///
	dup>0 & to_collapse==. & to_collapse[_n-1]==1
replace mtr_remove_date = mtr_remove_date[_n+1] if to_collapse==1 & to_drop==. & ///
		to_collapse[_n+1]==. & to_drop[_n+1]==1
replace mtr_remove_hour = mtr_remove_hour[_n+1] if to_collapse==1 & to_drop==. & ///
		to_collapse[_n+1]==. & to_drop[_n+1]==1
unique sp_uuid pge_badge_nbr
local uniq = r(unique)
drop if to_drop==1
unique sp_uuid pge_badge_nbr
assert r(unique)==`uniq'		
drop to_collapse to_drop dup	
	
** Resolve duplicates (when the same meter starts the same day it just stopped)
duplicates t sp_uuid pge_badge_nbr, gen(dup)
sort sp_uuid pge_badge_nbr mtr_install_date mtr_install_hour mtr_remove_date mtr_remove_hour
//br if dup>0
gen to_collapse = .
replace to_collapse = 1 if mtr_remove_date==mtr_install_date[_n+1] & ///
	 sp_uuid==sp_uuid[_n+1] & pge_badge_nbr==pge_badge_nbr[_n+1] & dup>0
gen to_drop = .
replace to_drop = 1 if sp_uuid==sp_uuid[_n-1] & pge_badge_nbr==pge_badge_nbr[_n-1] & ///
	dup>0 & to_collapse==. & to_collapse[_n-1]==1
replace mtr_remove_date = mtr_remove_date[_n+1] if to_collapse==1 & to_drop==. & ///
		to_collapse[_n+1]==. & to_drop[_n+1]==1
replace mtr_remove_hour = mtr_remove_hour[_n+1] if to_collapse==1 & to_drop==. & ///
		to_collapse[_n+1]==. & to_drop[_n+1]==1
unique sp_uuid pge_badge_nbr
local uniq = r(unique)
drop if to_drop==1
unique sp_uuid pge_badge_nbr
assert r(unique)==`uniq'		
drop to_collapse to_drop dup	

** Resolve duplicates (when the same meter starts within 15 days of the day it just stopped)
duplicates t sp_uuid pge_badge_nbr, gen(dup)
sort sp_uuid pge_badge_nbr mtr_install_date mtr_install_hour mtr_remove_date mtr_remove_hour
//br if dup>0
gen to_collapse = .
replace to_collapse = 1 if mtr_install_date[_n+1]-mtr_remove_date<=15 & ///
	 sp_uuid==sp_uuid[_n+1] & pge_badge_nbr==pge_badge_nbr[_n+1] & dup>0
gen to_drop = .
replace to_drop = 1 if sp_uuid==sp_uuid[_n-1] & pge_badge_nbr==pge_badge_nbr[_n-1] & ///
	dup>0 & to_collapse==. & to_collapse[_n-1]==1
replace mtr_remove_date = mtr_remove_date[_n+1] if to_collapse==1 & to_drop==. & ///
		to_collapse[_n+1]==. & to_drop[_n+1]==1
replace mtr_remove_hour = mtr_remove_hour[_n+1] if to_collapse==1 & to_drop==. & ///
		to_collapse[_n+1]==. & to_drop[_n+1]==1
unique sp_uuid pge_badge_nbr
local uniq = r(unique)
drop if to_drop==1
unique sp_uuid pge_badge_nbr
assert r(unique)==`uniq'		
drop to_collapse to_drop dup

** Tranpose dates into lapses, for remaining 36 duplicates
duplicates t sp_uuid pge_badge_nbr, gen(dup)
unique sp_uuid pge_badge_nbr if dup>0 // 18 unique dups
local uniq_dups = r(unique)
sort sp_uuid pge_badge_nbr mtr_install_date mtr_install_hour
gen temp_first = dup>0 & !(sp_uuid==sp_uuid[_n-1] & pge_badge_nbr==pge_badge_nbr[_n-1])
gen start1 = mtr_install_date if temp_first	
gen start1h = mtr_install_hour if temp_first	
gen stop1 = mtr_remove_date if temp_first	
gen stop1h = mtr_remove_hour if temp_first	
gen start2 = mtr_install_date[_n+1] if temp_first & ///
	(sp_uuid==sp_uuid[_n+1] & pge_badge_nbr==pge_badge_nbr[_n+1])
gen start2h = mtr_install_hour[_n+1] if temp_first & ///
	(sp_uuid==sp_uuid[_n+1] & pge_badge_nbr==pge_badge_nbr[_n+1])
gen stop2 = mtr_remove_date[_n+1] if temp_first & ///
	(sp_uuid==sp_uuid[_n+1] & pge_badge_nbr==pge_badge_nbr[_n+1])
gen stop2h = mtr_remove_hour[_n+1] if temp_first & ///
	(sp_uuid==sp_uuid[_n+1] & pge_badge_nbr==pge_badge_nbr[_n+1])
format %td start? stop?
assert start2>=stop1 if temp_first
	
	// keep one observation per duplicate
keep if temp_first | dup==0
unique sp_uuid pge_badge_nbr if dup>0 
assert `uniq_dups'==r(unique) 
assert r(unique)==r(N)

	// assign first and last date as start/stop
replace mtr_remove_date = stop2 if dup>0 & temp_first
replace mtr_remove_hour = stop2 if dup>0 & temp_first

	// rename lapse dates
rename stop1 mtr_lapse_start1
rename start2 mtr_lapse_stop1
assert mtr_lapse_stop1!=. if dup>0 & temp_first // no open-ended lapses
drop dup temp_first start* stop*
la var mtr_lapse_start1 "Start date of lapse 1 (when meter number wasn't listed as active)"
la var mtr_lapse_stop1 "Stop date of lapse 1 (when meter number wasn't listed as active)"

** Confirm uniqueness
unique sp_uuid pge_badge_nbr
assert r(unique)==r(N)

** Save
compress
save "$dirpath_data/pge_cleaned/meter_badge_number_data_20180322.dta", replace	

}

*******************************************************************************
*******************************************************************************

** 2. July 2018 data pull
{

// NOTE: This is much smaller (relative to the size of the pull) than the March 2018 
// meter history file, because of the way it was constructed. We don't see every meter
// at every SP, only those meters that show up in the pump test data. (We don't care
// about meters not in the pump test data; we care about the SP!)
 
** Load raw PGE customer data
use "$dirpath_data/pge_raw/meter_badge_number_data_20180719.dta", clear
duplicates drop

** Service point ID
assert sp_uuid!=""
assert length(sp_uuid)==10
duplicates r sp_uuid
unique sp_uuid // 13314 unique service points
unique sp_uuid pge_badge_nbr // 15631 unique service point-meters (ALMOST unique)
la var sp_uuid "Service Point ID (anonymized, 10-digit)"

** PGE meter badge number
assert pge_badge_nbr!="" 
count if length(pge_badge_nbr)==10 // 76% have 10 digits
gen len = length(pge_badge_nbr)
tab len
drop len
unique pge_badge_nbr // 15091 unique badge numbers 
la var pge_badge_nbr "PG&E meter badge number"

** Meter install/removal dates and times
assert mtr_install_dttm!=.
gen mtr_install_date = dofc(mtr_install_dttm)
format %td mtr_install_date
gen mtr_install_hour = hh(mtr_install_dttm)
assert mtr_remove_dttm!=.
gen mtr_remove_date = dofc(mtr_remove_dttm)
format %td mtr_remove_date
gen mtr_remove_hour = hh(mtr_remove_dttm)
drop mtr_install_dttm mtr_remove_dttm
replace mtr_remove_date = . if year(mtr_remove_date)==9099
replace mtr_remove_hour = . if mtr_remove_date==.
la var mtr_install_date "Meter install date"
la var mtr_install_hour "Meter install hour"
la var mtr_remove_date "Meter remove date (if missing, never removed)"
la var mtr_remove_hour "Meter remove hour (if missing, never removed)"

** Resolve duplicates (when the same meter starts in the same hour it just stopped)
duplicates t sp_uuid pge_badge_nbr, gen(dup)
sort sp_uuid pge_badge_nbr mtr_install_date mtr_install_hour mtr_remove_date mtr_remove_hour
//br if dup>0
gen to_collapse = .
replace to_collapse = 1 if mtr_remove_date==mtr_install_date[_n+1] & ///
	mtr_remove_hour==mtr_install_hour[_n+1] & sp_uuid==sp_uuid[_n+1] & ///
	pge_badge_nbr==pge_badge_nbr[_n+1] & dup>0
gen to_drop = .
replace to_drop = 1 if sp_uuid==sp_uuid[_n-1] & pge_badge_nbr==pge_badge_nbr[_n-1] & ///
	dup>0 & to_collapse==. & to_collapse[_n-1]==1
replace mtr_remove_date = mtr_remove_date[_n+1] if to_collapse==1 & to_drop==. & ///
		to_collapse[_n+1]==. & to_drop[_n+1]==1
replace mtr_remove_hour = mtr_remove_hour[_n+1] if to_collapse==1 & to_drop==. & ///
		to_collapse[_n+1]==. & to_drop[_n+1]==1
unique sp_uuid pge_badge_nbr
local uniq = r(unique)
drop if to_drop==1
unique sp_uuid pge_badge_nbr
assert r(unique)==`uniq'		
drop to_collapse to_drop dup	

** Resolve duplicates (when the same meter starts the hour after it just stopped)
duplicates t sp_uuid pge_badge_nbr, gen(dup)
sort sp_uuid pge_badge_nbr mtr_install_date mtr_install_hour mtr_remove_date mtr_remove_hour
//br if dup>0
gen to_collapse = .
replace to_collapse = 1 if mtr_remove_date==mtr_install_date[_n+1] & ///
	mtr_remove_hour+1==mtr_install_hour[_n+1] & sp_uuid==sp_uuid[_n+1] & ///
	pge_badge_nbr==pge_badge_nbr[_n+1] & dup>0
gen to_drop = .
replace to_drop = 1 if sp_uuid==sp_uuid[_n-1] & pge_badge_nbr==pge_badge_nbr[_n-1] & ///
	dup>0 & to_collapse==. & to_collapse[_n-1]==1
replace mtr_remove_date = mtr_remove_date[_n+1] if to_collapse==1 & to_drop==. & ///
		to_collapse[_n+1]==. & to_drop[_n+1]==1
replace mtr_remove_hour = mtr_remove_hour[_n+1] if to_collapse==1 & to_drop==. & ///
		to_collapse[_n+1]==. & to_drop[_n+1]==1
unique sp_uuid pge_badge_nbr
local uniq = r(unique)
drop if to_drop==1
unique sp_uuid pge_badge_nbr
assert r(unique)==`uniq'		
drop to_collapse to_drop dup	

duplicates t sp_uuid pge_badge_nbr, gen(dup)
sort sp_uuid pge_badge_nbr mtr_install_date mtr_install_hour mtr_remove_date mtr_remove_hour
//br if dup>0
gen to_collapse = .
replace to_collapse = 1 if mtr_remove_date+1==mtr_install_date[_n+1] & ///
	mtr_remove_hour==23 & mtr_install_hour[_n+1]==0 & sp_uuid==sp_uuid[_n+1] & ///
	pge_badge_nbr==pge_badge_nbr[_n+1] & dup>0
gen to_drop = .
replace to_drop = 1 if sp_uuid==sp_uuid[_n-1] & pge_badge_nbr==pge_badge_nbr[_n-1] & ///
	dup>0 & to_collapse==. & to_collapse[_n-1]==1
replace mtr_remove_date = mtr_remove_date[_n+1] if to_collapse==1 & to_drop==. & ///
		to_collapse[_n+1]==. & to_drop[_n+1]==1
replace mtr_remove_hour = mtr_remove_hour[_n+1] if to_collapse==1 & to_drop==. & ///
		to_collapse[_n+1]==. & to_drop[_n+1]==1
unique sp_uuid pge_badge_nbr
local uniq = r(unique)
drop if to_drop==1
unique sp_uuid pge_badge_nbr
assert r(unique)==`uniq'		
drop to_collapse to_drop dup	
	
** Resolve duplicates (when the same meter starts the same day it just stopped)
duplicates t sp_uuid pge_badge_nbr, gen(dup)
sort sp_uuid pge_badge_nbr mtr_install_date mtr_install_hour mtr_remove_date mtr_remove_hour
//br if dup>0
gen to_collapse = .
replace to_collapse = 1 if mtr_remove_date==mtr_install_date[_n+1] & ///
	 sp_uuid==sp_uuid[_n+1] & pge_badge_nbr==pge_badge_nbr[_n+1] & dup>0
gen to_drop = .
replace to_drop = 1 if sp_uuid==sp_uuid[_n-1] & pge_badge_nbr==pge_badge_nbr[_n-1] & ///
	dup>0 & to_collapse==. & to_collapse[_n-1]==1
replace mtr_remove_date = mtr_remove_date[_n+1] if to_collapse==1 & to_drop==. & ///
		to_collapse[_n+1]==. & to_drop[_n+1]==1
replace mtr_remove_hour = mtr_remove_hour[_n+1] if to_collapse==1 & to_drop==. & ///
		to_collapse[_n+1]==. & to_drop[_n+1]==1
unique sp_uuid pge_badge_nbr
local uniq = r(unique)
drop if to_drop==1
unique sp_uuid pge_badge_nbr
assert r(unique)==`uniq'		
drop to_collapse to_drop dup	

** Resolve duplicates (when the same meter starts within 15 days of the day it just stopped)
duplicates t sp_uuid pge_badge_nbr, gen(dup)
sort sp_uuid pge_badge_nbr mtr_install_date mtr_install_hour mtr_remove_date mtr_remove_hour
//br if dup>0
gen to_collapse = .
replace to_collapse = 1 if mtr_install_date[_n+1]-mtr_remove_date<=15 & ///
	 sp_uuid==sp_uuid[_n+1] & pge_badge_nbr==pge_badge_nbr[_n+1] & dup>0
gen to_drop = .
replace to_drop = 1 if sp_uuid==sp_uuid[_n-1] & pge_badge_nbr==pge_badge_nbr[_n-1] & ///
	dup>0 & to_collapse==. & to_collapse[_n-1]==1
replace mtr_remove_date = mtr_remove_date[_n+1] if to_collapse==1 & to_drop==. & ///
		to_collapse[_n+1]==. & to_drop[_n+1]==1
replace mtr_remove_hour = mtr_remove_hour[_n+1] if to_collapse==1 & to_drop==. & ///
		to_collapse[_n+1]==. & to_drop[_n+1]==1
unique sp_uuid pge_badge_nbr
local uniq = r(unique)
drop if to_drop==1
unique sp_uuid pge_badge_nbr
assert r(unique)==`uniq'		
drop to_collapse to_drop dup

** Tranpose dates into lapses, for remaining 6 duplicates
duplicates t sp_uuid pge_badge_nbr, gen(dup)
unique sp_uuid pge_badge_nbr if dup>0 // 3 unique dups
local uniq_dups = r(unique)
sort sp_uuid pge_badge_nbr mtr_install_date mtr_install_hour
gen temp_first = dup>0 & !(sp_uuid==sp_uuid[_n-1] & pge_badge_nbr==pge_badge_nbr[_n-1])
gen start1 = mtr_install_date if temp_first	
gen start1h = mtr_install_hour if temp_first	
gen stop1 = mtr_remove_date if temp_first	
gen stop1h = mtr_remove_hour if temp_first	
gen start2 = mtr_install_date[_n+1] if temp_first & ///
	(sp_uuid==sp_uuid[_n+1] & pge_badge_nbr==pge_badge_nbr[_n+1])
gen start2h = mtr_install_hour[_n+1] if temp_first & ///
	(sp_uuid==sp_uuid[_n+1] & pge_badge_nbr==pge_badge_nbr[_n+1])
gen stop2 = mtr_remove_date[_n+1] if temp_first & ///
	(sp_uuid==sp_uuid[_n+1] & pge_badge_nbr==pge_badge_nbr[_n+1])
gen stop2h = mtr_remove_hour[_n+1] if temp_first & ///
	(sp_uuid==sp_uuid[_n+1] & pge_badge_nbr==pge_badge_nbr[_n+1])
format %td start? stop?
assert start2>=stop1 if temp_first
	
	// keep one observation per duplicate
keep if temp_first | dup==0
unique sp_uuid pge_badge_nbr if dup>0 
assert `uniq_dups'==r(unique) 
assert r(unique)==r(N)

	// assign first and last date as start/stop
replace mtr_remove_date = stop2 if dup>0 & temp_first
replace mtr_remove_hour = stop2 if dup>0 & temp_first

	// rename lapse dates
rename stop1 mtr_lapse_start1
rename start2 mtr_lapse_stop1
assert mtr_lapse_stop1!=. if dup>0 & temp_first // no open-ended lapses
drop dup temp_first start* stop*
la var mtr_lapse_start1 "Start date of lapse 1 (when meter number wasn't listed as active)"
la var mtr_lapse_stop1 "Stop date of lapse 1 (when meter number wasn't listed as active)"

** Confirm uniqueness
unique sp_uuid pge_badge_nbr
assert r(unique)==r(N)

** Save
compress
save "$dirpath_data/pge_cleaned/meter_badge_number_data_20180719.dta", replace	

}

*******************************************************************************
*******************************************************************************

