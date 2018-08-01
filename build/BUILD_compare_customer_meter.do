clear all
version 13
set more off

*************************************************************************************************
**** Script to compare customer data with meter history data, and create SP-date-meter xwalk ****
*************************************************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

*******************************************************************************
*******************************************************************************

** 1. March 2018 data pull
if 1==0{

** Load cleaned PGE bililng data
use "$dirpath_data/pge_cleaned/pge_cust_detail_20180322.dta", clear
unique sp_uuid sa_uuid
assert r(unique)==r(N)

** Merge with billing data
rename pge_badge_nbr pge_badge_nbrM
joinby sp_uuid using "$dirpath_data/pge_cleaned/meter_badge_number_data_20180322.dta", unmatched(both)
tab _merge
assert pge_badge_nbrM=="" if _merge==1 // all unmatched SPs have missing badge number
count if _merge!=1 & pge_badge_nbrM==""
local n = r(N) 
count if pge_badge_nbrM=="" 
di `n'/r(N) // 81% of SPs with missing badge number match into meter history data

** Pare down duplicates
duplicates t sp_uuid sa_uuid, gen(dup)
tab dup
sort sp_uuid mtr_install_date sa_sp_start
br prsn_uuid sp_uuid sa_uuid pge_badge_nbr* sa_sp_start sa_sp_stop mtr_install_date ///
	mtr_remove_date _merge if dup>0

	// flag cases where dates match
gen temp_dt_match_start = sa_sp_start>=mtr_install_date & _merge==3
gen temp_dt_match_end =  sa_sp_stop<=mtr_remove_date & _merge==3
gen temp_dt_match = temp_dt_match_start==1 & temp_dt_match_end==1
egen temp_dt_match_max = max(temp_dt_match), by(sp_uuid sa_uuid)

	// drop if dates don't match BUT dates do match for another meter, for the same SP/SA
unique sp_uuid sa_uuid
local uniq = r(unique)
drop if temp_dt_match==0 & temp_dt_match_max==1
unique sp_uuid sa_uuid
assert r(unique)==`uniq'

	// tag remaining dups
duplicates t sp_uuid sa_uuid, gen(dup2)
tab dup2
br prsn_uuid sp_uuid sa_uuid pge_badge_nbr* sa_sp_start sa_sp_stop mtr_install_date ///
	mtr_remove_date dup2 if dup2>0 & temp_dt_match_max==0
br prsn_uuid sp_uuid sa_uuid pge_badge_nbr* sa_sp_start sa_sp_stop mtr_install_date ///
	mtr_remove_date dup2 if dup2>0 & temp_dt_match_max==1
br prsn_uuid sp_uuid sa_uuid pge_badge_nbr* sa_sp_start sa_sp_stop mtr_install_date ///
	mtr_remove_date dup2 if dup2>0 & temp_dt_match_max==1 & sa_sp_stop- sa_sp_start>0	

	// flag cases where dates are in lapses
gen temp_dt_lapse_start = sa_sp_start>=mtr_lapse_start1 & _merge==3
gen temp_dt_lapse_end =  sa_sp_stop<=mtr_lapse_stop1 & _merge==3
gen temp_dt_lapse = temp_dt_lapse_start==1 & temp_dt_lapse_end==1
egen temp_dt_lapse_max = max(temp_dt_lapse), by(sp_uuid sa_uuid)
tab temp_dt_lapse temp_dt_lapse_max // only 2 observations, so I'm ignoring this
	
	// Not much room for further disambiguation here. I went into this thinking I would add
	// the correct meter number to the customer data, but that isn't really possible in any
	// way that's close to unique. And, we don't really care about meter number per se, except 
	// as a means to link pump test data to SPs & SAs. So instead, I'm saving this as a 
	// crosswalk for SP/SA/date/meter. Now, for any badge number-date, we will have a unique
	// SP/SA to link to customer/billing/interval data.
	
** Keep only essential identifying variables for crosswalk
keep sp_uuid sa_uuid pge_badge_nbr sa_sp_start sa_sp_stop mtr_install_date mtr_remove_date
unique sp_uuid sa_uuid pge_badge_nbr mtr_install_date
assert r(unique)==r(N)	
unique sp_uuid sa_uuid pge_badge_nbr mtr_install_date
assert r(unique)==r(N)	
sort sp_uuid mtr_install_date sa_sp_start pge_badge_nbr sa_uuid 

** Save
compress
save "$dirpath_data/pge_cleaned/xwalk_sp_meter_date_20180322.dta", replace

}

*******************************************************************************
*******************************************************************************

** 2. July 2018 data pull
{

** Load cleaned PGE bililng data
use "$dirpath_data/pge_cleaned/pge_cust_detail_20180719.dta", clear
unique sp_uuid sa_uuid
assert r(unique)==r(N)

** Merge with billing data
rename pge_badge_nbr pge_badge_nbrM
joinby sp_uuid using "$dirpath_data/pge_cleaned/meter_badge_number_data_20180719.dta", unmatched(both)
tab _merge
assert pge_badge_nbrM=="" if _merge==1 // all unmatched SPs have missing badge number
count if _merge!=1 & pge_badge_nbrM==""
local n = r(N) 
count if pge_badge_nbrM=="" 
di `n'/r(N) // 81% of SPs with missing badge number match into meter history data

** Pare down duplicates
duplicates t sp_uuid sa_uuid, gen(dup)
tab dup
sort sp_uuid mtr_install_date sa_sp_start
br prsn_uuid sp_uuid sa_uuid pge_badge_nbr* sa_sp_start sa_sp_stop mtr_install_date ///
	mtr_remove_date _merge if dup>0

	// flag cases where dates match
gen temp_dt_match_start = sa_sp_start>=mtr_install_date & _merge==3
gen temp_dt_match_end =  sa_sp_stop<=mtr_remove_date & _merge==3
gen temp_dt_match = temp_dt_match_start==1 & temp_dt_match_end==1
egen temp_dt_match_max = max(temp_dt_match), by(sp_uuid sa_uuid)

	// drop if dates don't match BUT dates do match for another meter, for the same SP/SA
unique sp_uuid sa_uuid
local uniq = r(unique)
drop if temp_dt_match==0 & temp_dt_match_max==1
unique sp_uuid sa_uuid
assert r(unique)==`uniq'

	// tag remaining dups
duplicates t sp_uuid sa_uuid, gen(dup2)
tab dup2
br prsn_uuid sp_uuid sa_uuid pge_badge_nbr* sa_sp_start sa_sp_stop mtr_install_date ///
	mtr_remove_date dup2 if dup2>0 & temp_dt_match_max==0
br prsn_uuid sp_uuid sa_uuid pge_badge_nbr* sa_sp_start sa_sp_stop mtr_install_date ///
	mtr_remove_date dup2 if dup2>0 & temp_dt_match_max==1
br prsn_uuid sp_uuid sa_uuid pge_badge_nbr* sa_sp_start sa_sp_stop mtr_install_date ///
	mtr_remove_date dup2 if dup2>0 & temp_dt_match_max==1 & sa_sp_stop- sa_sp_start>0	

	// flag cases where dates are in lapses
gen temp_dt_lapse_start = sa_sp_start>=mtr_lapse_start1 & _merge==3
gen temp_dt_lapse_end =  sa_sp_stop<=mtr_lapse_stop1 & _merge==3
gen temp_dt_lapse = temp_dt_lapse_start==1 & temp_dt_lapse_end==1
egen temp_dt_lapse_max = max(temp_dt_lapse), by(sp_uuid sa_uuid)
tab temp_dt_lapse temp_dt_lapse_max // only 2 observations, so I'm ignoring this
	
	// Not much room for further disambiguation here. I went into this thinking I would add
	// the correct meter number to the customer data, but that isn't really possible in any
	// way that's close to unique. And, we don't really care about meter number per se, except 
	// as a means to link pump test data to SPs & SAs. So instead, I'm saving this as a 
	// crosswalk for SP/SA/date/meter. Now, for any badge number-date, we will have a unique
	// SP/SA to link to customer/billing/interval data.
	
** Keep only essential identifying variables for crosswalk
keep sp_uuid sa_uuid pge_badge_nbr sa_sp_start sa_sp_stop mtr_install_date mtr_remove_date
unique sp_uuid sa_uuid pge_badge_nbr mtr_install_date
assert r(unique)==r(N)	
unique sp_uuid sa_uuid pge_badge_nbr mtr_install_date
assert r(unique)==r(N)	
sort sp_uuid mtr_install_date sa_sp_start pge_badge_nbr sa_uuid 

** Save
compress
save "$dirpath_data/pge_cleaned/xwalk_sp_meter_date_20180719.dta", replace

}

*******************************************************************************
*******************************************************************************


