clear all
version 13
set more off

*****************************************************************************
**** Script to merge full panel of billing-customer-meter-pump test data ****
*****************************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

	***** COME BACK AND FIX THIS STUFF LATER:

*******************************************************************************
*******************************************************************************

** 1. Merge together billing, customer, meter, and APEP datasets
{

** Load cleaned PGE bililng data
use "$dirpath_data/pge_cleaned/billing_data.dta", clear

** Merge in customer details
joinby sa_uuid using "$dirpath_data/pge_cleaned/pge_cust_detail.dta", unmatched(both)
assert _merge!=1 // confirm that all SAs in billing data exist in customer data

	// Keep the correct SPs for each SA, and confirm uniqueness
unique sa_uuid bill_start_dt if _merge==3
local uniq = r(unique)
drop if _merge==3 & !inlist(sp_uuid,sp_uuid1,sp_uuid2,sp_uuid3)
unique sa_uuid bill_start_dt if _merge==3
assert `uniq'==r(unique)
unique sa_uuid sp_uuid bill_start_dt if _merge==3
assert r(unique)==r(N)
assert (_merge==2 & in_billing==0) | (_merge==3 & in_billing==1)

	// Rename merge variable (to keep track of below)
rename _merge merge_billing_customer

** Merge in PGE meter data
rename pge_badge_nbr pge_badge_nbrBAD
joinby sa_uuid sp_uuid sa_sp_start sa_sp_stop using ///
	"$dirpath_data/pge_cleaned/xwalk_sp_meter_date.dta", unmatched(both)
	
	// Resolve duplicates using xwalk (based on bill start/end dates)
unique sa_uuid sp_uuid bill_start_dt if merge_billing_customer==3
local uniq = r(unique)
gen temp_to_keep = _merge==3 & merge_billing_customer==3 & ///
	(inrange(bill_start_dt,mtr_install_date,mtr_remove_date) | ///
	inrange(bill_end_dt,mtr_install_date,mtr_remove_date))
egen temp_to_keep_max = max(temp_to_keep) if merge_billing_customer==3 & _merge==3, ///
	by(sa_uuid sp_uuid bill_start_dt)
drop if temp_to_keep==0 & temp_to_keep_max==1
unique sa_uuid sp_uuid bill_start_dt if merge_billing_customer==3
assert `uniq'==r(unique)

	// Diagnose remaining duplicates using xwalk (based on bill start/end dates)
duplicates t sa_uuid sp_uuid bill_start_dt, gen(dup)
tab dup if _merge==3 & merge_billing_customer==3
sort sa_uuid sp_uuid bill_start_dt 
br if dup>0 & _merge==3 & merge_billing_customer==3
	// Remaining dups are meters with install/repalcement dates that straddle a bill
	// Keep these dupes for now
	
	// Diagnose dups that did not merge into billing data 
tab dup	if merge_billing_customer==2 & _merge==3
br if dup>0 & merge_billing_customer==2 & _merge==3	
	// Remainign dups have SA/SP start/stop dates that partially overlap meter dates
	// Keep these dupes for now
drop dup temp*	
	
	// Append meter IDs that got dropped, to maximize chances of merging into APEP data
preserve
keep pge_badge_nbr
duplicates drop
merge 1:m pge_badge_nbr using "$dirpath_data/pge_cleaned/xwalk_sp_meter_date.dta", ///
	keep(2)
duplicates drop
tempfile meters
save `meters'
restore	
append using `meters'

	// Rename merge variable (to keep track of below)
rename _merge merge_customer_meters

	// Drop bad meter ID variable held over from customer data
drop pge_badge_nbrBAD	

** Merge in APEP data
preserve
use "$dirpath_data/pge_cleaned/pump_test_data.dta", clear
unique apeptestid test_date_stata customertype farmtype waterenduse pge_badge_nbr
assert r(unique)==r(N)
keep apeptestid test_date_stata customertype farmtype waterenduse pge_badge_nbr
tempfile apep
save `apep'
restore
joinby pge_badge_nbr using `apep', unmatched(both)
tab _merge
unique apeptestid test_date_stata
local uniq = r(unique)
unique apeptestid test_date_stata if _merge==3
di r(unique)/`uniq' // about 60% of pump tests merge to a meter
unique sp_uuid
local uniq = r(unique)
unique sp_uuid if _merge==3
di r(unique)/`uniq'
unique sp_uuid if _merge==3 & merge_customer_meter==3
di r(unique)/`uniq'
rename _merge merge_apep_test
	
** Merge in APEP project data
joinby pge_badge_nbr using "$dirpath_data/pge_cleaned/pump_test_project_data.dta", ///
	unmatched(both)
unique sp_uuid
local uniq = r(unique)
unique sp_uuid if _merge==3 & merge_apep_test==3 & merge_customer_meter==3
di r(unique)/`uniq'
rename _merge merge_apep_proj

** Save whole messy temporary file 
duplicates drop
compress
save "$dirpath_data/merged/temp_big_merge.dta", replace

}

*******************************************************************************
*******************************************************************************

