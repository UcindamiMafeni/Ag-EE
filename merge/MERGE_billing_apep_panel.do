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

** 2. De-dupification of the big merged dataset
{
use "$dirpath_data/merged/temp_big_merge.dta", clear

	// First item of business: identify SAs/SPs/persons/meters that ever match to a pump test/project
gen MATCH = merge_apep_proj==3 | (merge_apep_test==3 & sp_uuid!="")

unique pge_badge_nbr if merge_customer_meter==3
local uniq = r(unique)
unique pge_badge_nbr if merge_customer_meter==3 & MATCH==1
di r(unique)/`uniq' // 8,802 meters (6.1%)

unique sa_uuid if merge_customer_meter==3
local uniq = r(unique)
unique sa_uuid if merge_customer_meter==3 & MATCH==1
di r(unique)/`uniq' // 13,293 SAs (8.2%)

unique sp_uuid if merge_customer_meter==3
local uniq = r(unique)
unique sp_uuid if merge_customer_meter==3 & MATCH==1
di r(unique)/`uniq' // 7,670 SPs (10.6%)

unique prsn_uuid if merge_customer_meter==3
local uniq = r(unique)
unique prsn_uuid if merge_customer_meter==3 & MATCH==1
di r(unique)/`uniq' // 2,667 persons (8.3%)

	// Create list of missing meters to send back to PGE
unique pge_badge_nbr if (test_date_stata!=. | apep_proj_id!=.) 
local uniq = r(unique) // 15216 total APEP meters
unique pge_badge_nbr if (test_date_stata!=. | apep_proj_id!=.) ///
	& length(pge_badge_nbr)==10 
di r(unique)/`uniq' //  76% of all APEP meters have 10 digits
	
unique pge_badge_nbr if (test_date_stata!=. | apep_proj_id!=.) & sp_uuid!=""
local uniq = r(unique) // 8957 matched APEP meters
unique pge_badge_nbr if (test_date_stata!=. | apep_proj_id!=.) & sp_uuid!="" ///
	& length(pge_badge_nbr)==10 
di r(unique)/`uniq' //  74% of matched APEP meters have 10 digits

preserve
gen temp = (test_date_stata!=. | apep_proj_id!=.) & sp_uuid==""
egen temp_min = min(temp), by(pge_badge_nbr)
unique pge_badge_nbr if temp_min==1 // 6259 unmached APEP meters
keep if temp_min==1
assert sp_uuid=="" 
assert merge_customer_meter==.
assert merge_apep_test==2 | merge_apep_test==.
keep pge_badge_nbr apep_proj_id apeptestid customertype farmtype waterenduse test_date_stata
tab waterenduse customertype, missing
gen len = length(p)
gen year = year(test_date_stata)
tab year len
drop apeptestid test_date_stata apep_proj_id
duplicates drop
tab waterenduse customertype, missing
unique pge_badge_nbr
drop farmtype
duplicates drop
unique pge_badge_nbr
duplicates t pge_badge_nbr, gen(dup)
sort pge_badge_nbr
br if dup>0
keep pge_badge_nbr len
duplicates drop
sort len pge_badge_nbr
drop len
outsheet using "$dirpath_data/misc/missing_meters.csv", comma replace
restore

	// Flag cross-sectional units that ever matchto a pump test/project
foreach v of varlist pge_badge_nbr sa_uuid sp_uuid prsn_uuid {
	egen MATCH_max_`v' = max(MATCH), by(`v')
	replace MATCH_max_`v' = 0 if mi(`v')
}

	// Drop units that never match, which we have no way of knowing if they even do pumping
drop if MATCH_max_pge_badge_nbr==0 & MATCH_max_sa_uuid==0 & ///
	MATCH_max_sp_uuid==0 & MATCH_max_prsn_uuid==0
count if MATCH_max_pge_badge_nbr==0 & MATCH_max_sa_uuid==0 & ///
	MATCH_max_sp_uuid==0 & MATCH_max_prsn_uuid==1
di r(N)/_N  // 56% of obs are for a *person* who has *different* a SA/SP/meter that matches

	// Drop SP/SA/meters that never match, which we have no way of knowing if they ever pump
drop if MATCH_max_pge_badge_nbr==0 & MATCH_max_sa_uuid==0 & ///
	MATCH_max_sp_uuid==0
tab MATCH_max_pge_badge_nbr
tab MATCH_max_sa_uuid
tab MATCH_max_sp_uuid // 99.93% of remaming observations have an SP that matches

	// Drop any SPs that are missing from the billing data
FOR NOW
drop bill_start_dt-merge_billing_customer
duplicates drop	
sort sp_uuid
br sp_uuid sa_uuid pge_badge_nbr sa_sp_start sa_sp_stop in_billing mtr_install_date ///
	mtr_remove_date merge_apep_test test_date_stata merge_apep_proj MATCH*

}

*******************************************************************************
*******************************************************************************
