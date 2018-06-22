clear all
version 13
set more off

*************************************************************************
**** Script to merge full panel of customer-meter-pump test datasets ****
*************************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

	***** COME BACK AND FIX THIS STUFF LATER:

*******************************************************************************
*******************************************************************************

** 1. Merge together customer, meter, and APEP datasets
{

** Load cleaned PGE customer data
use "$dirpath_data/pge_cleaned/pge_cust_detail.dta", clear

** Merge in PGE meter data
rename pge_badge_nbr pge_badge_nbrBAD
joinby sa_uuid sp_uuid sa_sp_start sa_sp_stop using ///
	"$dirpath_data/pge_cleaned/xwalk_sp_meter_date.dta", unmatched(both)
assert _merge==3
drop _merge
assert sp_uuid!=""

	// Drop bad meter ID variable held over from customer data
drop pge_badge_nbrBAD	

** Merge in APEP data
preserve
use "$dirpath_data/pge_cleaned/pump_test_data.dta", clear
unique apeptestid test_date_stata customertype farmtype waterenduse pge_badge_nbr
assert r(unique)==r(N)
keep apeptestid test_date_stata customertype farmtype waterenduse pge_badge_nbr
gen apeptestid_uniq = _n
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
di r(unique)/`uniq' // 7776 SPs merge to an APEP test
rename _merge merge_apep_test
	
** Merge in APEP project data
joinby pge_badge_nbr using "$dirpath_data/pge_cleaned/pump_test_project_data.dta", ///
	unmatched(both)
unique sp_uuid
local uniq = r(unique)
unique sp_uuid if _merge==3 & merge_apep_test==3 
di r(unique)/`uniq'	// 428 SPs merge to an APEP project
rename _merge merge_apep_proj

** First item of business: identify SAs/SPs/persons/meters that ever match to a pump test/project
gen MATCH = merge_apep_test==3 | (merge_apep_proj==3 & sp_uuid!="")

unique pge_badge_nbr if sp_uuid!=""
local uniq = r(unique)
unique pge_badge_nbr if sp_uuid!="" & MATCH==1
di r(unique)/`uniq' // 8,957 meters (5.7%)

unique sa_uuid if sp_uuid!=""
local uniq = r(unique)
unique sa_uuid if sp_uuid!="" & MATCH==1
di r(unique)/`uniq' // 13,293 SAs (8.2%)

unique sp_uuid if sp_uuid!=""
local uniq = r(unique)
unique sp_uuid if sp_uuid!="" & MATCH==1
di r(unique)/`uniq' // 7,670 SPs (10.6%)

unique prsn_uuid if sp_uuid!=""
local uniq = r(unique)
unique prsn_uuid if sp_uuid!="" & MATCH==1
di r(unique)/`uniq' // 2,667 persons (8.3%)

** Create list of missing meters to send back to PGE
unique pge_badge_nbr if (test_date_stata!=. | apep_proj_id!=.) 
local uniq = r(unique) // 15,216 total APEP meters
unique pge_badge_nbr if (test_date_stata!=. | apep_proj_id!=.) ///
	& length(pge_badge_nbr)==10 
di r(unique)/`uniq' //  76% of all APEP meters have 10 digits
	
unique pge_badge_nbr if (test_date_stata!=. | apep_proj_id!=.) & sp_uuid!=""
local uniq = r(unique) // 8,957 matched APEP meters
unique pge_badge_nbr if (test_date_stata!=. | apep_proj_id!=.) & sp_uuid!="" ///
	& length(pge_badge_nbr)==10 
di r(unique)/`uniq' //  74% of matched APEP meters have 10 digits

preserve
gen temp = (test_date_stata!=. | apep_proj_id!=.) & sp_uuid==""
egen temp_min = min(temp), by(pge_badge_nbr)
unique pge_badge_nbr if temp_min==1 // 6259 unmached APEP meters
keep if temp_min==1
assert sp_uuid=="" 
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

** Flag cross-sectional units that ever match to a pump test/project
foreach v of varlist pge_badge_nbr sa_uuid sp_uuid prsn_uuid {
	egen MATCH_max_`v' = max(MATCH), by(`v')
	replace MATCH_max_`v' = 0 if mi(`v')
}

** Drop units that never match, which we have no way of knowing if they even do pumping
unique sp_uuid
local uniq = r(unique)
drop if MATCH_max_pge_badge_nbr==0 & MATCH_max_sa_uuid==0 & ///
	MATCH_max_sp_uuid==0 & MATCH_max_prsn_uuid==0
unique sp_uuid
di r(unique)/`uniq' // 40% of all SPs

unique sp_uuid if MATCH_max_pge_badge_nbr==0 & MATCH_max_sa_uuid==0 & ///
	MATCH_max_sp_uuid==0 & MATCH_max_prsn_uuid==1
di r(unique)/`uniq'  // 29% of SPs are for a *person* who has a *different* SP/SA/meter that matches

** Drop SP/SA/meters that never match, which we have no way of knowing if they ever pump
drop if MATCH_max_pge_badge_nbr==0 & MATCH_max_sa_uuid==0 & ///
	MATCH_max_sp_uuid==0
unique sp_uuid
di r(unique)/`uniq' // 10% of all SPs
	
tab MATCH_max_pge_badge_nbr
tab MATCH_max_sa_uuid
tab MATCH_max_sp_uuid // 99.88% of remaming observations have an SP that matches

sort sp_uuid sa_sp_start mtr_install_date test_date_stata
br sp_uuid sa_uuid pge_badge_nbr sa_sp_start sa_sp_stop bill_dt_first bill_dt_last mtr_install_date ///
	mtr_remove_date merge_apep_test test_date_stata merge_apep_proj MATCH*

** Pare down duplicates by SA start/stop, bill first/last, meter install/remove, and pump test dates

	// First: pump tests with dates that coincide with only one SA within an SP-meter (Sa start/stop AND bill first/last)
duplicates t sp_uuid pge_badge_nbr apeptestid_uniq apep_proj_id, gen(dup)
br sp_uuid sa_uuid pge_badge_nbr sa_sp_start sa_sp_stop bill_dt_first bill_dt_last mtr_install_date ///
	mtr_remove_date test_date_stata apeptestid_uniq merge_apep_proj MATCH* ///
	if dup>0 & test_date_stata!=.
gen temp_keep = test_date_stata>=bill_dt_first & test_date_stata<=bill_dt_last & ///
	test_date_stata>=sa_sp_start & test_date_stata<=sa_sp_stop & dup>0 & test_date_stata!=.
egen temp_keep_max = max(temp_keep), by(sp_uuid pge_badge_nbr apeptestid_uniq apep_proj_id)	
unique sp_uuid pge_badge_nbr apeptestid_uniq apep_proj_id
local uniq = r(unique)
drop if temp_keep==0 & temp_keep_max==1
unique sp_uuid pge_badge_nbr apeptestid_uniq apep_proj_id
assert `uniq'==r(unique)
drop dup temp*

	// Second: pump tests with dates that coincide with only one SA within an SP-meter (bill first/last)
duplicates t sp_uuid pge_badge_nbr apeptestid_uniq apep_proj_id, gen(dup)
br sp_uuid sa_uuid pge_badge_nbr sa_sp_start sa_sp_stop bill_dt_first bill_dt_last mtr_install_date ///
	mtr_remove_date test_date_stata apeptestid_uniq merge_apep_proj MATCH* ///
	if dup>0 & test_date_stata!=.
gen temp_keep = (test_date_stata>=bill_dt_first & test_date_stata<=bill_dt_last) & ///
	dup>0 & test_date_stata!=.
egen temp_keep_max = max(temp_keep), by(sp_uuid pge_badge_nbr apeptestid_uniq apep_proj_id)	
tab dup temp_keep_max if test_date_stata!=., missing
br sp_uuid sa_uuid pge_badge_nbr sa_sp_start sa_sp_stop bill_dt_first bill_dt_last mtr_install_date ///
	mtr_remove_date test_date_stata apeptestid_uniq merge_apep_proj MATCH* ///
	if dup>0 & test_date_stata!=. & temp_keep_max==1
unique sp_uuid pge_badge_nbr apeptestid_uniq apep_proj_id
local uniq = r(unique)
drop if temp_keep==0 & temp_keep_max==1
unique sp_uuid pge_badge_nbr apeptestid_uniq apep_proj_id
assert `uniq'==r(unique)
drop dup temp*

	// Third: drop remaining pump tests with dates don't coincide any SA (due to missing SA's!!!!)
duplicates t sp_uuid pge_badge_nbr apeptestid_uniq apep_proj_id, gen(dup)
gen temp_keep = (test_date_stata>=bill_dt_first & test_date_stata<=bill_dt_last) & test_date_stata!=.
egen temp_keep_max = max(temp_keep), by(sp_uuid pge_badge_nbr apeptestid_uniq apep_proj_id)	
assert temp_keep==temp_keep_max // confirm no remaining dups
tab dup temp_keep if test_date_stata!=.
unique pge_badge_nbr if test_date_stata!=.
local uniq = r(unique)
unique pge_badge_nbr if test_date_stata!=. & temp_keep==0
di r(unique)/`uniq' // 14% of MATCHED meters don't
drop if temp_keep==0
drop dup temp_keep temp_keep_max
unique sp_uuid pge_badge_nbr apeptestid_uniq apep_proj_id if test_date_stata!=.
assert r(unique)==r(N)

** True-up APEP tests with APEP project test dates
egen temp_sp_proj = max(merge_apep_proj==3), by(sp_uuid)
unique sp_uuid
local uniq = r(unique)
unique sp_uuid if temp_sp_proj==1
di r(unique)/`uniq'
unique sp_uuid if apep_proj_id!=. // 361 SPs with projects
unique sp_uuid apep_proj_id if temp_sp_proj==1 // 576 projects across these 361 SPs
sort sp_uuid test_date_stata
br sp_uuid test_date_stata apeptestid_uniq merge_apep_proj date_proj_finish-date_test_subs ///
	flag_date_problem flag_subs_after_proj flag_subs_before_proj est_savings_kwh_yr subsidy_proj ///
	iswell run flag_apep_mismatch if temp_sp_proj==1
drop temp*	

	// Left-join APEP projects on sp_uuid, in order to pare down pre/post dates
preserve
keep sp_uuid merge_apep_proj apep_proj_id-flag_apep_mismatch
duplicates drop
drop if apep_proj_id==.
unique apep_proj_id
assert r(unique)==r(N)
assert merge_apep_proj==3
drop merge_apep_proj
unique sp_uuid
local n_sp = r(unique)
tempfile projs
save `projs'
restore
drop merge_apep_proj apep_proj_id-flag_apep_mismatch
joinby sp_uuid using `projs', unmatched(both)
assert _merge!=2
unique sp_uuid if _merge==3
assert r(unique)==`n_sp'
rename _merge merge_apep_proj	
	
	// Construct new versions of APEP project variables linked to the pump test date
gen apep_date_test_pre = test_date_stata==date_test_pre & test_date_stata!=.
gen apep_date_test_post = test_date_stata==date_test_post & test_date_stata!=.
gen apep_date_test_subs = test_date_stata==date_test_subs & test_date_stata!=.
	
	// Duplicates drop if no dates match
duplicates t sp_uuid apeptestid_uniq test_date_stata date_proj_finish subsidy_proj, gen(dup)
tab dup 
gen temp = apep_date_test_pre==1 | apep_date_test_post==1 | apep_date_test_subs==1
egen temp_max = max(temp), by(sp_uuid apeptestid_uniq test_date_stata date_proj_finish subsidy_proj)
unique sp_uuid apeptestid_uniq test_date_stata date_proj_finish subsidy_proj
local uniq = r(unique)
drop if dup>0 & temp==0 & temp_max==1
unique sp_uuid apeptestid_uniq test_date_stata date_proj_finish subsidy_proj
assert r(unique)==`uniq'
drop dup temp*

	// Remaining dups: drop project test date variables and project ID, then duplicates drop
duplicates t sp_uuid apeptestid_uniq test_date_stata date_proj_finish subsidy_proj, gen(dup)
tab dup 
br sp_uuid test_date_stata apeptestid_uniq apep_proj_id date_proj_finish-date_test_subs ///
	flag_date_problem flag_subs_after_proj flag_subs_before_proj est_savings_kwh_yr subsidy_proj ///
	iswell run flag_apep_mismatch if dup>0
drop apep_date_test_pre apep_date_test_post apep_date_test_subs apep_proj_id
unique sp_uuid apeptestid_uniq test_date_stata date_proj_finish subsidy_proj
local uniq = r(unique)
duplicates drop	
unique sp_uuid apeptestid_uniq test_date_stata date_proj_finish subsidy_proj
assert r(unique)==`uniq'
drop dup

	// Remaining dups: drop dup where subsidy date conflicts
duplicates t sp_uuid apeptestid_uniq test_date_stata date_proj_finish subsidy_proj ///
	date_test_pre date_test_post, gen(dup)
tab dup 
br sp_uuid test_date_stata apeptestid_uniq date_proj_finish-date_test_subs ///
	flag_date_problem flag_subs_after_proj flag_subs_before_proj est_savings_kwh_yr subsidy_proj ///
	iswell run flag_apep_mismatch if dup>0
gen temp = date_test_subs==test_date_stata
egen temp_max = max(temp), by(sp_uuid apeptestid_uniq test_date_stata date_proj_finish ///
	subsidy_proj date_test_pre date_test_post)
unique sp_uuid apeptestid_uniq test_date_stata date_proj_finish subsidy_proj ///
	date_test_pre date_test_post
local uniq = r(unique)
drop if temp==0 & temp_max==1 & dup>0
unique sp_uuid apeptestid_uniq test_date_stata date_proj_finish subsidy_proj ///
	date_test_pre date_test_post
assert r(unique)==`uniq'
drop dup temp*

	// Remaining dups: drop dup where iswell==0
duplicates t sp_uuid apeptestid_uniq test_date_stata date_proj_finish subsidy_proj ///
	date_test_pre date_test_post, gen(dup)
tab dup 
br sp_uuid test_date_stata apeptestid_uniq date_proj_finish-date_test_subs ///
	flag_date_problem flag_subs_after_proj flag_subs_before_proj est_savings_kwh_yr subsidy_proj ///
	iswell run flag_apep_mismatch if dup>0
egen temp_max = max(iswell), by(sp_uuid apeptestid_uniq test_date_stata date_proj_finish ///
	subsidy_proj date_test_pre date_test_post)
unique sp_uuid apeptestid_uniq test_date_stata date_proj_finish subsidy_proj ///
	date_test_pre date_test_post
local uniq = r(unique)
drop if iswell==0 & temp_max==1 & dup>0
unique sp_uuid apeptestid_uniq test_date_stata date_proj_finish subsidy_proj ///
	date_test_pre date_test_post
assert r(unique)==`uniq'
drop dup temp*
	
	// Remaining dups: drop dup where subsidy is after project end
duplicates t sp_uuid apeptestid_uniq test_date_stata date_proj_finish subsidy_proj ///
	date_test_pre date_test_post, gen(dup)
tab dup 
br sp_uuid test_date_stata apeptestid_uniq date_proj_finish-date_test_subs ///
	flag_date_problem flag_subs_after_proj flag_subs_before_proj est_savings_kwh_yr subsidy_proj ///
	iswell run flag_apep_mismatch if dup>0
egen temp_min = min(flag_subs_after_proj), by(sp_uuid apeptestid_uniq test_date_stata date_proj_finish ///
	subsidy_proj date_test_pre date_test_post)
unique sp_uuid apeptestid_uniq test_date_stata date_proj_finish subsidy_proj ///
	date_test_pre date_test_post
local uniq = r(unique)
drop if flag_subs_after_proj==1 & temp_min==0 & dup>0
unique sp_uuid apeptestid_uniq test_date_stata date_proj_finish subsidy_proj ///
	date_test_pre date_test_post
assert r(unique)==`uniq'
drop dup temp*
	
	// Remaining dups: drop dup where subsidy is before project start
duplicates t sp_uuid apeptestid_uniq test_date_stata date_proj_finish subsidy_proj ///
	date_test_pre date_test_post, gen(dup)
tab dup 
br sp_uuid test_date_stata apeptestid_uniq date_proj_finish-date_test_subs ///
	flag_date_problem flag_subs_after_proj flag_subs_before_proj est_savings_kwh_yr subsidy_proj ///
	iswell run flag_apep_mismatch if dup>0
egen temp_min = min(flag_subs_before_proj), by(sp_uuid apeptestid_uniq test_date_stata date_proj_finish ///
	subsidy_proj date_test_pre date_test_post)
unique sp_uuid apeptestid_uniq test_date_stata date_proj_finish subsidy_proj ///
	date_test_pre date_test_post
local uniq = r(unique)
drop if flag_subs_before_proj==1 & temp_min==0 & dup>0
unique sp_uuid apeptestid_uniq test_date_stata date_proj_finish subsidy_proj ///
	date_test_pre date_test_post
assert r(unique)==`uniq'
drop dup temp*
	
	// Remaining dups: take latest subsidy date
duplicates t sp_uuid apeptestid_uniq test_date_stata date_proj_finish subsidy_proj ///
	date_test_pre date_test_post, gen(dup)
tab dup 
br sp_uuid test_date_stata apeptestid_uniq date_proj_finish-date_test_subs ///
	flag_date_problem flag_subs_after_proj flag_subs_before_proj est_savings_kwh_yr subsidy_proj ///
	iswell run flag_apep_mismatch if dup>0
egen temp_max = max(date_test_subs), by(sp_uuid apeptestid_uniq test_date_stata date_proj_finish ///
	subsidy_proj date_test_pre date_test_post)
unique sp_uuid apeptestid_uniq test_date_stata date_proj_finish subsidy_proj ///
	date_test_pre date_test_post
local uniq = r(unique)
drop if date_test_subs<temp_max & dup>0
unique sp_uuid apeptestid_uniq test_date_stata date_proj_finish subsidy_proj ///
	date_test_pre date_test_post
assert r(unique)==`uniq'
drop dup temp*

	// Confirm uniqueness
unique sp_uuid apeptestid_uniq test_date_stata date_proj_finish subsidy_proj ///
	date_test_pre date_test_post
assert r(unique)==r(N)
unique sp_uuid apeptestid_uniq test_date_stata date_proj_finish subsidy_proj 
assert r(unique)==r(N)
unique sp_uuid  test_date_stata date_proj_finish subsidy_proj 
assert r(unique)==r(N)

}

*******************************************************************************
*******************************************************************************
