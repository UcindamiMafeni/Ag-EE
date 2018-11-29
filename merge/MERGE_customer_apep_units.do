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
use "$dirpath_data/pge_cleaned/pge_cust_detail_20180719.dta", clear


** Merge in PGE meter data
rename pge_badge_nbr pge_badge_nbrBAD
joinby sa_uuid sp_uuid sa_sp_start sa_sp_stop using ///
	"$dirpath_data/pge_cleaned/xwalk_sp_meter_date_20180719.dta", unmatched(both)
assert _merge!=1
unique sp_uuid if _merge==2 // 9 SPs in meter xwalk don't merge into customer data (out of 13314)
gen sp_not_in_cust_detail = _merge==2 // flag these 9 meters
rename _merge merge_sp_meter_xwalk
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
unique apeptestid test_date_stata if test_date_stata!=.
local uniq = r(unique)
unique apeptestid test_date_stata if test_date_stata!=. &_merge==3
di r(unique)/`uniq' // 99.5% of pump tests merge to a meter (huzzah!)
unique sp_uuid if sp_uuid!=""
local uniq = r(unique)
unique sp_uuid if _merge==3 & sp_uuid!=""
di r(unique)/`uniq' // all 13314 SPs merge to an APEP test (huzZAH!)
rename _merge merge_apep_test
	
	
** Merge in APEP project data
joinby pge_badge_nbr using "$dirpath_data/pge_cleaned/pump_test_project_data.dta", ///
	unmatched(both)
unique sp_uuid
local uniq = r(unique)
unique sp_uuid if _merge==3 & merge_apep_test==3 
di r(unique)/`uniq'	// 735 SPs merge to an APEP project
rename _merge merge_apep_proj

** First item of business: identify SAs/SPs/persons/meters that ever match to a pump test/project
gen MATCH = merge_apep_test==3 | (merge_apep_proj==3 & sp_uuid!="")
replace MATCH = 0 if sp_not_in_cust_detail==1

unique pge_badge_nbr if sp_uuid!=""
local uniq = r(unique)
unique pge_badge_nbr if sp_uuid!="" & MATCH==1
di r(unique)/`uniq' // 15,088 meters (all but 2)

unique sa_uuid if sp_uuid!=""
local uniq = r(unique)
unique sa_uuid if sp_uuid!="" & MATCH==1
di r(unique)/`uniq' // 40,857 SAs (all non-missings)

unique sp_uuid if sp_uuid!=""
local uniq = r(unique)
unique sp_uuid if sp_uuid!="" & MATCH==1
di r(unique)/`uniq' // 13,305 SPs (all but the 9 from the SP-meter xwalk flagged above)

unique prsn_uuid if sp_uuid!=""
local uniq = r(unique)
unique prsn_uuid if sp_uuid!="" & MATCH==1
di r(unique)/`uniq' // 7,764 persons (all non-missings)

/*
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
*/


** Flag cross-sectional units that ever match to a pump test/project
foreach v of varlist pge_badge_nbr sa_uuid sp_uuid {
	egen MATCH_max_`v' = max(MATCH), by(`v')
	replace MATCH_max_`v' = 0 if mi(`v')
}
egen temp = rowmax(MATCH_max*)
br if temp==0
egen temp2 = rowmin(MATCH_max*)
tab temp temp2
br if temp2==0
br if inlist(sp_uuid,"4076610000","5707910021","7681710016","4503710033","1893310030", /// 
			"1229110037","5001410005","6580910154","7136710095") | ///
		inlist(pge_badge_nbr,"W00997","83P256","1004793951","1006925479","1009390128", ///
			"20P008","1009410723","1009974273","5000047605")
sort pge_badge_nbr apeptestid // all but 2 SP-meter combos have matches elsewhere,
	// meaning that the others are redundant xwalk false positives
br if inlist(sp_uuid,"1893310030","7136710095") | ///
		inlist(pge_badge_nbr,"1009390128","5000047605") // looks like we're only losing 2 pump tests 
drop temp*

** Drop units that never match, which we have no way of knowing if they even do pumping
unique sp_uuid if sp_uuid!=""
local uniq = r(unique)
unique apeptestid if apeptestid!=.
local uniq2 = r(unique)
drop if MATCH_max_pge_badge_nbr==0 & MATCH_max_sa_uuid==0 & MATCH_max_sp_uuid==0 
unique sp_uuid if sp_uuid!=""
di `uniq' - r(unique) // we lose only 2 (totally unmatched) SPs
unique apeptestid if apeptestid!=.
di `uniq2' - r(unique) // we lose only 121 pump tests

tab MATCH_max_pge_badge_nbr
tab MATCH_max_sa_uuid
tab MATCH_max_sp_uuid // 99.88% of remaming observations have an SP that matches

sort sp_uuid sa_sp_start mtr_install_date test_date_stata
br sp_uuid sa_uuid pge_badge_nbr sa_sp_start sa_sp_stop bill_dt_first bill_dt_last mtr_install_date ///
	mtr_remove_date merge_apep_test test_date_stata merge_apep_proj MATCH*

	
** Pare down duplicates by SA start/stop, bill first/last, meter install/remove, and pump test dates

	// Before starting: drop APEP project data (for now), to dedupify one thing at a time
drop merge_apep_proj apep_proj_id-flag_apep_mismatch
unique sp_uuid
local uniq1 = r(unique)
unique apeptestid_uniq
local uniq2 = r(unique)
unique pge_badge_nbr
local uniq3 = r(unique)
duplicates drop
unique sp_uuid
assert `uniq1'==r(unique)
unique apeptestid_uniq
assert `uniq2'==r(unique)
unique pge_badge_nbr
assert `uniq3'==r(unique)

	// First: pump tests with dates that coincide with only one SA within an SP-meter (SA start/stop AND bill first/last)
duplicates t sp_uuid pge_badge_nbr apeptestid_uniq, gen(dup)
br sp_uuid sa_uuid pge_badge_nbr sa_sp_start sa_sp_stop bill_dt_first bill_dt_last mtr_install_date ///
	mtr_remove_date test_date_stata apeptestid_uniq MATCH* ///
	if dup>0 & test_date_stata!=.
gen temp_keep = test_date_stata>=bill_dt_first & test_date_stata<=bill_dt_last & ///
	test_date_stata>=sa_sp_start & test_date_stata<=sa_sp_stop & dup>0 & test_date_stata!=.
egen temp_keep_max = max(temp_keep), by(sp_uuid pge_badge_nbr apeptestid_uniq)	
unique sp_uuid pge_badge_nbr apeptestid_uniq
local uniq = r(unique)
drop if temp_keep==0 & temp_keep_max==1
unique sp_uuid pge_badge_nbr apeptestid_uniq
assert `uniq'==r(unique)
drop dup temp*

	// Second: pump tests with dates that coincide with only one SA within an SP-meter (bill first/last)
duplicates t sp_uuid pge_badge_nbr apeptestid_uniq, gen(dup)
br sp_uuid sa_uuid pge_badge_nbr sa_sp_start sa_sp_stop bill_dt_first bill_dt_last mtr_install_date ///
	mtr_remove_date test_date_stata apeptestid_uniq MATCH* ///
	if dup>0 & test_date_stata!=.
gen temp_keep = (test_date_stata>=bill_dt_first & test_date_stata<=bill_dt_last) & ///
	dup>0 & test_date_stata!=.
egen temp_keep_max = max(temp_keep), by(sp_uuid pge_badge_nbr apeptestid_uniq)	
tab dup temp_keep_max if test_date_stata!=., missing
br sp_uuid sa_uuid pge_badge_nbr sa_sp_start sa_sp_stop bill_dt_first bill_dt_last mtr_install_date ///
	mtr_remove_date test_date_stata apeptestid_uniq MATCH* ///
	if dup>0 & test_date_stata!=. & temp_keep_max==1
unique sp_uuid pge_badge_nbr apeptestid_uniq
local uniq = r(unique)
drop if temp_keep==0 & temp_keep_max==1
unique sp_uuid pge_badge_nbr apeptestid_uniq
assert `uniq'==r(unique)
drop dup temp*

	// Third: drop pump tests with dates don't coincide with any SA, but meter switched SPs
duplicates t sp_uuid pge_badge_nbr apeptestid_uniq, gen(dup)
tab dup
gen temp_keep = (test_date_stata>=bill_dt_first & test_date_stata<=bill_dt_last) & test_date_stata!=.
egen temp_keep_max = max(temp_keep), by(sp_uuid pge_badge_nbr apeptestid_uniq)	
assert temp_keep==temp_keep_max // confirm no remaining disambiguated SP-APEP dups
tab dup temp_keep if test_date_stata!=.
unique pge_badge_nbr if test_date_stata!=.
local uniq = r(unique)
unique pge_badge_nbr if test_date_stata!=. & temp_keep==0
di r(unique)/`uniq' // 3.7% of matched pump test meters don't have dates that don't coincide with any SA
egen temp_keep_max2 = max(temp_keep), by(apeptestid_uniq) // what if the meter chnaged SPs?
egen temp_keep_min2 = min(temp_keep), by(apeptestid_uniq) // what if the meter chnaged SPs?
tab temp_keep temp_keep_max2 if test_date_stata!=. // looks like this is the case for about 40%
sort apeptestid_uniq sp_uuid
br sp_uuid sa_uuid pge_badge_nbr sa_sp_start sa_sp_stop bill_dt_first bill_dt_last mtr_install_date ///
	mtr_remove_date test_date_stata apeptestid_uniq MATCH* temp_keep temp_keep*2 ///
	if test_date_stata!=. & temp_keep_max2==1 & temp_keep_min2==0
gen temp_keep_mtr = (test_date_stata>=mtr_install_date & test_date_stata<=mtr_remove_date) & test_date_stata!=.
egen temp_keep_mtr_max = max(temp_keep_mtr), by(apeptestid_uniq)
egen temp_keep_mtr_min = min(temp_keep_mtr), by(apeptestid_uniq)
br sp_uuid sa_uuid pge_badge_nbr sa_sp_start sa_sp_stop bill_dt_first bill_dt_last mtr_install_date ///
	mtr_remove_date test_date_stata apeptestid_uniq MATCH* temp_keep temp_keep*2 temp_keep_mtr* ///
	if test_date_stata!=. & temp_keep_max2==1 & temp_keep_min2==0 & temp_keep_mtr_min<temp_keep_mtr_max
tab temp_keep temp_keep_mtr if temp_keep_max2==1 & temp_keep_min2==0 & temp_keep_mtr_min<temp_keep_mtr_max
	// keep based on meter dates is almost in perfect agreement with keep based on bill dates
unique sp_uuid pge_badge_nbr apeptestid_uniq
local uniq1 = r(unique)
unique apeptestid_uniq
local uniq2 = r(unique)
unique sp_uuid
local uniq3 = r(unique)
drop if temp_keep==0 & temp_keep_max2==1
unique sp_uuid pge_badge_nbr apeptestid_uniq
di `uniq1' - r(unique) // 357 bad SP-APEP combos got dropped
unique apeptestid_uniq
assert `uniq2'==r(unique) // confirm we didn't lose any pump tests
unique sp_uuid
di `uniq3' - r(unique) // 220 entire SPs are gone, were non-APEP SPs that got a once/future APEP meter
drop dup temp_keep temp_keep_max

	// Fourth: break remaining ties within SP-APEP by picking the dates that are closest
duplicates t sp_uuid pge_badge_nbr apeptestid_uniq, gen(dup)
tab dup
sort apeptestid_uniq sp_uuid sa_sp_start
br sp_uuid sa_uuid pge_badge_nbr sa_sp_start sa_sp_stop bill_dt_first bill_dt_last mtr_install_date ///
	mtr_remove_date test_date_stata apeptestid_uniq MATCH* dup ///
	if test_date_stata!=. & dup>0
gen temp_days_after_stop = test_date_stata-sa_sp_stop
gen temp_days_before_start = sa_sp_start-test_date_stata
gen temp_days_after_last = test_date_stata-bill_dt_last
gen temp_days_before_first = bill_dt_first-test_date_stata
foreach v of varlist temp_days_* {
	replace `v' = . if `v'<0
	egen `v'_min = min(`v'), by(sp_uuid pge_badge_nbr apeptestid_uniq)
}

unique sp_uuid pge_badge_nbr apeptestid_uniq
local uniq = r(unique)
	// Of SP/SAs that preceed APEP test date: keep only the most recent
drop if dup>0 & temp_days_after_stop!=. & temp_days_after_stop>temp_days_after_stop_min & ///
	temp_days_after_last!=. & temp_days_after_last>temp_days_after_last_min
drop if dup>0 & temp_days_after_stop!=. & temp_days_after_stop>temp_days_after_stop_min
drop if dup>0 & temp_days_after_last!=. & temp_days_after_last>temp_days_after_last_min
	// Of SP/SAs that follow APEP test date: keep only the most earliest
drop if dup>0 & temp_days_before_start!=. & temp_days_before_start>temp_days_before_start_min & ///
	temp_days_before_first!=. & temp_days_before_first>temp_days_before_first_min
drop if dup>0 & temp_days_before_start!=. & temp_days_before_start>temp_days_before_start_min
drop if dup>0 & temp_days_before_first!=. & temp_days_before_first>temp_days_before_first_min
unique sp_uuid pge_badge_nbr apeptestid_uniq
assert `uniq'==r(unique)

duplicates t sp_uuid pge_badge_nbr apeptestid_uniq, gen(dup2)
tab dup2
br sp_uuid sa_uuid pge_badge_nbr sa_sp_start sa_sp_stop bill_dt_first bill_dt_last mtr_install_date ///
	mtr_remove_date test_date_stata apeptestid_uniq MATCH* temp* dup2 ///
	if test_date_stata!=. & dup2>0
unique sp_uuid pge_badge_nbr apeptestid_uniq
local uniq = r(unique)
	// If either the before/after SA is missing billing data, drop it in favor of the after/before
drop if dup2>0 & temp_days_before_first_min==. & temp_days_after_last_min!=. & ///
	temp_days_after_last==temp_days_after_last_min
drop if dup2>0 & temp_days_after_last_min==. & temp_days_before_first_min!=. & ///
	temp_days_before_first==temp_days_before_first_min
unique sp_uuid pge_badge_nbr apeptestid_uniq
assert `uniq'==r(unique)

duplicates t sp_uuid pge_badge_nbr apeptestid_uniq, gen(dup3)
tab dup3
br sp_uuid sa_uuid pge_badge_nbr sa_sp_start sa_sp_stop bill_dt_first bill_dt_last mtr_install_date ///
	mtr_remove_date test_date_stata apeptestid_uniq MATCH* temp* dup3 ///
	if test_date_stata!=. & dup3>0
unique sp_uuid pge_badge_nbr apeptestid_uniq
local uniq = r(unique)
	// If a good before and after option exists, pick the closest date-wise
drop if dup3>0 & temp_days_after_stop_min!=. & temp_days_before_start_min!=. & ///
	temp_days_after_last_min!=. & temp_days_before_first_min!=. & ///
	temp_days_after_stop_min<temp_days_before_start_min & ///
	temp_days_after_last_min<temp_days_before_first_min & ///
	temp_days_before_start==temp_days_before_start_min & ///
	temp_days_before_first==temp_days_before_first_min
drop if dup3>0 & temp_days_after_stop_min!=. & temp_days_before_start_min!=. & ///
	temp_days_after_last_min!=. & temp_days_before_first_min!=. & ///
	temp_days_after_stop_min>temp_days_before_start_min & ///
	temp_days_after_last_min>temp_days_before_first_min & ///
	temp_days_after_stop==temp_days_after_stop_min & ///
	temp_days_after_last==temp_days_after_last_min
unique sp_uuid pge_badge_nbr apeptestid_uniq
assert `uniq'==r(unique)
	
duplicates t sp_uuid pge_badge_nbr apeptestid_uniq, gen(dup4)
tab dup4
br sp_uuid sa_uuid pge_badge_nbr sa_sp_start sa_sp_stop bill_dt_first bill_dt_last mtr_install_date ///
	mtr_remove_date test_date_stata apeptestid_uniq MATCH* temp* dup4 ///
	if test_date_stata!=. & dup4>0
unique sp_uuid pge_badge_nbr apeptestid_uniq
local uniq = r(unique)
	// Same idea, but less strict on missings
drop if dup4>0 & temp_days_after_stop_min!=. & temp_days_before_start_min!=. & ///
	temp_days_after_last_min!=. & temp_days_before_first_min!=. & ///
	temp_days_after_stop_min<temp_days_before_start_min & ///
	temp_days_after_last_min<temp_days_before_first_min & ///
	((temp_days_before_start==temp_days_before_start_min | temp_days_before_start==.) & temp_days_after_stop!=.) & ///
	((temp_days_before_first==temp_days_before_first_min | temp_days_before_first==.) & temp_days_after_last!=.)
drop if dup4>0 & temp_days_after_stop_min!=. & temp_days_before_start_min!=. & ///
	temp_days_after_last_min!=. & temp_days_before_first_min!=. & ///
	temp_days_after_stop_min>temp_days_before_start_min & ///
	temp_days_after_last_min>temp_days_before_first_min & ///
	((temp_days_after_stop==temp_days_after_stop_min | temp_days_after_stop==.) & temp_days_before_start!=.) & ///
	((temp_days_after_last==temp_days_after_last_min | temp_days_after_last==.) & temp_days_before_first!=.)
unique sp_uuid pge_badge_nbr apeptestid_uniq
assert `uniq'==r(unique)
	
duplicates t sp_uuid pge_badge_nbr apeptestid_uniq, gen(dup5)
tab dup5
br sp_uuid sa_uuid pge_badge_nbr sa_sp_start sa_sp_stop bill_dt_first bill_dt_last mtr_install_date ///
	mtr_remove_date test_date_stata apeptestid_uniq MATCH* temp* dup5 ///
	if test_date_stata!=. & dup5>0		
	// Same idea, but allowing one set of dates to be missing
gen temp_to_drop = 0	
replace temp_to_drop = 1 if dup5>0 & ///
	((temp_days_after_stop_min!=. & temp_days_before_start_min!=. & ///
	temp_days_after_stop_min<temp_days_before_start_min & ///
	((temp_days_before_start==temp_days_before_start_min | temp_days_before_start==.) & temp_days_after_stop!=.) ///
	) |  ///
	(temp_days_after_last_min!=. & temp_days_before_first_min!=. & ///
	temp_days_after_last_min<temp_days_before_first_min & ///
	((temp_days_before_first==temp_days_before_first_min | temp_days_before_first==.) & temp_days_after_last!=.) ///
	))
replace temp_to_drop = 1 if dup5>0 & ///
	((temp_days_after_stop_min!=. & temp_days_before_start_min!=. & ///
	temp_days_after_stop_min>temp_days_before_start_min & ///
	((temp_days_after_stop==temp_days_after_stop_min | temp_days_after_stop==.) & temp_days_before_start!=.) ///
	) | ///
	(temp_days_after_last_min!=. & temp_days_before_first_min!=. & ///
	temp_days_after_last_min>temp_days_before_first_min & ///
	((temp_days_after_last==temp_days_after_last_min | temp_days_after_last==.) & temp_days_before_first!=.) ///
	))
egen temp_to_drop_min = min(temp_to_drop), by(sp_uuid pge_badge_nbr apeptestid_uniq)
unique sp_uuid pge_badge_nbr apeptestid_uniq
local uniq = r(unique)
drop if temp_to_drop_min==0 & temp_to_drop==1
unique sp_uuid pge_badge_nbr apeptestid_uniq
assert `uniq'==r(unique)	
	
duplicates t sp_uuid pge_badge_nbr apeptestid_uniq, gen(dup6)
tab dup6
br sp_uuid sa_uuid pge_badge_nbr sa_sp_start sa_sp_stop bill_dt_first bill_dt_last mtr_install_date ///
	mtr_remove_date test_date_stata apeptestid_uniq MATCH* temp* dup6 ///
	if test_date_stata!=. & dup6>0		
	// At this point, I'm just picking one. Dates aren't matching and billing data is mostly missing, so this
	// is a low-stakes decision I think
unique sp_uuid pge_badge_nbr apeptestid_uniq
local uniq = r(unique)
duplicates drop sp_uuid pge_badge_nbr apeptestid_uniq, force
unique sp_uuid pge_badge_nbr apeptestid_uniq
assert `uniq'==r(unique)
assert r(unique)==r(N)
	
	// Clean up some messy temp variables
drop dup* temp*

	// Fifth: For APEP tests matched to multiple SPs, keep the one with contemporaneous meter dates
assert test_date_stata!=. & apeptestid_uniq!=.
assert sp_uuid!="" & sa_uuid!=""
duplicates t apeptestid_uniq, gen(dup)
tab dup
br sp_uuid sa_uuid pge_badge_nbr sa_sp_start sa_sp_stop bill_dt_first bill_dt_last mtr_install_date ///
	mtr_remove_date test_date_stata apeptestid_uniq MATCH* dup ///
	if dup>0		
gen temp_keep = inrange(test_date_stata,mtr_install_date,mtr_remove_date)
egen temp_keep_max = max(temp_keep), by(apeptestid_uniq)
egen temp_keep_min = min(temp_keep), by(apeptestid_uniq)	
unique apeptestid_uniq
local uniq1 = r(unique)
unique sp_uuid
local uniq2 = r(unique)
drop if dup>0 & temp_keep==0 & temp_keep_max==1
unique apeptestid_uniq
assert `uniq1'==r(unique)
unique sp_uuid
di `uniq2' - r(unique) // 223 entire SPs are gone, which apparently had a once/future APEP meter 
drop dup* temp*
	
	// Sixth: Break ties in APEP test id based on modal SP at each pge_badge_nbr
duplicates t apeptestid_uniq, gen(dup)
tab dup
br sp_uuid sa_uuid pge_badge_nbr sa_sp_start sa_sp_stop bill_dt_first bill_dt_last mtr_install_date ///
	mtr_remove_date test_date_stata apeptestid_uniq MATCH* dup ///
	if dup>0		
egen temp_max_dup = max(dup), by(pge_badge_nbr)
tab dup temp_max_dup
sort pge_badge_nbr
br sp_uuid sa_uuid pge_badge_nbr sa_sp_start sa_sp_stop bill_dt_first bill_dt_last mtr_install_date ///
	mtr_remove_date test_date_stata apeptestid_uniq dup ///
	if temp_max_dup>0
egen temp_sp_mode = mode(sp_uuid), by(pge_badge_nbr)	
unique apeptestid_uniq
local uniq1 = r(unique)
unique sp_uuid
local uniq2 = r(unique)
drop if dup>0 & temp_max_dup>0 & temp_sp_mode!="" & sp_uuid!=temp_sp_mode
unique apeptestid_uniq
assert `uniq1'==r(unique)
unique sp_uuid
di `uniq2' - r(unique) // 5 entire SPs are gone, which apparently had a once/future APEP meter 
drop dup* temp*
	
	// Seventh: Break ties in APEP test id based on whether SAs were/weren't on AG tariffs
duplicates t apeptestid_uniq, gen(dup)
tab dup
br sp_uuid sa_uuid pge_badge_nbr sa_sp_start sa_sp_stop bill_dt_first bill_dt_last mtr_install_date ///
	mtr_remove_date test_date_stata apeptestid_uniq dup ///
	if dup>0		
egen temp_max_dup = max(dup), by(pge_badge_nbr)
tab dup temp_max_dup
assert temp_max_dup==dup
preserve
keep if dup>0
keep sp_uuid sa_uuid pge_badge_nbr
duplicates drop
joinby sa_uuid using "$dirpath_data/pge_cleaned/billing_data_20180719.dta"
assert inlist(sp_uuid,sp_uuid1,sp_uuid2,sp_uuid3)
gen ag_rate = substr(rt_sched_cd,1,2)=="AG" | substr(rt_sched_cd,1,3)=="HAG"
keep sp_uuid sa_uuid pge_badge_nbr ag_rate
gen nbills = 1
collapse (sum) nbills, by(sp_uuid sa_uuid pge_badge_nbr ag_rate) fast
unique sp_uuid sa_uuid pge_badge_nbr 
assert r(unique)==r(N) // ever SP/SA is either all ag or all non-ag
drop nbills
tempfile sasp_dups
save `sasp_dups'
restore
merge m:1 sp_uuid sa_uuid pge_badge_nbr using `sasp_dups'
gen temp_keep = ag_rate==1
egen temp_keep_max = max(temp_keep), by(apeptestid_uniq)
egen temp_keep_min = min(temp_keep), by(apeptestid_uniq)
unique apeptestid_uniq
local uniq1 = r(unique)
unique sp_uuid
local uniq2 = r(unique)
drop if dup>0 & temp_max_dup>0 & temp_keep==0 & temp_keep_min==0 & temp_keep_max==1
unique apeptestid_uniq
assert `uniq1'==r(unique)
unique sp_uuid
di `uniq2' - r(unique) // 6 entire SPs are gone, which apparently had a once/future APEP meter 
drop dup* temp* ag_rate _merge

	// Eighth: Break ties in APEP test id based on the least inconsistent date
duplicates t apeptestid_uniq, gen(dup)
tab dup
br sp_uuid sa_uuid pge_badge_nbr sa_sp_start sa_sp_stop bill_dt_first bill_dt_last mtr_install_date ///
	mtr_remove_date test_date_stata apeptestid_uniq dup ///
	if dup>0		
gen temp_in_sasp_range = inrange(test_date_stata,sa_sp_start,sa_sp_stop)
gen temp_in_bill_range = inrange(test_date_stata,bill_dt_first,bill_dt_last)
gen temp_in_meter_range = inrange(test_date_stata,mtr_install_date,mtr_remove_date)	
egen temp_ranges = rowtotal(temp_in_*_range)	
egen temp_ranges_max = max(temp_ranges), by(apeptestid_uniq)
unique apeptestid_uniq
local uniq1 = r(unique)
unique sp_uuid
local uniq2 = r(unique)
drop if dup>0 & temp_ranges<temp_ranges_max
unique apeptestid_uniq
assert `uniq1'==r(unique)
unique sp_uuid
di `uniq2' - r(unique) // 3 entire SPs are gone, which apparently had a once/future APEP meter 
drop dup* temp*
	
	// Ninth: Break ties in APEP test id based on the least inconsistent date (manually)
duplicates t apeptestid_uniq, gen(dup)
tab dup
br sp_uuid sa_uuid pge_badge_nbr sa_sp_start sa_sp_stop bill_dt_first bill_dt_last mtr_install_date ///
	mtr_remove_date test_date_stata apeptestid_uniq dup ///
	if dup>0		
unique apeptestid_uniq
local uniq1 = r(unique)
unique sp_uuid
local uniq2 = r(unique)
drop if sp_uuid=="6121910009" & sa_uuid=="0833420564" & apeptestid_uniq==2859
drop if sp_uuid=="2400910094" & sa_uuid=="7259720409" & apeptestid_uniq==3283
drop if sp_uuid=="7387210054" & sa_uuid=="3000320679" & apeptestid_uniq==3283
drop if sp_uuid=="1985110110" & sa_uuid=="9744220630" & apeptestid_uniq==4215
drop if sp_uuid=="2970010085" & sa_uuid=="9696820270" & apeptestid_uniq==14612
unique apeptestid_uniq
assert `uniq1'==r(unique)
unique sp_uuid
di `uniq2' - r(unique) // 3 entire SPs are gone, which apparently had a once/future APEP meter 
drop dup* 

	// Confirm uniqueness
unique apeptestid_uniq
assert r(unique)==r(N)	
drop MATCH*
assert sp_not_in_cust_detail==0
drop sp_not_in_cust_detail


** Save outcome of de-dupified merge
preserve
keep sp_uuid sa_uuid pge_badge_nbr apeptestid apeptestid_uniq test_date_stata
compress
save "$dirpath_data/merged/sp_apeptest_xwalk.dta", replace
restore


** Merge back in APEP project data
joinby pge_badge_nbr using "$dirpath_data/pge_cleaned/pump_test_project_data.dta", ///
	unmatched(both)
unique sp_uuid
local uniq = r(unique)
unique sp_uuid if _merge==3 & merge_apep_test==3 
di r(unique)/`uniq'	// 708 SPs merge to an APEP project
rename _merge merge_apep_proj
tab merge_apep_proj // 11 project observatios that don't merge
unique pge_badge_nbr if merge_apep_proj==2 // 11 badge numbers that don't merge

preserve 
keep if merge_apep_proj==2
keep pge_badge_nbr
duplicates drop
merge 1:m pge_badge_nbr using "$dirpath_data/pge_cleaned/xwalk_sp_meter_date_20180719.dta"
assert _merge!=3 // this confirms that all 11 badge numbers are missing from the SP-meter xwalk
restore

preserve 
keep if merge_apep_proj==2
keep pge_badge_nbr
duplicates drop
merge 1:m pge_badge_nbr using "$dirpath_data/pge_cleaned/pump_test_data.dta"
assert _merge!=3 // this confirms that these 11 badge numbers are also missing from the APEP test dataset
restore

preserve 
keep if merge_apep_proj==2
keep pge_badge_nbr
duplicates drop
merge 1:m pge_badge_nbr using "$dirpath_data/pge_cleaned/pump_test_project_data.dta"
tab _merge flag_apep_mismatch
assert flag_apep_mismatch==1 if _merge==3 // these 11 badge numbers were already flagged as problematic
restore

drop if merge_apep_proj==2 // drop these 11 unusable APEP projects


** Redo join at the SP level, since APEP project data might not have contemporaneously correct meters
preserve
keep if merge_apep_proj==3
keep sp_uuid pge_badge_nbr apep_proj_id date_proj_finish date_test_pre date_test_post date_test_subs ///
	 flag_date_problem flag_subs_after_proj flag_subs_before_proj est_savings_kwh_yr subsidy_proj ///
	 iswell run flag_apep_mismatch
rename pge_badge_nbr pge_badge_nbr_PROJ	
duplicates drop
tempfile proj_sp
save `proj_sp'
restore
drop merge_apep_proj apep_proj_id date_proj_finish date_test_pre date_test_post date_test_subs ///
	flag_date_problem flag_subs_after_proj flag_subs_before_proj est_savings_kwh_yr subsidy_proj ///
	iswell run flag_apep_mismatch
duplicates drop
joinby sp_uuid using `proj_sp', unmatched(both)
assert _merge!=2
tab _merge
rename _merge merge_apep_proj	
unique sp_uuid 
local uniq = r(unique)
unique sp_uuid if merge_apep_proj==3
di r(unique)/`uniq' // 5.5% of SPs have projects


** True-up APEP tests with APEP project test dates
	
	// Construct new versions of APEP project variables linked to the pump test date
gen apep_date_test_pre = test_date_stata==date_test_pre & test_date_stata!=.
gen apep_date_test_post = test_date_stata==date_test_post & test_date_stata!=.
gen apep_date_test_subs = test_date_stata==date_test_subs & test_date_stata!=.
egen apep_date_test_max = rowmax(apep_date_test_*)
egen apep_date_test_max2 = max(apep_date_test_max), by(apep_proj_id)
sum apep_date_test_* if merge_apep_proj==3 // 
unique apep_proj_id if merge_apep_proj==3
local uniq = r(unique)
unique apep_proj_id if merge_apep_proj==3 & apep_date_test_max2==1
di r(unique)/`uniq' // ALL BUT 6 APEP projects have A matching SP-DATE!!!

	// Duplicates drop if no dates match 
duplicates t sp_uuid apep_proj_id, gen(dup)
tab dup merge_apep_proj
br sp_uuid test_date_stata apeptestid_uniq apep_proj_id date_proj_finish-date_test_subs ///
	apep_date* flag_apep_mismatch dup if dup>0 & merge_apep_proj==3
gen temp = apep_date_test_max==1
egen temp_max = max(temp), by(sp_uuid apep_proj_id)
egen temp_min = min(temp), by(sp_uuid apep_proj_id)
tab temp temp_max
egen temp_max2 = max(temp), by(sp_uuid apeptestid_uniq) // to make sure we don't lose any tests outright
unique sp_uuid apeptestid_uniq 
local uniq1 = r(unique)
unique sp_uuid apep_proj_id
local uniq2 = r(unique)
drop if dup>0 & temp==0 & temp_max==1 & temp_max2==1
unique sp_uuid apeptestid_uniq 
assert r(unique)==`uniq1'
unique sp_uuid apep_proj_id
assert r(unique)==`uniq2'
drop dup temp*

	// Remaining dups: drop project test date variables and project ID, then duplicates drop
duplicates t sp_uuid apep_proj_id, gen(dup)
tab dup merge_apep_proj
br sp_uuid test_date_stata apeptestid_uniq apep_proj_id date_proj_finish-date_test_subs ///
	apep_date* flag_apep_mismatch dup if dup>5 & merge_apep_proj==3
	// Looks pretty good, not a ton of low-hanging fruit
drop dup pge_badge_nbr_PROJ


** Save dataset at the APEP project level, with merged SPs and dummies for dates matching
preserve
keep sp_uuid apep_proj_id-apep_date_test_max2
keep if apep_proj_id!=.
duplicates drop
foreach v of varlist apep_date_test_pre apep_date_test_post apep_date_test_subs {
	egen temp = max(`v'), by(apep_proj_id)
	replace `v' = temp
	drop temp
}
drop apep_date_test_max apep_date_test_max2
duplicates drop
unique apep_proj_id
assert r(unique)==r(N)
egen apep_date_test_max = rowmax(apep_date_test*)
tab flag_apep_mismatch apep_date_test_max
assert apep_date_test_max==1-flag_apep_mismatch
drop apep_date_test_max
la var apep_date_test_pre "Dummy for matches to APEP test on pre date"
la var apep_date_test_post "Dummy for matches to APEP test on post date"
la var apep_date_test_subs "Dummy for matches to APEP test on subs date"
order sp_uuid apep_proj_id date* apep* est_savings subsidy
sort apep_proj_id
compress
save "$dirpath_data/merged/apep_projects_sp_merge.dta", replace
restore	


** 	Make unique by SP-APEP test ID, reshaping duplicate projects wide

unique sp_uuid apeptestid_uniq apep_proj_id
assert r(unique)==r(N)

duplicates t sp_uuid apeptestid_uniq, gen(dup)
tab dup
br sp_uuid test_date_stata apeptestid_uniq apep_proj_id date_proj_finish-date_test_subs ///
	flag_date_problem flag_subs_after_proj flag_subs_before_proj est_savings_kwh_yr subsidy_proj ///
	iswell run flag_apep_mismatch if dup>0

	// Discard APEP project ID, and make a new project ID based on savings/subsidy amounts
drop apep_proj_id
egen apep_proj_group = group(est_saving subsidy_proj sp_uuid)
egen apep_proj_group2 = group(est_saving subsidy_proj)
egen apep_proj_group3 = group(est_saving subsidy_proj date_proj_finish)
assert apep_proj_group==apep_proj_group2	
assert apep_proj_group==apep_proj_group3	
drop apep_proj_group2 apep_proj_group3

	// For multiple subsidized test dates, take the max for each specific apeptestid_uniq
egen temp = max(apep_date_test_subs), by(sp_uuid apeptestid_uniq apep_proj_group)
replace apep_date_test_subs = temp
drop temp date_test_subs
duplicates drop
	
	// For the pre/post flags, take the minimum w/in each apep_proj_group
foreach v of varlist flag_subs_after_proj flag_subs_before_proj flag_apep_mismatch {
	egen temp = min(`v'), by(sp_uuid apep_proj_group)
	replace `v' = temp
	drop temp
}
duplicates drop

	// Remaining dups are the "iswell" and "run" variables, which seem redundant to me (we'll know ///
	// if it's a well based on the APEP test variables...)
drop iswell run
drop apep_date_test_max* // also these variables 
duplicates drop
drop dup*

	// Confirm uniqueness
unique sp_uuid apeptestid_uniq apep_proj_group
assert r(unique)==r(N)

	// Reshape wide
duplicates t sp_uuid apeptestid_uniq, gen(dup)
tab dup // 16 SP-tests with multiple project groups
assert dup<2
br sp_uuid test_date_stata apeptestid_uniq date_proj_finish date_test_pre date_test_post ///
	flag_date_problem flag_subs_after_proj flag_subs_before_proj est_savings_kwh_yr subsidy_proj ///
	flag_apep_mismatch if dup>0 // 4 SPs had multiple APEP projects
gsort sp_uuid apeptestid_uniq date_proj_finish -subsidy_proj
rename apep_date_test_* proj_date_test_*
gen to_drop = dup==1 & sp_uuid==sp_uuid[_n-1] & apeptestid_uniq==apeptestid_uniq[_n-1]
foreach v of varlist date_proj_finish date_test_pre date_test_post flag_date_problem flag_subs_after_proj ///
	flag_subs_before_proj est_savings_kwh_yr subsidy_proj flag_apep_mismatch proj_date_test_pre ///
	proj_date_test_post proj_date_test_subs	{
	gen `v'2 = .
	replace `v'2 = `v'[_n+1] if dup==1 & to_drop==0
}
format %td date_proj_finish2 date_test_pre2 date_test_post2 
unique sp_uuid apeptestid_uniq
local uniq = r(unique)
drop if to_drop==1
unique sp_uuid apeptestid_uniq
assert r(unique)==`uniq'
assert r(unique)==r(N)	
	

** Clean up and label
drop dup to_drop apep_proj_group
assert inlist(flag_apep_mismatch,0,.) & inlist(flag_apep_mismatch2,0,.)
drop flag_apep_mismatch*
la var date_proj_finish2 "Date of project finish (proj #2)"
la var date_test_pre2 "Date of pre-project pump test (for project) (proj #2)"
la var date_test_post2 "Date of post-project pum test (for project) (proj #2)"
la var flag_date_problem2 "Date inconsisency (pre after post; pre after finish)(proj #2)"
la var flag_subs_after_proj2 "Subsizided pump test AFTER project, likely not compliers (proj #2)"
la var flag_subs_before_proj2 "Subsidized pump test BEFORE project, maybe inrdirect influence (proj #2)"
la var est_savings_kwh_yr2 "Engineering estimate(?) of gross kWh savings in first year (proj #2)"
la var subsidy_proj2 "Subsidy offered for project (proj #2)"
la var proj_date_test_pre "Dummy if APEP test date matches project pre test date"
la var proj_date_test_post "Dummy if APEP test date matches project post test date"
la var proj_date_test_subs "Dummy if APEP test date matches a project subsidized test date"
la var proj_date_test_pre2 "Dummy if APEP test date matches project pre test date (proj #2)"
la var proj_date_test_post2 "Dummy if APEP test date matches project post test date (proj #2)"
la var proj_date_test_subs2 "Dummy if APEP test date matches a project subsidized test date (proj #2)"

egen temp = max(merge_apep_proj), by(sp_uuid)
assert merge_apep_proj==temp
gen apep_proj_count = 0
replace apep_proj_count = 1 if merge_apep_proj==3
replace apep_proj_count = 2 if merge_apep_proj==3 & date_proj_finish2!=.
preserve
keep sp_uuid apep_proj_count
duplicates drop
tab apep_proj_count // 12,142 with none, 704 with 1, 4 with 2
restore
la var apep_proj_count "Number of APEP projects linked to SP"
drop temp merge_apep_proj

assert merge_sp_meter_xwalk==3
drop merge_sp_meter_xwalk

assert merge_apep_test==3
drop merge_apep_test

la var test_date_stata "APEP test date"
la var apeptestid_uniq "Unique ID for observations in APEP test dataset"
	
	
** Save messy but finalized (for now) dataset of SPs-tests-projects
duplicates drop
unique sp_uuid apeptestid_uniq
assert r(unique)==r(N)
compress
save "$dirpath_data/merged/sp_apep_proj_merged.dta", replace

}

*******************************************************************************
*******************************************************************************

** 2. Create anonymized APEP test/project dataset for determining APEP eligibility/subsidies
{

** Start with harmonized pump test and project dataset
use "$dirpath_data/merged/sp_apep_proj_merged.dta", clear

** Create anonymized SP ID
preserve
keep sp_uuid 
duplicates drop
gen temp = runiform()
sort temp
gen sp_id_anon = _n
drop temp
tempfile sp_anon
save `sp_anon'
restore
merge m:1 sp_uuid using `sp_anon', nogen keep(1 3) 
la var sp_id_anon "Anonymized service point ID (think of this as the exact location of the pump)"

** Drop all PGE electricity variables
drop prsn_uuid-mtr_remove_date

** Merge in APEP-specific variables
merge 1:1 customertype farmtype waterenduse apeptestid using ///
	"$dirpath_data/pge_cleaned/pump_test_data.dta", keep(3) nogen
	
** Drop non-essential and/or identifying variables
drop farmtype apeptestid crop pumplatnew pumplongnew pge_badge_nbr mtrsn ratesch ///
	mtrmake pumpmke gearheadmake apep_proj_count

** Verify no identifying information in notes
replace notes = trim(itrim(notes))
br notes if notes!="" // looks fine, but i'm dropping it anyways
drop notes
br 

** Verify no identifying information in afterloadoutofrange
tab afterloadoutofrange // was a dropdown menu

** Verfity no identifying information in memo
replace memo = trim(itrim(memo))
br memo if memo!="" // not as clean so I'm dropping it
drop memo
br

** Sort by anonymized APEP test id
sort apeptestid_uniq
unique apeptestid_uniq
assert r(unique)==r(N)

** Keep year of test date
gen test_year = year(test_date_stata)
la var test_year "Year of pump test"
gen test_month = month(test_date_stata)
la var test_month "Month of pump test"
drop test_date_stata

** Create re-anonymized resoted identifier and crosswalk
gen temp = runiform()
sort temp
gen apeptestid_uniq_anon = _n
la var apeptestid_uniq_anon "Anonymized unique ID for observatiosn in APEP test dataset"
sort apeptestid_uniq
preserve 
keep apeptestid_uniq apeptestid_uniq_anon
save "$dirpath_data/merged/apep_proj_merged_anon_xwalk.dta", replace
restore
drop apeptestid_uniq temp

** Keep pump test project indicators and subsidies only, not dates
/*
br customertype-proj_date_test_subs economicanalysis run nbr_of_runs totlift mtrload kwhaf af24hrs ///
	ope ope_ideal ope_round ope_after flow_gpm flow_gpm_after pwl pwl_after hp hp_after afperyr afperyr_after ///
	tdh tdh_after annualcost* if flag_date_problem==. ///
	& proj_date_test_post==1 & ///
	proj_date_test_subs==1
gen annualcost_diff = annualcost_after - annualcost
sum annualcost_diff if proj_date_test_subs==1, detail
*/
gen proj_year = year(date_proj_finish)
la var proj_year "Year of project finish"
gen proj_month = month(date_proj_finish)
la var proj_month "Month of project finish"
gen proj_year2 = year(date_proj_finish)
la var proj_year2 "Year of project 2 finish"
gen proj_month2 = month(date_proj_finish)
la var proj_month2 "Month of project 2 finish"
drop date_proj_finish date_test_pre date_test_post date_proj_finish2 date_test_pre2 date_test_post2
gen linked_to_project = flag_date_problem!=.
la var linked_to_project "Indicator for pump tests at a pump that ever received a subsidized APEP project"

** Obscure non-ag enduses
gen end_use_ag = waterenduse=="agriculture" | waterenduse=="irrigation"
la var end_use_ag "Dummy for water end use listed as 'ag' or 'irrigation' "
drop waterenduse

** Order and sort
order sp_id_anon apeptestid_uniq_anon test_year test_month linked_to_project customertype end_use_ag ///
	flag_date_problem-proj_date_test_subs proj_year proj_month flag_date_problem2-proj_date_test_subs2 ///
	proj_year2 proj_month2 economicanalysis run nbr_of_runs pwl pwl_after tdh tdh_after flow_gpm ///
	flow_gpm_after ope ope_after hp hp_after kw_input kw_input_after af24hrs af24hrs_after ///
	kwhaf kwhaf_after avgcost_kwh avgcost_kwh_after afperyr afperyr_after kwhperyr kwhperyr_after ///
	annualcost annualcost_after water_hp water_hp_after ope_ideal ope_round totlift mtrload 
sort sp_id_anon apeptestid_uniq_anon

** Drop seemingly meaningless subsidized test indicators
drop proj_date_test_subs proj_date_test_subs2

** Save
unique apeptestid_uniq_anon
assert r(unique)==r(N)
compress
save "$dirpath_data/merged/apep_proj_merged_anon.dta", replace
save "$dirpath_data/merged/data.dta", replace


}

*******************************************************************************
*******************************************************************************

** 3. Compare SP vs APEP coordinates, and groundwater levels, and figure out missing drawdown
{

** Start with SP-APEP merge
use "$dirpath_data/merged/sp_apep_proj_merged.dta", clear

** Merge in SP-level GIS data
merge m:1 sp_uuid using "$dirpath_data/pge_cleaned/sp_premise_gis.dta", keep(1 3) nogen ///
	keepusing(prem_lat prem_long bad_geocode_flag missing_geocode_flag wdist_id county_fips ///
	basin_id basin_sub_id)
foreach v of varlist bad_geocode_flag missing_geocode_flag wdist_id county_fips basin_id basin_sub_id {
	rename `v' `v'SP
}

** Merge in pump-level GIS data
merge 1:1 apeptestid customertype farmtyp waterenduse test_date_stata using ///
	"$dirpath_data/pge_cleaned/pump_test_data.dta", keep(1 3) nogen //keepusing(crop totlift pwl swl rwl)
merge m:1 apeptestid crop using "$dirpath_data/pge_cleaned/apep_pump_gis.dta", keep(1 3) nogen ///
	keepusing(pumplatnew pumplongnew latlon_group bad_geocode_flag wdist_id county_fips basin_id basin_sub_id)

** Create numeric groups	
egen temp_basin_id_group = group(basin_id)
egen temp_basin_sub_id_group = group(basin_sub_id)
egen temp_basin_idSP_group = group(basin_idSP)
egen temp_basin_sub_idSP_group = group(basin_sub_idSP)

** Calculate distance between the 2 competing geocodes
geodist prem_lat prem_long pumplatnew pumplongnew, miles gen(sp_miles_from_pump)
assert sp_miles_from_pump==. if missing_geocode_flagSP==1

hist sp_miles_from_pump
sum sp_miles_from_pump, detail // median is 10.6 miles, which is Not Great Bob!

hist sp_miles_from_pump if bad_geocode_flagSP==0
sum sp_miles_from_pump if bad_geocode_flagSP==0, detail

hist sp_miles_from_pump if bad_geocode_flagSP==0 & bad_geocode_flag==0
sum sp_miles_from_pump if bad_geocode_flagSP==0 & bad_geocode_flag==0, detail

egen temp_tagSP = tag(sp_uuid)
hist sp_miles_from_pump if bad_geocode_flagSP==0 & bad_geocode_flag==0 & temp_tagSP // not really an over/under sampling issue
sum sp_miles_from_pump if bad_geocode_flagSP==0 & bad_geocode_flag==0 & temp_tagSP, detail

hist sp_miles_from_pump if bad_geocode_flagSP==0 & bad_geocode_flag==0 & temp_tagSP & ///
	customertype=="Individ Farms"
sum sp_miles_from_pump if bad_geocode_flagSP==0 & bad_geocode_flag==0 & temp_tagSP & ///
	customertype=="Individ Farms", detail // removing water districts, meadin drops to 8.4 miles

hist sp_miles_from_pump if bad_geocode_flagSP==0 & bad_geocode_flag==0 & temp_tagSP & ///
	customertype=="Individ Farms" & waterenduse=="agriculture"
sum sp_miles_from_pump if bad_geocode_flagSP==0 & bad_geocode_flag==0 & temp_tagSP & ///
	customertype=="Individ Farms" & waterenduse=="agriculture", detail // same
	
hist sp_miles_from_pump if bad_geocode_flagSP==0 & bad_geocode_flag==0 & temp_tagSP & ///
	customertype=="Individ Farms" & waterenduse=="agriculture" & subsidy_proj!=.
sum sp_miles_from_pump if bad_geocode_flagSP==0 & bad_geocode_flag==0 & temp_tagSP & ///
	customertype=="Individ Farms" & waterenduse=="agriculture" & subsidy_proj!=., detail 
	// keeping only farms that received projects, median falls to 5.9 miles

** See if 2 competing geocodes are assign to the same polygons
gen temp_county_match = county_fipsSP==county_fips
gen temp_basin_match = basin_idSP==basin_id
gen temp_basin_sub_match = basin_idSP==basin_id & basin_sub_idSP==basin_sub_id
gen temp_wdist_match = wdist_idSP==wdist_id

sum temp_*_match // ehhhhhh Not Great Bob
sum temp_*_match if temp_tagSP
sum temp_*_match if temp_tagSP & bad_geocode_flagSP==0
sum temp_*_match if temp_tagSP & bad_geocode_flagSP==0 & bad_geocode_flag==0
sum temp_*_match if temp_tagSP & bad_geocode_flagSP==0 & bad_geocode_flag==0 & ///
	customertype=="Individ Farms"
sum temp_*_match if temp_tagSP & bad_geocode_flagSP==0 & bad_geocode_flag==0 & ///
	customertype=="Individ Farms" & waterenduse=="agriculture"
sum temp_*_match if temp_tagSP & bad_geocode_flagSP==0 & bad_geocode_flag==0 & ///
	customertype=="Individ Farms" & waterenduse=="agriculture" & subsidy_proj!=.
	// Water Districts NEVER get over 50%, dammit.

** Create month and quarter variables for test date
gen modate = ym(year(test_date_stata),month(test_date_stata))
format %tm modate
gen quarter = .
replace quarter = 1 if inlist(month(test_date_stata),1,2,3) 	
replace quarter = 2 if inlist(month(test_date_stata),4,5,6) 	
replace quarter = 3 if inlist(month(test_date_stata),7,8,9) 	
replace quarter = 4 if inlist(month(test_date_stata),10,11,12)
gen qtr = yq(year(test_date_stata),quarter)
format %tq qtr
hist modate, width(1) freq
hist qtr, width(1) freq

** Merge in monthly and quarterly groundwater depths at the SP-level
merge m:1 sp_uuid modate using "$dirpath_data/groundwater/groundwater_depth_sp_month_rast.dta", ///
	keep(1 3) nogen keepusing(gw_*)
merge m:1 sp_uuid qtr using "$dirpath_data/groundwater/groundwater_depth_sp_quarter_rast.dta", ///
	keep(1 3) nogen keepusing(gw_*)
rename gw_* gw_*SP

** Merge in monthly and quarterly groundwater depths at the SP-level
merge m:1 latlon_group modate using "$dirpath_data/groundwater/groundwater_depth_apep_month_rast.dta", ///
	keep(1 3) nogen keepusing(gw_*)
merge m:1 latlon_group qtr using "$dirpath_data/groundwater/groundwater_depth_apep_quarter_rast.dta", ///
	keep(1 3) nogen keepusing(gw_*)

	
***	
** Correlations, full sample
***
	
** Correlations with standing water level (APEP lat/lons)
foreach v of varlist gw_rast_depth_???_?s {
	qui correlate swl `v' // weak correlations w/ rasters, qtr > mth, 1 > 2 > 3 
	di "`v'  " round(r(rho),0.001)
}
foreach v of varlist gw_rast_depth_???_?b {
	qui correlate swl `v' // same
	di "`v'  " round(r(rho),0.001)
}
foreach v of varlist gw_???_bsn_mean? {
	qui correlate swl `v' // much weaker using basin avgs, so hooray for rasters???
	di "`v'  " round(r(rho),0.001)
}	
	
** Correlations with standing water level (SP lat/lons)
foreach v of varlist gw_rast_depth_???_?sSP {
	qui correlate swl `v' // same pattern, but better than APEP coordinates!
	di "`v'  " round(r(rho),0.001)
}
foreach v of varlist gw_rast_depth_???_?bSP {
	qui correlate swl `v' // same pattern, but better than APEP coordinates!
	di "`v'  " round(r(rho),0.001)
}
foreach v of varlist gw_???_bsn_mean?SP {
	qui correlate swl `v' // much weaker using basin avgs, still better than APEP coordinates, so hooray???
	di "`v'  " round(r(rho),0.001)
}

	
***	
** Correlations, good lat/lons 
***
	
** Correlations with standing water level (APEP lat/lons)
foreach v of varlist gw_rast_depth_???_?s {
	qui correlate swl `v' 
	di "`v'  " round(r(rho),0.001)
	qui correlate swl `v' if bad_geocode_flag==0
	di "`v'  " round(r(rho),0.001) // no difference
}
		
** Correlations with stnading water level (SP lat/lons)
foreach v of varlist gw_rast_depth_???_?sSP {
	qui correlate swl `v' 
	di "`v'  " round(r(rho),0.001)
	qui correlate swl `v' if bad_geocode_flag==0 & missing_geocode_flag==0
	di "`v'  " round(r(rho),0.001) // no difference
}

	
***	
** Correlations, good lat/lons, individual farms 
***
	
** Correlations with standing water level (APEP lat/lons)
foreach v of varlist gw_rast_depth_???_?s {
	qui correlate swl `v' 
	di "`v'  " round(r(rho),0.001)
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms"
	di "`v'  " round(r(rho),0.001) // slightly better
}
		
** Correlations with standing water level (SP lat/lons)
foreach v of varlist gw_rast_depth_???_?sSP {
	qui correlate swl `v' 
	di "`v'  " round(r(rho),0.001)
	qui correlate swl `v' if bad_geocode_flag==0 & missing_geocode_flag==0 & customertype=="Individ Farms"
	di "`v'  " round(r(rho),0.001) // slightly better
}


***	
** Correlations, good lat/lons, individual farms, ag end use 
***
	
** Correlations with standing water level (APEP lat/lons)
foreach v of varlist gw_rast_depth_???_?s {
	qui correlate swl `v' 
	di "`v'  " round(r(rho),0.001)
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" & waterenduse=="agriculture"
	di "`v'  " round(r(rho),0.001) // slightly better for quarterly
}
		
** Correlations with standing water level (SP lat/lons)
foreach v of varlist gw_rast_depth_???_?sSP {
	qui correlate swl `v' 
	di "`v'  " round(r(rho),0.001)
	qui correlate swl `v' if bad_geocode_flag==0 & missing_geocode_flag==0 & customertype=="Individ Farms" & waterenduse=="agriculture"
	di "`v'  " round(r(rho),0.001) // about the same
}


***	
** Correlations, good lat/lons, individual farms, ag end use, SP-APEP coords w/in 10 miles 
***
	
** Correlations with standing water level (APEP lat/lons)
foreach v of varlist gw_rast_depth_???_?s {
	qui correlate swl `v' 
	di "`v'  " round(r(rho),0.001)
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" & waterenduse=="agriculture" & sp_miles_from_pump<10
	di "`v'  " round(r(rho),0.001) // much better, except for gw3
}

** Correlations with standing water level (SP lat/lons)
foreach v of varlist gw_rast_depth_???_?sSP {
	qui correlate swl `v' 
	di "`v'  " round(r(rho),0.001)
	qui correlate swl `v' if bad_geocode_flag==0 & missing_geocode_flag==0 & customertype=="Individ Farms" & waterenduse=="agriculture" & sp_miles_from_pump<10
	di "`v'  " round(r(rho),0.001) // slightly better, except for gw3
}


***	
** Correlations, good lat/lons, individual farms, ag end use, SP-APEP coords w/in 10 miles
** nearest gw measurement within 20 miles 
***
	
** Correlations with stnading water level (APEP lat/lons)
foreach v of varlist gw_rast_depth_???_?s {
	qui correlate swl `v' 
	di "`v'  " round(r(rho),0.001)
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<10
	di "`v'  " round(r(rho),0.001) 
	local vd = substr(subinstr("`v'","_depth_","_dist_",1),1,18)
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<10 & `vd'<20
	di "`v'  " round(r(rho),0.001) // MUCH better!
}
		
** Correlations with standing water level (SP lat/lons)
foreach v of varlist gw_rast_depth_???_?sSP {
	qui correlate swl `v' 
	di "`v'  " round(r(rho),0.001)
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<10
	di "`v'  " round(r(rho),0.001) 
	local vd = substr(subinstr("`v'","_depth_","_dist_",1),1,18) + "SP"
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<10 & `vd'<20
	di "`v'  " round(r(rho),0.001) // MUCH better!
}

	
***	
** Correlations, good lat/lons, individual farms, ag end use, SP-APEP coords w/in 10 miles
** nearest gw measurement within 10 miles 
***
	
** Correlations with standing water level (APEP lat/lons)
foreach v of varlist gw_rast_depth_???_?s {
	qui correlate swl `v' 
	di "`v'  " r(N) "  " round(r(rho),0.001)
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<10
	di "`v'  " r(N) "  " round(r(rho),0.001) 
	local vd = substr(subinstr("`v'","_depth_","_dist_",1),1,18)
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<10 & `vd'<20
	di "`v'  " r(N) "  " round(r(rho),0.001) 
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<10 & `vd'<10
	di "`v'  " r(N) "  " round(r(rho),0.001) // MUCH better!
}
		
** Correlations with standing water level (SP lat/lons)
foreach v of varlist gw_rast_depth_???_?sSP {
	qui correlate swl `v' 
	di "`v'  " r(N) "  " round(r(rho),0.001)
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<10
	di "`v'  " r(N) "  " round(r(rho),0.001) 
	local vd = substr(subinstr("`v'","_depth_","_dist_",1),1,18) + "SP"
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<10 & `vd'<20
	di "`v'  " r(N) "  " round(r(rho),0.001) 
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<10 & `vd'<10
	di "`v'  " r(N) "  " round(r(rho),0.001) // MUCH better!
}

	
***	
** Correlations, good lat/lons, individual farms, ag end use, 
** nearest gw measurement within 10 miles 
***
	
** Correlations with standing water level (APEP lat/lons)
foreach v of varlist gw_rast_depth_???_?s {
	local vd = substr(subinstr("`v'","_depth_","_dist_",1),1,18)
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<10 & `vd'<10
	di "`v'  " r(N) "  " round(r(rho),0.001) 
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & `vd'<10
	di "`v'  " r(N) "  " round(r(rho),0.001) // we need low sp_miles_from_pump!
}
		
** Correlations with standing water level (SP lat/lons)
foreach v of varlist gw_rast_depth_???_?sSP {
	local vd = substr(subinstr("`v'","_depth_","_dist_",1),1,18) + "SP"
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<10 & `vd'<10
	di "`v'  " r(N) "  " round(r(rho),0.001) 
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & `vd'<10
	di "`v'  " r(N) "  " round(r(rho),0.001) // only *slightly* worse!
}


***	
** Correlations, good lat/lons, individual farms, ag end use, SP-APEP coords w/in 5 miles
** nearest gw measurement within 5 miles 
***
	
** Correlations with standing water level (APEP lat/lons)
foreach v of varlist gw_rast_depth_???_?s {
	local vd = substr(subinstr("`v'","_depth_","_dist_",1),1,18)
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<20 & `vd'<20
	di "`v'  " r(N) "  " round(r(rho),0.001) 
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<10 & `vd'<10
	di "`v'  " r(N) "  " round(r(rho),0.001) 
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<5 & `vd'<5
	di "`v'  " r(N) "  " round(r(rho),0.001) 
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<3 & `vd'<3
	di "`v'  " r(N) "  " round(r(rho),0.001) 
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<1 & `vd'<1
	di "`v'  " r(N) "  " round(r(rho),0.001) 
}
		
** Correlations with standing water level (SP lat/lons)
foreach v of varlist gw_rast_depth_???_?sSP {
	local vd = substr(subinstr("`v'","_depth_","_dist_",1),1,18) + "SP"
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<20 & `vd'<20
	di "`v'  " r(N) "  " round(r(rho),0.001) 
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<10 & `vd'<10
	di "`v'  " r(N) "  " round(r(rho),0.001) 
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<5 & `vd'<5
	di "`v'  " r(N) "  " round(r(rho),0.001) 
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<3 & `vd'<3
	di "`v'  " r(N) "  " round(r(rho),0.001) 
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<1 & `vd'<1
	di "`v'  " r(N) "  " round(r(rho),0.001) 
}

** Correlations with standing water level (APEP lat/lons; releasing closeness criterion)
foreach v of varlist gw_rast_depth_???_?s {
	local vd = substr(subinstr("`v'","_depth_","_dist_",1),1,18)
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<1 & `vd'<1
	di "`v'  " r(N) "  " round(r(rho),0.001) 
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<5 & `vd'<1
	di "`v'  " r(N) "  " round(r(rho),0.001) 
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<10 & `vd'<1
	di "`v'  " r(N) "  " round(r(rho),0.001) 
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<20 & `vd'<1
	di "`v'  " r(N) "  " round(r(rho),0.001) 
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<50 & `vd'<1
	di "`v'  " r(N) "  " round(r(rho),0.001) // drops off, but still pretty good, corr > 0.7 for monthly
}

** Correlations with standing water level (SP lat/lons; releasing closeness criterion)
foreach v of varlist gw_rast_depth_???_?sSP {
	local vd = substr(subinstr("`v'","_depth_","_dist_",1),1,18) + "SP"
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<1 & `vd'<1
	di "`v'  " r(N) "  " round(r(rho),0.001) 
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<5 & `vd'<1
	di "`v'  " r(N) "  " round(r(rho),0.001) 
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<10 & `vd'<1
	di "`v'  " r(N) "  " round(r(rho),0.001) 
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<20 & `vd'<1
	di "`v'  " r(N) "  " round(r(rho),0.001) 
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<50 & `vd'<1
	di "`v'  " r(N) "  " round(r(rho),0.001) // hardly drops off at all!!
}


** Correlations with standing water level (SP lat/lons; releasing closeness criterion)
foreach v of varlist gw_rast_depth_???_?sSP {
	local vd = substr(subinstr("`v'","_depth_","_dist_",1),1,18) + "SP"
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<1 & `vd'<10
	di "`v'  " r(N) "  " round(r(rho),0.001) 
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<5 & `vd'<10
	di "`v'  " r(N) "  " round(r(rho),0.001) 
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<10 & `vd'<10
	di "`v'  " r(N) "  " round(r(rho),0.001) 
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<20 & `vd'<10
	di "`v'  " r(N) "  " round(r(rho),0.001) 
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<50 & `vd'<10
	di "`v'  " r(N) "  " round(r(rho),0.001) // hardly drops off at all!!
}

** Correlations with standing water level (SP lat/lons; releasing closeness criterion)
foreach v of varlist gw_rast_depth_???_?sSP {
	local vd = substr(subinstr("`v'","_depth_","_dist_",1),1,18) + "SP"
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<1 & `vd'<20
	di "`v'  " r(N) "  " round(r(rho),0.001) 
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<5 & `vd'<20
	di "`v'  " r(N) "  " round(r(rho),0.001) 
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<10 & `vd'<20
	di "`v'  " r(N) "  " round(r(rho),0.001) 
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<20 & `vd'<20
	di "`v'  " r(N) "  " round(r(rho),0.001) 
	qui correlate swl `v' if bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<50 & `vd'<20
	di "`v'  " r(N) "  " round(r(rho),0.001) // actually gets better...
}

	
** Compare SWL vs rasterized GW depth
foreach v of varlist gw_rast_depth_???_?s* {
	gen diff_`v' = swl - `v'
}
sum diff_gw_*s
sum diff_gw_*sSP
sum diff_gw_*sSP if  bad_geocode_flag==0 
sum diff_gw_*sSP if  bad_geocode_flag==0 & customertype=="Individ Farms"
sum diff_gw_*sSP if  bad_geocode_flag==0 & customertype=="Individ Farms" ///
	& waterenduse=="agriculture" 
sum diff_gw_*sSP if  bad_geocode_flag==0 & customertype=="Individ Farms" ///
	& waterenduse=="agriculture" & sp_miles_from_pump<20 
foreach v of varlist gw_rast_depth_???_?sSP {
	local vd = substr(subinstr("`v'","_depth_","_dist_",1),1,18) + "SP"
	sum diff_`v' if  bad_geocode_flag==0 & customertype=="Individ Farms" ///
		& waterenduse=="agriculture" & sp_miles_from_pump<5 & `vd'<3, detail
}

hist diff_gw_rast_depth_qtr_1sSP // at least it's centered on zero!
hist diff_gw_rast_depth_qtr_1sSP if gw_rast_dist_qtr_1SP<5 
hist diff_gw_rast_depth_qtr_1sSP if gw_rast_dist_qtr_1SP<5 & inrange(diff_gw_rast_depth_qtr_1sSP,-100,100)
hist diff_gw_rast_depth_qtr_1sSP if gw_rast_dist_qtr_1SP<3 
hist diff_gw_rast_depth_qtr_1sSP if gw_rast_dist_qtr_1SP<3 & inrange(diff_gw_rast_depth_qtr_1sSP,-100,100)
hist diff_gw_rast_depth_qtr_1sSP if gw_rast_dist_qtr_1SP<1 
hist diff_gw_rast_depth_qtr_1sSP if gw_rast_dist_qtr_1SP<1 & inrange(diff_gw_rast_depth_qtr_1sSP,-100,100)

hist diff_gw_rast_depth_mth_1sSP // at least it's centered on zero!
hist diff_gw_rast_depth_mth_1sSP if gw_rast_dist_mth_1SP<5
hist diff_gw_rast_depth_mth_1sSP if gw_rast_dist_mth_1SP<5 & inrange(diff_gw_rast_depth_mth_1sSP,-100,100)
hist diff_gw_rast_depth_mth_1sSP if gw_rast_dist_mth_1SP<3
hist diff_gw_rast_depth_mth_1sSP if gw_rast_dist_mth_1SP<3 & inrange(diff_gw_rast_depth_mth_1sSP,-100,100)
hist diff_gw_rast_depth_mth_1sSP if gw_rast_dist_mth_1SP<1
hist diff_gw_rast_depth_mth_1sSP if gw_rast_dist_mth_1SP<1 & inrange(diff_gw_rast_depth_mth_1sSP,-100,100)
hist diff_gw_rast_depth_mth_1sSP if gw_rast_dist_mth_1SP<1 & sp_miles_from_pump<20 & inrange(diff_gw_rast_depth_mth_1sSP,-100,100)
hist diff_gw_rast_depth_mth_1sSP if gw_rast_dist_mth_1SP<1 & sp_miles_from_pump<10 & inrange(diff_gw_rast_depth_mth_1sSP,-100,100)
hist diff_gw_rast_depth_mth_1sSP if gw_rast_dist_mth_1SP<1 & sp_miles_from_pump<5 & inrange(diff_gw_rast_depth_mth_1sSP,-100,100)
hist diff_gw_rast_depth_mth_1sSP if gw_rast_dist_mth_1SP<1 & sp_miles_from_pump<3 & inrange(diff_gw_rast_depth_mth_1sSP,-100,100)
hist diff_gw_rast_depth_mth_1sSP if gw_rast_dist_mth_1SP<1 & sp_miles_from_pump<1 & inrange(diff_gw_rast_depth_mth_1sSP,-100,100)

twoway (scatter swl gw_rast_depth_mth_1sSP ///
	if gw_rast_dist_mth_1SP<1 & sp_miles_from_pump<1 & inrange(diff_gw_rast_depth_mth_1sSP,-100,100)) ///
	(function y=x, range (0 500))
twoway (scatter swl gw_rast_depth_mth_2sSP ///
	if gw_rast_dist_mth_2SP<1 & sp_miles_from_pump<1 & inrange(diff_gw_rast_depth_mth_2sSP,-100,100)) ///
	(function y=x, range (0 500))
twoway (scatter swl gw_rast_depth_qtr_1sSP ///
	if gw_rast_dist_qtr_1SP<1 & sp_miles_from_pump<1 & inrange(diff_gw_rast_depth_qtr_1sSP,-100,100)) ///
	(function y=x, range (0 500))
twoway (scatter swl gw_rast_depth_qtr_2sSP ///
	if gw_rast_dist_qtr_2SP<1 & sp_miles_from_pump<1 & inrange(diff_gw_rast_depth_qtr_2sSP,-100,100)) ///
	(function y=x, range (0 500))

** Some regressions to turn correlations into predictions
gen test_year = year(test_date_stata)
reghdfe swl gw_rast_depth_mth_2sSP ///
	if bad_geocode_flag==0 & customertype=="Individ Farms" ///
	& waterenduse=="agriculture" & sp_miles_from_pump<50 & gw_rast_dist_mth_1SP<5, ///
	absorb(temp_basin_id_group#test_year)
	// Not a super convincing case for overturning the simple default: swl_hat = raster value!


***** 
**	Estimating drawdown where missing
*****	
	
count 
count if swl==.
count if swl==. & rwl==.
gen test_year = year(test_date_stata)
gen temp_swl_missing = swl==.
tab test_year temp_swl_missing
tabstat temp_swl_missing, by(test_year)
tabstat temp_swl_missing if bad_geocode_flag==0, by(test_year)
tabstat temp_swl_missing if bad_geocode_flag==0 & customertype=="Individ Farms", by(test_year)
tabstat temp_swl_missing if bad_geocode_flag==0 & customertype=="Individ Farms" & ///
	& waterenduse=="agriculture", by(test_year)
tabstat temp_swl_missing if bad_geocode_flag==0 & customertype=="Individ Farms" & ///
	& waterenduse=="agriculture" & sp_miles_from_pump<20, by(test_year)
tab test_year if swl==. 
tab test_year if swl==. & bad_geocode_flag==0 & customertype=="Individ Farms" & ///
	& waterenduse=="agriculture" // 4,404 missings 
tab test_year if swl==. & bad_geocode_flag==0 & customertype=="Individ Farms" & ///
	& waterenduse=="agriculture" & gw_rast_dist_qtr_1SP>20 
	// only 638 missings more than 20 miles from a GW measurement point, not bad
	
egen temp_swl_missing_min = min(temp_swl_missing), by(latlon_group)
egen temp_swl_missing_min_sp = min(temp_swl_missing), by(latlon_group sp_uuid)
count if temp_swl_missing==1
count if temp_swl_missing_min_sp==1
count if temp_swl_missing_min==1
di r(N)/_N
unique sp_uuid
local uniq = r(unique)
unique sp_uuid if temp_swl_missing_min==1
di r(unique)/`uniq'



** ESTIMATING DRAWDOWN using PHYSICS
br swl pwl rwl drwdwn flag_bad_drwdwn temp_swl_missing if rwl!=swl 
gen ln_drwdwn = ln(1+drwdwn)
gen ln_flow_gpm = ln(1+flow_gpm)
gen rwl_2 = rwl^2
gen rwl_3 = rwl^3
gen rwl_4 = rwl^4
reghdfe ln_drwdwn ln_flow_gpm if flag_bad_drwdwn==0 & temp_swl_missing==0, absorb(temp_basin_id_group)
reghdfe ln_drwdwn ln_flow_gpm if flag_bad_drwdwn==0 & temp_swl_missing==0, absorb(temp_basin_id_group#test_year)
reghdfe ln_drwdwn c.ln_flow_gpm#i.temp_basin_id_group if flag_bad_drwdwn==0 & temp_swl_missing==0, absorb(temp_basin_id_group#test_year)
reghdfe ln_drwdwn c.ln_flow_gpm##c.rwl if flag_bad_drwdwn==0 & temp_swl_missing==0, absorb(temp_basin_id_group#test_year)
reghdfe ln_drwdwn ln_flow_gpm if flag_bad_drwdwn==0 & temp_swl_missing==0, absorb(temp_basin_id_group#test_year##c.rwl)
reghdfe ln_drwdwn ln_flow_gpm if flag_bad_drwdwn==0 & temp_swl_missing==0, absorb(temp_basin_idSP_group#test_year##c.rwl)
reghdfe ln_drwdwn ln_flow_gpm if flag_bad_drwdwn==0 & temp_swl_missing==0, absorb(temp_basin_sub_idSP_group#test_year##c.rwl)	
reghdfe ln_drwdwn ln_flow_gpm if flag_bad_drwdwn==0 & temp_swl_missing==0, absorb(temp_basin_sub_idSP_group#c.rwl)	
reghdfe ln_drwdwn ln_flow_gpm if flag_bad_drwdwn==0 & temp_swl_missing==0, absorb(temp_basin_id_group#c.rwl)	
reghdfe ln_drwdwn ln_flow_gpm if flag_bad_drwdwn==0 & temp_swl_missing==0, absorb(temp_basin_sub_id_group#c.rwl wdist_id#c.rwl)	

reghdfe ln_drwdwn ln_flow_gpm if flag_bad_drwdwn==0 & temp_swl_missing==0, absorb(temp_basin_id_group)	
reghdfe ln_drwdwn ln_flow_gpm if flag_bad_drwdwn==0 & temp_swl_missing==0, absorb(temp_basin_id_group#c.rwl)	
reghdfe ln_drwdwn ln_flow_gpm if flag_bad_drwdwn==0 & temp_swl_missing==0, absorb(temp_basin_id_group#c.(rwl rwl_2))	
reghdfe ln_drwdwn ln_flow_gpm if flag_bad_drwdwn==0 & temp_swl_missing==0, absorb(temp_basin_id_group#c.(rwl rwl_2 rwl_3))	
reghdfe ln_drwdwn ln_flow_gpm if flag_bad_drwdwn==0 & temp_swl_missing==0, absorb(temp_basin_id_group#c.(rwl rwl_2 rwl_3 rwl_4))
	// The simplest model works! basin FEs and basin-specific slopes! Amazing!! Great, Bob!!!

}

/* 

Takeaway 1: Lots of discrepancies between SP vs. APEP coordinates. Median distance apart is 8.4 miles.
            Half of SPs are in different water district than matched APEP coordinates.
			
Takeaway 2: SP premise lat/lons	out-perform APEP pump lat/lons in comparing SWL with groundwater rasters!
            This is very surprising, but the correlations are speaking loud and clear.
			
Takeaway 3: SWL is highly correlated with rasterize groundwater levels, when DWR measurements are close to
            SP lat/lons (rho>0.9). Monthly>quarterly when measurements are close, quarterly>monthly otherwise.

Takeaway 4: Despite many attempts to dig for a convincing complicating wrinkle that woudl help translate 
            rasterized groundwater depths into predicted SWL, I see no reason not use the simple prediction 
            function: swl_hat = gw_rast_depth_mth_1SP (or qtr)
			
Takeaway 5: For estimating drawdown, physics wins again! The simplest theoretically informed regression has
            R^2 = 0.90! Use this to linearly predict missing drawdown, and to help interpolate drawdown between
			pump tests.

*******************************************************************************
*******************************************************************************
