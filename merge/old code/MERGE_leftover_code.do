
*** LEFTOVER CODE FOR INCORPORARING APEP INTO REGRESSION DATASETS
{
	
** Collapse a few APEP variables to the SP-month level, for merging in
use "$dirpath_data/merged/sp_apep_merged_notunique.dta", clear
keep sp_uuid customertype farmtype waterenduse apeptestid test_date_stata ///
	merge_apep_proj-hp
duplicates drop	
drop if test_date_stata==.

	// Create modate variable
gen modate = ym(year(test_date_stata),month(test_date_stata))
format %tm modate
drop test_date_stata apeptestid

	// Sum some variables that make sense to sum
foreach v of varlist subsidy {
	egen double temp = sum(`v'), by(sp_uuid modate)
	replace `v' = temp
	drop temp
}
	
	// Average some variables that make sense to average
foreach v of varlist kwhaf khmg ope ope_after hp hp_after {
	egen double temp = mean(`v'), by(sp_uuid modate)
	replace `v' = temp
	drop temp
}

	// Collapse, before dealing with project variables!
keep sp_uuid modate subsidy kwhaf khmg ope ope_after hp hp_after ///
	merge_apep_proj-subsidy_proj
duplicates drop
duplicates t sp_uuid modate, gen(dup)
tab dup	
order sp_uuid modate
sort sp_uuid modate
br if dup>0

	// For the 7 dups, pick the earlier project completion date
egen temp = min(date_proj_finish), by(sp_uuid modate)
gen temp2 = temp==date_proj_finish
drop if temp2==0 & dup>0
drop temp* dup

	// For the 7 dups, pick the late subsidized test date
duplicates t sp_uuid modate, gen(dup)
br if dup>0
egen temp = max(date_test_subs), by(sp_uuid modate)
gen temp2 = temp==date_test_subs
drop if temp2==0 & dup>0
drop temp* dup

	// Confirm uniqueness
unique sp_uuid modate
assert r(unique)==r(N)	

	// Save temp file
tempfile temp
save `temp'

** Merge temp file into SP-modate panel
use "$dirpath_data/merged/sp_month_panel_apep.dta", clear
merge 1:1 sp_uuid modate using `temp'
drop if _merge==2

** Dummy for subsidized test in month
gen apep_test = subsidy!=.
la var apep_test "Dummy for APEP test in month"
egen test = max(apep_test), by(sp_uuid)
assert test==1
drop test

** Carry forward APEP test variables
sort sp_uuid modate
foreach v of varlist kwhaf khmg ope_after ope hp_after hp {
	replace `v' = `v'[_n-1] if `v'==. & sp_uuid==sp_uuid[_n-1]
}

** Carry backwards APEP test variables, where still missing
assert ope!=. if apep_test
gen apep_carry_backwards = ope==. 
la var apep_carry_backwards "Dummy for APEP test variables carried backwards"
gsort sp_uuid -modate
foreach v of varlist kwhaf khmg ope_after ope hp_after hp {
	replace `v' = `v'[_n-1] if `v'==. & sp_uuid==sp_uuid[_n-1]
}
drop _merge

** Dummy for ever-project
egen apep_proj_ever = max(merge_apep_proj==3), by(sp_uuid)
la var apep_proj_ever "Dummy for an SP ever having an APEP project"

** Dummy for the month the project finished
unique sp_uuid if apep_proj_ever==1
local uniq = r(unique)
unique sp_uuid date_proj_finish if apep_proj_ever==1 & date_proj_finish!=.
assert r(unique)==`uniq'
egen temp = mode(date_proj_finish), by(sp_uuid)
gen temp_ym =  ym(year(temp),month(temp))
format %tm temp_ym
gen apep_proj_finish_dummy = temp_ym==modate & modate!=.
tab apep_proj_finish_dummy apep_proj_ever
egen temp1 = mean(modate) if apep_proj_ever==1 & apep_proj_finish_dummy==1, by(sp_uuid)
egen apep_proj_finish_modate = mean(temp1) if apep_proj_ever==1, by(sp_uuid)
format %tm apep_proj_finish_modate
drop temp*
gen apep_proj_post = apep_proj_finish_modate>=modate
replace apep_proj_post = 0 if apep_proj_ever==0
la var apep_proj_finish_dummy "Dummy = 1 during month APEP project finishes"
la var apep_proj_finish_modate "Month-year that SP's APEP project finished"
la var apep_proj_post "Post-indicator for SPs with APEP projects"

** Cumulative subsidy over time
rename subsidy subsidy_test // to avoid confusion
unique sp_uuid if apep_proj_ever==1
local uniq = r(unique)
unique sp_uuid subsidy_proj if apep_proj_ever==1 & subsidy_proj!=.
assert r(unique)==`uniq'
egen temp = mean(subsidy_proj), by(sp_uuid)
replace subsidy_proj = temp 
replace subsidy_proj = 0 if apep_proj_post==0
la var subsidy_proj "Cumulative APEP project subsidy received (post-finish, $)" 
drop temp

** Estimated savings post-project 
unique sp_uuid if apep_proj_ever==1
local uniq = r(unique)
unique sp_uuid est_savings_kwh_yr if apep_proj_ever==1 & est_savings_kwh_yr!=.
assert r(unique)==`uniq'
egen temp = mean(est_savings_kwh_yr), by(sp_uuid)
replace est_savings_kwh_yr = temp 
replace est_savings_kwh_yr = 0 if apep_proj_post==0
drop temp merge_apep_proj

** Drop if modate is missing (and not in billing data)	
drop if modate==.	

** Save
sort sp_uuid modate
unique sp_uuid modate
assert r(unique)==r(N)
compress
save "$dirpath_data/merged/sp_month_panel_apep.dta", replace


}

*******************************************************************************
*******************************************************************************

*** LEFTOVER CODE FOR DE-DUPIFYING METERS AFTER MERGING INTO BILLING DATA
{
	// Resolve duplicates using xwalk (based on bill start/end dates)
unique sa_uuid sp_uuid if merge_billing_customer==3
local uniq = r(unique)
gen temp_to_keep = _merge==3 & ///
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

}

*******************************************************************************
*******************************************************************************
