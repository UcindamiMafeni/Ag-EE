clear all
version 13
set more off

*****************************************************
**** Script to craete analysis datasets for camp ****
*****************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"


*******************************************************************************
*******************************************************************************

** 1. Monthified billing data with prices, with only APEP customers
{
** Start with merged APEP SPs
use "$dirpath_data/merged/sp_apep_merged_notunique.dta", clear

** Keep only  customer detail variables, drop duplicates
keep prsn_uuid-ee_measure_count
duplicates drop
unique sp_uuid sa_uuid
assert r(unique)==r(N)	

** Merge in monthified billing data
joinby sa_uuid using "$dirpath_data/pge_cleaned/billing_data_monthified.dta", ///
	unmatched(master)
assert _merge!=1 | bill_dt_first==.
drop _merge
	
** De-dupify using SP xwalk
gen temp_to_keep = sp_uuid==sp_uuid1 | sp_uuid==sp_uuid2 | sp_uuid==sp_uuid3
egen temp_to_keep_max = max(temp_to_keep), by(sa_uuid modate)
tab temp*
drop if temp_to_keep==0 & temp_to_keep_max==1
unique sa_uuid modate
assert r(unique)==r(N)
drop sp_uuid? temp*

** Merge in monthified prices
rename rt_sched_cd rt_sched_cdM
merge 1:1 sa_uuid modate using "$dirpath_data/merged/monthified_avg_prices_nomerge", ///
	keep(1 3)
tab rt_sched_cdM if _merge==1 & bill_dt_first!=.
drop if rt_sched_cdM=="ECLSD"
drop rt_sched_cdM
assert _merge!=1 | bill_dt_first==.
drop _merge

** Collapse to SP-month level
duplicates t sp_uuid modate, gen(dup)
sort sp_uuid modate sa_sp_start sa_uuid
br if dup>0 // lots of mid-month bill changeovers, also lost of multi-SA SPs

	// Take max of flag variables
foreach v of varlist net_mtr_ind dr_ind in_calif in_pge in_pou bad_geocode_flag ///
	bad_cz_flag missing_geocode_flag in_billing in_interval flag_* interval_bill_corr		{
	egen temp = max(`v'), by(sp_uuid modate)
	replace `v' = temp if dup>0
	drop temp
}

	// Take min/max of date variables
foreach v of varlist sa_sp_start bill_dt_first interval_dt_first min_p_kwh {
	egen temp = min(`v'), by(sp_uuid modate)
	replace `v' = temp if dup>0
	drop temp
}
foreach v of varlist sa_sp_stop bill_dt_last interval_dt_last max_p_kwh {
	egen temp = max(`v'), by(sp_uuid modate)
	replace `v' = temp if dup>0
	drop temp
}
drop sa_sp_lapse* day_first day_last

	// Take mode of strings
foreach v of varlist dr_program prsn_naics naics_descr pou_name rt_sched_cd {
	egen temp = mode(`v'), by(sp_uuid modate) minmode
	replace `v' = temp if dup>0
	drop temp
}

	// Take weight-averages of mean price variables
foreach v of varlist  mean_p_kwh mean_p_kw_max mean_p_kw_peak mean_p_kw_partpeak {
	egen double temp_num1 = sum(`v'*mnth_bill_kwh) if `v'!=. & mnth_bill_kwh!=., by(sp_uuid modate)
	egen double temp_num2 = mean(temp_num1), by(sp_uuid modate)
	egen double temp_denom1 = sum(mnth_bill_kwh) if `v'!=. & mnth_bill_kwh!=., by(sp_uuid modate)
	egen double temp_denom2 = mean(temp_denom1), by(sp_uuid modate)
	replace `v' = temp_num2/temp_denom2 if temp_denom2!=0 & temp_denom2!=.
	egen temp = mean(`v'), by(sp_uuid modate)
	replace `v' = temp if temp_denom2==0 | temp_denom2==.
	drop temp*
} 

	// Sum  bill kWh and $, and count of EE measures
foreach v of varlist mnth_bill_kwh mnth_bill_amount ee_measure_count {
	egen double temp = sum(`v'), by(sp_uuid modate)
	replace `v' = temp if dup>0
	drop temp
}	

	// Collapse!
drop prsn_uuid sa_uuid days dup
duplicates drop
unique sp_uuid modate
assert r(unique)==r(N)

** Save
sort sp_uuid modate
compress
save "$dirpath_data/merged/sp_month_panel_apep.dta", replace
	
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

** 2. Hourly version without all the APEP test variables
{
** Start with merged APEP SPs
use "$dirpath_data/merged/sp_apep_merged_notunique.dta", clear

** Keep only customer detail variables, drop duplicates
keep prsn_uuid-ee_measure_count
duplicates drop
unique sp_uuid sa_uuid
assert r(unique)==r(N)	

** Keep only SAs whta merge into interval data
keep if in_interval==1

** Merge in billing data
joinby sa_uuid using "$dirpath_data/pge_cleaned/billing_data.dta", ///
	unmatched(master)
assert _merge==3
drop _merge
	
** De-dupify using SP xwalk
gen temp_to_keep = sp_uuid==sp_uuid1 | sp_uuid==sp_uuid2 | sp_uuid==sp_uuid3
egen temp_to_keep_max = max(temp_to_keep), by(sa_uuid bill_start_dt)
tab temp*
drop if temp_to_keep==0 & temp_to_keep_max==1
unique sa_uuid bill_start_dt
assert r(unique)==r(N)
drop sp_uuid? temp*

** Confirm uniqueness
unique sa_uuid bill_start_dt
assert r(unique)==r(N)

** Keep only bills with interval data
drop if bill_end_dt<interval_dt_first | bill_start_dt>interval_dt_last

** Drop variables I won't need to reduce file size
keep sp_uuid sa_uuid bill_start_dt net_mtr_ind rt_sched_cd

** Merge into hourly interval data with prices
merge 1:m sa_uuid bill_start_dt using "$dirpath_data/merged/hourly_with_prices.dta", ///
	keep(1 3)
tab rt_sched_cd if _merge==1
gen year = year(bill_start_dt)
tab year _merge
drop if _merge==1
	
** Drop unnecessary variables 
drop sa_uuid bill_start_dt rt_sched_cd group _merge year	

** Collapse to SP-hour level
foreach v of varlist kwh p_kwh {
	egen double temp = mean(`v'), by(sp_uuid date hour)
	replace `v' = temp 
	drop temp
}
foreach v of varlist net_mtr_ind {
	egen temp = max(`v'), by(sp_uuid date hour)
	replace `v' = temp 
	drop temp
}
duplicates drop
unique sp_uuid date hour
assert r(unique)==r(N)

** Save
sort sp_uuid date hour
compress
save "$dirpath_data/merged/sp_hour_panel_apep.dta", replace

}
