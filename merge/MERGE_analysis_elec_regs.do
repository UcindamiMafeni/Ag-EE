clear all
version 13
set more off

*****************************************************
**** Script to craete analysis datasets for camp ****
*****************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

** PENDING:

** Files for hourly regressions (at SA and SP levels)
** Incorporate APEP variables?
** Add GIS variables

*******************************************************************************
*******************************************************************************

** 1. Monthified billing data with prices, at SA and SP levels
if 1==0{

** Start with cleaned customer + monthified billing data (all three data pulls)
foreach tag in 20180719 20180322 20180827 {
		
	// Load customer data
	use "$dirpath_data/pge_cleaned/pge_cust_detail_`tag'.dta", clear
	
	// Flag which pull these data are from
	gen pull = "`tag'"
	la var pull "Which pull are these data from?"
	
	// Keep relevant variable
	keep prsn_uuid sp_uuid sa_uuid prem_lat prem_long net_mtr_ind dr_ind dr_program ///
		climate_zone_cd sa_sp_start sa_sp_stop in_calif in_pge in_pou pou_name ///
		bad_geocode_flag climate_zone_cd_gis bad_cz_flag missing_geocode_flag ///
		in_billing bill_dt_first bill_dt_last in_interval interval_dt_first ///
		interval_dt_last ee_measure_count pull
		
	// Confirm uniqueness
	duplicates drop
	unique sp_uuid sa_uuid
	assert r(unique)==r(N)
	
	// Drop if not in billing data
	drop if in_billing==0
	drop in_billing
	
	// Merge in monthified billing data
	joinby sa_uuid using "$dirpath_data/pge_cleaned/billing_data_monthified_`tag'.dta", ///
		unmatched(master)
	assert _merge!=1 | bill_dt_first==.
	drop _merge
	
	// Partially de-dupify SA/SPs using SP xwalk
	gen temp_to_keep = sp_uuid==sp_uuid1 | sp_uuid==sp_uuid2 | sp_uuid==sp_uuid3
	egen temp_to_keep_max = max(temp_to_keep), by(sa_uuid modate)
	tab temp*
	drop if temp_to_keep==0 & temp_to_keep_max==1
	drop sp_uuid? temp*

	// Save as temp file
	compress
	tempfile merged`tag'
	save `merged`tag''
	
}

** Merge together monthified billing data across all three data pulls
use `merged20180719', clear
merge 1:1 sp_uuid sa_uuid modate using `merged20180322'
assert pull=="20180322" if _merge==2
assert pull=="20180719" if _merge!=2
drop _merge
merge 1:1 sp_uuid sa_uuid modate using `merged20180827'
assert pull=="20180827" if _merge==2
assert pull!="20180827" if _merge!=2 
drop _merge

** Merge in monthified billing data
rename rt_sched_cd RT_sched_cd 
merge m:1 sa_uuid modate pull using "$dirpath_data/merged/monthified_avg_prices_nomerge"
tab RT_sched_cd if _merge==1 // non-AG rates
drop if _merge==1
drop RT_sched_cd _merge
		
** Deal with duplicates SP/SAs
duplicates t sa_uuid modate, gen(dup)
tab dup
sort sa_uuid modate sp_uuid
//br if dup>0 // Virtually all have identical lat/lons
	
	// Sum up frequencies of SP
preserve 
contract sp_uuid, freq(temp_sp_freq)
tempfile sp_freq
save `sp_freq'
restore
merge m:1 sp_uuid using `sp_freq', nogen

	// Drop duplicates with lower SP frequency
egen temp_sp_freq_max = max(temp_sp_freq) if dup>0, by(sa_uuid modate)
unique sa_uuid modate
local uniq = r(unique)
drop if dup>0 & temp_sp_freq<temp_sp_freq_max
unique sa_uuid modate
assert r(unique)==`uniq'

	// Flag remaining dups
duplicates t sa_uuid modate, gen(dup2)
tab dup2
sort sa_uuid modate sp_uuid
//br sa_uuid modate sp_uuid dup dup2 temp_sp_freq pull if dup2>0

	// Pick an SP at random
unique sa_uuid modate
local uniq = r(unique)
drop if dup2>0 & sa_uuid==sa_uuid[_n-1] & modate==modate[_n-1]
unique sa_uuid modate
assert r(unique)==`uniq'

	// Confirm uniqueness
assert r(unique)==r(N)	
drop dup* temp*

** Save uncollapsed version at the SA-month level (allowing us to play with tariffs in regressions)
sort sp_uuid modate sa_uuid
compress
save "$dirpath_data/merged/sa_month_elec_panel.dta", replace

** Collapse to SP-month level
duplicates t sp_uuid modate, gen(dup)
sort sp_uuid modate sa_sp_start sa_uuid
br if dup>0 // lots of mid-month bill changeovers, also lots of multi-SA SPs

	// Take max of flag variables
foreach v of varlist net_mtr_ind dr_ind in_calif in_pge in_pou bad_geocode_flag ///
	bad_cz_flag missing_geocode_flag in_interval flag_* interval_bill_corr {
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
drop day_first day_last

	// Take mode of strings
foreach v of varlist dr_program pou_name rt_sched_cd {
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

	// Drop with remaining dups (SPs split across multiple pulls)
duplicates t sp_uuid modate, gen(dup)
egen temp_max = max(pull=="20180322"), by(sp_uuid modate)
egen temp_min = min(pull=="20180322"), by(sp_uuid modate)
unique sp_uuid modate
local uniq = r(unique)
drop if temp_min<temp_max & pull=="20180827" & dup>0
unique sp_uuid modate
assert r(unique)==`uniq'

	// Confirm uniqueness
assert r(unique)==r(N)

** Save
sort sp_uuid modate
compress
save "$dirpath_data/merged/sp_month_elec_panel.dta", replace

}

*******************************************************************************
*******************************************************************************	
	
** 2. Billing data without prices, at SP level
if 1==0{

** Start with cleaned customer + monthified billing data (all three data pulls)
foreach tag in 20180719 20180322 20180827 {
		
	// Load customer data
	use "$dirpath_data/pge_cleaned/pge_cust_detail_`tag'.dta", clear
	
	// Flag which pull these data are from
	gen pull = "`tag'"
	la var pull "Which pull are these data from?"
	
	// Keep relevant variable
	keep prsn_uuid sp_uuid sa_uuid prem_lat prem_long net_mtr_ind dr_ind dr_program ///
		climate_zone_cd sa_sp_start sa_sp_stop in_calif in_pge in_pou pou_name ///
		bad_geocode_flag climate_zone_cd_gis bad_cz_flag missing_geocode_flag ///
		in_billing bill_dt_first bill_dt_last in_interval interval_dt_first ///
		interval_dt_last ee_measure_count pull
		
	// Confirm uniqueness
	duplicates drop
	unique sp_uuid sa_uuid
	assert r(unique)==r(N)
	
	// Drop if not in billing data
	drop if in_billing==0
	drop in_billing
	
	// Merge in monthified billing data
	joinby sa_uuid using "$dirpath_data/pge_cleaned/billing_data_`tag'.dta", ///
		unmatched(master)
	assert _merge!=1 | bill_dt_first==.
	drop _merge
	
	// Partially de-dupify SA/SPs using SP xwalk
	gen temp_to_keep = sp_uuid==sp_uuid1 | sp_uuid==sp_uuid2 | sp_uuid==sp_uuid3
	egen temp_to_keep_max = max(temp_to_keep), by(sa_uuid bill_start_dt)
	tab temp*
	drop if temp_to_keep==0 & temp_to_keep_max==1
	drop sp_uuid? temp*

	// Save as temp file
	compress
	tempfile merged`tag'
	save `merged`tag''
	
}

** Merge together monthified billing data across all three data pulls
use `merged20180719', clear
merge 1:1 sp_uuid sa_uuid bill_start_dt using `merged20180322'
assert pull=="20180322" if _merge==2
assert pull=="20180719" if _merge!=2
drop _merge
merge 1:1 sp_uuid sa_uuid bill_start_dt using `merged20180827'
assert pull=="20180827" if _merge==2
assert pull!="20180827" if _merge!=2 
drop _merge

** Deal with duplicates SP/SAs
duplicates t sa_uuid bill_start_dt, gen(dup)
tab dup
sort sa_uuid bill_start_dt sp_uuid
//br if dup>0 // Virtually all have identical lat/lons
	
	// Sum up frequencies of SP
preserve 
contract sp_uuid, freq(temp_sp_freq)
tempfile sp_freq
save `sp_freq'
restore
merge m:1 sp_uuid using `sp_freq', nogen

	// Drop duplicates with lower SP frequency
egen temp_sp_freq_max = max(temp_sp_freq) if dup>0, by(sa_uuid bill_start_dt)
unique sa_uuid bill_start_dt
local uniq = r(unique)
drop if dup>0 & temp_sp_freq<temp_sp_freq_max
unique sa_uuid bill_start_dt
assert r(unique)==`uniq'

	// Flag remaining dups
duplicates t sa_uuid bill_start_dt, gen(dup2)
tab dup2
sort sa_uuid bill_start_dt sp_uuid
//br sa_uuid bill_start_dt sp_uuid dup dup2 temp_sp_freq pull if dup2>0

	// Pick an SP at random
unique sa_uuid bill_start_dt
local uniq = r(unique)
drop if dup2>0 & sa_uuid==sa_uuid[_n-1] & bill_start_dt==bill_start_dt[_n-1]
unique sa_uuid bill_start_dt
assert r(unique)==`uniq'

	// Confirm uniqueness
assert r(unique)==r(N)	
drop dup* temp*
	
** Save panel unique by SA-bill
compress
save "$dirpath_data/merged/sa_bill_elec_panel.dta", replace

}

*******************************************************************************
*******************************************************************************	
	
** 3. Interval data with prices, at SP level
if 1==1{

** Merge with hourly data, for each data pull
foreach tag in /*"20180719"*/ "20180322" "20180827" {

	** Load full SA-bill panel dataset
	use "$dirpath_data/merged/sa_bill_elec_panel.dta", clear

	** Keep SAs in interval data
	keep if in_interval==1

	** Drop bills before (after) first (last) appearance in interval data
	drop if bill_end_dt<interval_dt_first
	drop if bill_start_dt>interval_dt_last

	** Drop variables not needed for merge
	keep sp_uuid sa_uuid bill_start_dt pull

	** Merge into hourly interval data with prices
	merge 1:m sa_uuid bill_start_dt using "$dirpath_data/merged/hourly_with_prices_`tag'.dta", ///
		keep(2 3)
	assert _merge!=2
	cap drop group
	tab pull
	
	** Drop unnecessary variables 
	drop sa_uuid bill_start_dt _merge 	
		
	** Collapse to SP-hour level
	foreach v of varlist kwh {
		egen double temp = sum(`v'), by(sp_uuid date hour)
		replace `v' = temp 
		drop temp
	}
	foreach v of varlist p_kwh {
		egen double temp = mean(`v'), by(sp_uuid date hour)
		replace `v' = temp 
		drop temp
	}
	duplicates drop
	unique sp_uuid date hour
	assert r(unique)==r(N)

	** Save
	sort sp_uuid date hour
	compress
	save "$dirpath_data/merged/sp_hourly_elec_panel_`tag'.dta", replace

}

}

*******************************************************************************
*******************************************************************************	

** 4. Resolve mismatched data pulls in hourly datasets

** are mismatched observations actually dups?
** drop pull (and dups?) from SP-hour datasets
** drop group (and dups?) from SA-hour datasets

*******************************************************************************
*******************************************************************************	

** 5. Transform Q and P 

/*	
START HERE	
	
	
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

*/
*******************************************************************************
*******************************************************************************

