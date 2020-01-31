clear all
version 13
set more off

*********************************************************************
**** Script to merge APEP and usage data to calculate incentives ****
*********************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"
tempfile test_proj kwh_year

** Load merged dataset of pump tests and projects
use "$dirpath_data/merged/sp_apep_proj_merged.dta", clear

** Merge in full set of pump test variables
merge 1:1 apeptestid test_date_stata customertype farmtype waterenduse using ///
	"$dirpath_data/pge_cleaned/pump_test_data.dta", keep(3)
	
** Keep only relevant tests/projects
keep if customertype == "Individ Farms" // keep only farms
keep if waterenduse == "agriculture" | waterenduse == "irrigation" // keep only ag
*keep if flag_date_problem != . // keep only projects

** Select relevant test for each project based on multiple criteria
gen date_diff = test_date_stata - date_proj_finish // calculate days from test to project
gen date_diff_abs = abs(date_diff)
egen min_diff_abs = min(date_diff_abs), by(sp_uuid) // time to closest test
gen closest_test = (date_diff_abs == min_diff_abs) // closest test
gen date_diff_neg = date_diff if date_diff <= 0
egen min_diff_neg = max(date_diff_neg), by(sp_uuid) // time to last test before
gen last_test_before = (date_diff == min_diff_neg) // last test before
gen date_diff_pos = date_diff if date_diff >= 0
egen min_diff_pos = max(date_diff_pos), by(sp_uuid) // time to first test after
gen first_test_after = (date_diff == date_diff_pos) // first test after
drop date_diff* min_diff*

** Create identifiers for pump type and efficiency cutoff
gen pump_boost = strpos(pumptype, "Booster") > 0 // booster pumps
gen pump_sub = strpos(pumptype, "Submersible") > 0 // submersible pumps
gen ope_inelig = ope <= 30 // pumps ineleigible if OPE <= 30%
replace ope_inelig = ope <= 20 if pump_sub // submerisble pumps ineligible if OPE <= 20%
gen ope_low = ope > 30 & ope <= 50 // cutoff at 50% OPE
replace ope_low = ope > 20 & ope <= 40 if pump_sub // cutoff at 40% OPE for submersible pumps
gen ope_high = ope > 50 // cutoff at 50% OPE
replace ope_high = ope > 40 if pump_sub // cutoff at 40% OPE for submersible pumps

** Construct kWh savings using test data
gen ope_save_after = 1 - ope / ope_after // percent savings relative to OPE after project
gen ope_save_pre = ope_after / ope - 1 // percent savings relative to OPE before project
gen con_savings_kwh_yr_test_pre = ope_low * 0.25 * kwhperyr + ///
	ope_high * ope_save_after * kwhperyr // constructed kwh/yr savings from existing consumption recorded in test
gen con_savings_basis_test = min(ope_save_pre, .5) * kwhperyr_after // constructed savings basis for predicted savings
gen con_savings_kwh_yr_test_post = ope_low * 0.25 * (kwhperyr_after + con_savings_basis_test) + ///
	ope_high * con_savings_basis_test // constructed kwh/yr savings from predicted consumption recorded in test

** Prepare dataset for regressions
keep if last_test_before // drop tests after a project
gen project = (date_proj_finish != .) // create indicator for project

** Run RDs for non-submersible pumps
gen diff_run_30 = ope - 30
gen treat_30 = (ope > 30)
gen treat_diff_run_30 = treat_30 * diff_run_30
gen diff_run_50 = ope - 50
gen treat_50 = (ope > 50)
gen treat_diff_run_50 = treat_50 * diff_run_50
reg project treat_30 diff_run_30 treat_diff_run_30 if pump_sub == 0 & ope >= 20 & ope <= 40
reg project treat_50 diff_run_50 treat_diff_run_50 if pump_sub == 0 & ope >= 40 & ope <= 60

** Run RDs for submersible pumps
gen diff_run_20 = ope - 20
gen treat_20 = (ope > 20)
gen treat_diff_run_20 = treat_20 * diff_run_20
gen diff_run_40 = ope - 40
gen treat_40 = (ope > 40)
gen treat_diff_run_40 = treat_40 * diff_run_40
reg project treat_20 diff_run_20 treat_diff_run_20 if pump_sub == 1 & ope >= 10 & ope <= 30
reg project treat_40 diff_run_40 treat_diff_run_40 if pump_sub == 1 & ope >= 30 & ope <= 50

** Create figure of project vs. binned OPE	
gen ope_bin = ceil(ope) - 1
gen count = 1
collapse (mean) project (count) count, by(ope_bin pump_sub)
scatter project ope_bin if pump_sub == 0 || scatter project ope_bin if pump_sub == 1
