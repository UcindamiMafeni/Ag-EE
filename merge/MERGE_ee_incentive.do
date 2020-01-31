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
	"$dirpath_data/pge_cleaned/pump_test_data.dta"
	
** Keep only relevant tests/projects
keep if customertype == "Individ Farms" // keep only farms
keep if waterenduse == "agriculture" | waterenduse == "irrigation" // keep only ag
keep if flag_date_problem != . // keep only projects

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

** Save merged dataset of tests and projects
save `test_proj'

** Join project and test data with monthly bill data
joinby sp_uuid using "$dirpath_data/merged/sp_month_elec_panel.dta"

** Calculate electricity consumption for years before and after project
gen month_proj_finish = mofd(date_proj_finish) // month of project
format month_proj_finish %tm // format month of project
gen month_pre_proj = month_proj_finish - modate // months before project
gen year_pre_proj = (month_pre_proj >= 0) * ceil(month_pre_proj / 12) + ///
	(month_pre_proj < 0) * floor(month_pre_proj / 12) // map months to years
gen year_proj_finish = yofd(date_proj_finish) // year of project
gen calyr_pre_proj = year_proj_finish - year // calendar years before project
preserve
collapse (sum) year_kwh_ = mnth_bill_kwh (count) bills_in_year = mnth_bill_kwh, ///
	by(sp_uuid date_proj_finish test_date_stata apeptestid year_pre_proj) // collapse to year pre-project
keep if inlist(year_pre_proj, -1, 1, 2, 3, 4, 5) // keep five years pre and one year post
replace year_pre_proj = 0 if year_pre_proj == -1 // year 0 is post-project year
keep if bills_in_year == 12 // keep only full years
drop bills_in_year
reshape wide year_kwh_, ///
	i(sp_uuid date_proj_finish test_date_stata apeptestid) j(year_pre_proj) // long to wide
save `kwh_year'
restore
collapse (sum) calyr_kwh_ = mnth_bill_kwh (count) bills_in_calyr = mnth_bill_kwh, ///
	by(sp_uuid date_proj_finish test_date_stata apeptestid calyr_pre_proj) // collapse to calendar year pre-project
keep if inlist(calyr_pre_proj, 1, 2, 3, 4, 5) // keep five calendar years pre-project
keep if bills_in_calyr == 12 // keep only full years
drop bills_in_calyr
reshape wide calyr_kwh_, ///
	i(sp_uuid date_proj_finish test_date_stata apeptestid) j(calyr_pre_proj)  // long to wide
merge 1:1 sp_uuid date_proj_finish test_date_stata apeptestid using `kwh_year', nogen // merge year and calendar year consumption
forvalues i = 2 / 5 { // loop over years 2 to 5
	egen year_kwh_`i'_mean = rowmean(year_kwh_1 - year_kwh_`i') // mean of i years
	egen year_kwh_`i'_max = rowmax(year_kwh_1 - year_kwh_`i') // max of i years
	egen calyr_kwh_`i'_mean = rowmean(calyr_kwh_1 - calyr_kwh_`i') // mean of i calendar years
	egen calyr_kwh_`i'_max = rowmax(calyr_kwh_1 - calyr_kwh_`i') // max of i calendar years
}
drop *2 *3 *4 *5

** Construct kWh savings using consumption data
merge 1:1 sp_uuid date_proj_finish test_date_stata apeptestid using `test_proj', gen(merge_test_proj)
gen con_savings_kwh_yr_year_1 = ope_low * 0.25 * year_kwh_1 + ///
	ope_high * ope_save_after * year_kwh_1 // constructed kwh/yr savings using year pre-project
gen con_savings_kwh_yr_calyr_1 = ope_low * 0.25 * calyr_kwh_1 + ///
	ope_high * ope_save_after * calyr_kwh_1 // constructed kwh/yr savings using calendar year pre-project
forvalues i = 2 / 5 { // loop over years 2 to 5
	gen con_savings_kwh_yr_year_`i'_mean = ope_low * 0.25 * year_kwh_`i'_mean + ///
		ope_high * ope_save_after * year_kwh_`i'_mean // constructed kwh/yr savings using mean of i years pre-project
	gen con_savings_kwh_yr_year_`i'_max = ope_low * 0.25 * year_kwh_`i'_max + ///
		ope_high * ope_save_after * year_kwh_`i'_max // constructed kwh/yr savings using max of i years pre-project
	gen con_savings_kwh_yr_calyr_`i'_mean = ope_low * 0.25 * calyr_kwh_`i'_mean + ///
		ope_high * ope_save_after * calyr_kwh_`i'_mean // constructed kwh/yr savings using mean of i calendar years pre-project
	gen con_savings_kwh_yr_calyr_`i'_max = ope_low * 0.25 * calyr_kwh_`i'_max + ///
		ope_high * ope_save_after * calyr_kwh_`i'_max // constructed kwh/yr savings using max of i calendar years pre-project
}
gen con_savings_basis_year_0 = min(ope_save_pre, .5) * year_kwh_0 // constructed savings basis for year post-project
gen con_savings_kwh_yr_year_0 = ope_low * 0.25 * (year_kwh_0 + con_savings_basis_year_0) + ///
	ope_high * con_savings_basis_year_0 // constructed kwh/yr savings using year post-project

** Apply kWh rate based on date of project
gen kwh_rate = .09 // $.09/kWh for projects before 5/15/14
replace kwh_rate = .12 if date_proj_finish >= date("15may2014", "DMY") // $.12/kWh for projects between 5/15/14 and 3/31/17
replace kwh_rate = .08 if date_proj_finish >= date("31march2017", "DMY") // $.08/kWH for projects after 3/31/17

** Construct kW savings
gen con_savings_kw = .07159 if ope_low // kW save rate for low OPE pumps
replace con_savings_kw = .009694 * ope_save_after / 5 if ope_high // kW save rate for high OPE pumps with savings < 5%
replace con_savings_kw = .009694 + (.02237 - .009694) * (ope_save_after - 5) / 5 ///
	if ope_high & ope_save_after > 5 // kW save rate for high OPE pumps with savings 5-10%
replace con_savings_kw = .02237 + (.03729 - .02237) * (ope_save_after - 10) / 5 ///
	if ope_high & ope_save_after > 10 // kW save rate for high OPE pumps with savings 10-15%
replace con_savings_kw = .03729 + (.05369 - .03729) * (ope_save_after - 15) / 5 ///
	if ope_high & ope_save_after > 15 // kW save rate for high OPE pumps with savings 15-20%
replace con_savings_kw = .05369 + (.07159 - .05369) * (ope_save_after - 20) / 5 ///
	if ope_high & ope_save_after > 20 // kW save rate for high OPE pumps with savings 20-25%
replace con_savings_kw = .07159 + (.08874 - .07159) * (ope_save_after - 25) / 5 ///
	if ope_high & ope_save_after > 25 // kW save rate for high OPE pumps with savings 25-30%
replace con_savings_kw = .08874 + (.1044 - .08874) * (ope_save_after - 30) / 5 ///
	if ope_high & ope_save_after > 30 // kW save rate for high OPE pumps with savings 30-35%
replace con_savings_kw = .1044 + (.1163 - .1044) * (ope_save_after - 35) / 5 ///
	if ope_high & ope_save_after > 35 // kW save rate for high OPE pumps with savings 35-40%
replace con_savings_kw = .1163 + (.126 - .1163) * (ope_save_after - 40) / 5 ///
	if ope_high & ope_save_after > 40 // kW save rate for high OPE pumps with savings 40-45%
replace con_savings_kw = .126 + (.132 - .126) * (ope_save_after - 45) / 5 ///
	if ope_high & ope_save_after > 45 // kW save rate for high OPE pumps with savings 45-50%
replace con_savings_kw = .132 + (.1357 - .132) * (ope_save_after - 50) / 5 ///
	if ope_high & ope_save_after > 50 // kW save rate for high OPE pumps with savings 50-55%
replace con_savings_kw = .1357 + (.184 - .1357) * (ope_save_after - 55) / 5 ///
	if ope_high & ope_save_after > 55 // kW save rate for high OPE pumps with savings 55-60%
	replace con_savings_kw = .184 if ope_high & ope_save_after > 60 // kW save rate for high OPE pumps with savings > 60%

** Apply kW rate based on date of project
gen kw_rate = 0 // $0/kW for projects before 1/1/11
replace kw_rate = 100 if date_proj_finish >= date("01jan2011", "DMY") // $100/kW for projects between 1/1/11 and 7/1/14
replace kw_rate = 150 if date_proj_finish >= date("01july2014", "DMY") // $150/kW for projects between 7/1/14 and 1/19/2016
replace kw_rate = 0 if date_proj_finish >= date("19jan2016", "DMY") // $0/kW for projects after 1/19/16
	
** Construct total incentive amount for each constructed kWh savings
gen con_subsidy_proj_test_pre = con_savings_kwh_yr_test_pre * kwh_rate + ///
	con_savings_kw * kw_rate * hp_nameplate // constructed incentive using existing consumption recorded in test
gen con_subsidy_proj_test_post = con_savings_kwh_yr_test_post * kwh_rate + ///
	con_savings_kw * kw_rate * hp_nameplate // constructed incentive using predicted consumption recorded in test
gen con_subsidy_proj_year_1 = con_savings_kwh_yr_year_1 * kwh_rate + ///
	con_savings_kw * kw_rate * hp_nameplate // constructed incentive using year pre-project
gen con_subsidy_proj_calyr_1 = con_savings_kwh_yr_calyr_1 * kwh_rate + ///
	con_savings_kw * kw_rate * hp_nameplate // constructed incentive using calendar year pre-project
forvalues i = 2 / 5 { // loop over years 2 to 5
	gen con_subsidy_proj_year_`i'_mean = con_savings_kwh_yr_year_`i'_mean * kwh_rate + ///
		con_savings_kw * kw_rate * hp_nameplate // constructed incentive using mean of i years pre-project
	gen con_subsidy_proj_year_`i'_max = con_savings_kwh_yr_year_`i'_max * kwh_rate + ///
		con_savings_kw * kw_rate * hp_nameplate // constructed incentive using max of i years pre-project
	gen con_subsidy_proj_calyr_`i'_mean = con_savings_kwh_yr_calyr_`i'_mean * kwh_rate + ///
		con_savings_kw * kw_rate * hp_nameplate // constructed incentive using mean of i calendar years pre-project
	gen con_subsidy_proj_calyr_`i'_max = con_savings_kwh_yr_calyr_`i'_max * kwh_rate + ///
		con_savings_kw * kw_rate * hp_nameplate // constructed incentive using max of i calendar years pre-project
}
gen con_subsidy_proj_year_0 = con_savings_kwh_yr_year_0 * kwh_rate + ///
	con_savings_kw * kw_rate * hp_nameplate // constructed incentive using year post-project
	
/*
** Format dataset for regressions
keep if test_date_stata == date_test_pre // keep only test used for project pre-test
gen early = date_proj_finish < date("08april2015", "DMY") // rules change on 1 Aug 2014
gen late = 1 - early // later projects use calendar years rather than 12-month periods
gen ope_group = "" // ope group in regression results
gen time_period = "" // time period in regression results
gen con_savings_var = "" // constructed savings variable in regression results
gen coeff = . // coefficient in regression results
gen r2 = . // R^2 in regression results
gen obs = . // observations in regression results
local reg_suffix "test_pre test_post year_0 year_1 calyr_1" // start list of regression suffixes
forvalues i = 2 / 5 { // loop over years 2 to 5
	foreach y in year calyr { // loop over 12-month year and calendar year
		foreach m in mean max { // loop over mean and max
			local reg_suffix "`reg_suffix' `y'_`i'_`m'" // add to list of suffixes
		}
	}
}

** Run regressions for each data group and save results
local n = 1 // set index for regressions
foreach o in low high { // loop over ope group
	foreach t in early late { // loop over time period
		foreach s in `reg_suffix' { // loop over variable suffixes
			reg est_savings_kwh_yr con_savings_kwh_yr_`s' if ope_`o' & `t' // run regresssion
			replace ope_group = "`o'" if _n == `n' // record ope group
			replace time_period = "`t'" if _n == `n' // record time period
			replace con_savings_var = "`s'" if _n == `n' // record constructed savings variable
			replace coeff = _b[con] if _n == `n' // records coefficient
			replace r2 = e(r2) if _n == `n' // record R^2
			replace obs = e(N) if _n == `n' // record number of observations
			local n = `n' + 1 // increment regression index
		}
	}
}
keep ope_group-obs // keep regression results
drop if coeff == . // drop empty rows
*/

** Format dataset for regressions
keep if test_date_stata == date_test_pre // keep only test used for project pre-test
gen early = date_proj_finish < date("08april2015", "DMY") // rules change on 1 Aug 2014
gen late = 1 - early // later projects use calendar years rather than 12-month periods
gen ope_group = "" // ope group in regression results
gen time_period = "" // time period in regression results
gen con_savings_var = "" // constructed savings variable in regression results
gen coeff = . // coefficient in regression results
gen r2 = . // R^2 in regression results
gen obs = . // observations in regression results
local reg_suffix "test_pre test_post year_0 year_1 calyr_1" // start list of regression suffixes
forvalues i = 2 / 5 { // loop over years 2 to 5
	foreach y in year calyr { // loop over 12-month year and calendar year
		foreach m in mean max { // loop over mean and max
			local reg_suffix "`reg_suffix' `y'_`i'_`m'" // add to list of suffixes
		}
	}
}

** Run regressions for each data group and save results
local n = 1 // set index for regressions
foreach o in low high { // loop over ope group
	foreach t in early late { // loop over time period
		foreach s in `reg_suffix' { // loop over variable suffixes
			reg subsidy_proj con_subsidy_proj_`s' if ope_`o' & `t' // run regresssion
			replace ope_group = "`o'" if _n == `n' // record ope group
			replace time_period = "`t'" if _n == `n' // record time period
			replace con_savings_var = "`s'" if _n == `n' // record constructed savings variable
			replace coeff = _b[con] if _n == `n' // records coefficient
			replace r2 = e(r2) if _n == `n' // record R^2
			replace obs = e(N) if _n == `n' // record number of observations
			local n = `n' + 1 // increment regression index
		}
	}
}
keep ope_group-obs // keep regression results
drop if coeff == . // drop empty rows
