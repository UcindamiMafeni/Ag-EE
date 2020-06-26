clear all
version 13
set more off

*************************************************************************
** Script to model crop choice as a multinomial probit discreet choice **
*************************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

************************************************
************************************************

** load annual water dataset
use "$dirpath_data/merged/sp_annual_water_panel.dta", clear

// drop bad observations
keep if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & flag_weird_pump==0
drop if mode_Annual + mode_FruitNutPerennial + mode_OtherPerennial + mode_Noncrop > 1
	
// create modal crop type factor variable
gen mode_crop = 0
replace mode_crop = 1 if mode_Annual
replace mode_crop = 2 if mode_FruitNutPerennial
replace mode_crop = 3 if mode_OtherPerennial

** estimate model with county and year fixed effects 
// drop counties missing at least one crop type
egen temp_tag = tag(mode_crop county_group)
egen temp_sum = sum(temp_tag), by(county_group)
drop if temp_sum < 4
drop temp*
	
// estimate model
set seed 321
cmp (mode_crop = mean_p_af_rast_dd_mth_2SP i.year i.county_group, iia) (mean_p_af_rast_dd_mth_2SP = mean_p_kwh_ag_default i.year i.county_group), nolr ind($cmp_mprobit $cmp_cont)
	
// loop over outcomes to get marginal effects and semi-elasticities
foreach out in 1 2 3 4 {
	
	// marginal effect
	margins, dydx(mean_p_af_rast_dd_mth_2SP) predict(eq(#`out') pr)
	mat b = r(b)
	local mod1_marg_`out' = b[1,1]
	mat V = r(V)
	local mod1_marg_se_`out' = V[1,1]
		
	// marginal effect
	margins, dyex(mean_p_af_rast_dd_mth_2SP) predict(eq(#`out') pr)
	mat b = r(b)
	local mod1_elas_`out' = b[1,1]
	mat V = r(V)
	local mod1_elas_se_`out' = V[1,1]
}

** estimate model with county-by-year fixed effects 
// drop county-years missing at least one crop type
egen temp_tag = tag(mode_crop county_group year)
egen temp_sum = sum(temp_tag), by(county_group year)
drop if temp_sum < 4
drop temp*
	
// estimate model
set matsize 1000
set seed 321
cmp (mode_crop = mean_p_af_rast_dd_mth_2SP i.year#i.county_group, iia) (mean_p_af_rast_dd_mth_2SP = mean_p_kwh_ag_default i.year#i.county_group), nolr ind($cmp_mprobit $cmp_cont)
	
// loop over outcomes to get marginal effects and semi-elasticities
foreach out in 1 2 3 4 {

	// marginal effect
	margins, dydx(mean_p_af_rast_dd_mth_2SP) predict(eq(#`out') pr)
	mat b = r(b)
	local mod2_marg_`out' = b[1,1]
	mat V = r(V)
	local mod2_marg_se_`out' = V[1,1]
		
	// marginal effect
	margins, dyex(mean_p_af_rast_dd_mth_2SP) predict(eq(#`out') pr)
	mat b = r(b)
	local mod2_elas_`out' = b[1,1]
	mat V = r(V)
	local mod2_elas_se_`out' = V[1,1]
}
	
** save results
// initialize results
gen model = .
gen outcome = .
gen fes = ""
gen crop_type = ""
gen mfx = .
gen mfx_se = .
gen elas = .
gen elas_se = .
keep model outcome fes crop_type mfx mfx_se elas elas_se
local i = 1
		
// loop over models
foreach mod in 1 2 {
	
	// loop over outcomes
	foreach out in 1 2 3 4 {
		
		// record results
		replace model = `mod' if _n == `i'
		replace outcome = `out' if _n == `i'
		if `mod' == 1 {
			replace fes = "county and year" if _n == `i'
		}
		else {
			replace fes = "county-by-year" if _n == `i'
		}
		if `out' == 1 {
			replace crop_type = "no crop" if _n == `i'
		}
		else if `out' == 2 {
			replace crop_type = "annual" if _n == `i'
		}
		else if `out' == 3 {
			replace crop_type = "fruit or nut perennial" if _n == `i'
		}
		else {
			replace crop_type = "other perennial" if _n == `i'
		}
		replace mfx = `mod`mod'_marg_`out'' if _n == `i'
		replace mfx_se = sqrt(`mod`mod'_marg_se_`out'') if _n == `i'
		replace elas = `mod`mod'_elas_`out'' if _n == `i'
		replace elas_se = sqrt(`mod`mod'_elas_se_`out'') if _n == `i'
		local i = `i'+1
	}
}

// save results
drop if model == .
save "$dirpath_data/results/logit_crop_choice.dta", replace
