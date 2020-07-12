clear all
version 13
set more off

*************************************************************************
** Script to model crop choice as a multinomial probit discreet choice **
*************************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

*************************************************************************
*************************************************************************

** estimate model with county and year fixed effects 

** load annual water dataset at CLU level
use "$dirpath_data/merged_pge/clu_annual_water_panel.dta", clear

// drop bad observations
keep if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & flag_weird_pump==0
drop if mode_Annual + mode_FruitNutPerennial + mode_OtherPerennial + mode_Noncrop > 1
	
// create modal crop type factor variable
gen mode_crop = 1
replace mode_crop = 2 if mode_Annual
replace mode_crop = 3 if mode_FruitNutPerennial
replace mode_crop = 4 if mode_OtherPerennial

// drop counties missing at least one crop type
egen temp_tag = tag(mode_crop county_group)
egen temp_sum = sum(temp_tag), by(county_group)
drop if temp_sum < 4
drop temp*

// save number of observations and number of CLUs
qui distinct clu_id
local n_obs = r(N)
local n_clu = r(ndistinct)
	
** estimate model
set seed 321
cmp (mode_crop = mean_p_af_rast_dd_mth_2SP i.year i.county_group, iia) (mean_p_af_rast_dd_mth_2SP = mean_p_kwh_ag_default i.year i.county_group), nolr ind($cmp_mprobit $cmp_cont) vce(cluster clu_id)

** calculate first-stage chi-squared statistic
qui test mean_p_kwh_ag_default
local chi2 = r(chi2)

** calculate average marginal effects and average semi-elasticities
// loop over outcomes
foreach out in 1 2 3 4 {
	
	// average marginal effect
	margins, dydx(mean_p_af_rast_dd_mth_2SP) predict(eq(#`out') pr)
	mat b = r(b)
	local marg_`out' = b[1,1]
	mat V = r(V)
	local marg_se_`out' = V[1,1]
		
	// average semi-elasticity
	margins, dyex(mean_p_af_rast_dd_mth_2SP) predict(eq(#`out') pr)
	mat b = r(b)
	local elas_`out' = b[1,1]
	mat V = r(V)
	local elas_se_`out' = V[1,1]
}

** simulate counterfactual acreage by crop type under groundwater tax scenarios
// save water price for counterfactual simulations
gen p_water = mean_p_af_rast_dd_mth_2SP

// predict counterfactual probabilities under groundwater tax scenarios
foreach tax in 0 5 10 15 20 25 {

	// set counterfactual groundwater price
	replace mean_p_af_rast_dd_mth_2SP = p_water + `tax'
	
	// loop over outcomes at each tax level
	foreach out in 1 2 3 4 {
		
		// predict probability of crop choice and expected acreage
		predict prob_`tax'tax_`out', pr eq(#`out')
		gen acres_`tax'tax_`out' = prob_`tax'tax_`out' * cluacres
	}
}

// calculate counterfactual acreage and format
collapse (sum) acres*tax*, by(year)
collapse (mean) acres*tax*
gen temp = 1
reshape long acres_0tax_ acres_5tax_ acres_10tax_ acres_15tax_ acres_20tax_ acres_25tax_, i(temp) j(outcome)
drop temp
rename acres*_ acres*

** save results
// initialize results
gen crop_type = ""
replace crop_type = "no crop" if outcome == 1
replace crop_type = "annual" if outcome == 2
replace crop_type = "fruit or nut perennial" if outcome == 3
replace crop_type = "other perennial" if outcome == 4
gen model = 1
gen fes = "county and year"
gen mfx = .
gen mfx_se = .
gen elas = .
gen elas_se = .
		
// loop over outcomes
foreach out in 1 2 3 4 {
	
	// record results
	replace mfx = `marg_`out'' if outcome == `out'
	replace mfx_se = sqrt(`marg_se_`out'') if outcome == `out'
	replace elas = `elas_`out'' if outcome == `out'
	replace elas_se = sqrt(`elas_se_`out'') if outcome == `out'
}

// record additional results
gen n_obs = `n_obs'
gen n_clu = `n_clu'
gen chi2 = `chi2'

// save results
order model fes outcome crop_type mfx mfx_se elas elas_se acres* n_obs n_clu chi2
save "$dirpath_data/results/probit_crop_choice.dta", replace

*************************************************************************
*************************************************************************

** estimate model with county-by-year fixed effects 

** load annual water dataset at CLU level
use "$dirpath_data/merged_pge/clu_annual_water_panel.dta", clear

// drop bad observations
keep if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & flag_weird_pump==0
drop if mode_Annual + mode_FruitNutPerennial + mode_OtherPerennial + mode_Noncrop > 1
	
// create modal crop type factor variable
gen mode_crop = 1
replace mode_crop = 2 if mode_Annual
replace mode_crop = 3 if mode_FruitNutPerennial
replace mode_crop = 4 if mode_OtherPerennial

// drop county-years missing at least one crop type
egen temp_tag = tag(mode_crop county_group year)
egen temp_sum = sum(temp_tag), by(county_group year)
drop if temp_sum < 4
drop temp*

// save number of observations and number of CLUs
qui distinct clu_id
local n_obs = r(N)
local n_clu = r(ndistinct)
	
// estimate model
set matsize 1000
set seed 321
cmp (mode_crop = mean_p_af_rast_dd_mth_2SP i.year#i.county_group, iia) (mean_p_af_rast_dd_mth_2SP = mean_p_kwh_ag_default i.year#i.county_group), nolr ind($cmp_mprobit $cmp_cont) vce(cluster clu_id)

** calculate first-stage chi-squared statistic
qui test mean_p_kwh_ag_default
local chi2 = r(chi2)

** calculate average marginal effects and average semi-elasticities
// loop over outcomes
foreach out in 1 2 3 4 {
	
	// average marginal effect
	margins, dydx(mean_p_af_rast_dd_mth_2SP) predict(eq(#`out') pr)
	mat b = r(b)
	local marg_`out' = b[1,1]
	mat V = r(V)
	local marg_se_`out' = V[1,1]
		
	// average semi-elasticity
	margins, dyex(mean_p_af_rast_dd_mth_2SP) predict(eq(#`out') pr)
	mat b = r(b)
	local elas_`out' = b[1,1]
	mat V = r(V)
	local elas_se_`out' = V[1,1]
}

** simulate counterfactual acreage by crop type under groundwater tax scenarios
// save water price for counterfactual simulations
gen p_water = mean_p_af_rast_dd_mth_2SP

// predict counterfactual probabilities under groundwater tax scenarios
foreach tax in 0 5 10 15 20 25 {

	// set counterfactual groundwater price
	replace mean_p_af_rast_dd_mth_2SP = p_water + `tax'
	
	// loop over outcomes at each tax level
	foreach out in 1 2 3 4 {
		
		// predict probability of crop choice and expected acreage
		predict prob_`tax'tax_`out', pr eq(#`out')
		gen acres_`tax'tax_`out' = prob_`tax'tax_`out' * cluacres
	}
}

// save interim results at CLU level
preserve
collapse (mean) prob*tax* acres*tax*, by(clu_id)
decode clu_id, gen(temp)
drop clu_id
rename temp clu_id
order clu_id *
save "$dirpath_data/results/probit_crop_choice_clu.dta", replace
restore

// calculate counterfactual acreage and format
collapse (sum) acres*tax*, by(year)
collapse (mean) acres*tax*
gen temp = 1
reshape long acres_0tax_ acres_5tax_ acres_10tax_ acres_15tax_ acres_20tax_ acres_25tax_, i(temp) j(outcome)
drop temp
rename acres*_ acres*

** save results
// initialize results
gen crop_type = ""
replace crop_type = "no crop" if outcome == 1
replace crop_type = "annual" if outcome == 2
replace crop_type = "fruit or nut perennial" if outcome == 3
replace crop_type = "other perennial" if outcome == 4
gen model = 2
gen fes = "county-by-year"
gen mfx = .
gen mfx_se = .
gen elas = .
gen elas_se = .
		
// loop over outcomes
foreach out in 1 2 3 4 {
	
	// record results
	replace mfx = `marg_`out'' if outcome == `out'
	replace mfx_se = sqrt(`marg_se_`out'') if outcome == `out'
	replace elas = `elas_`out'' if outcome == `out'
	replace elas_se = sqrt(`elas_se_`out'') if outcome == `out'
}

// record additional results
gen n_obs = `n_obs'
gen n_clu = `n_clu'
gen chi2 = `chi2'

// save results
order model fes outcome crop_type mfx mfx_se elas elas_se acres* n_obs n_clu chi2
append using "$dirpath_data/results/probit_crop_choice.dta"
sort model outcome
save "$dirpath_data/results/probit_crop_choice.dta", replace
