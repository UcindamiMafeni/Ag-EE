clear all
version 13
set more off

*********************************************************************************
** Script to run monthly electricity regressions -- sensitivities to main spec **
*********************************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

************************************************
************************************************

** 1. Monthly regressions
{ 

// Load monthly panel
use "$dirpath_data/merged/sp_month_elec_panel.dta", clear
local panel = "monthly"

// Keep APEP data pull only
drop if pull!="20180719" 
local pull = "20180719" 

// Drop solar NEM customers
keep if flag_nem==0 

// Drop bad geocodes (i.e. not in PGE service territory, or California)
keep if flag_geocode_badmiss==0 

// Drop irregular bills (first bill, last bill, long bill, short bill, etc.)
keep if flag_irregular_bill==0 

// Drop weird customers (non-ag rates, irrigation districts, etc.)
keep if flag_weird_cust==0 

// Drop SPs that don't merge into APEP dataset (for consistency with water regressiosn)
keep if merge_sp_water_panel==3
	
// Create empty variables to populate for storign results
gen panel = ""
drop pull
gen pull = ""
gen sens = ""
gen sample = ""
gen depvar = ""
gen fes = ""
gen rhs = ""
gen beta_log_p_mean = .
gen se_log_p_mean = .
gen t_log_p_mean = .
gen vce = ""
gen n_obs = .
gen n_SPs = .
gen n_modates = .
gen dof = .
gen fstat_rk = .
gen fs_beta_default = .
gen fs_se_default = .
gen fs_t_default = .
gen fs_beta_modal = .
gen fs_se_modal = .
gen fs_t_modal = .
gen fs_beta_deflag12 = .
gen fs_se_deflag12 = .
gen fs_t_deflag12 = .
gen fs_beta_deflag6 = .
gen fs_se_deflag6 = .
gen fs_t_deflag6 = .
gen fs_beta_modlag12 = .
gen fs_se_modlag12 = .
gen fs_t_modlag12 = .
gen fs_beta_modlag6 = .
gen fs_se_modlag6 = .
gen fs_t_modlag6 = .

// Define default global: sample
global if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3"
	
// Define default global: dependent variable
global DEPVAR = "ihs_kwh"
	
// Define default global: RHS	
global RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
	
// Define default global: FEs
global FEs = "sp_group#month sp_group#rt_large_ag modate"
	
// Define default global: cluster variables
global VCE = "sp_group modate"	

	
	
// Loop over sensitivities
forvalues c = 1/25 {

	// Reset default locals
	local if_sample = "${if_sample}"
	local DEPVAR = "${DEPVAR}"
	local RHS = "${RHS}"
	local FEs = "${FEs}"
	local VCE = "${VCE}"

	if `c'==1 {
		local sens = "Basin by month-of-sample FEs"
		local FEs = "sp_group#month sp_group#rt_large_ag modate#basin_group" 
	}
	if `c'==2 {
		local sens = "IV with modal tariff, not default tariff"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_modal)"
	}
	if `c'==3 {
		local sens = "IV with lagged modal tariff, not default tariff"
		local RHS = "(log_p_mean = log_p_mean_modlag*)"
	}
	if `c'==4 {
		local sens = "Depvar: log(Q)"
		local DEPVAR = "log_kwh"
	}
	if `c'==5 {
		local sens = "Depvar: log(1+Q)"
		local DEPVAR = "log1_kwh"
	}
	if `c'==6 {
		local sens = "Depvar: log(100*Q)"
		local DEPVAR = "log_100kwh"
	}
	if `c'==7 {
		local sens = "Depvar: log(1+100*Q)"
		local DEPVAR = "log1_100kwh"
	}
	if `c'==8 {
		local sens = "Within-category switchers only"
		local if_sample = "${if_sample} & sp_same_rate_in_cat==0"
	}
	if `c'==9 {
		local sens = "Within-category non-switchers only"
		local if_sample = "${if_sample} & sp_same_rate_in_cat==1"
	}
	if `c'==10 {
		local sens = "Within 60 months of pump test"
		local if_sample = "${if_sample} & months_to_nearest_test<=60"
	}
	if `c'==11 {
		local sens = "Within 48 months of pump test"
		local if_sample = "${if_sample} & months_to_nearest_test<=48"
	}
	if `c'==12 {
		local sens = "Within 36 months of pump test"
		local if_sample = "${if_sample} & months_to_nearest_test<=36"
	}
	if `c'==13 {
		local sens = "Within 24 months of pump test"
		local if_sample = "${if_sample} & months_to_nearest_test<=24"
	}
	if `c'==14 {
		local sens = "Dropping APEP-subsidized projects"
		local if_sample = "${if_sample} & apep_proj_count==0"
	}
	if `c'==15 {
		local sens = "Latlons within 100 miles"
		local if_sample = "${if_sample} & latlon_miles_apart<=100"
	}
	if `c'==16 {
		local sens = "Latlons within 50 miles"
		local if_sample = "${if_sample} & latlon_miles_apart<=50"
	}
	if `c'==17 {
		local sens = "Latlons within 25 miles"
		local if_sample = "${if_sample} & latlon_miles_apart<=25"
	}
	if `c'==18 {
		local sens = "Latlons within 10 miles"
		local if_sample = "${if_sample} & latlon_miles_apart<=10"
	}
	if `c'==19 {
		local sens = "Latlons within 5 miles"
		local if_sample = "${if_sample} & latlon_miles_apart<=5"
	}
	if `c'==20 {
		local sens = "Latlons within 1 miles"
		local if_sample = "${if_sample} & latlon_miles_apart<=1"
	}
	if `c'==21 {
		local sens = "Summer months only"
		local if_sample = "${if_sample} & summer==1"
	}
	if `c'==22 {
		local sens = "Winter months only"
		local if_sample = "${if_sample} & summer==0"
	}
	if `c'==23 {
		local sens = "San Joaquin basin only"
		local if_sample = "${if_sample} & basin_group==122"
	}
	if `c'==24 {
		local sens = "Sacramento basin only"
		local if_sample = "${if_sample} & basin_group==121"
	}
	if `c'==25 {
		local sens = "Salinas basin only"
		local if_sample = "${if_sample} & basin_group==68"
	}
	
	// Run non-IV specification	
	if regexm("`RHS'","=")==0 {
					
		// Run OLS regression
		reghdfe `DEPVAR' `RHS' `if_sample' , absorb(`FEs') vce(cluster `VCE')
		
		// Store results
		replace panel = "`panel'" in `c'
		replace pull = "`pull'" in `c'
		replace sens = "`sens'" in `c'
		replace sample = "`if_sample'" in `c'
		replace depvar = "`DEPVAR'" in `c'
		replace fes = "`FEs'" in `c'
		replace rhs = "`RHS'" in `c'
		replace beta_log_p_mean = _b[log_p_mean] in `c'
		replace se_log_p_mean = _se[log_p_mean] in `c'
		replace t_log_p_mean =  _b[log_p_mean]/_se[log_p_mean] in `c'
		replace vce = "cluster `VCE'" in `c'
		replace n_obs = e(N) in `c'
		replace n_SPs = e(N_clust1) in `c'
		replace n_modates = e(N_clust2) in `c'
		replace dof = e(df_r) in `c'
				
	}
	
	// Run IV specifications
	else {
					
		// Run 2SLS regression
		ivreghdfe `DEPVAR' `RHS' `if_sample', absorb(`FEs') cluster(`VCE')
		
		// Store results
		replace panel = "`panel'" in `c'
		replace pull = "`pull'" in `c'
		replace sens = "`sens'" in `c'
		replace sample = "`if_sample'" in `c'
		replace depvar = "`DEPVAR'" in `c'
		replace fes = "`FEs'" in `c'
		replace rhs = "`RHS'" in `c'
		replace beta_log_p_mean = _b[log_p_mean] in `c'
		replace se_log_p_mean = _se[log_p_mean] in `c'
		replace t_log_p_mean =  _b[log_p_mean]/_se[log_p_mean] in `c'
		replace vce = "cluster `VCE'" in `c'
		replace n_obs = e(N) in `c'
		replace n_SPs = e(N_clust1) in `c'
		replace n_modates = e(N_clust2) in `c'
		replace dof = e(df_r) in `c'
		replace fstat_rk = e(rkf) in `c'
				
		// Run first stage regression
		local RHSfs = subinstr(subinstr(subinstr("`RHS'","(","",.),")","",.),"=","",.)
		reghdfe `RHSfs' , absorb(`FEs') vce(cluster `VCE')
		
		if regexm("`RHSfs'","log_mean_p_kwh_ag_default") {
			replace fs_beta_default = _b[log_mean_p_kwh_ag_default] in `c'
			replace fs_se_default = _se[log_mean_p_kwh_ag_default] in `c'
			replace fs_t_default =  _b[log_mean_p_kwh_ag_default]/_se[log_mean_p_kwh_ag_default] in `c'
		}
		else if regexm("`RHSfs'","log_mean_p_kwh_ag_modal") {
			replace fs_beta_modal = _b[log_mean_p_kwh_ag_modal] in `c'
			replace fs_se_modal = _se[log_mean_p_kwh_ag_modal] in `c'
			replace fs_t_modal =  _b[log_mean_p_kwh_ag_modal]/_se[log_mean_p_kwh_ag_modal] in `c'
		}
		else if regexm("`RHSfs'","log_p_mean_deflag*") {
			replace fs_beta_deflag12 = _b[log_p_mean_deflag12] in `c'
			replace fs_se_deflag12 = _se[log_p_mean_deflag12] in `c'
			replace fs_t_deflag12 =  _b[log_p_mean_deflag12]/_se[log_p_mean_deflag12] in `c'
			replace fs_beta_deflag6 = _b[log_p_mean_deflag6] in `c'
			replace fs_se_deflag6 = _se[log_p_mean_deflag6] in `c'
			replace fs_t_deflag6 =  _b[log_p_mean_deflag6]/_se[log_p_mean_deflag6] in `c'
		}				
		else if regexm("`RHSfs'","log_p_mean_modlag*") {
			replace fs_beta_modlag12 = _b[log_p_mean_modlag12] in `c'
			replace fs_se_modlag12 = _se[log_p_mean_modlag12] in `c'
			replace fs_t_modlag12 =  _b[log_p_mean_modlag12]/_se[log_p_mean_modlag12] in `c'
			replace fs_beta_modlag6 = _b[log_p_mean_modlag6] in `c'
			replace fs_se_modlag6 = _se[log_p_mean_modlag6] in `c'
			replace fs_t_modlag6 =  _b[log_p_mean_modlag6]/_se[log_p_mean_modlag6] in `c'
		}				
	}
}

// Save output
keep panel-fs_t_modlag6
dropmiss, obs force
dropmiss, force
compress
save "$dirpath_data/results/regs_appx_elec_monthly.dta", replace

}

************************************************
************************************************
