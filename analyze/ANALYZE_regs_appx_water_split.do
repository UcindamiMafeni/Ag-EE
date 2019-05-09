clear all
version 13
set more off

***********************************************************************************
** Script to run monthly water regressions (split) -- sensitivities to main spec **
***********************************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

************************************************
************************************************

** 1. Monthly regressions with decomposed P^groundwater
{ 

// Load monthly panel
use "$dirpath_data/merged/sp_month_water_panel.dta", clear
local panel = "monthly"

// Keep APEP data pull only
cap drop if pull!="20180719" 
local pull = "20180719" 

// Drop solar NEM customers
keep if flag_nem==0 

// Drop bad geocodes (i.e. not in PGE service territory, or California)
keep if flag_geocode_badmiss==0 

// Drop irregular bills (first bill, last bill, long bill, short bill, etc.)
keep if flag_irregular_bill==0 

// Drop weird pumps (implausible technical specs)
keep if flag_weird_pump==0 

// Drop weird customers (non-ag rates, irrigation districts, etc.)
keep if flag_weird_cust==0 
	
// Create empty variables to populate for storign results
gen panel = ""
cap drop pull
gen pull = ""
gen sens = ""
gen sample = ""
gen depvar = ""
gen fes = ""
gen rhs = ""
gen if_sample = ""
gen beta_log_p_kwh = .
gen se_log_p_kwh = .
gen t_log_p_kwh = .
gen beta_log_kwhaf = .
gen se_log_kwhaf = .
gen t_log_kwhaf = .
gen pval_equal = .
gen vce = ""
gen n_obs = .
gen n_SPs = .
gen n_modates = .
gen dof = .
gen fstat_rk = .
forvalues v = 1/4 {
	gen fse_beta_var`v' = .
	gen fse_se_var`v' = .
	gen fse_t_var`v' = .
}
forvalues v = 1/4 {
	gen fsw_beta_var`v' = .
	gen fsw_se_var`v' = .
	gen fsw_t_var`v' = .
}

// Define default global: sample
global if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	
// Define default global: dependent variable
global DEPVAR = "ihs_kwh"
	
// Define default global: RHS	
global RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)"
	
// Define default global: FEs
global FEs = "sp_group#month sp_group#rt_large_ag modate"
	
// Define default global: cluster variables
global VCE = "sp_group modate"	

	
	
// Loop over sensitivities
forvalues c = 1/34 {

	// Reset default locals
	local if_sample = "${if_sample}"
	local DEPVAR = "${DEPVAR}"
	local RHS = "${RHS}"
	local FEs = "${FEs}"
	local VCE = "${VCE}"

	if `c'==1 {
		local sens = "IV with modal tariff, not default tariff"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_modal ln_gw_mean_depth_mth_2SP)" 
	}
	if `c'==2 {
		local sens = "IV with lagged modal tariff, not default tariff"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_p_mean_modlag* ln_gw_mean_depth_mth_2SP)" 
	}
	if `c'==3 {
		local sens = "IV with lagged default tariff AND lagged average depth"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_p_mean_deflag* L6_ln_gw_mean_depth_mth_2SP L12_ln_gw_mean_depth_mth_2SP)" 
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
		local sens = "Within 12 months of pump test"
		local if_sample = "${if_sample} & months_to_nearest_test<=12"
	}
	if `c'==15 {
		local sens = "Dropping APEP-subsidized projects"
		local if_sample = "${if_sample} & apep_proj_count==0"
	}
	if `c'==16 {
		local sens = "Latlons within 100 miles"
		local if_sample = "${if_sample} & latlon_miles_apart<=100"
	}
	if `c'==17 {
		local sens = "Latlons within 50 miles"
		local if_sample = "${if_sample} & latlon_miles_apart<=50"
	}
	if `c'==18 {
		local sens = "Latlons within 25 miles"
		local if_sample = "${if_sample} & latlon_miles_apart<=25"
	}
	if `c'==19 {
		local sens = "Latlons within 10 miles"
		local if_sample = "${if_sample} & latlon_miles_apart<=10"
	}
	if `c'==20 {
		local sens = "Latlons within 5 miles"
		local if_sample = "${if_sample} & latlon_miles_apart<=5"
	}
	if `c'==21 {
		local sens = "Latlons within 1 miles"
		local if_sample = "${if_sample} & latlon_miles_apart<=1"
	}
	if `c'==22 {
		local sens = "Summer months only"
		local if_sample = "${if_sample} & summer==1"
	}
	if `c'==23 {
		local sens = "Winter months only"
		local if_sample = "${if_sample} & summer==0"
	}
	if `c'==24 {
		local sens = "Removing SPs with exactly 1 pump test"
		local if_sample = "${if_sample} & apep_interp_case!=1"
	}
	if `c'==25 {
		local sens = "Removing SPs with >1 pump tests"
		local if_sample = "${if_sample} & apep_interp_case==1"
	}
	if `c'==26 {
		local sens = "Removing SPs with multiple pumps"
		local if_sample = "${if_sample} & inlist(apep_interp_case,1,2) "
	}
	if `c'==27 {
		local sens = "Removing SPs with a bad drawdown flag"
		local if_sample = "${if_sample} & flag_bad_drwdwn==1 "
	}
	if `c'==28 {
		local sens = "Removing SPs without high-confidence drawdown predictions"
		local if_sample = "${if_sample} & drwdwn_predict_step<=2 "
	}
	if `c'==29 {
		local sens = "Removing SPs without medium-to-high-confidence drawdown predictions"
		local if_sample = "${if_sample} & drwdwn_predict_step<=4 "
	}
	if `c'==30 {
		local sens = "APEP-measured KWHAF (independent of depth), with avg depth IV"
		local RHS = "(log_p_mean ln_kwhaf_apep_measured = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)"
	}
	if `c'==31 {
		local sens = "Using pump latlon instead of SP latlon"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2 = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2)"
	}
	if `c'==32 {
		local sens = "Calculating KWHAF using predicted drawdown instead of fixed drawdown"
		local RHS = "(log_p_mean ln_kwhaf_rast_ddhat_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)"
	}
	if `c'==33 {
		local sens = "Calculating KWHAF using mean depth instead of rasterized depth"
		local RHS = "(log_p_mean ln_kwhaf_mean_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)"
	}
	if `c'==34 {
		local sens = "Calculating KWHAF using mean depth AND predicted drawdown"
		local RHS = "(log_p_mean ln_kwhaf_mean_ddhat_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)"
	}
	
	
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
	replace beta_log_p_kwh = _b[log_p_mean] in `c'
	replace se_log_p_kwh = _se[log_p_mean] in `c'
	replace t_log_p_kwh =  _b[log_p_mean]/_se[log_p_mean] in `c'
	local kwhaf_var = word("`RHS'",2)
	replace beta_log_kwhaf = _b[`kwhaf_var']-1 in `c'
	replace se_log_kwhaf = _se[`kwhaf_var'] in `c'
	replace t_log_kwhaf = (_b[`kwhaf_var']-1)/_se[`kwhaf_var'] in `c'
	test log_p_mean = `kwhaf_var' - 1
	replace pval_equal = r(p) in `c'
	replace vce = "cluster `VCE'" in `c'
	replace n_obs = e(N) in `c'
	replace n_SPs = e(N_clust1) in `c'
	replace n_modates = e(N_clust2) in `c'
	replace dof = e(df_r) in `c'
	replace fstat_rk = e(rkf) in `c'
			
	// Run first stage regression (electricity price)
	local RHSfs = subinstr(subinstr(subinstr("`RHS'","(","",.),")","",.),"=","",.)
	local RHSfs_elec = subinstr("`RHSfs'","`kwhaf_var'","",1)
	reghdfe `RHSfs_elec' `if_sample', absorb(`FEs') vce(cluster `VCE')
	
	forvalues v = 1/4 {
		local var = word(e(indepvars),`v')
		if "`var'"!="_cons" {
			cap replace fse_beta_var`v' = _b[`var'] in `c'
			cap replace fse_se_var`v' = _se[`var'] in `c'
			cap replace fse_t_var`v' =  _b[`var']/_se[`var'] in `c'
		}	
	}

	// Run first stage regression (kwhaf)
	local RHSfs_water = subinstr("`RHSfs'","log_p_mean","",1)
	reghdfe `RHSfs_water' `if_sample', absorb(`FEs') vce(cluster `VCE')
	
	forvalues v = 1/4 {
		local var = word(e(indepvars),`v')
		if "`var'"!="_cons" {
			cap replace fsw_beta_var`v' = _b[`var'] in `c'
			cap replace fsw_se_var`v' = _se[`var'] in `c'
			cap replace fsw_t_var`v' =  _b[`var']/_se[`var'] in `c'
		}
	}

}

// Save output
keep panel-fsw_t_var4
dropmiss, obs force
dropmiss, force
compress
save "$dirpath_data/results/regs_appx_water_split.dta", replace

}

************************************************
************************************************
