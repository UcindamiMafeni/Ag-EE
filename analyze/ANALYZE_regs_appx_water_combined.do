clear all
version 13
set more off

**************************************************************************************
** Script to run monthly water regressions (combined) -- sensitivities to main spec **
**************************************************************************************

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
gen beta_log_p_water = .
gen se_log_p_water = .
gen t_log_p_water = .
gen vce = ""
gen n_obs = .
gen n_SPs = .
gen n_modates = .
gen dof = .
gen fstat_rk = .
forvalues v = 1/4 {
	gen fs_beta_var`v' = .
	gen fs_se_var`v' = .
	gen fs_t_var`v' = .
}

// Define default global: sample
global if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	
// Define default global: dependent variable
global DEPVAR = "ihs_af_rast_dd_mth_2SP"
	
// Define default global: RHS	
global RHS = "(ln_mean_p_af_rast_dd_mth_2SP = log_mean_p_kwh_ag_default)" 

// Define default global: FEs
global FEs = "sp_group#month sp_group#rt_large_ag modate"
	
// Define default global: cluster variables
global VCE = "sp_group modate"	

	
	
// Loop over sensitivities
forvalues c = 1/49 {

	// Reset default locals
	local if_sample = "${if_sample}"
	local DEPVAR = "${DEPVAR}"
	local RHS = "${RHS}"
	local FEs = "${FEs}"
	local VCE = "${VCE}"

	if `c'==1 {
		local sens = "OLS, no unit*capital FEs"
		local RHS = "ln_mean_p_af_rast_dd_mth_2SP"
		local FEs = "sp_group#month modate"
	}
	if `c'==2 {
		local sens = "IV, no unit*capital FEs"
		local FEs = "sp_group#month modate"
	}
	if `c'==3 {
		local sens = "IV with modal tariff, not default tariff"
		local RHS = "(ln_mean_p_af_rast_dd_mth_2SP = log_mean_p_kwh_ag_modal)"
	}
	if `c'==4 {
		local sens = "IV with lagged modal tariff, not default tariff"
		local RHS = "(ln_mean_p_af_rast_dd_mth_2SP = log_p_mean_modlag*)"
	}
	if `c'==5 {
		local sens = "IV with average depth, not default tariff"
		local RHS = "(ln_mean_p_af_rast_dd_mth_2SP = ln_gw_mean_depth_mth_2SP)"
	}
	if `c'==6 {
		local sens = "IV with lagged average depth, not default tariff"
		local RHS = "(ln_mean_p_af_rast_dd_mth_2SP = L6_ln_gw_mean_depth_mth_2SP L12_ln_gw_mean_depth_mth_2SP)"
	}
	if `c'==7 {
		local sens = "Depvar: log(Q)"
		local DEPVAR = "log_af_rast_dd_mth_2SP"
	}
	if `c'==8 {
		local sens = "Depvar: log(1+Q)"
		local DEPVAR = "log1_af_rast_dd_mth_2SP"
	}
	if `c'==9 {
		local sens = "Depvar: log(1+100*Q)"
		local DEPVAR = "log1_100af_rast_dd_mth_2SP"
	}
	if `c'==10 {
		local sens = "Depvar: log(1+10000*Q)"
		local DEPVAR = "log1_10000af_rast_dd_mth_2SP"
	}
	if `c'==11 {
		local sens = "Depvar: log(Q); IV with average depth"
		local DEPVAR = "log_af_rast_dd_mth_2SP"
		local RHS = "(ln_mean_p_af_rast_dd_mth_2SP = ln_gw_mean_depth_mth_2SP)"
	}
	if `c'==12 {
		local sens = "Depvar: log(1+Q); IV with average depth"
		local DEPVAR = "log1_af_rast_dd_mth_2SP"
		local RHS = "(ln_mean_p_af_rast_dd_mth_2SP = ln_gw_mean_depth_mth_2SP)"
	}
	if `c'==13 {
		local sens = "Depvar: log(1+100*Q); IV with average depth"
		local DEPVAR = "log1_100af_rast_dd_mth_2SP"
		local RHS = "(ln_mean_p_af_rast_dd_mth_2SP = ln_gw_mean_depth_mth_2SP)"
	}	
	if `c'==14 {
		local sens = "Depvar: log(1+10000*Q); IV with average depth"
		local DEPVAR = "log1_10000af_rast_dd_mth_2SP"
		local RHS = "(ln_mean_p_af_rast_dd_mth_2SP = ln_gw_mean_depth_mth_2SP)"
	}	
	if `c'==15 {
		local sens = "Within-category switchers only"
		local if_sample = "${if_sample} & sp_same_rate_in_cat==0"
	}
	if `c'==16 {
		local sens = "Within-category non-switchers only"
		local if_sample = "${if_sample} & sp_same_rate_in_cat==1"
	}
	if `c'==17 {
		local sens = "Within 60 months of pump test"
		local if_sample = "${if_sample} & months_to_nearest_test<=60"
	}
	if `c'==18 {
		local sens = "Within 48 months of pump test"
		local if_sample = "${if_sample} & months_to_nearest_test<=48"
	}
	if `c'==19 {
		local sens = "Within 36 months of pump test"
		local if_sample = "${if_sample} & months_to_nearest_test<=36"
	}
	if `c'==20 {
		local sens = "Within 24 months of pump test"
		local if_sample = "${if_sample} & months_to_nearest_test<=24"
	}
	if `c'==21 {
		local sens = "Within 12 months of pump test"
		local if_sample = "${if_sample} & months_to_nearest_test<=12"
	}
	if `c'==22 {
		local sens = "Dropping APEP-subsidized projects"
		local if_sample = "${if_sample} & apep_proj_count==0"
	}
	if `c'==23 {
		local sens = "Latlons within 100 miles"
		local if_sample = "${if_sample} & latlon_miles_apart<=100"
	}
	if `c'==24 {
		local sens = "Latlons within 50 miles"
		local if_sample = "${if_sample} & latlon_miles_apart<=50"
	}
	if `c'==25 {
		local sens = "Latlons within 25 miles"
		local if_sample = "${if_sample} & latlon_miles_apart<=25"
	}
	if `c'==26 {
		local sens = "Latlons within 10 miles"
		local if_sample = "${if_sample} & latlon_miles_apart<=10"
	}
	if `c'==27 {
		local sens = "Latlons within 5 miles"
		local if_sample = "${if_sample} & latlon_miles_apart<=5"
	}
	if `c'==28 {
		local sens = "Latlons within 1 miles"
		local if_sample = "${if_sample} & latlon_miles_apart<=1"
	}
	if `c'==29 {
		local sens = "Summer months only"
		local if_sample = "${if_sample} & summer==1"
	}
	if `c'==30 {
		local sens = "Winter months only"
		local if_sample = "${if_sample} & summer==0"
	}
	if `c'==31 {
		local sens = "Removing SPs with exactly 1 pump test"
		local if_sample = "${if_sample} & apep_interp_case!=1"
	}
	if `c'==32 {
		local sens = "Removing SPs with >1 pump tests"
		local if_sample = "${if_sample} & apep_interp_case==1"
	}
	if `c'==33 {
		local sens = "Removing SPs with multiple pumps"
		local if_sample = "${if_sample} & inlist(apep_interp_case,1,2) "
	}
	if `c'==34 {
		local sens = "Removing SPs with a bad drawdown flag"
		local if_sample = "${if_sample} & flag_bad_drwdwn==1 "
	}
	if `c'==35 {
		local sens = "Removing SPs without high-confidence drawdown predictions"
		local if_sample = "${if_sample} & drwdwn_predict_step<=2 "
	}
	if `c'==36 {
		local sens = "Removing SPs without medium-to-high-confidence drawdown predictions"
		local if_sample = "${if_sample} & drwdwn_predict_step<=4 "
	}
	if `c'==37 {
		local sens = "APEP-measured KWHAF (independent of depth), with avg depth IV"
		local DEPVAR = "ihs_af_apep_measured"
		local RHS = "(ln_mean_p_af_apep_measured = log_mean_p_kwh_ag_default)" 
	}
	if `c'==38 {
		local sens = "Using pump latlon instead of SP latlon"
		local DEPVAR = "ihs_af_rast_dd_mth_2"
		local RHS = "(ln_mean_p_af_rast_dd_mth_2 = log_mean_p_kwh_ag_default)" 
	}
	if `c'==39 {
		local sens = "Calculating KWHAF using predicted drawdown instead of fixed drawdown"
		local DEPVAR = "ihs_af_rast_ddhat_mth_2SP"
		local RHS = "(ln_mean_p_af_rast_ddhat_mth_2SP = log_mean_p_kwh_ag_default)" 
	}
	if `c'==40 {
		local sens = "Calculating KWHAF using mean depth instead of rasterized depth"
		local DEPVAR = "ihs_af_mean_dd_mth_2SP"
		local RHS = "(ln_mean_p_af_mean_dd_mth_2SP = log_mean_p_kwh_ag_default)" 
	}
	if `c'==41 {
		local sens = "Calculating KWHAF using mean depth AND predicted drawdown"
		local DEPVAR = "ihs_af_mean_ddhat_mth_2SP"
		local RHS = "(ln_mean_p_af_mean_ddhat_mth_2SP = log_mean_p_kwh_ag_default)" 
	}
	if `c'==42 {
		local sens = "San Joaquin basin only"
		local if_sample = "${if_sample} & basin_group==122"
	}
	if `c'==43 {
		local sens = "Sacramento basin only"
		local if_sample = "${if_sample} & basin_group==121"
	}
	if `c'==44 {
		local sens = "Salinas basin only"
		local if_sample = "${if_sample} & basin_group==68"
	}
	if `c'==45 {
		local sens = "Salinas, Sacramento, and San Joaquin basins"
		local if_sample = "${if_sample} & inlist(basin_group,68,121,122)"
	}
	if `c'==46 {
		local sens = "Unit-specific linear time trends"
		local FEs = "sp_group#month sp_group#rt_large_ag modate sp_group#c.modate"
	}
	if `c'==47 {
		local sens = "Basin by month-of-sample FEs"
		local FEs = "sp_group#month sp_group#rt_large_ag modate#basin_group" 
	}
	if `c'==48 {
		local sens = "Water district by month-of-sample FEs"
		local FEs = "sp_group#month sp_group#rt_large_ag modate#wdist_group" 
	}
	
	
	// Run non-IV specification	
	if substr("`RHS'",1,1)!="(" {
					
		// Run OLS regression
		reghdfe `DEPVAR' `RHS' `if_sample', absorb(`FEs') vce(cluster `VCE')
		local p_water_var = subinstr(word("`RHS'",1),"(","",1)
		
		// Store results
		replace panel = "`panel'" in `c'
		replace pull = "`pull'" in `c'
		replace sens = "`sens'" in `c'
		replace sample = "`if_sample'" in `c'
		replace depvar = "`DEPVAR'" in `c'
		replace fes = "`FEs'" in `c'
		replace rhs = "`RHS'" in `c'
		replace if_sample = "`if_sample'" in `c'
		replace beta_log_p_water = _b[`p_water_var'] in `c'
		replace se_log_p_water = _se[`p_water_var'] in `c'
		replace t_log_p_water =  _b[`p_water_var']/_se[`p_water_var'] in `c'
		replace vce = "cluster `VCE'" in `c'
		replace n_obs = e(N) in `c'
		replace n_SPs = e(N_clust1) in `c'
		replace n_modates = e(N_clust2) in `c'
		replace dof = e(df_r) in `c'
				
	}

	else {
		
		// Run 2SLS regression
		ivreghdfe `DEPVAR' `RHS' `if_sample', absorb(`FEs') cluster(`VCE')
		local p_water_var = subinstr(word("`RHS'",1),"(","",1)
		
		// Store results
		replace panel = "`panel'" in `c'
		replace pull = "`pull'" in `c'
		replace sens = "`sens'" in `c'
		replace sample = "`if_sample'" in `c'
		replace depvar = "`DEPVAR'" in `c'
		replace fes = "`FEs'" in `c'
		replace rhs = "`RHS'" in `c'
		replace beta_log_p_water = _b[`p_water_var'] in `c'
		replace se_log_p_water = _se[`p_water_var'] in `c'
		replace t_log_p_water =  _b[`p_water_var']/_se[`p_water_var'] in `c'
		replace vce = "cluster `VCE'" in `c'
		replace n_obs = e(N) in `c'
		replace n_SPs = e(N_clust1) in `c'
		replace n_modates = e(N_clust2) in `c'
		replace dof = e(df_r) in `c'
		replace fstat_rk = e(rkf) in `c'
				
		// Run first stage regression (electricity price)
		local RHSfs = subinstr(subinstr(subinstr("`RHS'","(","",.),")","",.),"=","",.)
		reghdfe `RHSfs' `if_sample', absorb(`FEs') vce(cluster `VCE')
		
		forvalues v = 1/4 {
			local var = word(e(indepvars),`v')
			if "`var'"!="_cons" {
				cap replace fs_beta_var`v' = _b[`var'] in `c'
				cap replace fs_se_var`v' = _se[`var'] in `c'
				cap replace fs_t_var`v' =  _b[`var']/_se[`var'] in `c'
			}	
		}
	}


}

// Save output
keep panel-fs_t_var4
dropmiss, obs force
dropmiss, force
compress
save "$dirpath_data/results/regs_appx_water_combined.dta", replace

}

************************************************
************************************************
