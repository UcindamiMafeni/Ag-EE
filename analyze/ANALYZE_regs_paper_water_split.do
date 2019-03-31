clear all
version 13
set more off

****************************************************************************
** Script to run monthly water regressions (split) for main text of paper **
****************************************************************************

global dirpath "S:/Matt/ag_pump"
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

// Drop SPs that don't merge into APEP dataset (for consistency with water regressiosn)
keep if merge_sp_water_panel==3
	
// Define dependent variable
local DEPVAR = "ihs_kwh"
	
// Define cluster variables
local VCE = "sp_group modate"	
	
// Create empty variables to populate for storign results
gen panel = ""
cap drop pull
gen pull = ""
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
gen vce = ""
gen n_obs = .
gen n_SPs = .
gen n_modates = .
gen dof = .
gen fstat_rk = .
gen fse_beta_default = .
gen fse_se_default = .
gen fse_t_default = .
gen fse_beta_depth_mth = .
gen fse_se_depth_mth = .
gen fse_t_depth_mth = .
gen fse_beta_depth_qtr = .
gen fse_se_depth_qtr = .
gen fse_t_depth_qtr = .
gen fse_beta_depthlag12 = .
gen fse_se_depthlag12 = .
gen fse_t_depthlag12 = .
gen fse_beta_depthlag6 = .
gen fse_se_depthlag6 = .
gen fse_t_depthlag6 = .
gen fsw_beta_default = .
gen fsw_se_default = .
gen fsw_t_default = .
gen fsw_beta_depth_mth = .
gen fsw_se_depth_mth = .
gen fsw_t_depth_mth = .
gen fsw_beta_depth_qtr = .
gen fsw_se_depth_qtr = .
gen fsw_t_depth_qtr = .
gen fsw_beta_depthlag12 = .
gen fsw_se_depthlag12 = .
gen fsw_t_depthlag12 = .
gen fsw_beta_depthlag6 = .
gen fsw_se_depthlag6 = .
gen fsw_t_depthlag6 = .
	
		
// Loop over 6 regressions
foreach c in 1 2 3 4 5 6 {

	if `c'==1 {
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default) ln_kwhaf_rast_dd_mth_2SP"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==2 {
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==3 {
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & inlist(basin_group,68,121,122)"
	}
	if `c'==4 {
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_qtr_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_qtr_2SP)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==5 {
		local FEs = "sp_group#month sp_group#rt_large_ag wdist_group#year basin_group#year modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==6 {
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default L6_ln_gw_mean_depth_mth_2SP L12_ln_gw_mean_depth_mth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}

			
	// Run 2SLS regression
	ivreghdfe `DEPVAR' `RHS' `if_sample', absorb(`FEs') cluster(`VCE')
	
	// Store results
	replace panel = "`panel'" in `c'
	replace pull = "`pull'" in `c'
	replace sample = "`if_sample'" in `c'
	replace depvar = "`DEPVAR'" in `c'
	replace fes = "`FEs'" in `c'
	replace rhs = "`RHS'" in `c'
	replace if_sample = "`if_sample'" in `c'
	replace beta_log_p_kwh = _b[log_p_mean] in `c'
	replace se_log_p_kwh = _se[log_p_mean] in `c'
	replace t_log_p_kwh =  _b[log_p_mean]/_se[log_p_mean] in `c'
	cap replace beta_log_kwhaf = _b[ln_kwhaf_rast_dd_mth_2SP]-1 in `c'
	cap replace se_log_kwhaf = _se[ln_kwhaf_rast_dd_mth_2SP] in `c'
	cap replace t_log_kwhaf = (_b[ln_kwhaf_rast_dd_mth_2SP]-1)/_se[ln_kwhaf_rast_dd_mth_2SP] in `c'
	cap replace beta_log_kwhaf = _b[ln_kwhaf_rast_dd_qtr_2SP]-1 in `c'
	cap replace se_log_kwhaf = _se[ln_kwhaf_rast_dd_qtr_2SP] in `c'
	cap replace t_log_kwhaf = (_b[ln_kwhaf_rast_dd_qtr_2SP]-1)/_se[ln_kwhaf_rast_dd_qtr_2SP] in `c'
	replace vce = "cluster `VCE'" in `c'
	replace n_obs = e(N) in `c'
	replace n_SPs = e(N_clust1) in `c'
	replace n_modates = e(N_clust2) in `c'
	replace dof = e(df_r) in `c'
	replace fstat_rk = e(rkf) in `c'
			
	// Run first stage regression (electricity price)
	local RHSfs = subinstr(subinstr(subinstr("`RHS'","(","",.),")","",.),"=","",.)
	local RHSfs_elec = subinstr(subinstr("`RHSfs'","ln_kwhaf_rast_dd_mth_2SP","",.),"ln_kwhaf_rast_dd_qtr_2SP","",.)
	reghdfe `RHSfs_elec' `if_sample', absorb(`FEs') vce(cluster `VCE')
	
	replace fse_beta_default = _b[log_mean_p_kwh_ag_default] in `c'
	replace fse_se_default = _se[log_mean_p_kwh_ag_default] in `c'
	replace fse_t_default =  _b[log_mean_p_kwh_ag_default]/_se[log_mean_p_kwh_ag_default] in `c'
	cap replace fse_beta_depth_mth = _b[ln_gw_mean_depth_mth_2SP] in `c'
	cap replace fse_se_depth_mth = _se[ln_gw_mean_depth_mth_2SP] in `c'
	cap replace fse_t_depth_mth =  _b[ln_gw_mean_depth_mth_2SP]/_se[ln_gw_mean_depth_mth_2SP] in `c'
	cap replace fse_beta_depth_qtr = _b[ln_gw_mean_depth_qtr_2SP] in `c'
	cap replace fse_se_depth_qtr = _se[ln_gw_mean_depth_qtr_2SP] in `c'
	cap replace fse_t_depth_qtr =  _b[ln_gw_mean_depth_qtr_2SP]/_se[ln_gw_mean_depth_qtr_2SP] in `c'
	cap replace fse_beta_depthlag12 = _b[L12_ln_gw_mean_depth_mth_2SP] in `c'
	cap replace fse_se_depthlag12 = _se[L12_ln_gw_mean_depth_mth_2SP] in `c'
	cap replace fse_t_depthlag12 =  _b[L12_ln_gw_mean_depth_mth_2SP]/_se[L12_ln_gw_mean_depth_mth_2SP] in `c'
	cap replace fse_beta_depthlag6 = _b[L6_ln_gw_mean_depth_mth_2SP] in `c'
	cap replace fse_se_depthlag6 = _se[L6_ln_gw_mean_depth_mth_2SP] in `c'
	cap replace fse_t_depthlag6 =  _b[L6_ln_gw_mean_depth_mth_2SP]/_se[L6_ln_gw_mean_depth_mth_2SP] in `c'

	
	// Run first stage regression (kwhaf)
	if inlist(`c',1)==0 {

		local RHSfs_water = subinstr("`RHSfs'","log_p_mean","",.)
		reghdfe `RHSfs_water' `if_sample', absorb(`FEs') vce(cluster `VCE')
		
		replace fsw_beta_default = _b[log_mean_p_kwh_ag_default] in `c'
		replace fsw_se_default = _se[log_mean_p_kwh_ag_default] in `c'
		replace fsw_t_default =  _b[log_mean_p_kwh_ag_default]/_se[log_mean_p_kwh_ag_default] in `c'
		cap replace fsw_beta_depth_mth = _b[ln_gw_mean_depth_mth_2SP] in `c'
		cap replace fsw_se_depth_mth = _se[ln_gw_mean_depth_mth_2SP] in `c'
		cap replace fsw_t_depth_mth =  _b[ln_gw_mean_depth_mth_2SP]/_se[ln_gw_mean_depth_mth_2SP] in `c'
		cap replace fsw_beta_depth_qtr = _b[ln_gw_mean_depth_qtr_2SP] in `c'
		cap replace fsw_se_depth_qtr = _se[ln_gw_mean_depth_qtr_2SP] in `c'
		cap replace fsw_t_depth_qtr =  _b[ln_gw_mean_depth_qtr_2SP]/_se[ln_gw_mean_depth_qtr_2SP] in `c'
		cap replace fsw_beta_depthlag12 = _b[L12_ln_gw_mean_depth_mth_2SP] in `c'
		cap replace fsw_se_depthlag12 = _se[L12_ln_gw_mean_depth_mth_2SP] in `c'
		cap replace fsw_t_depthlag12 =  _b[L12_ln_gw_mean_depth_mth_2SP]/_se[L12_ln_gw_mean_depth_mth_2SP] in `c'
		cap replace fsw_beta_depthlag6 = _b[L6_ln_gw_mean_depth_mth_2SP] in `c'
		cap replace fsw_se_depthlag6 = _se[L6_ln_gw_mean_depth_mth_2SP] in `c'
		cap replace fsw_t_depthlag6 =  _b[L6_ln_gw_mean_depth_mth_2SP]/_se[L6_ln_gw_mean_depth_mth_2SP] in `c'
	}
	
	
}

// Save output
keep panel-fsw_t_depthlag6
dropmiss, obs force
compress
save "$dirpath_data/results/regs_paper_water_split.dta", replace

}

************************************************
************************************************
