clear all
version 13
set more off

*******************************************************************************
** Script to run monthly water regressions (combined) for main text of paper **
*******************************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

************************************************
************************************************

** 1. Monthly regressions with combined P^groundwater
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
gen beta_log_p_water = .
gen se_log_p_water = .
gen t_log_p_water = .
gen vce = ""
gen n_obs = .
gen n_SPs = .
gen n_modates = .
gen dof = .
gen fstat_rk = .
gen fs_beta_default = .
gen fs_se_default = .
gen fs_t_default = .
gen fs_beta_deflag12 = .
gen fs_se_deflag12 = .
gen fs_t_deflag12 = .
gen fs_beta_deflag6 = .
gen fs_se_deflag6 = .
gen fs_t_deflag6 = .
	
		
// Loop over 6 regressions
foreach c in 1 2 3 4 5 6 {

	if `c'==1 {
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local RHS = "ln_mean_p_af_rast_dd_mth_2SP"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==2 {
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local RHS = "(ln_mean_p_af_rast_dd_mth_2SP = log_mean_p_kwh_ag_default)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==3 {
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local RHS = "(ln_mean_p_af_rast_dd_mth_2SP = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & inlist(basin_group,68,121,122)"
	}
	if `c'==4 {
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local DEPVAR = "ihs_af_rast_dd_qtr_2SP"
		local RHS = "(ln_mean_p_af_rast_dd_qtr_2SP = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==5 {
		local FEs = "sp_group#month sp_group#rt_large_ag wdist_group#year basin_group#year modate"
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local RHS = "(ln_mean_p_af_rast_dd_mth_2SP = log_mean_p_kwh_ag_default)"  
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==6 {
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local RHS = "(ln_mean_p_af_rast_dd_mth_2SP = log_p_mean_deflag*)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}

	// Run non-IV specification	
	if inlist(`c',1) {
					
		// Run OLS regression
		reghdfe `DEPVAR' `RHS' `if_sample', absorb(`FEs') vce(cluster `VCE')
		
		// Store results
		replace panel = "`panel'" in `c'
		replace pull = "`pull'" in `c'
		replace sample = "`if_sample'" in `c'
		replace depvar = "`DEPVAR'" in `c'
		replace fes = "`FEs'" in `c'
		replace rhs = "`RHS'" in `c'
		replace if_sample = "`if_sample'" in `c'
		cap replace beta_log_p_water = _b[ln_mean_p_af_rast_dd_mth_2SP] in `c'
		cap replace se_log_p_water = _se[ln_mean_p_af_rast_dd_mth_2SP] in `c'
		cap replace t_log_p_water =  _b[ln_mean_p_af_rast_dd_mth_2SP]/_se[ln_mean_p_af_rast_dd_mth_2SP] in `c'
		cap replace beta_log_p_water = _b[ln_mean_p_af_rast_dd_qr_2SP] in `c'
		cap replace se_log_p_water = _se[ln_mean_p_af_rast_dd_qtr_2SP] in `c'
		cap replace t_log_p_water =  _b[ln_mean_p_af_rast_dd_qtr_2SP]/_se[ln_mean_p_af_rast_dd_qtr_2SP] in `c'
		replace vce = "cluster `VCE'" in `c'
		replace n_obs = e(N) in `c'
		replace n_SPs = e(N_clust1) in `c'
		replace n_modates = e(N_clust2) in `c'
		replace dof = e(df_r) in `c'
				
	}
	
	else {
	
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
		cap replace beta_log_p_water = _b[ln_mean_p_af_rast_dd_mth_2SP] in `c'
		cap replace se_log_p_water = _se[ln_mean_p_af_rast_dd_mth_2SP] in `c'
		cap replace t_log_p_water =  _b[ln_mean_p_af_rast_dd_mth_2SP]/_se[ln_mean_p_af_rast_dd_mth_2SP] in `c'
		cap replace beta_log_p_water = _b[ln_mean_p_af_rast_dd_qtr_2SP] in `c'
		cap replace se_log_p_water = _se[ln_mean_p_af_rast_dd_qtr_2SP] in `c'
		cap replace t_log_p_water =  _b[ln_mean_p_af_rast_dd_qtr_2SP]/_se[ln_mean_p_af_rast_dd_qtr_2SP] in `c'
		replace vce = "cluster `VCE'" in `c'
		replace n_obs = e(N) in `c'
		replace n_SPs = e(N_clust1) in `c'
		replace n_modates = e(N_clust2) in `c'
		replace dof = e(df_r) in `c'
		replace fstat_rk = e(rkf) in `c'
				
		// Run first stage regression 
		local RHSfs = subinstr(subinstr(subinstr("`RHS'","(","",.),")","",.),"=","",.)
		reghdfe `RHSfs' `if_sample', absorb(`FEs') vce(cluster `VCE')
	
		if `c'!=6 {
			replace fs_beta_default = _b[log_mean_p_kwh_ag_default] in `c'
			replace fs_se_default = _se[log_mean_p_kwh_ag_default] in `c'
			replace fs_t_default =  _b[log_mean_p_kwh_ag_default]/_se[log_mean_p_kwh_ag_default] in `c'
		}
		else {
			replace fs_beta_deflag12 = _b[log_p_mean_deflag12] in `c'
			replace fs_se_deflag12 = _se[log_p_mean_deflag12] in `c'
			replace fs_t_deflag12 =  _b[log_p_mean_deflag12]/_se[log_p_mean_deflag12] in `c'
			replace fs_beta_deflag6 = _b[log_p_mean_deflag6] in `c'
			replace fs_se_deflag6 = _se[log_p_mean_deflag6] in `c'
			replace fs_t_deflag6 =  _b[log_p_mean_deflag6]/_se[log_p_mean_deflag6] in `c'
		}				
	}
}

// Save output
keep panel-fs_t_deflag6
dropmiss, obs force
compress
save "$dirpath_data/results/regs_paper_water_combined.dta", replace

}

************************************************
************************************************
