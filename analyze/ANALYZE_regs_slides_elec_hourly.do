clear all
version 13
set more off

**********************************************************************************
** Script to run hourly electricity regressions for slides (maybe for appendix) **
**********************************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

************************************************
************************************************

** 1. Hourly regressions
{ 

// Load monthly panel to grab sample restrictors
use "$dirpath_data/merged/sp_month_elec_panel.dta", clear
keep sp_uuid modate rt_large_ag flag_irregular_bill flag_weird_cust merge_sp_water_panel
drop if modate<ym(2011,1)
tempfile sp_month
save `sp_month'

// Load hourly panel
use "$dirpath_data/merged/sp_hourly_elec_panel_collapsed_20180719.dta", clear
local panel = "hourly"
local pull = "20180719" 

// Drop solar NEM customers
keep if flag_nem==0 

// Drop bad geocodes (i.e. not in PGE service territory, or California)
keep if flag_geocode_badmiss==0 

// Merge in monthly sample restrictors
merge m:1 sp_uuid modate using `sp_month', nogen keep(3)

// Drop irregular bills (first bill, last bill, long bill, short bill, etc.)
keep if flag_irregular_bill==0 

// Drop weird customers (non-ag rates, irrigation districts, etc.)
keep if flag_weird_cust==0 

// Drop SPs that don't merge into APEP dataset (for consistency with water regressiosn)
keep if merge_sp_water_panel==3

// Define sample local to store
local if_sample_base = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3"
	
// Define dependent variable
local DEPVAR = "ihs_kwh"
	
// Define cluster variables
local VCE = "sp_group modate"	
	
// Drop variables I'm not using
drop weekend flag_nem cz_group flag_geocode_badmiss wdist_group county_group basin_group ///
	gw_qtr_bsn_mean1 gw_qtr_bsn_mean2 log_p_kwh_e1_lo log_p_kwh_e1_mi log_p_kwh_e1_hi ///
	log_p_kwh_e20 log_p_lag12 log_p_lag6 flag_irregular_bill flag_weird_cust merge_sp_water_panel	
	
// Create empty variables to populate for storign results
gen panel = ""
gen pull = ""
gen sample_base = "`if_sample_base'"
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
	
	
// Loop over regressions
foreach c in 1 2 3 4 5 6 7 8 {

	if `c'==1 {
		local if_sample = ""
		local FEs = "sp_group#month#hour sp_group#rt_large_ag modate"
		local RHS = "(log_p = log_p_kwh_ag_default)"
	}
	if `c'==2 {
		local if_sample = "if summer==1"
		local FEs = "sp_group#month#hour sp_group#rt_large_ag modate"
		local RHS = "(log_p = log_p_kwh_ag_default)"
	}
	if `c'==3 {
		local if_sample = "if summer==0"
		local FEs = "sp_group#month#hour sp_group#rt_large_ag modate"
		local RHS = "(log_p = log_p_kwh_ag_default)"
	}
	if `c'==4 {
		local if_sample = "if sp_same_rate_in_cat==0"
		local FEs = "sp_group#month#hour sp_group#rt_large_ag modate"
		local RHS = "(log_p = log_p_kwh_ag_default)"
	}
	if `c'==5 {
		local if_sample = "if sp_same_rate_in_cat==1"
		local FEs = "sp_group#month#hour sp_group#rt_large_ag modate"
		local RHS = "(log_p = log_p_kwh_ag_default)"
	}
	if `c'==6 {
		local if_sample = ""
		local FEs = "sp_group#month#hour sp_group#rt_large_ag modate#rt_group"
		local RHS = "(log_p = log_p_kwh_ag_default)"
	}
	if `c'==7 {
		local if_sample = ""
		local FEs = "sp_group#month sp_group#hour sp_group#rt_large_ag modate"
		local RHS = "(log_p = log_p_kwh_ag_default)"
	}
	if `c'==8 {
		local if_sample = ""
		local FEs = "sp_group#month sp_group#hour sp_group#rt_large_ag modate#rt_group"
		local RHS = "(log_p = log_p_kwh_ag_default)"
	}
	
	// Run IV specifications
	{
					
		// Run 2SLS regression
		if "`if_sample'"=="" {
			ivreghdfe `DEPVAR' `RHS' [fw=fwt], absorb(`FEs') cluster(`VCE')
		}
		else {
			preserve
			keep `if_sample'
			ivreghdfe `DEPVAR' `RHS' [fw=fwt], absorb(`FEs') cluster(`VCE')
			restore
		}
		// Store results
		replace panel = "`panel'" in `c'
		replace pull = "`pull'" in `c'
		replace sample = "`if_sample'" in `c'
		replace depvar = "`DEPVAR'" in `c'
		replace fes = "`FEs'" in `c'
		replace rhs = "`RHS'" in `c'
		replace beta_log_p = _b[log_p] in `c'
		replace se_log_p = _se[log_p] in `c'
		replace t_log_p =  _b[log_p]/_se[log_p] in `c'
		replace vce = "cluster `VCE'" in `c'
		replace n_obs = e(N) in `c'
		replace n_SPs = e(N_clust1) in `c'
		replace n_modates = e(N_clust2) in `c'
		replace dof = e(df_r) in `c'
		replace fstat_rk = e(rkf) in `c'
				
		// Run first stage regression
		local RHSfs = subinstr(subinstr(subinstr("`RHS'","(","",.),")","",.),"=","",.)
		if "`if_sample'"=="" {
			reghdfe `RHSfs' [fw=fwt], absorb(`FEs') vce(cluster `VCE')
		}
		else {
			preserve
			keep `if_sample'
			reghdfe `RHSfs' [fw=fwt], absorb(`FEs') vce(cluster `VCE')
			restore
		}		
		// Store results
		replace fs_beta_default = _b[log_p_kwh_ag_default] in `c'
		replace fs_se_default = _se[log_p_kwh_ag_default] in `c'
		replace fs_t_default =  _b[log_p_kwh_ag_default]/_se[log_p_kwh_ag_default] in `c'
	}
}

// Save output
keep panel-fs_t_default
dropmiss, obs force
drop if panel==""
compress
save "$dirpath_data/results/regs_slides_elec_hourly.dta", replace

}

************************************************
************************************************
