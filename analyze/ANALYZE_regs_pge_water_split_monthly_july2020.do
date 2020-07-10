clear all
version 13
set more off

***********************************************************************************************
** Script to run main set of monthly PGE water regressions (split) for July 2020 paper draft **
***********************************************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

************************************************
************************************************

** 1. Monthly regressions with decomposed P^groundwater
{ 

// Load monthly panel
use "$dirpath_data/merged_pge/sp_month_water_panel.dta", clear
local panel = "monthly"

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

// Define baseline sample criteria (common to all regressions)
local ifs_base = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"

// Keep APEP data pull only
cap drop if pull!="20180719" 
cap drop pull
local pull = "PGE 20180719" 

// Define cluster variables
local VCE = "sp_group modate"	

// Population missing groups for group-wise FEs, to avoid dropping them when we don't want to
replace wdist_group = 0 if wdist_group==.
	
// Create empty variables to populate for storign results
gen panel = ""
gen pull = ""
gen ifs_base = ""
gen ifs_sample = ""
gen depvar = ""
gen fes = ""
gen rhs = ""
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
	
local row = 0	
	
// Loop over 8 combinations of outcome+sample
foreach c1 of numlist 1/8 {

	if `c1'==1 {
		local DEPVAR = "ihs_kwh"
		local ifs_sample = ""
	}
	if `c1'==2 {
		local DEPVAR = "ihs_kwh"
		local ifs_sample = " & inlist(basin_group,68,121,122)"
	}
	if `c1'==3 {
		local DEPVAR = "ihs_kwh"
		local ifs_sample = " & log_kwh!=."
	}
	if `c1'==4 {
		local DEPVAR = "log_kwh"
		local ifs_sample = ""
	}
	if `c1'==5 {
		local DEPVAR = "log1_kwh"
		local ifs_sample = ""
	}
	if `c1'==6 {
		local DEPVAR = "log1_100kwh"
		local ifs_sample = ""
	}
	if `c1'==7 {
		local DEPVAR = "elec_binary"
		local ifs_sample = ""
	}
	if `c1'==8 {
		local DEPVAR = "ihs_kwh"
		local ifs_sample = " & elec_binary_frac > 0.9"
	}
	
	
	// Loop over 5 specifications
	foreach c2 of numlist 1/5 {
		
		if `c2'==1 {
			local FEs = "sp_group#month sp_group#rt_large_ag modate"
			local RHS = "(log_p_mean = log_mean_p_kwh_ag_default) ln_kwhaf_rast_dd_mth_2SP"
		}
		if `c2'==2 {
			local FEs = "sp_group#month sp_group#rt_large_ag modate"
			local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)"
		}
		if `c2'==3 {
			local FEs = "sp_group#month sp_group#rt_large_ag modate"
			local RHS = "(log_p_mean ln_kwhaf_rast_dd_qtr_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_qtr_2SP)" 
		}
		if `c2'==4 {
			local FEs = "sp_group#month sp_group#rt_large_ag wdist_group#year basin_group#year modate"
			local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)" 
		}
		if `c2'==5 {
			local FEs = "sp_group#month sp_group#rt_large_ag modate"
			local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default L6_ln_gw_mean_depth_mth_2SP L12_ln_gw_mean_depth_mth_2SP)"
		}
	

		local row = `row' + 1
				
		// Run 2SLS regression
		ivreghdfe `DEPVAR' `RHS' `ifs_base' `ifs_sample', absorb(`FEs') cluster(`VCE')
		
		// Store results
		replace panel = "`panel'" in `row'
		replace pull = "`pull'" in `row'
		replace ifs_base = "`ifs_base'" in `row'
		replace ifs_sample = "`ifs_sample'" in `row'
		replace depvar = "`DEPVAR'" in `row'
		replace fes = "`FEs'" in `row'
		replace rhs = "`RHS'" in `row'
		replace beta_log_p_kwh = _b[log_p_mean] in `row'
		replace se_log_p_kwh = _se[log_p_mean] in `row'
		replace t_log_p_kwh =  _b[log_p_mean]/_se[log_p_mean] in `row'
		local kwhaf_var = word("`RHS'",2)
		if "`kwhaf_var'"=="=" {
			local kwhaf_var = word("`RHS'",wordcount("`RHS'"))
			assert regexm("`kwhaf_var'","kwhaf")
		}
		replace beta_log_kwhaf = _b[`kwhaf_var']-1 in `row'
		replace se_log_kwhaf = _se[`kwhaf_var'] in `row'
		replace t_log_kwhaf = (_b[`kwhaf_var']-1)/_se[`kwhaf_var'] in `row'
		test log_p_mean = `kwhaf_var' - 1
		replace pval_equal = r(p) in `row'
		replace vce = "cluster `VCE'" in `row'
		replace n_obs = e(N) in `row'
		replace n_SPs = e(N_clust1) in `row'
		replace n_modates = e(N_clust2) in `row'
		replace dof = e(df_r) in `row'
		replace fstat_rk = e(rkf) in `row'
				
		// Run first stage regression (electricity price)
		local RHSfs = subinstr(subinstr(subinstr("`RHS'","(","",.),")","",.),"=","",.)
		local RHSfs_elec = subinstr("`RHSfs'","`kwhaf_var'","",.)
		reghdfe `RHSfs_elec' `ifs_base' `ifs_sample', absorb(`FEs') vce(cluster `VCE')
		
		replace fse_beta_default = _b[log_mean_p_kwh_ag_default] in `row'
		replace fse_se_default = _se[log_mean_p_kwh_ag_default] in `row'
		replace fse_t_default =  _b[log_mean_p_kwh_ag_default]/_se[log_mean_p_kwh_ag_default] in `row'
		cap replace fse_beta_depth_mth = _b[ln_gw_mean_depth_mth_2SP] in `row'
		cap replace fse_se_depth_mth = _se[ln_gw_mean_depth_mth_2SP] in `row'
		cap replace fse_t_depth_mth =  _b[ln_gw_mean_depth_mth_2SP]/_se[ln_gw_mean_depth_mth_2SP] in `row'
		cap replace fse_beta_depth_qtr = _b[ln_gw_mean_depth_qtr_2SP] in `row'
		cap replace fse_se_depth_qtr = _se[ln_gw_mean_depth_qtr_2SP] in `row'
		cap replace fse_t_depth_qtr =  _b[ln_gw_mean_depth_qtr_2SP]/_se[ln_gw_mean_depth_qtr_2SP] in `row'
		cap replace fse_beta_depthlag12 = _b[L12_ln_gw_mean_depth_mth_2SP] in `row'
		cap replace fse_se_depthlag12 = _se[L12_ln_gw_mean_depth_mth_2SP] in `row'
		cap replace fse_t_depthlag12 =  _b[L12_ln_gw_mean_depth_mth_2SP]/_se[L12_ln_gw_mean_depth_mth_2SP] in `row'
		cap replace fse_beta_depthlag6 = _b[L6_ln_gw_mean_depth_mth_2SP] in `row'
		cap replace fse_se_depthlag6 = _se[L6_ln_gw_mean_depth_mth_2SP] in `row'
		cap replace fse_t_depthlag6 =  _b[L6_ln_gw_mean_depth_mth_2SP]/_se[L6_ln_gw_mean_depth_mth_2SP] in `row'

		// Run first stage regression (kwhaf)
		if word("`RHS'",2)=="`kwhaf_var'" {

			local RHSfs_water = subinstr("`RHSfs'","log_p_mean","",.)
			reghdfe `RHSfs_water' `ifs_base' `ifs_sample', absorb(`FEs') vce(cluster `VCE')
			
			replace fsw_beta_default = _b[log_mean_p_kwh_ag_default] in `row'
			replace fsw_se_default = _se[log_mean_p_kwh_ag_default] in `row'
			replace fsw_t_default =  _b[log_mean_p_kwh_ag_default]/_se[log_mean_p_kwh_ag_default] in `row'
			cap replace fsw_beta_depth_mth = _b[ln_gw_mean_depth_mth_2SP] in `row'
			cap replace fsw_se_depth_mth = _se[ln_gw_mean_depth_mth_2SP] in `row'
			cap replace fsw_t_depth_mth =  _b[ln_gw_mean_depth_mth_2SP]/_se[ln_gw_mean_depth_mth_2SP] in `row'
			cap replace fsw_beta_depth_qtr = _b[ln_gw_mean_depth_qtr_2SP] in `row'
			cap replace fsw_se_depth_qtr = _se[ln_gw_mean_depth_qtr_2SP] in `row'
			cap replace fsw_t_depth_qtr =  _b[ln_gw_mean_depth_qtr_2SP]/_se[ln_gw_mean_depth_qtr_2SP] in `row'
			cap replace fsw_beta_depthlag12 = _b[L12_ln_gw_mean_depth_mth_2SP] in `row'
			cap replace fsw_se_depthlag12 = _se[L12_ln_gw_mean_depth_mth_2SP] in `row'
			cap replace fsw_t_depthlag12 =  _b[L12_ln_gw_mean_depth_mth_2SP]/_se[L12_ln_gw_mean_depth_mth_2SP] in `row'
			cap replace fsw_beta_depthlag6 = _b[L6_ln_gw_mean_depth_mth_2SP] in `row'
			cap replace fsw_se_depthlag6 = _se[L6_ln_gw_mean_depth_mth_2SP] in `row'
			cap replace fsw_t_depthlag6 =  _b[L6_ln_gw_mean_depth_mth_2SP]/_se[L6_ln_gw_mean_depth_mth_2SP] in `row'
		}
	
		// Save output
		if `c1'==1 & `c2'==1 {
			cap erase "$dirpath_data/results/regs_pge_water_split_monthly_july2020.dta"
		}
		preserve
		keep panel-fsw_t_depthlag6
		dropmiss, obs force
		tempfile reg_out
		save `reg_out'
		clear
		cap use "$dirpath_data/results/regs_pge_water_split_monthly_july2020.dta"
		cap append using `reg_out'
		duplicates drop
		compress
		save "$dirpath_data/results/regs_pge_water_split_monthly_july2020.dta", replace
		restore
		
	}	
}

}

************************************************
************************************************
