clear all
version 13
set more off

************************************************************************************************
** Script to run crop splits of monthly PGE electricity regressions for July 2020 paper draft **
************************************************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

************************************************
************************************************

** 1. Monthly regressions (main set of 6 regressions, for 6 outcome+sample combinations)
{ 

// Load monthly panel
use "$dirpath_data/merged_pge/sp_month_elec_panel.dta", clear
local panel = "monthly"

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
	
// Drop partial-year SPs
keep if flag_partial_year==0

// Define baseline sample criteria (common to all regressions)
local ifs_base = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	
// Confirm that by forcing a match to the water panel, we've forced out the two non-APEP-specific PGE data pullspulls
assert pull=="20180719" 
local pull = "PGE 20180719" 
drop pull

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
gen fs_beta_deflag12 = .
gen fs_se_deflag12 = .
gen fs_t_deflag12 = .
gen fs_beta_deflag6 = .
gen fs_se_deflag6 = .
gen fs_t_deflag6 = .

local row = 0	
	
// Loop over 6 combinations of outcome+sample
foreach c1 of numlist 1/6 {

	if `c1'==1 {
		local DEPVAR = "no_crop"
		local ifs_sample = ""
	}
	if `c1'==2 {
		local DEPVAR = "annual"
		local ifs_sample = ""
	}
	if `c1'==3 {
		local DEPVAR = "perennial"
		local ifs_sample = ""
	}
	if `c1'==4 {
		local DEPVAR = "ihs_kwh"
		local ifs_sample = " & annual_ever==0"
	}
	if `c1'==5 {
		local DEPVAR = "ihs_kwh"
		local ifs_sample = " & perennial_ever==0"
	}
	if `c1'==6 {
		local DEPVAR = "ihs_kwh"
		local ifs_sample = " & ann_per_switcher==1"
	}
	}
	
	// Loop over 6 specifications
	foreach c2 of numlist 1/6 {
		
		if `c2'==1 {
			local FEs = "sp_group#month modate"
			local RHS = "log_p_mean"
		}
		if `c2'==2 {
			local FEs = "sp_group#month modate"
			local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		}
		if `c2'==3 {
			local FEs = "sp_group#month sp_group#rt_large_ag modate"
			local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		}
		if `c2'==4 {
			local FEs = "sp_group#month sp_group#rt_large_ag basin_group#year wdist_group#year modate"
			local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		}
		if `c2'==5 {
			local FEs = "sp_group#month sp_group#rt_large_ag modate"
			local RHS = "(log_p_mean = log_p_mean_deflag*)"
		}
		if `c2'==6 {
			local FEs = "sp_group#month sp_group#rt_large_ag modate sp_group#c.modate"
			local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		}
	
	
		// Set row to store output
		local row = `row' + 1
		
		// Run non-IV specification	
		if substr("`RHS'",1,1)!="(" {
						
			// Run OLS regression
			reghdfe `DEPVAR' `RHS' `ifs_base' `ifs_sample', absorb(`FEs') vce(cluster `VCE')
			
			// Store results
			replace panel = "`panel'" in `row'
			replace pull = "`pull'" in `row'
			replace ifs_base = "`ifs_base'" in `row'
			replace ifs_sample = "`ifs_sample'" in `row'
			replace depvar = "`DEPVAR'" in `row'
			replace fes = "`FEs'" in `row'
			replace rhs = "`RHS'" in `row'
			replace beta_log_p_mean = _b[log_p_mean] in `row'
			replace se_log_p_mean = _se[log_p_mean] in `row'
			replace t_log_p_mean =  _b[log_p_mean]/_se[log_p_mean] in `row'
			replace vce = "cluster `VCE'" in `row'
			replace n_obs = e(N) in `row'
			replace n_SPs = e(N_clust1) in `row'
			replace n_modates = e(N_clust2) in `row'
			replace dof = e(df_r) in `row'
					
		}
		
		// Run IV specifications
		else {
						
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
			replace beta_log_p_mean = _b[log_p_mean] in `row'
			replace se_log_p_mean = _se[log_p_mean] in `row'
			replace t_log_p_mean =  _b[log_p_mean]/_se[log_p_mean] in `row'
			replace vce = "cluster `VCE'" in `row'
			replace n_obs = e(N) in `row'
			replace n_SPs = e(N_clust1) in `row'
			replace n_modates = e(N_clust2) in `row'
			replace dof = e(df_r) in `row'
			replace fstat_rk = e(rkf) in `row'
					
			// Run first stage regression
			local RHSfs = subinstr(subinstr(subinstr("`RHS'","(","",.),")","",.),"=","",.)
			reghdfe `RHSfs' `ifs_base' `ifs_sample', absorb(`FEs') vce(cluster `VCE')
			
			// Store results
			cap replace fs_beta_default = _b[log_mean_p_kwh_ag_default] in `row'
			cap replace fs_se_default = _se[log_mean_p_kwh_ag_default] in `row'
			cap replace fs_t_default =  _b[log_mean_p_kwh_ag_default]/_se[log_mean_p_kwh_ag_default] in `row'
			cap replace fs_beta_deflag12 = _b[log_p_mean_deflag12] in `row'
			cap replace fs_se_deflag12 = _se[log_p_mean_deflag12] in `row'
			cap replace fs_t_deflag12 =  _b[log_p_mean_deflag12]/_se[log_p_mean_deflag12] in `row'
			cap replace fs_beta_deflag6 = _b[log_p_mean_deflag6] in `row'
			cap replace fs_se_deflag6 = _se[log_p_mean_deflag6] in `row'
			cap replace fs_t_deflag6 =  _b[log_p_mean_deflag6]/_se[log_p_mean_deflag6] in `row'
			
		}
		
		// Save output
		if `c1'==1 & `c2'==1 {
			cap erase "$dirpath_data/results/regs_pge_elec_monthly_crop_july2020.dta"
		}
		preserve
		keep panel-fs_t_deflag6
		dropmiss, obs force
		cap append using "$dirpath_data/results/regs_pge_elec_monthly_crop_july2020.dta"
		duplicates drop
		compress
		save "$dirpath_data/results/regs_pge_elec_monthly_crop_july2020.dta", replace
		restore
		
	}
}

}

************************************************
************************************************





