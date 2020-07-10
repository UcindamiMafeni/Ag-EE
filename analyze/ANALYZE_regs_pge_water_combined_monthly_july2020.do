clear all
version 13
set more off

**************************************************************************************************
** Script to run main set of monthly PGE water regressions (combined) for July 2020 paper draft **
**************************************************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

************************************************
************************************************

** 1. Monthly regressions with combined P^groundwater
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
	
local row = 0	
	
// Loop over 12 combinations of outcome+sample
foreach c1 of numlist 1/12 {

	if `c1'==1 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = ""
	}
	if `c1'==2 {
		local DEPVAR = "ihs_af_rast_dd_qtr_2SP"
		local ifs_sample = ""
	}
	if `c1'==3 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & log_af_rast_dd_mth_2SP!=."
	}
	if `c1'==4 {
		local DEPVAR = "log_af_rast_dd_mth_2SP"
		local ifs_sample = ""
	}
	if `c1'==5 {
		local DEPVAR = "log1_af_rast_dd_mth_2SP"
		local ifs_sample = ""
	}
	if `c1'==6 {
		local DEPVAR = "log1_100af_rast_dd_mth_2SP"
		local ifs_sample = ""
	}
	if `c1'==7 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & inlist(basin_group,68,121,122)"
	}
	if `c1'==8 {
		local DEPVAR = "ihs_af_rast_dd_qtr_2SP"
		local ifs_sample = " & inlist(basin_group,68,121,122)"
	}
	if `c1'==9 {
		local DEPVAR = "elec_binary"
		local ifs_sample = ""
	}
	if `c1'==10 {
		local DEPVAR = "elec_binary"
		local ifs_sample = ""
	}
	if `c1'==11 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & elec_binary_frac>0.9"
	}
	if `c1'==12 {
		local DEPVAR = "ihs_af_rast_dd_qtr_2SP"
		local ifs_sample = " & elec_binary_frac>0.9"
	}
	
	local stub = subinstr(subinstr(subinstr(subinstr("`DEPVAR'","ihs_","",1),"log_","",1),"log1_","",1),"100","",1)
	if "`stub'"=="elec_binary" & `c1'==9 {
		local stub = "af_rast_dd_mth_2SP"
	}
	else if "`stub'"=="elec_binary" & `c1'==10 {
		local stub = "af_rast_dd_qtr_2SP"
	}
	
	// Loop over 4 specifications
	foreach c2 of numlist 1/4 {
		
		if `c2'==1 {
			local FEs = "sp_group#month sp_group#rt_large_ag modate"
			local RHS = "ln_mean_p_`stub'"
		}
		if `c2'==2 {
			local FEs = "sp_group#month sp_group#rt_large_ag modate"
			local RHS = "(ln_mean_p_`stub' = log_mean_p_kwh_ag_default)"
		}
		if `c2'==3 {
			local FEs = "sp_group#month sp_group#rt_large_ag wdist_group#year basin_group#year modate"
			local RHS = "(ln_mean_p_`stub' = log_mean_p_kwh_ag_default)"
		}
		if `c2'==4 {
			local FEs = "sp_group#month sp_group#rt_large_ag modate"
			local RHS = "(ln_mean_p_`stub' = log_p_mean_deflag*)"
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
			replace beta_log_p_water = _b[ln_mean_p_`stub'] in `row'
			replace se_log_p_water = _se[ln_mean_p_`stub'] in `row'
			replace t_log_p_water =  _b[ln_mean_p_`stub']/_se[ln_mean_p_`stub'] in `row'
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
			replace beta_log_p_water = _b[ln_mean_p_`stub'] in `row'
			replace se_log_p_water = _se[ln_mean_p_`stub'] in `row'
			replace t_log_p_water =  _b[ln_mean_p_`stub']/_se[ln_mean_p_`stub'] in `row'
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
			cap erase "$dirpath_data/results/regs_pge_water_combined_monthly_july2020.dta"
		}
		preserve
		keep panel-fs_t_deflag6
		dropmiss, obs force
		cap append using "$dirpath_data/results/regs_pge_water_combined_monthly_july2020.dta"
		duplicates drop
		compress
		save "$dirpath_data/results/regs_pge_water_combined_monthly_july2020.dta", replace
		restore
	}	
}


}

************************************************
************************************************
