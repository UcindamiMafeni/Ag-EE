clear all
version 13
set more off

**********************************************************************
** Script to run annual water regressions (combined) for NBER paper **
**********************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

************************************************
************************************************

** 1. Monthly regressions with combined P^groundwater
{ 

// Load monthly panel
use "$dirpath_data/merged/sp_annual_water_panel.dta", clear
local panel = "annual"

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

// Drop SP-years without a full year of data (< 12 months of bill data)
keep if flag_partial_year==0

// Define cluster variables
local VCE = "sp_group year"	
	
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
gen n_years = .
gen dof = .
gen fstat_rk = .
gen fs_beta_default = .
gen fs_se_default = .
gen fs_t_default = .
gen fs_beta_deflag = .
gen fs_se_deflag = .
gen fs_t_deflag = .
	
		
// Loop over 45 regressions
foreach c of numlist 1/45 {

	if `c'==1 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "ihs_af_rast_dd_2SP"
		local RHS = "ln_mean_p_af_rast_dd_2SP"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==2 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "ihs_af_rast_dd_2SP"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_mean_p_kwh_ag_default)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==3 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "ihs_af_rast_dd_2SP"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & inlist(basin_group,68,121,122)"
	}
	if `c'==4 {
		local FEs = "sp_group#rt_large_ag basin_group#year wdist_group#year"
		local DEPVAR = "ihs_af_rast_dd_2SP"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_mean_p_kwh_ag_default)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==5 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "ihs_af_rast_dd_2SP"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_p_mean_deflag)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==6 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "elec_binary"
		local RHS = "ln_mean_p_af_rast_dd_2SP"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==7 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "elec_binary"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_mean_p_kwh_ag_default)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==8 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "elec_binary"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & inlist(basin_group,68,121,122)"
	}
	if `c'==9 {
		local FEs = "sp_group#rt_large_ag basin_group#year wdist_group#year"
		local DEPVAR = "elec_binary"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_mean_p_kwh_ag_default)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==10 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "elec_binary"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_p_mean_deflag)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==11 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "ihs_af_rast_dd_2SP"
		local RHS = "ln_mean_p_af_rast_dd_2SP"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & elec_binary_frac==1"
	}
	if `c'==12 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "ihs_af_rast_dd_2SP"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_mean_p_kwh_ag_default)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & elec_binary_frac==1"
	}
	if `c'==13 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "ihs_af_rast_dd_2SP"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & inlist(basin_group,68,121,122) & elec_binary_frac==1"
	}
	if `c'==14 {
		local FEs = "sp_group#rt_large_ag basin_group#year wdist_group#year"
		local DEPVAR = "ihs_af_rast_dd_2SP"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_mean_p_kwh_ag_default)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & elec_binary_frac==1"
	}
	if `c'==15 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "ihs_af_rast_dd_2SP"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_p_mean_deflag)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & elec_binary_frac==1"
	}
	if `c'==16 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "no_crop"
		local RHS = "ln_mean_p_af_rast_dd_2SP"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==17 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "no_crop"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_mean_p_kwh_ag_default)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==18 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "no_crop"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & inlist(basin_group,68,121,122)"
	}
	if `c'==19 {
		local FEs = "sp_group#rt_large_ag basin_group#year wdist_group#year"
		local DEPVAR = "no_crop"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_mean_p_kwh_ag_default)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==20 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "no_crop"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_p_mean_deflag)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==21 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "annual"
		local RHS = "ln_mean_p_af_rast_dd_2SP"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==22 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "annual"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_mean_p_kwh_ag_default)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==23 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "annual"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & inlist(basin_group,68,121,122)"
	}
	if `c'==24 {
		local FEs = "sp_group#rt_large_ag basin_group#year wdist_group#year"
		local DEPVAR = "annual"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_mean_p_kwh_ag_default)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==25 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "annual"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_p_mean_deflag)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==26 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "perennial"
		local RHS = "ln_mean_p_af_rast_dd_2SP"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==27 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "perennial"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_mean_p_kwh_ag_default)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==28 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "perennial"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & inlist(basin_group,68,121,122)"
	}
	if `c'==29 {
		local FEs = "sp_group#rt_large_ag basin_group#year wdist_group#year"
		local DEPVAR = "perennial"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_mean_p_kwh_ag_default)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==30 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "perennial"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_p_mean_deflag)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==31 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "ihs_af_rast_dd_2SP"
		local RHS = "ln_mean_p_af_rast_dd_2SP"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & annual_ever==0"
	}
	if `c'==32 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "ihs_af_rast_dd_2SP"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_mean_p_kwh_ag_default)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & annual_ever==0"
	}
	if `c'==33 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "ihs_af_rast_dd_2SP"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & inlist(basin_group,68,121,122) & annual_ever==0"
	}
	if `c'==34 {
		local FEs = "sp_group#rt_large_ag basin_group#year wdist_group#year"
		local DEPVAR = "ihs_af_rast_dd_2SP"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_mean_p_kwh_ag_default)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & annual_ever==0"
	}
	if `c'==35 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "ihs_af_rast_dd_2SP"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_p_mean_deflag)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & annual_ever==0"
	}
	if `c'==36 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "ihs_af_rast_dd_2SP"
		local RHS = "ln_mean_p_af_rast_dd_2SP"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & perennial_ever==0"
	}
	if `c'==37 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "ihs_af_rast_dd_2SP"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_mean_p_kwh_ag_default)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & perennial_ever==0"
	}
	if `c'==38 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "ihs_af_rast_dd_2SP"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & inlist(basin_group,68,121,122) & perennial_ever==0"
	}
	if `c'==39 {
		local FEs = "sp_group#rt_large_ag basin_group#year wdist_group#year"
		local DEPVAR = "ihs_af_rast_dd_2SP"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_mean_p_kwh_ag_default)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & perennial_ever==0"
	}
	if `c'==40 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "ihs_af_rast_dd_2SP"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_p_mean_deflag)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & perennial_ever==0"
	}
	if `c'==41 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "ihs_af_rast_dd_2SP"
		local RHS = "ln_mean_p_af_rast_dd_2SP"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & ann_per_switcher==1"
	}
	if `c'==42 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "ihs_af_rast_dd_2SP"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_mean_p_kwh_ag_default)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & ann_per_switcher==1"
	}
	if `c'==43 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "ihs_af_rast_dd_2SP"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & inlist(basin_group,68,121,122) & ann_per_switcher==1"
	}
	if `c'==44 {
		local FEs = "sp_group#rt_large_ag basin_group#year wdist_group#year"
		local DEPVAR = "ihs_af_rast_dd_2SP"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_mean_p_kwh_ag_default)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & ann_per_switcher==1"
	}
	if `c'==45 {
		local FEs = "sp_group#rt_large_ag year"
		local DEPVAR = "ihs_af_rast_dd_2SP"
		local RHS = "(ln_mean_p_af_rast_dd_2SP = log_p_mean_deflag)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & ann_per_switcher==1"
	}

	// Run non-IV specification	
	if inlist(`c',1,6,11,16,21,26,31,36,41) {
					
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
		cap replace beta_log_p_water = _b[ln_mean_p_af_rast_dd_2SP] in `c'
		cap replace se_log_p_water = _se[ln_mean_p_af_rast_dd_2SP] in `c'
		cap replace t_log_p_water =  _b[ln_mean_p_af_rast_dd_2SP]/_se[ln_mean_p_af_rast_dd_2SP] in `c'
		replace vce = "cluster `VCE'" in `c'
		replace n_obs = e(N) in `c'
		replace n_SPs = e(N_clust1) in `c'
		replace n_years = e(N_clust2) in `c'
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
		cap replace beta_log_p_water = _b[ln_mean_p_af_rast_dd_2SP] in `c'
		cap replace se_log_p_water = _se[ln_mean_p_af_rast_dd_2SP] in `c'
		cap replace t_log_p_water =  _b[ln_mean_p_af_rast_dd_2SP]/_se[ln_mean_p_af_rast_dd_2SP] in `c'
		replace vce = "cluster `VCE'" in `c'
		replace n_obs = e(N) in `c'
		replace n_SPs = e(N_clust1) in `c'
		replace n_years = e(N_clust2) in `c'
		replace dof = e(df_r) in `c'
		replace fstat_rk = e(rkf) in `c'
				
		// Run first stage regression 
		local RHSfs = subinstr(subinstr(subinstr("`RHS'","(","",.),")","",.),"=","",.)
		reghdfe `RHSfs' `if_sample', absorb(`FEs') vce(cluster `VCE')
	
		if inlist(`c',5,10,15,20,25,30,35,40,45) {
			replace fs_beta_deflag = _b[log_p_mean_deflag] in `c'
			replace fs_se_deflag = _se[log_p_mean_deflag] in `c'
			replace fs_t_deflag =  _b[log_p_mean_deflag]/_se[log_p_mean_deflag] in `c'
		}
		else {
			replace fs_beta_default = _b[log_mean_p_kwh_ag_default] in `c'
			replace fs_se_default = _se[log_mean_p_kwh_ag_default] in `c'
			replace fs_t_default =  _b[log_mean_p_kwh_ag_default]/_se[log_mean_p_kwh_ag_default] in `c'
		}				
	}
}

// Save output
keep panel-fs_t_deflag
dropmiss, obs force
compress
save "$dirpath_data/results/regs_nber_water_combined_annual.dta", replace

}

************************************************
************************************************
