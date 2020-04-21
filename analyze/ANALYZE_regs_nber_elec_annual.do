clear all
version 13
set more off

*****************************************************************
** Script to run annual electricity regressions for NBER paper **
*****************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

************************************************
************************************************

** 1. Monthly regressions
{ 

// Load monthly panel
use "$dirpath_data/merged/sp_annual_elec_panel.dta", clear
local panel = "annual"

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

// Drop SP-years without a full year of data (< 12 months of bill data)
keep if flag_partial_year==0

// Define cluster variables
local VCE = "sp_group year"	
	
// Create empty variables to populate for storign results
gen panel = ""
drop pull
gen pull = ""
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
gen n_years = .
gen dof = .
gen fstat_rk = .
gen fs_beta_default = .
gen fs_se_default = .
gen fs_t_default = .
gen fs_beta_deflag = .
gen fs_se_deflag = .
gen fs_t_deflag = .
	
	
// Loop over 54 regressions
foreach c of numlist 1/54 {

	if `c'==1 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group year"
		local RHS = "log_p_mean"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	}
	if `c'==2 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	}
	if `c'==3 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	}
	if `c'==4 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag basin_group#year wdist_group#year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	}
	if `c'==5 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean = log_p_mean_deflag)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	}
	if `c'==6 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year sp_group#c.year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	}
	if `c'==7 {
		local DEPVAR = "elec_binary"
		local FEs = "sp_group year"
		local RHS = "log_p_mean"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	}
	if `c'==8 {
		local DEPVAR = "elec_binary"
		local FEs = "sp_group year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	}
	if `c'==9 {
		local DEPVAR = "elec_binary"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	}
	if `c'==10 {
		local DEPVAR = "elec_binary"
		local FEs = "sp_group#rt_large_ag basin_group#year wdist_group#year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	}
	if `c'==11 {
		local DEPVAR = "elec_binary"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean = log_p_mean_deflag)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	}
	if `c'==12 {
		local DEPVAR = "elec_binary"
		local FEs = "sp_group#rt_large_ag year sp_group#c.year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	}
	if `c'==13 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group year"
		local RHS = "log_p_mean"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0 & elec_binary_frac==1"
	}
	if `c'==14 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0 & elec_binary_frac==1"
	}
	if `c'==15 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0 & elec_binary_frac==1"
	}
	if `c'==16 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag basin_group#year wdist_group#year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0 & elec_binary_frac==1"
	}
	if `c'==17 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean = log_p_mean_deflag)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0 & elec_binary_frac==1"
	}
	if `c'==18 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year sp_group#c.year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0 & elec_binary_frac==1"
	}
	if `c'==19 {
		local DEPVAR = "no_crop"
		local FEs = "sp_group year"
		local RHS = "log_p_mean"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	}
	if `c'==20 {
		local DEPVAR = "no_crop"
		local FEs = "sp_group year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	}
	if `c'==21 {
		local DEPVAR = "no_crop"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	}
	if `c'==22 {
		local DEPVAR = "no_crop"
		local FEs = "sp_group#rt_large_ag basin_group#year wdist_group#year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	}
	if `c'==23 {
		local DEPVAR = "no_crop"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean = log_p_mean_deflag)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	}
	if `c'==24 {
		local DEPVAR = "no_crop"
		local FEs = "sp_group#rt_large_ag year sp_group#c.year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	}
	if `c'==25 {
		local DEPVAR = "annual"
		local FEs = "sp_group year"
		local RHS = "log_p_mean"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	}
	if `c'==26 {
		local DEPVAR = "annual"
		local FEs = "sp_group year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	}
	if `c'==27 {
		local DEPVAR = "annual"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	}
	if `c'==28 {
		local DEPVAR = "annual"
		local FEs = "sp_group#rt_large_ag basin_group#year wdist_group#year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	}
	if `c'==29 {
		local DEPVAR = "annual"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean = log_p_mean_deflag)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	}
	if `c'==30 {
		local DEPVAR = "annual"
		local FEs = "sp_group#rt_large_ag year sp_group#c.year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	}
	if `c'==31 {
		local DEPVAR = "perennial"
		local FEs = "sp_group year"
		local RHS = "log_p_mean"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	}
	if `c'==32 {
		local DEPVAR = "perennial"
		local FEs = "sp_group year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	}
	if `c'==33 {
		local DEPVAR = "perennial"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	}
	if `c'==34 {
		local DEPVAR = "perennial"
		local FEs = "sp_group#rt_large_ag basin_group#year wdist_group#year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	}
	if `c'==35 {
		local DEPVAR = "perennial"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean = log_p_mean_deflag)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	}
	if `c'==36 {
		local DEPVAR = "perennial"
		local FEs = "sp_group#rt_large_ag year sp_group#c.year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
	}
	if `c'==37 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group year"
		local RHS = "log_p_mean"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0 & annual_ever==0"
	}
	if `c'==38 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0 & annual_ever==0"
	}
	if `c'==39 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0 & annual_ever==0"
	}
	if `c'==40 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag basin_group#year wdist_group#year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0 & annual_ever==0"
	}
	if `c'==41 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean = log_p_mean_deflag)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0 & annual_ever==0"
	}
	if `c'==42 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year sp_group#c.year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0 & annual_ever==0"
	}
	if `c'==43 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group year"
		local RHS = "log_p_mean"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0 & perennial_ever==0"
	}
	if `c'==44 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0 & perennial_ever==0"
	}
	if `c'==45 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0 & perennial_ever==0"
	}
	if `c'==46 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag basin_group#year wdist_group#year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0 & perennial_ever==0"
	}
	if `c'==47 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean = log_p_mean_deflag)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0 & perennial_ever==0"
	}
	if `c'==48 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year sp_group#c.year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0 & perennial_ever==0"
	}
	if `c'==49 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group year"
		local RHS = "log_p_mean"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0 & ann_per_switcher==1"
	}
	if `c'==50 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0 & ann_per_switcher==1"
	}
	if `c'==51 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0 & ann_per_switcher==1"
	}
	if `c'==52 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag basin_group#year wdist_group#year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0 & ann_per_switcher==1"
	}
	if `c'==53 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean = log_p_mean_deflag)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0 & ann_per_switcher==1"
	}
	if `c'==54 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year sp_group#c.year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0 & ann_per_switcher==1"
	}

	
	// Run non-IV specification	
	if inlist(`c',1,7,13,19,25,31,37,43,49) {
					
		// Run OLS regression
		reghdfe `DEPVAR' `RHS' `if_sample', absorb(`FEs') vce(cluster `VCE')
		
		// Store results
		replace panel = "`panel'" in `c'
		replace pull = "`pull'" in `c'
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
		replace n_years = e(N_clust2) in `c'
		replace dof = e(df_r) in `c'
	}
	
	// Run IV specifications
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
		replace beta_log_p_mean = _b[log_p_mean] in `c'
		replace se_log_p_mean = _se[log_p_mean] in `c'
		replace t_log_p_mean =  _b[log_p_mean]/_se[log_p_mean] in `c'
		replace vce = "cluster `VCE'" in `c'
		replace n_obs = e(N) in `c'
		replace n_SPs = e(N_clust1) in `c'
		replace n_years = e(N_clust2) in `c'
		replace dof = e(df_r) in `c'
		replace fstat_rk = e(rkf) in `c'
				
		// Run first stage regression
		local RHSfs = subinstr(subinstr(subinstr("`RHS'","(","",.),")","",.),"=","",.)
		reghdfe `RHSfs' , absorb(`FEs') vce(cluster `VCE')
		
		if inlist(`c',5,11,17,23,29,35,41,47,53) {
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
save "$dirpath_data/results/regs_nber_elec_annual.dta", replace

}

************************************************
************************************************
