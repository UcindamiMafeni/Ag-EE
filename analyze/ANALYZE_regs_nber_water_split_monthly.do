clear all
version 13
set more off

********************************************************************
** Script to run monthly water regressions (split) for NBER paper **
********************************************************************

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
	
		
// Loop over 54 regressions
foreach c of numlist 1/54 {

	if `c'==1 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default) ln_kwhaf_rast_dd_mth_2SP"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==2 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==3 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & inlist(basin_group,68,121,122)"
	}
	if `c'==4 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_qtr_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_qtr_2SP)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==5 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#month sp_group#rt_large_ag wdist_group#year basin_group#year modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==6 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default L6_ln_gw_mean_depth_mth_2SP L12_ln_gw_mean_depth_mth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==7 {
		local DEPVAR = "elec_binary"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default) ln_kwhaf_rast_dd_mth_2SP"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==8 {
		local DEPVAR = "elec_binary"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==9 {
		local DEPVAR = "elec_binary"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & inlist(basin_group,68,121,122)"
	}
	if `c'==10 {
		local DEPVAR = "elec_binary"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_qtr_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_qtr_2SP)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==11 {
		local DEPVAR = "elec_binary"
		local FEs = "sp_group#month sp_group#rt_large_ag wdist_group#year basin_group#year modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==12 {
		local DEPVAR = "elec_binary"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default L6_ln_gw_mean_depth_mth_2SP L12_ln_gw_mean_depth_mth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==13 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default) ln_kwhaf_rast_dd_mth_2SP"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & elec_binary_frac > 0.9"
	}
	if `c'==14 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & elec_binary_frac > 0.9"
	}
	if `c'==15 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & elec_binary_frac > 0.9 & inlist(basin_group,68,121,122)"
	}
	if `c'==16 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_qtr_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_qtr_2SP)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & elec_binary_frac > 0.9"
	}
	if `c'==17 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#month sp_group#rt_large_ag wdist_group#year basin_group#year modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & elec_binary_frac > 0.9"
	}
	if `c'==18 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default L6_ln_gw_mean_depth_mth_2SP L12_ln_gw_mean_depth_mth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & elec_binary_frac > 0.9"
	}
	if `c'==19 {
		local DEPVAR = "no_crop"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default) ln_kwhaf_rast_dd_mth_2SP"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==20 {
		local DEPVAR = "no_crop"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==21 {
		local DEPVAR = "no_crop"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & inlist(basin_group,68,121,122)"
	}
	if `c'==22 {
		local DEPVAR = "no_crop"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_qtr_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_qtr_2SP)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==23 {
		local DEPVAR = "no_crop"
		local FEs = "sp_group#month sp_group#rt_large_ag wdist_group#year basin_group#year modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==24 {
		local DEPVAR = "no_crop"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default L6_ln_gw_mean_depth_mth_2SP L12_ln_gw_mean_depth_mth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==25 {
		local DEPVAR = "annual"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default) ln_kwhaf_rast_dd_mth_2SP"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==26 {
		local DEPVAR = "annual"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==27 {
		local DEPVAR = "annual"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & inlist(basin_group,68,121,122)"
	}
	if `c'==28 {
		local DEPVAR = "annual"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_qtr_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_qtr_2SP)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==29 {
		local DEPVAR = "annual"
		local FEs = "sp_group#month sp_group#rt_large_ag wdist_group#year basin_group#year modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==30 {
		local DEPVAR = "annual"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default L6_ln_gw_mean_depth_mth_2SP L12_ln_gw_mean_depth_mth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==31 {
		local DEPVAR = "perennial"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default) ln_kwhaf_rast_dd_mth_2SP"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==32 {
		local DEPVAR = "perennial"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==33 {
		local DEPVAR = "perennial"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & inlist(basin_group,68,121,122)"
	}
	if `c'==34 {
		local DEPVAR = "perennial"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_qtr_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_qtr_2SP)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==35 {
		local DEPVAR = "perennial"
		local FEs = "sp_group#month sp_group#rt_large_ag wdist_group#year basin_group#year modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==36 {
		local DEPVAR = "perennial"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default L6_ln_gw_mean_depth_mth_2SP L12_ln_gw_mean_depth_mth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `c'==37 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default) ln_kwhaf_rast_dd_mth_2SP"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & annual_ever==0"
	}
	if `c'==38 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & annual_ever==0"
	}
	if `c'==39 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & annual_ever==0 & inlist(basin_group,68,121,122)"
	}
	if `c'==40 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_qtr_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_qtr_2SP)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & annual_ever==0"
	}
	if `c'==41 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#month sp_group#rt_large_ag wdist_group#year basin_group#year modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & annual_ever==0"
	}
	if `c'==42 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default L6_ln_gw_mean_depth_mth_2SP L12_ln_gw_mean_depth_mth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & annual_ever==0"
	}
	if `c'==43 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default) ln_kwhaf_rast_dd_mth_2SP"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & perennial_ever==0"
	}
	if `c'==44 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & perennial_ever==0"
	}
	if `c'==45 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & perennial_ever==0 & inlist(basin_group,68,121,122)"
	}
	if `c'==46 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_qtr_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_qtr_2SP)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & perennial_ever==0"
	}
	if `c'==47 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#month sp_group#rt_large_ag wdist_group#year basin_group#year modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & perennial_ever==0"
	}
	if `c'==48 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default L6_ln_gw_mean_depth_mth_2SP L12_ln_gw_mean_depth_mth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & perennial_ever==0"
	}
	if `c'==49 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default) ln_kwhaf_rast_dd_mth_2SP"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & ann_per_switcher==1"
	}
	if `c'==50 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & ann_per_switcher==1"
	}
	if `c'==51 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & ann_per_switcher==1 & inlist(basin_group,68,121,122)"
	}
	if `c'==52 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_qtr_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_qtr_2SP)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & ann_per_switcher==1"
	}
	if `c'==53 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#month sp_group#rt_large_ag wdist_group#year basin_group#year modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)" 
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & ann_per_switcher==1"
	}
	if `c'==54 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#month sp_group#rt_large_ag modate"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default L6_ln_gw_mean_depth_mth_2SP L12_ln_gw_mean_depth_mth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & ann_per_switcher==1"
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
	local kwhaf_var = word("`RHS'",2)
	if "`kwhaf_var'"=="=" {
		local kwhaf_var = word("`RHS'",wordcount("`RHS'"))
	}
	if inlist(`c',1,2,3,4,5,6,13,14,15,16,17,18,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54) {
		replace beta_log_kwhaf = _b[`kwhaf_var']-1 in `c'
		replace se_log_kwhaf = _se[`kwhaf_var'] in `c'
		replace t_log_kwhaf = (_b[`kwhaf_var']-1)/_se[`kwhaf_var'] in `c'
		test log_p_mean = `kwhaf_var' - 1
	}
	else {
		replace beta_log_kwhaf = _b[`kwhaf_var'] in `c'
		replace se_log_kwhaf = _se[`kwhaf_var'] in `c'
		replace t_log_kwhaf = (_b[`kwhaf_var'])/_se[`kwhaf_var'] in `c'
		test log_p_mean = `kwhaf_var'
	}
	replace pval_equal = r(p) in `c'
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
	if inlist(`c',1,7,13,19,25,31,37,43,49)==0 {

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
save "$dirpath_data/results/regs_nber_water_split_monthly.dta", replace

}

************************************************
************************************************
