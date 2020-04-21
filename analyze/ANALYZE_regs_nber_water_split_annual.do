clear all
version 13
set more off

*******************************************************************
** Script to run annual water regressions (split) for NBER paper **
*******************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

************************************************
************************************************

** 1. Monthly regressions with decomposed P^groundwater
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
gen n_years = .
gen dof = .
gen fstat_rk = .
gen fse_beta_default = .
gen fse_se_default = .
gen fse_t_default = .
gen fse_beta_depth = .
gen fse_se_depth = .
gen fse_t_depth = .
gen fse_beta_depthlag = .
gen fse_se_depthlag = .
gen fse_t_depthlag = .
gen fsw_beta_default = .
gen fsw_se_default = .
gen fsw_t_default = .
gen fsw_beta_depth = .
gen fsw_se_depth = .
gen fsw_t_depth = .
gen fsw_beta_depthlag = .
gen fsw_se_depthlag = .
gen fsw_t_depthlag = .
	
		
// Loop over 36 regressions
foreach c of numlist 1/36 {

	if `c'==1 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default) ln_kwhaf_rast_dd_2SP"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==2 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==3 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & inlist(basin_group,68,121,122)"
	}
	if `c'==4 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_2SP = log_mean_p_kwh_ag_default L1.ln_gw_mean_depth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==5 {
		local DEPVAR = "elec_binary"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default) ln_kwhaf_rast_dd_2SP"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==6 {
		local DEPVAR = "elec_binary"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==7 {
		local DEPVAR = "elec_binary"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & inlist(basin_group,68,121,122)"
	}
	if `c'==8 {
		local DEPVAR = "elec_binary"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_2SP = log_mean_p_kwh_ag_default L1.ln_gw_mean_depth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==9 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default) ln_kwhaf_rast_dd_2SP"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & elec_binary_frac==1"
	}
	if `c'==10 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & elec_binary_frac==1"
	}
	if `c'==11 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & inlist(basin_group,68,121,122) & elec_binary_frac==1"
	}
	if `c'==12 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_2SP = log_mean_p_kwh_ag_default L1.ln_gw_mean_depth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & elec_binary_frac==1"
	}
	if `c'==13 {
		local DEPVAR = "no_crop"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default) ln_kwhaf_rast_dd_2SP"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==14 {
		local DEPVAR = "no_crop"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==15 {
		local DEPVAR = "no_crop"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & inlist(basin_group,68,121,122)"
	}
	if `c'==16 {
		local DEPVAR = "no_crop"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_2SP = log_mean_p_kwh_ag_default L1.ln_gw_mean_depth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==17 {
		local DEPVAR = "annual"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default) ln_kwhaf_rast_dd_2SP"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==18 {
		local DEPVAR = "annual"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==19 {
		local DEPVAR = "annual"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & inlist(basin_group,68,121,122)"
	}
	if `c'==20 {
		local DEPVAR = "annual"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_2SP = log_mean_p_kwh_ag_default L1.ln_gw_mean_depth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==21 {
		local DEPVAR = "perennial"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default) ln_kwhaf_rast_dd_2SP"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==22 {
		local DEPVAR = "perennial"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==23 {
		local DEPVAR = "perennial"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & inlist(basin_group,68,121,122)"
	}
	if `c'==24 {
		local DEPVAR = "perennial"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_2SP = log_mean_p_kwh_ag_default L1.ln_gw_mean_depth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	}
	if `c'==25 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default) ln_kwhaf_rast_dd_2SP"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & annual_ever==0"
	}
	if `c'==26 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & annual_ever==0"
	}
	if `c'==27 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & inlist(basin_group,68,121,122) & annual_ever==0"
	}
	if `c'==28 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_2SP = log_mean_p_kwh_ag_default L1.ln_gw_mean_depth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & annual_ever==0"
	}
	if `c'==29 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default) ln_kwhaf_rast_dd_2SP"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & perennial_ever==0"
	}
	if `c'==30 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & perennial_ever==0"
	}
	if `c'==31 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & inlist(basin_group,68,121,122) & perennial_ever==0"
	}
	if `c'==32 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_2SP = log_mean_p_kwh_ag_default L1.ln_gw_mean_depth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & perennial_ever==0"
	}
	if `c'==33 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_default) ln_kwhaf_rast_dd_2SP"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & ann_per_switcher==1"
	}
	if `c'==34 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & ann_per_switcher==1"
	}
	if `c'==35 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & inlist(basin_group,68,121,122) & ann_per_switcher==1"
	}
	if `c'==36 {
		local DEPVAR = "ihs_kwh"
		local FEs = "sp_group#rt_large_ag year"
		local RHS = "(log_p_mean ln_kwhaf_rast_dd_2SP = log_mean_p_kwh_ag_default L1.ln_gw_mean_depth_2SP)"
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & ann_per_switcher==1"
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
	if inlist(`c',1,2,3,4,9,10,11,12,25,26,27,28,29,30,31,32,33,34,35,36) {
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
	replace n_years = e(N_clust2) in `c'
	replace dof = e(df_r) in `c'
	replace fstat_rk = e(rkf) in `c'
			
	// Run first stage regression (electricity price)
	local RHSfs = subinstr(subinstr(subinstr("`RHS'","(","",.),")","",.),"=","",.)
	local RHSfs_elec = subinstr("`RHSfs'","ln_kwhaf_rast_dd_2SP","",.)
	reghdfe `RHSfs_elec' `if_sample', absorb(`FEs') vce(cluster `VCE')
	
	replace fse_beta_default = _b[log_mean_p_kwh_ag_default] in `c'
	replace fse_se_default = _se[log_mean_p_kwh_ag_default] in `c'
	replace fse_t_default =  _b[log_mean_p_kwh_ag_default]/_se[log_mean_p_kwh_ag_default] in `c'
	cap replace fse_beta_depth = _b[ln_gw_mean_depth_2SP] in `c'
	cap replace fse_se_depth = _se[ln_gw_mean_depth_2SP] in `c'
	cap replace fse_t_depth =  _b[ln_gw_mean_depth_2SP]/_se[ln_gw_mean_depth_2SP] in `c'
	cap replace fse_beta_depthlag = _b[L1.ln_gw_mean_depth_2SP] in `c'
	cap replace fse_se_depthlag = _se[L1.ln_gw_mean_depth_2SP] in `c'
	cap replace fse_t_depthlag =  _b[L1.ln_gw_mean_depth_2SP]/_se[L1.ln_gw_mean_depth_2SP] in `c'

	
	// Run first stage regression (kwhaf)
	if inlist(`c',1,5,9,13,17,21,25,29,33)==0 {

		local RHSfs_water = subinstr("`RHSfs'","log_p_mean","",.)
		reghdfe `RHSfs_water' `if_sample', absorb(`FEs') vce(cluster `VCE')
		
		replace fsw_beta_default = _b[log_mean_p_kwh_ag_default] in `c'
		replace fsw_se_default = _se[log_mean_p_kwh_ag_default] in `c'
		replace fsw_t_default =  _b[log_mean_p_kwh_ag_default]/_se[log_mean_p_kwh_ag_default] in `c'
		cap replace fsw_beta_depth = _b[ln_gw_mean_depth_2SP] in `c'
		cap replace fsw_se_depth = _se[ln_gw_mean_depth_2SP] in `c'
		cap replace fsw_t_depth =  _b[ln_gw_mean_depth_2SP]/_se[ln_gw_mean_depth_2SP] in `c'
		cap replace fsw_beta_depthlag = _b[L1.ln_gw_mean_depth_2SP] in `c'
		cap replace fsw_se_depthlag = _se[L1.ln_gw_mean_depth_2SP] in `c'
		cap replace fsw_t_depthlag =  _b[L1.ln_gw_mean_depth_2SP]/_se[L1.ln_gw_mean_depth_2SP] in `c'
	}
	
	
}

// Save output
keep panel-fsw_t_depthlag
dropmiss, obs force
compress
save "$dirpath_data/results/regs_nber_water_split_annual.dta", replace

}

************************************************
************************************************
