clear all
version 13
set more off

***********************************************************************************
** Script to run monthly PGE electricity sensitivities for July 2020 paper draft **
***********************************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

************************************************
************************************************

** 1. Monthly regressions (main set of sensitivities)
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
	
// Define baseline sample criteria (common to all regressions)
local ifs_base = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3"
	
// Confirm that by forcing a match to the water panel, we've forced out the two non-APEP-specific PGE data pullspulls
assert pull=="20180719" 
local pull = "PGE 20180719" 
drop pull

// Define cluster variables
local VCE = "sp_group modate"	

// Population missing groups for group-wise FEs, to avoid dropping them when we don't want to
replace wdist_group = 0 if wdist_group==.

// Define default global: dependent variable
global DEPVAR = "ihs_kwh"
	
// Define default global: RHS	
global RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
	
// Define default global: FEs
global FEs = "sp_group#month sp_group#rt_large_ag modate"
	
// Define default global: cluster variables
global VCE = "sp_group modate"	

// Create lags of precipitaion and temperature variables
sort sp_group modate
gen Lprecip_mm = L.precip_mm
gen LdegreesC_min = L.degreesC_min
gen LdegreesC_max = L.degreesC_max
gen LdegreesC_mean = L.degreesC_mean

// Create empty variables to populate for storing results
gen panel = ""
gen pull = ""
gen sens = ""
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
gen fs_beta_modal = .
gen fs_se_modal = .
gen fs_t_modal = .
gen fs_beta_deflag12 = .
gen fs_se_deflag12 = .
gen fs_t_deflag12 = .
gen fs_beta_deflag6 = .
gen fs_se_deflag6 = .
gen fs_t_deflag6 = .
gen fs_beta_modlag12 = .
gen fs_se_modlag12 = .
gen fs_t_modlag12 = .
gen fs_beta_modlag6 = .
gen fs_se_modlag6 = .
gen fs_t_modlag6 = .

	
// Loop over sensitivities
foreach c of numlist 60/61 {

	// Reset default locals
	local ifs_sample = ""
	local DEPVAR = "${DEPVAR}"
	local RHS = "${RHS}"
	local FEs = "${FEs}"
	local VCE = "${VCE}"

	if `c'==1 {
		local sens = "County by month-of-sample FEs"
		local FEs = "sp_group#month sp_group#rt_large_ag modate#county_group" 
	}
	if `c'==2 {
		local sens = "Climate zone by month-of-sample FEs"
		local FEs = "sp_group#month sp_group#rt_large_ag modate#cz_group" 
	}
	if `c'==3 {
		local sens = "Basin by month-of-sample FEs"
		local FEs = "sp_group#month sp_group#rt_large_ag modate#basin_group" 
	}
	if `c'==4 {
		local sens = "Sub-basin by month-of-sample FEs"
		local FEs = "sp_group#month sp_group#rt_large_ag modate#basin_sub_group" 
	}
	if `c'==5 {
		local sens = "Water district by month-of-sample FEs"
		local FEs = "sp_group#month sp_group#rt_large_ag modate#wdist_group" 
	}
	if `c'==6 {
		local sens = "SP-by-year FEs"
		local FEs = "sp_group#month sp_group#rt_large_ag sp_group#year modate" 
	}
	if `c'==7 {
		local sens = "SP-by-year FEs, but not SP-by-month FEs"
		local FEs = "sp_group#rt_large_ag sp_group#year modate" 
	}
	if `c'==8 {
		local sens = "Drop SPs without water districts"
		local ifs_sample = " & wdist_group!=0" 
	}
	if `c'==9 {
		local sens = "Water district by year FEs, drop SPs without water districts"
		local FEs = "sp_group#month sp_group#rt_large_ag modate wdist_group#year" 
		local ifs_sample = " & wdist_group!=0" 
	}
	if `c'==10 {
		local sens = "Drop SPs with water districts"
		local ifs_sample = " & wdist_group==0" 
	}
	if `c'==11 {
		local sens = "IV with modal tariff, not default tariff"
		local RHS = "(log_p_mean = log_mean_p_kwh_ag_modal)"
	}
	if `c'==12 {
		local sens = "IV with lagged modal tariff, not default tariff"
		local RHS = "(log_p_mean = log_p_mean_modlag*)"
	}
	if `c'==13 {
		local sens = "Depvar: Q in levels"
		local DEPVAR = "mnth_bill_kwh"
	}
	if `c'==14 {
		local sens = "Depvar: Q in levels, dropping high outliers"
		local DEPVAR = "mnth_bill_kwh"
		local ifs_sample = " & mnth_bill_kwh<100000" // close to 99th pctile
 	}
	if `c'==15 {
		local sens = "Within-category switchers only"
		local ifs_sample = " & sp_same_rate_in_cat==0"
	}
	if `c'==16 {
		local sens = "Within-category non-switchers only"
		local ifs_sample = " & sp_same_rate_in_cat==1"
	}
	if `c'==17 {
		local sens = "Within 60 months of pump test"
		local ifs_sample = " & months_to_nearest_test<=60"
	}
	if `c'==18 {
		local sens = "Within 48 months of pump test"
		local ifs_sample = " & months_to_nearest_test<=48"
	}
	if `c'==19 {
		local sens = "Within 36 months of pump test"
		local ifs_sample = " & months_to_nearest_test<=36"
	}
	if `c'==20 {
		local sens = "Within 24 months of pump test"
		local ifs_sample = " & months_to_nearest_test<=24"
	}
	if `c'==21 {
		local sens = "Within 12 months of pump test"
		local ifs_sample = " & months_to_nearest_test<=12"
	}
	if `c'==22 {
		local sens = "Dropping APEP-subsidized projects"
		local ifs_sample = " & apep_proj_count==0"
	}
	if `c'==23 {
		local sens = "Latlons within 100 miles"
		local ifs_sample = " & latlon_miles_apart<=100"
	}
	if `c'==24 {
		local sens = "Latlons within 50 miles"
		local ifs_sample = " & latlon_miles_apart<=50"
	}
	if `c'==25 {
		local sens = "Latlons within 25 miles"
		local ifs_sample = " & latlon_miles_apart<=25"
	}
	if `c'==26 {
		local sens = "Latlons within 10 miles"
		local ifs_sample = " & latlon_miles_apart<=10"
	}
	if `c'==27 {
		local sens = "Latlons within 5 miles"
		local ifs_sample = " & latlon_miles_apart<=5"
	}
	if `c'==28 {
		local sens = "Latlons within 1 miles"
		local ifs_sample = " & latlon_miles_apart<=1"
	}
	if `c'==29 {
		local sens = "Summer months only"
		local ifs_sample = " & summer==1"
	}
	if `c'==30 {
		local sens = "Winter months only"
		local ifs_sample = " & summer==0"
	}
	if `c'==31 {
		local sens = "San Joaquin basin only"
		local ifs_sample = " & basin_group==122"
	}
	if `c'==32 {
		local sens = "Sacramento basin only"
		local ifs_sample = " & basin_group==121"
	}
	if `c'==33 {
		local sens = "Salinas basin only"
		local ifs_sample = " & basin_group==68"
	}
	if `c'==34 {
		local sens = "Salinas, Sacramento, and San Joaquin basins"
		local ifs_sample = " & inlist(basin_group,68,121,122)"
	}
	if `c'==35 {
		local sens = "Control for basin-wide average depth (qtr, all)"
		local RHS = "${RHS} gw_qtr_bsn_mean1"
	}
	if `c'==36 {
		local sens = "Control for basin-wide average depth (qtr, non-questionable)"
		local RHS = "${RHS} gw_qtr_bsn_mean2"
	}
	if `c'==37 {
		local sens = "Control for basin-wide average depth (mth, all)"
		local RHS = "${RHS} gw_mth_bsn_mean1"
	}
	if `c'==38 {
		local sens = "Control for basin-wide average depth (mth, non-questionable)"
		local RHS = "${RHS} gw_mth_bsn_mean2"
	}
	if `c'==39 {
		local sens = "Cluster by CLU and month-of-sample"
		local VCE = "clu_id modate"
	}
	if `c'==40 {
		local sens = "Cluster by CLU_group0 and month-of-sample"
		local VCE = "clu_group0 modate"
	}
	if `c'==41 {
		local sens = "Cluster by CLU_group75 and month-of-sample"
		local VCE = "clu_group0 modate"
	}
	if `c'==42 {
		local sens = "CLU matches (SP-APEP) only"
		local ifs_sample = " & flag_clu_match==1"
	}
	if `c'==43 {
		local sens = "CLU_group75 matches (SP-APEP) only"
		local ifs_sample = " & flag_clu_group75_match==1"
	}
	if `c'==44 {
		local sens = "CLU_group0 matches (SP-APEP) only"
		local ifs_sample = " & flag_clu_group0_match==1"
	}
	if `c'==45 {
		local sens = "Dropping CLU inconsistencies"
		local ifs_sample = " & flag_clu_inconsistency==0"
	}
	if `c'==46 {
		local sens = "Single-SP CLUs"
		local ifs_sample = " & spcount_clu_id==1"
	}
	if `c'==47 {
		local sens = "Multi-SP CLUs"
		local ifs_sample = " & spcount_clu_id>1 & spcount_clu_id!=."
	}
	if `c'==48 {
		local sens = "Single-SP CLU_group0s"
		local ifs_sample = " & spcount_clu_group0==1"
	}
	if `c'==49 {
		local sens = "Multi-SP CLU_group0s"
		local ifs_sample = " & spcount_clu_group0>1 & spcount_clu_group0!=."
	}
	if `c'==50 {
		local sens = "Drop questionable water district (non)assignments"
		local ifs_sample = " & flag_wdist_ques==0"
	}
	if `c'==51 {
		local sens = "Drop SPs with EE measure"
		local ifs_sample = " & ee_measure_count==."
	}
	if `c'==52 {
		local sens = "SP lat/lon is <=20m from CLU edge"
		local ifs_sample = " & clu_ec_edge_dist_m<=20"
	}
	if `c'==53 {
		local sens = "SP lat/lon is >20m from CLU edge"
		local ifs_sample = " & clu_ec_edge_dist_m>20" // roughly the median
	}
	if `c'==54 {
		local sens = "SP lat/lon is inside CLU"
		local ifs_sample = " & clu_ec_edge_dist_m<=20" // roughly the median
	}
	if `c'==55 {
		local sens = "SP lat/lon is inside or w/in 20m of CLU"
		local ifs_sample = " & clu_ec_nearest_dist_m<=20" // 20 to be consistennt with above
	}
	if `c'==56 {
		local sens = "SP lat/lon is <=50m from nearest non-assigned CLU"
		local ifs_sample = " & neighbor_clu_ec_dist_m<=50" // roughly the median
	}
	if `c'==57 {
		local sens = "SP lat/lon is >50m from nearest non-assigned CLU"
		local ifs_sample = " & neighbor_clu_ec_dist_m>50" // roughly the median
	}
	if `c'==58 {
		local sens = "Control for monthly precipitation"
		local RHS = "${RHS} precip_mm"
	}
	if `c'==59 {
		local sens = "Control for monthly precipitation and temperature"
		local RHS = "${RHS} precip_mm degreesC_*"
	}
	if `c'==60 {
		local sens = "Control for current & lagged monthly precipitation"
		local RHS = "${RHS} precip_mm Lprecip_mm"
	}
	if `c'==61 {
		local sens = "Control for current & lagged monthly precipitation and temperature"
		local RHS = "${RHS} precip_mm Lprecip_mm degreesC_* LdegreesC_min LdegreesC_max LdegreesC_mean"
	}
	
		
	// Run non-IV specification	
	if substr("`RHS'",1,1)!="(" {
					
		// Run OLS regression
		reghdfe `DEPVAR' `RHS' `ifs_base' `ifs_sample' , absorb(`FEs') vce(cluster `VCE')
		
		// Store results
		replace panel = "`panel'" in `c'
		replace pull = "`pull'" in `c'
		replace sens = "`sens'" in `c'
		replace ifs_base = "`ifs_base'" in `c'
		replace ifs_sample = "`ifs_sample'" in `c'
		replace depvar = "`DEPVAR'" in `c'
		replace fes = "`FEs'" in `c'
		replace rhs = "`RHS'" in `c'
		replace beta_log_p_mean = _b[log_p_mean] in `c'
		replace se_log_p_mean = _se[log_p_mean] in `c'
		replace t_log_p_mean =  _b[log_p_mean]/_se[log_p_mean] in `c'
		replace vce = "cluster `VCE'" in `c'
		replace n_obs = e(N) in `c'
		replace n_SPs = e(N_clust1) in `c'
		replace n_modates = e(N_clust2) in `c'
		replace dof = e(df_r) in `c'
				
	}
	
	// Run IV specifications
	else {
					
		// Run 2SLS regression
		ivreghdfe `DEPVAR' `RHS' `ifs_base' `ifs_sample' , absorb(`FEs') cluster(`VCE')
		
		// Store results
		replace panel = "`panel'" in `c'
		replace pull = "`pull'" in `c'
		replace sens = "`sens'" in `c'
		replace ifs_base = "`ifs_base'" in `c'
		replace ifs_sample = "`ifs_sample'" in `c'
		replace depvar = "`DEPVAR'" in `c'
		replace fes = "`FEs'" in `c'
		replace rhs = "`RHS'" in `c'
		replace beta_log_p_mean = _b[log_p_mean] in `c'
		replace se_log_p_mean = _se[log_p_mean] in `c'
		replace t_log_p_mean =  _b[log_p_mean]/_se[log_p_mean] in `c'
		replace vce = "cluster `VCE'" in `c'
		replace n_obs = e(N) in `c'
		replace n_SPs = e(N_clust1) in `c'
		replace n_modates = e(N_clust2) in `c'
		replace dof = e(df_r) in `c'
		replace fstat_rk = e(rkf) in `c'
				
		// Run first stage regression
		local RHSfs = subinstr(subinstr(subinstr("`RHS'","(","",.),")","",.),"=","",.)
		reghdfe `RHSfs' `ifs_base' `ifs_sample', absorb(`FEs') vce(cluster `VCE')
		
		if regexm("`RHSfs'","log_mean_p_kwh_ag_default") {
			replace fs_beta_default = _b[log_mean_p_kwh_ag_default] in `c'
			replace fs_se_default = _se[log_mean_p_kwh_ag_default] in `c'
			replace fs_t_default =  _b[log_mean_p_kwh_ag_default]/_se[log_mean_p_kwh_ag_default] in `c'
		}
		else if regexm("`RHSfs'","log_mean_p_kwh_ag_modal") {
			replace fs_beta_modal = _b[log_mean_p_kwh_ag_modal] in `c'
			replace fs_se_modal = _se[log_mean_p_kwh_ag_modal] in `c'
			replace fs_t_modal =  _b[log_mean_p_kwh_ag_modal]/_se[log_mean_p_kwh_ag_modal] in `c'
		}
		else if regexm("`RHSfs'","log_p_mean_deflag*") {
			replace fs_beta_deflag12 = _b[log_p_mean_deflag12] in `c'
			replace fs_se_deflag12 = _se[log_p_mean_deflag12] in `c'
			replace fs_t_deflag12 =  _b[log_p_mean_deflag12]/_se[log_p_mean_deflag12] in `c'
			replace fs_beta_deflag6 = _b[log_p_mean_deflag6] in `c'
			replace fs_se_deflag6 = _se[log_p_mean_deflag6] in `c'
			replace fs_t_deflag6 =  _b[log_p_mean_deflag6]/_se[log_p_mean_deflag6] in `c'
		}				
		else if regexm("`RHSfs'","log_p_mean_modlag*") {
			replace fs_beta_modlag12 = _b[log_p_mean_modlag12] in `c'
			replace fs_se_modlag12 = _se[log_p_mean_modlag12] in `c'
			replace fs_t_modlag12 =  _b[log_p_mean_modlag12]/_se[log_p_mean_modlag12] in `c'
			replace fs_beta_modlag6 = _b[log_p_mean_modlag6] in `c'
			replace fs_se_modlag6 = _se[log_p_mean_modlag6] in `c'
			replace fs_t_modlag6 =  _b[log_p_mean_modlag6]/_se[log_p_mean_modlag6] in `c'
		}				
	}

	// Intermediate output
	di "******** Sensitivity `c' done *********"
	
	// Save output
	if `c'==1 {
		cap erase "$dirpath_data/results/regs_pge_elec_monthly_sens_july2020.dta"
	}
	preserve
	keep panel-fs_t_modlag6
	dropmiss, obs force
	cap append using "$dirpath_data/results/regs_pge_elec_monthly_sens_july2020.dta"
	duplicates drop
	compress
	save "$dirpath_data/results/regs_pge_elec_monthly_sens_july2020.dta", replace
	restore
		
}

}

************************************************
************************************************

