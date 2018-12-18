clear all
version 13
set more off

*******************************************************************
**** Script to export data to calculate open access externality ***
*******************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"
global dirpath_code "S:/Louis/backup/AgEE/AgEE_code/build"
*global R_exe_path "C:/PROGRA~1/MIE74D~1/ROPEN~1/bin/x64/R"
*global R_lib_path "C:/Program Files/Microsoft/R Open/R-3.4.4/library"

*******************************************************************************
*******************************************************************************

** 1. Prep panel dataset to merge in to 
if 1==1{

** Load water panel dataset 
use "$dirpath_data/merged/sp_month_water_panel.dta", clear

** Flag if in main estimation sample
gen in_regs = flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & ///
	flag_weird_cust==0 & flag_weird_pump==0
	
** Drop if bad/missing geocode
drop if flag_geocode_badmiss==1 
	// Everything here hinges on lat/lon, and we can't use these farms
	
** Drop if weird pump or weird customer 
drop if flag_weird_cust==1 | flag_weird_pump==1 
	// these aren't just observations we don't want in our elasticity estimates.
	// we don't trust their APEP variables, so they should be in the pool of 
	// farms where we calculate the externality either (since that relies on pump specs)
	
** Keep essential variables only
keep sp_uuid modate month year prem_lat prem_long in_regs mnth_bill_kwh rt_sched_cd ///
	mean_p_kwh log_p_mean ihs_kwh basin_group sp_same_rate* gw_qtr_bsn_mean2 sp_group ///
	summer apep_proj_count months_to_nearest_test nearest_test_modate latlon_group ///
	flag_bad_drwdwn drwdwn_predict* kwhaf_* gw_* af_* mean_p_af_*

** Merge back in variables needed to convert from SWL to KWHAF
merge 1:1 sp_uuid modate using "$dirpath_data/merged/sp_month_kwhaf_panel.dta", nogen ///
	keep(1 3) keepusing(drwdwn_apep ddhat_* af24hrs ope flow_gpm tdh_adder)

** Merge in groundwater basin identifiers
merge m:1 sp_uuid using "$dirpath_data/pge_cleaned/sp_premise_gis.dta", nogen keep(1 3) ///
	keepusing(basin_object_id basin_id basin_name basin_dist_miles)
	
** Assign groundwater demand elasticities
gen elast_water = -1.12163 // from Column (2) of Table 6	
	
** Keep 2016 observations only
keep if year==2016	

** Drop raster-derived variables I'm not using
drop *rast_*_3* *mean_*_3* *cnt3* *qtr_1* mean_p_af_mean_* af_mean* kwhaf_mean_* ddhat_mean_*
	
** Export to csv
outsheet using "$dirpath_data/misc/panel_for_externality_calcs.csv", comma replace
	
}
	

*******************************************************************************
*******************************************************************************
