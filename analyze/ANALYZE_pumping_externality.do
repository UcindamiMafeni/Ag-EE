clear all
version 13
set more off

*******************************************************************
**** Script to export data to calculate open access externality ***
*******************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"
global dirpath_code "T:/Home/Louis/backup/AgEE/AgEE_code/build"
*global R_exe_path "C:/PROGRA~1/MIE74D~1/ROPEN~1/bin/x64/R"
*global R_lib_path "C:/Program Files/Microsoft/R Open/R-3.4.4/library"

*******************************************************************************
*******************************************************************************

** 1. Prep panel dataset to send to R to do GIS
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
	keep(1 3) keepusing(drwdwn_apep ddhat_* af24hrs ope flow_gpm tdh_adder tdh_*)

** Merge in groundwater basin identifiers
merge m:1 sp_uuid using "$dirpath_data/pge_cleaned/sp_premise_gis.dta", nogen keep(1 3) ///
	keepusing(basin_object_id basin_id basin_name basin_dist_miles)
	
** Assign groundwater demand elasticities
gen elast_water = -1.12163 // from Column (2) of Table 6	
	
** Keep 2016 observations only
keep if year==2016	

** Drop raster-derived variables I'm not using
drop *rast_*_3* *mean_*_3* *cnt3* *qtr_1* mean_p_af_mean_* af_mean* kwhaf_mean_* ///
	ddhat_mean_* tdh_mean_*
	
** Re-jigger tdh_adder and OPE so that they exactly map to averaged KWHAF
	// The KWHAF calculation is close to exact within a pump, but we've done
	// averaging both via interperolation over time, and also collapsing multiple
	// pumps into a single SP-month observation. So, because there's a denominator
	// to the KWHAF conversion (i.e. OPE), we need to do some within-observation 
	// averaging in order be able to reconstruct counterfactual KWHAF in a way that's
	// consistent!
gen temp_test1 = tdh_rast_dd_mth_2SP * flow_gpm / ope / 39.6 * 0.7457 * 24 / af24hrs	
gen temp_test2 = tdh_rast_dd_mth_2SP / ope * 102.26728 
gen temp_diff = temp_test1 - temp_test2	
sum temp_diff, detail // double checking my unit conversion math, which checks out
gen temp_pct_diff = temp_diff/temp_test1
sum temp_pct_diff, detail
drop temp*

foreach v of varlist kwhaf_rast* {
	local v_tdh = subinstr("`v'","kwhaf_","tdh_",1)
	local v_ope = subinstr("`v'","kwhaf_","ope_",1)
	gen `v_ope' = `v_tdh' * 102.26728 / `v'
	la var `v_ope' "Reconstructed OPE for counterfactuals, to deal w/ avging ratios"
}	
	
** Export to csv
outsheet using "$dirpath_data/misc/panel_for_externality_calcs.csv", comma replace
	
}

*******************************************************************************
*******************************************************************************

** 2. Run "ANALYZE_externality_calcs.R" in R

*******************************************************************************
*******************************************************************************

** 3. Import results from R
if 1==1{

insheet using "$dirpath_data/misc/externality_calcs_june2016_rast_dd_mth_2SP.csv", clear
foreach v of varlist * {
	cap replace `v' = "" if `v'=="NA"
	destring `v', replace
}
sum dcs_i, detail
assert dcs_i<=0 | dcs_i==.

assert sum_dcs_j_pos1>=0 | sum_dcs_j_pos1==. 
assert sum_dcs_j_pos2>=0 | sum_dcs_j_pos2==. 
assert sum_dcs_j_pos5>=0 | sum_dcs_j_pos5==. 
assert sum_dcs_j_pos10>=0 | sum_dcs_j_pos10==. 
assert sum_dcs_j_pos20>=0 | sum_dcs_j_pos20==. 
assert sum_dcs_j_pos30>=0 | sum_dcs_j_pos30==. 
assert sum_dcs_j_pos40>=0 | sum_dcs_j_pos40==. 

foreach r in 1 2 5 10 20 30 40 {
	gen dW_`r' = dcs_i + sum_dcs_j_pos`r'
}

sum sum_dcs_j_pos1, detail
sum sum_dcs_j_pos2, detail
sum sum_dcs_j_pos5, detail
sum sum_dcs_j_pos10, detail
sum sum_dcs_j_pos20, detail
sum sum_dcs_j_pos30, detail
sum sum_dcs_j_pos40, detail

sum dW_1, detail
sum dW_2, detail
sum dW_5, detail
sum dW_10, detail
sum dW_20, detail
sum dW_30, detail
sum dW_40, detail

sum dW_40, detail
sum dW_40 if basin_name=="SAN JOAQUIN VALLEY", detail
sum dW_40 if basin_name=="SACRAMENTO VALLEY", detail 
sum dW_40 if basin_name=="SALINAS VALLEY", detail

tab basin_name basin_group if inlist(basin_name,"SAN JOAQUIN VALLEY","SACRAMENTO VALLEY","SALINAS VALLEY") 

preserve
use "$dirpath_data/merged/sp_month_elec_panel.dta", clear
drop if flag_geocode_badmiss==1 
drop if flag_weird_cust==1 | flag_weird_pump==1 
keep if inlist(basin_group,68,121,122)
keep if modate==ym(2016,6)
tab merge_sp_water_panel
tab merge_sp_water_panel if basin_group==68
tab merge_sp_water_panel if basin_group==121
tab merge_sp_water_panel if basin_group==122
tab merge_sp_water_panel if basin_group==68 & mnth_bill_kwh>0
tab merge_sp_water_panel if basin_group==121 & mnth_bill_kwh>0
tab merge_sp_water_panel if basin_group==122 & mnth_bill_kwh>0
tabstat mnth_bill_kwh if merge_sp_water_panel==3, by(rt_sched_cd) s(mean sum)
tabstat mnth_bill_kwh if merge_sp_water_panel==1, by(rt_sched_cd) s(mean sum)

tabstat mnth_bill_kwh if merge_sp_water_panel==3 & basin_group==68, by(rt_sched_cd) s(mean count sum)
tabstat mnth_bill_kwh if merge_sp_water_panel==1 & basin_group==68, by(rt_sched_cd) s(mean count sum)
tabstat mnth_bill_kwh if basin_group==68 & inlist(rt_sched_cd,"AG-4B","AG-5B","AG-5C"), by(merge_sp_water_panel) s(mean count sum)

tabstat mnth_bill_kwh if merge_sp_water_panel==3 & basin_group==121, by(rt_sched_cd) s(mean count sum)
tabstat mnth_bill_kwh if merge_sp_water_panel==1 & basin_group==121, by(rt_sched_cd) s(mean count sum)
tabstat mnth_bill_kwh if basin_group==121 & inlist(rt_sched_cd,"AG-4B","AG-4C","AG-5B"), by(merge_sp_water_panel) s(mean count sum)

tabstat mnth_bill_kwh if merge_sp_water_panel==3 & basin_group==122, by(rt_sched_cd) s(mean count sum)
tabstat mnth_bill_kwh if merge_sp_water_panel==1 & basin_group==122, by(rt_sched_cd) s(mean count sum)
tabstat mnth_bill_kwh if basin_group==122 & inlist(rt_sched_cd,"AG-4B","AG-5B","AG-5C"), by(merge_sp_water_panel) s(mean count sum)

// based on pure counts
di 100/27 // scale up 68  (Salinas) by  3.7
di 100/5  // scale up 121 (Sacramento) by 20
di 100/14 // scale up 122 (San Joaquin) by 7.1

// based on rates
di 2105/934 // scale up 68  (Salinas) by  2.3
di 5730/529  // scale up 121 (Sacramento) by 10.8
di 20950/4925 // scale up 122 (San Joaquin) by 4.3

restore

gen scale_up_counts = .
replace scale_up_counts = 3.7 if basin_group==68
replace scale_up_counts = 20 if basin_group==121
replace scale_up_counts = 7.1 if basin_group==122

gen scale_up_rates = .
replace scale_up_rates = 2.3 if basin_group==68
replace scale_up_rates = 10.8 if basin_group==121
replace scale_up_rates = 4.3 if basin_group==122

foreach r in 1 2 5 10 20 30 40 {
	gen n_j_pos`r'_upc = n_j_pos`r'*scale_up_counts
	gen n_j_pos`r'_upr = n_j_pos`r'*scale_up_rates
	gen dW_`r'_upc = dcs_i + sum_dcs_j_pos`r'*scale_up_counts
	gen dW_`r'_upr = dcs_i + sum_dcs_j_pos`r'*scale_up_rates
}

sum dW_20 if basin_group==68, detail
sum dW_20_upr if basin_group==68, detail
sum dW_20_upc if basin_group==68, detail

sum dW_20 if basin_group==121, detail
sum dW_20_upr if basin_group==121, detail
sum dW_20_upc if basin_group==121, detail

sum dW_20 if basin_group==122, detail
sum dW_20_upr if basin_group==122, detail
sum dW_20_upc if basin_group==122, detail

compress
save "$dirpath_data/results/externality_calcs_june2016_rast_dd_mth_2SP.dta", replace
}

*******************************************************************************
*******************************************************************************

