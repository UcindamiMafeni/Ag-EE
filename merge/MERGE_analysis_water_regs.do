clear all
version 13
set more off

**********************************************************************
**** Script to create analysis datasets monthly water regressions ****
**********************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

*******************************************************************************
*******************************************************************************

** 1. Merge together SP-month electricity panel and kwhaf panel
if 1==1{

** Load monthly dataset for electricity regressions
use "$dirpath_data/merged/sp_month_elec_panel.dta", clear

** Drop old versions of variables I want to overwrite here
cap drop flag_bad_drwdwn
cap drop flag_weird_pump
cap drop flag_weird_cust
cap drop months_until_test
cap drop months_since_test
cap drop months_to_nearest_test
cap drop latlon_group
cap drop latlon_miles_apart
cap drop apep_proj_count
cap drop merge_sp_water_panel

** Merge in SP-month panel of constructed KWHAF conversions
merge 1:1 sp_uuid modate using "$dirpath_data/merged/sp_month_kwhaf_panel.dta"
egen temp = mode(pull), by(sp_uuid)
replace pull = temp if _merge==2 & pull==""
tab pull _merge, missing
assert _merge==1 if inlist(pull,"20180322","20180827")
drop if inlist(pull,"20180322","20180827")
	
	// Some diagnostics
egen temp1 = min(_merge), by(sp_uuid)
egen temp2 = max(_merge), by(sp_uuid)
unique sp_uuid if temp1==1 & temp2==1 // 165 SPs are missing from the SP-APEP merge, not sure why?
unique sp_uuid if temp1==2 & temp2==2 // 502 SPs aren't in elec regs, probably non-ag rates
tab flag_weird_cust
tab flag_weird_cust if temp1==2 & temp2==2 // SPs not in elec regs more likely to be irrigation districts
tab flag_weird_cust if temp1==3 & temp2==3 
assert temp1==2 if temp1<temp2 & _merge==3 //
assert temp1==temp2 if _merge==1 // unmatched SPs are ALWAYS missing from APEP

	// Keep only observations that merge
keep if _merge==3
drop _merge pull temp*	

** Drop components of KWHAF that aren't necessary for regressions
drop drwdwn_apep tdh_adder ope af24hrs flow_gpm ddhat_* tdh_*

** Calcualte water quantities: AF_water = kwh_elec / kwh/AF
foreach v of varlist kwhaf* {
	local v2 = subinstr("`v'","kwhaf_","af_",1)
	gen `v2' = mnth_bill_kwh / `v'
	local vlab: variable label `v'
	local v2lab = subinstr("`vlab'","Predicted KWH/AF","AF water",1)
	la var `v2' "`v2lab'"
}
	
** Apply inverse hyperbolic sine tranformation to AF_water 
foreach v of varlist af* {
	local v2 = subinstr("`v'","af_","ihs_af_",1)
	gen `v2' = ln(10000*`v' + sqrt((10000*`v')^2+1))
	replace `v2' = . if mnth_bill_kwh<0
	local vlab: variable label `v'
	local v2lab = subinstr("`vlab'","AF water","IHS 1e4*AF water",1)
	la var `v2' "`v2lab'"
}

** Calcualte water prices: $/AF = $/kwh * kwh/AF
foreach v of varlist kwhaf* {
	local v2 = subinstr("`v'","kwhaf_","mean_p_af_",1)
	gen `v2' = mean_p_kwh * `v'
	replace `v2' = . if `v2'<0
	local vlab: variable label `v'
	local v2lab = subinstr("`vlab'","Predicted KWH/AF","Avg marg P_water $/AF",1)
	la var `v2' "`v2lab'"
}

** Log-transform water price composite variables
foreach v of varlist mean_p_af_* {
	gen ln_`v' = ln(`v') // I actually want the zeros to become missings
	local vlab: variable label `v'
	la var ln_`v' "Log `vlab'"
}

** Log-transform kwhaf variables
foreach v of varlist kwhaf* {
	gen ln_`v' = ln(`v') // I actually want the zeros to become missings
	local vlab: variable label `v'
	la var ln_`v' "Log `vlab'"
}

** Log-transform mean gw depth variables
foreach v of varlist gw_mean_* {
	gen ln_`v' = ln(`v') // I actually want the zeros to become missings
	local vlab: variable label `v'
	la var ln_`v' "Log `vlab'"
}

** Deal with extreme values prices (driven by crazy pump tests)
hist mean_p_kwh 
hist mean_p_af_rast_dd_qtr_1SP
hist mean_p_af_rast_dd_qtr_1SP if mean_p_af_rast_dd_qtr_1SP<150
hist mean_p_af_rast_ddhat_qtr_1SP if mean_p_af_rast_ddhat_qtr_1SP<150
hist kwhaf_apep_measured if nearest_test_modate==modate
sum kwhaf_apep_measured if nearest_test_modate==modate, detail
hist kwhaf_apep_measured if nearest_test_modate==modate & kwhaf_apep_measured<1500 
	// 1500 is above the 99% pctile and seems like a good cutoff in the distribution of kwhaf
unique sp_uuid 
local uniq = r(unique)
unique sp_uuid if kwhaf_apep_measured!=. & kwhaf_apep_measured>=1500
di r(unique)/`uniq' // only 0.6% of SPs
sum mean_p_af_rast_dd_qtr_1SP if kwhaf_apep_measured<1500, detail
sum mean_p_af_rast_dd_qtr_1SP if kwhaf_apep_measured>=1500, detail
	// this doesn't fix the outlier issue, but it's probably not a huge deal?
sum mean_p_af_rast_dd_qtr_1SP if kwhaf_apep_measured<1500 & flag_weird_pump==0 & flag_weird_cust==0, detail
sum mean_p_af_rast_dd_qtr_1SP if kwhaf_apep_measured>=1500 | flag_weird_pump==1 | flag_weird_cust==1, detail
hist ln_mean_p_af_rast_dd_qtr_1SP	

** Construct cross-sectionally stable versions of measured kWh/AF, for instrumenting
sum kwhaf_apep_measured, detail
sum kwhaf_apep_measured if year==2008, detail
egen temp1 = min(months_to_nearest_test), by(sp_uuid)
egen temp2 = min(modate) if months_to_nearest_test==temp1, by(sp_uuid)
egen temp3 = mean(kwhaf_apep_measured) if modate==temp2, by(sp_uuid)
egen double kwhaf_apep_measured_init = mean(temp3), by(sp_uuid)
assert kwhaf_apep_measured_init!=.
la var kwhaf_apep_measured_init "KWH/AF as measued by initial APEP tests (constant within SP)"
drop temp*

** Construct distance between SP and APEP coordinates
preserve 
use "$dirpath_data/pge_cleaned/apep_pump_gis.dta", clear
keep latlon_group pumplatnew pumplongnew
duplicates drop
unique latlon_group
assert r(unique)==r(N)
tempfile latlon_pump
save `latlon_pump'
restore
merge m:1 latlon_group using `latlon_pump', keep(1 3) nogen
geodist prem_lat prem_long pumplatnew pumplongnew, gen(latlon_miles_apart) miles
la var pumplatnew "APEP pump latitude"
la var pumplongnew "APEP pump longitude"
la var latlon_miles_apart "Miles b/tw matched SP lat/lon and APEP lat/lon"

** Lag depth instrument(s)
preserve
use "$dirpath_data/groundwater/groundwater_depth_sp_month_rast.dta", clear
keep basin_id gw_mth_bsn_mean2 modate
duplicates drop
unique basin_id modate
assert r(unique)==r(N)
egen temp = group(basin_id)
tsset temp modate
gen L6_gw_mth_bsn_mean2 = L6.gw_mth_bsn_mean2
gen L12_gw_mth_bsn_mean2 = L12.gw_mth_bsn_mean2
drop temp
tempfile basins
save `basins'
merge 1:m basin_id modate using "$dirpath_data/groundwater/groundwater_depth_sp_month_rast.dta" 
keep sp_uuid modate L6 L12
tempfile lagged_depth
save `lagged_depth'
restore
merge 1:1 sp_uuid modate using `lagged_depth', nogen keep(1 3)
rename L6_gw_mth_bsn_mean2 L6_gw_mean_depth_mth_2SP
rename L12_gw_mth_bsn_mean2 L12_gw_mean_depth_mth_2SP
gen L6_ln_gw_mean_depth_mth_2SP = ln(L6_gw_mean_depth_mth_2SP)
gen L12_ln_gw_mean_depth_mth_2SP = ln(L12_gw_mean_depth_mth_2SP)
la var L6_gw_mean_depth_mth_2SP "6-month lag of gw_mean_depth_mth_2SP"
la var L12_gw_mean_depth_mth_2SP "12-month lag of gw_mean_depth_mth_2SP"
la var L6_ln_gw_mean_depth_mth_2SP "6-month lag of ln_gw_mean_depth_mth_2SP"
la var L12_ln_gw_mean_depth_mth_2SP "12-month lag of ln_gw_mean_depth_mth_2SP"


** Compress, and save
unique sp_uuid modate
assert r(unique)==r(N)
sort sp_uuid modate
tsset sp_group modate
compress
save "$dirpath_data/merged/sp_month_water_panel.dta", replace

	
}

*******************************************************************************
*******************************************************************************

** 2. Merge a few things back into electricity panel 
if 1==1{
use "$dirpath_data/merged/sp_month_elec_panel.dta", clear
cap drop flag_bad_drwdwn
cap drop flag_weird_pump
cap drop flag_weird_cust
cap drop months_until_test
cap drop months_since_test
cap drop months_to_nearest_test
cap drop latlon_group
cap drop latlon_miles_apart
cap drop apep_proj_count
cap drop merge_sp_water_panel
merge 1:1 sp_uuid modate using "$dirpath_data/merged/sp_month_water_panel.dta", ///
	keepusing(flag_bad_drwdwn flag_weird_pump flag_weird_cust ///
	months_until_test months_since_test months_to_nearest_test ///
	latlon_group latlon_miles_apart apep_proj_count) ///
	keep(1 3) gen(merge_sp_water_panel)
la var merge_sp_water_panel "3 = merges into corresponding SP-month panel for water regressions"	
compress
save "$dirpath_data/merged/sp_month_elec_panel.dta", replace

}

*******************************************************************************
*******************************************************************************

