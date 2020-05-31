clear all
version 13
set more off

**********************************************************************
**** Script to create analysis datasets monthly water regressions ****
**********************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

*******************************************************************************
*******************************************************************************

** 1. Merge together SP-month electricity panel and kwhaf panel
if 1==1{

** Load monthly dataset for electricity regressions
use "$dirpath_data/merged/sp_month_elec_panel.dta", clear

** Drop versions of variables I want to overwrite, and then merge back into electricity panel (below)
cap drop flag_bad_drwdwn
cap drop flag_weird_pump
cap drop flag_weird_cust
cap drop months_until_test
cap drop months_since_test
cap drop months_to_nearest_test
cap drop latlon_group
cap drop latlon_miles_apart
cap drop flag_parcel_match 
cap drop flag_clu_match 
cap drop flag_clu_group*_match 
cap drop spcount_*
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
drop drwdwn_apep tdh_adder af24hrs flow_gpm ddhat_* tdh_*

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

** Apply log, log+1 tranformations to AF_water 
foreach v of varlist af_rast_dd_mth_2SP {
	local vlab: variable label `v'

	local v2 = subinstr("`v'","af_","log1_10000af_",1)
	gen `v2' = ln(10000*`v'+1)
	replace `v2' = . if `v'<0
	local v2lab = subinstr("`vlab'","AF water","Log+1 10000*AF water",1)
	la var `v2' "`v2lab'"

	local v2 = subinstr("`v'","af_","log1_100af_",1)
	gen `v2' = ln(100*`v'+1)
	replace `v2' = . if `v'<0
	local v2lab = subinstr("`vlab'","AF water","Log+1 100*AF water",1)
	la var `v2' "`v2lab'"
	
	local v2 = subinstr("`v'","af_","log1_af_",1)
	gen `v2' = ln(`v'+1)
	replace `v2' = . if `v'<0
	local v2lab = subinstr("`vlab'","AF water","Log+1 AF water",1)
	la var `v2' "`v2lab'"

	local v2 = subinstr("`v'","af_","log_af_",1)
	gen `v2' = ln(`v')
	local v2lab = subinstr("`vlab'","AF water","Log AF water",1)
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

** Construct distance between SP and APEP coordinates, and merge in APEP CLU assignments
preserve 
use "$dirpath_data/pge_cleaned/apep_pump_gis.dta", clear
keep latlon_group pumplatnew pumplongnew parcelid_conc clu_id_ec clu_group*_ec
duplicates drop
unique latlon_group
assert r(unique)==r(N)
foreach v of varlist parcelid_conc clu* {
	rename `v' `v'APEP // rename APEP-assigned gis variables to make distinct from SP-assigned versions
}
tempfile latlon_pump
save `latlon_pump'
restore
merge m:1 latlon_group using `latlon_pump', keep(1 3) nogen
geodist prem_lat prem_long pumplatnew pumplongnew, gen(latlon_miles_apart) miles
la var pumplatnew "APEP pump latitude"
la var pumplongnew "APEP pump longitude"
la var latlon_miles_apart "Miles b/tw matched SP lat/lon and APEP lat/lon"

** Encode parcel/CLU assignemnts
foreach v of varlist parcelid_concAPEP {
	rename `v' temp
	encode temp, gen(`v') label(parcelid_conc)
	drop temp
}
foreach v of varlist clu_id_ecAPEP {
	rename `v' temp
	encode temp, gen(`v') label(clu_id_ec)
	drop temp
}
foreach v of varlist clu_group*_ecAPEP {
	rename `v' temp
	encode temp, gen(`v') label(clu_group0_ec)
	drop temp
}

**Compare parcel/CLU assignments
gen flag_parcel_match = parcelid_conc==parcelid_concAPEP
gen flag_clu_match = clu_id_ec==clu_id_ecAPEP
foreach s in 0 10 25 50 75 {
	gen flag_clu_group`s'_match = clu_group`s'_ec==clu_group`s'_ecAPEP
}
egen temp_tag = tag(sp_uuid)
sum flag*match if temp_tag // at best 30% matches here
sum latlon_miles_apart if temp_tag, detail 
_pctile latlon_miles_apart if temp_tag, p(30)
return list // for comparison, 30th pctile distance is 0.15 miles apart
drop temp_tag
la var flag_parcel_match "Flag=1 if SP and APEP lat/lons assigend to same parcel"
la var flag_clu_match "Flag=1 if SP and APEP lat/lons assigend to same CLU"
foreach s in 0 10 25 50 75 {
	la var flag_clu_group`s'_match "Flag=1 if SP and APEP lat/lons assigend to same CLU group`s'"
}

** Number of pump-matched SPs per CLU[group]
preserve
keep sp_uuid modate parcelid_conc* clu_id_ec* clu_group*_ec*
duplicates drop sp_uuid, force
unique sp_uuid
assert r(unique)==r(N)
foreach v of varlist parcelid_conc* clu_id_ec* clu_group*_ec* {
	egen spcount_`v' = count(modate), by(`v')
	replace spcount_`v' = . if `v'==.
	la var spcount_`v' "Count of unique SPs by `v'"
}
keep sp_uuid spcount_*
tempfile spcounts
save `spcounts'
restore
merge m:1 sp_uuid using `spcounts', nogen

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

** Create horsepower bins
gen hp_bin_large = 0
replace hp_bin_large = 1 if hp_nameplate >= 35
xtile hp_bin_quart = hp_nameplate if hp_nameplate >= 35, nq(4)
replace hp_bin_quart = 0 if hp_nameplate < 35
xtile hp_bin_dec = hp_nameplate if hp_nameplate >= 35, nq(10)
replace hp_bin_dec = 0 if hp_nameplate < 35
drop hp hp_nameplate
la var hp_bin_large "Two bins for nameplate horsepower; cutoff at 35 hp"
la var hp_bin_quart "Five bins for nameplate horsepower; one below 35 hp, quartiles above"
la var hp_bin_dec "Eleven bins for nameplate horsepower; one below 35 hp, deciles above"

** Create kilowatt bins
gen kw_bin_large = 0
replace kw_bin_large = 1 if kw_input >= 26.11
xtile kw_bin_quart = kw_input if kw_input >= 26.11, nq(4)
replace kw_bin_quart = 0 if kw_input < 26.11
xtile kw_bin_dec = kw_input if kw_input >= 26.11, nq(10)
replace kw_bin_dec = 0 if kw_input < 26.11
drop kw_input
la var kw_bin_large "Two bins for kw power usage; cutoff at 26.11 kw"
la var kw_bin_quart "Five bins for kw power usage; one below 26.11 kw, quartiles above"
la var kw_bin_dec "Eleven bins for kw power usage; one below 26.11 kw, deciles above"

** Create OPE bins
xtile ope_bin_quart = ope, nq(4)
xtile ope_bin_dec = ope, nq(10)
drop ope
la var ope_bin_quart "Quartiles of OPE"
la var ope_bin_dec "Deciles of OPE"

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

	// Load electricity panel
use "$dirpath_data/merged/sp_month_elec_panel.dta", clear

	// Drop variables I may have regenerated and want to re-merge in
cap drop flag_bad_drwdwn
cap drop flag_weird_pump
cap drop flag_weird_cust
cap drop months_until_test
cap drop months_since_test
cap drop months_to_nearest_test
cap drop latlon_group
cap drop latlon_miles_apart
cap drop flag_parcel_match 
cap drop flag_clu_match 
cap drop flag_clu_group*_match 
cap drop spcount_*
cap drop apep_proj_count
cap drop merge_sp_water_panel

	// Merge them in
merge 1:1 sp_uuid modate using "$dirpath_data/merged/sp_month_water_panel.dta", ///
	keepusing(flag_bad_drwdwn flag_weird_pump flag_weird_cust ///
	months_until_test months_since_test months_to_nearest_test ///
	latlon_group latlon_miles_apart flag_parcel_match flag_clu_match ///
	flag_clu_group*_match spcount_* apep_proj_count hp* kw_* ope*) ///
	keep(1 3) gen(merge_sp_water_panel)
la var merge_sp_water_panel "3 = merges into corresponding SP-month panel for water regressions"	
cap drop spcount_*APEP

	// Save
sort sp_uuid modate
unique sp_uuid modate
assert r(unique)==r(N)
compress
save "$dirpath_data/merged/sp_month_elec_panel.dta", replace

}

*******************************************************************************
*******************************************************************************

