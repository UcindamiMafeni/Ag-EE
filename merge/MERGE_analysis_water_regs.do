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

** 1. Merge together SP-month electric panel and kwhaf panel
if 1==1{

** Load monthly dataset for electricity regressions
use "$dirpath_data/merged/sp_month_elec_panel.dta", clear

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
drop drwdwn_apep tdh_adder ope af24hrs flow_gpm ddhat_* 

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
	gen ln_`v' = ln(`v') // i actually want the zeros to become missings
	local vlab: variable label `v'
	la var ln_`v' "Log `vlab'"
}

** Log-transform kwhaf variables
foreach v of varlist kwhaf* {
	gen ln_`v' = ln(`v') // i actually want the zeros to become missings
	local vlab: variable label `v'
	la var ln_`v' "Log `vlab'"
}

** Flag extreme values prices (driven by crazy pump tests)
hist mean_p_kwh if mean_p_kwh>0
hist mean_p_af_rast_dd_qtr_1SP if mean_p_af_rast_dd_qtr_1SP<100
hist mean_p_af_rast_ddhat_qtr_1SP if mean_p_af_rast_ddhat_qtr_1SP<100
hist kwhaf_apep_measured if nearest_test_modate==modate
sum kwhaf_apep_measured if nearest_test_modate==modate, detail
hist kwhaf_apep_measured if nearest_test_modate==modate & kwhaf_apep_measured<1500 
	// 1500 is above the 99% pctile and seems like a good cutoff in the distribution of kwhaf
unique sp_uuid 
local uniq = r(unique)
unique sp_uuid if kwhaf_apep_measured!=. & kwhaf_apep_measured>=1500
di r(unique)/`uniq' // only 0.6% of SPs
hist mean_p_kwh if mean_p_kwh>0 & kwhaf_apep_measured<1500 


}

*******************************************************************************
*******************************************************************************


