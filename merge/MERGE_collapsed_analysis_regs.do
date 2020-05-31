clear all
version 13
set more off

*****************************************************************************
**** Script to collapse datasets for annual, CLU, and parcel regressions ****
*****************************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

*****************************************************************************
*****************************************************************************

** 1. SP-by-year electricity regression dataset
if 1==0{

** Start with monthly electricity dataset
use "$dirpath_data/merged/sp_month_elec_panel.dta", clear

** Collapse to SP-year level
duplicates report sp_group year
gen winter = 1-summer
gen days = .
replace days = 31 if inlist(month,1,3,5,7,8,10,12)
replace days = 30 if inlist(month,4,6,9,11)
replace days = 29 if month==2 & inlist(year,2008,2012,2016)
replace days = 28 if month==2 & !inlist(year,2008,2012,2016)

	// Sum kwh and bill amounts for year and for summer/winter
foreach v of varlist mnth* {
	foreach s of varlist summer winter {
		local v2 = subinstr("`v'","mnth","`s'",1)
		egen `v2' = sum(`v'*`s'), by(sp_group year)
		local vlab1: variable label `v'
		local vlab2 = subinstr("`vlab1'","monthified","`s'",1)
		la var `v2' "`vlab2'"
	}
	local v2 = subinstr("`v'","mnth","ann",1)
	egen `v2' = sum(`v'), by(sp_group year)
	local vlab1: variable label `v'
	local vlab2 = subinstr("`vlab1'","monthified","annual",1)
	la var `v2' "`vlab2'"
	drop `v'
}

	// Take mean of electricity prices for year and for summer/winter
foreach v of varlist mean_p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(sp_group year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(sp_group year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take min of electricity prices for year and for summer/winter
foreach v of varlist min_p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = min(temp), by(sp_group year)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = min(`v'), by(sp_group year)
	replace `v' = temp
	drop temp
}

	// Take max of electricity prices for year and for summer/winter
foreach v of varlist max_p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = max(temp), by(sp_group year)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = max(`v'), by(sp_group year)
	replace `v' = temp
	drop temp
}

	// Take mean of tiered electricity prices for year and for summer/winter
foreach v of varlist p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "mean_`v'_`s'"
		egen `v2' = wtmean(temp), by(sp_group year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "Avg daily `vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	local v2 = "mean_`v'"
	egen `v2' = wtmean(`v'), by(sp_group year) weight(days)
	local vlab1: variable label `v'
	local vlab2 = "Avg daily `vlab1'"
	la var `v2' "`vlab2'"
	drop `v'
}

	// Take mean of demand charges for year and for summer/winter
foreach v of varlist mean_p_kw_* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(sp_group year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(sp_group year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take max of flag variables
foreach v of varlist flag_* {
	egen temp = max(`v'), by(sp_group year)
	replace `v' = temp
	drop temp
}

	// Create flags for important switches within a year
foreach v of varlist rt_sched_cd rt_category rt_large_ag hp_bin_dec kw_bin_dec ope_bin_dec {
	egen temp_tag = tag(`v' sp_group year), missing
	egen temp = sum(temp_tag), by(sp_group year)
	local v2 = "flag_`v'_switch"
	gen `v2' = temp>1
	drop temp*
}
la var flag_rt_sched_cd_switch "Flag indicating SP switched rate within year"
la var flag_rt_category_switch "Flag indicating SP switched rate category within year"
la var flag_rt_large_ag_switch "Flag indicating SP switched rate group within year"
la var flag_hp_bin_dec_switch "Flag indicating SP switched horsepower decile within year"
la var flag_kw_bin_dec_switch "Flag indicating SP switched killowatt decile within year"
la var flag_ope_bin_dec_switch "Flag indicating SP switched OPE decile within year"

	// Create a flag for SP-years with < 12 months
egen temp = count(modate), by(sp_group year)
gen flag_partial_year = (temp<12)
drop temp
la var flag_partial_year "Flag for SP-years with <12 months of bills"
replace flag_irregular_bill = max(flag_irregular_bill,flag_partial_year)

	// Take mode of string rate variables
foreach v of varlist rt_sched_cd rt_default rt_modal {
	egen temp = mode(`v'), by(sp_group year)
	replace `v' = temp
	local vlab1: variable label `v'
	local vlab2 = "`vlab1', mode of months in year"
	la var `v' "`vlab2'"
	drop temp
}

	// Take mode of numeric rate and bin variables
foreach v of varlist rt_group rt_category rt_large_ag hp* kw* ope* {
	egen temp = mode(`v'), maxmode by(sp_group year)
	replace `v' = temp
	drop temp
	local vlab1: variable label `v'
	local vlab2 = "`vlab1', mode of months in year"
	la var `v' "`vlab2'"
}

	// Take max of other indicator variables
foreach v of varlist in_interval net_mtr_ind dr_ind elec_binary {
	egen temp = max(`v'), by(sp_group year)
	replace `v' = temp
	drop temp
}

	// Take mean of temperatures for year and for summer/winter
foreach v of varlist degreesC* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(sp_group year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(sp_group year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take mean of groundwater variables for year and for summer/winter
foreach v of varlist gw* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(sp_group year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(sp_group year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take mean of other numeric variables
foreach v of varlist interval_bill_corr {
	egen temp = wtmean(`v'), by(sp_group year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take mode of location variables
foreach v of varlist prem* latlon* {
	egen temp = mode(`v'), maxmode by(sp_group year)
	replace `v' = temp
	drop temp
}

	// Take minimum of date variables
foreach v of varlist sa_sp* *dt_first {
	egen temp = min(`v'), by(sp_group year)
	replace `v' = temp
	drop temp
}

	// Take maximum of date variables
foreach v of varlist *dt_last {
	egen temp = max(`v'), by(sp_group year)
	replace `v' = temp
	drop temp
}

	// Annualize monthly time differences
foreach v of varlist months* {
	gen temp = ceil(`v'/12)
	local v2 = subinstr("`v'","months","years",1)
	egen `v2' = min(temp), by(sp_group year)
	local vlab1: variable label `v'
	local vlab2 = subinstr("`vlab1'","Months","Years",1)
	la var `v2' "`vlab2'"
	drop `v' temp
}

	// Take max of EE measures
egen temp = max(ee_measure_count), by(sp_group)
replace ee_measure_count = temp
drop temp

	// Create indicators and take mode of data pull
levelsof pull, local(pulls)
foreach p of local pulls {
	local v = "pull_`p'"
	gen temp = (pull=="`p'")
	egen `v' = max(temp), by(sp_group year)
	la var `v' "SP-year contains data from `p' pull"
	drop temp
}	
egen temp = mode(pull), by(sp_group year)
replace pull = temp
drop temp
la var pull "Modal data pull for SP-year"

	// Drop remaining monthly variables
drop modate month summer winter days ctrl_fxn* log* ihs*

	// Collapse
duplicates drop
unique sp_group year
assert r(unique)==r(N)

** Inverse hyperbolic sine and log transform electricity quantity
foreach v of varlist *bill_kwh {
	if strpos("`v'","summer")>0 {
		local vpost = "_summer"
	}
	else if strpos("`v'","winter")>0 {
		local vpost = "_winter"
	}
	else {
		local vpost = ""
	}
	local v_ihs = "ihs_kwh`vpost'"
	gen `v_ihs' = ln(100*`v' + sqrt((100*`v')^2+1))
	replace `v_ihs' = . if `v'<0
	local v_log = "log_kwh`vpost'"
	gen `v_log' = ln(`v')
	local v_log1 = "log1_kwh`vpost'"
	gen `v_log1' = ln(`v'+1)
	replace `v_log1' = . if `v'<0
	local v_log1_100 = "log1_100kwh`vpost'"
	gen `v_log1_100' = ln(100*`v'+1)
	replace `v_log1_100' = . if `v'<0
	local vlab_mid = subinstr("`vpost'","_"," ",1)
	local vlab_ihs = "Inverse hyperbolic sine of 100*kWh`vlab_mid' elec consumption"
	local vlab_log = "Log of kWh`vlab_mid' elec consumption"
	local vlab_log1 = "Log+1 of kWh`vlab_mid' elec consumption"
	local vlab_log1_100 = "Log+1 of 100*kWh`vlab_mid' elec consumption"
	la var `v_ihs' "`vlab_ihs'"
	la var `v_log' "`vlab_log'"
	la var `v_log1' "`vlab_log1'"
	la var `v_log1_100' "`vlab_log1_100'"
}

** Log-transform marginal electricity prices
foreach v of varlist mean_p_kwh min_p_kwh max_p_kwh {
	foreach vsuf in "" "_summer" "_winter" {
		local v2 = "`v'`vsuf'"
		local vpre = substr("`v2'",1,strpos("`v2'","_")-1)
		local v3 = subinstr(subinstr("`v2'","`vpre'","log",1),"kwh","`vpre'",1)
		gen `v3' = ln(`v2')
		local lab: var label `v2'
		la var `v3' "Log `lab'"
	}
}

** Log-transform prices to be used as instruments
foreach v of varlist *p_kwh_e1* *p_kwh_e20* *p_kwh_ag_default* *p_kwh_ag_modal* *p_kwh_init* {
	gen log_`v' = ln(`v')
	local lab: var label `v'
	la var log_`v' "Log `lab'"
}	

** Construct instruments of lagged electricity prices
tsset sp_group year
foreach v of varlist log_p* {
	local vpre = subinstr(subinstr(substr("`v'",7,.),"_summer","",1),"_winter","",1)
	local v2 = subinstr("`v'","`vpre'","`vpre'_lag",1)
	gen `v2' = L.`v'
	local labpre = proper(subinstr("`vpre'","mean","avg",1))
	if strpos("`v'","summer")>0 {
		local labpost = " summer"
	}
	else if strpos("`v'","winter")>0 {
		local labpost = " winter"
	}
	else {
		local labpost = ""
	}
	la var `v2' "`labpre' SP-spec marg elec price (log $/kWh), lagged`labpost'"
}

** Construct instruments of lagged default prices
foreach v of varlist log*default* {
	local vpos1 = strpos("`v'","_")+1
	local vpos2 = strpos("`v'","p_kwh")
	local vpre = substr("`v'",`vpos1',`vpos2'-`vpos1'-1)
	local v2 = subinstr(subinstr(subinstr("`v'","_`vpre'","",1),"kwh","`vpre'",1),"ag_default","deflag",1)
	gen `v2' = L.`v'
	local labpre = proper(subinstr("`vpre'","mean","avg",1))
	if strpos("`v'","summer")>0 {
		local labpost = " summer"
	}
	else if strpos("`v'","winter")>0 {
		local labpost = " winter"
	}
	else {
		local labpost = ""
	}
	la var `v2' "`labpre' SP-spec default marg elec price (log $/kWh), lagged`labpost'"
}

** Construct instruments of lagged modal prices
foreach v of varlist log*modal* {
	local vpos1 = strpos("`v'","_")+1
	local vpos2 = strpos("`v'","p_kwh")
	local vpre = substr("`v'",`vpos1',`vpos2'-`vpos1'-1)
	local v2 = subinstr(subinstr(subinstr("`v'","_`vpre'","",1),"kwh","`vpre'",1),"ag_modal","modlag",1)
	gen `v2' = L.`v'
	local labpre = proper(subinstr("`vpre'","mean","avg",1))
	if strpos("`v'","summer")>0 {
		local labpost = " summer"
	}
	else if strpos("`v'","winter")>0 {
		local labpost = " winter"
	}
	else {
		local labpost = ""
	}
	la var `v2' "`labpre' SP-spec modal marg elec price (log $/kWh), lagged`labpost'"
}

** Save
order sp_uuid year
sort sp_uuid year
unique sp_uuid year
assert r(unique)==r(N)
compress
save "$dirpath_data/merged/sp_annual_elec_panel.dta", replace

}

*****************************************************************************
*****************************************************************************

** 2. CLU-by-year electricity regression dataset
if 1==0{

** Start with monthly electricity dataset
use "$dirpath_data/merged/sp_month_elec_panel.dta", clear

** Keep only SP-months that merge to water data and have a CLU ID
keep if merge_sp_water_panel==3
keep if clu_id!=.

** Collapse to CLU-year level
duplicates report clu_id year
gen winter = 1-summer
gen days = .
replace days = 31 if inlist(month,1,3,5,7,8,10,12)
replace days = 30 if inlist(month,4,6,9,11)
replace days = 29 if month==2 & inlist(year,2008,2012,2016)
replace days = 28 if month==2 & !inlist(year,2008,2012,2016)

	// Sum kwh and bill amounts for year and for summer/winter
foreach v of varlist mnth* {
	foreach s of varlist summer winter {
		local v2 = subinstr("`v'","mnth","`s'",1)
		egen `v2' = sum(`v'*`s'), by(clu_id year)
		local vlab1: variable label `v'
		local vlab2 = subinstr("`vlab1'","monthified","`s'",1)
		la var `v2' "`vlab2'"
	}
	local v2 = subinstr("`v'","mnth","ann",1)
	egen `v2' = sum(`v'), by(clu_id year)
	local vlab1: variable label `v'
	local vlab2 = subinstr("`vlab1'","monthified","annual",1)
	la var `v2' "`vlab2'"
	drop `v'
}

	// Take mean of electricity prices for year and for summer/winter
foreach v of varlist mean_p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(clu_id year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(clu_id year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take min of electricity prices for year and for summer/winter
foreach v of varlist min_p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = min(temp), by(clu_id year)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = min(`v'), by(clu_id year)
	replace `v' = temp
	drop temp
}

	// Take max of electricity prices for year and for summer/winter
foreach v of varlist max_p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = max(temp), by(clu_id year)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = max(`v'), by(clu_id year)
	replace `v' = temp
	drop temp
}

	// Take mean of tiered electricity prices for year and for summer/winter
foreach v of varlist p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "mean_`v'_`s'"
		egen `v2' = wtmean(temp), by(clu_id year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "Avg daily `vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	local v2 = "mean_`v'"
	egen `v2' = wtmean(`v'), by(clu_id year) weight(days)
	local vlab1: variable label `v'
	local vlab2 = "Avg daily `vlab1'"
	la var `v2' "`vlab2'"
	drop `v'
}

	// Take mean of demand charges for year and for summer/winter
foreach v of varlist mean_p_kw_* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(clu_id year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(clu_id year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take max of flag variables
foreach v of varlist *flag* {
	egen temp = max(`v'), by(clu_id year)
	replace `v' = temp
	drop temp
}

	// Create flags for important switches within a year
foreach v of varlist rt_sched_cd rt_category rt_large_ag hp_bin_dec kw_bin_dec ope_bin_dec {
	egen temp_tag = tag(`v' sp_group year), missing
	egen temp_sp = sum(temp_tag), by(sp_group year)
	gen temp_flag = (temp_sp>1)
	local v2 = "flag_`v'_switch"
	egen `v2' = max(temp_flag), by(clu_id year)
	drop temp*
}
la var flag_rt_sched_cd_switch "Flag indicating SP in CLU switched rate within year"
la var flag_rt_category_switch "Flag indicating SP in CLU switched rate category within year"
la var flag_rt_large_ag_switch "Flag indicating SP in CLU switched rate group within year"
la var flag_hp_bin_dec_switch "Flag indicating SP in CLU switched horsepower decile within year"
la var flag_kw_bin_dec_switch "Flag indicating SP in CLU switched killowatt decile within year"
la var flag_ope_bin_dec_switch "Flag indicating SP in CLU switched OPE decile within year"

	// Create a flag for SP-years with < 12 months
egen temp = count(modate), by(sp_group year)
gen temp_flag = (temp<12)
egen flag_partial_year = max(temp_flag), by(clu_id year)
drop temp*
la var flag_partial_year "Flag indicating a CLU-year includes an SP-year with <12 months of bills"
replace flag_irregular_bill = max(flag_irregular_bill,flag_partial_year)

	// Take mode of string rate variables
foreach v of varlist rt_sched_cd rt_default rt_modal rt_sched_cd_init {
	egen temp = mode(`v'), by(clu_id year)
	replace `v' = temp
	local vlab1: variable label `v'
	local vlab2 = "`vlab1', mode of SP-months in CLU-year"
	la var `v' "`vlab2'"
	drop temp
}

	// Take mode of numeric rate and bin variables
foreach v of varlist rt_group rt_category rt_large_ag hp* kw* ope* {
	egen temp = mode(`v'), maxmode by(clu_id year)
	replace `v' = temp
	drop temp
	local vlab1: variable label `v'
	local vlab2 = "`vlab1', mode of SP-months in CLU-year"
	la var `v' "`vlab2'"
}

	// Take mode of group variables
foreach v of varlist wdist_group county_group basin_group cz_group {
	egen temp = mode(`v'), maxmode by(clu_id year)
	replace `v' = temp
	drop temp
	local vlab1: variable label `v'
	local vlab2 = "`vlab1', mode of SP-months in CLU-year"
	la var `v' "`vlab2'"
}

	// Take min of rate indicator variables
foreach v of varlist sp_same_rate* {
	egen temp = min(`v'), by(clu_id year)
	replace `v' = temp
	drop temp
	local vlab1: variable label `v'
	local vlab2 = subinstr("`vlab1'","SPs","all SPs in CLU",1)
	la var `v' "`vlab2'"
}

	// Take max of other indicator variables
foreach v of varlist in_calif in_pge in_pou in_interval net_mtr_ind dr_ind elec_binary {
	egen temp = max(`v'), by(clu_id year)
	replace `v' = temp
	drop temp
}

	// Calculate fraction of CLU-months with electricity consumption
egen temp_mo = max(elec_binary), by(clu_id modate)
egen temp_mean = mean(temp_mo), by(clu_id)
replace elec_binary_frac = temp_mean
la var elec_binary_frac "Fraction of CLU's months with kwh>0"
drop temp*

	// Take mean of temperatures for year and for summer/winter
foreach v of varlist degreesC* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(clu_id year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(clu_id year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take mean of groundwater variables for year and for summer/winter
foreach v of varlist gw* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(clu_id year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(clu_id year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take mean of other numeric variables
foreach v of varlist interval_bill_corr {
	egen temp = wtmean(`v'), by(clu_id year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take max of EE measures
egen temp_tag = tag(sp_group clu_id)
egen temp_count = max(ee_measure_count), by(sp_group)
gen temp_prod = temp_tag*temp_count
egen temp_clu = sum(temp_prod), by(clu_id)
replace ee_measure_count = temp_clu
drop temp*

	// Take sum of APEP projects
egen temp_tag = tag(sp_group clu_id)
gen temp_count = temp_tag*apep_proj_count
egen temp_clu = sum(temp_count), by(clu_id)
replace apep_proj_count = temp_clu
drop temp*

	// Create indicators and take mode of data pull
levelsof pull, local(pulls)
foreach p of local pulls {
	local v = "pull_`p'"
	gen temp = (pull=="`p'")
	egen `v' = max(temp), by(clu_id year)
	la var `v' "CLU-year contains data from `p' pull"
	drop temp
}	
egen temp = mode(pull), by(clu_id year)
replace pull = temp
drop temp
la var pull "Modal data pull for CLU-year"

	// Count SPs in CLU and CLU-year
egen temp_clu = tag(sp_group clu_id)
egen spcount_clu = sum(temp_clu), by(clu_id)
egen temp_cy = tag(sp_group clu_id year)
egen spcount_clu_year = sum(temp_cy), by(clu_id year)
la var spcount_clu "Number of SPs in CLU over all years"
la var spcount_clu_year "Number of SPs in CLU in a year"
drop temp*

	// Create indicator for set of SPs within a CLU for each year
preserve
keep sp_uuid clu_id year
duplicates drop
sort clu_id year sp_uuid
egen clu_year_group = group(clu_id year)
bysort clu_year_group (sp_uuid) : gen clu_year_n = _n
reshape wide sp_uuid, i(clu_id year) j(clu_year_n)
egen clu_sp_group = group(clu_id sp_uuid*), missing
keep clu_id year clu_sp_group
tempfile clu_sp_group
save `clu_sp_group'
restore
merge m:1 clu_id year using `clu_sp_group'
assert _merge==3
drop _merge
la var clu_sp_group "Identifier for group of SPs comprising the CLU"

	// Drop remaining monthly variables
drop modate month summer winter days ctrl_fxn* log* ihs* sp_uuid sp_group ///
     prem* sa_sp* *dt* parcelid_conc latlon* spcount_parcelid_conc months*

	// Collapse
duplicates drop
unique clu_id year
assert r(unique)==r(N)

** Inverse hyperbolic sine and log transform electricity quantity
foreach v of varlist *bill_kwh {
	if strpos("`v'","summer")>0 {
		local vpost = "_summer"
	}
	else if strpos("`v'","winter")>0 {
		local vpost = "_winter"
	}
	else {
		local vpost = ""
	}
	local v_ihs = "ihs_kwh`vpost'"
	gen `v_ihs' = ln(100*`v' + sqrt((100*`v')^2+1))
	replace `v_ihs' = . if `v'<0
	local v_log = "log_kwh`vpost'"
	gen `v_log' = ln(`v')
	local v_log1 = "log1_kwh`vpost'"
	gen `v_log1' = ln(`v'+1)
	replace `v_log1' = . if `v'<0
	local v_log1_100 = "log1_100kwh`vpost'"
	gen `v_log1_100' = ln(100*`v'+1)
	replace `v_log1_100' = . if `v'<0
	local vlab_mid = subinstr("`vpost'","_"," ",1)
	local vlab_ihs = "Inverse hyperbolic sine of 100*kWh`vlab_mid' elec consumption"
	local vlab_log = "Log of kWh`vlab_mid' elec consumption"
	local vlab_log1 = "Log+1 of kWh`vlab_mid' elec consumption"
	local vlab_log1_100 = "Log+1 of 100*kWh`vlab_mid' elec consumption"
	la var `v_ihs' "`vlab_ihs'"
	la var `v_log' "`vlab_log'"
	la var `v_log1' "`vlab_log1'"
	la var `v_log1_100' "`vlab_log1_100'"
}

** Log-transform marginal electricity prices
foreach v of varlist mean_p_kwh min_p_kwh max_p_kwh {
	foreach vsuf in "" "_summer" "_winter" {
		local v2 = "`v'`vsuf'"
		local vpre = substr("`v2'",1,strpos("`v2'","_")-1)
		local v3 = subinstr(subinstr("`v2'","`vpre'","log",1),"kwh","`vpre'",1)
		gen `v3' = ln(`v2')
		local lab: var label `v2'
		la var `v3' "Log `lab'"
	}
}

** Log-transform prices to be used as instruments
foreach v of varlist *p_kwh_e1* *p_kwh_e20* *p_kwh_ag_default* *p_kwh_ag_modal* *p_kwh_init* {
	gen log_`v' = ln(`v')
	local lab: var label `v'
	la var log_`v' "Log `lab'"
}	

** Construct instruments of lagged electricity prices
tsset clu_id year
foreach v of varlist log_p* {
	local vpre = subinstr(subinstr(substr("`v'",7,.),"_summer","",1),"_winter","",1)
	local v2 = subinstr("`v'","`vpre'","`vpre'_lag",1)
	gen `v2' = L.`v'
	local labpre = proper(subinstr("`vpre'","mean","avg",1))
	if strpos("`v'","summer")>0 {
		local labpost = " summer"
	}
	else if strpos("`v'","winter")>0 {
		local labpost = " winter"
	}
	else {
		local labpost = ""
	}
	la var `v2' "`labpre' CLU-spec marg elec price (log $/kWh), lagged`labpost'"
}

** Construct instruments of lagged default prices
foreach v of varlist log*default* {
	local vpos1 = strpos("`v'","_")+1
	local vpos2 = strpos("`v'","p_kwh")
	local vpre = substr("`v'",`vpos1',`vpos2'-`vpos1'-1)
	local v2 = subinstr(subinstr(subinstr("`v'","_`vpre'","",1),"kwh","`vpre'",1),"ag_default","deflag",1)
	gen `v2' = L.`v'
	local labpre = proper(subinstr("`vpre'","mean","avg",1))
	if strpos("`v'","summer")>0 {
		local labpost = " summer"
	}
	else if strpos("`v'","winter")>0 {
		local labpost = " winter"
	}
	else {
		local labpost = ""
	}
	la var `v2' "`labpre' CLU-spec default marg elec price (log $/kWh), lagged`labpost'"
}

** Construct instruments of lagged modal prices
foreach v of varlist log*modal* {
	local vpos1 = strpos("`v'","_")+1
	local vpos2 = strpos("`v'","p_kwh")
	local vpre = substr("`v'",`vpos1',`vpos2'-`vpos1'-1)
	local v2 = subinstr(subinstr(subinstr("`v'","_`vpre'","",1),"kwh","`vpre'",1),"ag_modal","modlag",1)
	gen `v2' = L.`v'
	local labpre = proper(subinstr("`vpre'","mean","avg",1))
	if strpos("`v'","summer")>0 {
		local labpost = " summer"
	}
	else if strpos("`v'","winter")>0 {
		local labpost = " winter"
	}
	else {
		local labpost = ""
	}
	la var `v2' "`labpre' CLU-spec modal marg elec price (log $/kWh), lagged`labpost'"
}

** Save
order clu_id year
sort clu_id year
unique clu_id year
assert r(unique)==r(N)
compress
save "$dirpath_data/merged/clu_annual_elec_panel.dta", replace

}

*****************************************************************************
*****************************************************************************

** 3. Parcel-by-year electricity regression dataset
if 1==1{

** Start with monthly electricity dataset
use "$dirpath_data/merged/sp_month_elec_panel.dta", clear

** Keep only SP-months that merge to water data and have a parcel ID
keep if merge_sp_water_panel==3
keep if parcelid_conc!=.

** Collapse to parcel-year level
duplicates report parcelid_conc year
gen winter = 1-summer
gen days = .
replace days = 31 if inlist(month,1,3,5,7,8,10,12)
replace days = 30 if inlist(month,4,6,9,11)
replace days = 29 if month==2 & inlist(year,2008,2012,2016)
replace days = 28 if month==2 & !inlist(year,2008,2012,2016)

	// Sum kwh and bill amounts for year and for summer/winter
foreach v of varlist mnth* {
	foreach s of varlist summer winter {
		local v2 = subinstr("`v'","mnth","`s'",1)
		egen `v2' = sum(`v'*`s'), by(parcelid_conc year)
		local vlab1: variable label `v'
		local vlab2 = subinstr("`vlab1'","monthified","`s'",1)
		la var `v2' "`vlab2'"
	}
	local v2 = subinstr("`v'","mnth","ann",1)
	egen `v2' = sum(`v'), by(parcelid_conc year)
	local vlab1: variable label `v'
	local vlab2 = subinstr("`vlab1'","monthified","annual",1)
	la var `v2' "`vlab2'"
	drop `v'
}

	// Take mean of electricity prices for year and for summer/winter
foreach v of varlist mean_p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(parcelid_conc year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(parcelid_conc year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take min of electricity prices for year and for summer/winter
foreach v of varlist min_p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = min(temp), by(parcelid_conc year)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = min(`v'), by(parcelid_conc year)
	replace `v' = temp
	drop temp
}

	// Take max of electricity prices for year and for summer/winter
foreach v of varlist max_p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = max(temp), by(parcelid_conc year)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = max(`v'), by(parcelid_conc year)
	replace `v' = temp
	drop temp
}

	// Take mean of tiered electricity prices for year and for summer/winter
foreach v of varlist p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "mean_`v'_`s'"
		egen `v2' = wtmean(temp), by(parcelid_conc year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "Avg daily `vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	local v2 = "mean_`v'"
	egen `v2' = wtmean(`v'), by(parcelid_conc year) weight(days)
	local vlab1: variable label `v'
	local vlab2 = "Avg daily `vlab1'"
	la var `v2' "`vlab2'"
	drop `v'
}

	// Take mean of demand charges for year and for summer/winter
foreach v of varlist mean_p_kw_* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(parcelid_conc year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(parcelid_conc year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take max of flag variables
foreach v of varlist *flag* {
	egen temp = max(`v'), by(parcelid_conc year)
	replace `v' = temp
	drop temp
}

	// Create flags for important switches within a year
foreach v of varlist rt_sched_cd rt_category rt_large_ag hp_bin_dec kw_bin_dec ope_bin_dec {
	egen temp_tag = tag(`v' sp_group year), missing
	egen temp_sp = sum(temp_tag), by(sp_group year)
	gen temp_flag = (temp_sp>1)
	local v2 = "flag_`v'_switch"
	egen `v2' = max(temp_flag), by(parcelid_conc year)
	drop temp*
}
la var flag_rt_sched_cd_switch "Flag indicating SP in parcel switched rate within year"
la var flag_rt_category_switch "Flag indicating SP in parcel switched rate category within year"
la var flag_rt_large_ag_switch "Flag indicating SP in parcel switched rate group within year"
la var flag_hp_bin_dec_switch "Flag indicating SP in parcel switched horsepower decile within year"
la var flag_kw_bin_dec_switch "Flag indicating SP in parcel switched killowatt decile within year"
la var flag_ope_bin_dec_switch "Flag indicating SP in parcel switched OPE decile within year"

	// Create a flag for SP-years with < 12 months
egen temp = count(modate), by(sp_group year)
gen temp_flag = (temp<12)
egen flag_partial_year = max(temp_flag), by(parcelid_conc year)
drop temp*
la var flag_partial_year "Flag indicating a parcel-year includes an SP-year with <12 months of bills"
replace flag_irregular_bill = max(flag_irregular_bill,flag_partial_year)

	// Take mode of string rate variables
foreach v of varlist rt_sched_cd rt_default rt_modal rt_sched_cd_init {
	egen temp = mode(`v'), by(parcelid_conc year)
	replace `v' = temp
	local vlab1: variable label `v'
	local vlab2 = "`vlab1', mode of SP-months in parcel-year"
	la var `v' "`vlab2'"
	drop temp
}

	// Take mode of numeric rate and bin variables
foreach v of varlist rt_group rt_category rt_large_ag hp* kw* ope* {
	egen temp = mode(`v'), maxmode by(parcelid_conc year)
	replace `v' = temp
	drop temp
	local vlab1: variable label `v'
	local vlab2 = "`vlab1', mode of SP-months in parcel-year"
	la var `v' "`vlab2'"
}

	// Take mode of group variables
foreach v of varlist wdist_group county_group basin_group cz_group {
	egen temp = mode(`v'), maxmode by(parcelid_conc year)
	replace `v' = temp
	drop temp
	local vlab1: variable label `v'
	local vlab2 = "`vlab1', mode of SP-months in parcel-year"
	la var `v' "`vlab2'"
}

	// Take mode of county
egen temp = mode(county), maxmode by(parcelid_conc year)
replace county = temp
drop temp

	// Take min of rate indicator variables
foreach v of varlist sp_same_rate* {
	egen temp = min(`v'), by(parcelid_conc year)
	replace `v' = temp
	drop temp
	local vlab1: variable label `v'
	local vlab2 = subinstr("`vlab1'","SPs","all SPs in parcel",1)
	la var `v' "`vlab2'"
}

	// Take max of other indicator variables
foreach v of varlist in_calif in_pge in_pou in_interval net_mtr_ind dr_ind elec_binary {
	egen temp = max(`v'), by(parcelid_conc year)
	replace `v' = temp
	drop temp
}

	// Calculate fraction of parcel-months with electricity consumption
egen temp_mo = max(elec_binary), by(parcelid_conc modate)
egen temp_mean = mean(temp_mo), by(parcelid_conc)
replace elec_binary_frac = temp_mean
la var elec_binary_frac "Fraction of parcel's months with kwh>0"
drop temp*

	// Take mean of temperatures for year and for summer/winter
foreach v of varlist degreesC* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(parcelid_conc year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(parcelid_conc year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take mean of groundwater variables for year and for summer/winter
foreach v of varlist gw* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(parcelid_conc year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(parcelid_conc year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take mean of other numeric variables
foreach v of varlist interval_bill_corr {
	egen temp = wtmean(`v'), by(parcelid_conc year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take max of EE measures
egen temp_tag = tag(sp_group parcelid_conc)
egen temp_count = max(ee_measure_count), by(sp_group)
gen temp_prod = temp_tag*temp_count
egen temp_parc = sum(temp_prod), by(parcelid_conc)
replace ee_measure_count = temp_parc
drop temp*

	// Take sum of APEP projects
egen temp_tag = tag(sp_group parcelid_conc)
gen temp_count = temp_tag*apep_proj_count
egen temp_parc = sum(temp_count), by(parcelid_conc)
replace apep_proj_count = temp_parc
drop temp*

	// Create indicators and take mode of data pull
levelsof pull, local(pulls)
foreach p of local pulls {
	local v = "pull_`p'"
	gen temp = (pull=="`p'")
	egen `v' = max(temp), by(parcelid_conc year)
	la var `v' "parcel-year contains data from `p' pull"
	drop temp
}	
egen temp = mode(pull), by(parcelid_conc year)
replace pull = temp
drop temp
la var pull "Modal data pull for parcel-year"

	// Count SPs in parcel and parcel-year
egen temp_parc = tag(sp_group parcelid_conc)
egen spcount_parcel = sum(temp_parc), by(parcelid_conc)
egen temp_py = tag(sp_group parcelid_conc year)
egen spcount_parcel_year = sum(temp_py), by(parcelid_conc year)
la var spcount_parcel "Number of SPs in parcel over all years"
la var spcount_parcel_year "Number of SPs in parcel in a year"
drop temp*

	// Create indicator for set of SPs within a parcel for each year
preserve
keep sp_uuid parcelid_conc year
duplicates drop
sort parcelid_conc year sp_uuid
egen parc_year_group = group(parcelid_conc year)
bysort parc_year_group (sp_uuid) : gen parc_year_n = _n
reshape wide sp_uuid, i(parcelid_conc year) j(parc_year_n)
egen parcel_sp_group = group(parcelid_conc sp_uuid*), missing
keep parcelid_conc year parcel_sp_group
tempfile parcel_sp_group
save `parcel_sp_group'
restore
merge m:1 parcelid_conc year using `parcel_sp_group'
assert _merge==3
drop _merge
la var parcel_sp_group "Identifier for group of SPs comprising the parcel"

	// Drop CLU-specific variables
drop clu* crop* frac* ever* acres* mode* 

	// Drop remaining monthly variables
drop modate month summer winter days ctrl_fxn* log* ihs* sp_uuid sp_group ///
     prem* sa_sp* *dt* latlon* spcount_parcelid_conc months*

	// Collapse
duplicates drop
unique parcelid_conc year
assert r(unique)==r(N)

** Inverse hyperbolic sine and log transform electricity quantity
foreach v of varlist *bill_kwh {
	if strpos("`v'","summer")>0 {
		local vpost = "_summer"
	}
	else if strpos("`v'","winter")>0 {
		local vpost = "_winter"
	}
	else {
		local vpost = ""
	}
	local v_ihs = "ihs_kwh`vpost'"
	gen `v_ihs' = ln(100*`v' + sqrt((100*`v')^2+1))
	replace `v_ihs' = . if `v'<0
	local v_log = "log_kwh`vpost'"
	gen `v_log' = ln(`v')
	local v_log1 = "log1_kwh`vpost'"
	gen `v_log1' = ln(`v'+1)
	replace `v_log1' = . if `v'<0
	local v_log1_100 = "log1_100kwh`vpost'"
	gen `v_log1_100' = ln(100*`v'+1)
	replace `v_log1_100' = . if `v'<0
	local vlab_mid = subinstr("`vpost'","_"," ",1)
	local vlab_ihs = "Inverse hyperbolic sine of 100*kWh`vlab_mid' elec consumption"
	local vlab_log = "Log of kWh`vlab_mid' elec consumption"
	local vlab_log1 = "Log+1 of kWh`vlab_mid' elec consumption"
	local vlab_log1_100 = "Log+1 of 100*kWh`vlab_mid' elec consumption"
	la var `v_ihs' "`vlab_ihs'"
	la var `v_log' "`vlab_log'"
	la var `v_log1' "`vlab_log1'"
	la var `v_log1_100' "`vlab_log1_100'"
}

** Log-transform marginal electricity prices
foreach v of varlist mean_p_kwh min_p_kwh max_p_kwh {
	foreach vsuf in "" "_summer" "_winter" {
		local v2 = "`v'`vsuf'"
		local vpre = substr("`v2'",1,strpos("`v2'","_")-1)
		local v3 = subinstr(subinstr("`v2'","`vpre'","log",1),"kwh","`vpre'",1)
		gen `v3' = ln(`v2')
		local lab: var label `v2'
		la var `v3' "Log `lab'"
	}
}

** Log-transform prices to be used as instruments
foreach v of varlist *p_kwh_e1* *p_kwh_e20* *p_kwh_ag_default* *p_kwh_ag_modal* *p_kwh_init* {
	gen log_`v' = ln(`v')
	local lab: var label `v'
	la var log_`v' "Log `lab'"
}	

** Construct instruments of lagged electricity prices
tsset parcelid_conc year
foreach v of varlist log_p* {
	local vpre = subinstr(subinstr(substr("`v'",7,.),"_summer","",1),"_winter","",1)
	local v2 = subinstr("`v'","`vpre'","`vpre'_lag",1)
	gen `v2' = L.`v'
	local labpre = proper(subinstr("`vpre'","mean","avg",1))
	if strpos("`v'","summer")>0 {
		local labpost = " summer"
	}
	else if strpos("`v'","winter")>0 {
		local labpost = " winter"
	}
	else {
		local labpost = ""
	}
	la var `v2' "`labpre' parcel-spec marg elec price (log $/kWh), lagged`labpost'"
}

** Construct instruments of lagged default prices
foreach v of varlist log*default* {
	local vpos1 = strpos("`v'","_")+1
	local vpos2 = strpos("`v'","p_kwh")
	local vpre = substr("`v'",`vpos1',`vpos2'-`vpos1'-1)
	local v2 = subinstr(subinstr(subinstr("`v'","_`vpre'","",1),"kwh","`vpre'",1),"ag_default","deflag",1)
	gen `v2' = L.`v'
	local labpre = proper(subinstr("`vpre'","mean","avg",1))
	if strpos("`v'","summer")>0 {
		local labpost = " summer"
	}
	else if strpos("`v'","winter")>0 {
		local labpost = " winter"
	}
	else {
		local labpost = ""
	}
	la var `v2' "`labpre' parcel-spec default marg elec price (log $/kWh), lagged`labpost'"
}

** Construct instruments of lagged modal prices
foreach v of varlist log*modal* {
	local vpos1 = strpos("`v'","_")+1
	local vpos2 = strpos("`v'","p_kwh")
	local vpre = substr("`v'",`vpos1',`vpos2'-`vpos1'-1)
	local v2 = subinstr(subinstr(subinstr("`v'","_`vpre'","",1),"kwh","`vpre'",1),"ag_modal","modlag",1)
	gen `v2' = L.`v'
	local labpre = proper(subinstr("`vpre'","mean","avg",1))
	if strpos("`v'","summer")>0 {
		local labpost = " summer"
	}
	else if strpos("`v'","winter")>0 {
		local labpost = " winter"
	}
	else {
		local labpost = ""
	}
	la var `v2' "`labpre' parcel-spec modal marg elec price (log $/kWh), lagged`labpost'"
}

** Save
order parcelid_conc year
sort parcelid_conc year
unique parcelid_conc year
assert r(unique)==r(N)
compress
save "$dirpath_data/merged/parcel_annual_elec_panel.dta", replace

}

*****************************************************************************
*****************************************************************************

** 4. CLU75-by-year electricity regression dataset
if 1==0{

** Start with monthly electricity dataset
use "$dirpath_data/merged/sp_month_elec_panel.dta", clear

** Keep only SP-months that merge to water data and have a CLU group75 ID
keep if merge_sp_water_panel==3
keep if clu_group75!=.

** Collapse to CLU75-year level
duplicates report clu_group75 year
gen winter = 1-summer
gen days = .
replace days = 31 if inlist(month,1,3,5,7,8,10,12)
replace days = 30 if inlist(month,4,6,9,11)
replace days = 29 if month==2 & inlist(year,2008,2012,2016)
replace days = 28 if month==2 & !inlist(year,2008,2012,2016)

	// Sum kwh and bill amounts for year and for summer/winter
foreach v of varlist mnth* {
	foreach s of varlist summer winter {
		local v2 = subinstr("`v'","mnth","`s'",1)
		egen `v2' = sum(`v'*`s'), by(clu_group75 year)
		local vlab1: variable label `v'
		local vlab2 = subinstr("`vlab1'","monthified","`s'",1)
		la var `v2' "`vlab2'"
	}
	local v2 = subinstr("`v'","mnth","ann",1)
	egen `v2' = sum(`v'), by(clu_group75 year)
	local vlab1: variable label `v'
	local vlab2 = subinstr("`vlab1'","monthified","annual",1)
	la var `v2' "`vlab2'"
	drop `v'
}

	// Take mean of electricity prices for year and for summer/winter
foreach v of varlist mean_p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(clu_group75 year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(clu_group75 year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take min of electricity prices for year and for summer/winter
foreach v of varlist min_p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = min(temp), by(clu_group75 year)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = min(`v'), by(clu_group75 year)
	replace `v' = temp
	drop temp
}

	// Take max of electricity prices for year and for summer/winter
foreach v of varlist max_p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = max(temp), by(clu_group75 year)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = max(`v'), by(clu_group75 year)
	replace `v' = temp
	drop temp
}

	// Take mean of tiered electricity prices for year and for summer/winter
foreach v of varlist p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "mean_`v'_`s'"
		egen `v2' = wtmean(temp), by(clu_group75 year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "Avg daily `vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	local v2 = "mean_`v'"
	egen `v2' = wtmean(`v'), by(clu_group75 year) weight(days)
	local vlab1: variable label `v'
	local vlab2 = "Avg daily `vlab1'"
	la var `v2' "`vlab2'"
	drop `v'
}

	// Take mean of demand charges for year and for summer/winter
foreach v of varlist mean_p_kw_* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(clu_group75 year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(clu_group75 year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take max of flag variables
foreach v of varlist *flag* {
	egen temp = max(`v'), by(clu_group75 year)
	replace `v' = temp
	drop temp
}

	// Create flags for important switches within a year
foreach v of varlist rt_sched_cd rt_category rt_large_ag hp_bin_dec kw_bin_dec ope_bin_dec {
	egen temp_tag = tag(`v' sp_group year), missing
	egen temp_sp = sum(temp_tag), by(sp_group year)
	gen temp_flag = (temp_sp>1)
	local v2 = "flag_`v'_switch"
	egen `v2' = max(temp_flag), by(clu_group75 year)
	drop temp*
}
la var flag_rt_sched_cd_switch "Flag indicating SP in CLU75 switched rate within year"
la var flag_rt_category_switch "Flag indicating SP in CLU75 switched rate category within year"
la var flag_rt_large_ag_switch "Flag indicating SP in CLU75 switched rate group within year"
la var flag_hp_bin_dec_switch "Flag indicating SP in CLU75 switched horsepower decile within year"
la var flag_kw_bin_dec_switch "Flag indicating SP in CLU75 switched killowatt decile within year"
la var flag_ope_bin_dec_switch "Flag indicating SP in CLU75 switched OPE decile within year"

	// Create a flag for SP-years with < 12 months
egen temp = count(modate), by(sp_group year)
gen temp_flag = (temp<12)
egen flag_partial_year = max(temp_flag), by(clu_group75 year)
drop temp*
la var flag_partial_year "Flag indicating a CLU75-year includes an SP-year with <12 months of bills"
replace flag_irregular_bill = max(flag_irregular_bill,flag_partial_year)

	// Take mode of string rate variables
foreach v of varlist rt_sched_cd rt_default rt_modal rt_sched_cd_init {
	egen temp = mode(`v'), by(clu_group75 year)
	replace `v' = temp
	local vlab1: variable label `v'
	local vlab2 = "`vlab1', mode of SP-months in CLU75-year"
	la var `v' "`vlab2'"
	drop temp
}

	// Take mode of numeric rate and bin variables
foreach v of varlist rt_group rt_category rt_large_ag hp* kw* ope* {
	egen temp = mode(`v'), maxmode by(clu_group75 year)
	replace `v' = temp
	drop temp
	local vlab1: variable label `v'
	local vlab2 = "`vlab1', mode of SP-months in CLU75-year"
	la var `v' "`vlab2'"
}

	// Take mode of group variables
foreach v of varlist wdist_group county_group basin_group cz_group {
	egen temp = mode(`v'), maxmode by(clu_group75 year)
	replace `v' = temp
	drop temp
	local vlab1: variable label `v'
	local vlab2 = "`vlab1', mode of SP-months in CLU75-year"
	la var `v' "`vlab2'"
}

	// Take min of rate indicator variables
foreach v of varlist sp_same_rate* {
	egen temp = min(`v'), by(clu_group75 year)
	replace `v' = temp
	drop temp
	local vlab1: variable label `v'
	local vlab2 = subinstr("`vlab1'","SPs","all SPs in CLU75",1)
	la var `v' "`vlab2'"
}

	// Take max of other indicator variables
foreach v of varlist in_calif in_pge in_pou in_interval net_mtr_ind dr_ind elec_binary {
	egen temp = max(`v'), by(clu_group75 year)
	replace `v' = temp
	drop temp
}

	// Calculate fraction of CLU-months with electricity consumption
egen temp_mo = max(elec_binary), by(clu_group75 modate)
egen temp_mean = mean(temp_mo), by(clu_group75)
replace elec_binary_frac = temp_mean
la var elec_binary_frac "Fraction of CLU75's months with kwh>0"
drop temp*

	// Take mean of temperatures for year and for summer/winter
foreach v of varlist degreesC* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(clu_group75 year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(clu_group75 year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take mean of groundwater variables for year and for summer/winter
foreach v of varlist gw* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(clu_group75 year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(clu_group75 year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take mean of other numeric variables
foreach v of varlist interval_bill_corr {
	egen temp = wtmean(`v'), by(clu_group75 year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take max of EE measures
egen temp_tag = tag(sp_group clu_group75)
egen temp_count = max(ee_measure_count), by(sp_group)
gen temp_prod = temp_tag*temp_count
egen temp_clu = sum(temp_prod), by(clu_group75)
replace ee_measure_count = temp_clu
drop temp*

	// Take sum of APEP projects
egen temp_tag = tag(sp_group clu_group75)
gen temp_count = temp_tag*apep_proj_count
egen temp_clu = sum(temp_count), by(clu_group75)
replace apep_proj_count = temp_clu
drop temp*

	// Create indicators and take mode of data pull
levelsof pull, local(pulls)
foreach p of local pulls {
	local v = "pull_`p'"
	gen temp = (pull=="`p'")
	egen `v' = max(temp), by(clu_group75 year)
	la var `v' "CLU-year contains data from `p' pull"
	drop temp
}	
egen temp = mode(pull), by(clu_group75 year)
replace pull = temp
drop temp
la var pull "Modal data pull for CLU75-year"

	// Count SPs in CLU75 and CLU75-year
egen temp_clu = tag(sp_group clu_group75)
egen spcount_clu75 = sum(temp_clu), by(clu_group75)
egen temp_cy = tag(sp_group clu_group75 year)
egen spcount_clu75_year = sum(temp_cy), by(clu_group75 year)
la var spcount_clu75 "Number of SPs in CLU75 over all years"
la var spcount_clu75_year "Number of SPs in CLU75 in a year"
drop temp*

	// Create indicator for set of SPs within a CLU75 for each year
preserve
keep sp_uuid clu_group75 year
duplicates drop
sort clu_group75 year sp_uuid
egen clu_year_group = group(clu_group75 year)
bysort clu_year_group (sp_uuid) : gen clu_year_n = _n
reshape wide sp_uuid, i(clu_group75 year) j(clu_year_n)
egen clu75_sp_group = group(clu_group75 sp_uuid*), missing
keep clu_group75 year clu75_sp_group
tempfile clu75_sp_group
save `clu75_sp_group'
restore
merge m:1 clu_group75 year using `clu75_sp_group'
assert _merge==3
drop _merge
la var clu75_sp_group "Identifier for group of SPs comprising the CLU75"

	// Drop CLU-specific variables
drop clu_id clu_group0 clu_group10 clu_group25 clu_group50 cluacres crop* frac* ever* acres* mode* 

	// Drop remaining monthly variables
drop modate month summer winter days ctrl_fxn* log* ihs* sp_uuid sp_group ///
     prem* sa_sp* *dt* parcelid_conc latlon* spcount_parcelid_conc months*

	// Collapse
duplicates drop
unique clu_group75 year
assert r(unique)==r(N)

** Inverse hyperbolic sine and log transform electricity quantity
foreach v of varlist *bill_kwh {
	if strpos("`v'","summer")>0 {
		local vpost = "_summer"
	}
	else if strpos("`v'","winter")>0 {
		local vpost = "_winter"
	}
	else {
		local vpost = ""
	}
	local v_ihs = "ihs_kwh`vpost'"
	gen `v_ihs' = ln(100*`v' + sqrt((100*`v')^2+1))
	replace `v_ihs' = . if `v'<0
	local v_log = "log_kwh`vpost'"
	gen `v_log' = ln(`v')
	local v_log1 = "log1_kwh`vpost'"
	gen `v_log1' = ln(`v'+1)
	replace `v_log1' = . if `v'<0
	local v_log1_100 = "log1_100kwh`vpost'"
	gen `v_log1_100' = ln(100*`v'+1)
	replace `v_log1_100' = . if `v'<0
	local vlab_mid = subinstr("`vpost'","_"," ",1)
	local vlab_ihs = "Inverse hyperbolic sine of 100*kWh`vlab_mid' elec consumption"
	local vlab_log = "Log of kWh`vlab_mid' elec consumption"
	local vlab_log1 = "Log+1 of kWh`vlab_mid' elec consumption"
	local vlab_log1_100 = "Log+1 of 100*kWh`vlab_mid' elec consumption"
	la var `v_ihs' "`vlab_ihs'"
	la var `v_log' "`vlab_log'"
	la var `v_log1' "`vlab_log1'"
	la var `v_log1_100' "`vlab_log1_100'"
}

** Log-transform marginal electricity prices
foreach v of varlist mean_p_kwh min_p_kwh max_p_kwh {
	foreach vsuf in "" "_summer" "_winter" {
		local v2 = "`v'`vsuf'"
		local vpre = substr("`v2'",1,strpos("`v2'","_")-1)
		local v3 = subinstr(subinstr("`v2'","`vpre'","log",1),"kwh","`vpre'",1)
		gen `v3' = ln(`v2')
		local lab: var label `v2'
		la var `v3' "Log `lab'"
	}
}

** Log-transform prices to be used as instruments
foreach v of varlist *p_kwh_e1* *p_kwh_e20* *p_kwh_ag_default* *p_kwh_ag_modal* *p_kwh_init* {
	gen log_`v' = ln(`v')
	local lab: var label `v'
	la var log_`v' "Log `lab'"
}	

** Construct instruments of lagged electricity prices
tsset clu_group75 year
foreach v of varlist log_p* {
	local vpre = subinstr(subinstr(substr("`v'",7,.),"_summer","",1),"_winter","",1)
	local v2 = subinstr("`v'","`vpre'","`vpre'_lag",1)
	gen `v2' = L.`v'
	local labpre = proper(subinstr("`vpre'","mean","avg",1))
	if strpos("`v'","summer")>0 {
		local labpost = " summer"
	}
	else if strpos("`v'","winter")>0 {
		local labpost = " winter"
	}
	else {
		local labpost = ""
	}
	la var `v2' "`labpre' CLU75-spec marg elec price (log $/kWh), lagged`labpost'"
}

** Construct instruments of lagged default prices
foreach v of varlist log*default* {
	local vpos1 = strpos("`v'","_")+1
	local vpos2 = strpos("`v'","p_kwh")
	local vpre = substr("`v'",`vpos1',`vpos2'-`vpos1'-1)
	local v2 = subinstr(subinstr(subinstr("`v'","_`vpre'","",1),"kwh","`vpre'",1),"ag_default","deflag",1)
	gen `v2' = L.`v'
	local labpre = proper(subinstr("`vpre'","mean","avg",1))
	if strpos("`v'","summer")>0 {
		local labpost = " summer"
	}
	else if strpos("`v'","winter")>0 {
		local labpost = " winter"
	}
	else {
		local labpost = ""
	}
	la var `v2' "`labpre' CLU75-spec default marg elec price (log $/kWh), lagged`labpost'"
}

** Construct instruments of lagged modal prices
foreach v of varlist log*modal* {
	local vpos1 = strpos("`v'","_")+1
	local vpos2 = strpos("`v'","p_kwh")
	local vpre = substr("`v'",`vpos1',`vpos2'-`vpos1'-1)
	local v2 = subinstr(subinstr(subinstr("`v'","_`vpre'","",1),"kwh","`vpre'",1),"ag_modal","modlag",1)
	gen `v2' = L.`v'
	local labpre = proper(subinstr("`vpre'","mean","avg",1))
	if strpos("`v'","summer")>0 {
		local labpost = " summer"
	}
	else if strpos("`v'","winter")>0 {
		local labpost = " winter"
	}
	else {
		local labpost = ""
	}
	la var `v2' "`labpre' CLU75-spec modal marg elec price (log $/kWh), lagged`labpost'"
}

** Merge crop data
drop county
rename clu_group75 clu_group75_encode
decode clu_group75_encode, gen(clu_group75)
merge 1:1 clu_group75 year using "$dirpath_data/cleaned_spatial/CDL_panel_clugroup75_bigcat_year_wide.dta", nogen keep(3)
drop clu_group75
rename clu_group75_encode clu_group75

** Save
order clu_group75 year
sort clu_group75 year
unique clu_group75 year
assert r(unique)==r(N)
compress
save "$dirpath_data/merged/clu75_annual_elec_panel.dta", replace

}

*****************************************************************************
*****************************************************************************

** 5. SP-by-year water regression dataset
if 1==0{

** Start with monthly water dataset
use "$dirpath_data/merged/sp_month_water_panel.dta", clear

** Collapse to SP-year level
duplicates report sp_group year
gen winter = 1-summer
gen days = .
replace days = 31 if inlist(month,1,3,5,7,8,10,12)
replace days = 30 if inlist(month,4,6,9,11)
replace days = 29 if month==2 & inlist(year,2008,2012,2016)
replace days = 28 if month==2 & !inlist(year,2008,2012,2016)

	// Sum kwh and bill amounts for year and for summer/winter
foreach v of varlist mnth* {
	foreach s of varlist summer winter {
		local v2 = subinstr("`v'","mnth","`s'",1)
		egen `v2' = sum(`v'*`s'), by(sp_group year)
		local vlab1: variable label `v'
		local vlab2 = subinstr("`vlab1'","monthified","`s'",1)
		la var `v2' "`vlab2'"
	}
	local v2 = subinstr("`v'","mnth","ann",1)
	egen `v2' = sum(`v'), by(sp_group year)
	local vlab1: variable label `v'
	local vlab2 = subinstr("`vlab1'","monthified","annual",1)
	la var `v2' "`vlab2'"
	drop `v'
}

	// Take mean of electricity prices for year and for summer/winter
foreach v of varlist mean_p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(sp_group year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(sp_group year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take min of electricity prices for year and for summer/winter
foreach v of varlist min_p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = min(temp), by(sp_group year)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = min(`v'), by(sp_group year)
	replace `v' = temp
	drop temp
}

	// Take max of electricity prices for year and for summer/winter
foreach v of varlist max_p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = max(temp), by(sp_group year)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = max(`v'), by(sp_group year)
	replace `v' = temp
	drop temp
}

	// Take mean of tiered electricity prices for year and for summer/winter
foreach v of varlist p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "mean_`v'_`s'"
		egen `v2' = wtmean(temp), by(sp_group year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "Avg daily `vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	local v2 = "mean_`v'"
	egen `v2' = wtmean(`v'), by(sp_group year) weight(days)
	local vlab1: variable label `v'
	local vlab2 = "Avg daily `vlab1'"
	la var `v2' "`vlab2'"
	drop `v'
}

	// Take mean of demand charges for year and for summer/winter
foreach v of varlist mean_p_kw_* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(sp_group year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(sp_group year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take mean of kwh/af for year and summer/winter
foreach v of varlist kwhaf* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(sp_group year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(sp_group year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take mean of groundwater depth for year and summer/winter
foreach v of varlist gw*depth* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(sp_group year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(sp_group year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take mean, min, and max of distance to gw measurement for year and summer/winter
foreach v of varlist gw*dist* {
	local v_mean = subinstr("`v'","dist","distmean",1)
	local v_min = subinstr("`v'","dist","distmin",1)
	local v_max = subinstr("`v'","dist","distmax",1)
	egen `v_mean' = wtmean(`v'), by(sp_group year) weight(days)
	egen `v_min' = min(`v'), by(sp_group year)
	egen `v_max' = max(`v'), by(sp_group year)
	local vlab1: variable label `v'
	local vlab2 = subinstr(lower("`vlab1'"),"measurement","meas",1)
	if strpos("`v'","mth")>0 {
		local vtime = "mth"
	}
	else if strpos("`v'","qtr")>0 {
		local vtime = "qtr"
	}
	local vlab_mean = "Mean (over `vtime's) of `vlab2'"
	local vlab_min = "Min (over `vtime's) of `vlab2'"
	local vlab_max = "Max (over `vtime's) of `vlab2'"
	la var `v_mean' "`vlab_mean'"
	la var `v_min' "`vlab_min'"
	la var `v_max' "`vlab_max'"
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2_mean = "`v_mean'_`s'"
		local v2_min = "`v_min'_`s'"
		local v2_max = "`v_max'_`s'"
		egen `v2_mean' = wtmean(temp), by(sp_group year) weight(days)
		egen `v2_min' = min(temp), by(sp_group year)
		egen `v2_max' = max(temp), by(sp_group year)
		local vlab2_mean = "`vlab_mean', `s'"
		local vlab2_min = "`vlab_min', `s'"
		local vlab2_max = "`vlab_max', `s'"
		la var `v2_mean' "`vlab2_mean'"
		la var `v2_min' "`vlab2_min'"
		la var `v2_max' "`vlab2_max'"
		drop temp
	}
	drop `v'
}

	// Take mean, min, and max of gw measurement counts for year and summer/winter
foreach v of varlist gw*cnt* {
	local v_mean = subinstr("`v'","cnt","cntmean",1)
	local v_min = subinstr("`v'","cnt","cntmin",1)
	local v_max = subinstr("`v'","cnt","cntmax",1)
	egen `v_mean' = wtmean(`v'), by(sp_group year) weight(days)
	egen `v_min' = min(`v'), by(sp_group year)
	egen `v_max' = max(`v'), by(sp_group year)
	local vlab1: variable label `v'
	local vlab2 = subinstr(subinstr(subinstr(lower("`vlab1'"),"measurements","meas",1),"questionable","ques",1),"observation","obs",1)
	local vlab_mean = "Mean `vlab2'"
	local vlab_min = "Min `vlab2'"
	local vlab_max = "Max `vlab2'"
	la var `v_mean' "`vlab_mean'"
	la var `v_min' "`vlab_min'"
	la var `v_max' "`vlab_max'"
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2_mean = "`v_mean'_`s'"
		local v2_min = "`v_min'_`s'"
		local v2_max = "`v_max'_`s'"
		egen `v2_mean' = wtmean(temp), by(sp_group year) weight(days)
		egen `v2_min' = min(temp), by(sp_group year)
		egen `v2_max' = max(temp), by(sp_group year)
		local vlab2_mean = "`vlab_mean', `s'"
		local vlab2_min = "`vlab_min', `s'"
		local vlab2_max = "`vlab_max', `s'"
		la var `v2_mean' "`vlab2_mean'"
		la var `v2_min' "`vlab2_min'"
		la var `v2_max' "`vlab2_max'"
		drop temp
	}
	drop `v'
}

	// Sum acre-feet of water for year and for summer/winter
foreach v of varlist af* {
	foreach s of varlist summer winter {
		local v2 = "`v'_`s'"
		egen `v2' = sum(`v'*`s'), by(sp_group year)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
	}
	egen temp = sum(`v'), by(sp_group year)
	replace `v' = temp
	drop temp
}

	// Take mean of water prices for year and for summer/winter
foreach v of varlist mean_p_af* {
	foreach s of varlist summer winter {
		local s2 = substr("`s'",1,2)
		gen temp = `v' if `s'
		local v2 = "`v'_`s2'"
		egen `v2' = wtmean(temp), by(sp_group year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(sp_group year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take max of flag variables
foreach v of varlist flag_* {
	egen temp = max(`v'), by(sp_group year)
	replace `v' = temp
	drop temp
}

	// Create flags for important switches within a year
foreach v of varlist rt_sched_cd rt_category rt_large_ag hp_bin_dec kw_bin_dec ope_bin_dec {
	egen temp_tag = tag(`v' sp_group year), missing
	egen temp = sum(temp_tag), by(sp_group year)
	local v2 = "flag_`v'_switch"
	gen `v2' = temp>1
	drop temp*
}
la var flag_rt_sched_cd_switch "Flag indicating SP switched rate within year"
la var flag_rt_category_switch "Flag indicating SP switched rate category within year"
la var flag_rt_large_ag_switch "Flag indicating SP switched rate group within year"
la var flag_hp_bin_dec_switch "Flag indicating SP switched horsepower decile within year"
la var flag_kw_bin_dec_switch "Flag indicating SP switched killowatt decile within year"
la var flag_ope_bin_dec_switch "Flag indicating SP switched OPE decile within year"

	// Create a flag for SP-years with < 12 months
egen temp = count(modate), by(sp_group year)
gen flag_partial_year = (temp<12)
drop temp
la var flag_partial_year "Flag for SP-years with <12 months of bills"
replace flag_irregular_bill = max(flag_irregular_bill,flag_partial_year)

	// Take mode of string rate variables
foreach v of varlist rt_sched_cd rt_default rt_modal {
	egen temp = mode(`v'), by(sp_group year)
	replace `v' = temp
	local vlab1: variable label `v'
	local vlab2 = "`vlab1', mode of months in year"
	la var `v' "`vlab2'"
	drop temp
}

	// Take mode of numeric rate and bin variables
foreach v of varlist rt_group rt_category rt_large_ag hp* kw* ope* {
	egen temp = mode(`v'), maxmode by(sp_group year)
	replace `v' = temp
	drop temp
	local vlab1: variable label `v'
	local vlab2 = "`vlab1', mode of months in year"
	la var `v' "`vlab2'"
}

	// Take mode of drawdown prediction variables
egen temp = mode(drwdwn_predict_step), maxmode by(sp_group year)
gen temp_desc = drwdwn_predict_step_desc if temp==drwdwn_predict_step
replace drwdwn_predict_step = temp
egen temp_mode = mode(temp_desc), by(sp_group year)
replace drwdwn_predict_step_desc = temp_mode
local vlab1: variable label drwdwn_predict_step
local vlab2 = "`vlab1', mode of months in year"
la var drwdwn_predict_step "`vlab2'"
local vlab1: variable label drwdwn_predict_step_desc
local vlab2 = "`vlab1', mode of months in year"
la var drwdwn_predict_step_desc "`vlab2'"
drop temp*

	// Take max of other indicator variables
foreach v of varlist in_interval net_mtr_ind dr_ind elec_binary {
	egen temp = max(`v'), by(sp_group year)
	replace `v' = temp
	drop temp
}

	// Take mean of temperatures for year and for summer/winter
foreach v of varlist degreesC* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(sp_group year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(sp_group year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take mean of other numeric variables
foreach v of varlist interval_bill_corr {
	egen temp = wtmean(`v'), by(sp_group year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take mode of location variables and IDs
foreach v of varlist prem* latlon* pump* parcelid_concAPEP clu_*_ecAPEP {
	egen temp = mode(`v'), maxmode by(sp_group year)
	replace `v' = temp
	drop temp
}

	// Assign SP counts for APEP location IDs
foreach v of varlist parcelid_concAPEP clu_*_ecAPEP {
	local v2 = "spcount_`v'"
	egen temp = tag(sp_group `v')
	egen temp_sum = sum(temp), by(`v')
	replace `v2' = temp_sum
	drop temp*
}

	// Take minimum of date variables
foreach v of varlist sa_sp* *dt_first {
	egen temp = min(`v'), by(sp_group year)
	replace `v' = temp
	drop temp
}

	// Take maximum of date variables
foreach v of varlist *dt_last {
	egen temp = max(`v'), by(sp_group year)
	replace `v' = temp
	drop temp
}

	// Annualize monthly time differences
foreach v of varlist months* {
	gen temp = ceil(`v'/12)
	local v2 = subinstr("`v'","months","years",1)
	egen `v2' = min(temp), by(sp_group year)
	local vlab1: variable label `v'
	local vlab2 = subinstr("`vlab1'","Months","Years",1)
	la var `v2' "`vlab2'"
	drop `v' temp
}

	// Annualize test month-dates
foreach v of varlist *test_modate* {
	local v2 = subinstr("`v'","modate","year",1)
	gen temp = abs(modate-`v')
	if strpos("`v'","before")>0 {
		replace temp = . if year(dofm(modate))<=year(dofm(`v'))
	}
	else if strpos("`v'","after")>0 {
		replace temp = . if year(dofm(modate))>=year(dofm(`v'))
	}
	egen temp_min = min(temp), by(sp_group year)
	gen temp_year = year(dofm(`v')) if temp_min==temp & temp_min!=.
	egen `v2' = mode(temp_year), maxmode by(sp_group year)
	local vlab1: variable label `v'
	local vlab2 = subinstr(subinstr("`vlab1'","SP-month","SP-year",1),"Month","Year",1)
	la var `v2' "`vlab2'"
	drop `v' temp*
}

	// Take max of APEP project indicators
foreach v of varlist post_apep_proj_finish extrap* {
	egen temp = max(`v'), by(sp_group year)
	replace `v' = temp
	drop temp
}

	// Take mode of other APEP indicators and IDs
foreach v of varlist apep_interp_case apeptestid* {
	egen temp = mode(`v'), maxmode by(sp_group year)
	replace `v' = temp
	drop temp
	local vlab1: variable label `v'
	local vlab2 = "`vlab1', mode of months in year"
	la var `v' "`vlab2'"
}

	// Take max of EE measures
egen temp = max(ee_measure_count), by(sp_group)
replace ee_measure_count = temp
drop temp

	// Drop remaining monthly variables
drop modate month summer winter days ctrl_fxn* log* ln* ihs* L12* L6* gw_qtr_bsn*

	// Collapse
duplicates drop
unique sp_group year
assert r(unique)==r(N)

** Inverse hyperbolic sine and log transform electricity quantity
foreach v of varlist *bill_kwh {
	if strpos("`v'","summer")>0 {
		local vpost = "_summer"
	}
	else if strpos("`v'","winter")>0 {
		local vpost = "_winter"
	}
	else {
		local vpost = ""
	}
	local v_ihs = "ihs_kwh`vpost'"
	gen `v_ihs' = ln(100*`v' + sqrt((100*`v')^2+1))
	replace `v_ihs' = . if `v'<0
	local v_log = "log_kwh`vpost'"
	gen `v_log' = ln(`v')
	local v_log1 = "log1_kwh`vpost'"
	gen `v_log1' = ln(`v'+1)
	replace `v_log1' = . if `v'<0
	local v_log1_100 = "log1_100kwh`vpost'"
	gen `v_log1_100' = ln(100*`v'+1)
	replace `v_log1_100' = . if `v'<0
	local vlab_mid = subinstr("`vpost'","_"," ",1)
	local vlab_ihs = "Inverse hyperbolic sine of 100*kWh`vlab_mid' elec consumption"
	local vlab_log = "Log of kWh`vlab_mid' elec consumption"
	local vlab_log1 = "Log+1 of kWh`vlab_mid' elec consumption"
	local vlab_log1_100 = "Log+1 of 100*kWh`vlab_mid' elec consumption"
	la var `v_ihs' "`vlab_ihs'"
	la var `v_log' "`vlab_log'"
	la var `v_log1' "`vlab_log1'"
	la var `v_log1_100' "`vlab_log1_100'"
}

** Log-transform marginal electricity prices
foreach v of varlist mean_p_kwh min_p_kwh max_p_kwh {
	foreach vsuf in "" "_summer" "_winter" {
		local v2 = "`v'`vsuf'"
		local vpre = substr("`v2'",1,strpos("`v2'","_")-1)
		local v3 = subinstr(subinstr("`v2'","`vpre'","log",1),"kwh","`vpre'",1)
		gen `v3' = ln(`v2')
		local lab: var label `v2'
		la var `v3' "Log `lab'"
	}
}

** Log-transform electricity prices to be used as instruments
foreach v of varlist *p_kwh_e1* *p_kwh_e20* *p_kwh_ag_default* *p_kwh_ag_modal* *p_kwh_init* {
	gen log_`v' = ln(`v')
	local lab: var label `v'
	la var log_`v' "Log `lab'"
}	

** Construct instruments of lagged electricity prices
tsset sp_group year
foreach v of varlist log_p* {
	local vpre = subinstr(subinstr(substr("`v'",7,.),"_summer","",1),"_winter","",1)
	local v2 = subinstr("`v'","`vpre'","`vpre'_lag",1)
	gen `v2' = L.`v'
	local labpre = proper(subinstr("`vpre'","mean","avg",1))
	if strpos("`v'","summer")>0 {
		local labpost = " summer"
	}
	else if strpos("`v'","winter")>0 {
		local labpost = " winter"
	}
	else {
		local labpost = ""
	}
	la var `v2' "`labpre' SP-spec marg elec price (log $/kWh), lagged`labpost'"
}

** Construct instruments of lagged default electricity prices
foreach v of varlist log*default* {
	local vpos1 = strpos("`v'","_")+1
	local vpos2 = strpos("`v'","p_kwh")
	local vpre = substr("`v'",`vpos1',`vpos2'-`vpos1'-1)
	local v2 = subinstr(subinstr(subinstr("`v'","_`vpre'","",1),"kwh","`vpre'",1),"ag_default","deflag",1)
	gen `v2' = L.`v'
	local labpre = proper(subinstr("`vpre'","mean","avg",1))
	if strpos("`v'","summer")>0 {
		local labpost = " summer"
	}
	else if strpos("`v'","winter")>0 {
		local labpost = " winter"
	}
	else {
		local labpost = ""
	}
	la var `v2' "`labpre' SP-spec default marg elec price (log $/kWh), lagged`labpost'"
}

** Construct instruments of lagged modal electricity prices
foreach v of varlist log*modal* {
	local vpos1 = strpos("`v'","_")+1
	local vpos2 = strpos("`v'","p_kwh")
	local vpre = substr("`v'",`vpos1',`vpos2'-`vpos1'-1)
	local v2 = subinstr(subinstr(subinstr("`v'","_`vpre'","",1),"kwh","`vpre'",1),"ag_modal","modlag",1)
	gen `v2' = L.`v'
	local labpre = proper(subinstr("`vpre'","mean","avg",1))
	if strpos("`v'","summer")>0 {
		local labpost = " summer"
	}
	else if strpos("`v'","winter")>0 {
		local labpost = " winter"
	}
	else {
		local labpost = ""
	}
	la var `v2' "`labpre' SP-spec modal marg elec price (log $/kWh), lagged`labpost'"
}

** Apply inverse hyperbolic sine tranformation to AF_water 
foreach v of varlist af* {
	local v2 = subinstr("`v'","af_","ihs_af_",1)
	gen `v2' = ln(10000*`v' + sqrt((10000*`v')^2+1))
	replace `v2' = . if ann_bill_kwh<0
	local vlab: variable label `v'
	local v2lab = subinstr("`vlab'","AF water","IHS 1e4*AF water",1)
	la var `v2' "`v2lab'"
}

** Apply log, log+1 tranformations to AF_water 
foreach v of varlist af_rast_dd_mth_2SP* {
	local vlab: variable label `v'

	local v2 = subinstr(subinstr(subinstr("`v'","af_","log1_10000af_",1),"summer","su",1),"winter","wi",1)
	gen `v2' = ln(10000*`v'+1)
	replace `v2' = . if `v'<0
	local v2lab = subinstr("`vlab'","AF water","Log+1 10000*AF water",1)
	la var `v2' "`v2lab'"

	local v2 = subinstr(subinstr(subinstr("`v'","af_","log1_100af_",1),"summer","su",1),"winter","wi",1)
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

** Log-transform water price composite variables
foreach v of varlist mean_p_af_* {
	local v2 = subinstr("ln_`v'","mean","mn",1)
	gen `v2' = ln(`v') // I actually want the zeros to become missings
	local vlab: variable label `v'
	la var `v2' "Log `vlab'"
}

** Log-transform kwhaf variables
foreach v of varlist kwhaf* {
	local v2 = subinstr(subinstr("ln_`v'","summer","su",1),"winter","wi",1)
	gen `v2' = ln(`v') // I actually want the zeros to become missings
	local vlab: variable label `v'
	la var `v2' "Log `vlab'"
}

** Log-transform mean gw depth variables
foreach v of varlist gw_mean_* {
	gen ln_`v' = ln(`v') // I actually want the zeros to become missings
	local vlab: variable label `v'
	la var ln_`v' "Log `vlab'"
}

** Lag depth instrument(s)
tsset sp_group year
foreach v of varlist *gw_mean_depth_mth_2SP* {
	local v2 = subinstr(subinstr("L_`v'","summer","su",1),"winter","wi",1)
	gen `v2' = L.`v'
	la var `v2' "Lag of `v'"
}

** Save
order sp_uuid year
sort sp_uuid year
unique sp_uuid year
assert r(unique)==r(N)
compress
save "$dirpath_data/merged/sp_annual_water_panel.dta", replace

}

*****************************************************************************
*****************************************************************************

** 6. CLU-by-year water regression dataset
if 1==0{

** Start with monthly water dataset
use "$dirpath_data/merged/sp_month_water_panel.dta", clear

** Keep only SP-months that have a CLU ID
keep if clu_id!=.

** Collapse to CLU-year level
duplicates report clu_id year
gen winter = 1-summer
gen days = .
replace days = 31 if inlist(month,1,3,5,7,8,10,12)
replace days = 30 if inlist(month,4,6,9,11)
replace days = 29 if month==2 & inlist(year,2008,2012,2016)
replace days = 28 if month==2 & !inlist(year,2008,2012,2016)

	// Sum kwh and bill amounts for year and for summer/winter
foreach v of varlist mnth* {
	foreach s of varlist summer winter {
		local v2 = subinstr("`v'","mnth","`s'",1)
		egen `v2' = sum(`v'*`s'), by(clu_id year)
		local vlab1: variable label `v'
		local vlab2 = subinstr("`vlab1'","monthified","`s'",1)
		la var `v2' "`vlab2'"
	}
	local v2 = subinstr("`v'","mnth","ann",1)
	egen `v2' = sum(`v'), by(clu_id year)
	local vlab1: variable label `v'
	local vlab2 = subinstr("`vlab1'","monthified","annual",1)
	la var `v2' "`vlab2'"
	drop `v'
}

	// Take mean of electricity prices for year and for summer/winter
foreach v of varlist mean_p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(clu_id year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(clu_id year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take min of electricity prices for year and for summer/winter
foreach v of varlist min_p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = min(temp), by(clu_id year)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = min(`v'), by(clu_id year)
	replace `v' = temp
	drop temp
}

	// Take max of electricity prices for year and for summer/winter
foreach v of varlist max_p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = max(temp), by(clu_id year)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = max(`v'), by(clu_id year)
	replace `v' = temp
	drop temp
}

	// Take mean of tiered electricity prices for year and for summer/winter
foreach v of varlist p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "mean_`v'_`s'"
		egen `v2' = wtmean(temp), by(clu_id year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "Avg daily `vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	local v2 = "mean_`v'"
	egen `v2' = wtmean(`v'), by(clu_id year) weight(days)
	local vlab1: variable label `v'
	local vlab2 = "Avg daily `vlab1'"
	la var `v2' "`vlab2'"
	drop `v'
}

	// Take mean of demand charges for year and for summer/winter
foreach v of varlist mean_p_kw_* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(clu_id year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(clu_id year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take mean of kwh/af for year and summer/winter
foreach v of varlist kwhaf* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(clu_id year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(clu_id year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take mean of groundwater depth for year and summer/winter
foreach v of varlist gw*depth* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(clu_id year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(clu_id year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take mean, min, and max of distance to gw measurement for year and summer/winter
foreach v of varlist gw*dist* {
	local v_mean = subinstr("`v'","dist","distmean",1)
	local v_min = subinstr("`v'","dist","distmin",1)
	local v_max = subinstr("`v'","dist","distmax",1)
	egen `v_mean' = wtmean(`v'), by(clu_id year) weight(days)
	egen `v_min' = min(`v'), by(clu_id year)
	egen `v_max' = max(`v'), by(clu_id year)
	local vlab1: variable label `v'
	local vlab2 = subinstr(lower("`vlab1'"),"measurement","meas",1)
	if strpos("`v'","mth")>0 {
		local vtime = "mth"
	}
	else if strpos("`v'","qtr")>0 {
		local vtime = "qtr"
	}
	local vlab_mean = "Mean (over SP-`vtime's) of `vlab2'"
	local vlab_min = "Min (over SP-`vtime's) of `vlab2'"
	local vlab_max = "Max (over SP-`vtime's) of `vlab2'"
	la var `v_mean' "`vlab_mean'"
	la var `v_min' "`vlab_min'"
	la var `v_max' "`vlab_max'"
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2_mean = "`v_mean'_`s'"
		local v2_min = "`v_min'_`s'"
		local v2_max = "`v_max'_`s'"
		egen `v2_mean' = wtmean(temp), by(clu_id year) weight(days)
		egen `v2_min' = min(temp), by(clu_id year)
		egen `v2_max' = max(temp), by(clu_id year)
		local vlab2_mean = "`vlab_mean', `s'"
		local vlab2_min = "`vlab_min', `s'"
		local vlab2_max = "`vlab_max', `s'"
		la var `v2_mean' "`vlab2_mean'"
		la var `v2_min' "`vlab2_min'"
		la var `v2_max' "`vlab2_max'"
		drop temp
	}
	drop `v'
}

	// Take mean, min, and max of gw measurement counts for year and summer/winter
foreach v of varlist gw*cnt* {
	local v_mean = subinstr("`v'","cnt","cntmean",1)
	local v_min = subinstr("`v'","cnt","cntmin",1)
	local v_max = subinstr("`v'","cnt","cntmax",1)
	egen `v_mean' = wtmean(`v'), by(clu_id year) weight(days)
	egen `v_min' = min(`v'), by(clu_id year)
	egen `v_max' = max(`v'), by(clu_id year)
	local vlab1: variable label `v'
	local vlab2 = subinstr(subinstr(subinstr(lower("`vlab1'"),"measurements","meas",1),"questionable","ques",1),"observation","obs",1)
	local vlab_mean = "Mean `vlab2'"
	local vlab_min = "Min `vlab2'"
	local vlab_max = "Max `vlab2'"
	la var `v_mean' "`vlab_mean'"
	la var `v_min' "`vlab_min'"
	la var `v_max' "`vlab_max'"
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2_mean = "`v_mean'_`s'"
		local v2_min = "`v_min'_`s'"
		local v2_max = "`v_max'_`s'"
		egen `v2_mean' = wtmean(temp), by(clu_id year) weight(days)
		egen `v2_min' = min(temp), by(clu_id year)
		egen `v2_max' = max(temp), by(clu_id year)
		local vlab2_mean = "`vlab_mean', `s'"
		local vlab2_min = "`vlab_min', `s'"
		local vlab2_max = "`vlab_max', `s'"
		la var `v2_mean' "`vlab2_mean'"
		la var `v2_min' "`vlab2_min'"
		la var `v2_max' "`vlab2_max'"
		drop temp
	}
	drop `v'
}

	// Sum acre-feet of water for year and for summer/winter
foreach v of varlist af* {
	foreach s of varlist summer winter {
		local v2 = "`v'_`s'"
		egen `v2' = sum(`v'*`s'), by(clu_id year)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
	}
	egen temp = sum(`v'), by(clu_id year)
	replace `v' = temp
	drop temp
}

	// Take mean of water prices for year and for summer/winter
foreach v of varlist mean_p_af* {
	foreach s of varlist summer winter {
		local s2 = substr("`s'",1,2)
		gen temp = `v' if `s'
		local v2 = "`v'_`s2'"
		egen `v2' = wtmean(temp), by(clu_id year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(clu_id year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take max of flag variables
foreach v of varlist *flag* {
	egen temp = max(`v'), by(clu_id year)
	replace `v' = temp
	drop temp
}

	// Create flags for important switches within a year
foreach v of varlist rt_sched_cd rt_category rt_large_ag hp_bin_dec kw_bin_dec ope_bin_dec {
	egen temp_tag = tag(`v' sp_group year), missing
	egen temp_sp = sum(temp_tag), by(sp_group year)
	gen temp_flag = (temp_sp>1)
	local v2 = "flag_`v'_switch"
	egen `v2' = max(temp_flag), by(clu_id year)
	drop temp*
}
la var flag_rt_sched_cd_switch "Flag indicating SP in CLU switched rate within year"
la var flag_rt_category_switch "Flag indicating SP in CLU switched rate category within year"
la var flag_rt_large_ag_switch "Flag indicating SP in CLU switched rate group within year"
la var flag_hp_bin_dec_switch "Flag indicating SP in CLU switched horsepower decile within year"
la var flag_kw_bin_dec_switch "Flag indicating SP in CLU switched killowatt decile within year"
la var flag_ope_bin_dec_switch "Flag indicating SP in CLU switched OPE decile within year"

	// Create a flag for SP-years with < 12 months
egen temp = count(modate), by(sp_group year)
gen temp_flag = (temp<12)
egen flag_partial_year = max(temp_flag), by(clu_id year)
drop temp*
la var flag_partial_year "Flag indicating a CLU-year includes an SP-year with <12 months of bills"
replace flag_irregular_bill = max(flag_irregular_bill,flag_partial_year)

	// Take mode of string rate variables
foreach v of varlist rt_sched_cd rt_default rt_modal rt_sched_cd_init {
	egen temp = mode(`v'), by(clu_id year)
	replace `v' = temp
	local vlab1: variable label `v'
	local vlab2 = "`vlab1', mode of SP-months in CLU-year"
	la var `v' "`vlab2'"
	drop temp
}

	// Take mode of numeric rate and bin variables
foreach v of varlist rt_group rt_category rt_large_ag hp* kw* ope* {
	egen temp = mode(`v'), maxmode by(clu_id year)
	replace `v' = temp
	drop temp
	local vlab1: variable label `v'
	local vlab2 = "`vlab1', mode of SP-months in CLU-year"
	la var `v' "`vlab2'"
}

	// Take mode of group variables
foreach v of varlist wdist_group county_group basin_group cz_group {
	egen temp = mode(`v'), maxmode by(clu_id year)
	replace `v' = temp
	drop temp
	local vlab1: variable label `v'
	local vlab2 = "`vlab1', mode of SP-months in CLU-year"
	la var `v' "`vlab2'"
}

	// Take min of rate indicator variables
foreach v of varlist sp_same_rate* {
	egen temp = min(`v'), by(clu_id year)
	replace `v' = temp
	drop temp
	local vlab1: variable label `v'
	local vlab2 = subinstr("`vlab1'","SPs","all SPs in CLU",1)
	la var `v' "`vlab2'"
}

	// Take mode of drawdown prediction variables
egen temp = mode(drwdwn_predict_step), maxmode by(clu_id year)
gen temp_desc = drwdwn_predict_step_desc if temp==drwdwn_predict_step
replace drwdwn_predict_step = temp
egen temp_mode = mode(temp_desc), by(clu_id year)
replace drwdwn_predict_step_desc = temp_mode
local vlab1: variable label drwdwn_predict_step
local vlab2 = "`vlab1', mode of SP-months in CLU-year"
la var drwdwn_predict_step "`vlab2'"
local vlab1: variable label drwdwn_predict_step_desc
local vlab2 = "`vlab1', mode of SP-months in CLU-year"
la var drwdwn_predict_step_desc "`vlab2'"
drop temp*

	// Take max of other indicator variables
foreach v of varlist in_calif in_pge in_pou in_interval net_mtr_ind dr_ind elec_binary {
	egen temp = max(`v'), by(clu_id year)
	replace `v' = temp
	drop temp
}

	// Calculate fraction of CLU-months with electricity consumption
egen temp_mo = max(elec_binary), by(clu_id modate)
egen temp_mean = mean(temp_mo), by(clu_id)
replace elec_binary_frac = temp_mean
la var elec_binary_frac "Fraction of CLU's months with kwh>0"
drop temp*

	// Take mean of temperatures for year and for summer/winter
foreach v of varlist degreesC* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(clu_id year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(clu_id year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take mean of other numeric variables
foreach v of varlist interval_bill_corr {
	egen temp = wtmean(`v'), by(clu_id year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take max of EE measures
egen temp_tag = tag(sp_group clu_id)
egen temp_count = max(ee_measure_count), by(sp_group)
gen temp_prod = temp_tag*temp_count
egen temp_clu = sum(temp_prod), by(clu_id)
replace ee_measure_count = temp_clu
drop temp*

	// Take max of APEP project indicators
foreach v of varlist post_apep_proj_finish extrap* {
	egen temp = max(`v'), by(clu_id year)
	replace `v' = temp
	drop temp
}

	// Take min of APEP project finish date
egen temp = min(date_proj_finish), by(clu_id year)
replace date_proj_finish = temp
la var date_proj_finish "Date of first project finsihed in CLU"
drop temp

	// Take mode of other APEP indicators and IDs
foreach v of varlist apep_interp_case apeptestid* {
	egen temp = mode(`v'), maxmode by(clu_id year)
	replace `v' = temp
	drop temp
	local vlab1: variable label `v'
	local vlab2 = "`vlab1', mode of SP-months in CLU-year"
	la var `v' "`vlab2'"
}

	// Take sum of APEP projects
egen temp_tag = tag(sp_group clu_id)
gen temp_count = temp_tag*apep_proj_count
egen temp_clu = sum(temp_count), by(clu_id)
replace apep_proj_count = temp_clu
drop temp*

	// Count SPs in CLU and CLU-year
egen temp_clu = tag(sp_group clu_id)
egen spcount_clu = sum(temp_clu), by(clu_id)
egen temp_cy = tag(sp_group clu_id year)
egen spcount_clu_year = sum(temp_cy), by(clu_id year)
la var spcount_clu "Number of SPs in CLU over all years"
la var spcount_clu_year "Number of SPs in CLU in a year"
drop temp*

	// Create indicator for set of SPs within a CLU for each year
preserve
keep sp_uuid clu_id year
duplicates drop
sort clu_id year sp_uuid
egen clu_year_group = group(clu_id year)
bysort clu_year_group (sp_uuid) : gen clu_year_n = _n
reshape wide sp_uuid, i(clu_id year) j(clu_year_n)
egen clu_sp_group = group(clu_id sp_uuid*), missing
keep clu_id year clu_sp_group
tempfile clu_sp_group
save `clu_sp_group'
restore
merge m:1 clu_id year using `clu_sp_group'
assert _merge==3
drop _merge
la var clu_sp_group "Identifier for group of SPs comprising the CLU"

	// Drop remaining monthly variables
drop modate month summer winter days ctrl_fxn* log* ln* ihs* L12* L6* gw_qtr_bsn* ///
	 sp_uuid sp_group prem* sa_sp* *dt* parcelid_conc* latlon* pump* ///
	 spcount_parcelid_conc* *clu*APEP* months* *test_modate*

	// Collapse
duplicates drop
unique clu_id year
assert r(unique)==r(N)

** Inverse hyperbolic sine and log transform electricity quantity
foreach v of varlist *bill_kwh {
	if strpos("`v'","summer")>0 {
		local vpost = "_summer"
	}
	else if strpos("`v'","winter")>0 {
		local vpost = "_winter"
	}
	else {
		local vpost = ""
	}
	local v_ihs = "ihs_kwh`vpost'"
	gen `v_ihs' = ln(100*`v' + sqrt((100*`v')^2+1))
	replace `v_ihs' = . if `v'<0
	local v_log = "log_kwh`vpost'"
	gen `v_log' = ln(`v')
	local v_log1 = "log1_kwh`vpost'"
	gen `v_log1' = ln(`v'+1)
	replace `v_log1' = . if `v'<0
	local v_log1_100 = "log1_100kwh`vpost'"
	gen `v_log1_100' = ln(100*`v'+1)
	replace `v_log1_100' = . if `v'<0
	local vlab_mid = subinstr("`vpost'","_"," ",1)
	local vlab_ihs = "Inverse hyperbolic sine of 100*kWh`vlab_mid' elec consumption"
	local vlab_log = "Log of kWh`vlab_mid' elec consumption"
	local vlab_log1 = "Log+1 of kWh`vlab_mid' elec consumption"
	local vlab_log1_100 = "Log+1 of 100*kWh`vlab_mid' elec consumption"
	la var `v_ihs' "`vlab_ihs'"
	la var `v_log' "`vlab_log'"
	la var `v_log1' "`vlab_log1'"
	la var `v_log1_100' "`vlab_log1_100'"
}

** Log-transform marginal electricity prices
foreach v of varlist mean_p_kwh min_p_kwh max_p_kwh {
	foreach vsuf in "" "_summer" "_winter" {
		local v2 = "`v'`vsuf'"
		local vpre = substr("`v2'",1,strpos("`v2'","_")-1)
		local v3 = subinstr(subinstr("`v2'","`vpre'","log",1),"kwh","`vpre'",1)
		gen `v3' = ln(`v2')
		local lab: var label `v2'
		la var `v3' "Log `lab'"
	}
}

** Log-transform electricity prices to be used as instruments
foreach v of varlist *p_kwh_e1* *p_kwh_e20* *p_kwh_ag_default* *p_kwh_ag_modal* *p_kwh_init* {
	gen log_`v' = ln(`v')
	local lab: var label `v'
	la var log_`v' "Log `lab'"
}	

** Construct instruments of lagged electricity prices
tsset clu_id year
foreach v of varlist log_p* {
	local vpre = subinstr(subinstr(substr("`v'",7,.),"_summer","",1),"_winter","",1)
	local v2 = subinstr("`v'","`vpre'","`vpre'_lag",1)
	gen `v2' = L.`v'
	local labpre = proper(subinstr("`vpre'","mean","avg",1))
	if strpos("`v'","summer")>0 {
		local labpost = " summer"
	}
	else if strpos("`v'","winter")>0 {
		local labpost = " winter"
	}
	else {
		local labpost = ""
	}
	la var `v2' "`labpre' CLU-spec marg elec price (log $/kWh), lagged`labpost'"
}

** Construct instruments of lagged default electricity prices
foreach v of varlist log*default* {
	local vpos1 = strpos("`v'","_")+1
	local vpos2 = strpos("`v'","p_kwh")
	local vpre = substr("`v'",`vpos1',`vpos2'-`vpos1'-1)
	local v2 = subinstr(subinstr(subinstr("`v'","_`vpre'","",1),"kwh","`vpre'",1),"ag_default","deflag",1)
	gen `v2' = L.`v'
	local labpre = proper(subinstr("`vpre'","mean","avg",1))
	if strpos("`v'","summer")>0 {
		local labpost = " summer"
	}
	else if strpos("`v'","winter")>0 {
		local labpost = " winter"
	}
	else {
		local labpost = ""
	}
	la var `v2' "`labpre' CLU-spec default marg elec price (log $/kWh), lagged`labpost'"
}

** Construct instruments of lagged modal electricity prices
foreach v of varlist log*modal* {
	local vpos1 = strpos("`v'","_")+1
	local vpos2 = strpos("`v'","p_kwh")
	local vpre = substr("`v'",`vpos1',`vpos2'-`vpos1'-1)
	local v2 = subinstr(subinstr(subinstr("`v'","_`vpre'","",1),"kwh","`vpre'",1),"ag_modal","modlag",1)
	gen `v2' = L.`v'
	local labpre = proper(subinstr("`vpre'","mean","avg",1))
	if strpos("`v'","summer")>0 {
		local labpost = " summer"
	}
	else if strpos("`v'","winter")>0 {
		local labpost = " winter"
	}
	else {
		local labpost = ""
	}
	la var `v2' "`labpre' CLU-spec modal marg elec price (log $/kWh), lagged`labpost'"
}

** Apply inverse hyperbolic sine tranformation to AF_water 
foreach v of varlist af* {
	local v2 = subinstr("`v'","af_","ihs_af_",1)
	gen `v2' = ln(10000*`v' + sqrt((10000*`v')^2+1))
	replace `v2' = . if ann_bill_kwh<0
	local vlab: variable label `v'
	local v2lab = subinstr("`vlab'","AF water","IHS 1e4*AF water",1)
	la var `v2' "`v2lab'"
}

** Apply log, log+1 tranformations to AF_water 
foreach v of varlist af_rast_dd_mth_2SP* {
	local vlab: variable label `v'

	local v2 = subinstr(subinstr(subinstr("`v'","af_","log1_10000af_",1),"summer","su",1),"winter","wi",1)
	gen `v2' = ln(10000*`v'+1)
	replace `v2' = . if `v'<0
	local v2lab = subinstr("`vlab'","AF water","Log+1 10000*AF water",1)
	la var `v2' "`v2lab'"

	local v2 = subinstr(subinstr(subinstr("`v'","af_","log1_100af_",1),"summer","su",1),"winter","wi",1)
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

** Log-transform water price composite variables
foreach v of varlist mean_p_af_* {
	local v2 = subinstr("ln_`v'","mean","mn",1)
	gen `v2' = ln(`v') // I actually want the zeros to become missings
	local vlab: variable label `v'
	la var `v2' "Log `vlab'"
}

** Log-transform kwhaf variables
foreach v of varlist kwhaf* {
	local v2 = subinstr(subinstr("ln_`v'","summer","su",1),"winter","wi",1)
	gen `v2' = ln(`v') // I actually want the zeros to become missings
	local vlab: variable label `v'
	la var `v2' "Log `vlab'"
}

** Log-transform mean gw depth variables
foreach v of varlist gw_mean_* {
	gen ln_`v' = ln(`v') // I actually want the zeros to become missings
	local vlab: variable label `v'
	la var ln_`v' "Log `vlab'"
}

** Lag depth instrument(s)
tsset clu_id year
foreach v of varlist *gw_mean_depth_mth_2SP* {
	local v2 = subinstr(subinstr("L_`v'","summer","su",1),"winter","wi",1)
	gen `v2' = L.`v'
	la var `v2' "Lag of `v'"
}

** Save
order clu_id year
sort clu_id year
unique clu_id year
assert r(unique)==r(N)
compress
save "$dirpath_data/merged/clu_annual_water_panel.dta", replace

}

*****************************************************************************
*****************************************************************************

** 7. Parcel-by-year water regression dataset
if 1==1{

** Start with monthly water dataset
use "$dirpath_data/merged/sp_month_water_panel.dta", clear

** Keep only SP-months that have a parcel ID
keep if parcelid_conc!=.

** Collapse to parcel-year level
duplicates report parcelid_conc year
gen winter = 1-summer
gen days = .
replace days = 31 if inlist(month,1,3,5,7,8,10,12)
replace days = 30 if inlist(month,4,6,9,11)
replace days = 29 if month==2 & inlist(year,2008,2012,2016)
replace days = 28 if month==2 & !inlist(year,2008,2012,2016)

	// Sum kwh and bill amounts for year and for summer/winter
foreach v of varlist mnth* {
	foreach s of varlist summer winter {
		local v2 = subinstr("`v'","mnth","`s'",1)
		egen `v2' = sum(`v'*`s'), by(parcelid_conc year)
		local vlab1: variable label `v'
		local vlab2 = subinstr("`vlab1'","monthified","`s'",1)
		la var `v2' "`vlab2'"
	}
	local v2 = subinstr("`v'","mnth","ann",1)
	egen `v2' = sum(`v'), by(parcelid_conc year)
	local vlab1: variable label `v'
	local vlab2 = subinstr("`vlab1'","monthified","annual",1)
	la var `v2' "`vlab2'"
	drop `v'
}

	// Take mean of electricity prices for year and for summer/winter
foreach v of varlist mean_p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(parcelid_conc year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(parcelid_conc year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take min of electricity prices for year and for summer/winter
foreach v of varlist min_p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = min(temp), by(parcelid_conc year)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = min(`v'), by(parcelid_conc year)
	replace `v' = temp
	drop temp
}

	// Take max of electricity prices for year and for summer/winter
foreach v of varlist max_p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = max(temp), by(parcelid_conc year)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = max(`v'), by(parcelid_conc year)
	replace `v' = temp
	drop temp
}

	// Take mean of tiered electricity prices for year and for summer/winter
foreach v of varlist p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "mean_`v'_`s'"
		egen `v2' = wtmean(temp), by(parcelid_conc year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "Avg daily `vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	local v2 = "mean_`v'"
	egen `v2' = wtmean(`v'), by(parcelid_conc year) weight(days)
	local vlab1: variable label `v'
	local vlab2 = "Avg daily `vlab1'"
	la var `v2' "`vlab2'"
	drop `v'
}

	// Take mean of demand charges for year and for summer/winter
foreach v of varlist mean_p_kw_* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(parcelid_conc year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(parcelid_conc year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take mean of kwh/af for year and summer/winter
foreach v of varlist kwhaf* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(parcelid_conc year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(parcelid_conc year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take mean of groundwater depth for year and summer/winter
foreach v of varlist gw*depth* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(parcelid_conc year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(parcelid_conc year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take mean, min, and max of distance to gw measurement for year and summer/winter
foreach v of varlist gw*dist* {
	local v_mean = subinstr("`v'","dist","distmean",1)
	local v_min = subinstr("`v'","dist","distmin",1)
	local v_max = subinstr("`v'","dist","distmax",1)
	egen `v_mean' = wtmean(`v'), by(parcelid_conc year) weight(days)
	egen `v_min' = min(`v'), by(parcelid_conc year)
	egen `v_max' = max(`v'), by(parcelid_conc year)
	local vlab1: variable label `v'
	local vlab2 = subinstr(lower("`vlab1'"),"measurement","meas",1)
	if strpos("`v'","mth")>0 {
		local vtime = "mth"
	}
	else if strpos("`v'","qtr")>0 {
		local vtime = "qtr"
	}
	local vlab_mean = "Mean (over SP-`vtime's) of `vlab2'"
	local vlab_min = "Min (over SP-`vtime's) of `vlab2'"
	local vlab_max = "Max (over SP-`vtime's) of `vlab2'"
	la var `v_mean' "`vlab_mean'"
	la var `v_min' "`vlab_min'"
	la var `v_max' "`vlab_max'"
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2_mean = "`v_mean'_`s'"
		local v2_min = "`v_min'_`s'"
		local v2_max = "`v_max'_`s'"
		egen `v2_mean' = wtmean(temp), by(parcelid_conc year) weight(days)
		egen `v2_min' = min(temp), by(parcelid_conc year)
		egen `v2_max' = max(temp), by(parcelid_conc year)
		local vlab2_mean = "`vlab_mean', `s'"
		local vlab2_min = "`vlab_min', `s'"
		local vlab2_max = "`vlab_max', `s'"
		la var `v2_mean' "`vlab2_mean'"
		la var `v2_min' "`vlab2_min'"
		la var `v2_max' "`vlab2_max'"
		drop temp
	}
	drop `v'
}

	// Take mean, min, and max of gw measurement counts for year and summer/winter
foreach v of varlist gw*cnt* {
	local v_mean = subinstr("`v'","cnt","cntmean",1)
	local v_min = subinstr("`v'","cnt","cntmin",1)
	local v_max = subinstr("`v'","cnt","cntmax",1)
	egen `v_mean' = wtmean(`v'), by(parcelid_conc year) weight(days)
	egen `v_min' = min(`v'), by(parcelid_conc year)
	egen `v_max' = max(`v'), by(parcelid_conc year)
	local vlab1: variable label `v'
	local vlab2 = subinstr(subinstr(subinstr(lower("`vlab1'"),"measurements","meas",1),"questionable","ques",1),"observation","obs",1)
	local vlab_mean = "Mean `vlab2'"
	local vlab_min = "Min `vlab2'"
	local vlab_max = "Max `vlab2'"
	la var `v_mean' "`vlab_mean'"
	la var `v_min' "`vlab_min'"
	la var `v_max' "`vlab_max'"
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2_mean = "`v_mean'_`s'"
		local v2_min = "`v_min'_`s'"
		local v2_max = "`v_max'_`s'"
		egen `v2_mean' = wtmean(temp), by(parcelid_conc year) weight(days)
		egen `v2_min' = min(temp), by(parcelid_conc year)
		egen `v2_max' = max(temp), by(parcelid_conc year)
		local vlab2_mean = "`vlab_mean', `s'"
		local vlab2_min = "`vlab_min', `s'"
		local vlab2_max = "`vlab_max', `s'"
		la var `v2_mean' "`vlab2_mean'"
		la var `v2_min' "`vlab2_min'"
		la var `v2_max' "`vlab2_max'"
		drop temp
	}
	drop `v'
}

	// Sum acre-feet of water for year and for summer/winter
foreach v of varlist af* {
	foreach s of varlist summer winter {
		local v2 = "`v'_`s'"
		egen `v2' = sum(`v'*`s'), by(parcelid_conc year)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
	}
	egen temp = sum(`v'), by(parcelid_conc year)
	replace `v' = temp
	drop temp
}

	// Take mean of water prices for year and for summer/winter
foreach v of varlist mean_p_af* {
	foreach s of varlist summer winter {
		local s2 = substr("`s'",1,2)
		gen temp = `v' if `s'
		local v2 = "`v'_`s2'"
		egen `v2' = wtmean(temp), by(parcelid_conc year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(parcelid_conc year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take max of flag variables
foreach v of varlist *flag* {
	egen temp = max(`v'), by(parcelid_conc year)
	replace `v' = temp
	drop temp
}

	// Create flags for important switches within a year
foreach v of varlist rt_sched_cd rt_category rt_large_ag hp_bin_dec kw_bin_dec ope_bin_dec {
	egen temp_tag = tag(`v' sp_group year), missing
	egen temp_sp = sum(temp_tag), by(sp_group year)
	gen temp_flag = (temp_sp>1)
	local v2 = "flag_`v'_switch"
	egen `v2' = max(temp_flag), by(parcelid_conc year)
	drop temp*
}
la var flag_rt_sched_cd_switch "Flag indicating SP in parcel switched rate within year"
la var flag_rt_category_switch "Flag indicating SP in parcel switched rate category within year"
la var flag_rt_large_ag_switch "Flag indicating SP in parcel switched rate group within year"
la var flag_hp_bin_dec_switch "Flag indicating SP in parcel switched horsepower decile within year"
la var flag_kw_bin_dec_switch "Flag indicating SP in parcel switched killowatt decile within year"
la var flag_ope_bin_dec_switch "Flag indicating SP in parcel switched OPE decile within year"

	// Create a flag for SP-years with < 12 months
egen temp = count(modate), by(sp_group year)
gen temp_flag = (temp<12)
egen flag_partial_year = max(temp_flag), by(parcelid_conc year)
drop temp*
la var flag_partial_year "Flag indicating a parcel-year includes an SP-year with <12 months of bills"
replace flag_irregular_bill = max(flag_irregular_bill,flag_partial_year)

	// Take mode of string rate variables
foreach v of varlist rt_sched_cd rt_default rt_modal rt_sched_cd_init {
	egen temp = mode(`v'), by(parcelid_conc year)
	replace `v' = temp
	local vlab1: variable label `v'
	local vlab2 = "`vlab1', mode of SP-months in parcel-year"
	la var `v' "`vlab2'"
	drop temp
}

	// Take mode of numeric rate and bin variables
foreach v of varlist rt_group rt_category rt_large_ag hp* kw* ope* {
	egen temp = mode(`v'), maxmode by(parcelid_conc year)
	replace `v' = temp
	drop temp
	local vlab1: variable label `v'
	local vlab2 = "`vlab1', mode of SP-months in parcel-year"
	la var `v' "`vlab2'"
}

	// Take mode of group variables
foreach v of varlist wdist_group county_group basin_group cz_group {
	egen temp = mode(`v'), maxmode by(parcelid_conc year)
	replace `v' = temp
	drop temp
	local vlab1: variable label `v'
	local vlab2 = "`vlab1', mode of SP-months in parcel-year"
	la var `v' "`vlab2'"
}

	// Take mode of county
egen temp = mode(county), maxmode by(parcelid_conc year)
replace county = temp
drop temp

	// Take min of rate indicator variables
foreach v of varlist sp_same_rate* {
	egen temp = min(`v'), by(parcelid_conc year)
	replace `v' = temp
	drop temp
	local vlab1: variable label `v'
	local vlab2 = subinstr("`vlab1'","SPs","all SPs in parcel",1)
	la var `v' "`vlab2'"
}

	// Take mode of drawdown prediction variables
egen temp = mode(drwdwn_predict_step), maxmode by(parcelid_conc year)
gen temp_desc = drwdwn_predict_step_desc if temp==drwdwn_predict_step
replace drwdwn_predict_step = temp
egen temp_mode = mode(temp_desc), by(parcelid_conc year)
replace drwdwn_predict_step_desc = temp_mode
local vlab1: variable label drwdwn_predict_step
local vlab2 = "`vlab1', mode of SP-months in parcel-year"
la var drwdwn_predict_step "`vlab2'"
local vlab1: variable label drwdwn_predict_step_desc
local vlab2 = "`vlab1', mode of SP-months in parcel-year"
la var drwdwn_predict_step_desc "`vlab2'"
drop temp*

	// Take max of other indicator variables
foreach v of varlist in_calif in_pge in_pou in_interval net_mtr_ind dr_ind elec_binary {
	egen temp = max(`v'), by(parcelid_conc year)
	replace `v' = temp
	drop temp
}

	// Calculate fraction of parcel-months with electricity consumption
egen temp_mo = max(elec_binary), by(parcelid_conc modate)
egen temp_mean = mean(temp_mo), by(parcelid_conc)
replace elec_binary_frac = temp_mean
la var elec_binary_frac "Fraction of parcel's months with kwh>0"
drop temp*

	// Take mean of temperatures for year and for summer/winter
foreach v of varlist degreesC* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(parcelid_conc year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(parcelid_conc year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take mean of other numeric variables
foreach v of varlist interval_bill_corr {
	egen temp = wtmean(`v'), by(parcelid_conc year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take max of EE measures
egen temp_tag = tag(sp_group parcelid_conc)
egen temp_count = max(ee_measure_count), by(sp_group)
gen temp_prod = temp_tag*temp_count
egen temp_parc = sum(temp_prod), by(parcelid_conc)
replace ee_measure_count = temp_parc
drop temp*

	// Take max of APEP project indicators
foreach v of varlist post_apep_proj_finish extrap* {
	egen temp = max(`v'), by(parcelid_conc year)
	replace `v' = temp
	drop temp
}

	// Take min of APEP project finish date
egen temp = min(date_proj_finish), by(parcelid_conc year)
replace date_proj_finish = temp
la var date_proj_finish "Date of first project finsihed in parcel"
drop temp

	// Take mode of other APEP indicators and IDs
foreach v of varlist apep_interp_case apeptestid* {
	egen temp = mode(`v'), maxmode by(parcelid_conc year)
	replace `v' = temp
	drop temp
	local vlab1: variable label `v'
	local vlab2 = "`vlab1', mode of SP-months in parcel-year"
	la var `v' "`vlab2'"
}

	// Take sum of APEP projects
egen temp_tag = tag(sp_group parcelid_conc)
gen temp_count = temp_tag*apep_proj_count
egen temp_parc = sum(temp_count), by(parcelid_conc)
replace apep_proj_count = temp_parc
drop temp*

	// Count SPs in parcel and parcel-year
egen temp_parc = tag(sp_group parcelid_conc)
egen spcount_parcel = sum(temp_parc), by(parcelid_conc)
egen temp_py = tag(sp_group parcelid_conc year)
egen spcount_parcel_year = sum(temp_py), by(parcelid_conc year)
la var spcount_parcel "Number of SPs in parcel over all years"
la var spcount_parcel_year "Number of SPs in parcel in a year"
drop temp*

	// Create indicator for set of SPs within a parcel for each year
preserve
keep sp_uuid parcelid_conc year
duplicates drop
sort parcelid_conc year sp_uuid
egen parc_year_group = group(parcelid_conc year)
bysort parc_year_group (sp_uuid) : gen parc_year_n = _n
reshape wide sp_uuid, i(parcelid_conc year) j(parc_year_n)
egen parcel_sp_group = group(parcelid_conc sp_uuid*), missing
keep parcelid_conc year parcel_sp_group
tempfile parcel_sp_group
save `parcel_sp_group'
restore
merge m:1 parcelid_conc year using `parcel_sp_group'
assert _merge==3
drop _merge
la var parcel_sp_group "Identifier for group of SPs comprising the parcel"

	// Drop CLU-specific variables
drop clu* crop* frac* ever* acres* mode* 

	// Drop remaining monthly variables
drop modate month summer winter days ctrl_fxn* log* ln* ihs* L12* L6* gw_qtr_bsn* ///
	 sp_uuid sp_group prem* sa_sp* *dt* parcelid_concAPEP latlon* pump* ///
	 spcount_parcelid_conc* *clu*APEP* months* *test_modate*

	// Collapse
duplicates drop
unique parcelid_conc year
assert r(unique)==r(N)

** Inverse hyperbolic sine and log transform electricity quantity
foreach v of varlist *bill_kwh {
	if strpos("`v'","summer")>0 {
		local vpost = "_summer"
	}
	else if strpos("`v'","winter")>0 {
		local vpost = "_winter"
	}
	else {
		local vpost = ""
	}
	local v_ihs = "ihs_kwh`vpost'"
	gen `v_ihs' = ln(100*`v' + sqrt((100*`v')^2+1))
	replace `v_ihs' = . if `v'<0
	local v_log = "log_kwh`vpost'"
	gen `v_log' = ln(`v')
	local v_log1 = "log1_kwh`vpost'"
	gen `v_log1' = ln(`v'+1)
	replace `v_log1' = . if `v'<0
	local v_log1_100 = "log1_100kwh`vpost'"
	gen `v_log1_100' = ln(100*`v'+1)
	replace `v_log1_100' = . if `v'<0
	local vlab_mid = subinstr("`vpost'","_"," ",1)
	local vlab_ihs = "Inverse hyperbolic sine of 100*kWh`vlab_mid' elec consumption"
	local vlab_log = "Log of kWh`vlab_mid' elec consumption"
	local vlab_log1 = "Log+1 of kWh`vlab_mid' elec consumption"
	local vlab_log1_100 = "Log+1 of 100*kWh`vlab_mid' elec consumption"
	la var `v_ihs' "`vlab_ihs'"
	la var `v_log' "`vlab_log'"
	la var `v_log1' "`vlab_log1'"
	la var `v_log1_100' "`vlab_log1_100'"
}

** Log-transform marginal electricity prices
foreach v of varlist mean_p_kwh min_p_kwh max_p_kwh {
	foreach vsuf in "" "_summer" "_winter" {
		local v2 = "`v'`vsuf'"
		local vpre = substr("`v2'",1,strpos("`v2'","_")-1)
		local v3 = subinstr(subinstr("`v2'","`vpre'","log",1),"kwh","`vpre'",1)
		gen `v3' = ln(`v2')
		local lab: var label `v2'
		la var `v3' "Log `lab'"
	}
}

** Log-transform electricity prices to be used as instruments
foreach v of varlist *p_kwh_e1* *p_kwh_e20* *p_kwh_ag_default* *p_kwh_ag_modal* *p_kwh_init* {
	gen log_`v' = ln(`v')
	local lab: var label `v'
	la var log_`v' "Log `lab'"
}	

** Construct instruments of lagged electricity prices
tsset parcelid_conc year
foreach v of varlist log_p* {
	local vpre = subinstr(subinstr(substr("`v'",7,.),"_summer","",1),"_winter","",1)
	local v2 = subinstr("`v'","`vpre'","`vpre'_lag",1)
	gen `v2' = L.`v'
	local labpre = proper(subinstr("`vpre'","mean","avg",1))
	if strpos("`v'","summer")>0 {
		local labpost = " summer"
	}
	else if strpos("`v'","winter")>0 {
		local labpost = " winter"
	}
	else {
		local labpost = ""
	}
	la var `v2' "`labpre' parcel-spec marg elec price (log $/kWh), lagged`labpost'"
}

** Construct instruments of lagged default electricity prices
foreach v of varlist log*default* {
	local vpos1 = strpos("`v'","_")+1
	local vpos2 = strpos("`v'","p_kwh")
	local vpre = substr("`v'",`vpos1',`vpos2'-`vpos1'-1)
	local v2 = subinstr(subinstr(subinstr("`v'","_`vpre'","",1),"kwh","`vpre'",1),"ag_default","deflag",1)
	gen `v2' = L.`v'
	local labpre = proper(subinstr("`vpre'","mean","avg",1))
	if strpos("`v'","summer")>0 {
		local labpost = " summer"
	}
	else if strpos("`v'","winter")>0 {
		local labpost = " winter"
	}
	else {
		local labpost = ""
	}
	la var `v2' "`labpre' parcel-spec default marg elec price (log $/kWh), lagged`labpost'"
}

** Construct instruments of lagged modal electricity prices
foreach v of varlist log*modal* {
	local vpos1 = strpos("`v'","_")+1
	local vpos2 = strpos("`v'","p_kwh")
	local vpre = substr("`v'",`vpos1',`vpos2'-`vpos1'-1)
	local v2 = subinstr(subinstr(subinstr("`v'","_`vpre'","",1),"kwh","`vpre'",1),"ag_modal","modlag",1)
	gen `v2' = L.`v'
	local labpre = proper(subinstr("`vpre'","mean","avg",1))
	if strpos("`v'","summer")>0 {
		local labpost = " summer"
	}
	else if strpos("`v'","winter")>0 {
		local labpost = " winter"
	}
	else {
		local labpost = ""
	}
	la var `v2' "`labpre' parcel-spec modal marg elec price (log $/kWh), lagged`labpost'"
}

** Apply inverse hyperbolic sine tranformation to AF_water 
foreach v of varlist af* {
	local v2 = subinstr("`v'","af_","ihs_af_",1)
	gen `v2' = ln(10000*`v' + sqrt((10000*`v')^2+1))
	replace `v2' = . if ann_bill_kwh<0
	local vlab: variable label `v'
	local v2lab = subinstr("`vlab'","AF water","IHS 1e4*AF water",1)
	la var `v2' "`v2lab'"
}

** Apply log, log+1 tranformations to AF_water 
foreach v of varlist af_rast_dd_mth_2SP* {
	local vlab: variable label `v'

	local v2 = subinstr(subinstr(subinstr("`v'","af_","log1_10000af_",1),"summer","su",1),"winter","wi",1)
	gen `v2' = ln(10000*`v'+1)
	replace `v2' = . if `v'<0
	local v2lab = subinstr("`vlab'","AF water","Log+1 10000*AF water",1)
	la var `v2' "`v2lab'"

	local v2 = subinstr(subinstr(subinstr("`v'","af_","log1_100af_",1),"summer","su",1),"winter","wi",1)
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

** Log-transform water price composite variables
foreach v of varlist mean_p_af_* {
	local v2 = subinstr("ln_`v'","mean","mn",1)
	gen `v2' = ln(`v') // I actually want the zeros to become missings
	local vlab: variable label `v'
	la var `v2' "Log `vlab'"
}

** Log-transform kwhaf variables
foreach v of varlist kwhaf* {
	local v2 = subinstr(subinstr("ln_`v'","summer","su",1),"winter","wi",1)
	gen `v2' = ln(`v') // I actually want the zeros to become missings
	local vlab: variable label `v'
	la var `v2' "Log `vlab'"
}

** Log-transform mean gw depth variables
foreach v of varlist gw_mean_* {
	gen ln_`v' = ln(`v') // I actually want the zeros to become missings
	local vlab: variable label `v'
	la var ln_`v' "Log `vlab'"
}

** Lag depth instrument(s)
tsset parcelid_conc year
foreach v of varlist *gw_mean_depth_mth_2SP* {
	local v2 = subinstr(subinstr("L_`v'","summer","su",1),"winter","wi",1)
	gen `v2' = L.`v'
	la var `v2' "Lag of `v'"
}

** Save
order parcelid_conc year
sort parcelid_conc year
unique parcelid_conc year
assert r(unique)==r(N)
compress
save "$dirpath_data/merged/parcel_annual_water_panel.dta", replace

}

*****************************************************************************
*****************************************************************************

** 8. CLU75-by-year water regression dataset
if 1==0{

** Start with monthly water dataset
use "$dirpath_data/merged/sp_month_water_panel.dta", clear

** Keep only SP-months that have a CLU group75 ID
keep if clu_group75!=.

** Collapse to CLU75-year level
duplicates report clu_group75 year
gen winter = 1-summer
gen days = .
replace days = 31 if inlist(month,1,3,5,7,8,10,12)
replace days = 30 if inlist(month,4,6,9,11)
replace days = 29 if month==2 & inlist(year,2008,2012,2016)
replace days = 28 if month==2 & !inlist(year,2008,2012,2016)

	// Sum kwh and bill amounts for year and for summer/winter
foreach v of varlist mnth* {
	foreach s of varlist summer winter {
		local v2 = subinstr("`v'","mnth","`s'",1)
		egen `v2' = sum(`v'*`s'), by(clu_group75 year)
		local vlab1: variable label `v'
		local vlab2 = subinstr("`vlab1'","monthified","`s'",1)
		la var `v2' "`vlab2'"
	}
	local v2 = subinstr("`v'","mnth","ann",1)
	egen `v2' = sum(`v'), by(clu_group75 year)
	local vlab1: variable label `v'
	local vlab2 = subinstr("`vlab1'","monthified","annual",1)
	la var `v2' "`vlab2'"
	drop `v'
}

	// Take mean of electricity prices for year and for summer/winter
foreach v of varlist mean_p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(clu_group75 year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(clu_group75 year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take min of electricity prices for year and for summer/winter
foreach v of varlist min_p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = min(temp), by(clu_group75 year)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = min(`v'), by(clu_group75 year)
	replace `v' = temp
	drop temp
}

	// Take max of electricity prices for year and for summer/winter
foreach v of varlist max_p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = max(temp), by(clu_group75 year)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = max(`v'), by(clu_group75 year)
	replace `v' = temp
	drop temp
}

	// Take mean of tiered electricity prices for year and for summer/winter
foreach v of varlist p_kwh* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "mean_`v'_`s'"
		egen `v2' = wtmean(temp), by(clu_group75 year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "Avg daily `vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	local v2 = "mean_`v'"
	egen `v2' = wtmean(`v'), by(clu_group75 year) weight(days)
	local vlab1: variable label `v'
	local vlab2 = "Avg daily `vlab1'"
	la var `v2' "`vlab2'"
	drop `v'
}

	// Take mean of demand charges for year and for summer/winter
foreach v of varlist mean_p_kw_* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(clu_group75 year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(clu_group75 year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take mean of kwh/af for year and summer/winter
foreach v of varlist kwhaf* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(clu_group75 year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(clu_group75 year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take mean of groundwater depth for year and summer/winter
foreach v of varlist gw*depth* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(clu_group75 year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(clu_group75 year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take mean, min, and max of distance to gw measurement for year and summer/winter
foreach v of varlist gw*dist* {
	local v_mean = subinstr("`v'","dist","distmean",1)
	local v_min = subinstr("`v'","dist","distmin",1)
	local v_max = subinstr("`v'","dist","distmax",1)
	egen `v_mean' = wtmean(`v'), by(clu_group75 year) weight(days)
	egen `v_min' = min(`v'), by(clu_group75 year)
	egen `v_max' = max(`v'), by(clu_group75 year)
	local vlab1: variable label `v'
	local vlab2 = subinstr(lower("`vlab1'"),"measurement","meas",1)
	if strpos("`v'","mth")>0 {
		local vtime = "mth"
	}
	else if strpos("`v'","qtr")>0 {
		local vtime = "qtr"
	}
	local vlab_mean = "Mean (over SP-`vtime's) of `vlab2'"
	local vlab_min = "Min (over SP-`vtime's) of `vlab2'"
	local vlab_max = "Max (over SP-`vtime's) of `vlab2'"
	la var `v_mean' "`vlab_mean'"
	la var `v_min' "`vlab_min'"
	la var `v_max' "`vlab_max'"
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2_mean = "`v_mean'_`s'"
		local v2_min = "`v_min'_`s'"
		local v2_max = "`v_max'_`s'"
		egen `v2_mean' = wtmean(temp), by(clu_group75 year) weight(days)
		egen `v2_min' = min(temp), by(clu_group75 year)
		egen `v2_max' = max(temp), by(clu_group75 year)
		local vlab2_mean = "`vlab_mean', `s'"
		local vlab2_min = "`vlab_min', `s'"
		local vlab2_max = "`vlab_max', `s'"
		la var `v2_mean' "`vlab2_mean'"
		la var `v2_min' "`vlab2_min'"
		la var `v2_max' "`vlab2_max'"
		drop temp
	}
	drop `v'
}

	// Take mean, min, and max of gw measurement counts for year and summer/winter
foreach v of varlist gw*cnt* {
	local v_mean = subinstr("`v'","cnt","cntmean",1)
	local v_min = subinstr("`v'","cnt","cntmin",1)
	local v_max = subinstr("`v'","cnt","cntmax",1)
	egen `v_mean' = wtmean(`v'), by(clu_group75 year) weight(days)
	egen `v_min' = min(`v'), by(clu_group75 year)
	egen `v_max' = max(`v'), by(clu_group75 year)
	local vlab1: variable label `v'
	local vlab2 = subinstr(subinstr(subinstr(lower("`vlab1'"),"measurements","meas",1),"questionable","ques",1),"observation","obs",1)
	local vlab_mean = "Mean `vlab2'"
	local vlab_min = "Min `vlab2'"
	local vlab_max = "Max `vlab2'"
	la var `v_mean' "`vlab_mean'"
	la var `v_min' "`vlab_min'"
	la var `v_max' "`vlab_max'"
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2_mean = "`v_mean'_`s'"
		local v2_min = "`v_min'_`s'"
		local v2_max = "`v_max'_`s'"
		egen `v2_mean' = wtmean(temp), by(clu_group75 year) weight(days)
		egen `v2_min' = min(temp), by(clu_group75 year)
		egen `v2_max' = max(temp), by(clu_group75 year)
		local vlab2_mean = "`vlab_mean', `s'"
		local vlab2_min = "`vlab_min', `s'"
		local vlab2_max = "`vlab_max', `s'"
		la var `v2_mean' "`vlab2_mean'"
		la var `v2_min' "`vlab2_min'"
		la var `v2_max' "`vlab2_max'"
		drop temp
	}
	drop `v'
}

	// Sum acre-feet of water for year and for summer/winter
foreach v of varlist af* {
	foreach s of varlist summer winter {
		local v2 = "`v'_`s'"
		egen `v2' = sum(`v'*`s'), by(clu_group75 year)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
	}
	egen temp = sum(`v'), by(clu_group75 year)
	replace `v' = temp
	drop temp
}

	// Take mean of water prices for year and for summer/winter
foreach v of varlist mean_p_af* {
	foreach s of varlist summer winter {
		local s2 = substr("`s'",1,2)
		gen temp = `v' if `s'
		local v2 = "`v'_`s2'"
		egen `v2' = wtmean(temp), by(clu_group75 year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(clu_group75 year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take max of flag variables
foreach v of varlist *flag* {
	egen temp = max(`v'), by(clu_group75 year)
	replace `v' = temp
	drop temp
}

	// Create flags for important switches within a year
foreach v of varlist rt_sched_cd rt_category rt_large_ag hp_bin_dec kw_bin_dec ope_bin_dec {
	egen temp_tag = tag(`v' sp_group year), missing
	egen temp_sp = sum(temp_tag), by(sp_group year)
	gen temp_flag = (temp_sp>1)
	local v2 = "flag_`v'_switch"
	egen `v2' = max(temp_flag), by(clu_group75 year)
	drop temp*
}
la var flag_rt_sched_cd_switch "Flag indicating SP in CLU75 switched rate within year"
la var flag_rt_category_switch "Flag indicating SP in CLU75 switched rate category within year"
la var flag_rt_large_ag_switch "Flag indicating SP in CLU75 switched rate group within year"
la var flag_hp_bin_dec_switch "Flag indicating SP in CLU75 switched horsepower decile within year"
la var flag_kw_bin_dec_switch "Flag indicating SP in CLU75 switched killowatt decile within year"
la var flag_ope_bin_dec_switch "Flag indicating SP in CLU75 switched OPE decile within year"

	// Create a flag for SP-years with < 12 months
egen temp = count(modate), by(sp_group year)
gen temp_flag = (temp<12)
egen flag_partial_year = max(temp_flag), by(clu_group75 year)
drop temp*
la var flag_partial_year "Flag indicating a CLU75-year includes an SP-year with <12 months of bills"
replace flag_irregular_bill = max(flag_irregular_bill,flag_partial_year)

	// Take mode of string rate variables
foreach v of varlist rt_sched_cd rt_default rt_modal rt_sched_cd_init {
	egen temp = mode(`v'), by(clu_group75 year)
	replace `v' = temp
	local vlab1: variable label `v'
	local vlab2 = "`vlab1', mode of SP-months in CLU75-year"
	la var `v' "`vlab2'"
	drop temp
}

	// Take mode of numeric rate and bin variables
foreach v of varlist rt_group rt_category rt_large_ag hp* kw* ope* {
	egen temp = mode(`v'), maxmode by(clu_group75 year)
	replace `v' = temp
	drop temp
	local vlab1: variable label `v'
	local vlab2 = "`vlab1', mode of SP-months in CLU75-year"
	la var `v' "`vlab2'"
}

	// Take mode of group variables
foreach v of varlist wdist_group county_group basin_group cz_group {
	egen temp = mode(`v'), maxmode by(clu_group75 year)
	replace `v' = temp
	drop temp
	local vlab1: variable label `v'
	local vlab2 = "`vlab1', mode of SP-months in CLU75-year"
	la var `v' "`vlab2'"
}

	// Take min of rate indicator variables
foreach v of varlist sp_same_rate* {
	egen temp = min(`v'), by(clu_group75 year)
	replace `v' = temp
	drop temp
	local vlab1: variable label `v'
	local vlab2 = subinstr("`vlab1'","SPs","all SPs in CLU75",1)
	la var `v' "`vlab2'"
}

	// Take mode of drawdown prediction variables
egen temp = mode(drwdwn_predict_step), maxmode by(clu_group75 year)
gen temp_desc = drwdwn_predict_step_desc if temp==drwdwn_predict_step
replace drwdwn_predict_step = temp
egen temp_mode = mode(temp_desc), by(clu_group75 year)
replace drwdwn_predict_step_desc = temp_mode
local vlab1: variable label drwdwn_predict_step
local vlab2 = "`vlab1', mode of SP-months in CLU75-year"
la var drwdwn_predict_step "`vlab2'"
local vlab1: variable label drwdwn_predict_step_desc
local vlab2 = "`vlab1', mode of SP-months in CLU75-year"
la var drwdwn_predict_step_desc "`vlab2'"
drop temp*

	// Take max of other indicator variables
foreach v of varlist in_calif in_pge in_pou in_interval net_mtr_ind dr_ind elec_binary {
	egen temp = max(`v'), by(clu_group75 year)
	replace `v' = temp
	drop temp
}

	// Calculate fraction of CLU75-months with electricity consumption
egen temp_mo = max(elec_binary), by(clu_group75 modate)
egen temp_mean = mean(temp_mo), by(clu_group75)
replace elec_binary_frac = temp_mean
la var elec_binary_frac "Fraction of CLU75's months with kwh>0"
drop temp*

	// Take mean of temperatures for year and for summer/winter
foreach v of varlist degreesC* {
	foreach s of varlist summer winter {
		gen temp = `v' if `s'
		local v2 = "`v'_`s'"
		egen `v2' = wtmean(temp), by(clu_group75 year) weight(days)
		local vlab1: variable label `v'
		local vlab2 = "`vlab1', `s'"
		la var `v2' "`vlab2'"
		drop temp
	}
	egen temp = wtmean(`v'), by(clu_group75 year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take mean of other numeric variables
foreach v of varlist interval_bill_corr {
	egen temp = wtmean(`v'), by(clu_group75 year) weight(days)
	replace `v' = temp
	drop temp
}

	// Take max of EE measures
egen temp_tag = tag(sp_group clu_group75)
egen temp_count = max(ee_measure_count), by(sp_group)
gen temp_prod = temp_tag*temp_count
egen temp_clu = sum(temp_prod), by(clu_group75)
replace ee_measure_count = temp_clu
drop temp*

	// Take max of APEP project indicators
foreach v of varlist post_apep_proj_finish extrap* {
	egen temp = max(`v'), by(clu_group75 year)
	replace `v' = temp
	drop temp
}

	// Take min of APEP project finish date
egen temp = min(date_proj_finish), by(clu_group75 year)
replace date_proj_finish = temp
la var date_proj_finish "Date of first project finsihed in CLU75"
drop temp

	// Take mode of other APEP indicators and IDs
foreach v of varlist apep_interp_case apeptestid* {
	egen temp = mode(`v'), maxmode by(clu_group75 year)
	replace `v' = temp
	drop temp
	local vlab1: variable label `v'
	local vlab2 = "`vlab1', mode of SP-months in CLU75-year"
	la var `v' "`vlab2'"
}

	// Take sum of APEP projects
egen temp_tag = tag(sp_group clu_group75)
gen temp_count = temp_tag*apep_proj_count
egen temp_clu = sum(temp_count), by(clu_group75)
replace apep_proj_count = temp_clu
drop temp*

	// Count SPs in CLU75 and CLU75-year
egen temp_clu = tag(sp_group clu_group75)
egen spcount_clu75 = sum(temp_clu), by(clu_group75)
egen temp_cy = tag(sp_group clu_group75 year)
egen spcount_clu75_year = sum(temp_cy), by(clu_group75 year)
la var spcount_clu75 "Number of SPs in CLU75 over all years"
la var spcount_clu75_year "Number of SPs in CLU75 in a year"
drop temp*

	// Create indicator for set of SPs within a CLU75 for each year
preserve
keep sp_uuid clu_group75 year
duplicates drop
sort clu_group75 year sp_uuid
egen clu_year_group = group(clu_group75 year)
bysort clu_year_group (sp_uuid) : gen clu_year_n = _n
reshape wide sp_uuid, i(clu_group75 year) j(clu_year_n)
egen clu75_sp_group = group(clu_group75 sp_uuid*), missing
keep clu_group75 year clu75_sp_group
tempfile clu75_sp_group
save `clu75_sp_group'
restore
merge m:1 clu_group75 year using `clu75_sp_group'
assert _merge==3
drop _merge
la var clu75_sp_group "Identifier for group of SPs comprising the CLU75"

	// Drop CLU-specific variables
drop clu_id clu_group0 clu_group10 clu_group25 clu_group50 cluacres crop* frac* ever* acres* mode* 

	// Drop remaining monthly variables
drop modate month summer winter days ctrl_fxn* log* ln* ihs* L12* L6* gw_qtr_bsn* ///
	 sp_uuid sp_group prem* sa_sp* *dt* parcelid_conc* latlon* pump* ///
	 spcount_parcelid_conc* *clu*APEP* months* *test_modate*

	// Collapse
duplicates drop
unique clu_group75 year
assert r(unique)==r(N)

** Inverse hyperbolic sine and log transform electricity quantity
foreach v of varlist *bill_kwh {
	if strpos("`v'","summer")>0 {
		local vpost = "_summer"
	}
	else if strpos("`v'","winter")>0 {
		local vpost = "_winter"
	}
	else {
		local vpost = ""
	}
	local v_ihs = "ihs_kwh`vpost'"
	gen `v_ihs' = ln(100*`v' + sqrt((100*`v')^2+1))
	replace `v_ihs' = . if `v'<0
	local v_log = "log_kwh`vpost'"
	gen `v_log' = ln(`v')
	local v_log1 = "log1_kwh`vpost'"
	gen `v_log1' = ln(`v'+1)
	replace `v_log1' = . if `v'<0
	local v_log1_100 = "log1_100kwh`vpost'"
	gen `v_log1_100' = ln(100*`v'+1)
	replace `v_log1_100' = . if `v'<0
	local vlab_mid = subinstr("`vpost'","_"," ",1)
	local vlab_ihs = "Inverse hyperbolic sine of 100*kWh`vlab_mid' elec consumption"
	local vlab_log = "Log of kWh`vlab_mid' elec consumption"
	local vlab_log1 = "Log+1 of kWh`vlab_mid' elec consumption"
	local vlab_log1_100 = "Log+1 of 100*kWh`vlab_mid' elec consumption"
	la var `v_ihs' "`vlab_ihs'"
	la var `v_log' "`vlab_log'"
	la var `v_log1' "`vlab_log1'"
	la var `v_log1_100' "`vlab_log1_100'"
}

** Log-transform marginal electricity prices
foreach v of varlist mean_p_kwh min_p_kwh max_p_kwh {
	foreach vsuf in "" "_summer" "_winter" {
		local v2 = "`v'`vsuf'"
		local vpre = substr("`v2'",1,strpos("`v2'","_")-1)
		local v3 = subinstr(subinstr("`v2'","`vpre'","log",1),"kwh","`vpre'",1)
		gen `v3' = ln(`v2')
		local lab: var label `v2'
		la var `v3' "Log `lab'"
	}
}

** Log-transform electricity prices to be used as instruments
foreach v of varlist *p_kwh_e1* *p_kwh_e20* *p_kwh_ag_default* *p_kwh_ag_modal* *p_kwh_init* {
	gen log_`v' = ln(`v')
	local lab: var label `v'
	la var log_`v' "Log `lab'"
}	

** Construct instruments of lagged electricity prices
tsset clu_group75 year
foreach v of varlist log_p* {
	local vpre = subinstr(subinstr(substr("`v'",7,.),"_summer","",1),"_winter","",1)
	local v2 = subinstr("`v'","`vpre'","`vpre'_lag",1)
	gen `v2' = L.`v'
	local labpre = proper(subinstr("`vpre'","mean","avg",1))
	if strpos("`v'","summer")>0 {
		local labpost = " summer"
	}
	else if strpos("`v'","winter")>0 {
		local labpost = " winter"
	}
	else {
		local labpost = ""
	}
	la var `v2' "`labpre' CLU75-spec marg elec price (log $/kWh), lagged`labpost'"
}

** Construct instruments of lagged default electricity prices
foreach v of varlist log*default* {
	local vpos1 = strpos("`v'","_")+1
	local vpos2 = strpos("`v'","p_kwh")
	local vpre = substr("`v'",`vpos1',`vpos2'-`vpos1'-1)
	local v2 = subinstr(subinstr(subinstr("`v'","_`vpre'","",1),"kwh","`vpre'",1),"ag_default","deflag",1)
	gen `v2' = L.`v'
	local labpre = proper(subinstr("`vpre'","mean","avg",1))
	if strpos("`v'","summer")>0 {
		local labpost = " summer"
	}
	else if strpos("`v'","winter")>0 {
		local labpost = " winter"
	}
	else {
		local labpost = ""
	}
	la var `v2' "`labpre' CLU75-spec default marg elec price (log $/kWh), lagged`labpost'"
}

** Construct instruments of lagged modal electricity prices
foreach v of varlist log*modal* {
	local vpos1 = strpos("`v'","_")+1
	local vpos2 = strpos("`v'","p_kwh")
	local vpre = substr("`v'",`vpos1',`vpos2'-`vpos1'-1)
	local v2 = subinstr(subinstr(subinstr("`v'","_`vpre'","",1),"kwh","`vpre'",1),"ag_modal","modlag",1)
	gen `v2' = L.`v'
	local labpre = proper(subinstr("`vpre'","mean","avg",1))
	if strpos("`v'","summer")>0 {
		local labpost = " summer"
	}
	else if strpos("`v'","winter")>0 {
		local labpost = " winter"
	}
	else {
		local labpost = ""
	}
	la var `v2' "`labpre' CLU75-spec modal marg elec price (log $/kWh), lagged`labpost'"
}

** Apply inverse hyperbolic sine tranformation to AF_water 
foreach v of varlist af* {
	local v2 = subinstr("`v'","af_","ihs_af_",1)
	gen `v2' = ln(10000*`v' + sqrt((10000*`v')^2+1))
	replace `v2' = . if ann_bill_kwh<0
	local vlab: variable label `v'
	local v2lab = subinstr("`vlab'","AF water","IHS 1e4*AF water",1)
	la var `v2' "`v2lab'"
}

** Apply log, log+1 tranformations to AF_water 
foreach v of varlist af_rast_dd_mth_2SP* {
	local vlab: variable label `v'

	local v2 = subinstr(subinstr(subinstr("`v'","af_","log1_10000af_",1),"summer","su",1),"winter","wi",1)
	gen `v2' = ln(10000*`v'+1)
	replace `v2' = . if `v'<0
	local v2lab = subinstr("`vlab'","AF water","Log+1 10000*AF water",1)
	la var `v2' "`v2lab'"

	local v2 = subinstr(subinstr(subinstr("`v'","af_","log1_100af_",1),"summer","su",1),"winter","wi",1)
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

** Log-transform water price composite variables
foreach v of varlist mean_p_af_* {
	local v2 = subinstr("ln_`v'","mean","mn",1)
	gen `v2' = ln(`v') // I actually want the zeros to become missings
	local vlab: variable label `v'
	la var `v2' "Log `vlab'"
}

** Log-transform kwhaf variables
foreach v of varlist kwhaf* {
	local v2 = subinstr(subinstr("ln_`v'","summer","su",1),"winter","wi",1)
	gen `v2' = ln(`v') // I actually want the zeros to become missings
	local vlab: variable label `v'
	la var `v2' "Log `vlab'"
}

** Log-transform mean gw depth variables
foreach v of varlist gw_mean_* {
	gen ln_`v' = ln(`v') // I actually want the zeros to become missings
	local vlab: variable label `v'
	la var ln_`v' "Log `vlab'"
}

** Lag depth instrument(s)
tsset clu_group75 year
foreach v of varlist *gw_mean_depth_mth_2SP* {
	local v2 = subinstr(subinstr("L_`v'","summer","su",1),"winter","wi",1)
	gen `v2' = L.`v'
	la var `v2' "Lag of `v'"
}

** Merge crop data
drop county
rename clu_group75 clu_group75_encode
decode clu_group75_encode, gen(clu_group75)
merge 1:1 clu_group75 year using "$dirpath_data/cleaned_spatial/CDL_panel_clugroup75_bigcat_year_wide.dta", nogen keep(3)
drop clu_group75
rename clu_group75_encode clu_group75

** Save
order clu_group75 year
sort clu_group75 year
unique clu_group75 year
assert r(unique)==r(N)
compress
save "$dirpath_data/merged/clu75_annual_water_panel.dta", replace

}

*****************************************************************************
*****************************************************************************
