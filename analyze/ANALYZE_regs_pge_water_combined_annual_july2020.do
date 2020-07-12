clear all
version 13
set more off

*************************************************************************************************
** Script to run main set of annual PGE water (combined) regressions for July 2020 paper draft **
*************************************************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

************************************************
************************************************

** 1. Annual regressions by SP (main set of 5 regressions, for 24 outcome+sample combinations)
{ 

// Load monthly panel
use "$dirpath_data/merged_pge/sp_annual_water_panel.dta", clear
local panel = "annual (sp)"

// Drop SP-years without a full year of data (< 12 months of bill data)
keep if flag_partial_year==0

// Define switcher/stayer flags
assert mode50_switcher==0 if ever50_Annual + ever50_FruitNutPerennial + ever50_Noncrop + ever50_OtherPerennial ==1
gen mode50_switcher_Ann_Perenn = ever50_Annual==1 & inlist(ever50_FruitNutPerennial+ever50_OtherPerennial,1,2)
gen mode50_switcher_Crop_NoCrop = ever50_Noncrop==1 & inlist(ever50_Annual+ever50_FruitNutPerennial+ever50_OtherPerennial,1,2,3)
gen mode50_switcher_Ann_NoCrop = ever50_Annual==1 & ever50_Noncrop==1 
gen mode50_switcher_Perenn_NoCrop = ever50_Noncrop==1 & inlist(ever50_FruitNutPerennial+ever50_OtherPerennial,1,2) 

// Fix inconsistencies in ever50_crop flags
foreach v of varlist ever50_* {
	local vm = subinstr("`v'","ever50_","mode50_",1)
	egen temp = max(`vm'), by(sp_group)
	replace `v' = temp
	drop temp
}

// Always annual or fallow
egen always50_Noncrop = min(mode50_Noncrop), by(sp_group)

egen always50_Annual = min(mode50_Annual), by(sp_group)
egen always50_FruitNutPerennial = min(mode50_FruitNutPerennial), by(sp_group)
egen always50_OtherPerennial = min(mode50_OtherPerennial), by(sp_group)
egen always50_Perennial = min(mode50_FruitNutPerennial + mode50_OtherPerennial), by(sp_group)
egen always50_SameType = rowmax(always50_Annual always50_FruitNutPerennial always50_OtherPerennial)

egen always50_Annual_NC = min(mode50_Annual + mode50_Noncrop), by(sp_group)
egen always50_FruitNutPerennial_NC = min(mode50_FruitNutPerennial + mode50_Noncrop), by(sp_group)
egen always50_OtherPerennial_NC = min(mode50_OtherPerennial + mode50_Noncrop), by(sp_group)
egen always50_Perennial_NC = min(mode50_FruitNutPerennial + mode50_OtherPerennial + mode50_Noncrop), by(sp_group)
egen always50_SameType_NC = rowmax(always50_Annual_NC always50_FruitNutPerennial_NC always50_OtherPerennial_NC)
replace always50_SameType_NC = 0 if always50_SameType==1

egen always50_Crop = min(mode50_Annual + mode50_FruitNutPerennial + mode50_OtherPerennial), by(sp_group)
assert mode50_Noncrop!=1 if always50_Crop
tab always50_Crop if always50_Annual==1 | always50_FruitNutPerennial==1 | always50_OtherPerennial==1
tab always50_Crop if always50_Annual==0 & always50_FruitNutPerennial==0 & always50_OtherPerennial==0
gen always50_Crop_Switcher = always50_Crop==1 & always50_Annual==0 & always50_FruitNutPerennial==0 & always50_OtherPerennial==0

gen ever50_Fallower = ever50_Noncrop==1 & (ever50_Annual+ever50_FruitNutPerennial+ever50_OtherPerennial>0) & (ever50_Annual+ever50_FruitNutPerennial+ever50_OtherPerennial!=.)
gen always50_Switcher_Fallower = ever50_Noncrop==1 & always50_Noncrop==0 & always50_Annual==0 & always50_FruitNutPerennial==0 & always50_OtherPerennial==0
replace always50_Switcher_Fallower = 0 if always50_SameType_NC==1
sum always50_SameType always50_SameType_NC always50_Crop_Switcher always50_Switcher_Fallower

unique sp_group // 12049 total
unique sp_group if always50_SameType // 4697
unique sp_group if always50_SameType_NC // 3373
unique sp_group if always50_Crop_Switcher // 1575 
unique sp_group if always50_Switcher_Fallower // 2042

gen temp = always50_SameType+always50_SameType_NC+always50_Crop_Switcher+always50_Switcher_Fallower
unique sp_group if temp==0
assert inlist(temp,0,1,.)
unique sp_group if temp==0 // 844
unique sp_group if temp==. // 482

br sp_group year mode50* if temp==0

egen never50_Annual = max(mode50_Annual), by(sp_group)
egen never50_FruitNutPerennial = max(mode50_FruitNutPerennial), by(sp_group)
egen never50_OtherPerennial = max(mode50_OtherPerennial), by(sp_group)
replace never50_Annual = 1 - never50_Annual
replace never50_FruitNutPerennial = 1 - never50_FruitNutPerennial
replace never50_OtherPerennial = 1 - never50_OtherPerennial
unique sp_group if ever50_Annual==1 & never50_FruitNutPerennial==1 & never50_OtherPerennial==1 & temp==0
unique sp_group if never50_Annual==1 & ever50_FruitNutPerennial==1 & never50_OtherPerennial==1 & temp==0
unique sp_group if never50_Annual==1 & never50_FruitNutPerennial==1 & ever50_OtherPerennial==1 & temp==0
drop temp

// Log average groundwater prioes
foreach v of varlist mean_p_af_* {
	cap gen ln_`v' = ln(`v')
}

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
	
// Define baseline sample criteria (common to all regressions)
local ifs_base = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
	
// Confirm that by forcing a match to the water panel, we've forced out the two non-APEP-specific PGE data pullspulls
local pull = "PGE 20180719" 
cap drop pull

// Define cluster variables
egen cty_yr = group(county_group year)
unique cty_yr
local VCE = "sp_group cty_yr"	

// Population missing groups for group-wise FEs, to avoid dropping them when we don't want to
replace wdist_group = 0 if wdist_group==.

	
// Create empty variables to populate for storing results
gen panel = ""
gen pull = ""
gen ifs_base = ""
gen ifs_sample = ""
gen depvar = ""
gen fes = ""
gen rhs = ""
gen beta_log_p_water = .
gen se_log_p_water = .
gen t_log_p_water = .
gen vce = ""
gen n_obs = .
gen n_SPs = .
gen n_cty_yrs = .
gen dof = .
gen fstat_rk = .
gen fs_beta_default = .
gen fs_se_default = .
gen fs_t_default = .
*gen fs_beta_deflag12 = .
*gen fs_se_deflag12 = .
*gen fs_t_deflag12 = .
*gen fs_beta_deflag6 = .
*gen fs_se_deflag6 = .
*gen fs_t_deflag6 = .

local row = 0	
	
// Loop over 24 combinations of outcome+sample
foreach c1 of numlist 1/41 {

	if `c1'==1 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = ""
	}
	if `c1'==2 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & log_af_rast_dd_mth_2SP!=."
	}
	if `c1'==3 {
		local DEPVAR = "log_af_rast_dd_mth_2SP"
		local ifs_sample = ""
	}
	if `c1'==4 {
		local DEPVAR = "log1_af_rast_dd_mth_2SP"
		local ifs_sample = ""
	}
	if `c1'==5 {
		local DEPVAR = "log1_100af_rast_dd_mth_2SP"
		local ifs_sample = ""
	}
	if `c1'==6 {
		local DEPVAR = "elec_binary"
		local ifs_sample = ""
	}
	if `c1'==7 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & elec_binary_frac>0.9"
	}
	if `c1'==8 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & ever50_Annual==1"
	}
	if `c1'==9 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & ever50_FruitNutPerennial==1"
	}
	if `c1'==10 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & ever50_OtherPerennial==1"
	}
	if `c1'==11 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & (ever50_OtherPerennial==1 | ever50_FruitNutPerennial==1)"
	}
	if `c1'==12 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & ever50_Noncrop==1"
	}
	if `c1'==13 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & mode50_switcher==1"
	}
	if `c1'==14 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & mode50_switcher_Ann_Perenn==1"
	}
	if `c1'==15 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & mode50_switcher_Crop_NoCrop==1"
	}
	if `c1'==16 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & mode50_switcher_Ann_NoCrop==1"
	}
	if `c1'==17 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & mode50_switcher_Perenn_NoCrop==1"
	}
	if `c1'==18 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & mode50_switcher==0"
	}
	if `c1'==19 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & mode50_switcher==0 & ever50_Annual==1"
	}
	if `c1'==20 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & mode50_switcher==0 & ever50_FruitNutPerennial==1"
	}
	if `c1'==21 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & mode50_switcher==0 & ever50_OtherPerennial==1"
	}
	if `c1'==22 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & mode50_switcher==0 & ever50_Noncrop==1"
	}
	if `c1'==23 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & (ever50_OtherPerennial==1 | ever50_FruitNutPerennial==1) & ever50_Noncrop==0 & ever50_Annual==0"
	}
	if `c1'==24 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & (ever50_OtherPerennial==1 | ever50_FruitNutPerennial==1 | ever50_Annual==1) & ever50_Noncrop==0"
	}
	if `c1'==25 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & always50_Noncrop==1"
	}
	if `c1'==26 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & always50_Annual==1"
	}
	if `c1'==27 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & always50_FruitNutPerennial==1"
	}
	if `c1'==28 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & always50_OtherPerennial==1"
	}
	if `c1'==29 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & always50_Perennial==1"
	}
	if `c1'==30 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & always50_Annual_NC==1"
	}
	if `c1'==31 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & always50_FruitNutPerennial_NC==1"
	}
	if `c1'==32 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & always50_OtherPerennial_NC==1"
	}
	if `c1'==33 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & always50_Perennial_NC==1"
	}
	if `c1'==34 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & always50_Crop==1"
	}
	if `c1'==35 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & always50_Crop_switcherP==1"
	}
	if `c1'==36 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & always50_Crop_switcherFNO==1"
	}
	if `c1'==37 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & ever50_Fallower==1"
	}
	if `c1'==38 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & always50_SameType==1"
	}
	if `c1'==39 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & always50_SameType_NC==1"
	}
	if `c1'==40 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & always50_Crop_Switcher==1"
	}
	if `c1'==41 {
		local DEPVAR = "ihs_af_rast_dd_mth_2SP"
		local ifs_sample = " & always50_Switcher_Fallower==1"
	}
	   
	
	local stub = subinstr(subinstr(subinstr(subinstr("`DEPVAR'","ihs_","",1),"log_","",1),"log1_","",1),"100","",1)
	if "`stub'"=="elec_binary" {
		local stub = "af_rast_dd_mth_2SP"
	}

	// Loop over 5 specifications
	foreach c2 of numlist 1/5 {
		
		if `c2'==1 {
			local FEs = "sp_group year"
			local RHS = "ln_mean_p_`stub'"
		}
		if `c2'==2 {
			local FEs = "sp_group year"
			local RHS = "(ln_mean_p_`stub' = log_mean_p_kwh_ag_default)"
		}
		if `c2'==3 {
			local FEs = "sp_group sp_group#rt_large_ag year"
			local RHS = "(ln_mean_p_`stub' = log_mean_p_kwh_ag_default)"
		}
		if `c2'==4 {
			local FEs = "sp_group sp_group#rt_large_ag basin_group#year wdist_group#year"
			local RHS = "(ln_mean_p_`stub' = log_mean_p_kwh_ag_default)"
		}
		if `c2'==5 {
			local FEs = "sp_group sp_group#rt_large_ag year sp_group#c.year"
			local RHS = "(ln_mean_p_`stub' = log_mean_p_kwh_ag_default)"
		}
	
	
		// Set row to store output
		local row = `row' + 1
		
		// Run non-IV specification	
		if substr("`RHS'",1,1)!="(" {
						
			// Run OLS regression
			reghdfe `DEPVAR' `RHS' `ifs_base' `ifs_sample', absorb(`FEs') vce(cluster `VCE')
			
			// Store results
			replace panel = "`panel'" in `row'
			replace pull = "`pull'" in `row'
			replace ifs_base = "`ifs_base'" in `row'
			replace ifs_sample = "`ifs_sample'" in `row'
			replace depvar = "`DEPVAR'" in `row'
			replace fes = "`FEs'" in `row'
			replace rhs = "`RHS'" in `row'
			replace beta_log_p_water = _b[ln_mean_p_`stub'] in `row'
			replace se_log_p_water = _se[ln_mean_p_`stub'] in `row'
			replace t_log_p_water =  _b[ln_mean_p_`stub']/_se[ln_mean_p_`stub'] in `row'
			replace vce = "cluster `VCE'" in `row'
			replace n_obs = e(N) in `row'
			replace n_SPs = e(N_clust1) in `row'
			replace n_cty_yrs = e(N_clust2) in `row'
			replace dof = e(df_r) in `row'
					
		}
		
		// Run IV specifications
		else {
						
			// Run 2SLS regression
			ivreghdfe `DEPVAR' `RHS' `ifs_base' `ifs_sample', absorb(`FEs') cluster(`VCE')
			
			// Store results
			replace panel = "`panel'" in `row'
			replace pull = "`pull'" in `row'
			replace ifs_base = "`ifs_base'" in `row'
			replace ifs_sample = "`ifs_sample'" in `row'
			replace depvar = "`DEPVAR'" in `row'
			replace fes = "`FEs'" in `row'
			replace rhs = "`RHS'" in `row'
			replace beta_log_p_water = _b[ln_mean_p_`stub'] in `row'
			replace se_log_p_water = _se[ln_mean_p_`stub'] in `row'
			replace t_log_p_water =  _b[ln_mean_p_`stub']/_se[ln_mean_p_`stub'] in `row'
			replace vce = "cluster `VCE'" in `row'
			replace n_obs = e(N) in `row'
			replace n_SPs = e(N_clust1) in `row'
			replace n_cty_yrs = e(N_clust2) in `row'
			replace dof = e(df_r) in `row'
			replace fstat_rk = e(rkf) in `row'
					
			// Run first stage regression
			local RHSfs = subinstr(subinstr(subinstr("`RHS'","(","",.),")","",.),"=","",.)
			reghdfe `RHSfs' `ifs_base' `ifs_sample', absorb(`FEs') vce(cluster `VCE')
			
			// Store results
			cap replace fs_beta_default = _b[log_mean_p_kwh_ag_default] in `row'
			cap replace fs_se_default = _se[log_mean_p_kwh_ag_default] in `row'
			cap replace fs_t_default =  _b[log_mean_p_kwh_ag_default]/_se[log_mean_p_kwh_ag_default] in `row'
			cap replace fs_beta_deflag12 = _b[log_p_mean_deflag12] in `row'
			cap replace fs_se_deflag12 = _se[log_p_mean_deflag12] in `row'
			cap replace fs_t_deflag12 =  _b[log_p_mean_deflag12]/_se[log_p_mean_deflag12] in `row'
			cap replace fs_beta_deflag6 = _b[log_p_mean_deflag6] in `row'
			cap replace fs_se_deflag6 = _se[log_p_mean_deflag6] in `row'
			cap replace fs_t_deflag6 =  _b[log_p_mean_deflag6]/_se[log_p_mean_deflag6] in `row'
			
		}
		
		// Save output
		if `c1'==1 & `c2'==1 {
			cap erase "$dirpath_data/results/regs_pge_water_combined_annual_sp_july2020.dta"
		}
		preserve
		keep panel-fs_t_default //fs_t_deflag6
		keep in `row'
		tempfile temp_out
		save `temp_out'
		clear
		cap use "$dirpath_data/results/regs_pge_water_combined_annual_sp_july2020.dta"
		cap append using `temp_out'
		duplicates drop
		compress
		save "$dirpath_data/results/regs_pge_water_combined_annual_sp_july2020.dta", replace
		restore
		
	}
}

}

************************************************
************************************************

