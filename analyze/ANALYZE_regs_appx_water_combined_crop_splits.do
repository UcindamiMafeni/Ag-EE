clear all
version 13
set more off

**************************************************************************************
** Script to run monthly water regressions (combined) -- sensitivities to main spec **
**************************************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

************************************************
************************************************

** 1. Splits by CLU/crop
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
	
// Create empty variables to populate for storign results
gen panel = ""
cap drop pull
gen pull = ""
gen sens = ""
gen sample = ""
gen depvar = ""
gen fes = ""
gen rhs = ""
gen if_sample = ""
gen beta_log_p_water = .
gen se_log_p_water = .
gen t_log_p_water = .
gen vce = ""
gen n_obs = .
gen n_SPs = .
gen n_modates = .
gen dof = .
gen fstat_rk = .
forvalues v = 1/4 {
	gen fs_beta_var`v' = .
	gen fs_se_var`v' = .
	gen fs_t_var`v' = .
}

// Define default global: sample
global if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	
// Define default global: dependent variable
global DEPVAR = "ihs_af_rast_dd_mth_2SP"
	
// Define default global: RHS	
global RHS = "(ln_mean_p_af_rast_dd_mth_2SP = log_mean_p_kwh_ag_default)" 

// Define default global: FEs
global FEs = "sp_group#month sp_group#rt_large_ag modate"
	
// Define default global: cluster variables
global VCE = "sp_group modate"	


// Merge in CLU-by-year wide panel (crops)
decode clu_id_ec, gen(clu_id)
merge m:1 clu_id year using "$dirpath_data/cleaned_spatial/CDL_panel_clu_crop_year_wide.dta"
keep if _merge==3
drop _merge
	
// Loop over sensitivities
forvalues c = 1/30 {

	// Reset default locals
	local if_sample = "${if_sample}"
	local DEPVAR = "${DEPVAR}"
	local RHS = "${RHS}"
	local FEs = "${FEs}"
	local VCE = "${VCE}"
	local sens = ""

	if `c'==1 {
		local sens = "CLU/crop: ever50_Alfalfa"
		local if_sample = "${if_sample} & ever50_Alfalfa==1"
	}
	if `c'==2 {
		local sens = "CLU/crop: ever50_Almonds"
		local if_sample = "${if_sample} & ever50_Almonds==1"
	}
	if `c'==3 {
		local sens = "CLU/crop: ever50_Barley"
		local if_sample = "${if_sample} & ever50_Barley==1"
	}
	if `c'==4 {
		local sens = "CLU/crop: ever50_Corn"
		local if_sample = "${if_sample} & ever50_Corn==1"
	}
	if `c'==5 {
		local sens = "CLU/crop: ever50_Cotton"
		local if_sample = "${if_sample} & ever50_Cotton==1"
	}
	if `c'==6 {
		local sens = "CLU/crop: ever50_Grapes"
		local if_sample = "${if_sample} & ever50_Grapes==1"
	}
	if `c'==7 {
		local sens = "CLU/crop: ever50_Oats"
		local if_sample = "${if_sample} & ever50_Oats==1"
	}
	if `c'==8 {
		local sens = "CLU/crop: ever50_Oranges"
		local if_sample = "${if_sample} & ever50_Oranges==1"
	}
	if `c'==9 {
		local sens = "CLU/crop: ever50_OtherHay"
		local if_sample = "${if_sample} & ever50_OtherHay==1"
	}
	if `c'==10 {
		local sens = "CLU/crop: ever50_Pistachios"
		local if_sample = "${if_sample} & ever50_Pistachios==1"
	}
	if `c'==11 {
		local sens = "CLU/crop: ever50_Rice"
		local if_sample = "${if_sample} & ever50_Rice==1"
	}
	if `c'==12 {
		local sens = "CLU/crop: ever50_Tomatoes"
		local if_sample = "${if_sample} & ever50_Tomatoes==1"
	}
	if `c'==13 {
		local sens = "CLU/crop: ever50_Walnuts"
		local if_sample = "${if_sample} & ever50_Walnuts==1"
	}
	if `c'==14 {
		local sens = "CLU/crop: ever50_Wheat"
		local if_sample = "${if_sample} & ever50_Wheat==1"
	}
	if `c'==15 {
		local sens = "CLU/crop: ever50_count==1"
		local if_sample = "${if_sample} & ever50_count==1"
	}
	if `c'==16 {
		local sens = "CLU/crop: ever50_count>1"
		local if_sample = "${if_sample} & ever50_count>1"
	}
	if `c'==17 {
		local sens = "CLU/crop: acres_Alfalfa>1"
		local if_sample = "${if_sample} & acres_Alfalfa>1"
	}
	if `c'==18 {
		local sens = "CLU/crop: acres_Almonds>1"
		local if_sample = "${if_sample} & acres_Almonds>1"
	}
	if `c'==19 {
		local sens = "CLU/crop: acres_Barley>1"
		local if_sample = "${if_sample} & acres_Barley>1"
	}
	if `c'==20 {
		local sens = "CLU/crop: acres_Corn>1"
		local if_sample = "${if_sample} & acres_Corn>1"
	}
	if `c'==21 {
		local sens = "CLU/crop: acres_Cotton>1"
		local if_sample = "${if_sample} & acres_Cotton>1"
	}
	if `c'==22 {
		local sens = "CLU/crop: acres_Grapes>1"
		local if_sample = "${if_sample} & acres_Grapes>1"
	}
	if `c'==23 {
		local sens = "CLU/crop: acres_Oats>1"
		local if_sample = "${if_sample} & acres_Oats>1"
	}
	if `c'==24 {
		local sens = "CLU/crop: acres_Oranges>1"
		local if_sample = "${if_sample} & acres_Oranges>1"
	}
	if `c'==25 {
		local sens = "CLU/crop: acres_OtherHay>1"
		local if_sample = "${if_sample} & acres_OtherHay>1"
	}
	if `c'==26 {
		local sens = "CLU/crop: acres_Pistachios>1"
		local if_sample = "${if_sample} & acres_Pistachios>1"
	}
	if `c'==27 {
		local sens = "CLU/crop: acres_Rice>1"
		local if_sample = "${if_sample} & acres_Rice>1"
	}
	if `c'==28 {
		local sens = "CLU/crop: acres_Tomatoes>1"
		local if_sample = "${if_sample} & acres_Tomatoes>1"
	}
	if `c'==29 {
		local sens = "CLU/crop: acres_Walnuts>1"
		local if_sample = "${if_sample} & acres_Walnuts>1"
	}
	if `c'==30 {
		local sens = "CLU/crop: acres_WinterWheat>1"
		local if_sample = "${if_sample} & acres_WinterWheat>1"
	}
	
	
	// Run non-IV specification	
	if substr("`RHS'",1,1)!="(" {
					
		// Run OLS regression
		reghdfe `DEPVAR' `RHS' `if_sample', absorb(`FEs') vce(cluster `VCE')
		local p_water_var = subinstr(word("`RHS'",1),"(","",1)
		
		// Store results
		replace panel = "`panel'" in `c'
		replace pull = "`pull'" in `c'
		replace sens = "`sens'" in `c'
		replace sample = "`if_sample'" in `c'
		replace depvar = "`DEPVAR'" in `c'
		replace fes = "`FEs'" in `c'
		replace rhs = "`RHS'" in `c'
		replace if_sample = "`if_sample'" in `c'
		replace beta_log_p_water = _b[`p_water_var'] in `c'
		replace se_log_p_water = _se[`p_water_var'] in `c'
		replace t_log_p_water =  _b[`p_water_var']/_se[`p_water_var'] in `c'
		replace vce = "cluster `VCE'" in `c'
		replace n_obs = e(N) in `c'
		replace n_SPs = e(N_clust1) in `c'
		replace n_modates = e(N_clust2) in `c'
		replace dof = e(df_r) in `c'
				
	}

	else {
		
		// Run 2SLS regression
		ivreghdfe `DEPVAR' `RHS' `if_sample', absorb(`FEs') cluster(`VCE')
		local p_water_var = subinstr(word("`RHS'",1),"(","",1)
		
		// Store results
		replace panel = "`panel'" in `c'
		replace pull = "`pull'" in `c'
		replace sens = "`sens'" in `c'
		replace sample = "`if_sample'" in `c'
		replace depvar = "`DEPVAR'" in `c'
		replace fes = "`FEs'" in `c'
		replace rhs = "`RHS'" in `c'
		replace beta_log_p_water = _b[`p_water_var'] in `c'
		replace se_log_p_water = _se[`p_water_var'] in `c'
		replace t_log_p_water =  _b[`p_water_var']/_se[`p_water_var'] in `c'
		replace vce = "cluster `VCE'" in `c'
		replace n_obs = e(N) in `c'
		replace n_SPs = e(N_clust1) in `c'
		replace n_modates = e(N_clust2) in `c'
		replace dof = e(df_r) in `c'
		replace fstat_rk = e(rkf) in `c'
				
		// Run first stage regression (electricity price)
		local RHSfs = subinstr(subinstr(subinstr("`RHS'","(","",.),")","",.),"=","",.)
		reghdfe `RHSfs' `if_sample', absorb(`FEs') vce(cluster `VCE')
		
		forvalues v = 1/4 {
			local var = word(e(indepvars),`v')
			if "`var'"!="_cons" {
				cap replace fs_beta_var`v' = _b[`var'] in `c'
				cap replace fs_se_var`v' = _se[`var'] in `c'
				cap replace fs_t_var`v' =  _b[`var']/_se[`var'] in `c'
			}	
		}
	}

	// Intermediate output
	di "******** Sensitivity `c' done *********"
}

// Save output
keep panel-fs_t_var4
dropmiss, obs force
dropmiss, force
compress
save "$dirpath_data/results/regs_appx_water_combined_crop_splits.dta", replace

}

************************************************
************************************************

** 2. Splits by CLU_group0/crop
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
	
// Create empty variables to populate for storign results
gen panel = ""
cap drop pull
gen pull = ""
gen sens = ""
gen sample = ""
gen depvar = ""
gen fes = ""
gen rhs = ""
gen if_sample = ""
gen beta_log_p_water = .
gen se_log_p_water = .
gen t_log_p_water = .
gen vce = ""
gen n_obs = .
gen n_SPs = .
gen n_modates = .
gen dof = .
gen fstat_rk = .
forvalues v = 1/4 {
	gen fs_beta_var`v' = .
	gen fs_se_var`v' = .
	gen fs_t_var`v' = .
}

// Define default global: sample
global if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	
// Define default global: dependent variable
global DEPVAR = "ihs_af_rast_dd_mth_2SP"
	
// Define default global: RHS	
global RHS = "(ln_mean_p_af_rast_dd_mth_2SP = log_mean_p_kwh_ag_default)" 

// Define default global: FEs
global FEs = "sp_group#month sp_group#rt_large_ag modate"
	
// Define default global: cluster variables
global VCE = "sp_group modate"	


// Merge in CLUgroup-by-year wide panel (crops)
decode clu_group0_ec, gen(clu_group0)
merge m:1 clu_group0 year using "$dirpath_data/cleaned_spatial/CDL_panel_clugroup0_crop_year_wide.dta"
keep if _merge==3
drop _merge
	
// Loop over sensitivities
forvalues c = 1/30 {

	// Reset default locals
	local if_sample = "${if_sample}"
	local DEPVAR = "${DEPVAR}"
	local RHS = "${RHS}"
	local FEs = "${FEs}"
	local VCE = "${VCE}"
	local sens = ""

	if `c'==1 {
		local sens = "CLUgroup0/crop: ever50_Alfalfa"
		local if_sample = "${if_sample} & ever50_Alfalfa==1"
	}
	if `c'==2 {
		local sens = "CLUgroup0/crop: ever50_Almonds"
		local if_sample = "${if_sample} & ever50_Almonds==1"
	}
	if `c'==3 {
		local sens = "CLUgroup0/crop: ever50_Barley"
		local if_sample = "${if_sample} & ever50_Barley==1"
	}
	if `c'==4 {
		local sens = "CLUgroup0/crop: ever50_Corn"
		local if_sample = "${if_sample} & ever50_Corn==1"
	}
	if `c'==5 {
		local sens = "CLUgroup0/crop: ever50_Cotton"
		local if_sample = "${if_sample} & ever50_Cotton==1"
	}
	if `c'==6 {
		local sens = "CLUgroup0/crop: ever50_Grapes"
		local if_sample = "${if_sample} & ever50_Grapes==1"
	}
	if `c'==7 {
		local sens = "CLUgroup0/crop: ever50_Oats"
		local if_sample = "${if_sample} & ever50_Oats==1"
	}
	if `c'==8 {
		local sens = "CLUgroup0/crop: ever50_Oranges"
		local if_sample = "${if_sample} & ever50_Oranges==1"
	}
	if `c'==9 {
		local sens = "CLUgroup0/crop: ever50_OtherHay"
		local if_sample = "${if_sample} & ever50_OtherHay==1"
	}
	if `c'==10 {
		local sens = "CLUgroup0/crop: ever50_Pistachios"
		local if_sample = "${if_sample} & ever50_Pistachios==1"
	}
	if `c'==11 {
		local sens = "CLUgroup0/crop: ever50_Rice"
		local if_sample = "${if_sample} & ever50_Rice==1"
	}
	if `c'==12 {
		local sens = "CLUgroup0/crop: ever50_Tomatoes"
		local if_sample = "${if_sample} & ever50_Tomatoes==1"
	}
	if `c'==13 {
		local sens = "CLUgroup0/crop: ever50_Walnuts"
		local if_sample = "${if_sample} & ever50_Walnuts==1"
	}
	if `c'==14 {
		local sens = "CLUgroup0/crop: ever50_Wheat"
		local if_sample = "${if_sample} & ever50_Wheat==1"
	}
	if `c'==15 {
		local sens = "CLUgroup0/crop: ever50_count==1"
		local if_sample = "${if_sample} & ever50_count==1"
	}
	if `c'==16 {
		local sens = "CLUgroup0/crop: ever50_count>1"
		local if_sample = "${if_sample} & ever50_count>1"
	}
	if `c'==17 {
		local sens = "CLUgroup0/crop: acres_Alfalfa>1"
		local if_sample = "${if_sample} & acres_Alfalfa>1"
	}
	if `c'==18 {
		local sens = "CLUgroup0/crop: acres_Almonds>1"
		local if_sample = "${if_sample} & acres_Almonds>1"
	}
	if `c'==19 {
		local sens = "CLUgroup0/crop: acres_Barley>1"
		local if_sample = "${if_sample} & acres_Barley>1"
	}
	if `c'==20 {
		local sens = "CLUgroup0/crop: acres_Corn>1"
		local if_sample = "${if_sample} & acres_Corn>1"
	}
	if `c'==21 {
		local sens = "CLUgroup0/crop: acres_Cotton>1"
		local if_sample = "${if_sample} & acres_Cotton>1"
	}
	if `c'==22 {
		local sens = "CLUgroup0/crop: acres_Grapes>1"
		local if_sample = "${if_sample} & acres_Grapes>1"
	}
	if `c'==23 {
		local sens = "CLUgroup0/crop: acres_Oats>1"
		local if_sample = "${if_sample} & acres_Oats>1"
	}
	if `c'==24 {
		local sens = "CLUgroup0/crop: acres_Oranges>1"
		local if_sample = "${if_sample} & acres_Oranges>1"
	}
	if `c'==25 {
		local sens = "CLUgroup0/crop: acres_OtherHay>1"
		local if_sample = "${if_sample} & acres_OtherHay>1"
	}
	if `c'==26 {
		local sens = "CLUgroup0/crop: acres_Pistachios>1"
		local if_sample = "${if_sample} & acres_Pistachios>1"
	}
	if `c'==27 {
		local sens = "CLUgroup0/crop: acres_Rice>1"
		local if_sample = "${if_sample} & acres_Rice>1"
	}
	if `c'==28 {
		local sens = "CLUgroup0/crop: acres_Tomatoes>1"
		local if_sample = "${if_sample} & acres_Tomatoes>1"
	}
	if `c'==29 {
		local sens = "CLUgroup0/crop: acres_Walnuts>1"
		local if_sample = "${if_sample} & acres_Walnuts>1"
	}
	if `c'==30 {
		local sens = "CLUgroup0/crop: acres_WinterWheat>1"
		local if_sample = "${if_sample} & acres_WinterWheat>1"
	}
	
	
	// Run non-IV specification	
	if substr("`RHS'",1,1)!="(" {
					
		// Run OLS regression
		reghdfe `DEPVAR' `RHS' `if_sample', absorb(`FEs') vce(cluster `VCE')
		local p_water_var = subinstr(word("`RHS'",1),"(","",1)
		
		// Store results
		replace panel = "`panel'" in `c'
		replace pull = "`pull'" in `c'
		replace sens = "`sens'" in `c'
		replace sample = "`if_sample'" in `c'
		replace depvar = "`DEPVAR'" in `c'
		replace fes = "`FEs'" in `c'
		replace rhs = "`RHS'" in `c'
		replace if_sample = "`if_sample'" in `c'
		replace beta_log_p_water = _b[`p_water_var'] in `c'
		replace se_log_p_water = _se[`p_water_var'] in `c'
		replace t_log_p_water =  _b[`p_water_var']/_se[`p_water_var'] in `c'
		replace vce = "cluster `VCE'" in `c'
		replace n_obs = e(N) in `c'
		replace n_SPs = e(N_clust1) in `c'
		replace n_modates = e(N_clust2) in `c'
		replace dof = e(df_r) in `c'
				
	}

	else {
		
		// Run 2SLS regression
		ivreghdfe `DEPVAR' `RHS' `if_sample', absorb(`FEs') cluster(`VCE')
		local p_water_var = subinstr(word("`RHS'",1),"(","",1)
		
		// Store results
		replace panel = "`panel'" in `c'
		replace pull = "`pull'" in `c'
		replace sens = "`sens'" in `c'
		replace sample = "`if_sample'" in `c'
		replace depvar = "`DEPVAR'" in `c'
		replace fes = "`FEs'" in `c'
		replace rhs = "`RHS'" in `c'
		replace beta_log_p_water = _b[`p_water_var'] in `c'
		replace se_log_p_water = _se[`p_water_var'] in `c'
		replace t_log_p_water =  _b[`p_water_var']/_se[`p_water_var'] in `c'
		replace vce = "cluster `VCE'" in `c'
		replace n_obs = e(N) in `c'
		replace n_SPs = e(N_clust1) in `c'
		replace n_modates = e(N_clust2) in `c'
		replace dof = e(df_r) in `c'
		replace fstat_rk = e(rkf) in `c'
				
		// Run first stage regression (electricity price)
		local RHSfs = subinstr(subinstr(subinstr("`RHS'","(","",.),")","",.),"=","",.)
		reghdfe `RHSfs' `if_sample', absorb(`FEs') vce(cluster `VCE')
		
		forvalues v = 1/4 {
			local var = word(e(indepvars),`v')
			if "`var'"!="_cons" {
				cap replace fs_beta_var`v' = _b[`var'] in `c'
				cap replace fs_se_var`v' = _se[`var'] in `c'
				cap replace fs_t_var`v' =  _b[`var']/_se[`var'] in `c'
			}	
		}
	}

	// Intermediate output
	di "******** Sensitivity `c' done *********"
}

// Save output
keep panel-fs_t_var4
dropmiss, obs force
dropmiss, force
compress
tempfile CLUgroup0_crop
save `CLUgroup0_crop'
use "$dirpath_data/results/regs_appx_water_combined_crop_splits.dta", clear
append using `CLUgroup0_crop'
save "$dirpath_data/results/regs_appx_water_combined_crop_splits.dta", replace

}

************************************************
************************************************

** 3. Splits by CLU/category
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
	
// Create empty variables to populate for storign results
gen panel = ""
cap drop pull
gen pull = ""
gen sens = ""
gen sample = ""
gen depvar = ""
gen fes = ""
gen rhs = ""
gen if_sample = ""
gen beta_log_p_water = .
gen se_log_p_water = .
gen t_log_p_water = .
gen vce = ""
gen n_obs = .
gen n_SPs = .
gen n_modates = .
gen dof = .
gen fstat_rk = .
forvalues v = 1/4 {
	gen fs_beta_var`v' = .
	gen fs_se_var`v' = .
	gen fs_t_var`v' = .
}

// Define default global: sample
global if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	
// Define default global: dependent variable
global DEPVAR = "ihs_af_rast_dd_mth_2SP"
	
// Define default global: RHS	
global RHS = "(ln_mean_p_af_rast_dd_mth_2SP = log_mean_p_kwh_ag_default)" 

// Define default global: FEs
global FEs = "sp_group#month sp_group#rt_large_ag modate"
	
// Define default global: cluster variables
global VCE = "sp_group modate"	


// Merge in CLU-by-year wide panel (crop categories)
decode clu_id_ec, gen(clu_id)
merge m:1 clu_id year using "$dirpath_data/cleaned_spatial/CDL_panel_clu_cat_year_wide.dta"
keep if _merge==3
drop _merge
	
// Loop over sensitivities
forvalues c = 1/21 {

	// Reset default locals
	local if_sample = "${if_sample}"
	local DEPVAR = "${DEPVAR}"
	local RHS = "${RHS}"
	local FEs = "${FEs}"
	local VCE = "${VCE}"
	local sens = ""

	if `c'==1 {
		local sens = "CLU/cat: ever_Cereal"
		local if_sample = "${if_sample} & ever_Cereal==1"
	}
	if `c'==2 {
		local sens = "CLU/cat: ever_Cotton"
		local if_sample = "${if_sample} & ever_Cotton==1"
	}
	if `c'==3 {
		local sens = "CLU/cat: ever_Feed"
		local if_sample = "${if_sample} & ever_Feed==1"
	}
	if `c'==4 {
		local sens = "CLU/cat: ever_Fruit"
		local if_sample = "${if_sample} & ever_Fruit==1"
	}
	if `c'==5 {
		local sens = "CLU/cat: ever_Grapes"
		local if_sample = "${if_sample} & ever_Grapes==1"
	}
	if `c'==6 {
		local sens = "CLU/cat: ever_Nuts"
		local if_sample = "${if_sample} & ever_Nuts==1"
	}
	if `c'==7 {
		local sens = "CLU/cat: ever_Vegetables"
		local if_sample = "${if_sample} & ever_Vegetables==1"
	}
	if `c'==8 {
		local sens = "CLU/cat: ever_Other"
		local if_sample = "${if_sample} & ever_Other==1"
	}
	if `c'==9 {
		local sens = "CLU/cat: mode_switcher==1"
		local if_sample = "${if_sample} & mode_switcher==1"
	}
	if `c'==10 {
		local sens = "CLU/cat: mode_switcher==0"
		local if_sample = "${if_sample} & mode_switcher==0"
	}
	if `c'==11 {
		local sens = "CLU/cat: acres_Cereal_A>1"
		local if_sample = "${if_sample} & acres_Cereal_A>1"
	}
	if `c'==12 {
		local sens = "CLU/cat: acres_Cotton_A>1"
		local if_sample = "${if_sample} & acres_Cotton_A>1"
	}
	if `c'==13 {
		local sens = "CLU/cat: acres_Fallow_A>1"
		local if_sample = "${if_sample} & acres_Fallow_A>1"
	}
	if `c'==14 {
		local sens = "CLU/cat: acres_Feed_A>1"
		local if_sample = "${if_sample} & acres_Feed_A>1"
	}
	if `c'==15 {
		local sens = "CLU/cat: acres_Feed_P>1"
		local if_sample = "${if_sample} & acres_Feed_P>1"
	}
	if `c'==16 {
		local sens = "CLU/cat: acres_Fruit_A>1"
		local if_sample = "${if_sample} & acres_Fruit_A>1"
	}
	if `c'==17 {
		local sens = "CLU/cat: acres_Fruit_P>1"
		local if_sample = "${if_sample} & acres_Fruit_P>1"
	}
	if `c'==18 {
		local sens = "CLU/cat: acres_Grapes_P>1"
		local if_sample = "${if_sample} & acres_Grapes_P>1"
	}
	if `c'==19 {
		local sens = "CLU/cat: acres_Nuts_P>1"
		local if_sample = "${if_sample} & acres_Nuts_P>1"
	}
	if `c'==20 {
		local sens = "CLU/cat: acres_Vegetables_A>1"
		local if_sample = "${if_sample} & acres_Vegetables_A>1"
	}
	if `c'==21 {
		local sens = "CLU/cat: acres_Vegetables_P>1"
		local if_sample = "${if_sample} & acres_Vegetables_P>1"
	}
	
	
	// Run non-IV specification	
	if substr("`RHS'",1,1)!="(" {
					
		// Run OLS regression
		reghdfe `DEPVAR' `RHS' `if_sample', absorb(`FEs') vce(cluster `VCE')
		local p_water_var = subinstr(word("`RHS'",1),"(","",1)
		
		// Store results
		replace panel = "`panel'" in `c'
		replace pull = "`pull'" in `c'
		replace sens = "`sens'" in `c'
		replace sample = "`if_sample'" in `c'
		replace depvar = "`DEPVAR'" in `c'
		replace fes = "`FEs'" in `c'
		replace rhs = "`RHS'" in `c'
		replace if_sample = "`if_sample'" in `c'
		replace beta_log_p_water = _b[`p_water_var'] in `c'
		replace se_log_p_water = _se[`p_water_var'] in `c'
		replace t_log_p_water =  _b[`p_water_var']/_se[`p_water_var'] in `c'
		replace vce = "cluster `VCE'" in `c'
		replace n_obs = e(N) in `c'
		replace n_SPs = e(N_clust1) in `c'
		replace n_modates = e(N_clust2) in `c'
		replace dof = e(df_r) in `c'
				
	}

	else {
		
		// Run 2SLS regression
		ivreghdfe `DEPVAR' `RHS' `if_sample', absorb(`FEs') cluster(`VCE')
		local p_water_var = subinstr(word("`RHS'",1),"(","",1)
		
		// Store results
		replace panel = "`panel'" in `c'
		replace pull = "`pull'" in `c'
		replace sens = "`sens'" in `c'
		replace sample = "`if_sample'" in `c'
		replace depvar = "`DEPVAR'" in `c'
		replace fes = "`FEs'" in `c'
		replace rhs = "`RHS'" in `c'
		replace beta_log_p_water = _b[`p_water_var'] in `c'
		replace se_log_p_water = _se[`p_water_var'] in `c'
		replace t_log_p_water =  _b[`p_water_var']/_se[`p_water_var'] in `c'
		replace vce = "cluster `VCE'" in `c'
		replace n_obs = e(N) in `c'
		replace n_SPs = e(N_clust1) in `c'
		replace n_modates = e(N_clust2) in `c'
		replace dof = e(df_r) in `c'
		replace fstat_rk = e(rkf) in `c'
				
		// Run first stage regression (electricity price)
		local RHSfs = subinstr(subinstr(subinstr("`RHS'","(","",.),")","",.),"=","",.)
		reghdfe `RHSfs' `if_sample', absorb(`FEs') vce(cluster `VCE')
		
		forvalues v = 1/4 {
			local var = word(e(indepvars),`v')
			if "`var'"!="_cons" {
				cap replace fs_beta_var`v' = _b[`var'] in `c'
				cap replace fs_se_var`v' = _se[`var'] in `c'
				cap replace fs_t_var`v' =  _b[`var']/_se[`var'] in `c'
			}	
		}
	}

	// Intermediate output
	di "******** Sensitivity `c' done *********"
}

// Save output
keep panel-fs_t_var4
dropmiss, obs force
dropmiss, force
compress
tempfile CLU_cat
save `CLU_cat'
use "$dirpath_data/results/regs_appx_water_combined_crop_splits.dta", clear
append using `CLU_cat'
save "$dirpath_data/results/regs_appx_water_combined_crop_splits.dta", replace

}

************************************************
************************************************

** 4. Splits by CLU_group0/category
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
	
// Create empty variables to populate for storign results
gen panel = ""
cap drop pull
gen pull = ""
gen sens = ""
gen sample = ""
gen depvar = ""
gen fes = ""
gen rhs = ""
gen if_sample = ""
gen beta_log_p_water = .
gen se_log_p_water = .
gen t_log_p_water = .
gen vce = ""
gen n_obs = .
gen n_SPs = .
gen n_modates = .
gen dof = .
gen fstat_rk = .
forvalues v = 1/4 {
	gen fs_beta_var`v' = .
	gen fs_se_var`v' = .
	gen fs_t_var`v' = .
}

// Define default global: sample
global if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	
// Define default global: dependent variable
global DEPVAR = "ihs_af_rast_dd_mth_2SP"
	
// Define default global: RHS	
global RHS = "(ln_mean_p_af_rast_dd_mth_2SP = log_mean_p_kwh_ag_default)" 

// Define default global: FEs
global FEs = "sp_group#month sp_group#rt_large_ag modate"
	
// Define default global: cluster variables
global VCE = "sp_group modate"	


// Merge in CLUgroup-by-year wide panel (crop categories)
decode clu_group0_ec, gen(clu_group0)
merge m:1 clu_group0 year using "$dirpath_data/cleaned_spatial/CDL_panel_clugroup0_cat_year_wide.dta"
keep if _merge==3
drop _merge
	
// Loop over sensitivities
forvalues c = 1/21 {

	// Reset default locals
	local if_sample = "${if_sample}"
	local DEPVAR = "${DEPVAR}"
	local RHS = "${RHS}"
	local FEs = "${FEs}"
	local VCE = "${VCE}"
	local sens = ""

	if `c'==1 {
		local sens = "CLUgroup0/cat: ever_Cereal"
		local if_sample = "${if_sample} & ever_Cereal==1"
	}
	if `c'==2 {
		local sens = "CLUgroup0/cat: ever_Cotton"
		local if_sample = "${if_sample} & ever_Cotton==1"
	}
	if `c'==3 {
		local sens = "CLUgroup0/cat: ever_Feed"
		local if_sample = "${if_sample} & ever_Feed==1"
	}
	if `c'==4 {
		local sens = "CLUgroup0/cat: ever_Fruit"
		local if_sample = "${if_sample} & ever_Fruit==1"
	}
	if `c'==5 {
		local sens = "CLUgroup0/cat: ever_Grapes"
		local if_sample = "${if_sample} & ever_Grapes==1"
	}
	if `c'==6 {
		local sens = "CLUgroup0/cat: ever_Nuts"
		local if_sample = "${if_sample} & ever_Nuts==1"
	}
	if `c'==7 {
		local sens = "CLUgroup0/cat: ever_Vegetables"
		local if_sample = "${if_sample} & ever_Vegetables==1"
	}
	if `c'==8 {
		local sens = "CLUgroup0/cat: ever_Other"
		local if_sample = "${if_sample} & ever_Other==1"
	}
	if `c'==9 {
		local sens = "CLUgroup0/cat: mode_switcher==1"
		local if_sample = "${if_sample} & mode_switcher==1"
	}
	if `c'==10 {
		local sens = "CLUgroup0/cat: mode_switcher==0"
		local if_sample = "${if_sample} & mode_switcher==0"
	}
	if `c'==11 {
		local sens = "CLUgroup0/cat: acres_Cereal_A>1"
		local if_sample = "${if_sample} & acres_Cereal_A>1"
	}
	if `c'==12 {
		local sens = "CLUgroup0/cat: acres_Cotton_A>1"
		local if_sample = "${if_sample} & acres_Cotton_A>1"
	}
	if `c'==13 {
		local sens = "CLUgroup0/cat: acres_Fallow_A>1"
		local if_sample = "${if_sample} & acres_Fallow_A>1"
	}
	if `c'==14 {
		local sens = "CLUgroup0/cat: acres_Feed_A>1"
		local if_sample = "${if_sample} & acres_Feed_A>1"
	}
	if `c'==15 {
		local sens = "CLUgroup0/cat: acres_Feed_P>1"
		local if_sample = "${if_sample} & acres_Feed_P>1"
	}
	if `c'==16 {
		local sens = "CLUgroup0/cat: acres_Fruit_A>1"
		local if_sample = "${if_sample} & acres_Fruit_A>1"
	}
	if `c'==17 {
		local sens = "CLUgroup0/cat: acres_Fruit_P>1"
		local if_sample = "${if_sample} & acres_Fruit_P>1"
	}
	if `c'==18 {
		local sens = "CLUgroup0/cat: acres_Grapes_P>1"
		local if_sample = "${if_sample} & acres_Grapes_P>1"
	}
	if `c'==19 {
		local sens = "CLUgroup0/cat: acres_Nuts_P>1"
		local if_sample = "${if_sample} & acres_Nuts_P>1"
	}
	if `c'==20 {
		local sens = "CLUgroup0/cat: acres_Vegetables_A>1"
		local if_sample = "${if_sample} & acres_Vegetables_A>1"
	}
	if `c'==21 {
		local sens = "CLUgroup0/cat: acres_Vegetables_P>1"
		local if_sample = "${if_sample} & acres_Vegetables_P>1"
	}
	
	
	// Run non-IV specification	
	if substr("`RHS'",1,1)!="(" {
					
		// Run OLS regression
		reghdfe `DEPVAR' `RHS' `if_sample', absorb(`FEs') vce(cluster `VCE')
		local p_water_var = subinstr(word("`RHS'",1),"(","",1)
		
		// Store results
		replace panel = "`panel'" in `c'
		replace pull = "`pull'" in `c'
		replace sens = "`sens'" in `c'
		replace sample = "`if_sample'" in `c'
		replace depvar = "`DEPVAR'" in `c'
		replace fes = "`FEs'" in `c'
		replace rhs = "`RHS'" in `c'
		replace if_sample = "`if_sample'" in `c'
		replace beta_log_p_water = _b[`p_water_var'] in `c'
		replace se_log_p_water = _se[`p_water_var'] in `c'
		replace t_log_p_water =  _b[`p_water_var']/_se[`p_water_var'] in `c'
		replace vce = "cluster `VCE'" in `c'
		replace n_obs = e(N) in `c'
		replace n_SPs = e(N_clust1) in `c'
		replace n_modates = e(N_clust2) in `c'
		replace dof = e(df_r) in `c'
				
	}

	else {
		
		// Run 2SLS regression
		ivreghdfe `DEPVAR' `RHS' `if_sample', absorb(`FEs') cluster(`VCE')
		local p_water_var = subinstr(word("`RHS'",1),"(","",1)
		
		// Store results
		replace panel = "`panel'" in `c'
		replace pull = "`pull'" in `c'
		replace sens = "`sens'" in `c'
		replace sample = "`if_sample'" in `c'
		replace depvar = "`DEPVAR'" in `c'
		replace fes = "`FEs'" in `c'
		replace rhs = "`RHS'" in `c'
		replace beta_log_p_water = _b[`p_water_var'] in `c'
		replace se_log_p_water = _se[`p_water_var'] in `c'
		replace t_log_p_water =  _b[`p_water_var']/_se[`p_water_var'] in `c'
		replace vce = "cluster `VCE'" in `c'
		replace n_obs = e(N) in `c'
		replace n_SPs = e(N_clust1) in `c'
		replace n_modates = e(N_clust2) in `c'
		replace dof = e(df_r) in `c'
		replace fstat_rk = e(rkf) in `c'
				
		// Run first stage regression (electricity price)
		local RHSfs = subinstr(subinstr(subinstr("`RHS'","(","",.),")","",.),"=","",.)
		reghdfe `RHSfs' `if_sample', absorb(`FEs') vce(cluster `VCE')
		
		forvalues v = 1/4 {
			local var = word(e(indepvars),`v')
			if "`var'"!="_cons" {
				cap replace fs_beta_var`v' = _b[`var'] in `c'
				cap replace fs_se_var`v' = _se[`var'] in `c'
				cap replace fs_t_var`v' =  _b[`var']/_se[`var'] in `c'
			}	
		}
	}

	// Intermediate output
	di "******** Sensitivity `c' done *********"
}

// Save output
keep panel-fs_t_var4
dropmiss, obs force
dropmiss, force
compress
tempfile CLUgroup0_cat
save `CLUgroup0_cat'
use "$dirpath_data/results/regs_appx_water_combined_crop_splits.dta", clear
append using `CLUgroup0_cat'
save "$dirpath_data/results/regs_appx_water_combined_crop_splits.dta", replace

}

************************************************
************************************************
