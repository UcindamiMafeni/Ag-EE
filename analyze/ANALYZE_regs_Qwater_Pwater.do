clear all
version 13
set more off

********************************************************************
** Script to run a ton of regressions estimating price elasticity **
**  of demand, using elec data & estimated KWH/AF converson rates **
********************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

************************************************
************************************************

** 1. Monthly regressions
{ 

// Load monthly panel
use "$dirpath_data/merged/sp_month_water_panel.dta", clear

// Loop through sample restrictions
foreach ifs in 1 2 3 4 5 6 {

	if `ifs'==1 {
		local if_sample = "if sp_same_rate_dumbsmart==1"
	}
	if `ifs'==2 {
		local if_sample = "if sp_same_rate_in_cat==0"
	}
	if `ifs'==3 {
		local if_sample = "if sp_same_rate_dumbsmart==0 & sp_same_rate_in_cat==1"
	}
	if `ifs'==4 {
		local if_sample = "if sp_same_rate_dumbsmart==1 & flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `ifs'==5 {
		local if_sample = "if sp_same_rate_in_cat==0 & flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `ifs'==6 {
		local if_sample = "if sp_same_rate_dumbsmart==0 & sp_same_rate_in_cat==1 & flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	
	// Loop over different combinations of fixed effects and interactions thereof
	foreach fe in 1 2 3 4 5 6 {
	
		if `fe'==1 {
			local FEs = "sp_group#month modate"
		}
		if `fe'==2 {
			local FEs = "sp_group#month wdist_group#year modate"
		}
		if `fe'==3 {
			local FEs = "sp_group#month wdist_group#year basin_group#year modate"
		}
		if `fe'==4 {
			local FEs = "sp_group#month modate sp_group#c.modate"
		}
		if `fe'==5 {
			local FEs = "sp_group#month wdist_group#year modate sp_group#c.modate"
		}
		if `fe'==6 {
			local FEs = "sp_group#month wdist_group#year basin_group#year modate sp_group#c.modate"
		}


		// Loop over Q and P variable asusmptions
		foreach assn in 2 /*1*/ 4 6 /*10*/ {
		
			if `assn'==1 {
				local ASSNs = "rast_dd_mth_1SP"
			}	
			if `assn'==2 {
				local ASSNs = "rast_dd_mth_2SP" // ex ante default
			}			
			if `assn'==3 {
				local ASSNs = "rast_dd_qtr_1SP"
			}			
			if `assn'==4 {
				local ASSNs = "rast_dd_qtr_2SP"
			}			
			if `assn'==5 {
				local ASSNs = "rast_ddhat_mth_1SP"
			}	
			if `assn'==6 {
				local ASSNs = "rast_ddhat_mth_2SP"
			}			
			if `assn'==7 {
				local ASSNs = "rast_ddhat_qtr_1SP"
			}			
			if `assn'==8 {
				local ASSNs = "rast_ddhat_qtr_2SP"
			}			
			if `assn'==9 {
				local ASSNs = "mean_dd_mth_1SP"
			}	
			if `assn'==10 {
				local ASSNs = "mean_dd_mth_2SP"
			}			
			if `assn'==11 {
				local ASSNs = "mean_dd_qtr_1SP"
			}			
			if `assn'==12 {
				local ASSNs = "mean_dd_qtr_2SP"
			}			
			if `assn'==13 {
				local ASSNs = "mean_ddhat_mth_1SP"
			}	
			if `assn'==14 {
				local ASSNs = "mean_ddhat_mth_2SP"
			}			
			if `assn'==15 {
				local ASSNs = "mean_ddhat_qtr_1SP"
			}			
			if `assn'==16 {
				local ASSNs = "mean_ddhat_qtr_2SP"
			}			
	
			// Loop over alternative RHS specifications, including IVs
			foreach rhs in 1 2 3 4 5 6 7 8 9 {

				if `rhs'==1 {
					local RHS_model = "ln_mean_p_af_X "
				}
				if `rhs'==2 {
					local RHS_model = "(ln_mean_p_af_X = c.log_*_p_kwh_ag_default#c.kwhaf_apep_measured)"
				}
				if `rhs'==3 {
					local RHS_model = "(ln_mean_p_af_X = c.log_p_m*_lag*#c.kwhaf_apep_measured)"
				}
				if `rhs'==4 {
					local RHS_model = "log_p_mean ln_kwhaf_X "
				}
				if `rhs'==5 {
					local RHS_model = "(log_p_mean = log_*_p_kwh_ag_default) ln_kwhaf_X "
				}
				if `rhs'==6 {
					local RHS_model = "(log_p_mean = log_p_m*_lag*) ln_kwhaf_X "
				}
				if `rhs'==7 {
					local RHS_model = "(ln_kwhaf_X = kwhaf_apep_measured) log_p_mean"
				}
				if `rhs'==8 {
					local RHS_model = "(log_p_mean ln_kwhaf_X = log_*_p_kwh_ag_default kwhaf_apep_measured) "
				}
				if `rhs'==9 {
					local RHS_model = "(log_p_mean ln_kwhaf_X = log_p_m*_lag* kwhaf_apep_measured) "
				}
				
				local RHS = subinstr("`RHS_model'","_X","_`ASSNs'",.)
				local depvar = "ihs_af_`ASSNs'"

				// Only do the IV regs that matches the sample!
				local skip = ""
				if regexm("`if_sample'","if sp_same_rate_dumbsmart==0 & sp_same_rate_in_cat==1")==0 ///
					& regexm("`RHS'","log_p_m*_lag*")==1 {
					local skip = "skip"
				}
				if regexm("`if_sample'","if sp_same_rate_in_cat==0")==0 ///
					& regexm("`RHS'","log_*_p_kwh_ag_default")==1 {
					local skip = "skip"
				}
				// Skip regressions that are already stored in output file
				preserve
				cap {
					use "$dirpath_data/results/regs_Qwater_Pwater.dta", clear
					count if panel=="monthly" & sample=="`if_sample'" & fes=="`FEs'" & rhs=="`RHS'" & assns=="`ASSNs'"
					if r(N)==1 {
						local skip = "skip"
					}
				}	
				restore

				// Flag IV specificaitons, which require different syntax
				local iv = ""
				if regexm("`RHS'"," = ") {
					local iv = "iv"
				}

				// Run non-IV specificaitons	
				if "`skip'"=="" & "`iv'"=="" {

					// Run regression
					reghdfe `depvar' `RHS' `if_sample', absorb(`FEs') vce(cluster sp_group modate)

					// Store output
					preserve
					clear
					set obs 1
					gen panel = "monthly"
					gen sample = "`if_sample'"
					gen fes = "`FEs'"
					gen rhs = "`RHS_model'"
					gen depvar = "`depvar'"
					gen assns = "`ASSNs'"
					if regexm("`RHS_model'","log_p_mean") {
						gen beta_log_p_kwh = _b[log_p_mean]
						gen se_log_p_kwh = _se[log_p_mean]
						gen t_log_p_kwh =  _b[log_p_mean]/_se[log_p_mean]
					}
					if regexm("`RHS_model'","ln_kwhaf_X") {
						gen beta_log_kwhaf = _b[ln_kwhaf_`ASSNs']
						gen se_log_kwhaf = _se[ln_kwhaf_`ASSNs']
						gen t_log_kwhaf =  _b[ln_kwhaf_`ASSNs']/_se[ln_kwhaf_`ASSNs']
					}
					if regexm("`RHS_model'","ln_mean_p_af_X") {
						gen beta_log_p_af = _b[ln_mean_p_af_`ASSNs']
						gen se_log_p_af = _se[ln_mean_p_af_`ASSNs']
						gen t_log_p_af =  _b[ln_mean_p_af_`ASSNs']/_se[ln_mean_p_af_`ASSNs']
					}
					gen n_obs = e(N)
					gen n_SPs = e(N_clust1)
					gen n_modates = e(N_clust2)
					gen dof = e(df_r)
					cap append using "$dirpath_data/results/regs_Qwater_Pwater.dta"
					duplicates drop 
					compress
					save "$dirpath_data/results/regs_Qwater_Pwater.dta", replace
					restore
				}
				
				// Run IV specificaitons	
				if "`skip'"=="" & "`iv'"=="iv" {
				
					// Run regression
					ivreghdfe `depvar' `RHS' `if_sample', absorb(`FEs') cluster(sp_group modate) 

					// Store output
					preserve
					clear
					set obs 1
					gen panel = "monthly"
					gen sample = "`if_sample'"
					gen fes = "`FEs'"
					gen rhs = "`RHS_model'"
					gen depvar = "`depvar'"
					gen assns = "`ASSNs'"
					if regexm("`RHS_model'","log_p_mean") {
						gen beta_log_p_kwh = _b[log_p_mean]
						gen se_log_p_kwh = _se[log_p_mean]
						gen t_log_p_kwh =  _b[log_p_mean]/_se[log_p_mean]
					}
					if regexm("`RHS_model'","ln_kwhaf_X") {
						gen beta_log_kwhaf = _b[ln_kwhaf_`ASSNs']
						gen se_log_kwhaf = _se[ln_kwhaf_`ASSNs']
						gen t_log_kwhaf =  _b[ln_kwhaf_`ASSNs']/_se[ln_kwhaf_`ASSNs']
					}
					if regexm("`RHS_model'","ln_mean_p_af_X") {
						gen beta_log_p_af = _b[ln_mean_p_af_`ASSNs']
						gen se_log_p_af = _se[ln_mean_p_af_`ASSNs']
						gen t_log_p_af =  _b[ln_mean_p_af_`ASSNs']/_se[ln_mean_p_af_`ASSNs']
					}
					gen n_obs = e(N)
					gen n_SPs = e(N_clust1)
					gen n_modates = e(N_clust2)
					gen dof = e(df_r)
					gen fstat_rk = e(rkf)
					gen fstat_cd = e(cdf)
					cap append using "$dirpath_data/results/regs_Qwater_Pwater.dta"
					duplicates drop 
					compress
					save "$dirpath_data/results/regs_Qwater_Pwater.dta", replace
					restore
				}		
			}
		}
	}	
}


}

************************************************
************************************************
