clear all
version 13
set more off

********************************************************************
** Script to run a ton of regressions estimating semi-elasticity  **
**  of demand, using electricity data only (not APEP stuff yet)   **
********************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

************************************************
************************************************

** 1. Monthly regressions
{ 

// Loop over data pulls
foreach pull in "20180719" /*"20180322" "20180827" "combined"*/ {

	// Load monthly panel
	use "$dirpath_data/merged/sp_month_elec_panel.dta", clear
	
	// Keep only observations in relevant pull
	if "`pull'"!="combined" {
		drop if pull!="`pull'"
	}
	
	// Loop through sample restrictions
	foreach ifs in 26 /*19 5 1 7 10 11 12 15 16*/ {
	
		if `ifs'==1 {
			local if_sample = ""
		}
		if `ifs'==2 {
			local if_sample = "if flag_nem==0"
		}
		if `ifs'==3 {
			local if_sample = "if flag_geocode_badmiss==0"
		}
		if `ifs'==4 {
			local if_sample = "if flag_irregular_bill==0"
		}
		if `ifs'==5 {
			local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0"
		}
		if `ifs'==6 {
			local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & in_interval==1"
		}
		if `ifs'==7 {
			local if_sample = "if sp_same_rate_dumbsmart==1"
		}
		if `ifs'==8 {
			local if_sample = "if sp_same_rate_in_cat==1"
		}
		if `ifs'==9 {
			local if_sample = "if sp_same_rate_dumbsmart==0"
		}
		if `ifs'==10 {
			local if_sample = "if sp_same_rate_in_cat==0"
		}
		if `ifs'==11 {
			local if_sample = "if sp_same_rate_dumbsmart==0 & sp_same_rate_in_cat==1"
		}
		if `ifs'==12 {
			local if_sample = "if sp_same_rate_dumbsmart==1 & flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0"
		}
		if `ifs'==13 {
			local if_sample = "if sp_same_rate_in_cat==1 & flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0"
		}
		if `ifs'==14 {
			local if_sample = "if sp_same_rate_dumbsmart==0 & flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0"
		}
		if `ifs'==15 {
			local if_sample = "if sp_same_rate_in_cat==0 & flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0"
		}
		if `ifs'==16 {
			local if_sample = "if sp_same_rate_dumbsmart==0 & sp_same_rate_in_cat==1 & flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0"
		}
		if `ifs'==17 {
			local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & summer==1"
		}
		if `ifs'==18 {
			local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & summer==0"
		}
		if `ifs'==19 {
			local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3"
		}
		if `ifs'==20 {
			local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & elec_binary_frac > 0.95"
		}
		if `ifs'==21 {
			local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & elec_binary_frac > 0.9"
		}
		if `ifs'==22 {
			local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & annual_always == 1"
		}
		if `ifs'==23 {
			local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & perennial_always == 1"
		}
		if `ifs'==24 {
			local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & perennial_ever == 0"
		}
		if `ifs'==25 {
			local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & annual_ever == 0"
		}
		if `ifs'==26 {
			local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & ann_per_switcher == 1"
		}
		
		// Loop over different combinations of fixed effects and interactions thereof
		foreach fe in 2 30 32 34 /*31 38 33 11 19 28 35 36 37 22 23 24 25 29*/ {
		
			if `fe'==1 {
				local FEs = "sp_group modate"
			}
			if `fe'==2 {
				local FEs = "sp_group#month modate"
			}
			if `fe'==3 {
				local FEs = "sp_group#year modate"
			}
			if `fe'==4 {
				local FEs = "sp_group#month sp_group#year modate"
			}
			if `fe'==5 {
				local FEs = "sp_group#month cz_group#year modate"
			}
			if `fe'==6 {
				local FEs = "sp_group#month sp_group#year cz_group#modate"
			}
			if `fe'==7 {
				local FEs = "sp_group#month county_group#year modate"
			}
			if `fe'==8 {
				local FEs = "sp_group#month sp_group#year county_group#modate"
			}
			if `fe'==9 {
				local FEs = "sp_group#month wdist_group#year modate"
			}
			if `fe'==10 {
				local FEs = "sp_group#month sp_group#year wdist_group#modate"
			}
			if `fe'==11 {
				local FEs = "sp_group#month basin_group#year modate"
			}
			if `fe'==12 {
				local FEs = "sp_group#month sp_group#year basin_group#modate"
			}
			if `fe'==13 {
				local FEs = "sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year modate"
			}
			if `fe'==14 {
				local FEs = "sp_group#month##c.gw_qtr_bsn_mean2 sp_group#year basin_group#modate"
			}
			if `fe'==15 {
				local FEs = "sp_group#month rt_group#year modate"
			}
			if `fe'==16 {
				local FEs = "sp_group#month sp_group#year rt_group#modate"
			}
			if `fe'==17 {
				local FEs = "sp_group#month#rt_group modate"
			}
			if `fe'==18 {
				local FEs = "sp_group#month#rt_group sp_group#year modate"
			}
			if `fe'==19 {
				local FEs = "sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year modate"
			}
			if `fe'==20 {
				local FEs = "sp_group#month sp_group#c.gw_qtr_bsn_mean2 basin_group#year modate"
			}
			if `fe'==21 {
				local FEs = "sp_group#month sp_group#c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year modate"
			}
			if `fe'==22 {
				local FEs = "sp_group#month modate sp_group#c.modate"
			}
			if `fe'==23 {
				local FEs = "sp_group#month basin_group#year modate sp_group#c.modate"
			}
			if `fe'==24 {
				local FEs = "sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year modate sp_group#c.modate"
			}
			if `fe'==25 {
				local FEs = "sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year modate sp_group#c.modate"
			}
			if `fe'==26 {
				local FEs = "sp_group#month sp_group#c.gw_qtr_bsn_mean2 basin_group#year modate sp_group#c.modate"
			}
			if `fe'==27 {
				local FEs = "sp_group#month sp_group#c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year modate sp_group#c.modate"
			}
			if `fe'==28 {
				local FEs = "sp_group#month basin_group#year wdist_group#year modate"
			}
			if `fe'==29 {
				local FEs = "sp_group#month basin_group#year wdist_group#year sp_group#c.modate"
			}
			if `fe'==30 {
				local FEs = "sp_group#month sp_group#rt_large_ag modate"
			}
			if `fe'==31 {
				local FEs = "sp_group#month sp_group#rt_large_ag basin_group#year modate"
			}
			if `fe'==32 {
				local FEs = "sp_group#month sp_group#rt_large_ag basin_group#year wdist_group#year modate"
			}
			if `fe'==33 {
				local FEs = "sp_group#month##c.gw_qtr_bsn_mean2 sp_group#rt_large_ag basin_group#year wdist_group#year modate"
			}
			if `fe'==34 {
				local FEs = "sp_group#rt_large_ag sp_group#month modate sp_group#c.modate"
			}
			if `fe'==35 {
				local FEs = "sp_group#rt_large_ag sp_group#month basin_group#year modate sp_group#c.modate"
			}
			if `fe'==36 {
				local FEs = "sp_group#rt_large_ag sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year modate sp_group#c.modate"
			}
			if `fe'==37 {
				local FEs = "sp_group#rt_large_ag sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year modate sp_group#c.modate"
			}
			if `fe'==38 {
				local FEs = "sp_group#rt_large_ag sp_group#month basin_group#year wdist_group#year modate sp_group#c.modate"
			}


			// Loop over alternative RHS specifications, including IVs
			foreach rhs in 1 7 19 /*8 18 */ /*15 16 17*/ {
			
				if `rhs'==1 {
					local RHS = "log_p_mean"
				}
				if `rhs'==2 {
					local RHS = "log_p_min log_p_max"
				}
				if `rhs'==3 {
					local RHS = "log_p_mean log_p_min log_p_max"
				}
				if `rhs'==4 {
					local RHS = "c.log_p_mean#i.summer"
				}
				if `rhs'==5 {
					local RHS = "(log_p_mean = c.log_mean_p_kwh_e1_lo#i.rt_group c.log_mean_p_kwh_e1_hi#i.rt_group)"
				}
				if `rhs'==6 {
					local RHS = "(log_p_mean = c.log_mean_p_kwh_e20#i.rt_group)"
				}
				if `rhs'==7 {
					local RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
				}
				if `rhs'==8 {
					local RHS = "(log_p_mean = log_mean_p_kwh_ag_default log_min_p_kwh_ag_default log_max_p_kwh_ag_default)"
				}
				if `rhs'==9 {
					local RHS = "c.log_p_mean#i.sp_same_rate_dumbsmart"
				}
				if `rhs'==10 {
					local RHS = "(log_p_mean = log_p_mean_lag12)"
				}
				if `rhs'==11 {
					local RHS = "(log_p_mean = log_p_m*_lag12 log_p_m*_lag6)"
				}
				if `rhs'==12 {
					local RHS = "(log_p_mean = log_mean_p_kwh_init)"
				}
				if `rhs'==13 {
					local RHS = "(log_p_mean = log_m*_p_kwh_init)"
				}
				if `rhs'==14 {
					local RHS = "log_p_mean ctrl_fxn_logs"
				}
				if `rhs'==15 {
					local RHS = "log_p_mean degreesC_* "
				}
				if `rhs'==16 {
					local RHS = "(log_p_mean = log_mean_p_kwh_ag_default log_min_p_kwh_ag_default log_max_p_kwh_ag_default) degreesC_* "
				}
				if `rhs'==17 {
					local RHS = "(log_p_mean = log_p_m*_lag12 log_p_m*_lag6) degreesC_* "
				}
				if `rhs'==18 {
					local RHS = "(log_p_mean = log_p_m*_deflag*)"
				}
				if `rhs'==19 {
					local RHS = "(log_p_mean = log_p_mean_deflag*)"
				}

				// Skip combinations of IV and switchers/rate FE interactions
				local skip = ""
				*if regexm("`if_sample'","sp_same")==1 & regexm("`RHS'"," = ") {
				*	local skip = "skip"
				*}
				if regexm("`if_sample'","sp_same")==1 & regexm("`RHS'","sp_same") {
					local skip = "skip"
				}
				if regexm("`if_sample'","summer")==1 & regexm("`RHS'","summer") {
					local skip = "skip"
				}
				*if regexm("`FEs'","rt_group")==1 & regexm("`RHS'"," = ") {
				*	local skip = "skip"
				*}
				
				// Skip regressions that are already stored in output file
				preserve
				cap {
					use "$dirpath_data/results/regs_Qelec_binary_Pelec_bigloop.dta", clear
					count if panel=="monthly" & pull=="`pull'" & sample=="`if_sample'" & fes=="`FEs'" & rhs=="`RHS'"
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
					reghdfe elec_binary `RHS' `if_sample', absorb(`FEs') vce(cluster sp_group modate)
					
					// Store output
					preserve
					clear
					set obs 1
					gen panel = "monthly"
					gen pull = "`pull'"
					gen sample = "`if_sample'"
					gen fes = "`FEs'"
					gen rhs = "`RHS'"
					if regexm("`RHS'","log_p_mean") & "`RHS'"!="c.log_p_mean#i.summer" & "`RHS'"!="c.log_p_mean#i.sp_same_rate_dumbsmart" {
						gen beta_log_p_mean = _b[log_p_mean]
						gen se_log_p_mean = _se[log_p_mean]
						gen t_log_p_mean =  _b[log_p_mean]/_se[log_p_mean]
					}
					if regexm("`RHS'","log_p_min") {
						gen beta_log_p_min = _b[log_p_min]
						gen se_log_p_min = _se[log_p_min]
						gen t_log_p_min =  _b[log_p_min]/_se[log_p_min]
					}
					if regexm("`RHS'","log_p_max") {
						gen beta_log_p_max = _b[log_p_max]
						gen se_log_p_max = _se[log_p_max]
						gen t_log_p_max =  _b[log_p_max]/_se[log_p_max]
					}
					if "`RHS'"=="c.log_p_mean#i.summer" {
						gen beta_log_p_mean_summer = _b[1.summer#c.log_p_mean]
						gen se_log_p_mean_summer = _se[1.summer#c.log_p_mean]
						gen t_log_p_mean_summer =  _b[1.summer#c.log_p_mean]/_se[1.summer#c.log_p_mean]
						gen beta_log_p_mean_winter = _b[0.summer#c.log_p_mean]
						gen se_log_p_mean_winter = _se[0.summer#c.log_p_mean]
						gen t_log_p_mean_winter =  _b[0.summer#c.log_p_mean]/_se[0.summer#c.log_p_mean]
					}
					if "`RHS'"=="c.log_p_mean#i.sp_same_rate_dumbsmart" {
						gen beta_log_p_mean_stayer = _b[1.sp_same_rate_dumbsmart#c.log_p_mean]
						gen se_log_p_mean_stayer = _se[1.sp_same_rate_dumbsmart#c.log_p_mean]
						gen t_log_p_mean_stayer =  _b[1.sp_same_rate_dumbsmart#c.log_p_mean]/_se[1.sp_same_rate_dumbsmart#c.log_p_mean]
						gen beta_log_p_mean_switcher = _b[0.sp_same_rate_dumbsmart#c.log_p_mean]
						gen se_log_p_mean_switcher = _se[0.sp_same_rate_dumbsmart#c.log_p_mean]
						gen t_log_p_mean_switcher =  _b[0.sp_same_rate_dumbsmart#c.log_p_mean]/_se[0.sp_same_rate_dumbsmart#c.log_p_mean]
					}
					gen n_obs = e(N)
					gen n_SPs = e(N_clust1)
					gen n_modates = e(N_clust2)
					gen dof = e(df_r)
					cap append using "$dirpath_data/results/regs_Qelec_binary_Pelec_bigloop.dta"
					duplicates drop 
					compress
					save "$dirpath_data/results/regs_Qelec_binary_Pelec_bigloop.dta", replace
					restore
				}
				
				// Run IV specificaitons	
				if "`skip'"=="" & "`iv'"=="iv" {
				
					// Run regression
					ivreghdfe elec_binary `RHS' `if_sample', absorb(`FEs') cluster(sp_group modate) 

					// Store output
					preserve
					clear
					set obs 1
					gen panel = "monthly"
					gen pull = "`pull'"
					gen sample = "`if_sample'"
					gen fes = "`FEs'"
					gen rhs = "`RHS'"
					gen beta_log_p_mean = _b[log_p_mean]
					gen se_log_p_mean = _se[log_p_mean]
					gen t_log_p_mean =  _b[log_p_mean]/_se[log_p_mean]
					gen n_obs = e(N)
					gen n_SPs = e(N_clust1)
					gen n_modates = e(N_clust2)
					gen dof = e(df_r)
					gen fstat_rk = e(rkf)
					gen fstat_cd = e(cdf)
					cap append using "$dirpath_data/results/regs_Qelec_binary_Pelec_bigloop.dta"
					duplicates drop 
					compress
					save "$dirpath_data/results/regs_Qelec_binary_Pelec_bigloop.dta", replace
					restore
				}		
			}
		}
	}
}	

}

************************************************
************************************************
