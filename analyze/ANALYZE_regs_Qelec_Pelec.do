clear all
version 13
set more off

********************************************************************
** Script to run a ton of regressions estimating price elasticity **
**  of demand, using electricity data only (not APEP stuff yet)   **
********************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

************************************************
************************************************

** 1. Monthly regressions
{ 

// Loop over data pulls
foreach pull in "20180719" "20180322" "20180827" "combined" {

	// Load monthly panel
	use "$dirpath_data/merged/sp_month_elec_panel.dta", clear
	
	// Keep only observations in relevant pull
	if "`pull'"!="combined" {
		drop if pull!="`pull'"
	}
	
	// Loop through sample restrictions
	forvalues ifs = 1/6 {
	
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
			local if_sample = "if sp_same_in_cat==1"
		}
		if `ifs'==9 {
			local if_sample = "if sp_same_rate_dumbsmart==0"
		}
		if `ifs'==10 {
			local if_sample = "if sp_same_in_cat==0"
		}
		if `ifs'==11 {
			local if_sample = "if sp_same_rate_dumbsmart==1 & flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0"
		}
		if `ifs'==12 {
			local if_sample = "if sp_same_in_cat==1 & flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0"
		}
		if `ifs'==13 {
			local if_sample = "if sp_same_rate_dumbsmart==0 & flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0"
		}
		if `ifs'==14 {
			local if_sample = "if sp_same_in_cat==0 & flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0"
		}
		
		// Loop over different combinations of fixed effects and interactions thereof
		forvalues fe = 1/16 {
		
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

			// Loop over alternative RHS specifications, including IVs
			foreach rhs in 1 2 3 4 7 8 {
			
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

				// Skip combinations of IV and switchers/rate FE interactions
				local skip = ""
				if regexm("`if_sample'","sp_same")==1 & regexm("`RHS'"," = ") {
					local skip = "skip"
				}
				if regexm("`FEs'","rt_group")==1 & regexm("`RHS'"," = ") {
					local skip = "skip"
				}
				
				// Flag IV specificaitons, which require different syntax
				local iv = ""
				if regexm("`RHS'"," = ") {
					local iv = "iv"
				}

				// Run non-IV specificaitons	
				if "`skip'"=="" & "`iv'"=="" {
				
					// Run regression
					reghdfe ihs_kwh `RHS' `if_sample', absorb(`FEs') vce(cluster sp_group modate)
					
					// Store output
					preserve
					clear
					set obs 1
					gen panel = "monthly"
					gen pull = "`pull'"
					gen sample = "`if_sample'"
					gen fes = "`FEs'"
					gen rhs = "`RHS'"
					if regexm("`RHS'","log_p_mean") & "`RHS'"!="c.log_p_mean#i.summer" {
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
						gen beta_log_p_mean_wintr = _b[0.summer#c.log_p_mean]
						gen se_log_p_mean_winter = _se[0.summer#c.log_p_mean]
						gen t_log_p_mean_winter =  _b[0.summer#c.log_p_mean]/_se[0.summer#c.log_p_mean]
					}
					gen n_obs = e(N)
					gen n_SPs = e(N_clust1)
					gen n_modates = e(N_clust2)
					gen dof = e(df_r)
					cap append using "$dirpath_data/results/regs_Qelec_Pelec.dta"
					duplicates drop 
					compress
					save "$dirpath_data/results/regs_Qelec_Pelec.dta", replace
					restore
				}
				
				// Run IV specificaitons	
				if "`skip'"=="" & "`iv'"=="iv" {
				
					// Run regression
					ivreghdfe ihs_kwh `RHS' `if_sample', absorb(`FEs') cluster(sp_group modate) 

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
					cap append using "$dirpath_data/results/regs_Qelec_Pelec.dta"
					duplicates drop 
					compress
					save "$dirpath_data/results/regs_Qelec_Pelec.dta", replace
					restore
				}		
			}
		}
	}
}	

}

************************************************
************************************************
