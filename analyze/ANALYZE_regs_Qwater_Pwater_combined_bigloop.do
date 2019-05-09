clear all
version 13
set more off

********************************************************************
** Script to run a ton of regressions estimating price elasticity **
**  of demand, using elec data & estimated KWH/AF converson rates **
********************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

************************************************
************************************************

** 1. Monthly regressions
{ 

// Load monthly panel
use "$dirpath_data/merged/sp_month_water_panel.dta", clear

// Loop through sample restrictions
foreach ifs in 9 10 11 8 7 /*1 2 3 4 5 6*/ {

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
	if `ifs'==7 {
		local if_sample = ""
	}
	if `ifs'==8 {
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0"
	}
	if `ifs'==9 {
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	}
	if `ifs'==10 {
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & inlist(basin_group,68,121,122)"
	}
	if `ifs'==11 {
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & apep_proj_count==0"
	}
	
	// Loop over different combinations of fixed effects and interactions thereof
	foreach fe in 1 2 3 4 5 6 /* 7 8 9 10 11 12*/ {
	
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
			local FEs = "sp_group#month sp_group#rt_large_ag modate"
		}
		if `fe'==5 {
			local FEs = "sp_group#month sp_group#rt_large_ag wdist_group#year modate"
		}
		if `fe'==6 {
			local FEs = "sp_group#month sp_group#rt_large_ag wdist_group#year basin_group#year modate"
		}
		if `fe'==7 {
			local FEs = "sp_group#month modate sp_group#c.modate"
		}
		if `fe'==8 {
			local FEs = "sp_group#month wdist_group#year modate sp_group#c.modate"
		}
		if `fe'==9 {
			local FEs = "sp_group#month wdist_group#year basin_group#year modate sp_group#c.modate"
		}
		if `fe'==10 {
			local FEs = "sp_group#month sp_group#rt_large_ag modate sp_group#c.modate"
		}
		if `fe'==11 {
			local FEs = "sp_group#month sp_group#rt_large_ag wdist_group#year modate sp_group#c.modate"
		}
		if `fe'==12 {
			local FEs = "sp_group#month sp_group#rt_large_ag wdist_group#year basin_group#year modate sp_group#c.modate"
		}


		// Loop over endogenous water price variables
		foreach p_af_var in 2 4 /*1 6 10*/ {
		
			if `p_af_var'==1 {
				local P_AF = "ln_mean_p_af_rast_dd_mth_1SP"
			}	
			if `p_af_var'==2 {
				local P_AF = "ln_mean_p_af_rast_dd_mth_2SP" // ex ante default
			}			
			if `p_af_var'==3 {
				local P_AF = "ln_mean_p_af_rast_dd_qtr_1SP"
			}			
			if `p_af_var'==4 {
				local P_AF = "ln_mean_p_af_rast_dd_qtr_2SP"
			}			
			if `p_af_var'==5 {
				local P_AF = "ln_mean_p_af_rast_ddhat_mth_1SP"
			}	
			if `p_af_var'==6 {
				local P_AF = "ln_mean_p_af_rast_ddhat_mth_2SP"
			}			
			if `p_af_var'==7 {
				local P_AF = "ln_mean_p_af_rast_ddhat_qtr_1SP"
			}			
			if `p_af_var'==8 {
				local P_AF = "ln_mean_p_af_rast_ddhat_qtr_2SP"
			}			
			if `p_af_var'==9 {
				local P_AF = "ln_mean_p_af_rast_dd_mth_1"
			}	
			if `p_af_var'==10 {
				local P_AF = "ln_mean_p_af_rast_dd_mth_2" 
			}			
			if `p_af_var'==11 {
				local P_AF = "ln_mean_p_af_rast_dd_qtr_1"
			}			
			if `p_af_var'==12 {
				local P_AF = "ln_mean_p_af_rast_dd_qtr_2"
			}			
			if `p_af_var'==13 {
				local P_AF = "ln_mean_p_af_rast_ddhat_mth_1"
			}	
			if `p_af_var'==14 {
				local P_AF = "ln_mean_p_af_rast_ddhat_mth_2"
			}			
			if `p_af_var'==15 {
				local P_AF = "ln_mean_p_af_rast_ddhat_qtr_1"
			}			
			if `p_af_var'==16 {
				local P_AF = "ln_mean_p_af_rast_ddhat_qtr_2"
			}			
	
			local depvar = subinstr("`P_AF'","ln_mean_p_af","ihs_af",1)
			
			// Loop over alternative RHS specifications
			foreach rhs in 1 2 3 {

				if `rhs'==1 {
					local RHS = "`P_AF'"
				}
				if `rhs'==2 {
					local RHS = "(`P_AF' = log_mean_p_kwh_ag_default)"
				}
				if `rhs'==3 {
					local RHS = "(`P_AF' = log_p_mean_deflag*)"
				}
				
				// Skip regressions that are already stored in output file
				local skip = ""
				preserve
				cap {
					use "$dirpath_data/results/regs_Qwater_Pwater_combined.dta", clear
					noi noi count if sample=="`if_sample'" & fes=="`FEs'" & rhs=="`RHS'" & depvar=="`depvar'"
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
					gen sample = "`if_sample'"
					gen fes = "`FEs'"
					gen rhs = "`RHS'"
					gen depvar = "`depvar'"
					gen beta_log_p_af = _b[`P_AF']
					gen se_log_p_af = _se[`P_AF']
					gen t_log_p_af =  _b[`P_AF']/_se[`P_AF']
					gen n_obs = e(N)
					gen n_SPs = e(N_clust1)
					gen n_modates = e(N_clust2)
					gen dof = e(df_r)
					cap append using "$dirpath_data/results/regs_Qwater_Pwater_combined.dta"
					duplicates drop 
					compress
					save "$dirpath_data/results/regs_Qwater_Pwater_combined.dta", replace
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
					gen sample = "`if_sample'"
					gen fes = "`FEs'"
					gen rhs = "`RHS'"
					gen depvar = "`depvar'"
					gen beta_log_p_af = _b[`P_AF']
					gen se_log_p_af = _se[`P_AF']
					gen t_log_p_af =  _b[`P_AF']/_se[`P_AF']
					gen n_obs = e(N)
					gen n_SPs = e(N_clust1)
					gen n_modates = e(N_clust2)
					gen dof = e(df_r)
					gen fstat_rk = e(rkf)
					gen fstat_cd = e(cdf)
					cap append using "$dirpath_data/results/regs_Qwater_Pwater_combined.dta"
					duplicates drop 
					compress
					save "$dirpath_data/results/regs_Qwater_Pwater_combined.dta", replace
					restore
				}	
			}		
		}
	}	
}


}

************************************************
************************************************
