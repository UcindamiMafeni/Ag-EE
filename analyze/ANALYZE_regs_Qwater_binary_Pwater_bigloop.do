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
foreach ifs in 18 /*1 2 3 4 5 6 7 8 9 10 11*/ {

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
	if `ifs'==12 {
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & elec_binary_frac > 0.95"
	}
	if `ifs'==13 {
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & elec_binary_frac > 0.9"
	}
	if `ifs'==14 {
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & annual_always == 1"
	}
	if `ifs'==15 {
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & perennial_always == 1"
	}
	if `ifs'==16 {
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & perennial_ever == 0"
	}
	if `ifs'==17 {
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & annual_ever == 0"
	}
	if `ifs'==18 {
		local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & ann_per_switcher == 1"
	}
	
	// Loop over different combinations of fixed effects and interactions thereof
	foreach fe in 4 6 /* 1 2 3 5 7 8 9 10 11 12*/ {
	
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


		// Loop over endogenous kwhaf variables
		foreach kwhaf_var in 2 /*4 1 6 10*/ {
		
			if `kwhaf_var'==1 {
				local KWHAF = "ln_kwhaf_rast_dd_mth_1SP"
			}	
			if `kwhaf_var'==2 {
				local KWHAF = "ln_kwhaf_rast_dd_mth_2SP" // ex ante default
			}			
			if `kwhaf_var'==3 {
				local KWHAF = "ln_kwhaf_rast_dd_qtr_1SP"
			}			
			if `kwhaf_var'==4 {
				local KWHAF = "ln_kwhaf_rast_dd_qtr_2SP"
			}			
			if `kwhaf_var'==5 {
				local KWHAF = "ln_kwhaf_rast_ddhat_mth_1SP"
			}	
			if `kwhaf_var'==6 {
				local KWHAF = "ln_kwhaf_rast_ddhat_mth_2SP"
			}			
			if `kwhaf_var'==7 {
				local KWHAF = "ln_kwhaf_rast_ddhat_qtr_1SP"
			}			
			if `kwhaf_var'==8 {
				local KWHAF = "ln_kwhaf_rast_ddhat_qtr_2SP"
			}			
			if `kwhaf_var'==9 {
				local KWHAF = "ln_kwhaf_rast_dd_mth_1"
			}	
			if `kwhaf_var'==10 {
				local KWHAF = "ln_kwhaf_rast_dd_mth_2" 
			}			
			if `kwhaf_var'==11 {
				local KWHAF = "ln_kwhaf_rast_dd_qtr_1"
			}			
			if `kwhaf_var'==12 {
				local KWHAF = "ln_kwhaf_rast_dd_qtr_2"
			}			
			if `kwhaf_var'==13 {
				local KWHAF = "ln_kwhaf_rast_ddhat_mth_1"
			}	
			if `kwhaf_var'==14 {
				local KWHAF = "ln_kwhaf_rast_ddhat_mth_2"
			}			
			if `kwhaf_var'==15 {
				local KWHAF = "ln_kwhaf_rast_ddhat_qtr_1"
			}			
			if `kwhaf_var'==16 {
				local KWHAF = "ln_kwhaf_rast_ddhat_qtr_2"
			}			
	
			// Loop over kwhaf instruments
			local KWHAF_stub = subinstr(subinstr(subinstr("`KWHAF'","ln_kwhaf_rast_","",1),"ddhat_","",1),"dd_","",1)
			foreach kwhaf_iv in 1 /*2 3 4 5 6 7*/ {
			
				if `kwhaf_iv'==1 {
					local KWHAF_IV = "ln_gw_mean_depth_`KWHAF_stub'" 
				}	
				if `kwhaf_iv'==2 {
					local KWHAF_IV = "L.ln_gw_mean_depth_`KWHAF_stub'" 
				}	
				if `kwhaf_iv'==3 {
					local KWHAF_IV = "c.ln_gw_mean_depth_`KWHAF_stub'#c.kwhaf_apep_measured_init" 
				}	
				if `kwhaf_iv'==4 {
					local KWHAF_IV = "cL.ln_gw_mean_depth_`KWHAF_stub'#c.kwhaf_apep_measured_init" 
				}	
				if `kwhaf_iv'==5 {
					local KWHAF_IV = "c.ln_gw_mean_depth_`KWHAF_stub'#c.kwhaf_apep_measured" 
				}	
				if `kwhaf_iv'==6 {
					local KWHAF_IV = "cL.ln_gw_mean_depth_`KWHAF_stub'#c.kwhaf_apep_measured" 
				}	
				if `kwhaf_iv'==7 {
					local KWHAF_IV = "L6.ln_gw_mean_depth_`KWHAF_stub' L12.ln_gw_mean_depth_`KWHAF_stub'" 
				}	
			
				// Loop over alternative RHS specifications
				foreach rhs in 2 5 /*1 4 3 6*/  {

					if `rhs'==1 {
						local RHS = "log_p_mean `KWHAF'"
					}
					if `rhs'==2 {
						local RHS = "(log_p_mean = log_mean_p_kwh_ag_default) `KWHAF'"
					}
					if `rhs'==3 {
						local RHS = "(log_p_mean = log_p_mean_deflag*) `KWHAF'"
					}
					if `rhs'==4 {
						local RHS = "(`KWHAF' = `KWHAF_IV') log_p_mean"
					}
					if `rhs'==5 {
						local RHS = "(log_p_mean `KWHAF' = log_mean_p_kwh_ag_default `KWHAF_IV')"
					}
					if `rhs'==6 {
						local RHS = "(log_p_mean `KWHAF' = log_p_mean_deflag* `KWHAF_IV')"
					}
					
					// Skip regressions that are already stored in output file
					local skip = ""
					preserve
					cap {
						use "$dirpath_data/results/regs_Qwater_binary_Pwater_bigloop.dta", clear
						noi noi count if sample=="`if_sample'" & fes=="`FEs'" & rhs=="`RHS'" & depvar=="elec_binary"
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
						gen sample = "`if_sample'"
						gen fes = "`FEs'"
						gen rhs = "`RHS'"
						gen depvar = "elec_binary"
						if regexm("`RHS'","log_p_mean") {
							gen beta_log_p_kwh = _b[log_p_mean]
							gen se_log_p_kwh = _se[log_p_mean]
							gen t_log_p_kwh =  _b[log_p_mean]/_se[log_p_mean]
						}
						if regexm("`RHS'","`KWHAF'") {
							gen beta_log_kwhaf = _b[`KWHAF']-1
							gen se_log_kwhaf = _se[`KWHAF']
							gen t_log_kwhaf =  (_b[`KWHAF']-1)/_se[`KWHAF']
						}
						gen n_obs = e(N)
						gen n_SPs = e(N_clust1)
						gen n_modates = e(N_clust2)
						gen dof = e(df_r)
						cap append using "$dirpath_data/results/regs_Qwater_binary_Pwater_bigloop.dta"
						duplicates drop 
						compress
						save "$dirpath_data/results/regs_Qwater_binary_Pwater_bigloop.dta", replace
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
						gen sample = "`if_sample'"
						gen fes = "`FEs'"
						gen rhs = "`RHS'"
						gen depvar = "elec_binary"
						if regexm("`RHS'","log_p_mean") {
							gen beta_log_p_kwh = _b[log_p_mean]
							gen se_log_p_kwh = _se[log_p_mean]
							gen t_log_p_kwh =  _b[log_p_mean]/_se[log_p_mean]
						}
						if regexm("`RHS'","`KWHAF'") {
							gen beta_log_kwhaf = _b[`KWHAF']
							gen se_log_kwhaf = _se[`KWHAF']
							gen t_log_kwhaf =  (_b[`KWHAF'])/_se[`KWHAF']
						}
						gen n_obs = e(N)
						gen n_SPs = e(N_clust1)
						gen n_modates = e(N_clust2)
						gen dof = e(df_r)
						gen fstat_rk = e(rkf)
						gen fstat_cd = e(cdf)
						cap append using "$dirpath_data/results/regs_Qwater_binary_Pwater_bigloop.dta"
						duplicates drop 
						compress
						save "$dirpath_data/results/regs_Qwater_binary_Pwater_bigloop.dta", replace
						restore
					}	
				}		
			}
		}
	}	
}


}

************************************************
************************************************
