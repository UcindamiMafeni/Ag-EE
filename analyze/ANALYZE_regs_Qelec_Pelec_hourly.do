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

** 1. Hourly regressions
{ 

// Outfile
local outfile = "regs_Qelec_Pelec_hourly"

// Loop over data pulls
foreach pull in "20180719" /*"20180322" "20180827"*/ {

	// Loop through sample restrictions
	foreach ifs in 6 7 8 {
	
		if `ifs'==1 {
			local if_sample = ""
		}
		if `ifs'==2 {
			local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_interval_disp20==0 & inrange(interval_bill_corr,0.9,1)"
		}
		if `ifs'==3 {
			local if_sample = "if sp_same_rate_dumbsmart==1"
		}
		if `ifs'==4 {
			local if_sample = "if sp_same_rate_dumbsmart==0 & sp_same_rate_in_cat==1"
		}
		if `ifs'==5 {
			local if_sample = "if sp_same_rate_in_cat==0"
		}
		if `ifs'==6 {
			local if_sample = "if sp_same_rate_dumbsmart==1 & flag_nem==0 & flag_geocode_badmiss==0 & flag_interval_disp20==0 & inrange(interval_bill_corr,0.9,1)"
		}
		if `ifs'==7 {
			local if_sample = "if sp_same_rate_dumbsmart==0 & sp_same_rate_in_cat==1 & flag_nem==0 & flag_geocode_badmiss==0 & flag_interval_disp20==0 & inrange(interval_bill_corr,0.9,1)"
		}
		if `ifs'==8 {
			local if_sample = "if sp_same_rate_in_cat==0 & flag_nem==0 & flag_geocode_badmiss==0 & flag_interval_disp20==0 & inrange(interval_bill_corr,0.9,1)"
		}
		
		// Load monthly panel
		use "$dirpath_data/merged/sp_hourly_elec_panel_collapsed_`pull'.dta", clear
		if "`if_sample'"!="" {
			keep `if_sample'
		}
		
		// Loop over different combinations of fixed effects and interactions thereof
		foreach fe in /*11 12 13 14 15 16*/ 17 {
		
			if `fe'==1 {
				local FEs = "sp_group#month modate"
			}
			if `fe'==2 {
				local FEs = "sp_group#month basin_group#year modate"
			}
			if `fe'==3 {
				local FEs  = "sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year modate"
			}
			if `fe'==4 {
				local FEs = "sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year modate"
			}
			if `fe'==5 {
				local FEs = "sp_group#month##c.gw_qtr_bsn_mean2 sp_group#year modate"
			}
			if `fe'==6 {
				local FEs = "sp_group#month#hour modate"
			}
			if `fe'==7 {
				local FEs = "sp_group#month#hour basin_group#year modate"
			}
			if `fe'==8 {
				local FEs  = "sp_group#month#hour##c.gw_qtr_bsn_mean2 basin_group#year modate"
			}
			if `fe'==9 {
				local FEs = "sp_group#month#hour##c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year modate"
			}
			if `fe'==10 {
				local FEs = "sp_group#month#hour##c.gw_qtr_bsn_mean2 sp_group#year modate"
			}
			if `fe'==11 {
				local FEs  = "sp_group#month#hour sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year modate"
			}
			if `fe'==12 {
				local FEs = "sp_group#month#hour sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year modate"
			}
			if `fe'==13 {
				local FEs = "sp_group#month#hour sp_group#month##c.gw_qtr_bsn_mean2 sp_group#year modate"
			}
			if `fe'==14 {
				local FEs  = "sp_group#month#hour sp_group##c.gw_qtr_bsn_mean2 basin_group#year modate"
			}
			if `fe'==15 {
				local FEs = "sp_group#month#hour sp_group##c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year modate"
			}
			if `fe'==16 {
				local FEs = "sp_group#month#hour sp_group##c.gw_qtr_bsn_mean2 sp_group#year modate"
			}
			if `fe'==17 {
				local FEs = "sp_group#month#hour basin_group#year wdist_group#year modate"
			}

			// Loop over alternative RHS specifications, including IVs
			foreach rhs in 1 2 3 {
			
				if `rhs'==1 {
					local RHS = "log_p"
				}
				if `rhs'==2 {
					local RHS = "(log_p = log_p_kwh_ag_default)"
				}
				if `rhs'==3 {
					local RHS = "(log_p = log_p_lag12 log_p_lag6)"
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
				if regexm("`if_sample'","sp_same_rate_dumbsmart==1") & regexm("`RHS'"," = ") {
					local skip = "skip"
				}
				if regexm("`if_sample'","sp_same_rate_dumbsmart==0 & sp_same_rate_in_cat==1") & regexm("`RHS'","default") {
					local skip = "skip"
				}
				if regexm("`if_sample'","sp_same_rate_in_cat==0") & regexm("`RHS'","_lag") {
					local skip = "skip"
				}
				
				// Skip regressions that are already stored in output file
				preserve
				cap {
					use "$dirpath_data/results/`outfile'.dta", clear
					count if panel=="hourly" & pull=="`pull'" & sample=="`if_sample'" & fes=="`FEs'" & rhs=="`RHS'"
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
					reghdfe ihs_kwh `RHS' [fw=fwt], absorb(`FEs') vce(cluster sp_group modate)

					// Store output
					preserve
					clear
					set obs 1
					gen panel = "hourly"
					gen pull = "`pull'"
					gen sample = "`if_sample'"
					gen fes = "`FEs'"
					gen rhs = "`RHS'"
					if regexm("`RHS'","log_p") & "`RHS'"!="c.log_p#i.summer" & "`RHS'"!="c.log_p#i.sp_same_rate_dumbsmart" {
						gen beta_log_p = _b[log_p]
						gen se_log_p = _se[log_p]
						gen t_log_p =  _b[log_p]/_se[log_p]
					}
					if "`RHS'"=="c.log_p#i.summer" {
						gen beta_log_p_summer = _b[1.summer#c.log_p]
						gen se_log_p_summer = _se[1.summer#c.log_p]
						gen t_log_p_summer =  _b[1.summer#c.log_p]/_se[1.summer#c.log_p]
						gen beta_log_p_winter = _b[0.summer#c.log_p]
						gen se_log_p_winter = _se[0.summer#c.log_p]
						gen t_log_p_winter =  _b[0.summer#c.log_p]/_se[0.summer#c.log_p]
					}
					if "`RHS'"=="c.log_p#i.sp_same_rate_dumbsmart" {
						gen beta_log_p_stayer = _b[1.sp_same_rate_dumbsmart#c.log_p]
						gen se_log_p_stayer = _se[1.sp_same_rate_dumbsmart#c.log_p]
						gen t_log_p_stayer =  _b[1.sp_same_rate_dumbsmart#c.log_p]/_se[1.sp_same_rate_dumbsmart#c.log_p]
						gen beta_log_p_switcher = _b[0.sp_same_rate_dumbsmart#c.log_p]
						gen se_log_p_switcher = _se[0.sp_same_rate_dumbsmart#c.log_p]
						gen t_log_p_switcher =  _b[0.sp_same_rate_dumbsmart#c.log_p]/_se[0.sp_same_rate_dumbsmart#c.log_p]
					}
					gen n_obs = e(N)
					gen n_SPs = e(N_clust1)
					gen n_modates = e(N_clust2)
					gen dof = e(df_r)
					cap append using "$dirpath_data/results/`outfile'.dta"
					duplicates drop 
					compress
					save "$dirpath_data/results/`outfile'.dta", replace
					restore
				}
				
				// Run IV specificaitons	
				if "`skip'"=="" & "`iv'"=="iv" {
				
					// Run regression
					ivreghdfe ihs_kwh `RHS' [fw=fwt], absorb(`FEs') cluster(sp_group modate)

					// Store output
					preserve
					clear
					set obs 1
					gen panel = "hourly"
					gen pull = "`pull'"
					gen sample = "`if_sample'"
					gen fes = "`FEs'"
					gen rhs = "`RHS'"
					gen beta_log_p = _b[log_p]
					gen se_log_p = _se[log_p]
					gen t_log_p =  _b[log_p]/_se[log_p]
					gen n_obs = e(N)
					gen n_SPs = e(N_clust1)
					gen n_modates = e(N_clust2)
					gen dof = e(df_r)
					gen fstat_rk = e(rkf)
					gen fstat_cd = e(cdf)
					cap append using "$dirpath_data/results/`outfile'.dta"
					duplicates drop 
					compress
					save "$dirpath_data/results/`outfile'.dta", replace
					restore
				}		
			}
		}
	}
}	

}

************************************************
************************************************
