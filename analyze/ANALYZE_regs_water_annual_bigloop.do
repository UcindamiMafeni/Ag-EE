clear all
version 13
set more off

*************************************************************************************
** Script to run a ton of regressions estimating annual price elasticity of demand **
**                        using electricity and water data                         **
*************************************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

************************************************
************************************************

// Loop over datasets
foreach data in /*sp clu parcel*/ clu75 {
	
	use "$dirpath_data/merged/`data'_annual_water_panel.dta", clear

	
	// Loop through sample restrictions
	foreach ifs in 1 2 3 4 5 6 7 8 9 10 {
	
		if `ifs'==1 {
			local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & flag_weird_pump==0"
		}
		if `ifs'==2 {
			local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & flag_weird_pump==0 & elec_binary_frac==1"
		}
		if `ifs'==3 {
			local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & flag_weird_pump==0 & elec_binary_frac>0.95"
		}
		if `ifs'==4 {
			local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & flag_weird_pump==0 & elec_binary_frac>0.9"
		}
		if `ifs'==5 {
			local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & flag_weird_pump==0 & ever50_Annual==1 & ever50_FruitNutPerennial==0 & ever50_OtherPerennial==0 & ever50_Noncrop==0"
		}
		if `ifs'==6 {
			local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & flag_weird_pump==0 & ever50_Annual==0 & (ever50_FruitNutPerennial==1 | ever50_OtherPerennial==1) & ever50_Noncrop==0"
		}
		if `ifs'==7 {
			local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & flag_weird_pump==0 & ever50_Annual==0 & ever50_FruitNutPerennial==0 & ever50_OtherPerennial==0 & ever50_Noncrop==1"
		}
		if `ifs'==8 {
			local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & flag_weird_pump==0 & ever50_Annual==0"
		}
		if `ifs'==9 {
			local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & flag_weird_pump==0 & ever50_FruitNutPerennial==0 & ever50_OtherPerennial==0"
		}
		if `ifs'==10 {
			local if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & flag_weird_pump==0 & mode50_switcher==1"
		}
		
		
		// Loop over dependent variables
		foreach depvar in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 {
			
			if `depvar'==1 {
				local DEPVAR = "ihs_af_rast_dd_mth_2SP"
			}
			if `depvar'==2 {
				local DEPVAR = "elec_binary"
			}
			if `depvar'==3 {
				local DEPVAR = "mode_Annual"
			}
			if `depvar'==4 {
				local DEPVAR = "mode50_Annual"
			}
			if `depvar'==5 {
				local DEPVAR = "mode_FruitNutPerennial"
			}
			if `depvar'==6 {
				local DEPVAR = "mode50_FruitNutPerennial"
			}
			if `depvar'==7 {
				local DEPVAR = "mode_OtherPerennial"
			}
			if `depvar'==8 {
				local DEPVAR = "mode50_OtherPerennial"
			}
			if `depvar'==9 {
				local DEPVAR = "mode_Noncrop"
			}
			if `depvar'==10 {
				local DEPVAR = "mode50_Noncrop"
			}
			if `depvar'==11 {
				local DEPVAR = "fraction_crop_planted"
			}
			if `depvar'==12 {
				local DEPVAR = "frac_Annual"
			}
			if `depvar'==13 {
				local DEPVAR = "frac_FruitNutPerennial"
			}
			if `depvar'==14 {
				local DEPVAR = "frac_OtherPerennial"
			}
			if `depvar'==15 {
				local DEPVAR = "frac_Noncrop"
			}
		
		
			// Loop over different combinations of fixed effects and interactions thereof
			foreach fe in 1 2 /*3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19*/ 20 21 22 23 24 25 {
			
				if `fe'==1 {
					local FEs = "constant"
				}
				if `fe'==2 {
					local FEs = "year"
				}
				if `fe'==3 {
					local FEs = "sp_group"
				}
				if `fe'==4 {
					local FEs = "sp_group year"
				}
				if `fe'==5 {
					local FEs = "sp_group#rt_large_ag year"
				}
				if `fe'==6 {
					local FEs = "sp_group#rt_large_ag basin_group#year wdist_group#year"
				}
				if `fe'==7 {
					local FEs = "sp_group#rt_large_ag year sp_group#c.year"
				}
				if `fe'==8 {
					local FEs = "clu_id"
				}
				if `fe'==9 {
					local FEs = "clu_id year"
				}
				if `fe'==10 {
					local FEs = "clu_id#clu_sp_group year"
				}
				if `fe'==11 {
					local FEs = "clu_id#clu_sp_group#rt_large_ag year"
				}
				if `fe'==12 {
					local FEs = "clu_id#clu_sp_group#rt_large_ag basin_group#year wdist_group#year"
				}
				if `fe'==13 {
					local FEs = "clu_id#clu_sp_group#rt_large_ag year clu_id#c.year"
				}
				if `fe'==14 {
					local FEs = "parcelid_conc"
				}
				if `fe'==15 {
					local FEs = "parcelid_conc year"
				}
				if `fe'==16 {
					local FEs = "parcelid_conc#parcel_sp_group year"
				}
				if `fe'==17 {
					local FEs = "parcelid_conc#parcel_sp_group#rt_large_ag year"
				}
				if `fe'==18 {
					local FEs = "parcelid_conc#parcel_sp_group#rt_large_ag basin_group#year wdist_group#year"
				}
				if `fe'==19 {
					local FEs = "parcelid_conc#parcel_sp_group#rt_large_ag year parcelid_conc#c.year"
				}
				if `fe'==20 {
					local FEs = "clu_group75"
				}
				if `fe'==21 {
					local FEs = "clu_group75 year"
				}
				if `fe'==22 {
					local FEs = "clu_group75#clu75_sp_group year"
				}
				if `fe'==23 {
					local FEs = "clu_group75#clu75_sp_group#rt_large_ag year"
				}
				if `fe'==24 {
					local FEs = "clu_group75#clu75_sp_group#rt_large_ag basin_group#year wdist_group#year"
				}
				if `fe'==25 {
					local FEs = "clu_group75#clu75_sp_group#rt_large_ag year clu_group75#c.year"
				}


				// Loop over alternative RHS specifications, including IVs
				foreach rhs in 1 2 3 {
				
					if `rhs'==1 {
						local RHS = "ln_mn_p_af_rast_dd_mth_2SP"
					}
					if `rhs'==2 {
						local RHS = "(ln_mn_p_af_rast_dd_mth_2SP = log_mean_p_kwh_ag_default)"
					}
					if `rhs'==3 {
						local RHS = "(ln_mn_p_af_rast_dd_mth_2SP = log_p_mean_deflag)"
					}

					
					// Loop over cluster variables
					foreach vce in /*1 2 3*/ 4 {

						if `vce'==1 {
							local VCE = "sp_group year"	
						}
						if `vce'==2 {
							local VCE = "clu_id year"	
						}
						if `vce'==3 {
							local VCE = "parcelid_conc year"	
						}
						if `vce'==4 {
							local VCE = "clu_group75 year"	
						}
						
						
						// Create constant variable if it is needed and does not exist
						if "`FEs'"=="constant" {
							cap gen constant = 1
						}
					
					
						// Skip invalid combinations of inputs
						local skip = ""
						if "`DEPVAR'"=="elec_binary" & strpos("`if_sample'","elec_binary")>0 {
							local skip = "skip"
						}
						if (strpos("`if_sample'","ever")>0 | strpos("`if_sample'","mode")>0) & (strpos("`DEPVAR'","mode")>0 | strpos("`DEPVAR'","frac")>0) {
							local skip = "skip"
						}
						
						
						// Skip regressions that are already stored in output file
						preserve
						cap {
							use "$dirpath_data/results/regs_water_annual_bigloop.dta", clear
							count if data=="`data'" & sample=="`if_sample'" & depvar=="`DEPVAR'" & fes=="`FEs'" & rhs=="`RHS'" & vce=="`VCE'"
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
							reghdfe `DEPVAR' `RHS' `if_sample', absorb(`FEs') vce(cluster `VCE')
							
							// Store output
							preserve
							clear
							set obs 1
							gen data = "`data'"
							gen sample = "`if_sample'"
							gen depvar = "`DEPVAR'"
							gen fes = "`FEs'"
							gen rhs = "`RHS'"
							gen vce = "`VCE'"
							gen beta_log_p_mean = _b[ln_mn_p_af_rast_dd_mth_2SP]
							gen se_log_p_mean = _se[ln_mn_p_af_rast_dd_mth_2SP]
							gen t_log_p_mean =  _b[ln_mn_p_af_rast_dd_mth_2SP]/_se[ln_mn_p_af_rast_dd_mth_2SP]
							gen n_obs = e(N)
							gen n_units = e(N_clust1)
							gen n_years = e(N_clust2)
							gen dof = e(df_r)
							cap append using "$dirpath_data/results/regs_water_annual_bigloop.dta"
							duplicates drop 
							compress
							save "$dirpath_data/results/regs_water_annual_bigloop.dta", replace
							restore
						}
						
						
						// Run IV specificaitons	
						if "`skip'"=="" & "`iv'"=="iv" {
						
							// Run regression
							ivreghdfe `DEPVAR' `RHS' `if_sample', absorb(`FEs') cluster(`VCE') 

							// Store output
							preserve
							clear
							set obs 1
							gen data = "`data'"
							gen sample = "`if_sample'"
							gen depvar = "`DEPVAR'"
							gen fes = "`FEs'"
							gen rhs = "`RHS'"
							gen vce = "`VCE'"
							gen beta_log_p_mean = _b[ln_mn_p_af_rast_dd_mth_2SP]
							gen se_log_p_mean = _se[ln_mn_p_af_rast_dd_mth_2SP]
							gen t_log_p_mean =  _b[ln_mn_p_af_rast_dd_mth_2SP]/_se[ln_mn_p_af_rast_dd_mth_2SP]
							gen n_obs = e(N)
							gen n_units = e(N_clust1)
							gen n_years = e(N_clust2)
							gen dof = e(df_r)
							gen fstat_rk = e(rkf)
							gen fstat_cd = e(cdf)
							cap append using "$dirpath_data/results/regs_water_annual_bigloop.dta"
							duplicates drop 
							compress
							save "$dirpath_data/results/regs_water_annual_bigloop.dta", replace
							restore
						}		
					}
				}
			}
		}
	}
}
