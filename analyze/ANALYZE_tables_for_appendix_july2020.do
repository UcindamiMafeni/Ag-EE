clear all
version 13
set more off

***************************************
** Script to make tables for slides  **
***************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"
global dirpath_output "$dirpath/output"

*global dirpath "/Users/louispreonas/Dropbox/Documents/Research/Energy Water Project"
*global dirpath "E:/Dropbox/Documents/Research/Energy Water Project"
*global dirpath_data "$dirpath"
*global dirpath_output "$dirpath/slides/tables"

************************************************
************************************************

** 1. Regression results: Water (split)
{
use "$dirpath_data/results/regs_pge_water_split_monthly_july2020.dta" , clear

keep if panel=="monthly"
keep if pull=="PGE 20180719"
keep if ifs_base=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
keep if regexm(fes,"sp_group#rt_large_ag")
keep if regexm(rhs,"_dd_mth_2SP") | regexm(rhs,"_dd_qtr_2SP")
drop if regexm(rhs,"deflag")
keep if regexm(rhs,"default")
keep if depvar=="ihs_kwh"

gen col = .			

replace col = 1 if fes=="sp_group#month sp_group#rt_large_ag modate" ///
	& rhs=="(log_p_mean = log_mean_p_kwh_ag_default) ln_kwhaf_rast_dd_mth_2SP" ///
	& ifs_sample==""
	
replace col = 2 if fes=="sp_group#month sp_group#rt_large_ag modate" ///
	& rhs=="(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)" ///
	& ifs_sample==""

replace col = 3 if fes=="sp_group#month sp_group#rt_large_ag modate" ///
	& rhs=="(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)" ///
	& ifs_sample==" & inlist(basin_group,68,121,122)"

replace col = 4 if fes=="sp_group#month sp_group#rt_large_ag modate" ///
	& rhs=="(log_p_mean ln_kwhaf_rast_dd_qtr_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_qtr_2SP)" ///
	& ifs_sample==""

replace col = 5 if fes=="sp_group#month sp_group#rt_large_ag wdist_group#year basin_group#year modate" ///
	& rhs=="(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)" ///
	& ifs_sample==""

replace col = 6 if fes=="sp_group#month sp_group#rt_large_ag modate" ///
	& rhs=="(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default L6_ln_gw_mean_depth_mth_2SP L12_ln_gw_mean_depth_mth_2SP)" ///
	& ifs_sample==""

	
drop if col==.
assert _N==6
sort col
order col

forvalues c = 1/6 {

	local betaE_`c' = string(beta_log_p_kwh[`c'],"%9.2f")
	local seE_`c' = string(se_log_p_kwh[`c'],"%9.2f")
	local pvalE_`c' = 2*ttail(dof[`c'],abs(t_log_p_kwh[`c']))
	if `pvalE_`c''<0.01 {
		local starsE_`c' = "$^{***}$"
	}
	else if `pvalE_`c''<0.05 {
		local starsE_`c' = "$^{**}$"
	}
	else if `pvalE_`c''<0.1 {
		local starsE_`c' = "$^{*}$"
	}
	else {
		local starsE_`c' = ""
	}
	
	local betaW_`c' = string(beta_log_kwhaf[`c'],"%9.2f")
	local seW_`c' = string(se_log_kwhaf[`c'],"%9.2f")
	local pvalW_`c' = 2*ttail(dof[`c'],abs(t_log_kwhaf[`c']))
	if `pvalW_`c''<0.01 {
		local starsW_`c' = "$^{***}$"
	}
	else if `pvalW_`c''<0.05 {
		local starsW_`c' = "$^{**}$"
	}
	else if `pvalW_`c''<0.1 {
		local starsW_`c' = "$^{*}$"
	}
	else {
		local starsW_`c' = ""
	}

	local n_sp_`c' = string(n_SPs[`c'],"%9.0fc")
	local n_mth_`c' = string(n_modates[`c'],"%9.0fc")
	local n_obs_`c' = string(n_obs[`c']/1e6,"%9.2f") + "M"
	local fstat_`c' = string(fstat_rk[`c'],"%9.0f")
	if "`fstat_`c''"=="." {
		local fstat_`c' = ""
	}
}


	// Build table
file open textab using "$dirpath_output/table_water_regs_split.tex", write text replace

file write textab "\begin{table}[t!]\centering" _n
file write textab "\small" _n
file write textab "\caption{Estimated Demand Elasticities Decomposed -- Groundwater  \label{tab:water_regs_split}}" _n
file write textab "\vspace{-0.1cm}" _n
file write textab "\small" _n
file write textab "\begin{adjustbox}{center} " _n
file write textab "\begin{tabular}{lcccccccc} " _n
file write textab "\hline \hline" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab " & (1)  & (2)  & (3)  & (4)  & (5)  & (6) \\ " _n
file write textab "[0.1em]" _n
file write textab " & IV & IV & IV & IV & IV & IV \\" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab "\cline{2-7}" _n
file write textab "\vspace{-0.27cm}" _n
file write textab "\\" _n
file write textab " $\log\big(P^{\text{elec}}_{it}\big)$: \$\hat\beta^{\text{e}}\$ ~ & " _n
file write textab " $`betaE_1'$`starsE_1'  & $`betaE_2'$`starsE_2' & $`betaE_3'$`starsE_3' & $`betaE_4'$`starsE_4' & $`betaE_5'$`starsE_5'  & $`betaE_6'$`starsE_6' \\ " _n
file write textab "& $(`seE_1')$ & $(`seE_2')$ & $(`seE_3')$ & $(`seE_4')$ & $(`seE_5')$ & $(`seE_6')$ \\" _n
file write textab "[0.75em] " _n
file write textab " $\log\Big(\widehat{\tfrac{{\text{kWh}}}{\text{AF}}}_{it}\Big)$: \$\hat\beta^{\text{w}}\$ ~ & " _n
file write textab " $`betaW_1'$`starsW_1'  & $`betaW_2'$`starsW_2' & $`betaW_3'$`starsW_3' & $`betaW_4'$`starsW_4' & $`betaW_5'$`starsW_5'  & $`betaW_6'$`starsW_6' \\ " _n
file write textab "[-0.3em] " _n
file write textab "& $(`seW_1')$ & $(`seW_2')$ & $(`seW_3')$ & $(`seW_4')$ & $(`seW_5')$ & $(`seW_6')$ \\" _n
file write textab "[1.5em] " _n
file write textab "Instrument(s): \\" _n
file write textab "[0.1em] " _n
file write textab "~~ Default $\log\big(P^{\text{elec}}_{it}\big)$  & Yes & Yes & Yes  & Yes  & Yes & Yes \\" _n
file write textab "[0.1em] " _n
file write textab "~~ $\log\big(\text{Avg depth in basin}\big)$  & & Yes & Yes & Yes & Yes & \\" _n
file write textab "[0.1em] " _n
file write textab "~~ $\log\big(\text{Avg depth in basin}\big)$, lagged  & & & & & & Yes \\" _n
file write textab "[1.5em] " _n
file write textab "Fixed effects: \\" _n
file write textab "[0.1em] " _n
file write textab "~~Unit $\times$ month-of-year  & Yes  & Yes  & Yes  & Yes  & Yes  & Yes   \\ " _n
file write textab "[0.1em] " _n
file write textab "~~Month-of-sample  & Yes  & Yes  & Yes  & Yes  & Yes  & Yes   \\ " _n
file write textab "[0.1em] " _n
file write textab "~~Unit $\times$ physical capital & Yes & Yes & Yes & Yes & Yes & Yes  \\" _n
file write textab "[0.1em] " _n
file write textab "~~Water basin $\times$ year & & &  & & Yes &   \\" _n
file write textab "[0.1em] " _n
file write textab "~~Water district $\times$ year & & & & & Yes &  \\" _n
file write textab "[1.5em] " _n
file write textab "Groundwater time step & Month & Month & Month & Quarter & Month & Month  \\ " _n
file write textab "[0.1em] " _n
file write textab "Only basins with \$>1000\$ SPs &  &  & Yes &  &  &   \\ " _n
file write textab "[1.5em] " _n
file write textab "Service point units & `n_sp_1' & `n_sp_2' & `n_sp_3' & `n_sp_4' & `n_sp_5' & `n_sp_6'  \\ " _n
file write textab "[0.1em] " _n
file write textab "Months  & `n_mth_1' & `n_mth_2' & `n_mth_3' & `n_mth_4' & `n_mth_5' & `n_mth_6' \\ " _n
file write textab "[0.1em] " _n
file write textab "Observations & `n_obs_1' & `n_obs_2' & `n_obs_3' & `n_obs_4' & `n_obs_5' & `n_obs_6' \\ " _n
file write textab "[0.1em] " _n
file write textab "First stage \$F\$-statistic & `fstat_1' & `fstat_2' & `fstat_3' & `fstat_4' & `fstat_5' & `fstat_6' \\ " _n
file write textab "[0.15em]" _n
file write textab "\hline" _n
file write textab "\end{tabular}" _n
file write textab "\end{adjustbox}" _n
file write textab "\captionsetup{width=\textwidth}" _n
file write textab "\caption*{\scriptsize \emph{Notes:} " _n
file write textab "Each regression estimates Equation (\ref{eq:reg_water}) at the service point by month level," _n
file write textab "where the dependent variable is the inverse hyperbolic sine transformation " _n
file write textab "of electricity consumed by service point \$i\$ in month \$t\$." _n
file write textab "We report estimates for \$\hat\beta^{\text{e}}\$ and \$\hat\beta^{\text{w}}\$, where the latter subtracts " _n
file write textab "1 from the estimated coefficient on \$\log \big(\widehat{\text{kWh}\big/\text{AF}}_{it}\big)\$." _n
file write textab "We estimate IV specifications via two-stage least squares, and all regressions instrument " _n
file write textab "for \$P^{\text{elec}}_{it}\$ with unit \$i\$'s within-category default logged electricity price " _n
file write textab "in month \$t\$ (consistent with our preferred specification from Table \ref{tab:elec_regs_main})." _n
file write textab "We instrument for \$\log \big(\widehat{\text{kWh}\big/\text{AF}}_{it}\big)\$ with either logged " _n
file write textab "average groundwater depth across unit \$i\$'s basin, or the 6- and 12-month lags of this variable." _n
file write textab "\`\`Physical capital'' is a categorical variable for (i) small pumps, (ii) large pumps, and (iii) " _n
file write textab "internal combustion engines, and unit \$\times\$ physical capital fixed effects control for shifts " _n
file write textab "in tariff category triggered by the installation of new pumping equipment." _n
file write textab "Water basin \$\times\$ year fixed effects control for broad geographic trends in groundwater depth." _n
file write textab "Water district \$\times\$ year fixed effects control for annual variation in surface water allocations." _n
file write textab "Column (3) restricts the sample to only the three most common water basins (San Joaquin Valley, " _n
file write textab "Sacramento Valley, and Salinas Valley), each of which contains over 1000 unique SPs in our estimation sample." _n
file write textab "Column (4) uses a quarterly panel of groundwater depths to construct \$\log \big(\widehat{\text{kWh}\big/\text{AF}}_{it}\big)\$ " _n
file write textab "and the instrument, rather than a monthly panel." _n
file write textab "All regressions drop solar NEM customers, customers with bad geocodes, months with irregular electricity bills " _n
file write textab "(e.g.\ first/last bills, bills longer/shorter than 1 month, overlapping bills for a single account)," _n
file write textab "and pumps with implausible test measurements." _n
file write textab "Standard errors (in parentheses) are two-way clustered by service point and by month-of-sample." _n
file write textab "Significance: *** \$p < 0.01\$, ** \$p < 0.05\$, * \$p < 0.10\$." _n
file write textab "}" _n
file write textab "\end{table}" _n

file close textab

}

************************************************
************************************************

** 2. Sensitivity: IHS vs log transformation, electricity
{
use "$dirpath_data/results/regs_pge_elec_monthly_july2020.dta" , clear

keep if pull=="PGE 20180719"
keep if panel=="monthly"
keep if ifs_base=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3"
keep if fes=="sp_group#month sp_group#rt_large_ag modate" 
keep if rhs=="(log_p_mean = log_mean_p_kwh_ag_default)"

gen col = .			
replace col = 1 if depvar=="ihs_kwh" & ifs_sample==""
replace col = 2 if depvar=="ihs_kwh" & ifs_sample==" & log_kwh!=."
replace col = 3 if depvar=="log_kwh"
replace col = 4 if depvar=="log1_kwh"
replace col = 5 if depvar=="log1_100kwh"
keep if col!=.
assert _N==5
sort col
order col

forvalues c = 1/5 {
	local beta_`c' = string(beta_log_p_mean[`c'],"%9.2f")
	local se_`c' = string(se_log_p_mean[`c'],"%9.2f")
	local pval_`c' = 2*ttail(dof[`c'],abs(t_log_p_mean[`c']))
	if `pval_`c''<0.01 {
		local stars_`c' = "$^{***}$"
	}
	else if `pval_`c''<0.05 {
		local stars_`c' = "$^{**}$"
	}
	else if `pval_`c''<0.1 {
		local stars_`c' = "$^{*}$"
	}
	else {
		local stars_`c' = ""
	}
	local n_sp_`c' = string(n_SPs[`c'],"%9.0fc")
	local n_mth_`c' = string(n_modates[`c'],"%9.0fc")
	local n_obs_`c' = string(n_obs[`c']/1e6,"%9.2f") + "M"
	local fstat_`c' = string(fstat_rk[`c'],"%9.0f")
	if "`fstat_`c''"=="." {
		local fstat_`c' = ""
	}
}

	// Build table
file open textab using "$dirpath_output/table_elec_regs_ihs_logs.tex", write text replace

file write textab "\begin{table}[t!]\centering" _n
file write textab "\small" _n
file write textab "\caption{Sensitivity to IHS vs.\ Log Transformation - Electricity  \label{tab:elec_regs_ihs_logs}}" _n
file write textab "\vspace{-0.1cm}" _n
file write textab "\small" _n
file write textab "\begin{adjustbox}{center} " _n
file write textab "\begin{tabular}{lcccccccc} " _n
file write textab "\hline \hline" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab " & (1)  & (2)  & (3)  & (4)  & (5)   \\ " _n
file write textab "[0.1em]" _n
file write textab " & IV & IV & IV & IV & IV  \\" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab "\cline{2-6}" _n
file write textab "\vspace{-0.27cm}" _n
file write textab "\\" _n
file write textab " $\log\big(P^{\text{elec}}_{it}\big)$ ~ & $`beta_1'$`stars_1'  & $`beta_2'$`stars_2' & $`beta_3'$`stars_3' & $`beta_4'$`stars_4' & $`beta_5'$`stars_5' \\ " _n
file write textab "& $(`se_1')$ & $(`se_2')$ & $(`se_3')$ & $(`se_4')$ & $(`se_5')$  \\" _n
file write textab "[1.5em] " _n
file write textab "LHS transformation: & \$ \sinh^{-1}(Q) \$ & \$ \sinh^{-1}(Q) \$ & \$\log(Q)\$  & \$\log(1+Q)\$   &  \$\log(1+100Q)\$  \\" _n
file write textab " & & \$Q>0\$ \\ " _n
file write textab "[1em] " _n
file write textab "IV: Default $\log\big(P^{\text{elec}}_{it}\big)$  & Yes & Yes & Yes  & Yes  &  Yes \\" _n
file write textab "[1em] " _n
file write textab "Fixed effects: \\" _n
file write textab "[0.1em] " _n
file write textab "~~Unit $\times$ month-of-year  & Yes  & Yes  & Yes  & Yes  & Yes   \\ " _n
file write textab "[0.1em] " _n
file write textab "~~Month-of-sample  & Yes  & Yes  & Yes  & Yes  & Yes     \\ " _n
file write textab "[0.1em] " _n
file write textab "~~Unit $\times$ physical capital & Yes & Yes & Yes & Yes & Yes  \\" _n
*file write textab "[0.1em] " _n
*file write textab "~~Water basin $\times$ year & & &  & Yes & &  \\" _n
*file write textab "[0.1em] " _n
*file write textab "~~Water district $\times$ year & & &  & Yes & & \\" _n
*file write textab "[0.1em] " _n
file write textab "[1em] " _n
file write textab "Service point units & `n_sp_1' & `n_sp_2' & `n_sp_3' & `n_sp_4' & `n_sp_5'   \\ " _n
file write textab "[0.1em] " _n
file write textab "Months  & `n_mth_1' & `n_mth_2' & `n_mth_3' & `n_mth_4' & `n_mth_5'  \\ " _n
file write textab "[0.1em] " _n
file write textab "Observations & `n_obs_1' & `n_obs_2' & `n_obs_3' & `n_obs_4' & `n_obs_5'  \\ " _n
file write textab "[0.1em] " _n
file write textab "First stage \$F\$-statistic & `fstat_1' & `fstat_2' & `fstat_3' & `fstat_4' & `fstat_5'  \\ " _n
file write textab "[0.15em]" _n
file write textab "\hline" _n
file write textab "\end{tabular}" _n
file write textab "\end{adjustbox}" _n
file write textab "\captionsetup{width=\textwidth}" _n
file write textab "\caption*{\scriptsize \emph{Notes:} This table conducts sensitivity analysis on the transformation of the dependent variable \$Q^{\text{elec}}\$." _n
file write textab "Column (1) reproduces our preferred specification from Column (3) of Table \ref{tab:elec_regs_main} using the inverse hyperbolic sine transformation." _n
file write textab "where the dependent variable is the inverse hyperbolic sine transformation of electricity consumed by service point \$i\$ in month \$t\$." _n
file write textab "Column (2) uses the same transformation but removed zeros to align with the natural log transformation in Column (3). " _n
file write textab "Columns (4)--(5) apply the natural log + 1 transformation. Column (5) also scales the dependent variable by 100, which nearly matches " _n
file write textab "our results using the inverse hyperbolic sine transformation. " _n
file write textab "See notes under Table \ref{tab:elec_regs_main} for further detail. " _n
file write textab "Standard errors (in parentheses) are two-way clustered by service point and by month-of-sample." _n
file write textab "Significance: *** \$p < 0.01\$, ** \$p < 0.05\$, * \$p < 0.10\$." _n
file write textab "}" _n
file write textab "\end{table}" _n

file close textab

}

************************************************
************************************************

** 3. Sensitivity: time varying confounders, electricity
{
use "$dirpath_data/results/regs_pge_elec_monthly_sens_july2020.dta" , clear
keep if regexm(sens,"by month-of-sample FEs")
assert _N==5
assert pull=="PGE 20180719"
assert panel=="monthly"
assert ifs_base=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3"
assert ifs_sample==""
assert rhs=="(log_p_mean = log_mean_p_kwh_ag_default)"

gen col = .			
replace col = 1 if fes=="sp_group#month sp_group#rt_large_ag modate#cz_group"
replace col = 2 if fes=="sp_group#month sp_group#rt_large_ag modate#county_group"
replace col = 3 if fes=="sp_group#month sp_group#rt_large_ag modate#basin_group"
replace col = 4 if fes=="sp_group#month sp_group#rt_large_ag modate#basin_sub_group"
replace col = 5 if fes=="sp_group#month sp_group#rt_large_ag modate#wdist_group"
sort col
order col


forvalues c = 1/5 {
	local beta_`c' = string(beta_log_p_mean[`c'],"%9.2f")
	local se_`c' = string(se_log_p_mean[`c'],"%9.2f")
	local pval_`c' = 2*ttail(dof[`c'],abs(t_log_p_mean[`c']))
	if `pval_`c''<0.01 {
		local stars_`c' = "$^{***}$"
	}
	else if `pval_`c''<0.05 {
		local stars_`c' = "$^{**}$"
	}
	else if `pval_`c''<0.1 {
		local stars_`c' = "$^{*}$"
	}
	else {
		local stars_`c' = ""
	}
	local n_sp_`c' = string(n_SPs[`c'],"%9.0fc")
	local n_mth_`c' = string(n_modates[`c'],"%9.0fc")
	local n_obs_`c' = string(n_obs[`c']/1e6,"%9.2f") + "M"
	local fstat_`c' = string(fstat_rk[`c'],"%9.0f")
	if "`fstat_`c''"=="." {
		local fstat_`c' = ""
	}
}

	// Build table
file open textab using "$dirpath_output/table_elec_regs_month_int_fes.tex", write text replace

file write textab "\begin{table}[t!]\centering" _n
file write textab "\small" _n
file write textab "\caption{Sensitivity to Geographic Confounders - Electricity  \label{tab:elec_regs_month_int_fes}}" _n
file write textab "\vspace{-0.1cm}" _n
file write textab "\small" _n
file write textab "\begin{adjustbox}{center} " _n
file write textab "\begin{tabular}{lcccccccc} " _n
file write textab "\hline \hline" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab " & (1)  & (2)  & (3)  & (4)  & (5)   \\ " _n
file write textab "[0.1em]" _n
file write textab " & IV & IV & IV & IV & IV  \\" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab "\cline{2-6}" _n
file write textab "\vspace{-0.27cm}" _n
file write textab "\\" _n
file write textab " $\log\big(P^{\text{elec}}_{it}\big)$ ~ & $`beta_1'$`stars_1'  & $`beta_2'$`stars_2' & $`beta_3'$`stars_3' & $`beta_4'$`stars_4' & $`beta_5'$`stars_5' \\ " _n
file write textab "& $(`se_1')$ & $(`se_2')$ & $(`se_3')$ & $(`se_4')$ & $(`se_5')$  \\" _n
file write textab "[1.5em] " _n
file write textab "Month-of-sample FEs & Climate & County & Basin  & Sub-Basin  &  Water  \\" _n
file write textab "~~~~~~~interaction & & zone & & & district \\ " _n
file write textab "[1em] " _n
file write textab "IV: Default $\log\big(P^{\text{elec}}_{it}\big)$  & Yes & Yes & Yes  & Yes  &  Yes \\" _n
file write textab "[1em] " _n
file write textab "Fixed effects: \\" _n
file write textab "[0.1em] " _n
file write textab "~~Unit $\times$ month-of-year  & Yes  & Yes  & Yes  & Yes  & Yes   \\ " _n
file write textab "[0.1em] " _n
file write textab "~~Month-of-sample  & Yes  & Yes  & Yes  & Yes  & Yes     \\ " _n
file write textab "[0.1em] " _n
file write textab "~~Unit $\times$ physical capital & Yes & Yes & Yes & Yes & Yes  \\" _n
*file write textab "[0.1em] " _n
*file write textab "~~Water basin $\times$ year & & &  & Yes & &  \\" _n
*file write textab "[0.1em] " _n
*file write textab "~~Water district $\times$ year & & &  & Yes & & \\" _n
*file write textab "[0.1em] " _n
file write textab "[1em] " _n
file write textab "Service point units & `n_sp_1' & `n_sp_2' & `n_sp_3' & `n_sp_4' & `n_sp_5'   \\ " _n
file write textab "[0.1em] " _n
file write textab "Months  & `n_mth_1' & `n_mth_2' & `n_mth_3' & `n_mth_4' & `n_mth_5'  \\ " _n
file write textab "[0.1em] " _n
file write textab "Observations & `n_obs_1' & `n_obs_2' & `n_obs_3' & `n_obs_4' & `n_obs_5'  \\ " _n
file write textab "[0.1em] " _n
file write textab "First stage \$F\$-statistic & `fstat_1' & `fstat_2' & `fstat_3' & `fstat_4' & `fstat_5'  \\ " _n
file write textab "[0.15em]" _n
file write textab "\hline" _n
file write textab "\end{tabular}" _n
file write textab "\end{adjustbox}" _n
file write textab "\captionsetup{width=\textwidth}" _n
file write textab "\caption*{\scriptsize \emph{Notes:} This table conducts sensitivity analysis on our preferred electricity specification from Column (3) " _n
file write textab "of Table \ref{tab:elec_regs_main}, by interacting month-of-sample fixed effects with different geographic variables.  " _n
file write textab "California comprises 16 climate zones, and PGE agriculture customers are distributed across 11 distinct climate zones." _n
file write textab "Sub-basins are administrative sub-divisions of groundwater basins; this estimation sample includes agricultural consumers  " _n
file write textab "from 46 unique groundwater basins and 95 unique sub-basins.  " _n
file write textab "The sample also includes units assigned to 125 unique water districts; Column (5) includes a separate set of month-of-sample " _n
file write textab "fixed effects for units not assigned to a water district." _n
file write textab "See notes under Table \ref{tab:elec_regs_main} for further detail. " _n
file write textab "Standard errors (in parentheses) are two-way clustered by service point and by month-of-sample." _n
file write textab "Significance: *** \$p < 0.01\$, ** \$p < 0.05\$, * \$p < 0.10\$." _n
file write textab "}" _n
file write textab "\end{table}" _n

file close textab

}

************************************************
************************************************

** 4. Sensitivity: Summer vs winter, electricity and water
{
use "$dirpath_data/results/regs_pge_elec_monthly_sens_july2020.dta" , clear
keep if inlist(sens,"Summer months only","Winter months only")
assert _N==2
assert pull=="PGE 20180719"
assert panel=="monthly"
assert ifs_base=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3"
assert fes=="sp_group#month sp_group#rt_large_ag modate" 
assert rhs=="(log_p_mean = log_mean_p_kwh_ag_default)"

gen col = .			
replace col = 1 if depvar=="ihs_kwh" & ifs_sample==" & summer==1"
replace col = 2 if depvar=="ihs_kwh" & ifs_sample==" & summer==0"
sort col
order col

forvalues c = 1/2 {
	local beta_`c' = string(beta_log_p_mean[`c'],"%9.2f")
	local se_`c' = string(se_log_p_mean[`c'],"%9.2f")
	local pval_`c' = 2*ttail(dof[`c'],abs(t_log_p_mean[`c']))
	if `pval_`c''<0.01 {
		local stars_`c' = "$^{***}$"
	}
	else if `pval_`c''<0.05 {
		local stars_`c' = "$^{**}$"
	}
	else if `pval_`c''<0.1 {
		local stars_`c' = "$^{*}$"
	}
	else {
		local stars_`c' = ""
	}
	local n_sp_`c' = string(n_SPs[`c'],"%9.0fc")
	local n_mth_`c' = string(n_modates[`c'],"%9.0fc")
	local n_obs_`c' = string(n_obs[`c']/1e6,"%9.2f") + "M"
	local fstat_`c' = string(fstat_rk[`c'],"%9.0f")
	if "`fstat_`c''"=="." {
		local fstat_`c' = ""
	}
}

use "$dirpath_data/results/regs_pge_water_combined_monthly_sens_july2020.dta" , clear
keep if inlist(sens,"Summer months only","Winter months only")
assert _N==2
assert pull=="PGE 20180719"
assert panel=="monthly"
assert ifs_base=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
gen col = .			
	
replace col = 3 if depvar=="ihs_af_rast_dd_mth_2SP" ///
	& fes=="sp_group#month sp_group#rt_large_ag modate" ///
	& rhs=="(ln_mean_p_af_rast_dd_mth_2SP = log_mean_p_kwh_ag_default)" ///
	& ifs_sample==" & summer==1"

replace col = 4 if depvar=="ihs_af_rast_dd_mth_2SP" ///
	& fes=="sp_group#month sp_group#rt_large_ag modate" ///
	& rhs=="(ln_mean_p_af_rast_dd_mth_2SP = log_mean_p_kwh_ag_default)" ///
	& ifs_sample==" & summer==0"
sort col
order col
set obs 4
replace col = 1 in 3
replace col = 2 in 4
sort col

forvalues c = 3/4 {

	local beta_`c' = string(beta_log_p_water[`c'],"%9.2f")
	local se_`c' = string(se_log_p_water[`c'],"%9.2f")
	local pval_`c' = 2*ttail(dof[`c'],abs(t_log_p_water[`c']))
	if `pval_`c''<0.01 {
		local stars_`c' = "$^{***}$"
	}
	else if `pval_`c''<0.05 {
		local stars_`c' = "$^{**}$"
	}
	else if `pval_`c''<0.1 {
		local stars_`c' = "$^{*}$"
	}
	else {
		local stars_`c' = ""
	}
	
	local n_sp_`c' = string(n_SPs[`c'],"%9.0fc")
	local n_mth_`c' = string(n_modates[`c'],"%9.0fc")
	local n_obs_`c' = string(n_obs[`c']/1e6,"%9.2f") + "M"
	local fstat_`c' = string(fstat_rk[`c'],"%9.0f")
	if "`fstat_`c''"=="." {
		local fstat_`c' = ""
	}
}



	// Build table
file open textab using "$dirpath_output/table_elec_water_regs_summer_winter.tex", write text replace

file write textab "\begin{table}[t!]\centering" _n
file write textab "\small" _n
file write textab "\caption{Monthly Elasticities during Summer vs.\ Winter  \label{tab:elec_water_regs_summer_winter}}" _n
file write textab "\vspace{-0.1cm}" _n
file write textab "\small" _n
file write textab "\begin{adjustbox}{center} " _n
file write textab "\begin{tabular}{lcccccccc} " _n
file write textab "\hline \hline" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab " & \multicolumn{2}{c}{Electricity} & \multicolumn{2}{c}{Groundwater} \\" _n
file write textab " \cmidrule(r){2-3} \cmidrule(l){4-5}" _n
file write textab " & (1)  & (2)  & (3)  & (4)     \\ " _n
file write textab "[0.1em]" _n
file write textab " & IV & IV & IV & IV  \\" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab "\cline{2-5}" _n
file write textab "\vspace{-0.27cm}" _n
file write textab "\\" _n
file write textab " $\log\big(P^{\text{elec}}_{it}\big)$ ~ & $`beta_1'$`stars_1'  & $`beta_2'$`stars_2' & $`beta_3'$`stars_3' & $`beta_4'$`stars_4' \\ " _n
file write textab "& $(`se_1')$ & $(`se_2')$ & $(`se_3')$ & $(`se_4')$  \\" _n
file write textab "[1.5em] " _n
file write textab "Sample months: & Summer & Winter & Summer & Winter \\" _n
file write textab "[1em] " _n
file write textab "IV: Default $\log\big(P^{\text{elec}}_{it}\big)$  & Yes & Yes &  Yes  &  Yes \\" _n
file write textab "[1em] " _n
file write textab "Fixed effects: \\" _n
file write textab "[0.1em] " _n
file write textab "~~Unit $\times$ month-of-year  & Yes  & Yes   & Yes  & Yes   \\ " _n
file write textab "[0.1em] " _n
file write textab "~~Month-of-sample  & Yes  & Yes   & Yes  & Yes     \\ " _n
file write textab "[0.1em] " _n
file write textab "~~Unit $\times$ physical capital & Yes & Yes & Yes & Yes  \\" _n
*file write textab "[0.1em] " _n
*file write textab "~~Water basin $\times$ year & & &  & Yes & &  \\" _n
*file write textab "[0.1em] " _n
*file write textab "~~Water district $\times$ year & & &  & Yes & & \\" _n
*file write textab "[0.1em] " _n
file write textab "[1em] " _n
file write textab "Service point units & `n_sp_1' & `n_sp_2' & `n_sp_3' & `n_sp_4'   \\ " _n
file write textab "[0.1em] " _n
file write textab "Months  & `n_mth_1' & `n_mth_2' & `n_mth_3' & `n_mth_4'   \\ " _n
file write textab "[0.1em] " _n
file write textab "Observations & `n_obs_1' & `n_obs_2' & `n_obs_3' & `n_obs_4' \\ " _n
file write textab "[0.1em] " _n
file write textab "First stage \$F\$-statistic & `fstat_1' & `fstat_2' & `fstat_3' & `fstat_4'   \\ " _n
file write textab "[0.15em]" _n
file write textab "\hline" _n
file write textab "\end{tabular}" _n
file write textab "\end{adjustbox}" _n
file write textab "\captionsetup{width=\textwidth}" _n
file write textab "\caption*{\scriptsize \emph{Notes:} This table estimates demand elasticities separately for summer (May--October) vs.\ winter (November--April). " _n
file write textab "Columns (1)--(2) replicate Column (3) from Table \ref{tab:elec_regs_main}, splitting the sample by season. " _n
file write textab "Columns (3)--(4) replicate Column (2) from Table \ref{tab:water_regs_combined}, splitting the sample by season. " _n
file write textab "See notes under these tables for further detail. " _n
file write textab "Standard errors (in parentheses) are two-way clustered by service point and by month-of-sample." _n
file write textab "Significance: *** \$p < 0.01\$, ** \$p < 0.05\$, * \$p < 0.10\$." _n
file write textab "}" _n
file write textab "\end{table}" _n

file close textab

}

************************************************
************************************************

** 5. Sensitivity: weather controls, electricity and water
{
use "$dirpath_data/results/regs_pge_elec_monthly_sens_july2020.dta" , clear
keep if regexm(sens,"precip")
assert _N==4
assert pull=="PGE 20180719"
assert panel=="monthly"
assert ifs_base=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3"
assert depvar=="ihs_kwh"
assert fes=="sp_group#month sp_group#rt_large_ag modate" 
assert strpos(rhs,"(log_p_mean = log_mean_p_kwh_ag_default)")

gen col = .			
replace col = 1 if rhs=="(log_p_mean = log_mean_p_kwh_ag_default) precip_mm"
replace col = 2 if rhs=="(log_p_mean = log_mean_p_kwh_ag_default) precip_mm degreesC_*"
replace col = 3 if rhs=="(log_p_mean = log_mean_p_kwh_ag_default) precip_mm Lprecip_mm degreesC_* LdegreesC_min LdegreesC_max LdegreesC_mean"
drop if col==.
assert _N==3
sort col
order col

forvalues c = 1/3 {
	local beta_`c' = string(beta_log_p_mean[`c'],"%9.2f")
	local se_`c' = string(se_log_p_mean[`c'],"%9.2f")
	local pval_`c' = 2*ttail(dof[`c'],abs(t_log_p_mean[`c']))
	if `pval_`c''<0.01 {
		local stars_`c' = "$^{***}$"
	}
	else if `pval_`c''<0.05 {
		local stars_`c' = "$^{**}$"
	}
	else if `pval_`c''<0.1 {
		local stars_`c' = "$^{*}$"
	}
	else {
		local stars_`c' = ""
	}
	local n_sp_`c' = string(n_SPs[`c'],"%9.0fc")
	local n_mth_`c' = string(n_modates[`c'],"%9.0fc")
	local n_obs_`c' = string(n_obs[`c']/1e6,"%9.2f") + "M"
	local fstat_`c' = string(fstat_rk[`c'],"%9.0f")
	if "`fstat_`c''"=="." {
		local fstat_`c' = ""
	}
}

use "$dirpath_data/results/regs_pge_water_combined_monthly_sens_july2020.dta" , clear
keep if regexm(sens,"precip")
assert _N==4
assert pull=="PGE 20180719"
assert panel=="monthly"
assert ifs_base=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
assert depvar=="ihs_af_rast_dd_mth_2SP"
assert fes=="sp_group#month sp_group#rt_large_ag modate" 
assert strpos(rhs,"(ln_mean_p_af_rast_dd_mth_2SP = log_mean_p_kwh_ag_default)")

gen col = .			
replace col = 4 if rhs=="(ln_mean_p_af_rast_dd_mth_2SP = log_mean_p_kwh_ag_default) precip_mm"
replace col = 5 if rhs=="(ln_mean_p_af_rast_dd_mth_2SP = log_mean_p_kwh_ag_default) precip_mm degreesC_*"
replace col = 6 if rhs=="(ln_mean_p_af_rast_dd_mth_2SP = log_mean_p_kwh_ag_default) precip_mm Lprecip_mm degreesC_* LdegreesC_min LdegreesC_max LdegreesC_mean"
drop if col==.
assert _N==3
sort col
order col
set obs 6
replace col = 1 in 4
replace col = 2 in 5
replace col = 3 in 6
sort col

forvalues c = 4/6 {

	local beta_`c' = string(beta_log_p_water[`c'],"%9.2f")
	local se_`c' = string(se_log_p_water[`c'],"%9.2f")
	local pval_`c' = 2*ttail(dof[`c'],abs(t_log_p_water[`c']))
	if `pval_`c''<0.01 {
		local stars_`c' = "$^{***}$"
	}
	else if `pval_`c''<0.05 {
		local stars_`c' = "$^{**}$"
	}
	else if `pval_`c''<0.1 {
		local stars_`c' = "$^{*}$"
	}
	else {
		local stars_`c' = ""
	}
	
	local n_sp_`c' = string(n_SPs[`c'],"%9.0fc")
	local n_mth_`c' = string(n_modates[`c'],"%9.0fc")
	local n_obs_`c' = string(n_obs[`c']/1e6,"%9.2f") + "M"
	local fstat_`c' = string(fstat_rk[`c'],"%9.0f")
	if "`fstat_`c''"=="." {
		local fstat_`c' = ""
	}
}



	// Build table
file open textab using "$dirpath_output/table_elec_water_regs_weather.tex", write text replace

file write textab "\begin{table}[t!]\centering" _n
file write textab "\small" _n
file write textab "\caption{Monthly Elasticities during Summer vs.\ Winter  \label{tab:elec_water_regs_weather}}" _n
file write textab "\vspace{-0.1cm}" _n
file write textab "\small" _n
file write textab "\begin{adjustbox}{center} " _n
file write textab "\begin{tabular}{lcccccccc} " _n
file write textab "\hline \hline" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab " & \multicolumn{3}{c}{Electricity} & \multicolumn{3}{c}{Groundwater} \\" _n
file write textab " \cmidrule(r){2-4} \cmidrule(l){5-7}" _n
file write textab " & (1)  & (2)  & (3)  & (4)  & (5) & (6)   \\ " _n
file write textab "[0.1em]" _n
file write textab " & IV & IV & IV & IV & IV & IV  \\" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab "\cline{2-7}" _n
file write textab "\vspace{-0.27cm}" _n
file write textab "\\" _n
file write textab " $\log\big(P^{\text{elec}}_{it}\big)$ ~ & $`beta_1'$`stars_1'  & $`beta_2'$`stars_2' & $`beta_3'$`stars_3' & $`beta_4'$`stars_4' & $`beta_5'$`stars_5' & $`beta_6'$`stars_6' \\ " _n
file write textab "& $(`se_1')$ & $(`se_2')$ & $(`se_3')$ & $(`se_4')$ & $(`se_5')$ & $(`se_6')$  \\" _n
file write textab "[1.5em] " _n
file write textab "Weather controls: \\" _n
file write textab "[0.1em] " _n
file write textab "~~ Precipitation  & Yes & Yes & Yes &  Yes  &  Yes & Yes \\" _n
file write textab "[0.1em] " _n
file write textab "~~ Temperature  & & Yes & Yes & &  Yes  &  Yes \\" _n
file write textab "[0.1em] " _n
file write textab "~~ Lagged precipitation  &  &  & Yes & &  &  Yes \\" _n
file write textab "[0.1em] " _n
file write textab "~~ Lagged temperature   &  &  & Yes & &  &  Yes \\" _n
file write textab "[1em] " _n
file write textab "IV: Default $\log\big(P^{\text{elec}}_{it}\big)$  & Yes & Yes & Yes & Yes &  Yes  &  Yes \\" _n
file write textab "[1em] " _n
file write textab "Fixed effects: \\" _n
file write textab "[0.1em] " _n
file write textab "~~Unit $\times$ month-of-year  & Yes & Yes & Yes & Yes &  Yes  &  Yes  \\ " _n
file write textab "[0.1em] " _n
file write textab "~~Month-of-sample  & Yes & Yes & Yes & Yes &  Yes  &  Yes   \\ " _n
file write textab "[0.1em] " _n
file write textab "~~Unit $\times$ physical capital & Yes & Yes & Yes & Yes &  Yes  &  Yes  \\" _n
*file write textab "[0.1em] " _n
*file write textab "~~Water basin $\times$ year & & &  & Yes & &  \\" _n
*file write textab "[0.1em] " _n
*file write textab "~~Water district $\times$ year & & &  & Yes & & \\" _n
*file write textab "[0.1em] " _n
file write textab "[1em] " _n
file write textab "Service point units & `n_sp_1' & `n_sp_2' & `n_sp_3' & `n_sp_4' & `n_sp_5' & `n_sp_6'  \\ " _n
file write textab "[0.1em] " _n
file write textab "Months  & `n_mth_1' & `n_mth_2' & `n_mth_3' & `n_mth_4' & `n_mth_5' & `n_mth_6'  \\ " _n
file write textab "[0.1em] " _n
file write textab "Observations & `n_obs_1' & `n_obs_2' & `n_obs_3' & `n_obs_4' & `n_obs_5' & `n_obs_6' \\ " _n
file write textab "[0.1em] " _n
file write textab "First stage \$F\$-statistic & `fstat_1' & `fstat_2' & `fstat_3' & `fstat_4' & `fstat_5' & `fstat_6'   \\ " _n
file write textab "[0.15em]" _n
file write textab "\hline" _n
file write textab "\end{tabular}" _n
file write textab "\end{adjustbox}" _n
file write textab "\captionsetup{width=\textwidth}" _n
file write textab "\caption*{\scriptsize \emph{Notes:} This table adds weather controls to our preferred specifications for electricity " _n
file write textab "(Column (3) of Table \ref{tab:elec_regs_main}) and groundwater (Column (2) of Table \ref{tab:water_regs_combined}). " _n
file write textab "We assign daily precipitation, maximum temperature, and minimum temperatures to each unit's latitude and longitude, " _n
file write textab "using daily rasters from PRISM. We sum daily precipitation over all days in each month, and average daily maximum  " _n
file write textab "and minimum temperatures over all days in each month. Columns (3) and (6) control for 1-month lags in all three variables. " _n
file write textab "See notes under Tables \ref{tab:elec_regs_main} and  Table \ref{tab:water_regs_combined} for further details. " _n
file write textab "Standard errors (in parentheses) are two-way clustered by service point and by month-of-sample." _n
file write textab "Significance: *** \$p < 0.01\$, ** \$p < 0.05\$, * \$p < 0.10\$." _n
file write textab "}" _n
file write textab "\end{table}" _n

file close textab

}

************************************************
************************************************

** 6. Sensitivity: time since pump test, water
{
use "$dirpath_data/results/regs_pge_water_combined_monthly_sens_july2020.dta" , clear
keep if regexm(sens,"months of pump test")
assert _N==5
assert pull=="PGE 20180719"
assert panel=="monthly"
assert ifs_base=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
assert depvar=="ihs_af_rast_dd_mth_2SP"
assert fes=="sp_group#month sp_group#rt_large_ag modate" 
assert rhs=="(ln_mean_p_af_rast_dd_mth_2SP = log_mean_p_kwh_ag_default)"
gen col = .			
	
replace col = 1 if  ifs_sample==" & months_to_nearest_test<=60"
replace col = 2 if  ifs_sample==" & months_to_nearest_test<=48"
replace col = 3 if  ifs_sample==" & months_to_nearest_test<=36"
replace col = 4 if  ifs_sample==" & months_to_nearest_test<=24"
replace col = 5 if  ifs_sample==" & months_to_nearest_test<=12"
sort col
order col

forvalues c = 1/5 {

	local beta_`c' = string(beta_log_p_water[`c'],"%9.2f")
	local se_`c' = string(se_log_p_water[`c'],"%9.2f")
	local pval_`c' = 2*ttail(dof[`c'],abs(t_log_p_water[`c']))
	if `pval_`c''<0.01 {
		local stars_`c' = "$^{***}$"
	}
	else if `pval_`c''<0.05 {
		local stars_`c' = "$^{**}$"
	}
	else if `pval_`c''<0.1 {
		local stars_`c' = "$^{*}$"
	}
	else {
		local stars_`c' = ""
	}
	
	local n_sp_`c' = string(n_SPs[`c'],"%9.0fc")
	local n_mth_`c' = string(n_modates[`c'],"%9.0fc")
	local n_obs_`c' = string(n_obs[`c']/1e6,"%9.2f") + "M"
	local fstat_`c' = string(fstat_rk[`c'],"%9.0f")
	if "`fstat_`c''"=="." {
		local fstat_`c' = ""
	}
}


	// Build table
file open textab using "$dirpath_output/table_water_months_from_pump_test.tex", write text replace

file write textab "\begin{table}[t!]\centering" _n
file write textab "\small" _n
file write textab "\caption{Sensitivity to Recent Pump Tests -- Groundwater  \label{tab:water_months_from_pump_test}}" _n
file write textab "\vspace{-0.1cm}" _n
file write textab "\small" _n
file write textab "\begin{adjustbox}{center} " _n
file write textab "\begin{tabular}{lcccccccc} " _n
file write textab "\hline \hline" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab " & (1)  & (2)  & (3)  & (4)  & (5)  \\ " _n
file write textab "[0.1em]" _n
file write textab " & IV & IV & IV & IV & IV \\" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab "\cline{2-6}" _n
file write textab "\vspace{-0.27cm}" _n
file write textab "\\" _n
file write textab " $\log\big(P^{\text{water}}_{it}\big)$ ~ & " _n
file write textab " $`beta_1'$`stars_1'  & $`beta_2'$`stars_2' & $`beta_3'$`stars_3' & $`beta_4'$`stars_4' & $`beta_5'$`stars_5' \\ " _n
file write textab "& $(`se_1')$ & $(`se_2')$ & $(`se_3')$ & $(`se_4')$ & $(`se_5')$  \\" _n
file write textab "[1.5em] " _n
file write textab "Months away from pump test: & 60 & 48 & 36 & 24 & 12 \\" _n
file write textab "[1em] " _n
file write textab "IV: Default $\log\big(P^{\text{elec}}_{it}\big)$  & Yes & Yes & Yes  & Yes  & Yes  \\" _n
file write textab "[1em] " _n
file write textab "Fixed effects: \\" _n
file write textab "[0.1em] " _n
file write textab "~~Unit $\times$ month-of-year  & Yes  & Yes  & Yes   & Yes  & Yes   \\ " _n
file write textab "[0.1em] " _n
file write textab "~~Month-of-sample  & Yes  & Yes  & Yes  & Yes  & Yes    \\ " _n
file write textab "[0.1em] " _n
file write textab "~~Unit $\times$ physical capital & Yes & Yes & Yes & Yes & Yes  \\" _n
*file write textab "[0.1em] " _n
*file write textab "~~Water basin $\times$ year & & &  & & Yes &   \\" _n
*file write textab "[0.1em] " _n
*file write textab "~~Water district $\times$ year & & & & & Yes &  \\" _n
file write textab "[1em] " _n
file write textab "Groundwater time step & Month & Month & Month & Month & Month  \\ " _n
*file write textab "[0.1em] " _n
*file write textab "Only basins with \$>1000\$ SPs &  &  & Yes &  &  &   \\ " _n
file write textab "[1.5em] " _n
file write textab "Service point units & `n_sp_1' & `n_sp_2' & `n_sp_3' & `n_sp_4' & `n_sp_5'  \\ " _n
file write textab "[0.1em] " _n
file write textab "Months  & `n_mth_1' & `n_mth_2' & `n_mth_3' & `n_mth_4' & `n_mth_5' \\ " _n
file write textab "[0.1em] " _n
file write textab "Observations & `n_obs_1' & `n_obs_2' & `n_obs_3' & `n_obs_4' & `n_obs_5'  \\ " _n
file write textab "[0.1em] " _n
file write textab "First stage \$F\$-statistic & `fstat_1' & `fstat_2' & `fstat_3' & `fstat_4' & `fstat_5'  \\ " _n
file write textab "[0.15em]" _n
file write textab "\hline" _n
file write textab "\end{tabular}" _n
file write textab "\end{adjustbox}" _n
file write textab "\captionsetup{width=\textwidth}" _n
file write textab "\caption*{\scriptsize \emph{Notes:} " _n
file write textab "Each regression replicates our preferred specification in Column (2) from Table \ref{tab:water_regs_combined}, " _n
file write textab "while restricting the sample to units with pump tests within \$m\$ months of sample month \$t\$. " _n
file write textab "For example, a March 2013 observation for unit \$i\$ is only included in Column (3) if we observe " _n
file write textab "a pump test for unit \$i\$ between March 2010 and March 2016. " _n
file write textab "These regressions reveal that unobserved changes in pump specifications are unlikely to be systematically biasing our " _n
file write textab "groundwater elasticity estimates. Stated differently, the mechanism underlying our estimates is unlikely to be " _n
file write textab "unobserved changes to farmers' irrigation capital. " _n
file write textab "See notes under Table \ref{tab:water_regs_combined} for further detail. " _n
file write textab "Standard errors (in parentheses) are two-way clustered by service point and by month-of-sample." _n
file write textab "Significance: *** \$p < 0.01\$, ** \$p < 0.05\$, * \$p < 0.10\$." _n
file write textab "}" _n
file write textab "\end{table}" _n

file close textab

}

************************************************
************************************************

** 7. Sensitivity: CLU groupings, water
{
use "$dirpath_data/results/regs_pge_water_combined_monthly_sens_july2020.dta" , clear
keep if regexm(sens,"CLU")
assert pull=="PGE 20180719"
assert panel=="monthly"
assert ifs_base=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
assert depvar=="ihs_af_rast_dd_mth_2SP"
assert fes=="sp_group#month sp_group#rt_large_ag modate" 
assert rhs=="(ln_mean_p_af_rast_dd_mth_2SP = log_mean_p_kwh_ag_default)"
gen col = .			
	
replace col = 1 if  ifs_sample==" & clu_ec_nearest_dist_m==0"
replace col = 2 if  ifs_sample==" & flag_clu_inconsistency==0"
replace col = 3 if  ifs_sample==" & spcount_clu_group0==1"
replace col = 4 if  ifs_sample==" & spcount_clu_group0>1 & spcount_clu_group0!=."
replace col = 5 if  ifs_sample=="" & vce=="cluster clu_group0 modate" & sens=="Cluster by CLU_group0 and month-of-sample"
keep if col!=.
assert _N==5
sort col
order col

forvalues c = 1/5 {

	local beta_`c' = string(beta_log_p_water[`c'],"%9.2f")
	local se_`c' = string(se_log_p_water[`c'],"%9.2f")
	local pval_`c' = 2*ttail(dof[`c'],abs(t_log_p_water[`c']))
	if `pval_`c''<0.01 {
		local stars_`c' = "$^{***}$"
	}
	else if `pval_`c''<0.05 {
		local stars_`c' = "$^{**}$"
	}
	else if `pval_`c''<0.1 {
		local stars_`c' = "$^{*}$"
	}
	else {
		local stars_`c' = ""
	}
	
	local n_sp_`c' = string(n_SPs[`c'],"%9.0fc")
	local n_mth_`c' = string(n_modates[`c'],"%9.0fc")
	local n_obs_`c' = string(n_obs[`c']/1e6,"%9.2f") + "M"
	local fstat_`c' = string(fstat_rk[`c'],"%9.0f")
	if "`fstat_`c''"=="." {
		local fstat_`c' = ""
	}
}


	// Build table
file open textab using "$dirpath_output/table_water_clu_sensitivities.tex", write text replace

file write textab "\begin{table}[t!]\centering" _n
file write textab "\small" _n
file write textab "\caption{Sensitivity to CLU Assignments and Groupings -- Groundwater  \label{tab:water_clu_sensitivities}}" _n
file write textab "\vspace{-0.1cm}" _n
file write textab "\small" _n
file write textab "\begin{adjustbox}{center} " _n
file write textab "\begin{tabular}{lcccccccc} " _n
file write textab "\hline \hline" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab " & (1)  & (2)  & (3)  & (4)  & (5)  \\ " _n
file write textab "[0.1em]" _n
file write textab " & IV & IV & IV & IV & IV \\" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab "\cline{2-6}" _n
file write textab "\vspace{-0.27cm}" _n
file write textab "\\" _n
file write textab " $\log\big(P^{\text{water}}_{it}\big)$ ~ & " _n
file write textab " $`beta_1'$`stars_1'  & $`beta_2'$`stars_2' & $`beta_3'$`stars_3' & $`beta_4'$`stars_4' & $`beta_5'$`stars_5' \\ " _n
file write textab "& $(`se_1')$ & $(`se_2')$ & $(`se_3')$ & $(`se_4')$ & $(`se_5')$  \\" _n
file write textab "[1.5em] " _n
file write textab "Sample criteria: \\" _n
file write textab "[0.1em] " _n
file write textab "~~ Inside CLU polygon  & Yes & & & & \\" _n
file write textab "[0.1em] " _n
file write textab "~~ Drop CLU inconsistencies  & & Yes & & & \\" _n
file write textab "[0.1em] " _n
file write textab "~~ Pumps per CLU group  & &  & 1 & 2+ & \\" _n
file write textab "[0.1em] " _n
file write textab "~~ Cluster by CLU group  & &  &  &  & Yes \\" _n
file write textab "[1em] " _n
file write textab "IV: Default $\log\big(P^{\text{elec}}_{it}\big)$  & Yes & Yes & Yes  & Yes  & Yes  \\" _n
file write textab "[1em] " _n
file write textab "Fixed effects: \\" _n
file write textab "[0.1em] " _n
file write textab "~~Unit $\times$ month-of-year  & Yes  & Yes  & Yes   & Yes  & Yes   \\ " _n
file write textab "[0.1em] " _n
file write textab "~~Month-of-sample  & Yes  & Yes  & Yes  & Yes  & Yes    \\ " _n
file write textab "[0.1em] " _n
file write textab "~~Unit $\times$ physical capital & Yes & Yes & Yes & Yes & Yes  \\" _n
*file write textab "[0.1em] " _n
*file write textab "~~Water basin $\times$ year & & &  & & Yes &   \\" _n
*file write textab "[0.1em] " _n
*file write textab "~~Water district $\times$ year & & & & & Yes &  \\" _n
file write textab "[1em] " _n
file write textab "Groundwater time step & Month & Month & Month & Month & Month  \\ " _n
*file write textab "[0.1em] " _n
*file write textab "Only basins with \$>1000\$ SPs &  &  & Yes &  &  &   \\ " _n
file write textab "[1em] " _n
file write textab "Service point units & `n_sp_1' & `n_sp_2' & `n_sp_3' & `n_sp_4' & `n_sp_5'  \\ " _n
file write textab "[0.1em] " _n
file write textab "Months  & `n_mth_1' & `n_mth_2' & `n_mth_3' & `n_mth_4' & `n_mth_5' \\ " _n
file write textab "[0.1em] " _n
file write textab "Observations & `n_obs_1' & `n_obs_2' & `n_obs_3' & `n_obs_4' & `n_obs_5'  \\ " _n
file write textab "[0.1em] " _n
file write textab "First stage \$F\$-statistic & `fstat_1' & `fstat_2' & `fstat_3' & `fstat_4' & `fstat_5'  \\ " _n
file write textab "[0.15em]" _n
file write textab "\hline" _n
file write textab "\end{tabular}" _n
file write textab "\end{adjustbox}" _n
file write textab "\captionsetup{width=\textwidth}" _n
file write textab "\caption*{\scriptsize \emph{Notes:} " _n
file write textab "Each regression replicates our preferred specification in Column (2) from Table \ref{tab:water_regs_combined}, " _n
file write textab "while conducting sensitivity on unit-specific assignments to CLU polygons (i.e.\ fields). " _n
file write textab "Column (1) includes only units with coordinates that are fully inside their assigned CLU polygon." _n
file write textab "Column (2) drops units with inconsistent, problematic, or internally conflicting CLU assignments." _n
file write textab "Columns (3)--(5) group CLUs that lie within the same tax parcels." _n
file write textab "Columns (3) includes only units that are the singleton (confirmed) groundwater pump in their CLU group." _n
file write textab "Columns (4) includes units in CLU groups with multiple (confirmed) groundwater pumps." _n
file write textab "Columns (5) two-way clusters by CLU group and by month-of-sample, with 4708 unique CLU groups." _n
file write textab "See notes under Table \ref{tab:water_regs_combined} for further detail. " _n
file write textab "Standard errors (in parentheses) are two-way clustered by service point and by month-of-sample, except in Column (5)." _n
file write textab "Significance: *** \$p < 0.01\$, ** \$p < 0.05\$, * \$p < 0.10\$." _n
file write textab "}" _n
file write textab "\end{table}" _n

file close textab

}

************************************************
************************************************

** 8. Sensitivity: KWHAF, water
{
use "$dirpath_data/results/regs_pge_water_combined_monthly_sens_july2020.dta" , clear
assert pull=="PGE 20180719"
assert panel=="monthly"
assert ifs_base=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
keep if fes=="sp_group#month sp_group#rt_large_ag modate" 
gen col = .			
	
replace col = 1 if ifs_sample=="" & depvar=="ihs_af_rast_dd_mth_2SP" & rhs=="(ln_mean_p_af_rast_dd_mth_2SP = ln_gw_mean_depth_mth_2SP)"
replace col = 2 if ifs_sample=="" & depvar=="ihs_af_apep_measured" & rhs=="(ln_mean_p_af_apep_measured = log_mean_p_kwh_ag_default)"
replace col = 3 if ifs_sample==" & flag_bad_drwdwn==1 " & depvar=="ihs_af_rast_dd_mth_2SP" & rhs=="(ln_mean_p_af_rast_dd_mth_2SP = log_mean_p_kwh_ag_default)"
replace col = 4 if ifs_sample=="" & depvar=="ihs_af_rast_ddhat_mth_2SP" & rhs=="(ln_mean_p_af_rast_ddhat_mth_2SP = log_mean_p_kwh_ag_default)"
replace col = 5 if ifs_sample=="" & depvar=="ihs_af_mean_ddhat_mth_2SP" & rhs=="(ln_mean_p_af_mean_ddhat_mth_2SP = log_mean_p_kwh_ag_default)"
replace col = 6 if ifs_sample==" & gw_rast_dist_mth_2SP<=8" & depvar=="ihs_af_rast_dd_mth_2SP" & rhs=="(ln_mean_p_af_rast_dd_mth_2SP = log_mean_p_kwh_ag_default)"

keep if col!=.
assert _N==6
sort col
order col

forvalues c = 1/6 {

	local beta_`c' = string(beta_log_p_water[`c'],"%9.2f")
	local se_`c' = string(se_log_p_water[`c'],"%9.2f")
	local pval_`c' = 2*ttail(dof[`c'],abs(t_log_p_water[`c']))
	if `pval_`c''<0.01 {
		local stars_`c' = "$^{***}$"
	}
	else if `pval_`c''<0.05 {
		local stars_`c' = "$^{**}$"
	}
	else if `pval_`c''<0.1 {
		local stars_`c' = "$^{*}$"
	}
	else {
		local stars_`c' = ""
	}
	
	local n_sp_`c' = string(n_SPs[`c'],"%9.0fc")
	local n_mth_`c' = string(n_modates[`c'],"%9.0fc")
	local n_obs_`c' = string(n_obs[`c']/1e6,"%9.2f") + "M"
	local fstat_`c' = string(fstat_rk[`c'],"%9.0f")
	if "`fstat_`c''"=="." {
		local fstat_`c' = ""
	}
}


	// Build table
file open textab using "$dirpath_output/table_water_kwhaf_sensitivities.tex", write text replace

file write textab "\begin{table}[t!]\centering" _n
file write textab "\small" _n
file write textab "\caption{Sensitivity to \$\widehat{\text{kWh}/\text{AF}}\$ Construction -- Groundwater  \label{tab:water_kwhaf_sensitivities}}" _n
file write textab "\vspace{-0.1cm}" _n
file write textab "\small" _n
file write textab "\begin{adjustbox}{center} " _n
file write textab "\begin{tabular}{lcccccccc} " _n
file write textab "\hline \hline" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab " & (1)  & (2)  & (3)  & (4)  & (5) & (6) \\ " _n
file write textab "[0.1em]" _n
file write textab " & IV & IV & IV & IV & IV & IV \\" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab "\cline{2-7}" _n
file write textab "\vspace{-0.27cm}" _n
file write textab "\\" _n
file write textab " $\log\big(P^{\text{water}}_{it}\big)$ ~ & " _n
file write textab " $`beta_1'$`stars_1'  & $`beta_2'$`stars_2' & $`beta_3'$`stars_3' & $`beta_4'$`stars_4' & $`beta_5'$`stars_5' & $`beta_6'$`stars_6' \\ " _n
file write textab "& $(`se_1')$ & $(`se_2')$ & $(`se_3')$ & $(`se_4')$ & $(`se_5')$ & $(`se_6')$  \\" _n
file write textab "[1.5em] " _n
file write textab "\$\widehat{{{\text{kWh}}}/{\text{AF}}}_{it}\$ criteria: \\" _n
file write textab "[0.1em] " _n
file write textab "~~ Measured, not estimated  & & Yes & & & & \\" _n
file write textab "[0.1em] " _n
file write textab "~~ Drop tests with bad drawdown  & &  & Yes & & \\" _n
file write textab "[0.1em] " _n
file write textab "~~ Time-varying predicted drawdown  & &  &  & Yes & Yes & \\" _n
file write textab "[0.1em] " _n
file write textab "~~ Mean groundwater depth  & &  &  &  & Yes & \\" _n
file write textab "[0.1em] " _n
file write textab "~~ Depth measured w/in 8 miles  & &  &  &  &  & Yes \\" _n
file write textab "[1em] " _n
file write textab "Instrument: \\" _n
file write textab "[0.1em] " _n
file write textab "~~ $\log\big(\text{Avg depth in basin}\big)$  & Yes & Yes &  &  & & \\" _n
file write textab "[0.1em] " _n
file write textab "~~ Default $\log\big(P^{\text{elec}}_{it}\big)$  & & & Yes  & Yes  & Yes & Yes \\" _n
file write textab "[1em] " _n
file write textab "Fixed effects: \\" _n
file write textab "[0.1em] " _n
file write textab "~~Unit $\times$ month-of-year  & Yes  & Yes  & Yes   & Yes  & Yes   \\ " _n
file write textab "[0.1em] " _n
file write textab "~~Month-of-sample  & Yes  & Yes  & Yes  & Yes  & Yes    \\ " _n
file write textab "[0.1em] " _n
file write textab "~~Unit $\times$ physical capital & Yes & Yes & Yes & Yes & Yes  \\" _n
*file write textab "[0.1em] " _n
*file write textab "~~Water basin $\times$ year & & &  & & Yes &   \\" _n
*file write textab "[0.1em] " _n
*file write textab "~~Water district $\times$ year & & & & & Yes &  \\" _n
file write textab "[1em] " _n
file write textab "Groundwater time step & Month & Month & Month & Month & Month & Month \\ " _n
*file write textab "[0.1em] " _n
*file write textab "Only basins with \$>1000\$ SPs &  &  & Yes &  &  &   \\ " _n
file write textab "[1em] " _n
file write textab "Service point units & `n_sp_1' & `n_sp_2' & `n_sp_3' & `n_sp_4' & `n_sp_5'  & `n_sp_6'  \\ " _n
file write textab "[0.1em] " _n
file write textab "Months  & `n_mth_1' & `n_mth_2' & `n_mth_3' & `n_mth_4' & `n_mth_5' & `n_mth_6' \\ " _n
file write textab "[0.1em] " _n
file write textab "Observations & `n_obs_1' & `n_obs_2' & `n_obs_3' & `n_obs_4' & `n_obs_5' & `n_obs_6'  \\ " _n
file write textab "[0.1em] " _n
file write textab "First stage \$F\$-statistic & `fstat_1' & `fstat_2' & `fstat_3' & `fstat_4' & `fstat_5' & `fstat_6'  \\ " _n
file write textab "[0.15em]" _n
file write textab "\hline" _n
file write textab "\end{tabular}" _n
file write textab "\end{adjustbox}" _n
file write textab "\captionsetup{width=\textwidth}" _n
file write textab "\caption*{\scriptsize \emph{Notes:} " _n
file write textab "Each regression replicates our preferred specification in Column (2) from Table \ref{tab:water_regs_combined}, " _n
file write textab "while altering our preferred method of specifying units' kWh/AF conversion factor. " _n
file write textab "Columns (1)--(2) maintain our preferred \$\widehat{\text{kWh}/\text{AF}}\$ definition, but instrument for groundwater price " _n
file write textab "using basin-wide average groundwater depths. This leverages only variation in \$P^{\text{water}}\$ driven by changes in depth. " _n
file write textab "Column (2) directly assigns kWh/AF as measured in an APEP pump test, which yields a \$P^{\text{water}}\$ variable that is " _n
file write textab "independent of changes in groundwater depth. " _n
file write textab "Column (3) removes units without a reliable drawdown measurement from an APEP pump test. " _n 
file write textab "Columns (4)--(5) construct \$\widehat{\text{kWh}/\text{AF}}\$ using predicted drawdown as a function of with groundwater depth, " _n
file write textab "rather than fixed drawdown within pumps over time. " _n
file write textab "Column (5) also applies basin-wide average depth to construct  \$\widehat{\text{kWh}/\text{AF}}\$, rather than " _n
file write textab "using localized measurements from groundwater rasters. " _n
file write textab "Column (6) uses rasterized groundwater measurements, but drop the (roughly half of) observations without a contemporaneous " _n
file write textab "groundwater measurement within 8 miles. " _n
file write textab "See notes under Table \ref{tab:water_regs_combined} for further detail. " _n
file write textab "Standard errors (in parentheses) are two-way clustered by service point and by month-of-sample." _n
file write textab "Significance: *** \$p < 0.01\$, ** \$p < 0.05\$, * \$p < 0.10\$." _n
file write textab "}" _n
file write textab "\end{table}" _n

file close textab

}

************************************************
************************************************


