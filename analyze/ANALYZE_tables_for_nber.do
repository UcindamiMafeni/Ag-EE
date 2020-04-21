clear all
version 13
set more off

*****************************************************
** Script to make additional tables for NBER paper **
*****************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"
global dirpath_output "$dirpath/output"

*global dirpath "/Users/louispreonas/Dropbox/Documents/Research/Energy Water Project"
*global dirpath "E:/Dropbox/Documents/Research/Energy Water Project"
*global dirpath_data "$dirpath"
*global dirpath_output "$dirpath/slides/tables"

************************************************
************************************************


** 1. Intensive/extensive margin results: Electricity and water
{
use "$dirpath_data/results/regs_nber_elec_annual.dta", clear
append using "$dirpath_data/results/regs_nber_water_combined_annual.dta"

drop if_sample
keep if fes == "sp_group#rt_large_ag basin_group#year wdist_group#year"
keep if regexm(depvar, "ihs") == 1 | depvar == "elec_binary"
drop if regexm(sample, "ever") == 1 | regexm(sample, "switcher") == 1
assert _N==6

gen col = .			
replace col = 1 if depvar == "ihs_kwh" & sample == "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
replace col = 2 if depvar == "ihs_kwh" & sample == "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0 & elec_binary_frac==1"
replace col = 3 if depvar == "elec_binary" & beta_log_p_mean != .
replace col = 4 if depvar == "ihs_af_rast_dd_2SP" & sample == "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
replace col = 5 if depvar == "ihs_af_rast_dd_2SP" & sample == "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0 & elec_binary_frac==1"
replace col = 6 if depvar == "elec_binary" & beta_log_p_water != .

assert col!=.
sort col
order col

forvalues c = 1/6 {
	if inlist(`c',1,2,3) {
		local beta_`c' = string(beta_log_p_mean[`c'],"%9.2f")
		local se_`c' = string(se_log_p_mean[`c'],"%9.2f")
		local pval_`c' = 2*ttail(dof[`c'],abs(t_log_p_mean[`c']))
	}
	else {
		local beta_`c' = string(beta_log_p_water[`c'],"%9.2f")
		local se_`c' = string(se_log_p_water[`c'],"%9.2f")
		local pval_`c' = 2*ttail(dof[`c'],abs(t_log_p_water[`c']))
	}
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
	local n_yr_`c' = string(n_years[`c'],"%9.0fc")
	local n_obs_`c' = string(n_obs[`c'],"%9.0fc")
	local fstat_`c' = string(fstat_rk[`c'],"%9.0f")
	if "`fstat_`c''"=="." {
		local fstat_`c' = ""
	}
}


	// Build table
file open textab using "$dirpath_output/table_ann_regs_main.tex", write text replace

file write textab "\begin{table}[t!]\centering" _n
file write textab "\small" _n
file write textab "\caption{Annual Demand Elasticities -- Electricity and Water \label{tab:ann_regs_main}}" _n
file write textab "\vspace{-0.1cm}" _n
file write textab "\small" _n
file write textab "\begin{adjustbox}{center} " _n
file write textab "\begin{tabular}{lcccccc} " _n
file write textab "\hline \hline" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab " & \multicolumn{3}{c}{Electricity} & \multicolumn{3}{c}{Water} \\" _n
file write textab " \cmidrule(r){2-4} \cmidrule(l){5-7}" _n
file write textab " & Overall & Intensive & Extensive & Overall & Intensive & Extensive \\" _n
file write textab " & elasticity & margin & margin & elasticity & margin & margin \\" _n
file write textab "[0.1em]" _n
file write textab " & (1)  & (2)  & (3)  & (4)  & (5)  & (6) \\ " _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab "\cline{2-7}" _n
file write textab "\vspace{-0.27cm}" _n
file write textab "\\" _n
file write textab " $\log\big(P^{\text{elec}}_{iy}\big)$ ~ & $`beta_1'$`stars_1'  & $`beta_2'$`stars_2' & $`beta_3'$`stars_3' &  &  &  \\ " _n
file write textab "& $(`se_1')$ & $(`se_2')$ & $(`se_3')$ &  &  &  \\" _n
file write textab "[0.1em] " _n
file write textab " $\log\big(P^{\text{water}}_{iy}\big)$ ~ &  &  &  & $`beta_4'$`stars_4' & $`beta_5'$`stars_5'  & $`beta_6'$`stars_6' \\ " _n
file write textab "&  &  &  & $(`se_4')$ & $(`se_5')$ & $(`se_6')$ \\" _n
file write textab "[1.5em] " _n
file write textab "Outcome: \\" _n
file write textab "~~ $\sinh^{-1}\big(Q_{iy}\big)$ & Yes & Yes & & Yes & Yes & \\" _n
file write textab "[0.1em] " _n
file write textab "~~ $1\big[Q_{iy}>0\big]$ & & & Yes & & & Yes \\" _n
file write textab "[1.5em] " _n
file write textab "Instrument: \\" _n
file write textab "[0.1em] " _n
file write textab "~~ Default $\log\big(P^{\text{elec}}_{iy}\big)$  & Yes & Yes & Yes  & Yes  & Yes & Yes \\" _n
file write textab "[1.5em] " _n
file write textab "Fixed effects: \\" _n
file write textab "[0.1em] " _n
file write textab "~~Unit $\times$ physical capital & Yes & Yes & Yes & Yes & Yes & Yes  \\" _n
file write textab "[0.1em] " _n
file write textab "~~Water basin $\times$ year & Yes & Yes & Yes & Yes & Yes & Yes \\" _n
file write textab "[0.1em] " _n
file write textab "~~Water district $\times$ year & Yes & Yes & Yes & Yes & Yes & Yes \\" _n
file write textab "[1.5em] " _n
file write textab "Restricted sample & & Yes & & & Yes & \\" _n
file write textab "[1.5em] " _n
file write textab "Service point units & `n_sp_1' & `n_sp_2' & `n_sp_3' & `n_sp_4' & `n_sp_5' & `n_sp_6'  \\ " _n
file write textab "[0.1em] " _n
file write textab "Years  & `n_yr_1' & `n_yr_2' & `n_yr_3' & `n_yr_4' & `n_yr_5' & `n_yr_6' \\ " _n
file write textab "[0.1em] " _n
file write textab "Observations & `n_obs_1' & `n_obs_2' & `n_obs_3' & `n_obs_4' & `n_obs_5' & `n_obs_6' \\ " _n
file write textab "[0.1em] " _n
file write textab "First stage \$F\$-statistic & `fstat_1' & `fstat_2' & `fstat_3' & `fstat_4' & `fstat_5' & `fstat_6' \\ " _n
file write textab "[0.15em]" _n
file write textab "\hline" _n
file write textab "\end{tabular}" _n
file write textab "\end{adjustbox}" _n
file write textab "\captionsetup{width=\textwidth}" _n
file write textab "\caption*{\scriptsize \emph{Notes:} Each regression estimates Equation (\ref{eq:reg_elec_annual}) or Equation (\ref{eq:reg_water_annual}) at the service point by year level." _n
file write textab "Columns (1)--(3) report results for electricity consumption, and Columns (4)--(6) report results for groundwater consumption." _n
file write textab "Columns (1) and (4) report demand elasticities for electricity and water, respectively." _n
file write textab "Columns (2) and (5) report analogous demand elasticities for the subset of service points that consume electricity or water, respectively, in every year of our sample." _n
file write textab "Columns (3) and (6) report semi-elasticities for the extensive margins by replacing the outcome variable with a binary indicator of electricity or water consumption, respectively." _n
file write textab "We estimate these regressions using two-stage least squares, instrumenting with unit \$i\$'s within-category default logged electricity price in year \$y\$." _n
file write textab "\`\`Physical capital'' is a categorical variable for (i) small pumps, (ii) large pumps, and (iii) internal combustion engines, and unit \$\times\$" _n
file write textab "physical capital fixed effects control for shifts in tariff category triggered by the installation of new pumping equipment." _n
file write textab "Water basin \$\times\$ year fixed effects control for broad geographic trends in groundwater depth." _n
file write textab "Water district \$\times\$ year fixed effects control for annual variation in surface water allocations." _n
file write textab "All regressions drop solar NEM customers, customers with bad geocodes, years with irregular electricity bills" _n
file write textab "(e.g.\ first/last bills, bills longer/shorter than 1 month, overlapping bills for a single account), and incomplete years." _n
file write textab "Standard errors (in parentheses) are clustered by service point and by year." _n
file write textab "Significance: *** \$p < 0.01\$, ** \$p < 0.05\$, * \$p < 0.10\$." _n
file write textab "}" _n
file write textab "\end{table}" _n

file close textab

}

************************************************
************************************************

** 2. Heterogeneity results: Electricity and water
{
use "$dirpath_data/results/regs_nber_elec_annual.dta", clear
append using "$dirpath_data/results/regs_nber_water_combined_annual.dta"

drop if_sample
keep if fes == "sp_group#rt_large_ag basin_group#year wdist_group#year"
keep if regexm(depvar, "ihs") == 1
keep if regexm(sample, "ever") == 1 | regexm(sample, "switcher") == 1
assert _N==6

gen col = .			
replace col = 1 if depvar == "ihs_kwh" & regexm(sample, "perennial_ever") == 1
replace col = 2 if depvar == "ihs_kwh" & regexm(sample, "annual_ever") == 1
replace col = 3 if depvar == "ihs_kwh" & regexm(sample, "switcher") == 1
replace col = 4 if depvar == "ihs_af_rast_dd_2SP" & regexm(sample, "perennial_ever") == 1
replace col = 5 if depvar == "ihs_af_rast_dd_2SP" & regexm(sample, "annual_ever") == 1
replace col = 6 if depvar == "ihs_af_rast_dd_2SP" & regexm(sample, "switcher") == 1

assert col!=.
sort col
order col

forvalues c = 1/6 {
	if inlist(`c',1,2,3) {
		local beta_`c' = string(beta_log_p_mean[`c'],"%9.2f")
		local se_`c' = string(se_log_p_mean[`c'],"%9.2f")
		local pval_`c' = 2*ttail(dof[`c'],abs(t_log_p_mean[`c']))
	}
	else {
		local beta_`c' = string(beta_log_p_water[`c'],"%9.2f")
		local se_`c' = string(se_log_p_water[`c'],"%9.2f")
		local pval_`c' = 2*ttail(dof[`c'],abs(t_log_p_water[`c']))
	}
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
	local n_yr_`c' = string(n_years[`c'],"%9.0fc")
	local n_obs_`c' = string(n_obs[`c'],"%9.0fc")
	local fstat_`c' = string(fstat_rk[`c'],"%9.0f")
	if "`fstat_`c''"=="." {
		local fstat_`c' = ""
	}
}


	// Build table
file open textab using "$dirpath_output/table_ann_regs_het.tex", write text replace

file write textab "\begin{table}[t!]\centering" _n
file write textab "\small" _n
file write textab "\caption{Annual Demand Elasticity Heterogeneity -- Electricity and Water \label{tab:ann_regs_het}}" _n
file write textab "\vspace{-0.1cm}" _n
file write textab "\small" _n
file write textab "\begin{adjustbox}{center} " _n
file write textab "\begin{tabular}{lcccccc} " _n
file write textab "\hline \hline" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab " & \multicolumn{3}{c}{Electricity} & \multicolumn{3}{c}{Water} \\" _n
file write textab " \cmidrule(r){2-4} \cmidrule(l){5-7}" _n
file write textab " & Annuals & Perennials & Switchers & Annuals & Perennials & Switchers \\" _n
file write textab "[0.1em]" _n
file write textab " & (1)  & (2)  & (3)  & (4)  & (5)  & (6) \\ " _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab "\cline{2-7}" _n
file write textab "\vspace{-0.27cm}" _n
file write textab "\\" _n
file write textab " $\log\big(P^{\text{elec}}_{iy}\big)$ ~ & $`beta_1'$`stars_1'  & $`beta_2'$`stars_2' & $`beta_3'$`stars_3' &  &  &  \\ " _n
file write textab "& $(`se_1')$ & $(`se_2')$ & $(`se_3')$ &  &  &  \\" _n
file write textab "[0.1em] " _n
file write textab " $\log\big(P^{\text{water}}_{iy}\big)$ ~ &  &  &  & $`beta_4'$`stars_4' & $`beta_5'$`stars_5'  & $`beta_6'$`stars_6' \\ " _n
file write textab "&  &  &  & $(`se_4')$ & $(`se_5')$ & $(`se_6')$ \\" _n
file write textab "[1.5em] " _n
file write textab "Instrument: \\" _n
file write textab "[0.1em] " _n
file write textab "~~ Default $\log\big(P^{\text{elec}}_{iy}\big)$  & Yes & Yes & Yes  & Yes  & Yes & Yes \\" _n
file write textab "[1.5em] " _n
file write textab "Fixed effects: \\" _n
file write textab "[0.1em] " _n
file write textab "~~Unit $\times$ physical capital & Yes & Yes & Yes & Yes & Yes & Yes  \\" _n
file write textab "[0.1em] " _n
file write textab "~~Water basin $\times$ year & Yes & Yes & Yes & Yes & Yes & Yes \\" _n
file write textab "[0.1em] " _n
file write textab "~~Water district $\times$ year & Yes & Yes & Yes & Yes & Yes & Yes \\" _n
file write textab "[1.5em] " _n
file write textab "Service point units & `n_sp_1' & `n_sp_2' & `n_sp_3' & `n_sp_4' & `n_sp_5' & `n_sp_6'  \\ " _n
file write textab "[0.1em] " _n
file write textab "Years  & `n_yr_1' & `n_yr_2' & `n_yr_3' & `n_yr_4' & `n_yr_5' & `n_yr_6' \\ " _n
file write textab "[0.1em] " _n
file write textab "Observations & `n_obs_1' & `n_obs_2' & `n_obs_3' & `n_obs_4' & `n_obs_5' & `n_obs_6' \\ " _n
file write textab "[0.1em] " _n
file write textab "First stage \$F\$-statistic & `fstat_1' & `fstat_2' & `fstat_3' & `fstat_4' & `fstat_5' & `fstat_6' \\ " _n
file write textab "[0.15em]" _n
file write textab "\hline" _n
file write textab "\end{tabular}" _n
file write textab "\end{adjustbox}" _n
file write textab "\captionsetup{width=\textwidth}" _n
file write textab "\caption*{\scriptsize \emph{Notes:} Each regression estimates Equation (\ref{eq:reg_elec_annual}) or Equation (\ref{eq:reg_water_annual}) at the service point by year level for a subset of service points." _n
file write textab "Columns (1)--(3) report results for electricity consumption, and Columns (4)--(6) report results for groundwater consumption." _n
file write textab "Columns (1) and (4) report demand elasticities for service points that have an annual crop or are fallowed in every year of our sample." _n
file write textab "Columns (2) and (5) report demand elasticities for service points that have a perennial crop or are fallowed in every year of our sample." _n
file write textab "Columns (3) and (6) report demand elasticities for service points that switch between annual and perennial crops during our sample." _n
file write textab "We estimate these regressions using two-stage least squares, instrumenting with unit \$i\$'s within-category default logged electricity price in year \$y\$." _n
file write textab "\`\`Physical capital'' is a categorical variable for (i) small pumps, (ii) large pumps, and (iii) internal combustion engines, and unit \$\times\$" _n
file write textab "physical capital fixed effects control for shifts in tariff category triggered by the installation of new pumping equipment." _n
file write textab "Water basin \$\times\$ year fixed effects control for broad geographic trends in groundwater depth." _n
file write textab "Water district \$\times\$ year fixed effects control for annual variation in surface water allocations." _n
file write textab "All regressions drop solar NEM customers, customers with bad geocodes, years with irregular electricity bills" _n
file write textab "(e.g.\ first/last bills, bills longer/shorter than 1 month, overlapping bills for a single account), and incomplete years." _n
file write textab "Standard errors (in parentheses) are clustered by service point and by year." _n
file write textab "Significance: *** \$p < 0.01\$, ** \$p < 0.05\$, * \$p < 0.10\$." _n
file write textab "}" _n
file write textab "\end{table}" _n

file close textab

}

************************************************
************************************************

** 3. Crop choice results: Electricity and water
{
use "$dirpath_data/results/regs_nber_elec_annual.dta", clear
append using "$dirpath_data/results/regs_nber_water_combined_annual.dta"

drop if_sample
keep if fes == "sp_group#rt_large_ag basin_group#year wdist_group#year"
keep if inlist(depvar, "no_crop", "annual", "perennial")
assert _N==6

gen col = .			
replace col = 1 if depvar == "annual" & beta_log_p_mean != .
replace col = 2 if depvar == "perennial" & beta_log_p_mean != .
replace col = 3 if depvar == "no_crop" & beta_log_p_mean != .
replace col = 4 if depvar == "annual" & beta_log_p_water != .
replace col = 5 if depvar == "perennial" & beta_log_p_water != .
replace col = 6 if depvar == "no_crop" & beta_log_p_water != .

assert col!=.
sort col
order col

forvalues c = 1/6 {
	if inlist(`c',1,2,3) {
		local beta_`c' = string(beta_log_p_mean[`c'],"%9.2f")
		local se_`c' = string(se_log_p_mean[`c'],"%9.2f")
		local pval_`c' = 2*ttail(dof[`c'],abs(t_log_p_mean[`c']))
	}
	else {
		local beta_`c' = string(beta_log_p_water[`c'],"%9.2f")
		local se_`c' = string(se_log_p_water[`c'],"%9.2f")
		local pval_`c' = 2*ttail(dof[`c'],abs(t_log_p_water[`c']))
	}
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
	local n_yr_`c' = string(n_years[`c'],"%9.0fc")
	local n_obs_`c' = string(n_obs[`c'],"%9.0fc")
	local fstat_`c' = string(fstat_rk[`c'],"%9.0f")
	if "`fstat_`c''"=="." {
		local fstat_`c' = ""
	}
}


	// Build table
file open textab using "$dirpath_output/table_ann_regs_crop.tex", write text replace

file write textab "\begin{table}[t!]\centering" _n
file write textab "\small" _n
file write textab "\caption{Annual Cropping Response to Electricity and Water Prices \label{tab:ann_regs_crop}}" _n
file write textab "\vspace{-0.1cm}" _n
file write textab "\small" _n
file write textab "\begin{adjustbox}{center} " _n
file write textab "\begin{tabular}{lcccccc} " _n
file write textab "\hline \hline" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab " & \multicolumn{3}{c}{Electricity} & \multicolumn{3}{c}{Water} \\" _n
file write textab " \cmidrule(r){2-4} \cmidrule(l){5-7}" _n
file write textab " & Annuals & Perennials & Fallow & Annuals & Perennials & Fallow \\" _n
file write textab "[0.1em]" _n
file write textab " & (1)  & (2)  & (3)  & (4)  & (5)  & (6) \\ " _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab "\cline{2-7}" _n
file write textab "\vspace{-0.27cm}" _n
file write textab "\\" _n
file write textab " $\log\big(P^{\text{elec}}_{iy}\big)$ ~ & $`beta_1'$`stars_1'  & $`beta_2'$`stars_2' & $`beta_3'$`stars_3' &  &  &  \\ " _n
file write textab "& $(`se_1')$ & $(`se_2')$ & $(`se_3')$ &  &  &  \\" _n
file write textab "[0.1em] " _n
file write textab " $\log\big(P^{\text{water}}_{iy}\big)$ ~ &  &  &  & $`beta_4'$`stars_4' & $`beta_5'$`stars_5'  & $`beta_6'$`stars_6' \\ " _n
file write textab "&  &  &  & $(`se_4')$ & $(`se_5')$ & $(`se_6')$ \\" _n
file write textab "[1.5em] " _n
file write textab "Instrument: \\" _n
file write textab "[0.1em] " _n
file write textab "~~ Default $\log\big(P^{\text{elec}}_{iy}\big)$  & Yes & Yes & Yes  & Yes  & Yes & Yes \\" _n
file write textab "[1.5em] " _n
file write textab "Fixed effects: \\" _n
file write textab "[0.1em] " _n
file write textab "~~Unit $\times$ physical capital & Yes & Yes & Yes & Yes & Yes & Yes  \\" _n
file write textab "[0.1em] " _n
file write textab "~~Water basin $\times$ year & Yes & Yes & Yes & Yes & Yes & Yes \\" _n
file write textab "[0.1em] " _n
file write textab "~~Water district $\times$ year & Yes & Yes & Yes & Yes & Yes & Yes \\" _n
file write textab "[1.5em] " _n
file write textab "Service point units & `n_sp_1' & `n_sp_2' & `n_sp_3' & `n_sp_4' & `n_sp_5' & `n_sp_6'  \\ " _n
file write textab "[0.1em] " _n
file write textab "Years  & `n_yr_1' & `n_yr_2' & `n_yr_3' & `n_yr_4' & `n_yr_5' & `n_yr_6' \\ " _n
file write textab "[0.1em] " _n
file write textab "Observations & `n_obs_1' & `n_obs_2' & `n_obs_3' & `n_obs_4' & `n_obs_5' & `n_obs_6' \\ " _n
file write textab "[0.1em] " _n
file write textab "First stage \$F\$-statistic & `fstat_1' & `fstat_2' & `fstat_3' & `fstat_4' & `fstat_5' & `fstat_6' \\ " _n
file write textab "[0.15em]" _n
file write textab "\hline" _n
file write textab "\end{tabular}" _n
file write textab "\end{adjustbox}" _n
file write textab "\captionsetup{width=\textwidth}" _n
file write textab "\caption*{\scriptsize \emph{Notes:} Each regression estimates Equation (\ref{eq:reg_crop_switch}) at the service point by year level for different categories of crops." _n
file write textab "Columns (1)--(3) report results for the cropping response to electricity price, and Columns (4)--(6) report results for the cropping response to groundwater price." _n
file write textab "Columns (1) and (4) report the semi-elasticity of having an annual crop with respect to the price of electricity or water, respectively." _n
file write textab "Columns (2) and (5) report the semi-elasticity of having a perennial crop with respect to the price of electricity or water, respectively." _n
file write textab "Columns (3) and (6) report the semi-elasticity of fallowing with respect to the price of electricity or water, respectively." _n
file write textab "We estimate these regressions using two-stage least squares, instrumenting with unit \$i\$'s within-category default logged electricity price in year \$y\$." _n
file write textab "\`\`Physical capital'' is a categorical variable for (i) small pumps, (ii) large pumps, and (iii) internal combustion engines, and unit \$\times\$" _n
file write textab "physical capital fixed effects control for shifts in tariff category triggered by the installation of new pumping equipment." _n
file write textab "Water basin \$\times\$ year fixed effects control for broad geographic trends in groundwater depth." _n
file write textab "Water district \$\times\$ year fixed effects control for annual variation in surface water allocations." _n
file write textab "All regressions drop solar NEM customers, customers with bad geocodes, years with irregular electricity bills" _n
file write textab "(e.g.\ first/last bills, bills longer/shorter than 1 month, overlapping bills for a single account), and incomplete years." _n
file write textab "Standard errors (in parentheses) are clustered by service point and by year." _n
file write textab "Significance: *** \$p < 0.01\$, ** \$p < 0.05\$, * \$p < 0.10\$." _n
file write textab "}" _n
file write textab "\end{table}" _n
file close textab

}

************************************************
************************************************
