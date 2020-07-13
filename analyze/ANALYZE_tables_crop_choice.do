clear all
version 13
set more off

*****************************************************
** Script to make additional tables for NBER paper **
*****************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"
global dirpath_output "$dirpath/output"

************************************************
************************************************

// load results for model with county-by-year FEs
use "$dirpath_data/results/probit_crop_choice.dta", clear
keep if model == 2
sort outcome

// calculate acreage changes from baseline
foreach v of varlist acres* {
	local v2 = subinstr("`v'","acres","delta",1)
	local v3 = "abs_`v2'"
	gen `v2' = `v' - acres_0tax
	gen `v3' = abs(`v2')
}

// loop over outcomes to save marginal effects and elasticity results
forvalues r = 1/4 {
	local mfx_`r' = string(mfx[`r']*100,"%9.3f")
	local mfx_se_`r' = string(mfx_se[`r']*100,"%9.3f")
	local elas_`r' = string(elas[`r'],"%9.3f")
	local elas_se_`r' = string(elas_se[`r'],"%9.3f")
	
	local pval_mfx_`r' = 2*ttail(999999,abs(mfx[`r']/mfx_se[`r']))
	if `pval_mfx_`r''<0.01 {
		local stars_mfx_`r' = "$^{***}$"
	}
	else if `pval_mfx_`r''<0.05 {
		local stars_mfx_`r' = "$^{**}$"
	}
	else if `pval_mfx_`r''<0.1 {
		local stars_mfx_`r' = "$^{*}$"
	}
	else {
		local stars_mfx_`r' = ""
	}

	local pval_elas_`r' = 2*ttail(999999,abs(elas[`r']/elas_se[`r']))
	if `pval_elas_`r''<0.01 {
		local stars_elas_`r' = "$^{***}$"
	}
	else if `pval_elas_`r''<0.05 {
		local stars_elas_`r' = "$^{**}$"
	}
	else if `pval_elas_`r''<0.1 {
		local stars_elas_`r' = "$^{*}$"
	}
	else {
		local stars_elas_`r' = ""
	}
}

// save number of CLUs and observations, first-stage chi-squared stat, and mean water price
local n_clu = string(n_clu[1],"%9.0fc")
local n_obs = string(n_obs[1],"%9.0fc")
local chi2 = string(chi2[1],"%9.0f")
*local mean_p_water = mean_p_water[1]
local mean_p_water = 41
local mean_p_water_str = string(`mean_p_water',"%9.2f")

// save total acreage in sample
qui summ acres_0tax
local acres_total = r(sum)
local acres_total_str = string(`acres_total',"%9.0fc")

// loop over tax counterfactuals to save counterfactual acreage results
foreach t in 0 5 10 15 20 25 {
	// loop over outcomes
	forvalues r = 1/4 {
		local acres_`t'tax_`r' = string(acres_`t'tax[`r']/1000,"%9.2f")
	}
	// save results for total reallocation
	qui summ abs_delta_`t'tax
	local acres_`t'tax_reall = string(r(sum)/1000,"%9.2f")
	local acres_`t'tax_reall_pct = string(r(sum)/`acres_total'*100,"%9.1f")
}

// calculate groundwater response for each tax level
use "$dirpath_data/results/regs_pge_water_combined_monthly_july2020.dta" , clear
keep if pull=="PGE 20180719" ///
	& panel=="monthly" ///
	& ifs_base=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0" ///
	& depvar=="ihs_af_rast_dd_mth_2SP" ///
	& fes=="sp_group#month sp_group#rt_large_ag modate" ///
	& rhs=="(ln_mean_p_af_rast_dd_mth_2SP = log_mean_p_kwh_ag_default)" ///
	& ifs_sample==""
local water_elas = beta_log_p_water[1]
local water_elas_str = string(`water_elas',"%9.2f")
foreach t in 0 5 10 15 20 25 {
	local water_`t'tax_pct = string(`t'/`mean_p_water'*`water_elas'*100,"%9.1f")
}

// build marginal effects and semi-elasticty table
file open textab using "$dirpath_output/table_probit_results.tex", write text replace

file write textab "\begin{table}[t!]\centering" _n
file write textab "\small" _n
file write textab "\caption{Discrete choice estimates of crop switching \label{tab:probit_results}}" _n
file write textab "\vspace{-0.1cm}" _n
file write textab "\small" _n
file write textab "\begin{adjustbox}{center} " _n
file write textab "\begin{tabular}{lcc} " _n
file write textab "\hline \hline" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab " & Marginal effect & Semi-elasticity \\" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab "\cline{2-3}" _n
file write textab "\vspace{-0.27cm}" _n
file write textab "\\" _n
file write textab "Annuals & $`mfx_2'$`stars_mfx_2' & $`elas_2'$`stars_elas_2' \\ " _n
file write textab "& $(`mfx_se_2')$ & $(`elas_se_2')$ \\ " _n
file write textab "[0.1em]" _n
file write textab "Fruit and nut perennials & $`mfx_3'$`stars_mfx_3' & $`elas_3'$`stars_elas_3' \\ " _n
file write textab "& $(`mfx_se_3')$ & $(`elas_se_3')$ \\ " _n
file write textab "[0.1em]" _n
file write textab "Other perennials & $`mfx_4'$`stars_mfx_4' & $`elas_4'$`stars_elas_4' \\ " _n
file write textab "& $(`mfx_se_4')$ & $(`elas_se_4')$ \\ " _n
file write textab "[0.1em]" _n
file write textab "Fallow & $`mfx_1'$`stars_mfx_1' & $`elas_1'$`stars_elas_1' \\ " _n
file write textab "& $(`mfx_se_1')$ & $(`elas_se_1')$ \\ " _n
file write textab "[1.5em]" _n
file write textab "Instrument: \\ " _n
file write textab "[0.1em] " _n
file write textab "~~Default $\log(P^{\text{elec}}_{iy})$ & \multicolumn{2}{c}{Yes} \\ " _n
file write textab "Fixed effects: \\ " _n
file write textab "[0.1em] " _n
file write textab "~~County $\times$ year $\times$ crop type & \multicolumn{2}{c}{Yes} \\ " _n
file write textab "[1.5em] " _n
file write textab "Common land units & \multicolumn{2}{c}{`n_clu'} \\ " _n
file write textab "[0.1em] " _n
file write textab "Observations & \multicolumn{2}{c}{`n_obs'} \\ " _n
file write textab "[0.1em] " _n
file write textab "First stage \$\chi^2\$-statistic & \multicolumn{2}{c}{`chi2'} \\ " _n
file write textab "[0.15em]" _n
file write textab "\hline" _n
file write textab "\end{tabular}" _n
file write textab "\end{adjustbox}" _n
file write textab "\captionsetup{width=\textwidth}" _n
file write textab "\caption*{\scriptsize \emph{Notes:} This table reports the mean marginal effects and semi-elasticities with respect to groundwater price that are implied by the results of our multinomial probit model of crop choice." _n
file write textab "Standard errors (in parentheses) are clustered at the common land unit (CLU)." _n
file write textab "}" _n
file write textab "\end{table}" _n

file close textab

// build counterfactual acreage table table
file open textab using "$dirpath_output/table_sim_acres.tex", write text replace

file write textab "\begin{table}[t!]\centering" _n
file write textab "\small" _n
file write textab "\caption{Crop choice and groundwater use under counterfactual groundwater taxes \label{tab:sim_acres}}" _n
file write textab "\vspace{-0.1cm}" _n
file write textab "\small" _n
file write textab "\begin{adjustbox}{center} " _n
file write textab "\begin{tabular}{lcccc} " _n
file write textab "\hline \hline" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab " & No tax & \\$5 tax & \\$10 tax & \\$15 tax \\" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab "\cline{2-5}" _n
file write textab "\vspace{-0.27cm}" _n
file write textab "\\" _n
file write textab "Simulated acreage (thousands of acres) & & & & \\ " _n
file write textab "~~Annuals & $`acres_0tax_2'$ & $`acres_5tax_2'$ & $`acres_10tax_2'$ & $`acres_15tax_2'$ \\ " _n
file write textab "[0.1em]" _n
file write textab "~~Fruit and nut perennials & $`acres_0tax_3'$ & $`acres_5tax_3'$ & $`acres_10tax_3'$ & $`acres_15tax_3'$ \\ " _n
file write textab "[0.1em]" _n
file write textab "~~Other perennials & $`acres_0tax_4'$ & $`acres_5tax_4'$ & $`acres_10tax_4'$ & $`acres_15tax_4'$ \\ " _n
file write textab "[0.1em]" _n
file write textab "~~Fallow & $`acres_0tax_1'$ & $`acres_5tax_1'$ & $`acres_10tax_1'$ & $`acres_15tax_1'$ \\ " _n
file write textab "[0.5em]" _n
file write textab "~~Total reallocation & & $`acres_5tax_reall'$ & $`acres_10tax_reall'$ & $`acres_15tax_reall'$ \\ " _n
file write textab "~~Total reallocation (percent) & & $`acres_5tax_reall_pct'\%$ & $`acres_10tax_reall_pct'\%$ & $`acres_15tax_reall_pct'\%$ \\ " _n
file write textab "[0.5em]" _n
file write textab "Change in groundwater consumption (percent) & & $`water_5tax_pct'\%$ & $`water_10tax_pct'\%$ & $`water_15tax_pct'\%$ \\ " _n
file write textab "[0.15em]" _n
file write textab "\hline" _n
file write textab "\end{tabular}" _n
file write textab "\end{adjustbox}" _n
file write textab "\captionsetup{width=\textwidth}" _n
file write textab "\caption*{\scriptsize \emph{Notes:} This table reports the results of adding counterfactual taxes on groundwater to the observed electricity prices in our sample." _n
file write textab "To simulate the impacts of a groundwater tax, we first calculate the choice probability of each crop type (annuals, fruit/nut perennials, other perennials, and no crop) for each CLU in our sample over our time series." _n
file write textab "This baseline allocation is represented in the first column, labeled \`\`No tax.'' The sample average marginal price is \\$`mean_p_water_str' per acre-foot. " _n
file write textab "In the subsequent columns, we take each CLU's average annual marginal price and add the reported tax level to it. We then calculate choice probabilities for this counterfactual groundwater price." _n
file write textab "The first four rows correspond to the four crop types in our analysis, and the table displays the total acreage in our sample that we predict would be cropped in each crop type under each of the tax levels." _n
file write textab "The fifth row reports the total acreage of cropland that is reallocated to a different crop type due to the groundwater tax, as compared to no tax." _n
file write textab "The sixth row displays the total percent change in land use for each tax level, as compared to no tax." _n
file write textab "These reallocations estimates are based on the `acres_total_str' acres of agricultural land matched to our sample." _n
file write textab "The final row reports the estimated change in groundwater consumption, using our groundwater elasticity estimate of $`water_elas_str'$, for each tax level. " _n
file write textab "}" _n
file write textab "\end{table}" _n

file close textab
