clear all
version 13
set more off

***************************************
** Script to make tables for slides  **
***************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

************************************************
************************************************

** 1. PGE summary stats
{
use "$dirpath_data/merged/sp_month_elec_panel.dta" , clear

unique sp_uuid
local denom1 = r(unique)
local A1 = string(r(unique),"%9.0fc")

unique sp_uuid if pull=="20180719"
local denom2 = r(unique)
local A2 = string(r(unique),"%9.0fc")

unique sp_uuid if in_interval==1
local B1 = string(100*r(unique)/`denom1',"%9.1f")

unique sp_uuid if in_interval==1 & pull=="20180719"
local B2 = string(100*r(unique)/`denom2',"%9.1f")

unique sp_uuid if !inlist(rt_sched_cd,"AG-1A","AG-1B")
local C1 = string(100*r(unique)/`denom1',"%9.1f")

unique sp_uuid if !inlist(rt_sched_cd,"AG-1A","AG-1B") & pull=="20180719"
local C2 = string(100*r(unique)/`denom2',"%9.1f")

unique sp_uuid if inlist(rt_sched_cd,"AG-4A","AG-4D","AG-4C","AG-4F","AG-5C","AG-5F")
local D1 = string(100*r(unique)/`denom1',"%9.1f")

unique sp_uuid if inlist(rt_sched_cd,"AG-4A","AG-4D","AG-4C","AG-4F","AG-5C","AG-5F") & pull=="20180719"
local D2 = string(100*r(unique)/`denom2',"%9.1f")

unique sp_uuid if inlist(rt_sched_cd,"AG-RA","AG-RB","AG-RD","AG-RE","AG-VA","AG-VB","AG-VD","AG-VE")
local E1 = string(100*r(unique)/`denom1',"%9.1f")

unique sp_uuid if inlist(rt_sched_cd,"AG-RA","AG-RB","AG-RD","AG-RE","AG-VA","AG-VB","AG-VD","AG-VE") & pull=="20180719"
local E2 = string(100*r(unique)/`denom2',"%9.1f")

	// Build table
file open textab using "$dirpath/output/table_slides_summary_stats.tex", write text replace

file write textab "\begin{table}\centering" _n
file write textab "\caption{\normalsize Summary Statistics: Electricity Data}" _n
file write textab "\begin{tabular}{lrrrrrr}" _n
file write textab "\hline" _n
file write textab "\\ " _n
file write textab "\vspace{-8mm}" _n
file write textab "\\" _n
file write textab "&& $\begin{matrix}\text{All Ag}\\ \text{Customers}\end{matrix}$  && $\begin{matrix}\text{Matched} \\ \text{to Pumps}\end{matrix}$ \\" _n
file write textab "[.1em]" _n
file write textab "\cline{3-5}" _n
file write textab "\\" _n
file write textab "\vspace{-7mm}" _n
file write textab "\\" _n
file write textab "Unique service points && `A1' && `A2' \\ " _n
file write textab "[.6em]" _n
file write textab "\% with hourly usage data  && `B1'  &&  `B2'  \\" _n
file write textab "[.6em]" _n
file write textab "\% on time-varying tariffs  && `C1' && `C2'   \\" _n
file write textab "[.6em]" _n
file write textab "\% on peak-day tariffs  && `D1' && `D2'   \\" _n
file write textab "[.6em]" _n
file write textab "\% on design-your-own tariffs  && `E1' && `E2'   \\" _n
file write textab "[.3em]" _n
file write textab "\hline" _n
file write textab "\vspace{-2mm}" _n
file write textab "\end{tabular}" _n
file write textab "\end{table}" _n

file close textab
}

************************************************
************************************************

** 2. Results: monthly, stayers
{
use "$dirpath_data/results/regs_Qelec_Pelec.dta" , clear

keep if pull=="20180719"
keep if panel=="monthly"
keep if rhs=="log_p_mean"
keep if regexm(sample,"sp_same_rate_dumbsmart==1")
keep if inlist(fes,"sp_group#month modate", ///
					"sp_group#month basin_group#year modate", ///
					"sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year modate", ///
					"sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year modate")

sort fes sample
keep if sample == "if sp_same_rate_dumbsmart==1"
assert _N==4		

gen col = .			
replace col = 1 if fes=="sp_group#month modate"
replace col = 2 if fes=="sp_group#month basin_group#year modate"
replace col = 3 if fes=="sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year modate"
replace col = 4 if fes=="sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year modate"
assert col!=.
sort col

forvalues c = 1/4 {
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
	local n_obs_`c' = string(n_obs[`c'],"%9.0fc")
}


	// Build table
file open textab using "$dirpath/output/table_regs_monthly_stayers.tex", write text replace

file write textab "\begin{table}\centering" _n
file write textab "\caption{\normalsize Monthly data --- \`\`{\bf Stayers}''}" _n
file write textab "\footnotesize" _n
file write textab "\vspace{-2mm}" _n
file write textab "\begin{tabular}{lccccccc}" _n
file write textab "\hline" _n
file write textab "\\ " _n
file write textab "\vspace{-7mm}" _n
file write textab "\\" _n
file write textab "& (1) & (2) & (3) & (4)  \\" _n
file write textab "[.1em]" _n
file write textab "\cline{2-5}" _n
file write textab "\\" _n
file write textab "\vspace{-7mm}" _n
file write textab "\\" _n
file write textab "\small \$\log(P_{\text{mean}})\$ & $`beta_1'$`stars_1'  & $`beta_2'$`stars_2' & $`beta_3'$`stars_3' & $`beta_4'$`stars_4'\\" _n
file write textab "& $(`se_1')$ & $(`se_2')$ & $(`se_3')$ & $(`se_4')$\\" _n
file write textab "[1em]" _n
file write textab "SP \$\times\$ month FEs  & Yes & Yes   & Yes & Yes \\" _n
file write textab "[.25em]" _n
file write textab "Month-of-sample FEs  &  Yes & Yes  & Yes  & Yes\\" _n
file write textab "[.25em]" _n
file write textab "Basin-by-year FEs & &  Yes & Yes & Yes \\" _n
file write textab "[.25em]" _n
file write textab "SP \$\times\$ month \$\times\$ groundwater depth &  & & Yes & Yes  \\" _n
file write textab "[.25em]" _n
file write textab "Water district \$\times\$ year FEs  & & & & Yes  \\" _n
file write textab "[1em]" _n
file write textab "Unique SPs & `n_sp_1' & `n_sp_2' & `n_sp_3' & `n_sp_4' \\" _n
file write textab "[.15em]" _n
file write textab "SP-month observations & `n_obs_1'& `n_obs_2'& `n_obs_3'& `n_obs_4' \\" _n
file write textab "\hline" _n
file write textab "\vspace{-2mm}" _n
file write textab "\\" _n
file write textab "\multicolumn{5}{l}{Standard errors two-way clustered by SP and month-of-sample." _n
file write textab "SP = service point.} \\" _n
file write textab "\multicolumn{5}{l}{Dependent variable is \$\log(\text{kWh})\$, from \`\`monthified'' billing data.} \\" _n
file write textab "\multicolumn{5}{l}{Significance: *** \$p < 0.01\$, ** \$p < 0.05\$, * \$p < 0.10\$.}" _n
file write textab "\end{tabular}" _n
file write textab "\end{table}" _n

file close textab
}

************************************************
************************************************

** 3. Results: monthly, forced switchers
{
use "$dirpath_data/results/regs_Qelec_Pelec.dta" , clear

keep if pull=="20180719"
keep if panel=="monthly"
keep if rhs=="log_p_mean" | regexm(rhs," = ")
drop if regexm(rhs,"init")
drop if rhs=="(log_p_mean = log_p_mean_lag12)"
drop if rhs=="(log_p_mean = log_mean_p_kwh_ag_default)"
keep if regexm(sample,"sp_same_rate_dumbsmart==0 & sp_same_rate_in_cat==1")
keep if inlist(fes,"sp_group#month modate", ///
					"sp_group#month basin_group#year modate", ///
					"sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year modate", ///
					"sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year modate")

sort fes rhs sample
keep if sample == "if sp_same_rate_dumbsmart==0 & sp_same_rate_in_cat==1"
keep if inlist(rhs,"log_p_mean","(log_p_mean = log_p_m*_lag12 log_p_m*_lag6)")
assert _N==8		

gen col = .			
replace col = 1 if fes=="sp_group#month modate" & rhs=="log_p_mean"
replace col = 2 if fes=="sp_group#month basin_group#year modate" & rhs=="log_p_mean"
replace col = 3 if fes=="sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year modate" & rhs=="log_p_mean"
replace col = 4 if fes=="sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year modate" & rhs=="log_p_mean"
replace col = 5 if fes=="sp_group#month modate" & rhs!="log_p_mean"
replace col = 6 if fes=="sp_group#month basin_group#year modate" & rhs!="log_p_mean"
replace col = 7 if fes=="sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year modate" & rhs!="log_p_mean"
replace col = 8 if fes=="sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year modate" & rhs!="log_p_mean"
assert col!=.
sort col

forvalues c = 1/8 {
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
	local n_obs_`c' = string(n_obs[`c'],"%9.0fc")
	local fstat_`c' = string(fstat_rk[`c'],"%9.0fc")
}


	// Build table
file open textab using "$dirpath/output/table_regs_monthly_forced_switchers.tex", write text replace

file write textab "\begin{table}\centering" _n
file write textab "\caption{\normalsize Monthly data --- \`\`{\bf Forced Switchers}''}" _n
file write textab "\footnotesize" _n
file write textab "\vspace{-2mm}" _n
file write textab "\begin{tabular}{lccccccc}" _n
file write textab "\hline" _n
file write textab "\\ " _n
file write textab "\vspace{-7mm}" _n
file write textab "\\" _n
file write textab "& (1) & (2) & (3) & (4)  \\" _n
file write textab "[.1em]" _n
file write textab "\cline{2-5}" _n
file write textab "\\" _n
file write textab "\vspace{-7mm}" _n
file write textab "\\" _n
file write textab "\color{lightgrey}\small OLS: \$\log(P_{\text{mean}})\$ & \color{lightgrey}$`beta_1'$`stars_1'  & \color{lightgrey}$`beta_2'$`stars_2' & \color{lightgrey}$`beta_3'$`stars_3' & \color{lightgrey} $`beta_4'$`stars_4'\\" _n
file write textab "&\color{lightgrey} $(`se_1')$ & \color{lightgrey} $(`se_2')$ & \color{lightgrey} $(`se_3')$ & \color{lightgrey} $(`se_4')$\\" _n
file write textab "[1em]" _n
file write textab "\small IV: \$\log(P_{\text{mean}})\$ & $`beta_5'$`stars_5'  & $`beta_6'$`stars_6' & $`beta_7'$`stars_7' & $`beta_8'$`stars_8'\\" _n
file write textab "& $(`se_5')$ & $(`se_6')$ & $(`se_7')$ & $(`se_8')$\\" _n
file write textab "[1em]" _n
file write textab "SP \$\times\$ month FEs  & Yes & Yes   & Yes & Yes \\" _n
file write textab "[.05em]" _n
file write textab "Month-of-sample FEs  &  Yes & Yes  & Yes  & Yes\\" _n
file write textab "[.05em]" _n
file write textab "Basin-by-year FEs & &  Yes & Yes & Yes \\" _n
file write textab "[.05em]" _n
file write textab "SP \$\times\$ month \$\times\$ groundwater depth &  & & Yes & Yes  \\" _n
file write textab "[.05em]" _n
file write textab "Water district \$\times\$ year FEs  & & & & Yes  \\" _n
file write textab "[0.5em]" _n
file write textab "Unique SPs & `n_sp_5' & `n_sp_6' & `n_sp_7' & `n_sp_8' \\" _n
file write textab "[.05em]" _n
file write textab "SP-month observations & `n_obs_5'& `n_obs_6'& `n_obs_7'& `n_obs_8' \\" _n
file write textab "[.05em]" _n
file write textab "First-stage \$F\$-stat (IV) & `fstat_5'& `fstat_6'& `fstat_7'& `fstat_8' \\" _n
file write textab "\hline" _n
file write textab "\vspace{-3mm}" _n
file write textab "\\" _n
file write textab "\multicolumn{5}{l}{Standard errors two-way clustered by SP and month-of-sample." _n
file write textab "SP = service point.} \\" _n
file write textab "\multicolumn{5}{l}{Dependent variable is \$\log(\text{kWh})\$, from \`\`monthified'' billing data. Instruments: 6-} \\" _n
file write textab "\multicolumn{5}{l}{and 12-month lagged prices. Significance: *** \$p < 0.01\$, ** \$p < 0.05\$, * \$p < 0.10\$.}" _n
file write textab "\end{tabular}" _n
file write textab "\end{table}" _n

file close textab
}

************************************************
************************************************

** 4. Results: monthly, choosers
{
use "$dirpath_data/results/regs_Qelec_Pelec.dta" , clear

keep if pull=="20180719"
keep if panel=="monthly"
keep if rhs=="log_p_mean" | regexm(rhs," = ")
drop if regexm(rhs,"init")
drop if rhs=="(log_p_mean = log_p_mean_lag12)"
drop if rhs=="(log_p_mean = log_mean_p_kwh_ag_default)"
keep if regexm(sample,"sp_same_rate_in_cat==0")
keep if inlist(fes,"sp_group#month modate", ///
					"sp_group#month basin_group#year modate", ///
					"sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year modate", ///
					"sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year modate")

sort fes rhs sample
keep if sample == "if sp_same_rate_in_cat==0"
keep if inlist(rhs,"log_p_mean","(log_p_mean = log_mean_p_kwh_ag_default log_min_p_kwh_ag_default log_max_p_kwh_ag_default)")
assert _N==8		

gen col = .			
replace col = 1 if fes=="sp_group#month modate" & rhs=="log_p_mean"
replace col = 2 if fes=="sp_group#month basin_group#year modate" & rhs=="log_p_mean"
replace col = 3 if fes=="sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year modate" & rhs=="log_p_mean"
replace col = 4 if fes=="sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year modate" & rhs=="log_p_mean"
replace col = 5 if fes=="sp_group#month modate" & rhs!="log_p_mean"
replace col = 6 if fes=="sp_group#month basin_group#year modate" & rhs!="log_p_mean"
replace col = 7 if fes=="sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year modate" & rhs!="log_p_mean"
replace col = 8 if fes=="sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year modate" & rhs!="log_p_mean"
assert col!=.
sort col

forvalues c = 1/8 {
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
	local n_obs_`c' = string(n_obs[`c'],"%9.0fc")
	local fstat_`c' = string(fstat_rk[`c'],"%9.0fc")
}


	// Build table
file open textab using "$dirpath/output/table_regs_monthly_choosers.tex", write text replace

file write textab "\begin{table}\centering" _n
file write textab "\caption{\normalsize Monthly data --- \`\`{\bf Choosers}''}" _n
file write textab "\footnotesize" _n
file write textab "\vspace{-2mm}" _n
file write textab "\begin{tabular}{lccccccc}" _n
file write textab "\hline" _n
file write textab "\\ " _n
file write textab "\vspace{-7mm}" _n
file write textab "\\" _n
file write textab "& (1) & (2) & (3) & (4)  \\" _n
file write textab "[.1em]" _n
file write textab "\cline{2-5}" _n
file write textab "\\" _n
file write textab "\vspace{-7mm}" _n
file write textab "\\" _n
file write textab "\color{lightgrey}\small OLS: \$\log(P_{\text{mean}})\$ & \color{lightgrey}$`beta_1'$`stars_1'  & \color{lightgrey}$`beta_2'$`stars_2' & \color{lightgrey}$`beta_3'$`stars_3' & \color{lightgrey} $`beta_4'$`stars_4'\\" _n
file write textab "&\color{lightgrey} $(`se_1')$ & \color{lightgrey} $(`se_2')$ & \color{lightgrey} $(`se_3')$ & \color{lightgrey} $(`se_4')$\\" _n
file write textab "[1em]" _n
file write textab "\small IV: \$\log(P_{\text{mean}})\$ & $`beta_5'$`stars_5'  & $`beta_6'$`stars_6' & $`beta_7'$`stars_7' & $`beta_8'$`stars_8'\\" _n
file write textab "& $(`se_5')$ & $(`se_6')$ & $(`se_7')$ & $(`se_8')$\\" _n
file write textab "[1em]" _n
file write textab "SP \$\times\$ month FEs  & Yes & Yes   & Yes & Yes \\" _n
file write textab "[.05em]" _n
file write textab "Month-of-sample FEs  &  Yes & Yes  & Yes  & Yes\\" _n
file write textab "[.05em]" _n
file write textab "Basin-by-year FEs & &  Yes & Yes & Yes \\" _n
file write textab "[.05em]" _n
file write textab "SP \$\times\$ month \$\times\$ groundwater depth &  & & Yes & Yes  \\" _n
file write textab "[.05em]" _n
file write textab "Water district \$\times\$ year FEs  & & & & Yes  \\" _n
file write textab "[0.5em]" _n
file write textab "Unique SPs & `n_sp_5' & `n_sp_6' & `n_sp_7' & `n_sp_8' \\" _n
file write textab "[.05em]" _n
file write textab "SP-month observations & `n_obs_5'& `n_obs_6'& `n_obs_7'& `n_obs_8' \\" _n
file write textab "[.05em]" _n
file write textab "First-stage \$F\$-stat (IV) & `fstat_5'& `fstat_6'& `fstat_7'& `fstat_8' \\" _n
file write textab "\hline" _n
file write textab "\vspace{-3mm}" _n
file write textab "\\" _n
file write textab "\multicolumn{5}{l}{Standard errors two-way clustered by SP and month-of-sample." _n
file write textab "SP = service point.} \\" _n
file write textab "\multicolumn{5}{l}{Dependent variable is \$\log(\text{kWh})\$, from \`\`monthified'' billing data. Instruments: } \\" _n
file write textab "\multicolumn{5}{l}{prices of default rates. Significance: *** \$p < 0.01\$, ** \$p < 0.05\$, * \$p < 0.10\$.}" _n
file write textab "\end{tabular}" _n
file write textab "\end{table}" _n

file close textab
}

************************************************
************************************************
