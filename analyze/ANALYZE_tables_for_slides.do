clear all
version 13
set more off

***************************************
** Script to make tables for slides  **
***************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"
global dirpath_output "$dirpath/output"

*global dirpath "/Users/louispreonas/Dropbox/Documents/Research/Energy Water Project"
*global dirpath "E:/Dropbox/Documents/Research/Energy Water Project"
*global dirpath_data "$dirpath"
*global dirpath_output "$dirpath/slides/tables"

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
file open textab using "$dirpath_output/table_slides_summary_stats.tex", write text replace

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

** 2. PGE rates (examples)
{
use "$dirpath_data/pge_cleaned/large_ag_rates.dta", clear
rename demandcharge demandcharge_large // large rates: demandcharge = "$/kW max (part)peak demand"
rename pdpcredit pdpcredit_large // large rates: pdpcredit = "$/kW"
gen large = 1
append using "$dirpath_data/pge_cleaned/small_ag_rates.dta"
replace large = 0 if large==.
rename demandcharge demandcharge_hp // small rates: demandcharge = "$/hp per month"
rename pdpcredit pdpcredit_hp // small rates: pdpcredit = "$/hp connected load"
replace demandcharge_hp = 0 if demandcharge_hp==. & large==0
la var demandcharge_hp "Fixed charge in $/hp-month of connected load (small ag rates only)"
la var pdpcredit_hp "Fixed charge in $/hp-month of connected load (small ag rates only)"
drop large
rename demandcharge_large demandcharge
rename pdpcredit_large pdpcredit
unique rateschedule-peak
assert r(unique)==r(N)
unique rateschedule-minute
assert r(unique)==r(N)

keep if rate_start_date==date("01mar2014","DMY")
keep if inlist(rates,"AG-4A","AG-4B","AG-4C","AG-5A","AG-5B","AG-5C")
drop tou group dow_num hour minute
duplicates drop

gen msize = ""
replace msize = "\$< 35\$ hp" if inlist(rates,"AG-4A","AG-5A") 
replace msize = "\$\ge 35\$ hp" if inlist(rates,"AG-4B","AG-5B","AG-4C","AG-5C")

gen fc1 = customercharge + metercharge
gen fc2 = demandcharge_hp 
replace fc1 = 0 if fc1==.
replace fc2 = 0 if fc2==.
replace maxdemandcharge = 0 if maxdemandcharge==.
replace demandcharge = 0 if demandcharge==.

gen group = 0
replace group = 1 if rateschedule=="AG-4A"
replace group = 2 if rateschedule=="AG-4B"
replace group = 3 if rateschedule=="AG-4C"
replace group = 4 if rateschedule=="AG-5A"
replace group = 5 if rateschedule=="AG-5B"
replace group = 6 if rateschedule=="AG-5C"


local c = 0
foreach rate in 1 2 6 {
	
	local c = `c' + 1
	
	preserve 
	qui keep if group==`rate'
	
	local rate_`c' = rateschedule[1]
	local msize_`c' = msize[1]
	
	sum fc1 
	assert r(sd)==0 | r(sd)==.
	local fc1_`c' = string(r(mean),"%9.2f")

	sum fc2 
	local fc2_`c' = string(r(max),"%9.2f")
	
	sum maxdemandcharge if season=="winter"
	local dc1_`c' = string(r(mean),"%9.2f")

	sum demandcharge if season=="winter" & partpeak==1 
	assert r(sd)==0 | r(sd)==.
	if r(mean)==. {
		local dc2_`c' = string(0,"%9.2f")
	}
	else {
		local dc2_`c' = string(r(mean),"%9.2f")
	}

	sum maxdemandcharge if season=="summer" 
	assert r(sd)==0 | r(sd)==.
	local dc3_`c' = string(r(mean),"%9.2f")
	
	sum demandcharge if season=="summer" & partpeak==1
	assert r(sd)==0 | r(sd)==.
	if r(mean)==. {
		local dc4_`c' = string(0,"%9.2f")
	}
	else {
		local dc4_`c' = string(r(mean),"%9.2f")
	}
	
	sum demandcharge if season=="summer" & peak==1
	assert r(sd)==0 | r(sd)==.
	local dc5_`c' = string(r(mean),"%9.2f")
	
	sum energycharge if season=="winter" & offpeak==1
	assert r(sd)==0 | r(sd)==.
	local vp1_`c' = string(r(mean),"%9.3f")
	
	sum energycharge if season=="winter" & partpeak==1
	assert r(sd)==0 | r(sd)==.
	if r(mean)==. {
		local vp2_`c' = string(`vp1_`c'',"%9.3f")
	}
	else {
		local vp2_`c' = string(r(mean),"%9.3f")
	}
	
	sum energycharge if season=="summer" & offpeak==1
	assert r(sd)==0 | r(sd)==.
	local vp3_`c' = string(r(mean),"%9.3f")
	
	sum energycharge if season=="summer" & partpeak==1
	assert r(sd)==0 | r(sd)==.
	if r(mean)==. {
		local vp4_`c' = string(`vp3_`c'',"%9.3f")
	}
	else {
		local vp4_`c' = string(r(mean),"%9.3f")
	}
	
	sum energycharge if season=="summer" & peak==1
	assert r(sd)==0 | r(sd)==.
	local vp5_`c' = string(r(mean),"%9.3f")
	
	sum pdpcharge
	if r(mean)!=0 {
		local pdp_`c' = "\$^{*}\$"
	}
	else {
		local pdp_`c' = ""
	}
	
	restore
}


	// Build table
file open textab using "$dirpath_output/table_rates_example.tex", write text replace

file write textab "\begin{table}\centering" _n
file write textab "\small" _n
file write textab "\begin{tabular}{|l|r|r|r|r|rr}" _n
file write textab "\hline" _n
file write textab "&&&\\ " _n
file write textab "\vspace{-8mm}" _n
file write textab "&&&\\" _n
file write textab "Tariff & \multicolumn{1}{c|}{ `rate_1'~} & \multicolumn{1}{c|}{ `rate_2'~} & \multicolumn{1}{c|}{ `rate_3'~} \\" _n
file write textab "[.1em]" _n
file write textab "\hline" _n
file write textab "&&&\\" _n
file write textab "\vspace{-7mm}" _n
file write textab "&&&\\" _n
file write textab "Motor size & `msize_1'~ & `msize_2'~ & `msize_3'~ \\" _n
file write textab "[.2em]" _n
file write textab "\hline" _n
file write textab "&&&\\" _n
file write textab "\vspace{-7mm}" _n
file write textab "&&&\\" _n
file write textab "Fixed charge (\\$/day)      & `fc1_1'~ & `fc1_2'~ & `fc1_3'~ \\" _n
file write textab "Fixed charge (\\$/hp-month) & `fc2_1'~ & `fc2_2'~ & `fc2_3'~~ \\" _n
file write textab "[.2em]" _n
file write textab "\hline" _n
file write textab "&&&\\" _n
file write textab "\vspace{-7mm}" _n
file write textab "&&&\\" _n
file write textab "Demand charges (\\$/kW) &&& \\" _n
file write textab "[.1em]" _n
file write textab "~~~ \color{midblue} Winter base             & \color{midblue} `dc1_1'~ & \color{midblue} `dc1_2'~ & \color{midblue} `dc1_3'~ \\" _n
file write textab "~~~ \color{midblue} Winter partial peak max & \color{midblue} `dc2_1'~ & \color{midblue} `dc2_2'~ & \color{midblue} `dc2_3'~ \\" _n
file write textab "~~~ \color{cranberry} Summer base             & \color{cranberry} `dc3_1'~ & \color{cranberry} `dc3_2'~ & \color{cranberry} `dc3_3'~ \\" _n
file write textab "~~~ \color{cranberry} Summer partial peak max & \color{cranberry} `dc4_1'~ & \color{cranberry} `dc4_2'~ & \color{cranberry} `dc4_3'~ \\" _n
file write textab "~~~ \color{cranberry} Summer peak max         & \color{cranberry} `dc5_1'~ & \color{cranberry} `dc5_2'~ & \color{cranberry} `dc5_3'~~ \\" _n
file write textab "[.2em]" _n
file write textab "\hline" _n
file write textab "&&&\\" _n
file write textab "\vspace{-7mm}" _n
file write textab "&&&\\" _n
file write textab "Volumetric prices (\\$/kWh) &&&\\" _n
file write textab "[.1em]" _n
file write textab "~~~ \color{midblue} Winter off-peak     & \color{midblue} `vp1_1'~ & \color{midblue} `vp1_2'~ & \color{midblue} `vp1_3'~ \\" _n
file write textab "~~~ \color{midblue} Winter partial peak & \color{midblue} `vp2_1'~ & \color{midblue} `vp2_2'~ & \color{midblue} `vp2_3'~ \\" _n
file write textab "~~~ \color{cranberry} Summer off-peak     & \color{cranberry} `vp3_1'~ & \color{cranberry} `vp3_2'~ & \color{cranberry} `vp3_3'~ \\" _n
file write textab "~~~ \color{cranberry} Summer partial peak & \color{cranberry} `vp4_1'~ & \color{cranberry} `vp4_2'~ & \color{cranberry} `vp4_3'~ \\" _n
file write textab "~~~ \color{cranberry} Summer peak         & \color{cranberry} `vp5_1'`pdp_1' & \color{cranberry} `vp5_2'`pdp_2'~ & \color{cranberry} `vp5_3'`pdp_3' \\" _n
file write textab "\hline" _n
file write textab "\multicolumn{1}{c}{\scriptsize Rates for March--April 2014.}" _n
file write textab "\end{tabular}" _n
file write textab "\end{table}" _n


file close textab
}

************************************************
************************************************

** 3. Results: electricity, monthly
{
use "$dirpath_data/results/regs_paper_elec_monthly.dta" , clear
br
keep if pull=="20180719"
keep if panel=="monthly"
keep if sample=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3"
keep if inlist(rhs,"log_p_mean","(log_p_mean = log_mean_p_kwh_ag_default)","(log_p_mean = log_p_mean_deflag*)")
keep if inlist(fes,"sp_group#month modate", ///
					"sp_group#month sp_group#rt_large_ag modate")
assert _N==4

gen col = .			
replace col = 1 if fes=="sp_group#month modate" & rhs=="log_p_mean"
replace col = 2 if fes=="sp_group#month modate" & rhs=="(log_p_mean = log_mean_p_kwh_ag_default)"
replace col = 3 if fes=="sp_group#month sp_group#rt_large_ag modate" & rhs=="(log_p_mean = log_mean_p_kwh_ag_default)"
replace col = 4 if fes=="sp_group#month sp_group#rt_large_ag modate" & rhs=="(log_p_mean = log_p_mean_deflag*)"

assert col!=.
sort col
order col

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
	local n_obs_`c' = string(n_obs[`c']/1e6,"%9.2f") + "M"
	local fstat_`c' = string(fstat_rk[`c'],"%9.0fc")
	if "`fstat_`c''"=="." {
		local fstat_`c' = ""
	}
}


	// Build table
file open textab using "$dirpath_output/table_regs_elec_main.tex", write text replace

file write textab "\begin{table}\centering" _n
file write textab "\caption{\normalsize Electricity Demand -- Monthly Elasticities}" _n
file write textab "\footnotesize" _n
file write textab "\vspace{-5mm}" _n
file write textab "\begin{tabular}{lccccccc}" _n
file write textab "\hline" _n
file write textab "\\ " _n
file write textab "\vspace{-7mm}" _n
file write textab "\\" _n
file write textab "& ~~~~~~(1)~~~~~~ & ~~~~~~(2)~~~~~~ & ~~~~~~(3)~~~~~~ & ~~~~~~(4)~~~~~~  \\" _n
file write textab "[.1em]" _n
file write textab "& OLS & IV & IV & IV \\" _n
file write textab "[.1em]" _n
file write textab "\cline{2-5}" _n
file write textab "\\" _n
file write textab "\vspace{-7mm}" _n
file write textab "\\" _n
file write textab "\small \$\log(P^{\text{elec}}_{it})\$ & $`beta_1'$`stars_1'  & $`beta_2'$`stars_2' & $`beta_3'$`stars_3' & $`beta_4'$`stars_4'\\" _n
file write textab "& $(`se_1')$ & $(`se_2')$ & $(`se_3')$ & $(`se_4')$\\" _n
*file write textab "[1em]" _n
*file write textab "Instrument(s): \\" _n
*file write textab "[0.1em] " _n
*file write textab "~~ Default $\log\big(P^{\text{elec}}_{it}\big)$  & & Yes & Yes  & \\" _n
*file write textab "[0.1em] " _n
*file write textab "~~ Default $\log\big(P^{\text{elec}}_{it}\big)$, lagged  & & & & Yes \\" _n
file write textab "[1.2em]" _n
file write textab "SP \$\times\$ month FEs  & Yes & Yes  & Yes & Yes \\" _n
file write textab "[.25em]" _n
file write textab "Month-of-sample FEs  &  Yes & Yes  & Yes  & Yes\\" _n
file write textab "[.25em]" _n
file write textab "SP \$\times\$ 1[\$\ge\$ 35 hp] FEs & &  & Yes & Yes \\" _n
file write textab "[.25em]" _n
file write textab "IV: 6- \& 12-month lags & &  &  & Yes \\" _n
*file write textab "[.25em]" _n
*file write textab "SP \$\times\$ month \$\times\$ groundwater depth &  & & Yes & Yes  \\" _n
*file write textab "[.25em]" _n
*file write textab "Water district \$\times\$ year FEs  & & & & Yes  \\" _n
file write textab "[1.2em]" _n
file write textab "Unique SPs & `n_sp_1' & `n_sp_2' & `n_sp_3' & `n_sp_4' \\" _n
file write textab "[.15em]" _n
file write textab "SP-month observations & `n_obs_1'& `n_obs_2'& `n_obs_3'& `n_obs_4' \\" _n
file write textab "[0.15em] " _n
file write textab "First stage \$F\$-statistic & `fstat_1' & `fstat_2' & `fstat_3' & `fstat_4' \\ " _n
file write textab "\hline" _n
file write textab "\vspace{-2mm}" _n
file write textab "\\" _n
file write textab "\multicolumn{5}{l}{Standard errors two-way clustered by SP and month-of-sample." _n
file write textab "SP = service point.} \\" _n
file write textab "\multicolumn{5}{l}{Dependent variable is \$\sinh^{-1}(Q^{\text{elec}}_{it})\$, from \`\`monthified'' billing data.} \\" _n
file write textab "\multicolumn{5}{l}{IV: within-category default \$\log\big(P^{\text{elec}}_{it}\big)\$.} \\" _n
file write textab "\multicolumn{5}{l}{Significance: *** \$p < 0.01\$, ** \$p < 0.05\$, * \$p < 0.10\$.}" _n
file write textab "\end{tabular}" _n
file write textab "\end{table}" _n

file close textab
}

************************************************
************************************************

** 4. Results: electricity, hourly
{
use "$dirpath_data/results/regs_slides_elec_hourly.dta" , clear
br
keep if pull=="20180719"
keep if panel=="hourly"
keep if sample=="" //| regexm(sample,"summer")
assert _N==4		

gen col = .			
replace col = 1 if fes=="sp_group#month sp_group#hour sp_group#rt_large_ag modate" & sample==""
replace col = 2 if fes=="sp_group#month sp_group#hour sp_group#rt_large_ag modate#rt_group" & sample==""
replace col = 3 if fes=="sp_group#month#hour sp_group#rt_large_ag modate" & sample==""
replace col = 4 if fes=="sp_group#month#hour sp_group#rt_large_ag modate#rt_group" & sample==""
assert col!=.
sort col

forvalues c = 1/4 {
	local beta_`c' = string(beta_log_p[`c'],"%9.2f")
	local se_`c' = string(se_log_p[`c'],"%9.2f")
	local pval_`c' = 2*ttail(dof[`c'],abs(t_log_p[`c']))
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
	local n_obs_`c' = string(n_obs[`c']/1e6,"%9.0f")
	local fstat_`c' = string(fstat_rk[`c'],"%9.0fc")
}


	// Build table
file open textab using "$dirpath_output/table_regs_hourly_pooled.tex", write text replace

file write textab "\begin{table}\centering" _n
file write textab "\caption{\normalsize Electricity Demand -- Hourly Elasticities}" _n
file write textab "\footnotesize" _n
file write textab "\vspace{-2mm}" _n
file write textab "\begin{tabular}{lccccccc}" _n
file write textab "\hline" _n
file write textab "\\ " _n
file write textab "\vspace{-7mm}" _n
file write textab "\\" _n
file write textab "& ~~~~~(1)~~~~~ & ~~~~~(2)~~~~~ & ~~~~~(3)~~~~~ & ~~~~(4)~~~~  \\" _n
file write textab "[.1em]" _n
file write textab "\cline{2-5}" _n
file write textab "\\" _n
file write textab "\vspace{-7mm}" _n
file write textab "\\" _n
file write textab "\small \$\log(P_{\text{mean}})\$ & $`beta_1'$`stars_1'  & $`beta_2'$`stars_2' & $`beta_3'$`stars_3' & $`beta_4'$`stars_4'\\" _n
file write textab "& $(`se_1')$ & $(`se_2')$ & $(`se_3')$ & $(`se_4')$\\" _n
file write textab "[1.2em]" _n
file write textab "SP \$\times\$ month FEs  & Yes & Yes   & Yes & Yes \\" _n
file write textab "[.15em]" _n
file write textab "SP \$\times\$ hour FEs  & Yes & Yes   & Yes & Yes \\" _n
file write textab "[.15em]" _n
file write textab "SP \$\times\$ 1[\$\ge\$ 35 hp] FEs & Yes & Yes & Yes & Yes \\" _n
file write textab "[.15em]" _n
file write textab "Month-of-sample FEs  &  Yes & Yes  & Yes  & Yes \\" _n
file write textab "[1.2em]" _n
file write textab "SP \$\times\$ month \$\times\$ hour FEs  &  &   & Yes & Yes \\" _n
file write textab "[.15em]" _n
file write textab "Month-of-sample \$\times\$ rate FEs & & Yes &  & Yes \\" _n
file write textab "[1.2em]" _n
file write textab "Unique SPs & `n_sp_1' & `n_sp_2' & `n_sp_3' & `n_sp_4' \\" _n
file write textab "[.15em]" _n
file write textab "SP-month-hour observations & `n_obs_1'M & `n_obs_2'M & `n_obs_3'M & `n_obs_4'M \\" _n
file write textab "[.15em]" _n
file write textab "First-stage \$F\$-stat (IV) & `fstat_1'& `fstat_2'& `fstat_3'& `fstat_4' \\" _n
file write textab "\hline" _n
file write textab "\vspace{-2mm}" _n
file write textab "\\" _n
file write textab "\multicolumn{5}{l}{Standard errors two-way clustered by SP and month-of-sample." _n
file write textab "SP = service point.} \\" _n
file write textab "\multicolumn{5}{l}{Dependent variable is \$\sinh^{-1}(Q^{\text{elec}}_{it})\$, from hourly interval data.} \\" _n
file write textab "\multicolumn{5}{l}{IV: within-category default \$\log\big(P^{\text{elec}}_{it}\big)\$. Sig: *** \$p < 0.01\$, ** \$p < 0.05\$, * \$p < 0.10\$.}" _n
file write textab "\end{tabular}" _n
file write textab "\end{table}" _n

file close textab
}

************************************************
************************************************

** 5. Results: groundwater, monthly, split
{
use "$dirpath_data/results/regs_Qwater_Pwater.dta" , clear

keep if regexm(sample,"if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0")
keep if regexm(fes,"sp_group#rt_large_ag")
keep if regexm(rhs,"_dd_mth_2SP")
drop if regexm(rhs,"deflag")
keep if regexm(rhs,"default")

gen col = .			

replace col = 1 if fes=="sp_group#month sp_group#rt_large_ag modate" ///
	& rhs=="(log_p_mean = log_mean_p_kwh_ag_default) ln_kwhaf_rast_dd_mth_2SP" ///
	& sample=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	
replace col = 2 if fes=="sp_group#month sp_group#rt_large_ag modate" ///
	& rhs=="(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)" ///
	& sample=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"

replace col = 3 if fes=="sp_group#month sp_group#rt_large_ag modate" ///
	& rhs=="(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)" ///
	& sample=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & inlist(basin_group,68,121,122)"


drop if col==.
assert _N==3
sort col
order col


forvalues c = 1/3 {
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
file open textab using "$dirpath_output/table_regs_water_split.tex", write text replace

file write textab "\begin{table}\centering" _n
*file write textab "\caption{\normalsize Groundwater Demand -- Split Elasticities}" _n
file write textab "\footnotesize" _n
file write textab "\vspace{-1mm}" _n
file write textab "\begin{tabular}{lccccccc}" _n
file write textab "\hline" _n
file write textab "\\ " _n
file write textab "\vspace{-7mm}" _n
file write textab "\\" _n
file write textab "& ~~~~~~(1)~~~~~~ & ~~~~~~(2)~~~~~~ & ~~~~~~(3)~~~~~~ \\" _n
file write textab "[.1em]" _n
*file write textab "& IV & IV & IV  \\" _n
*file write textab "[.1em]" _n
file write textab "\cline{2-4}" _n
file write textab "\\" _n
file write textab "\vspace{-7mm}" _n
file write textab "\\" _n
file write textab " \small $\log\big(P^{\text{elec}}_{it}\big)$: \$\hat\beta^{\text{e}}\$ ~ & " _n
file write textab " $`betaE_1'$`starsE_1'  & $`betaE_2'$`starsE_2' & $`betaE_3'$`starsE_3'  \\ " _n
file write textab "& $(`seE_1')$ & $(`seE_2')$ & $(`seE_3')$ \\" _n
file write textab "[0.75em] " _n
file write textab " \small $\log\Big(\widehat{\tfrac{{\text{kWh}}}{\text{AF}}}_{it}\Big)$: \$\hat\beta^{\text{w}}\$ ~ & " _n
file write textab " $`betaW_1'$`starsW_1'  & $`betaW_2'$`starsW_2' & $`betaW_3'$`starsW_3'  \\ " _n
file write textab "[-0.3em] " _n
file write textab "& $(`seW_1')$ & $(`seW_2')$ & $(`seW_3')$   \\" _n
file write textab "[1em] " _n
file write textab "IV: Default $\log\big(P^{\text{elec}}_{it}\big)$  & Yes & Yes & Yes  \\" _n
file write textab "[0.1em] " _n
file write textab "IV: Default $\log\big(\text{Avg depth in basin}\big)$  & & Yes & Yes  \\" _n
file write textab "[0.1em] " _n
file write textab "Only basins with \$> 1,000\$ SPs  & &  & Yes  \\" _n
file write textab "[1em]" _n
file write textab "SP \$\times\$ month FEs  & Yes & Yes  & Yes  \\" _n
*file write textab "[.25em]" _n
file write textab "Month-of-sample FEs  &  Yes & Yes  & Yes  \\" _n
*file write textab "[.25em]" _n
file write textab "SP \$\times\$ 1[\$\ge\$ 35 hp] FEs & Yes & Yes & Yes  \\" _n
*file write textab "[.25em]" _n
*file write textab "SP \$\times\$ month \$\times\$ groundwater depth &  & & Yes & Yes  \\" _n
*file write textab "[.25em]" _n
*file write textab "Water district \$\times\$ year FEs  & & & & Yes  \\" _n
*file write textab "[1.2em]" _n
file write textab "Unique SPs & `n_sp_1' & `n_sp_2' & `n_sp_3'  \\" _n
*file write textab "[.15em]" _n
file write textab "SP-month observations & `n_obs_1'& `n_obs_2'& `n_obs_3'  \\" _n
*file write textab "[0.15em] " _n
file write textab "First stage \$F\$-statistic & `fstat_1' & `fstat_2' & `fstat_3'  \\ " _n
file write textab "\hline" _n
file write textab "\vspace{-3.5mm}" _n
file write textab "\\" _n
file write textab "\multicolumn{4}{l}{\scriptsize Standard errors two-way clustered by SP and month-of-sample." _n
file write textab "SP = service point.} \\" _n
file write textab "\multicolumn{4}{l}{\scriptsize Dependent variable is \$\sinh^{-1}(Q^{\text{elec}}_{it})\$, from \`\`monthified'' billing data. IVs: within-category} \\" _n
file write textab "\multicolumn{4}{l}{\scriptsize  default \$\log\big(P^{\text{elec}}_{it}\big)\$ and avg depth across basin. Sig: *** \$p < 0.01\$, ** \$p < 0.05\$, * \$p < 0.10\$.}" _n
file write textab "\end{tabular}" _n
file write textab "\end{table}" _n

file close textab
}

************************************************
************************************************

** 6. Results: groundwater, monthly, combined
{
use "$dirpath_data/results/regs_Qwater_Pwater_combined.dta" , clear

keep if regexm(sample,"if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0")
*keep if regexm(fes,"sp_group#rt_large_ag")
keep if regexm(rhs,"_dd_mth_2SP") | regexm(rhs,"_dd_qtr_2SP")

gen col = .			

replace col = 1 if fes=="sp_group#month modate" ///
	& rhs=="ln_mean_p_af_rast_dd_mth_2SP" ///
	& sample=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	
replace col = 2 if fes=="sp_group#month modate" ///
	& rhs=="(ln_mean_p_af_rast_dd_mth_2SP = log_mean_p_kwh_ag_default)" ///
	& sample=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"

replace col = 3 if fes=="sp_group#month sp_group#rt_large_ag modate" ///
	& rhs=="(ln_mean_p_af_rast_dd_mth_2SP = log_mean_p_kwh_ag_default)" ///
	& sample=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"

replace col = 4 if fes=="sp_group#month sp_group#rt_large_ag modate" ///
	& rhs=="(ln_mean_p_af_rast_dd_mth_2SP = log_p_mean_deflag*)" ///
	& sample=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"

	
drop if col==.
assert _N==4
sort col
order col

forvalues c = 1/4 {
	local beta_`c' = string(beta_log_p_af[`c'],"%9.2f")
	local se_`c' = string(se_log_p_af[`c'],"%9.2f")
	local pval_`c' = 2*ttail(dof[`c'],abs(t_log_p_af[`c']))
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
	local n_obs_`c' = string(n_obs[`c']/1e6,"%9.2f") + "M"
	local fstat_`c' = string(fstat_rk[`c'],"%9.0fc")
	if "`fstat_`c''"=="." {
		local fstat_`c' = ""
	}
}


	// Build table
file open textab using "$dirpath_output/table_regs_water_main.tex", write text replace

file write textab "\begin{table}\centering" _n
file write textab "\caption{\normalsize Estimated Demand Elasticities -- Groundwater}" _n
file write textab "\footnotesize" _n
file write textab "\vspace{-5mm}" _n
file write textab "\begin{tabular}{lccccccc}" _n
file write textab "\hline" _n
file write textab "\\ " _n
file write textab "\vspace{-7mm}" _n
file write textab "\\" _n
file write textab "& ~~~~~~(1)~~~~~~ & ~~~~~~(2)~~~~~~ & ~~~~~~(3)~~~~~~ & ~~~~~~(4)~~~~~~  \\" _n
file write textab "[.1em]" _n
file write textab "& OLS & IV & IV & IV \\" _n
file write textab "[.1em]" _n
file write textab "\cline{2-5}" _n
file write textab "\\" _n
file write textab "\vspace{-7mm}" _n
file write textab "\\" _n
file write textab "\small \$\log(P^{\text{water}}_{it})\$ & $`beta_1'$`stars_1'  & $`beta_2'$`stars_2' & $`beta_3'$`stars_3' & $`beta_4'$`stars_4'\\" _n
file write textab "& $(`se_1')$ & $(`se_2')$ & $(`se_3')$ & $(`se_4')$\\" _n
*file write textab "[1em]" _n
*file write textab "Instrument(s): \\" _n
*file write textab "[0.1em] " _n
*file write textab "~~ Default $\log\big(P^{\text{elec}}_{it}\big)$  & & Yes & Yes  & \\" _n
*file write textab "[0.1em] " _n
*file write textab "~~ Default $\log\big(P^{\text{elec}}_{it}\big)$, lagged  & & & & Yes \\" _n
file write textab "[1.2em]" _n
file write textab "SP \$\times\$ month FEs  & Yes & Yes  & Yes & Yes \\" _n
file write textab "[.25em]" _n
file write textab "Month-of-sample FEs  &  Yes & Yes  & Yes  & Yes\\" _n
file write textab "[.25em]" _n
file write textab "SP \$\times\$ 1[\$\ge\$ 35] FEs & &  & Yes & Yes \\" _n
file write textab "[.25em]" _n
file write textab "IV: 6- \& 12-month lags & &  &  & Yes \\" _n
*file write textab "[.25em]" _n
*file write textab "SP \$\times\$ month \$\times\$ groundwater depth &  & & Yes & Yes  \\" _n
*file write textab "[.25em]" _n
*file write textab "Water district \$\times\$ year FEs  & & & & Yes  \\" _n
file write textab "[1.2em]" _n
file write textab "Unique SPs & `n_sp_1' & `n_sp_2' & `n_sp_3' & `n_sp_4' \\" _n
file write textab "[.15em]" _n
file write textab "SP-month observations & `n_obs_1'& `n_obs_2'& `n_obs_3'& `n_obs_4' \\" _n
file write textab "[0.15em] " _n
file write textab "First stage \$F\$-statistic & `fstat_1' & `fstat_2' & `fstat_3' & `fstat_4' \\ " _n
file write textab "\hline" _n
file write textab "\vspace{-2mm}" _n
file write textab "\\" _n
file write textab "\multicolumn{5}{l}{Standard errors two-way clustered by SP and month-of-sample." _n
file write textab "SP = service point.} \\" _n
file write textab "\multicolumn{5}{l}{Dependent variable is \$\sinh^{-1}(Q^{\text{water}}_{it})\$, where " _n
file write textab "\$Q^{\text{water}}_{it} = Q^{\text{elec}}_{it} \div \left(\frac{\text{kWh}}{\text{AF}}\right)_{it}\$. } \\" _n
file write textab "\multicolumn{5}{l}{IV: within-category default \$\log\big(P^{\text{elec}}_{it}\big)\$.} \\" _n
file write textab "\multicolumn{5}{l}{Significance: *** \$p < 0.01\$, ** \$p < 0.05\$, * \$p < 0.10\$.}" _n
file write textab "\end{tabular}" _n
file write textab "\end{table}" _n

file close textab
}

************************************************
************************************************


TABLES BELOW HERE ARE OUT OF DATE

** 3. Results: monthly, stayers
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
keep if sample != "if sp_same_rate_dumbsmart==1"
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
file open textab using "$dirpath_output/table_regs_monthly_stayers.tex", write text replace

file write textab "\begin{table}\centering" _n
file write textab "\caption{\normalsize Monthly data --- \`\`{\bf Stayers}''}" _n
file write textab "\footnotesize" _n
file write textab "\vspace{-2mm}" _n
file write textab "\begin{tabular}{lccccccc}" _n
file write textab "\hline" _n
file write textab "\\ " _n
file write textab "\vspace{-7mm}" _n
file write textab "\\" _n
file write textab "& ~~~~(1)~~~~ & ~~~~(2)~~~~ & ~~~~(3)~~~~ & ~~~~(4)~~~~  \\" _n
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

** 4. Results: monthly, forced switchers
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
keep if sample != "if sp_same_rate_dumbsmart==0 & sp_same_rate_in_cat==1"
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
file open textab using "$dirpath_output/table_regs_monthly_forced_switchers.tex", write text replace

file write textab "\begin{table}\centering" _n
file write textab "\caption{\normalsize Monthly data --- \`\`{\bf Forced Switchers}''}" _n
file write textab "\footnotesize" _n
file write textab "\vspace{-2mm}" _n
file write textab "\begin{tabular}{lccccccc}" _n
file write textab "\hline" _n
file write textab "\\ " _n
file write textab "\vspace{-7mm}" _n
file write textab "\\" _n
file write textab "& ~~~~(1)~~~~ & ~~~~(2)~~~~ & ~~~~(3)~~~~ & ~~~~(4)~~~~  \\" _n
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

** 5. Results: monthly, choosers
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
keep if sample != "if sp_same_rate_in_cat==0"
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
file open textab using "$dirpath_output/table_regs_monthly_choosers.tex", write text replace

file write textab "\begin{table}\centering" _n
file write textab "\caption{\normalsize Monthly data --- \`\`{\bf Choosers}''}" _n
file write textab "\footnotesize" _n
file write textab "\vspace{-2mm}" _n
file write textab "\begin{tabular}{lccccccc}" _n
file write textab "\hline" _n
file write textab "\\ " _n
file write textab "\vspace{-7mm}" _n
file write textab "\\" _n
file write textab "& ~~~~(1)~~~~ & ~~~~(2)~~~~ & ~~~~(3)~~~~ & ~~~~(4)~~~~  \\" _n
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

** 6. Results: hourly, stayers
{
use "$dirpath_data/results/regs_Qelec_Pelec_hourly.dta" , clear
br
keep if pull=="20180719"
keep if panel=="hourly"
keep if rhs=="log_p"
keep if regexm(sample,"sp_same_rate_dumbsmart==1")
keep if inlist(fes,"sp_group#month#hour modate", ///
					"sp_group#month#hour basin_group#year modate", ///
					"sp_group#month#hour sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year modate", ///
					"sp_group#month#hour sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year modate")

sort fes sample
keep if sample != "if sp_same_rate_dumbsmart==1"
assert _N==4		

gen col = .			
replace col = 1 if fes=="sp_group#month#hour modate"
replace col = 2 if fes=="sp_group#month#hour basin_group#year modate"
replace col = 3 if fes=="sp_group#month#hour sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year modate"
replace col = 4 if fes=="sp_group#month#hour sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year modate"
assert col!=.
sort col

forvalues c = 1/4 {
	local beta_`c' = string(beta_log_p[`c'],"%9.2f")
	local se_`c' = string(se_log_p[`c'],"%9.2f")
	local pval_`c' = 2*ttail(dof[`c'],abs(t_log_p[`c']))
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
	local n_obs_`c' = string(n_obs[`c']/1e6,"%9.0f")
}


	// Build table
file open textab using "$dirpath_output/table_regs_hourly_stayers.tex", write text replace

file write textab "\begin{table}\centering" _n
file write textab "\caption{\normalsize Hourly data --- \`\`{\bf Stayers}''}" _n
file write textab "\footnotesize" _n
file write textab "\vspace{-2mm}" _n
file write textab "\begin{tabular}{lccccccc}" _n
file write textab "\hline" _n
file write textab "\\ " _n
file write textab "\vspace{-7mm}" _n
file write textab "\\" _n
file write textab "& ~~~~(1)~~~~ & ~~~~(2)~~~~ & ~~~~(3)~~~~ & ~~~~(4)~~~~  \\" _n
file write textab "[.1em]" _n
file write textab "\cline{2-5}" _n
file write textab "\\" _n
file write textab "\vspace{-7mm}" _n
file write textab "\\" _n
file write textab "\small \$\log(P_{\text{mean}})\$ & $`beta_1'$`stars_1'  & $`beta_2'$`stars_2' & $`beta_3'$`stars_3' & $`beta_4'$`stars_4'\\" _n
file write textab "& $(`se_1')$ & $(`se_2')$ & $(`se_3')$ & $(`se_4')$\\" _n
file write textab "[1em]" _n
file write textab "SP \$\times\$ month \$\times\$ hour FEs  & Yes & Yes   & Yes & Yes \\" _n
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
file write textab "SP-month-hour observations & `n_obs_1'M & `n_obs_2'M & `n_obs_3'M & `n_obs_4'M \\" _n
file write textab "\hline" _n
file write textab "\vspace{-2mm}" _n
file write textab "\\" _n
file write textab "\multicolumn{5}{l}{Standard errors two-way clustered by SP and month-of-sample." _n
file write textab "SP = service point.} \\" _n
file write textab "\multicolumn{5}{l}{Dependent variable is \$\log(\text{kWh})\$, from hourly interval data.} \\" _n
file write textab "\multicolumn{5}{l}{Significance: *** \$p < 0.01\$, ** \$p < 0.05\$, * \$p < 0.10\$.}" _n
file write textab "\end{tabular}" _n
file write textab "\end{table}" _n

file close textab
}

************************************************
************************************************

** 7. Results: hourly, forced switchers
{
use "$dirpath_data/results/regs_Qelec_Pelec_hourly.dta" , clear

keep if pull=="20180719"
keep if panel=="hourly"
keep if rhs=="log_p" | regexm(rhs," = ")
drop if regexm(rhs,"init")
keep if regexm(sample,"sp_same_rate_dumbsmart==0 & sp_same_rate_in_cat==1")
keep if inlist(fes,"sp_group#month#hour modate", ///
					"sp_group#month#hour basin_group#year modate", ///
					"sp_group#month#hour sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year modate", ///
					"sp_group#month#hour sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year modate")

sort fes rhs sample
keep if sample != "if sp_same_rate_dumbsmart==0 & sp_same_rate_in_cat==1"
assert _N==8		

gen col = .			
replace col = 1 if fes=="sp_group#month#hour modate" & rhs=="log_p"
replace col = 2 if fes=="sp_group#month#hour basin_group#year modate" & rhs=="log_p"
replace col = 3 if fes=="sp_group#month#hour sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year modate" & rhs=="log_p"
replace col = 4 if fes=="sp_group#month#hour sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year modate" & rhs=="log_p"
replace col = 5 if fes=="sp_group#month#hour modate" & rhs!="log_p"
replace col = 6 if fes=="sp_group#month#hour basin_group#year modate" & rhs!="log_p"
replace col = 7 if fes=="sp_group#month#hour sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year modate" & rhs!="log_p"
replace col = 8 if fes=="sp_group#month#hour sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year modate" & rhs!="log_p"
assert col!=.
sort col

forvalues c = 1/8 {
	local beta_`c' = string(beta_log_p[`c'],"%9.2f")
	local se_`c' = string(se_log_p[`c'],"%9.2f")
	local pval_`c' = 2*ttail(dof[`c'],abs(t_log_p[`c']))
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
	local n_obs_`c' = string(n_obs[`c']/1e6,"%9.0f")
	local fstat_`c' = string(fstat_rk[`c'],"%9.0fc")
}


	// Build table
file open textab using "$dirpath_output/table_regs_hourly_forced_switchers.tex", write text replace

file write textab "\begin{table}\centering" _n
file write textab "\caption{\normalsize Hourly data --- \`\`{\bf Forced Switchers}''}" _n
file write textab "\footnotesize" _n
file write textab "\vspace{-2mm}" _n
file write textab "\begin{tabular}{lccccccc}" _n
file write textab "\hline" _n
file write textab "\\ " _n
file write textab "\vspace{-7mm}" _n
file write textab "\\" _n
file write textab "& ~~~~(1)~~~~ & ~~~~(2)~~~~ & ~~~~(3)~~~~ & ~~~~(4)~~~~  \\" _n
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
file write textab "SP \$\times\$ month \$\times\$ hour FEs  & Yes & Yes   & Yes & Yes \\" _n
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
file write textab "SP-hour observations & `n_obs_5'M & `n_obs_6'M & `n_obs_7'M & `n_obs_8'M \\" _n
file write textab "[.05em]" _n
file write textab "First-stage \$F\$-stat (IV) & `fstat_5'& `fstat_6'& `fstat_7'& `fstat_8' \\" _n
file write textab "\hline" _n
file write textab "\vspace{-3mm}" _n
file write textab "\\" _n
file write textab "\multicolumn{5}{l}{Standard errors two-way clustered by SP and month-of-sample." _n
file write textab "SP = service point.} \\" _n
file write textab "\multicolumn{5}{l}{Dependent variable is \$\log(\text{kWh})\$, from hourly interval data. Instruments: 6-} \\" _n
file write textab "\multicolumn{5}{l}{and 12-month lagged prices. Significance: *** \$p < 0.01\$, ** \$p < 0.05\$, * \$p < 0.10\$.}" _n
file write textab "\end{tabular}" _n
file write textab "\end{table}" _n

file close textab
}

************************************************
************************************************

** 8. Results: hourly, choosers
{
use "$dirpath_data/results/regs_Qelec_Pelec_hourly.dta" , clear

keep if pull=="20180719"
keep if panel=="hourly"
keep if rhs=="log_p" | regexm(rhs," = ")
keep if regexm(sample,"sp_same_rate_in_cat==0")
keep if inlist(fes,"sp_group#month#hour modate", ///
					"sp_group#month#hour basin_group#year modate", ///
					"sp_group#month#hour sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year modate", ///
					"sp_group#month#hour sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year modate")

sort fes rhs sample
keep if sample != "if sp_same_rate_in_cat==0"

assert _N==8		

gen col = .			
replace col = 1 if fes=="sp_group#month#hour modate" & rhs=="log_p"
replace col = 2 if fes=="sp_group#month#hour basin_group#year modate" & rhs=="log_p"
replace col = 3 if fes=="sp_group#month#hour sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year modate" & rhs=="log_p"
replace col = 4 if fes=="sp_group#month#hour sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year modate" & rhs=="log_p"
replace col = 5 if fes=="sp_group#month#hour modate" & rhs!="log_p"
replace col = 6 if fes=="sp_group#month#hour basin_group#year modate" & rhs!="log_p"
replace col = 7 if fes=="sp_group#month#hour sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year modate" & rhs!="log_p"
replace col = 8 if fes=="sp_group#month#hour sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year modate" & rhs!="log_p"
assert col!=.
sort col

forvalues c = 1/8 {
	local beta_`c' = string(beta_log_p[`c'],"%9.2f")
	local se_`c' = string(se_log_p[`c'],"%9.2f")
	local pval_`c' = 2*ttail(dof[`c'],abs(t_log_p[`c']))
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
	local n_obs_`c' = string(n_obs[`c']/1e6,"%9.0f")
	local fstat_`c' = string(fstat_rk[`c'],"%9.0fc")
}


	// Build table
file open textab using "$dirpath_output/table_regs_hourly_choosers.tex", write text replace

file write textab "\begin{table}\centering" _n
file write textab "\caption{\normalsize Hourly data --- \`\`{\bf Choosers}''}" _n
file write textab "\footnotesize" _n
file write textab "\vspace{-2mm}" _n
file write textab "\begin{tabular}{lccccccc}" _n
file write textab "\hline" _n
file write textab "\\ " _n
file write textab "\vspace{-7mm}" _n
file write textab "\\" _n
file write textab "& ~~~~(1)~~~~ & ~~~~(2)~~~~ & ~~~~(3)~~~~ & ~~~~(4)~~~~  \\" _n
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
file write textab "SP \$\times\$ month \$\times\$ hour FEs  & Yes & Yes   & Yes & Yes \\" _n
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
file write textab "SP-hour observations & `n_obs_5'M& `n_obs_6'M& `n_obs_7'M& `n_obs_8'M \\" _n
file write textab "[.05em]" _n
file write textab "First-stage \$F\$-stat (IV) & `fstat_5'& `fstat_6'& `fstat_7'& `fstat_8' \\" _n
file write textab "\hline" _n
file write textab "\vspace{-3mm}" _n
file write textab "\\" _n
file write textab "\multicolumn{5}{l}{Standard errors two-way clustered by SP and month-of-sample." _n
file write textab "SP = service point.} \\" _n
file write textab "\multicolumn{5}{l}{Dependent variable is \$\log(\text{kWh})\$, from hourly interval data. Instruments: } \\" _n
file write textab "\multicolumn{5}{l}{prices of default rates. Significance: *** \$p < 0.01\$, ** \$p < 0.05\$, * \$p < 0.10\$.}" _n
file write textab "\end{tabular}" _n
file write textab "\end{table}" _n

file close textab
}

************************************************
************************************************

** 9. Results: monthly, stayers
{
use "$dirpath_data/results/regs_Qwater_Pwater.dta" , clear

keep if regexm(sample,"sp_same_rate_dumbsmart==1")
keep if inlist(fes,"sp_group#month wdist_group#year modate")
keep if assns=="rast_dd_mth_2SP"
					
sort fes sample rhs
keep if sample != "if sp_same_rate_dumbsmart==1"
assert _N==4		

gen col = .			
replace col = 1 if trim(rhs)=="ln_mean_p_af_X"
replace col = 2 if trim(rhs)=="(ln_mean_p_af_X = kwhaf_apep_measured)"
replace col = 3 if trim(rhs)=="log_p_mean ln_kwhaf_X"
replace col = 4 if trim(rhs)=="(ln_kwhaf_X = kwhaf_apep_measured) log_p_mean"
assert col!=.
sort col

forvalues c = 1/4 {
	local beta_kwh_`c' = string(beta_log_p_kwh[`c'],"%9.2f")
	local se_kwh_`c' = string(se_log_p_kwh[`c'],"%9.2f")
	if "`se_kwh_`c''"!="." {
		local se_kwh_`c' = "(`se_kwh_`c'')" 
	}
	else {
		local beta_kwh_`c' = "" 
		local se_kwh_`c' = "" 
	}
	local pval_kwh_`c' = 2*ttail(dof[`c'],abs(t_log_p_kwh[`c']))
	if `pval_kwh_`c''<0.01 {
		local stars_kwh_`c' = "$^{***}$"
	}
	else if `pval_kwh_`c''<0.05 {
		local stars_kwh_`c' = "$^{**}$"
	}
	else if `pval_kwh_`c''<0.1 {
		local stars_kwh_`c' = "$^{*}$"
	}
	else {
		local stars_kwh_`c' = ""
	}
	
	local beta_kwhaf_`c' = string(beta_log_kwhaf[`c'],"%9.2f")
	local se_kwhaf_`c' = string(se_log_kwhaf[`c'],"%9.2f")
	if "`se_kwhaf_`c''"!="" {
		local se_kwhaf_`c' = "(`se_kwhaf_`c'')" 
	}
	else {
		local beta_kwhaf_`c' = "" 
		local se_kwhaf_`c' = "" 
	}
	local pval_kwhaf_`c' = 2*ttail(dof[`c'],abs(t_log_kwhaf[`c']))
	if `pval_kwhaf_`c''<0.01 {
		local stars_kwhaf_`c' = "$^{***}$"
	}
	else if `pval_kwhaf_`c''<0.05 {
		local stars_kwhaf_`c' = "$^{**}$"
	}
	else if `pval_kwhaf_`c''<0.1 {
		local stars_kwhaf_`c' = "$^{*}$"
	}
	else {
		local stars_kwhaf_`c' = ""
	}
	
	local beta_af_`c' = string(beta_log_p_af[`c'],"%9.2f")
	local se_af_`c' = string(se_log_p_af[`c'],"%9.2f")
	if "`se_af_`c''"!="" {
		local se_af_`c' = "(`se_af_`c'')" 
	}
	else {
		local beta_af_`c' = "" 
		local se_af_`c' = "" 
	}
	local pval_af_`c' = 2*ttail(dof[`c'],abs(t_log_p_af[`c']))
	if `pval_af_`c''<0.01 {
		local stars_af_`c' = "$^{***}$"
	}
	else if `pval_af_`c''<0.05 {
		local stars_af_`c' = "$^{**}$"
	}
	else if `pval_af_`c''<0.1 {
		local stars_af_`c' = "$^{*}$"
	}
	else {
		local stars_af_`c' = ""
	}
	
	local n_sp_`c' = string(n_SPs[`c'],"%9.0fc")
	local n_obs_`c' = string(n_obs[`c'],"%9.0fc")
}


	// Build table
file open textab using "$dirpath_output/table_regs_monthly_stayers_water.tex", write text replace

file write textab "\begin{table}\centering" _n
file write textab "\caption{\normalsize Monthly data --- \`\`{\bf Stayers}''}" _n
file write textab "\footnotesize" _n
file write textab "\vspace{-2mm}" _n
file write textab "\begin{tabular}{lccccccc}" _n
file write textab "\hline" _n
file write textab "\\ " _n
file write textab "\vspace{-7mm}" _n
file write textab "\\" _n
file write textab "& ~~~~(1)~~~~ & ~~~~(2)~~~~ & ~~~~(3)~~~~ & ~~~~(4)~~~~  \\" _n
file write textab "[.1em]" _n
file write textab "\cline{2-5}" _n
file write textab "\\" _n
file write textab "\vspace{-7mm}" _n
file write textab "\\" _n
file write textab "\small \$\log(P_{\text{water}})\$ & $`beta_af_1'$`stars_af_1'  & $`beta_af_2'$`stars_af_2' & $`beta_af_3'$`stars_af_3' & $`beta_af_4'$`stars_af_4'\\" _n
file write textab "& $`se_af_1'$ & $`se_af_2'$ & $`se_af_3'$ & $`se_af_4'$\\" _n
file write textab "[1em]" _n
file write textab "\small \$\log(P_{\text{elec}})\$ & $`beta_kwh_1'$`stars_kwh_1'  & $`beta_kwh_2'$`stars_kwh_2' & $`beta_kwh_3'$`stars_kwh_3' & $`beta_kwh_4'$`stars_kwh_4'\\" _n
file write textab "& $`se_kwh_1'$ & $`se_kwh_2'$ & $`se_kwh_3'$ & $`se_kwh_4'$\\" _n
file write textab "[1em]" _n
file write textab "\small \$\log(kWh/AF)\$ & $`beta_kwhaf_1'$`stars_kwhaf_1'  & $`beta_kwhaf_2'$`stars_kwhaf_2' & $`beta_kwhaf_3'$`stars_kwhaf_3' & $`beta_kwhaf_4'$`stars_kwhaf_4'\\" _n
file write textab "& $`se_kwhaf_1'$ & $`se_kwhaf_2'$ & $`se_kwhaf_3'$ & $`se_kwhaf_4'$\\" _n
file write textab "[1em]" _n
file write textab "SP \$\times\$ month FEs  & Yes & Yes   & Yes & Yes \\" _n
file write textab "[.25em]" _n
file write textab "Month-of-sample FEs  &  Yes & Yes  & Yes  & Yes\\" _n
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
