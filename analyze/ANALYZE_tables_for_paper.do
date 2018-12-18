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


** 1. Electricity summary stats
{
use "$dirpath_data/merged/sp_month_elec_panel.dta" , clear

count 
local A01 = string(r(N),"%9.0fc")

count if merge_sp_water_panel==3 & flag_weird_cust==0
local A02 = string(r(N),"%9.0fc")


unique sp_uuid
local denom1 = r(unique)
local A1 = string(r(unique),"%9.0fc")

unique sp_uuid if merge_sp_water_panel==3 & flag_weird_cust==0
local denom2 = r(unique)
local A2 = string(r(unique),"%9.0fc")


egen temp_rtcat_min = min(rt_category), by(sp_uuid)
egen temp_rtcat_max = max(rt_category), by(sp_uuid)

unique sp_uuid if temp_rtcat_min<temp_rtcat_max
local B1 = string(r(unique),"%9.0fc")

unique sp_uuid if temp_rtcat_min<temp_rtcat_max & merge_sp_water_panel==3 & flag_weird_cust==0
local B2 = string(r(unique),"%9.0fc")


egen temp_rtcat_cap_min = min(rt_large_ag), by(sp_uuid)
egen temp_rtcat_cap_max = max(rt_large_ag), by(sp_uuid)

unique sp_uuid if temp_rtcat_cap_min<temp_rtcat_cap_max
local C1 = string(r(unique),"%9.0fc")

unique sp_uuid if temp_rtcat_cap_min<temp_rtcat_cap_max & merge_sp_water_panel==3 & flag_weird_cust==0
local C2 = string(r(unique),"%9.0fc")


egen temp_rtcat_met_min = min(rt_category>=3), by(sp_uuid)
egen temp_rtcat_met_max = max(rt_category>=3), by(sp_uuid)

unique sp_uuid if temp_rtcat_met_min<temp_rtcat_met_max
local D1 = string(r(unique),"%9.0fc")

unique sp_uuid if temp_rtcat_met_min<temp_rtcat_met_max & merge_sp_water_panel==3 & flag_weird_cust==0
local D2 = string(r(unique),"%9.0fc")


/*
unique sp_uuid if in_interval==1
local B1 = string(r(unique)/`denom1',"%9.1f")

unique sp_uuid if in_interval==1 & pull=="20180719"
local B2 = string(r(unique)/`denom2',"%9.1f")
*/

count 
local denom1 = r(N)

count if merge_sp_water_panel==3 & flag_weird_cust==0
local denom2 = r(N)

count if !inlist(rt_sched_cd,"AG-1A","AG-1B")
local E1 = string(r(N)/`denom1',"%9.3f")

count if !inlist(rt_sched_cd,"AG-1A","AG-1B") & merge_sp_water_panel==3 & flag_weird_cust==0
local E2 = string(r(N)/`denom2',"%9.3f")


count if inlist(rt_sched_cd,"AG-4A","AG-4D","AG-4C","AG-4F","AG-5C","AG-5F")
local F1 = string(r(N)/`denom1',"%9.3f")

count if inlist(rt_sched_cd,"AG-4A","AG-4D","AG-4C","AG-4F","AG-5C","AG-5F") & merge_sp_water_panel==3 & flag_weird_cust==0
local F2 = string(r(N)/`denom2',"%9.3f")

/*
unique sp_uuid if inlist(rt_sched_cd,"AG-RA","AG-RB","AG-RD","AG-RE","AG-VA","AG-VB","AG-VD","AG-VE")
local E1 = string(r(unique)/`denom1',"%9.1f")

unique sp_uuid if inlist(rt_sched_cd,"AG-RA","AG-RB","AG-RD","AG-RE","AG-VA","AG-VB","AG-VD","AG-VE") & pull=="20180719"
local E2 = string(r(unique)/`denom2',"%9.1f")
*/

sum mnth_bill_kwh
local G1 = string(r(mean),"%9.1f")
local G1_sd = string(r(sd),"%9.1f")

sum mnth_bill_kwh if merge_sp_water_panel==3 & flag_weird_cust==0
local G2 = string(r(mean),"%9.1f")
local G2_sd = string(r(sd),"%9.1f")


sum mnth_bill_kwh if inlist(month,5,6,7,8,9,10)
local H1 = string(r(mean),"%9.1f")
local H1_sd = string(r(sd),"%9.1f")

sum mnth_bill_kwh if inlist(month,5,6,7,8,9,10) & merge_sp_water_panel==3 & flag_weird_cust==0
local H2 = string(r(mean),"%9.1f")
local H2_sd = string(r(sd),"%9.1f")


sum mnth_bill_kwh if !inlist(month,5,6,7,8,9,10)
local I1 = string(r(mean),"%9.1f")
local I1_sd = string(r(sd),"%9.1f")

sum mnth_bill_kwh if !inlist(month,5,6,7,8,9,10) & merge_sp_water_panel==3 & flag_weird_cust==0
local I2 = string(r(mean),"%9.1f")
local I2_sd = string(r(sd),"%9.1f")


sum mean_p_kwh 
local J1 = string(r(mean),"%9.3f")
local J1_sd = string(r(sd),"%9.3f")

sum mean_p_kwh if merge_sp_water_panel==3 & flag_weird_cust==0
local J2 = string(r(mean),"%9.3f")
local J2_sd = string(r(sd),"%9.3f")


sum mean_p_kwh if inlist(month,5,6,7,8,9,10)
local K1 = string(r(mean),"%9.3f")
local K1_sd = string(r(sd),"%9.3f")

sum mean_p_kwh if inlist(month,5,6,7,8,9,10) & merge_sp_water_panel==3 & flag_weird_cust==0
local K2 = string(r(mean),"%9.3f")
local K2_sd = string(r(sd),"%9.3f")


sum mean_p_kwh if !inlist(month,5,6,7,8,9,10)
local L1 = string(r(mean),"%9.3f")
local L1_sd = string(r(sd),"%9.3f")

sum mean_p_kwh if !inlist(month,5,6,7,8,9,10) & merge_sp_water_panel==3 & flag_weird_cust==0
local L2 = string(r(mean),"%9.3f")
local L2_sd = string(r(sd),"%9.3f")


egen temp_min_weird = min(flag_weird_cust), by(sp_uuid)
egen temp_max_weird = max(flag_weird_cust), by(sp_uuid)
egen tag = tag(sp_uuid) 
tab temp_*_weird if tag & merge_sp_water_panel==3
keep sp_uuid merge_sp_water_panel temp_max_weird
duplicates drop

merge 1:m sp_uuid using "$dirpath_data/merged/sa_bill_elec_panel.dta", keep(3) nogen ///
	keepusing(sa_uuid bill_start_dt bill_end_dt total_bill_amount)
gen date_mid = round((bill_end_dt + bill_start_dt)/2)
format %td date_mid
gen month_mid = month(date_mid)
assert month_mid!=. if bill_start_dt!=.
gen summer = inlist(month_mid,5,6,7,8,9,10)

sum total_bill_amount if total_bill_amount>=0
local M1 = string(r(mean),"%9.2f")
local M1_sd = string(r(sd),"%9.2f")

sum total_bill_amount if total_bill_amount>=0 & merge_sp_water_panel==3 & temp_max_weird==0
local M2 = string(r(mean),"%9.2f")
local M2_sd = string(r(sd),"%9.2f")

sum total_bill_amount if total_bill_amount>=0 & summer==1
local N1 = string(r(mean),"%9.2f")
local N1_sd = string(r(sd),"%9.2f")

sum total_bill_amount if total_bill_amount>=0 & summer==1 & merge_sp_water_panel==3 & temp_max_weird==0
local N2 = string(r(mean),"%9.2f")
local N2_sd = string(r(sd),"%9.2f")

sum total_bill_amount if total_bill_amount>=0 & summer==0
local O1 = string(r(mean),"%9.2f")
local O1_sd = string(r(sd),"%9.2f")

sum total_bill_amount if total_bill_amount>=0 & summer==0 & merge_sp_water_panel==3 & temp_max_weird==0
local O2 = string(r(mean),"%9.2f")
local O2_sd = string(r(sd),"%9.2f")

	
	
	// Build table
file open textab using "$dirpath_output/table_elec_summary_stats.tex", write text replace

file write textab "\begin{table}\centering" _n
file write textab "\caption{\normalsize Summary Statistics -- Electricity Data}" _n
file write textab "\label{tab:elec_summary_stats}" _n
file write textab "\begin{tabular}{lrcrcrr}" _n
file write textab "\hline" _n
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
file write textab "Service point-month observations && `A01' && `A02' \\ " _n
file write textab "[.2em]" _n
file write textab "Unique service points (SPs) && `A1' && `A2' \\ " _n
file write textab "[.2em]" _n
*file write textab "\% with hourly usage data  && `B1'  &&  `B2'  \\" _n
*file write textab "[.2em]" _n
file write textab "SPs that switch tariff categories  && `B1' && `B2'   \\" _n
file write textab "[.2em]" _n
file write textab "SPs that switch categories (pumping capital)  && `C1' && `C2'   \\" _n
file write textab "[.2em]" _n
file write textab "SPs that switch categories (smart meters)  && `D1' && `D2'   \\" _n
file write textab "[.2em]" _n
file write textab "Share of SP-months on time-varying tariffs  && `E1' && `E2'   \\" _n
file write textab "[.2em]" _n
file write textab "Share of SP-months on peak-day tariffs  && `F1' && `F2'   \\" _n
file write textab "[1.4em]" _n
*file write textab "\% on design-your-own tariffs  && `E1' && `E2'   \\" _n
*file write textab "[.3em]" _n
file write textab "Monthly electricity consumption (kWh) && `G1' && `G2'   \\" _n
file write textab " && (`G1_sd') && (`G2_sd')   \\" _n
file write textab "[.4em]" _n
file write textab "Monthly electricity consumption (kWh), summer && `H1' && `H2'   \\" _n
file write textab " && (`H1_sd') && (`H2_sd')   \\" _n
file write textab "[.4em]" _n
file write textab "Monthly electricity consumption (kWh), winter && `I1' && `I2'   \\" _n
file write textab " && (`I1_sd') && (`I2_sd')   \\" _n
file write textab "[1.4em]" _n
file write textab "Average marginal electricity price (\\$/kWh) && `J1' && `J2'   \\" _n
file write textab " && (`J1_sd') && (`J2_sd')   \\" _n
file write textab "[.4em]" _n
file write textab "Average marginal electricity price (\\$/kWh), summer && `K1' && `K2'   \\" _n
file write textab " && (`K1_sd') && (`K2_sd')   \\" _n
file write textab "[.4em]" _n
file write textab "Average marginal electricity price (\\$/kWh), winter && `L1' && `L2'   \\" _n
file write textab " && (`L1_sd') && (`L2_sd')   \\" _n
file write textab "[1.4em]" _n
file write textab "Average monthly bill (\\$, non-zero bills) && `M1' && `M2'   \\" _n
file write textab " && (`M1_sd') && (`M2_sd')   \\" _n
file write textab "[.4em]" _n
file write textab "Average monthly bill (\\$, non-zero bills), summer && `N1' && `N2'   \\" _n
file write textab " && (`N1_sd') && (`N2_sd')   \\" _n
file write textab "[.4em]" _n
file write textab "Average monthly bill (\\$, non-zero bills), winter && `O1' && `O2'   \\" _n
file write textab " && (`O1_sd') && (`O2_sd')   \\" _n
file write textab "[.2em]" _n
file write textab "\hline" _n
file write textab "\end{tabular}" _n
file write textab "\captionsetup{width=\textwidth}" _n
file write textab "\caption*{\scriptsize \emph{Notes:} The left column reports summary statistics " _n
file write textab "for the universe of agricultural electricity customers in PGE service territory, from 2008--2017." _n
file write textab "The right column includes the subset of agricultural customers that we successfully match to a " _n
file write textab "groundwater pump in the APEP pump test dataset---i.e., our main estimation sample." _n
file write textab "\`\`Pumping capital'' denotes tariff category switches driven by shifts between small pumps (\$<35\$ hp) and " _n
file write textab "large pumps (\$\ge 35\$ hp), or adding/removing an auxiliary internal combustion engine." _n
file write textab "Most tariff category switches were driven by PGE's smart meter rollout." _n
file write textab "Time-varying tariffs (i.e.\ all except 1A and 1B) have higher marginal prices during peak demand hours. " _n
file write textab "Peak-day tariffs (i.e.\ 4A, 4D, 4C, 4F, 5C, 5F) have very high marginal prices during peak" _n
file write textab "hours on the 14 highest-demand summer days." _n
file write textab "Monthly bills include both volumetric (\\$/kWh) and fixed charges (\\$/kW, \\$/hp, and \\$/day)." _n
file write textab "Summer months are May--October." _n
file write textab "Standard deviations of sample means in parentheses." _n
file write textab "}" _n
file write textab "\end{table}" _n

file close textab
}

************************************************
************************************************

** 2. Water summary stats
{
use "$dirpath_data/merged/sp_month_water_panel.dta" , clear
merge 1:1 sp_uuid modate using "$dirpath_data/merged/sp_month_kwhaf_panel.dta", keep(3) ///
	keepusing(ope) nogen
	
count if flag_weird_cust==0
local A01 = string(r(N),"%9.0fc")
	
unique sp_uuid if flag_weird_cust==0
local denom2 = r(unique)
local A1 = string(r(unique),"%9.0fc")
	
	
sum af_rast_dd_mth_2SP if flag_weird_cust==0
local B1 = string(r(mean),"%9.1f")
local B1_sd = string(r(sd),"%9.1f")

sum af_rast_dd_mth_2SP if inlist(month,5,6,7,8,9,10) & flag_weird_cust==0
local C1 = string(r(mean),"%9.1f")
local C1_sd = string(r(sd),"%9.1f")

sum af_rast_dd_mth_2SP if !inlist(month,5,6,7,8,9,10) & flag_weird_cust==0
local D1 = string(r(mean),"%9.1f")
local D1_sd = string(r(sd),"%9.1f")


sum mean_p_af_rast_dd_mth_2SP if flag_weird_cust==0
local E1 = string(r(mean),"%9.2f")
local E1_sd = string(r(sd),"%9.2f")

sum mean_p_af_rast_dd_mth_2SP if inlist(month,5,6,7,8,9,10) & flag_weird_cust==0
local F1 = string(r(mean),"%9.2f")
local F1_sd = string(r(sd),"%9.2f")

sum mean_p_af_rast_dd_mth_2SP if !inlist(month,5,6,7,8,9,10) & flag_weird_cust==0
local G1 = string(r(mean),"%9.2f")
local G1_sd = string(r(sd),"%9.2f")


sum ope if flag_weird_cust==0 & flag_weird_pump==0, detail
local H1 = string(r(mean),"%9.2f")
local H1_sd = string(r(sd),"%9.2f")

sum kwhaf_apep_measured if flag_weird_cust==0 & flag_weird_pump==0 , detail
local I1 = string(r(mean),"%9.2f")
local I1_sd = string(r(sd),"%9.2f")

sum kwhaf_rast_dd_mth_2SP if flag_weird_cust==0 & flag_weird_pump==0 & summer, detail
local J1 = string(r(mean),"%9.2f")
local J1_sd = string(r(sd),"%9.2f")
	
keep if flag_weird_cust==0
keep sp_uuid
duplicates drop	
merge 1:m sp_uuid using "$dirpath_data/merged/sp_apep_proj_merged.dta", keepusing(sp_uuid)
tab _merge
unique sp_uuid if _merge==3
assert r(unique)==real(subinstr("`A1'",",","",.))
keep if _merge==3
duplicates t sp_uuid, gen(dup)
replace dup = dup+1
sum dup, detail
local K1 = string(r(mean),"%9.2f")
local K1_sd = string(r(sd),"%9.2f")

	
	// Build table
file open textab using "$dirpath_output/table_water_summary_stats.tex", write text replace

file write textab "\begin{table}\centering" _n
file write textab "\caption{\normalsize Summary Statistics -- Pump Tests and Groundwater Consumption}" _n
file write textab "\label{tab:water_summary_stats}" _n
file write textab "\begin{tabular}{lrcrcrr}" _n
file write textab "\hline" _n
file write textab "\hline" _n
file write textab "\\ " _n
file write textab "\vspace{-8mm}" _n
file write textab "\\" _n
file write textab "&& $\begin{matrix}\text{Matched to Pumps}\end{matrix}$ \\" _n
file write textab "[.1em]" _n
file write textab "\cline{2-3}" _n
file write textab "\\" _n
file write textab "\vspace{-7mm}" _n
file write textab "\\" _n
file write textab "Service point-month observations && `A01' \\ " _n
file write textab "[.2em]" _n
file write textab "Unique service points (SPs) && `A1' \\ " _n
file write textab "[1.4em]" _n
file write textab "Matched APEP points per SP && `K1' \\ " _n
file write textab " && (`K1_sd') \\" _n
file write textab "[.4em]" _n
file write textab "Operating pump efficiency (\%) && `H1'   \\" _n
file write textab " && (`H1_sd')   \\" _n
file write textab "[.4em]" _n
file write textab "kWh per AF conversion factor (APEP measured) && `I1'   \\" _n
file write textab " && (`I1_sd')   \\" _n
file write textab "[.4em]" _n
file write textab "kWh per AF conversion factor (constructed) && `J1'   \\" _n
file write textab " && (`J1_sd')   \\" _n
file write textab "[1.4em]" _n
file write textab "Monthly groundwater consumption (AF) && `B1'   \\" _n
file write textab " && (`B1_sd')   \\" _n
file write textab "[.4em]" _n
file write textab "Monthly groundwater consumption (AF), summer && `C1'   \\" _n
file write textab " && (`C1_sd')   \\" _n
file write textab "[.4em]" _n
file write textab "Monthly groundwater consumption (AF), winter && `D1'   \\" _n
file write textab " && (`D1_sd')   \\" _n
file write textab "[1.4em]" _n
file write textab "Average marginal groundwater price (\\$/AF) && `E1'   \\" _n
file write textab " && (`E1_sd')   \\" _n
file write textab "[.4em]" _n
file write textab "Average marginal groundwater price (\\$/AF), summer && `F1'   \\" _n
file write textab " && (`F1_sd')   \\" _n
file write textab "[.4em]" _n
file write textab "Average marginal groundwater price (\\$/AF), winter && `G1'   \\" _n
file write textab " && (`G1_sd')   \\" _n
file write textab "[.2em]" _n
file write textab "\hline" _n
file write textab "\vspace{-2mm}" _n
file write textab "\end{tabular}" _n
file write textab "\captionsetup{width=.93\textwidth}" _n
file write textab "\caption*{\scriptsize \emph{Notes:} These summary stats are from the merged panel of " _n
file write textab "groundwater prices and quantities, which combines electricity data, pump test data, and groundwater data." _n
file write textab "We observe 3.45 unique APEP pump tests for the average matched service point, although 37 percent of " _n
file write textab "service points match to only a single APEP test." _n
file write textab "Our constructed kWh per AF conversion factor (i.e. \$\widehat{\text{kWh}\big/\text{AF}}_{it}\$) uses monthly " _n
file write textab "groundwater rasters to capture changes in (measured) kWh per AF over time, and estimation error " _n
file write textab "compresses the right tail of distribution of measured kWh per AF. " _n
file write textab "Monthly groundwater consumption divides electricity consumption (kWh) by \$\widehat{\text{kWh}\big/\text{AF}}_{it}\$. " _n
file write textab "Grounwater prices multiply marignal electricity prices (\\$/kWh) by \$\widehat{\text{kWh}\big/\text{AF}}_{it}\$. " _n
file write textab "Summer months are May--October." _n
file write textab "Standard deviations of sample means in parentheses." _n
file write textab "}" _n
file write textab "\end{table}" _n

file close textab
}

************************************************
************************************************

** 3. Regression results: Electricity 
{
use "$dirpath_data/results/regs_Qelec_Pelec.dta" , clear

keep if pull=="20180719"
keep if panel=="monthly"
keep if sample=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3"
keep if inlist(rhs,"log_p_mean","(log_p_mean = log_mean_p_kwh_ag_default)","(log_p_mean = log_p_mean_deflag*)")
keep if inlist(fes,"sp_group#month modate", ///
					"sp_group#month sp_group#rt_large_ag modate", ///
					"sp_group#month sp_group#rt_large_ag basin_group#year wdist_group#year modate", ///
					"sp_group#rt_large_ag sp_group#month basin_group#year wdist_group#year modate sp_group#c.modate")
drop if rhs=="log_p_mean" & fes!="sp_group#month modate"										
drop if rhs=="(log_p_mean = log_p_mean_deflag*)" & fes!="sp_group#month sp_group#rt_large_ag basin_group#year wdist_group#year modate"
assert _N==6

gen col = .			
replace col = 1 if fes=="sp_group#month modate" & rhs=="log_p_mean"
replace col = 2 if fes=="sp_group#month modate" & rhs=="(log_p_mean = log_mean_p_kwh_ag_default)"
replace col = 3 if fes=="sp_group#month sp_group#rt_large_ag modate" & rhs=="(log_p_mean = log_mean_p_kwh_ag_default)"
replace col = 4 if fes=="sp_group#month sp_group#rt_large_ag basin_group#year wdist_group#year modate" & rhs=="(log_p_mean = log_mean_p_kwh_ag_default)"
replace col = 5 if fes=="sp_group#month sp_group#rt_large_ag basin_group#year wdist_group#year modate" & rhs!="(log_p_mean = log_mean_p_kwh_ag_default)"
replace col = 6 if fes=="sp_group#rt_large_ag sp_group#month basin_group#year wdist_group#year modate sp_group#c.modate" & rhs=="(log_p_mean = log_mean_p_kwh_ag_default)"

assert col!=.
sort col
order col

forvalues c = 1/6 {
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
file open textab using "$dirpath_output/table_elec_regs_main.tex", write text replace

file write textab "\begin{table}[t!]\centering" _n
file write textab "\small" _n
file write textab "\caption{Estimated Demand Elasticities -- Electricity  \label{tab:elec_regs_main}}" _n
file write textab "\vspace{-0.1cm}" _n
file write textab "\small" _n
file write textab "\begin{adjustbox}{center} " _n
file write textab "\begin{tabular}{lcccccccc} " _n
file write textab "\hline \hline" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab " & (1)  & (2)  & (3)  & (4)  & (5)  & (6) \\ " _n
file write textab "[0.1em]" _n
file write textab " & OLS & IV & IV & IV & IV & IV \\" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab "\cline{2-7}" _n
file write textab "\vspace{-0.27cm}" _n
file write textab "\\" _n
file write textab " $\log\big(P^{\text{elec}}_{it}\big)$ ~ & $`beta_1'$`stars_1'  & $`beta_2'$`stars_2' & $`beta_3'$`stars_3' & $`beta_4'$`stars_4' & $`beta_5'$`stars_5'  & $`beta_6'$`stars_6' \\ " _n
file write textab "& $(`se_1')$ & $(`se_2')$ & $(`se_3')$ & $(`se_4')$ & $(`se_5')$ & $(`se_6')$ \\" _n
file write textab "[1.5em] " _n
file write textab "Instrument(s): \\" _n
file write textab "[0.1em] " _n
file write textab "~~ Default $\log\big(P^{\text{elec}}_{it}\big)$  & & Yes & Yes  & Yes  & & Yes \\" _n
file write textab "[0.1em] " _n
file write textab "~~ Default $\log\big(P^{\text{elec}}_{it}\big)$, lagged  & & & & & Yes & \\" _n
file write textab "[1.5em] " _n
file write textab "Fixed effects: \\" _n
file write textab "[0.1em] " _n
file write textab "~~Unit $\times$ month-of-year  & Yes  & Yes  & Yes  & Yes  & Yes  & Yes   \\ " _n
file write textab "[0.1em] " _n
file write textab "~~Month-of-sample  & Yes  & Yes  & Yes  & Yes  & Yes  & Yes   \\ " _n
file write textab "[0.1em] " _n
file write textab "~~Unit $\times$ physical capital & & & Yes & Yes & Yes & Yes  \\" _n
file write textab "[0.1em] " _n
file write textab "~~Water basin $\times$ year & & &  & Yes & Yes & Yes  \\" _n
file write textab "[0.1em] " _n
file write textab "~~Water district $\times$ year & & &  & Yes & Yes & Yes \\" _n
file write textab "[0.1em] " _n
*file write textab "~~Unit-specific slopes in depth & & &  &  & Yes & Yes & Yes \\" _n
*file write textab "[0.1em] " _n
file write textab "~~Unit-specific linear time trends & & & & & &  Yes \\" _n
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
file write textab "\caption*{\scriptsize \emph{Notes:} Each regression estimates Equation (\ref{eq:reg_elec}) at the service point by month level," _n
file write textab "where the dependent variable is the inverse hyperbolic sine transformation of electricity consumed by service point \$i\$ in month \$t\$." _n
file write textab "We estimate IV specifications via two-stage least squares, instrumenting with either unit \$i\$'s within-category default " _n
file write textab "logged electricity price in month \$t\$ \emph{or} the 6- and 12- month lags of this variable." _n
file write textab "\`\`Physical capital'' is a categorical variable for (i) small pumps, (ii) large pumps, and (iii) internal combustion engines, and unit \$\times\$" _n
file write textab "physical capital fixed effects control for shifts in tariff category triggered by the installation of new pumping equipment." _n
file write textab "Water basin \$\times\$ year fixed effects control for broad geographic trends in groundwater depth." _n
file write textab "Water district \$\times\$ year fixed effects control for annual variation in surface water allocations." _n
*file write textab "Unit-specific slopes in depth control for average groundwater depth at the basin-quarter level." _n
file write textab "All regressions drop solar NEM customers, customers with bad geocodes, and months with irregular electricity bills" _n
file write textab "(e.g.\ first/last bills, bills longer/shorter than 1 month, overlapping bills for a single account)." _n
file write textab "Standard errors (in parentheses) are clustered by service point and by month-of-sample." _n
file write textab "Significance: *** \$p < 0.01\$, ** \$p < 0.05\$, * \$p < 0.10\$." _n
file write textab "}" _n
file write textab "\end{table}" _n

file close textab

}

************************************************
************************************************

** 4. Regression results: Water and Electricity (separate) 
{
use "$dirpath_data/results/regs_Qwater_Pwater.dta" , clear

keep if regexm(sample,"if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0")
keep if regexm(fes,"sp_group#rt_large_ag")
keep if regexm(rhs,"_dd_mth_2SP") | regexm(rhs,"_dd_qtr_2SP")
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

replace col = 4 if fes=="sp_group#month sp_group#rt_large_ag modate" ///
	& rhs=="(log_p_mean ln_kwhaf_rast_dd_qtr_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_qtr_2SP)" ///
	& sample=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"

replace col = 5 if fes=="sp_group#month sp_group#rt_large_ag wdist_group#year basin_group#year modate" ///
	& rhs=="(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default ln_gw_mean_depth_mth_2SP)" ///
	& sample=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"

replace col = 6 if fes=="sp_group#month sp_group#rt_large_ag modate" ///
	& rhs=="(log_p_mean ln_kwhaf_rast_dd_mth_2SP = log_mean_p_kwh_ag_default L6.ln_gw_mean_depth_mth_2SP L12.ln_gw_mean_depth_mth_2SP)" ///
	& sample=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"

	
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
file open textab using "$dirpath_output/table_water_regs_main.tex", write text replace

file write textab "\begin{table}[t!]\centering" _n
file write textab "\small" _n
file write textab "\caption{Estimated Demand Elasticities -- Groundwater  \label{tab:water_regs_main}}" _n
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
*file write textab "[0.1em] " _n
*file write textab "~~Unit-specific slopes in depth & & &  &  & Yes & Yes & Yes \\" _n
*file write textab "[0.1em] " _n
*file write textab "~~Unit-specific linear time trends & & & & & &  Yes \\" _n
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
file write textab "Standard errors (in parentheses) are clustered by service point and by month-of-sample." _n
file write textab "Significance: *** \$p < 0.01\$, ** \$p < 0.05\$, * \$p < 0.10\$." _n
file write textab "}" _n
file write textab "\end{table}" _n

file close textab

}

************************************************
************************************************

** 5. Regression results: Water (combined) 
{
use "$dirpath_data/results/regs_Qwater_Pwater_combined.dta" , clear

keep if regexm(sample,"if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0")
keep if regexm(fes,"sp_group#rt_large_ag")
keep if regexm(rhs,"_dd_mth_2SP") | regexm(rhs,"_dd_qtr_2SP")

gen col = .			

replace col = 1 if fes=="sp_group#month sp_group#rt_large_ag modate" ///
	& rhs=="ln_mean_p_af_rast_dd_mth_2SP" ///
	& sample=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
	
replace col = 2 if fes=="sp_group#month sp_group#rt_large_ag modate" ///
	& rhs=="(ln_mean_p_af_rast_dd_mth_2SP = log_mean_p_kwh_ag_default)" ///
	& sample=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"

replace col = 3 if fes=="sp_group#month sp_group#rt_large_ag modate" ///
	& rhs=="(ln_mean_p_af_rast_dd_mth_2SP = log_mean_p_kwh_ag_default)" ///
	& sample=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & inlist(basin_group,68,121,122)"

replace col = 4 if fes=="sp_group#month sp_group#rt_large_ag modate" ///
	& rhs=="(ln_mean_p_af_rast_dd_qtr_2SP = log_mean_p_kwh_ag_default)" ///
	& sample=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"

replace col = 5 if fes=="sp_group#month sp_group#rt_large_ag wdist_group#year basin_group#year modate" ///
	& rhs=="(ln_mean_p_af_rast_dd_mth_2SP = log_mean_p_kwh_ag_default)" ///
	& sample=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"

replace col = 6 if fes=="sp_group#month sp_group#rt_large_ag modate" ///
	& rhs=="(ln_mean_p_af_rast_dd_mth_2SP = log_p_mean_deflag*)" ///
	& sample=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"

	
drop if col==.
assert _N==6
sort col
order col

forvalues c = 1/6 {

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
	local n_mth_`c' = string(n_modates[`c'],"%9.0fc")
	local n_obs_`c' = string(n_obs[`c']/1e6,"%9.2f") + "M"
	local fstat_`c' = string(fstat_rk[`c'],"%9.0f")
	if "`fstat_`c''"=="." {
		local fstat_`c' = ""
	}
}


	// Build table
file open textab using "$dirpath_output/table_water_regs_combined.tex", write text replace

file write textab "\begin{table}[t!]\centering" _n
file write textab "\small" _n
file write textab "\caption{Estimated Demand Elasticities -- Groundwater  \label{tab:water_regs_combined}}" _n
file write textab "\vspace{-0.1cm}" _n
file write textab "\small" _n
file write textab "\begin{adjustbox}{center} " _n
file write textab "\begin{tabular}{lcccccccc} " _n
file write textab "\hline \hline" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab " & (1)  & (2)  & (3)  & (4)  & (5)  & (6) \\ " _n
file write textab "[0.1em]" _n
file write textab " & OLS & IV & IV & IV & IV & IV \\" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab "\cline{2-7}" _n
file write textab "\vspace{-0.27cm}" _n
file write textab "\\" _n
file write textab " $\log\big(P^{\text{water}}_{it}\big)$ ~ & " _n
file write textab " $`beta_1'$`stars_1'  & $`beta_2'$`stars_2' & $`beta_3'$`stars_3' & $`beta_4'$`stars_4' & $`beta_5'$`stars_5'  & $`beta_6'$`stars_6' \\ " _n
file write textab "& $(`se_1')$ & $(`se_2')$ & $(`se_3')$ & $(`se_4')$ & $(`se_5')$ & $(`se_6')$ \\" _n
file write textab "[1.5em] " _n
file write textab "Instrument(s): \\" _n
file write textab "[0.1em] " _n
file write textab "~~ Default $\log\big(P^{\text{elec}}_{it}\big)$  &  & Yes & Yes  & Yes  & Yes & \\" _n
file write textab "[0.1em] " _n
file write textab "~~ Default $\log\big(P^{\text{elec}}_{it}\big)$, laggeed  & & &  &  & & Yes \\" _n
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
*file write textab "[0.1em] " _n
*file write textab "~~Unit-specific slopes in depth & & &  &  & Yes & Yes & Yes \\" _n
*file write textab "[0.1em] " _n
*file write textab "~~Unit-specific linear time trends & & & & & &  Yes \\" _n
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
file write textab "Each regression estimates Equation (\ref{eq:reg_water_combined}) at the service point by month level," _n
file write textab "where the dependent variable is the inverse hyperbolic sine transformation " _n
file write textab "of groudnwater consumed by service point \$i\$ in month \$t\$." _n
file write textab "We estimate IV specifications via two-stage least squares, and Columns (2)--(5) instrument  " _n
file write textab "for \$P^{\text{water}}_{it}\$ with unit \$i\$'s within-category default logged electricity price. " _n
file write textab "Column (6) instruments with the 6- and 12- month lags of this variable." _n
file write textab "\`\`Physical capital'' is a categorical variable for (i) small pumps, (ii) large pumps, and (iii) " _n
file write textab "internal combustion engines, and unit \$\times\$ physical capital fixed effects control for shifts " _n
file write textab "in tariff category triggered by the installation of new pumping equipment." _n
file write textab "Water basin \$\times\$ year fixed effects control for broad geographic trends in groundwater depth." _n
file write textab "Water district \$\times\$ year fixed effects control for annual variation in surface water allocations." _n
file write textab "Column (3) restricts the sample to only the three most common water basins (San Joaquin Valley, " _n
file write textab "Sacramento Valley, and Salinas Valley), each of which contains over 1000 unique SPs in our estimation sample." _n
file write textab "Column (4) uses a quarterly panel of groundwater depths to construct both \$Q^{\text{water}}_{it}\$ and \$P^{\text{water}}_{it}\$, " _n
file write textab "rather than a monthly panel." _n
file write textab "All regressions drop solar NEM customers, customers with bad geocodes, months with irregular electricity bills " _n
file write textab "(e.g.\ first/last bills, bills longer/shorter than 1 month, overlapping bills for a single account)," _n
file write textab "and pumps with implausible test measurements." _n
file write textab "Standard errors (in parentheses) are clustered by service point and by month-of-sample." _n
file write textab "Significance: *** \$p < 0.01\$, ** \$p < 0.05\$, * \$p < 0.10\$." _n
file write textab "}" _n
file write textab "\end{table}" _n

file close textab

}

************************************************
************************************************

START HERE


** VERSION FOR APPENDIX? 2. PGE rates (examples)
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
