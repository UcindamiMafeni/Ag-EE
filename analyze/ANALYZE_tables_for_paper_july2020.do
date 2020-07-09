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


** 1. Electricity summary stats
{
use "$dirpath_data/merged_pge/sp_month_elec_panel.dta" , clear

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

merge 1:m sp_uuid using "$dirpath_data/merged_pge/sa_bill_elec_panel.dta", keep(3) nogen ///
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
use "$dirpath_data/merged_pge/sp_month_water_panel.dta" , clear
merge 1:1 sp_uuid modate using "$dirpath_data/merged_pge/sp_month_kwhaf_panel.dta", keep(3) ///
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
merge 1:m sp_uuid using "$dirpath_data/merged_pge/sp_apep_proj_merged.dta", keepusing(sp_uuid)
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

** 3. Regression results: Electricity, monthly
{
use "$dirpath_data/results/regs_pge_elec_monthly_july2020.dta" , clear

keep if pull=="PGE 20180719"
keep if panel=="monthly"
keep if ifs_base=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3"
keep if inlist(rhs,"log_p_mean","(log_p_mean = log_mean_p_kwh_ag_default)","(log_p_mean = log_p_mean_deflag*)")
keep if inlist(fes,"sp_group#month modate", ///
					"sp_group#month sp_group#rt_large_ag modate", ///
					"sp_group#month sp_group#rt_large_ag basin_group#year wdist_group#year modate", ///
					"sp_group#month sp_group#rt_large_ag modate sp_group#c.modate")
drop if rhs=="log_p_mean" & fes!="sp_group#month modate"										
drop if rhs=="(log_p_mean = log_p_mean_deflag*)" & fes!="sp_group#month sp_group#rt_large_ag modate"
drop if depvar!="ihs_kwh"
drop if ifs_sample!=""
assert _N==6

gen col = .			
replace col = 1 if fes=="sp_group#month modate" & rhs=="log_p_mean"
replace col = 2 if fes=="sp_group#month modate" & rhs=="(log_p_mean = log_mean_p_kwh_ag_default)"
replace col = 3 if fes=="sp_group#month sp_group#rt_large_ag modate" & rhs=="(log_p_mean = log_mean_p_kwh_ag_default)"
replace col = 4 if fes=="sp_group#month sp_group#rt_large_ag basin_group#year wdist_group#year modate" & rhs=="(log_p_mean = log_mean_p_kwh_ag_default)"
replace col = 5 if fes=="sp_group#month sp_group#rt_large_ag modate" & rhs!="(log_p_mean = log_mean_p_kwh_ag_default)"
replace col = 6 if fes=="sp_group#month sp_group#rt_large_ag modate sp_group#c.modate" & rhs=="(log_p_mean = log_mean_p_kwh_ag_default)"

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
file write textab "~~Water basin $\times$ year & & &  & Yes & &  \\" _n
file write textab "[0.1em] " _n
file write textab "~~Water district $\times$ year & & &  & Yes & & \\" _n
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
file write textab "Water district \$\times\$ year fixed effects control for annual variation in surface water allocations; " _n
file write textab "we include a common \`\`no-water-district'' dummy for units not assigned to a water district, to avoid dropping them from the regression. " _n
file write textab "All regressions drop solar NEM customers, customers with bad geocodes, and months with irregular electricity bills" _n
file write textab "(e.g.\ first/last bills, bills longer/shorter than 1 month, overlapping bills for a single account)." _n
file write textab "Standard errors (in parentheses) are two-way clustered by service point and by month-of-sample." _n
file write textab "Significance: *** \$p < 0.01\$, ** \$p < 0.05\$, * \$p < 0.10\$." _n
file write textab "}" _n
file write textab "\end{table}" _n

file close textab

}

************************************************
************************************************

** 4. Regression results: Water (combined), monthly 
{
use "$dirpath_data/results/regs_pge_water_combined_monthly_july2020.dta" , clear

keep if pull=="PGE 20180719"
keep if panel=="monthly"
keep if ifs_base=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0"
gen col = .			

replace col = 1 if depvar=="ihs_af_rast_dd_mth_2SP" ///
	& fes=="sp_group#month sp_group#rt_large_ag modate" ///
	& rhs=="ln_mean_p_af_rast_dd_mth_2SP" ///
	& ifs_sample==""
	
replace col = 2 if depvar=="ihs_af_rast_dd_mth_2SP" ///
	& fes=="sp_group#month sp_group#rt_large_ag modate" ///
	& rhs=="(ln_mean_p_af_rast_dd_mth_2SP = log_mean_p_kwh_ag_default)" ///
	& ifs_sample==""

replace col = 3 if depvar=="ihs_af_rast_dd_mth_2SP" ///
	& fes=="sp_group#month sp_group#rt_large_ag modate" ///
	& rhs=="(ln_mean_p_af_rast_dd_mth_2SP = log_mean_p_kwh_ag_default)" ///
	& ifs_sample==" & inlist(basin_group,68,121,122)"

replace col = 4 if depvar=="ihs_af_rast_dd_qtr_2SP" ///
	& fes=="sp_group#month sp_group#rt_large_ag modate" ///
	& rhs=="(ln_mean_p_af_rast_dd_qtr_2SP = log_mean_p_kwh_ag_default)" ///
	& ifs_sample==""
	
replace col = 5 if depvar=="ihs_af_rast_dd_mth_2SP" ///
	& fes=="sp_group#month sp_group#rt_large_ag wdist_group#year basin_group#year modate" ///
	& rhs=="(ln_mean_p_af_rast_dd_mth_2SP = log_mean_p_kwh_ag_default)" ///
	& ifs_sample==""

replace col = 6 if depvar=="ihs_af_rast_dd_mth_2SP" ///
	& fes=="sp_group#month sp_group#rt_large_ag modate" ///
	& rhs=="(ln_mean_p_af_rast_dd_mth_2SP = log_p_mean_deflag*)" ///
	& ifs_sample==""
	
drop if col==.
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
file write textab "~~ Default $\log\big(P^{\text{elec}}_{it}\big)$, lagged  & & &  &  & & Yes \\" _n
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
file write textab "Water district \$\times\$ year fixed effects control for annual variation in surface water allocations; " _n
file write textab "we include a common \`\`no-water-district'' dummy for units not assigned to a water district, to avoid dropping them from the regression. " _n
file write textab "Column (3) restricts the sample to only the three most common water basins (San Joaquin Valley, " _n
file write textab "Sacramento Valley, and Salinas Valley), each of which contains over 1000 unique SPs in our estimation sample." _n
file write textab "Column (4) uses a quarterly panel of groundwater depths to construct both \$Q^{\text{water}}_{it}\$ and \$P^{\text{water}}_{it}\$, " _n
file write textab "rather than a monthly panel." _n
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



START HERE
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

** 6. Deadweight loss table
{
use "$dirpath_data/results/externality_calcs_june2016_rast_dd_mth_2SP.dta", clear
keep if in_regs==1

unique sp_uuid if q_old>1 & q_old!=. & basin_group==121
local A1 = string(r(unique),"%9.0fc")

unique sp_uuid if q_old>1 & q_old!=. & basin_group==68
local A2 = string(r(unique),"%9.0fc")

unique sp_uuid if q_old>1 & q_old!=. & basin_group==122
local A3 = string(r(unique),"%9.0fc")


sum dcs_i if q_old>1 & q_old!=. & basin_group==121, detail
local B1 = string(r(p25),"%9.2f")
local C1 = string(r(p50),"%9.2f")
local D1 = string(r(p75),"%9.2f")

sum dcs_i if q_old>1 & q_old!=. & basin_group==68, detail
local B2 = string(r(p25),"%9.2f")
local C2 = string(r(p50),"%9.2f")
local D2 = string(r(p75),"%9.2f")

sum dcs_i if q_old>1 & q_old!=. & basin_group==122, detail
local B3 = string(r(p25),"%9.2f")
local C3 = string(r(p50),"%9.2f")
local D3 = string(r(p75),"%9.2f")


foreach i in 10 20 30 {

	sum n_j_pos`i' if q_old>1 & q_old!=. & basin_group==121, detail
	local E1_`i' = string(r(mean), "%9.0fc")

	sum n_j_pos`i'_upr if q_old>1 & q_old!=. & basin_group==121, detail
	local E2_`i' = string(r(mean), "%9.0fc")

	sum n_j_pos`i' if q_old>1 & q_old!=. & basin_group==68, detail
	local E3_`i' = string(r(mean), "%9.0fc")

	sum n_j_pos`i'_upr if q_old>1 & q_old!=. & basin_group==68, detail
	local E4_`i' = string(r(mean), "%9.0fc")

	sum n_j_pos`i' if q_old>1 & q_old!=. & basin_group==122, detail
	local E5_`i' = string(r(mean), "%9.0fc")

	sum n_j_pos`i'_upr if q_old>1 & q_old!=. & basin_group==122, detail
	local E6_`i' = string(r(mean), "%9.0fc")

	sum dW_`i' if q_old>1 & q_old!=. & basin_group==121, detail
	local F1_`i' = string(r(p25),"%9.2f")
	if r(p25)>0 {
		local F1_`i' = "\mathbf{\phantom{-}" + "`F1_`i''" + "}"
	}
	local G1_`i' = string(r(p50),"%9.2f")
	if r(p50)>0 {
		local G1_`i' = "\mathbf{\phantom{-}" + "`G1_`i''" + "}"
	}
	local H1_`i' = string(r(p75),"%9.2f")
	if r(p75)>0 {
		local H1_`i' = "\mathbf{\phantom{-}" + "`H1_`i''" + "}"
	}

	sum dW_`i'_upr if q_old>1 & q_old!=. & basin_group==121, detail
	local F2_`i' = string(r(p25),"%9.2f")
	if r(p25)>0 {
		local F2_`i' = "\mathbf{\phantom{-}" + "`F2_`i''" + "}"
	}
	local G2_`i' = string(r(p50),"%9.2f")
	if r(p50)>0 {
		local G2_`i' = "\mathbf{\phantom{-}" + "`G2_`i''" + "}"
	}
	local H2_`i' = string(r(p75),"%9.2f")
	if r(p75)>0 {
		local H2_`i' = "\mathbf{\phantom{-}" + "`H2_`i''" + "}"
	}
	sum dW_`i' if q_old>1 & q_old!=. & basin_group==68, detail
	local F3_`i' = string(r(p25),"%9.2f")
	if r(p25)>0 {
		local F3_`i' = "\mathbf{\phantom{-}" + "`F3_`i''" + "}"
	}
	local G3_`i' = string(r(p50),"%9.2f")
	if r(p50)>0 {
		local G3_`i' = "\mathbf{\phantom{-}" + "`G3_`i''" + "}"
	}
	local H3_`i' = string(r(p75),"%9.2f")
	if r(p75)>0 {
		local H3_`i' = "\mathbf{\phantom{-}" + "`H3_`i''" + "}"
	}

	sum dW_`i'_upr if q_old>1 & q_old!=. & basin_group==68, detail
	local F4_`i' = string(r(p25),"%9.2f")
	if r(p25)>0 {
		local F4_`i' = "\mathbf{\phantom{-}" + "`F4_`i''" + "}"
	}
	local G4_`i' = string(r(p50),"%9.2f")
	if r(p50)>0 {
		local G4_`i' = "\mathbf{\phantom{-}" + "`G4_`i''" + "}"
	}
	local H4_`i' = string(r(p75),"%9.2f")
	if r(p75)>0 {
		local H4_`i' = "\mathbf{\phantom{-}" + "`H4_`i''" + "}"
	}

	sum dW_`i' if q_old>1 & q_old!=. & basin_group==122, detail
	local F5_`i' = string(r(p25),"%9.2f")
	if r(p25)>0 {
		local F5_`i' = "\mathbf{\phantom{-}" + "`F5_`i''" + "}"
	}
	local G5_`i' = string(r(p50),"%9.2f")
	if r(p50)>0 {
		local G5_`i' = "\mathbf{\phantom{-}" + "`G5_`i''" + "}"
	}
	local H5_`i' = string(r(p75),"%9.2f")
	if r(p75)>0 {
		local H5_`i' = "\mathbf{\phantom{-}" + "`H5_`i''" + "}"
	}

	sum dW_`i'_upr if q_old>1 & q_old!=. & basin_group==122, detail
	local F6_`i' = string(r(p25),"%9.2f")
	if r(p25)>0 {
		local F6_`i' = "\mathbf{\phantom{-}" + "`F6_`i''" + "}"
	}
	local G6_`i' = string(r(p50),"%9.2f")
	if r(p50)>0 {
		local G6_`i' = "\mathbf{\phantom{-}" + "`G6_`i''" + "}"
	}
	local H6_`i' = string(r(p75),"%9.2f")
	if r(p75)>0 {
		local H6_`i' = "\mathbf{\phantom{-}" + "`H6_`i''" + "}"
	}

}
	
	// Build table
file open textab using "$dirpath_output/table_externality_calcs.tex", write text replace

file write textab "\begin{table}\centering" _n
file write textab "\caption{\normalsize Deadweight Loss Calculations for June 2016}" _n
file write textab "\label{tab:externality_calcs}" _n
file write textab "\small" _n
file write textab "\begin{adjustbox}{center} " _n
file write textab "\begin{tabular}{lcccccccccc}" _n
file write textab "\hline" _n
file write textab "\hline" _n
file write textab "\\ " _n
file write textab "\vspace{-8mm}" _n
file write textab "\\" _n
file write textab "& \multicolumn{2}{c}{Sacramento Valley}" _n  
file write textab "&& \multicolumn{2}{c}{Salinas Valley} " _n
file write textab "&&  \multicolumn{2}{c}{San Joaqu\'{i}n Valley} \\" _n
file write textab "[.1em]" _n
file write textab "\cline{2-9}" _n
file write textab "\\" _n
file write textab "\vspace{-9mm}" _n
file write textab "\\" _n
file write textab "APEP units (\$i\$) &  \multicolumn{2}{c}{`A1'} &&  \multicolumn{2}{c}{`A2'} &&  \multicolumn{2}{c}{`A3'} \\ " _n
file write textab "[.5em]" _n
file write textab "\$\Delta CS_i\$ from 1 AF less \\ " _n
file write textab "[.15em]" _n
file write textab "~~~25th percentile   &  \multicolumn{2}{c}{\$`B1'\$} &&  \multicolumn{2}{c}{\$`B2'\$} &&  \multicolumn{2}{c}{\$`B3'\$}\\  " _n
file write textab "[.05em]" _n
file write textab "~~~50th percentile  &  \multicolumn{2}{c}{\$`C1'\$} &&  \multicolumn{2}{c}{\$`C2'\$} &&  \multicolumn{2}{c}{\$`C3'\$} \\ " _n
file write textab "[.05em]" _n
file write textab "~~~75th percentile  &  \multicolumn{2}{c}{\$`D1'\$} &&  \multicolumn{2}{c}{\$`D2'\$} &&  \multicolumn{2}{c}{\$`D3'\$} \\ " _n
foreach i in 10 20 30 {
	file write textab "[1.5em]" _n
	file write textab "\multicolumn{1}{c}{\bf `i'-mile radius~~~~}  & ~APEP~ & ~Scaled~ & &  ~APEP~ & ~Scaled~ & & ~APEP~ & ~Scaled~ \\ " _n
	file write textab "[.2em]" _n
	file write textab "\hline" _n
	file write textab "\\" _n
	file write textab "\vspace{-9mm}" _n
	file write textab "\\" _n
	file write textab "Mean \# of neighbors (\$j\$) & `E1_`i'' & `E2_`i'' && `E3_`i'' & `E4_`i'' && `E5_`i'' & `E6_`i'' \\" _n
	file write textab "[.5em]" _n
	file write textab "\$\Delta CS_i + \sum_j \Delta CS_j\$    \\ " _n
	file write textab "[.15em]" _n
	file write textab "~~~25th percentile  & \$`F1_`i''\$ & \$`F2_`i''\$ && \$`F3_`i''\$ & \$`F4_`i''\$ && $`F5_`i''\$ & \$`F6_`i''\$ \\  " _n
	file write textab "[.05em]" _n
	file write textab "~~~50th percentile  & \$`G1_`i''\$ & \$`G2_`i''\$ && \$`G3_`i''\$ & \$`G4_`i''\$ && $`G5_`i''\$ & \$`G6_`i''\$ \\  " _n
	file write textab "[.05em]" _n
	file write textab "~~~75th percentile  & \$`H1_`i''\$ & \$`H2_`i''\$ && \$`H3_`i''\$ & \$`H4_`i''\$ && $`H5_`i''\$ & \$`H6_`i''\$ \\  " _n
}
file write textab "[0.2em]" _n
file write textab "\hline" _n
file write textab "\end{tabular}" _n
file write textab "\end{adjustbox}" _n
file write textab "\captionsetup{width=\textwidth}" _n
file write textab "\caption*{\scriptsize \emph{Notes:} " _n
file write textab "This table reports calculations for the open-access externality, for service point \$i\$ in our main estimation sample located  " _n
file write textab "in the three largest groundwater basins (Sacramento Valley, Salinas Valley, and San Joaqu\'{i}n Valley). This simple exercise  " _n
file write textab "includes only service points with positive groundwater extraction in June 2016, with counts reported in the top row. For each " _n
file write textab "unit \$i\$, we calculate their private decrease in consumer surplus from pumping 1 AF less in June, 2016 (reported in the top  " _n
file write textab "panel). Next, we translate unit \$i\$'s 1-AF lower extraction in June into the corresponding marginal increase in groundwater  " _n
file write textab "levels in July across \$i\$-centered circles of radius \$r\$. For neighboring units \$j\$ within each circle, this marginal increase  " _n
file write textab "in groundwater levels translates to a marginal decrease in \$j\$'s effective price of groundwater. The bottom three panels report  " _n
file write textab "the net effect on \emph{total} consumer surplus, subtracting \$i\$'s lost consumer surplus in June from increased consumer surplus  " _n
file write textab "in July summed across all units \$j\$. Bolded numbers indicate positive welfare changes, consistent with unit \$i\$ imposing a negative  " _n
file write textab "open-access externality greater than its own private benefit. . \`\`APEP'' columns include only neighbors in our APEP-matched estimation sample, which almost certainly  " _n
file write textab "understates the magnitude of \$ \sum_j \Delta CS_j\$ (by summing over only a subset of nearby agricultural groundwater pumpers).  " _n
file write textab "\`\`Scaled'' columns conservately inflate the number of \$i\$'s neighbors based on the ratio of match-to-unmatched PGE agricultural  " _n
file write textab "service points in each groundwater basin. We calculate all changes in consumer surplus by parameterizing unit-specific groundwater  " _n
file write textab "demand curves, imposing a homogeneous and constant elasticity of \$\epsilon = -1.12\$ (based on our estimate from Table  " _n
file write textab "\ref{tab:water_regs_combined}, Column (2)). All units are in \\$/AF. " _n
file write textab "}" _n
file write textab "\end{table}" _n


file close textab
}

************************************************
************************************************

