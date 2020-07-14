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
file write textab "\caption{\normalsize Summary statistics -- Electricity data}" _n
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
duplicates drop
unique sp_uuid
assert r(unique)==r(N)
sum dup, detail
local K1 = string(r(mean),"%9.2f")
local K1_sd = string(r(sd),"%9.2f")

	
	// Build table
file open textab using "$dirpath_output/table_water_summary_stats.tex", write text replace

file write textab "\begin{table}\centering" _n
file write textab "\caption{\normalsize Summary statistics -- Pump tests and groundwater consumption}" _n
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
file write textab "\caption{Estimated Demand elasticities -- Electricity  \label{tab:elec_regs_main}}" _n
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
keep if ifs_base=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0"
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
file write textab "\caption{Estimated Demand elasticities -- Groundwater  \label{tab:water_regs_combined}}" _n
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

** 5. Regression results: Water, monthly, sensitivity to time since pump test
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
file write textab "\caption{Sensitivity to recent pump tests -- Groundwater  \label{tab:water_months_from_pump_test}}" _n
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

** 6. Intensive/extensive margin results: Electricity and water
{
use "$dirpath_data/results/regs_pge_elec_annual_sp_july2020.dta" , clear

keep if pull=="PGE 20180719"
keep if panel=="annual (sp)"
keep if ifs_base=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
keep if inlist(rhs,"(log_p_mean = log_mean_p_kwh_ag_default)")
keep if inlist(fes,"sp_group sp_group#rt_large_ag basin_group#year wdist_group#year")
gen col = .

replace col = 1 if depvar=="ihs_kwh" & ifs_sample==""
*replace col = 2 if depvar=="ihs_kwh" & ifs_sample==" & elec_binary_frac>0.9"
*replace col = 3 if depvar=="elec_binary"
drop if col==.
assert _N==1
sort col
order col

forvalues c = 1/1 {
	local beta_`c' = string(beta_log_p_mean[`c'],"%9.3f")
	local se_`c' = string(se_log_p_mean[`c'],"%9.3f")
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
	local n_cty_yr_`c' = string(n_cty_yrs[`c'],"%9.0fc")
	local n_obs_`c' = string(n_obs[`c']/1e3,"%9.1f") + "K"
	local fstat_`c' = string(fstat_rk[`c'],"%9.0f")
	if "`fstat_`c''"=="." {
		local fstat_`c' = ""
	}
}

use "$dirpath_data/results/regs_pge_water_combined_annual_sp_july2020.dta" , clear

keep if pull=="PGE 20180719"
keep if panel=="annual (sp)"
keep if ifs_base=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_pump==0 & flag_weird_cust==0 & flag_partial_year==0"
keep if inlist(rhs,"(ln_mean_p_af_rast_dd_mth_2SP = log_mean_p_kwh_ag_default)")
keep if inlist(fes,"sp_group sp_group#rt_large_ag basin_group#year wdist_group#year")
gen col = .			

replace col = 2 if depvar=="ihs_af_rast_dd_mth_2SP" & ifs_sample==""	
replace col = 3 if depvar=="ihs_af_rast_dd_mth_2SP" & ifs_sample==" & elec_binary_frac>0.9"	
replace col = 4 if depvar=="elec_binary"
drop if col==.
assert _N==3
set obs 4
replace col = 1 in 4
assert _N==4
sort col
order col

forvalues c = 2/4 {

	local beta_`c' = string(beta_log_p_water[`c'],"%9.3f")
	local se_`c' = string(se_log_p_water[`c'],"%9.3f")
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
	local n_cty_yr_`c' = string(n_cty_yrs[`c'],"%9.0fc")
	local n_obs_`c' = string(n_obs[`c']/1e3,"%9.1f") + "K"
	local fstat_`c' = string(fstat_rk[`c'],"%9.0f")
	if "`fstat_`c''"=="." {
		local fstat_`c' = ""
	}
}



	// Build table
file open textab using "$dirpath_output/table_elec_water_intens_extens.tex", write text replace

file write textab "\begin{table}[t!]\centering" _n
file write textab "\small" _n
file write textab "\caption{Annual demand elasticities -- Intensive vs.\ extensive margin \label{tab:elec_water_intens_extens}}" _n
file write textab "\vspace{-0.1cm}" _n
file write textab "\small" _n
file write textab "\begin{adjustbox}{center} " _n
file write textab "\begin{tabular}{lcccccc} " _n
file write textab "\hline \hline" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab " & \multicolumn{1}{c}{Electricity} & \multicolumn{3}{c}{Groundwater} \\" _n
file write textab " \cmidrule(r){2-2} \cmidrule(l){3-5}" _n
file write textab " & Overall & Overall & Intensive & Extensive \\" _n
file write textab " & elasticity & elasticity & margin & margin \\" _n
file write textab "[0.1em]" _n
file write textab " & (1)  & (2)  & (3)  & (4)  \\ " _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab "\cline{2-5}" _n
file write textab "\vspace{-0.27cm}" _n
file write textab "\\" _n
file write textab " $\log\big(P^{\text{elec}}_{iy}\big)$ ~ & $`beta_1'$`stars_1' &  &  &  \\ " _n
file write textab "& $(`se_1')$  &  &  &  \\" _n
file write textab "[0.1em] " _n
file write textab " $\log\big(P^{\text{water}}_{iy}\big)$ ~ &  & $`beta_2'$`stars_2' & $`beta_3'$`stars_3'  & $`beta_4'$`stars_4' \\ " _n
file write textab "&  & $(`se_2')$ & $(`se_3')$ & $(`se_4')$ \\" _n
file write textab "[1.5em] " _n
file write textab "Outcome: \\" _n
file write textab "~~ $\sinh^{-1}\big(Q_{iy}\big)$ & Yes & Yes & Yes & \\" _n
file write textab "[0.1em] " _n
file write textab "~~ $1\big[Q_{iy}>0\big]$ & & & & Yes \\" _n
file write textab "[1.5em] " _n
file write textab "Sample restriction: \\" _n
file write textab "~~ \$Q_{iy} > 0\$ in all years & & & Yes & \\" _n
file write textab "[1.5em] " _n
file write textab "Instrument: \\" _n
file write textab "[0.1em] " _n
file write textab "~~ Default $\log\big(P^{\text{elec}}_{iy}\big)$  & Yes & Yes & Yes  & Yes   \\" _n
file write textab "[1.5em] " _n
file write textab "Fixed effects: \\" _n
file write textab "[0.1em] " _n
file write textab "~~Unit $\times$ physical capital & Yes & Yes & Yes & Yes   \\" _n
file write textab "[0.1em] " _n
file write textab "~~Water basin $\times$ year & Yes & Yes & Yes & Yes \\" _n
file write textab "[0.1em] " _n
file write textab "~~Water district $\times$ year & Yes & Yes & Yes & Yes \\" _n
file write textab "[1.5em] " _n
file write textab "Service point units & `n_sp_1' & `n_sp_2' & `n_sp_3' & `n_sp_4'  \\ " _n
file write textab "[0.1em] " _n
file write textab "County \$\times\$ years  & `n_cty_yr_1' & `n_cty_yr_2' & `n_cty_yr_3' & `n_cty_yr_4' \\ " _n
file write textab "[0.1em] " _n
file write textab "Observations & `n_obs_1' & `n_obs_2' & `n_obs_3' & `n_obs_4' \\ " _n
file write textab "[0.1em] " _n
file write textab "First stage \$F\$-statistic & `fstat_1' & `fstat_2' & `fstat_3' & `fstat_4'  \\ " _n
file write textab "[0.15em]" _n
file write textab "\hline" _n
file write textab "\end{tabular}" _n
file write textab "\end{adjustbox}" _n
file write textab "\captionsetup{width=\textwidth}" _n
file write textab "\caption*{\scriptsize \emph{Notes:} Each regression estimates Equation (\ref{eq:reg_elec_annual}) or Equation (\ref{eq:reg_water_annual}) at the service point by year level." _n
file write textab "Columns (1) reports results for electricity consumption, and Columns (2)--(4) report results for groundwater consumption." _n
file write textab "Columns (1)--(2) report annual demand elasticities for electricity and water, respectively. These results are analogous to the monthly demand elasticities reported " _n
file write textab "in Column (4) of Table \ref{tab:elec_regs_main} and in Column (5) of Table \ref{tab:water_regs_combined}, respectively." _n
file write textab "Column (3) reports analogous demand elasticity for the subset of service points that consume groundwater in every year of our sample." _n
file write textab "Column (4) reports the semi-elasticity for the extensive margins by replacing the outcome variable with a binary indicator of groundwater consumption." _n
file write textab "We estimate these regressions using two-stage least squares, instrumenting with unit \$i\$'s within-category default logged electricity price in year \$y\$." _n
file write textab "\`\`Physical capital'' is a categorical variable for (i) small pumps, (ii) large pumps, and (iii) internal combustion engines, and unit \$\times\$" _n
file write textab "physical capital fixed effects control for shifts in tariff category triggered by the installation of new pumping equipment." _n
file write textab "Water basin \$\times\$ year fixed effects control for broad geographic trends in groundwater depth." _n
file write textab "Water district \$\times\$ year fixed effects control for annual variation in surface water allocations." _n
file write textab "All regressions drop solar NEM customers, customers with bad geocodes, years with irregular electricity bills" _n
file write textab "(e.g.\ first/last bills, bills longer/shorter than 1 month, overlapping bills for a single account), and incomplete years." _n
file write textab "Groundwater regressions use a monthly time interval to assign rasterized groundwater levels." _n
file write textab "Standard errors (in parentheses) are two-way clustered by service point and by county-year." _n
file write textab "Significance: *** \$p < 0.01\$, ** \$p < 0.05\$, * \$p < 0.10\$." _n
file write textab "}" _n
file write textab "\end{table}" _n

file close textab

}

************************************************
************************************************

** 7-8. Discrete choice results table, and table of counterfactuls
{

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
file write textab "\caption*{\scriptsize \emph{Notes:} We estimate the discrete choice model of Equation (\ref{eq:discrete_choice}) using IV probit, instrumenting for groundwater price with the default within-category electricity price. " _n
file write textab "We include county-by-year-by-crop-type fixed effects to flexibly estimate profit excluding the cost of groundwater pumping." _n
file write textab "The left column presents the average marginal effects of groundwater price on crop type choice probabilities." _n
file write textab "The right column reports the average semi-elasticities for each crop type choice probability with respect to the groundwater price." _n
file write textab "Each value is calculated for every observation in our analysis, and then we take the mean over all observations to yield these average values. " _n
file write textab "Standard errors (in parentheses) are clustered at the common land unit (CLU)." _n
file write textab "Significance: *** \$p < 0.01$, ** \$p < 0.05$, * \$p < 0.10$." _n
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

}

************************************************
************************************************

