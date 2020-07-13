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

** 5. Intensive/extensive margin results: Water
{
use "$dirpath_data/results/regs_pge_elec_annual_sp_july2020.dta" , clear

keep if pull=="PGE 20180719"
keep if panel=="annual (sp)"
keep if ifs_base=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
keep if inlist(rhs,"(log_p_mean = log_mean_p_kwh_ag_default)")
keep if inlist(fes,"sp_group sp_group#rt_large_ag basin_group#year wdist_group#year")
gen col = .

replace col = 1 if depvar=="ihs_kwh" & ifs_sample==""
replace col = 2 if depvar=="ihs_kwh" & ifs_sample==" & elec_binary_frac>0.9"
replace col = 3 if depvar=="elec_binary"
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

replace col = 4 if depvar=="ihs_af_rast_dd_mth_2SP" & ifs_sample==""	
replace col = 5 if depvar=="ihs_af_rast_dd_mth_2SP" & ifs_sample==" & elec_binary_frac>0.9"	
replace col = 6 if depvar=="elec_binary"
drop if col==.
assert _N==3
set obs 6
replace col = 1 in 4
replace col = 2 in 5
replace col = 3 in 6
assert _N==6
sort col
order col

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
	local n_cty_yr_`c' = string(n_cty_yrs[`c'],"%9.0fc")
	local n_obs_`c' = string(n_obs[`c']/1e3,"%9.1f") + "K"
	local fstat_`c' = string(fstat_rk[`c'],"%9.0f")
	if "`fstat_`c''"=="." {
		local fstat_`c' = ""
	}
}



	// Build table
file open textab using "$dirpath_output/table_water_intens_extens.tex", write text replace

file write textab "\begin{table}[t!]\centering" _n
file write textab "\small" _n
file write textab "\caption{Annual demand elasticities -- Intensive vs.\ extensive margins \label{tab:elec_water_intens_extens}}" _n
file write textab "\vspace{-0.1cm}" _n
file write textab "\small" _n
file write textab "\begin{adjustbox}{center} " _n
file write textab "\begin{tabular}{lcccc} " _n
file write textab "\hline \hline" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab " & Electricity & \multicolumn{3}{c}{Groundwater} \\" _n
file write textab " \cmidrule(r){2-2} \cmidrule(l){3-5}" _n
file write textab " & Overall   & Overall & Intensive & Extensive \\" _n
file write textab " & elasticity   & elasticity & margin & margin \\" _n
file write textab "[0.1em]" _n
file write textab " & (1)   & (2)  & (3)  & (4) \\ " _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab "\cline{2-5}" _n
file write textab "\vspace{-0.27cm}" _n
file write textab "\\" _n
file write textab " $\log\big(P^{\text{elec}}_{iy}\big)$ ~ & $`beta_1'$`stars_1'  &  &  &  \\ " _n
file write textab "& $(`se_1')$ &  &  &  \\" _n
file write textab "[0.1em] " _n
file write textab " $\log\big(P^{\text{water}}_{iy}\big)$ ~ &  & $`beta_4'$`stars_4' & $`beta_5'$`stars_5'  & $`beta_6'$`stars_6' \\ " _n
file write textab "&   & $(`se_4')$ & $(`se_5')$ & $(`se_6')$ \\" _n
file write textab "[1.5em] " _n
file write textab "Outcome: \\" _n
file write textab "~~ $\sinh^{-1}\big(Q_{iy}\big)$ & Yes& Yes & Yes & \\" _n
file write textab "[0.1em] " _n
file write textab "~~ $1\big[Q_{iy}>0\big]$ & & & & Yes \\" _n
file write textab "[1.5em] " _n
*file write textab "Sample restriction: \\" _n
*file write textab "~~ \$Q_{iy} > 0\$ in all years & & & &Yes \\" _n
*file write textab "[1.5em] " _n
file write textab "Instrument: \\" _n
file write textab "[0.1em] " _n
file write textab "~~ Default $\log\big(P^{\text{elec}}_{iy}\big)$  & Yes   & Yes  & Yes & Yes \\" _n
file write textab "[1.5em] " _n
file write textab "Fixed effects: \\" _n
file write textab "[0.1em] " _n
file write textab "~~Unit $\times$ physical capital & Yes & Yes & Yes & Yes  \\" _n
file write textab "[0.1em] " _n
file write textab "~~Water basin $\times$ year & Yes & Yes & Yes & Yes \\" _n
file write textab "[0.1em] " _n
file write textab "~~Water district $\times$ year &  Yes & Yes & Yes & Yes \\" _n
file write textab "[1.5em] " _n
file write textab "Service point units & `n_sp_1'  & `n_sp_4' & `n_sp_5' & `n_sp_6'  \\ " _n
file write textab "[0.1em] " _n
file write textab "County \$\times\$ years  & `n_cty_yr_1' & `n_cty_yr_4' & `n_cty_yr_5' & `n_cty_yr_6' \\ " _n
file write textab "[0.1em] " _n
file write textab "Observations & `n_obs_1'  & `n_obs_4' & `n_obs_5' & `n_obs_6' \\ " _n
file write textab "[0.1em] " _n
file write textab "First stage \$F\$-statistic & `fstat_1'  & `fstat_4' & `fstat_5' & `fstat_6' \\ " _n
file write textab "[0.15em]" _n
file write textab "\hline" _n
file write textab "\end{tabular}" _n
file write textab "\end{adjustbox}" _n
file write textab "\captionsetup{width=\textwidth}" _n
file write textab "\caption*{\scriptsize \emph{Notes:} Each regression estimates Equation (\ref{eq:reg_elec_annual}) or Equation (\ref{eq:reg_water_annual}) at the service point by year level." _n
file write textab "Column (1) reports results for electricity consumption, and Columns (2)--(4) report results for groundwater consumption." _n
file write textab "Columns (1) and (2) report annual demand elasticities for electricity and water, respectively. These results are analogous to the monthly demand elasticities reported " _n
file write textab "in Column (4) of Table \ref{tab:elec_regs_main} and in Column (5) of Table \ref{tab:water_regs_combined}, respectively." _n
file write textab "Column (3) reports an analogous demand elasticity for the subset of service points that consume water in every year of our sample." _n
file write textab "Column (4) reports the semi-elasticity for the extensive margin by replacing the outcome variable with a binary indicator water consumption." _n
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



/*

** 5. Regression results: Electricity and water, annual
{
use "$dirpath_data/results/regs_pge_elec_annual_sp_july2020.dta" , clear

keep if pull=="PGE 20180719"
keep if panel=="annual (sp)"
keep if ifs_base=="if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3 & flag_partial_year==0"
keep if inlist(rhs,"(log_p_mean = log_mean_p_kwh_ag_default)")
keep if inlist(fes,"sp_group sp_group#rt_large_ag basin_group#year wdist_group#year")
keep if depvar=="ihs_kwh"
keep if ifs_sample==""
assert _N==1
gen col = 1		

forvalues c = 1/1 {
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
keep if depvar=="ihs_af_rast_dd_mth_2SP"
gen col = .			

replace col = 2 if ifs_sample==""
replace col = 3 if ifs_sample==" & always50_SameType==1"
replace col = 4 if ifs_sample==" & always50_SameType_NC==1"
replace col = 5 if ifs_sample==" & always50_Crop_Switcher==1"
replace col = 6 if ifs_sample==" & always50_Switcher_Fallower==1"	
	
drop if col==.
assert _N==5
set obs 6
replace col = 1 if col==.
assert _N==6
sort col
order col

forvalues c = 2/6 {

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
	local n_cty_yr_`c' = string(n_cty_yrs[`c'],"%9.0fc")
	local n_obs_`c' = string(n_obs[`c']/1e3,"%9.1f") + "K"
	local fstat_`c' = string(fstat_rk[`c'],"%9.0f")
	if "`fstat_`c''"=="." {
		local fstat_`c' = ""
	}
}


	// Build table
file open textab using "$dirpath_output/table_elec_water_regs_annual.tex", write text replace

file write textab "\begin{table}[t!]\centering" _n
file write textab "\small" _n
file write textab "\caption{Estimated Annual Demand Elasticities  \label{tab:elec_water_regs_annual}}" _n
file write textab "\vspace{-0.1cm}" _n
file write textab "\small" _n
file write textab "\begin{adjustbox}{center} " _n
file write textab "\begin{tabular}{lcccccccc} " _n
file write textab "\hline \hline" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab " & \multicolumn{1}{c}{Electricity} & \multicolumn{5}{c}{Groundwater} \\" _n
file write textab " \cmidrule(r){2-2} \cmidrule(l){3-7}" _n
file write textab "  & Pooled & Pooled & 1 Crop & 1 Crop Type & Crop Type & Crop Switchers \\" _n
file write textab " &  &  & Type & and Fallow & Switchers & and Fallowers \\" _n
file write textab " &  &  & Crop Type & or Fallow & Switchers & and Fallowers \\" _n
file write textab "[0.1em]" _n
file write textab " & (1)  & (2)  & (3)  & (4)  & (5)  & (6) \\ " _n
file write textab "[0.1em]" _n
file write textab " & IV & IV & IV & IV & IV & IV \\" _n
file write textab "\vspace{-0.37cm}" _n
file write textab "\\" _n
file write textab "\cline{2-7}" _n
file write textab "\vspace{-0.27cm}" _n
file write textab "\\" _n
file write textab " $\log\big(P^{\text{elec}}_{iy}\big)$ ~ & " _n
file write textab " $`beta_1'$`stars_1'  & \\ " _n
file write textab "& $(`se_1')$ \\" _n
file write textab "[0.1em] " _n
file write textab " $\log\big(P^{\text{water}}_{iy}\big)$ ~ & " _n
file write textab "& $`beta_2'$`stars_2' & $`beta_3'$`stars_3' & $`beta_4'$`stars_4' & $`beta_5'$`stars_5'  & $`beta_6'$`stars_6' \\ " _n
file write textab "& & $(`se_2')$ & $(`se_3')$ & $(`se_4')$ & $(`se_5')$ & $(`se_6')$ \\" _n
file write textab "[1.5em] " _n
file write textab "Instrument: \\" _n
file write textab "[0.1em] " _n
file write textab "~~ Default $\log\big(P^{\text{elec}}_{iy}\big)$  & Yes & Yes & Yes  & Yes  & Yes & Yes \\" _n
file write textab "[1.5em] " _n
file write textab "Fixed effects: \\" _n
file write textab "[0.1em] " _n
file write textab "~~Unit $\times$ physical capital & Yes & Yes & Yes & Yes & Yes & Yes  \\" _n
file write textab "[0.1em] " _n
file write textab "~~Water basin $\times$ year  & Yes & Yes & Yes & Yes & Yes & Yes    \\" _n
file write textab "[0.1em] " _n
file write textab "~~Water district $\times$ year  & Yes & Yes & Yes & Yes & Yes & Yes  \\" _n
file write textab "[1.5em] " _n
file write textab "Service point units & `n_sp_1' & `n_sp_2' & `n_sp_3' & `n_sp_4' & `n_sp_5' & `n_sp_6'  \\ " _n
file write textab "[0.1em] " _n
file write textab "County \$\times\$ years  & `n_cty_yr_1' & `n_cty_yr_2' & `n_cty_yr_3' & `n_cty_yr_4' & `n_cty_yr_5' & `n_cty_yr_6' \\ " _n
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
file write textab "Each regression estimates Equation (\ref{eq:reg_elec_annual}) or Equation (\ref{eq:reg_water_annual}) at the service point by year level." _n
file write textab "Columns (1) report results for electricity consumption, while Columns (2)--(6) report results for groundwater consumption." _n
file write textab "Columns (1)--(2) include the full annual panel of SPs; they are analogous to the monthly regressions " _n
file write textab "in Column (4) of Table \ref{tab:elec_regs_main} and in Column (5) of Table \ref{tab:water_regs_combined}, respectively. " _n
file write textab "Columns (3)--(6) restrict the sample based on the observed crop history at each service point's assigned CLU (from 2008 to 2016). " _n
file write textab "We classify individual CLU-years as annual crop, fruit/nut perennial crop, other perennial crop, or non-crop." _n
file write textab "Column (3) includes service points in CLUs that always have the same crop type (annual, fruit/nut, other perennial) during our sample period." _n
file write textab "Column (4) includes units in CLUs that always have at most 1 of the 3 crop types, and also fallow." _n
file write textab "Column (5) includes units in CLUs that swtich between at least 2 of the crop types, and never fallow." _n
file write textab "Column (6) includes units in CLUs that switch between at least 2 of the crop types, and also fallow." _n
file write textab "These four subsamples are disjoint but not exhaustive, due to ambiguities in land cover classifications" _n
file write textab "(we require that at least 50 percent for CLU fall into a single category). " _n
file write textab "All regressions apply two-stage least squares, instrumenting with unit \$i\$'s within-category default logged electricity price in year \$y\$." _n
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
