
clear all
version 13
set more off

************************************************
**** Script to make figures for camp slides ****
************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

************************************************
************************************************
 
** 1. Histogram of average annual bill ($)
{
use "$dirpath_data/merged/sp_month_elec_panel.dta", clear
assert regexm(rt_sched_cd,"AG")==1
gen matched = merge_sp_water_panel==3 & flag_weird_cust==0
tab matched

	// collapse to annual billed amount per SP
unique sp_uuid modate
assert r(unique)==r(N)	
egen n_month = count(modate), by(sp_uuid)
gen n_year = n_month/12
*egen tag = tag(sp_uuid)
*hist n_year if tag
collapse (sum) mnth_bill_amount (min) matched, by(sp_uuid n_year n_month) fast
unique sp_uuid 
assert r(unique)==r(N)
gen yrly_bill_amount = mnth_bill_amount / n_year

	// bottom code zeros
replace yrly_bill_amount = 0 if yrly_bill_amount<0

sum yrly_bill_amount, detail

hist yrly_bill_amount if yrly_bill_amount<60000
hist yrly_bill_amount if yrly_bill_amount<60000 & matched==1
hist yrly_bill_amount if yrly_bill_amount<60000 & matched==0
tabstat yrly_bill_amount, by(matched) s(p5 p25 p50 p75 p95 n)

sum yrly_bill_amount if yrly_bill_amount>0 & matched, detail
gen yrly_bill_top15k = max(min(yrly_bill_amount,15000),0)
hist yrly_bill_top15k if matched

gen yrly_bill_top50k = max(min(yrly_bill_amount,50000),0)
hist yrly_bill_top50k if matched

gen yrly_bill_top100k = max(min(yrly_bill_amount,100000),0)
hist yrly_bill_top100k if matched

gen yrly_bill_top80k = max(min(yrly_bill_amount,80000),0)
hist yrly_bill_top80k if matched

gen yrly_bill_top60k = max(min(yrly_bill_amount,60000),0)
hist yrly_bill_top60k if matched

replace yrly_bill_top80k = yrly_bill_top80k/1000

twoway hist yrly_bill_top80k if matched ///
	, freq fcolor(navy) lcolor(black) lw(thin) w(2.5) s(0) ///
	xscale(r(0,60)) xlab(0(20)80, labsize(vlarge)) ///
	xtitle("Thousand dollars per year", size(vlarge)) ///
	yscale(r(0,1000)) ylab(0 400 800 1200,nogrid angle(0) labsize(vlarge)) ///
	ytitle("Number of service points", size(vlarge)) ///
	graphr(color(white) lc(white)) ///
	title("Average Amount Billed", size(vlarge) color(black))
graph export "$dirpath/output/hist_avg_annual_bill.eps", replace

}

************************************************
************************************************

** 2. Histogram of average monthly bill (kWh)
{
use "$dirpath_data/merged/sp_month_elec_panel.dta", clear
assert regexm(rt_sched_cd,"AG")==1
gen matched = merge_sp_water_panel==3 & flag_weird_cust==0
tab matched
unique sp_uuid modate
assert r(unique)==r(N)	

	// drop zero months
drop if mnth_bill_kwh<=0

	// collapse to SP level
collapse (mean) mnth_bill_kwh (min) matched, by(sp_uuid) fast

replace mnth_bill_kwh = mnth_bill_kwh/1000
gen mnth_bill_mwh_top30 = min(mnth_bill_kwh,30)
gen mnth_bill_mwh_top50 = min(mnth_bill_kwh,50)

twoway hist mnth_bill_mwh_top50
twoway hist mnth_bill_mwh_top50 if matched
twoway hist mnth_bill_mwh_top50 if !matched

twoway hist mnth_bill_mwh_top50 if matched ///
	, freq fcolor(navy) lcolor(black) lw(thin) w(2.5) s(0) ///
	xscale(r(0,50)) xlab(0(10)50, labsize(vlarge)) ///
	xtitle("MWh per (non-zero) bill", size(vlarge)) ///
	ylab(0(500)2000,nogrid angle(0) labsize(vlarge)) ytitle("Number of service points", size(vlarge)) ///
	graphr(color(white) lc(white)) ///
	title("Average MWh Billed", size(vlarge) color(black))
graph export "$dirpath/output/hist_avg_bill_mwh.eps", replace

}

************************************************
************************************************

** 3. Time series rate changes
{
use "$dirpath_data/merged/ag_rates_avg_by_month.dta", clear
replace rt_sched_cd = subinstr(rt_sched_cd,"AG-","",1)
reshape wide *kwh, i(modate) j(rt_sched_cd) string
tsset modate

local xmin = ym(2008,1)
local xmax = ym(2017,9)
local xlabmax = ym(2017,1)
keep if inrange(modate,`xmin',`xmax')

twoway (tsline mean_p_kwh4A, color(eltblue) lwidth(medthick)) ///
	(tsline mean_p_kwh4B, color(midblue) lwidth(medthick)) ///
	(tsline mean_p_kwh5C, color(dknavy) lwidth(medthick)), ///
	xscale(r(`xmin',`xmax')) xlab(`xmin'(36)`xlabmax', labsize(large)) ///
	xtitle("Month", size(large)) ///
	ylab(0(0.05)0.25,nogrid angle(0) labsize(large)) ytitle("Avg marginal price ($/kWh)", size(large)) ///
	graphr(color(white) lc(white)) ///
	title("Monthly Average Marginal Price", size(vlarge) color(black)) ///
	legend(order(1 "AG-4A    " 2 "AG-4B    " 3 "AG-5C")  col(3) size(large))
graph export "$dirpath/output/ts_marg_price_3rates.eps", replace


}

************************************************
************************************************

** 4. Kernel densities of DWL
{
use "$dirpath_data/results/externality_calcs_june2016_rast_dd_mth_2SP.dta", clear
keep if in_regs==1
keep if basin_group==122 // San Joaquin Valley only
keep if q_old>1 & q_old!=. 

local N_total = _N

	// Define 10-90 sample
sum dcs_i, detail
gen in_kdens = inrange(dcs_i,r(p10),r(p90))	

	// Create scaled-up estimates
foreach r in 5 10 20 30 {
	gen sum_dcs_j_pos`r'_upr = sum_dcs_j_pos`r'*scale_up_rates
	gen sum_dcs_j_pos`r'_upc = sum_dcs_j_pos`r'*scale_up_counts
}

twoway ///
	(kdensity dcs_i if in_kdens, lcolor(maroon) lw(medium))  ///
	, xline(0, lcolor(gs5) lw(thin)) xscale(r(-6,3.2)) xlab(-6(1)3, labsize(vlarge)) ///
	xtitle("$ per AF" " " " ", size(vlarge)) ///
	ylab(,nogrid nolabels noticks) ytitle("") yscale(lcolor(white)) ///
	graphr(color(white) lc(white)) ///
	/*title("TEMP", size(vlarge) color(black))*/ ///
	text(0.08 -4 "Farm {it:i}{subscript: }'s {&Delta}CS{it:{subscript:i}}", place(n) size(vlarge) color(maroon)) 
graph export "$dirpath/output/ext_dcs_i.eps", replace

twoway ///
	(kdensity dcs_i if in_kdens, lcolor(maroon) lw(medium))  ///
	(kdensity sum_dcs_j_pos10 if in_kdens, lcolor(green) lw(medium) lpattern(solid))  ///
	(kdensity sum_dcs_j_pos20 if in_kdens, lcolor(green) lw(medium) lpattern(longdash))  ///
	(kdensity sum_dcs_j_pos30 if in_kdens, lcolor(green) lw(medium) lpattern(shortdash))  ///
	, xline(0, lcolor(gs5) lw(thin)) xscale(r(-6,3.2)) xlab(-6(1)3, labsize(vlarge)) ///
	xtitle("$ per AF", size(vlarge)) ///
	ylab(,nogrid nolabels noticks) ytitle("") yscale(lcolor(white)) ///
	graphr(color(white) lc(white)) ///
	/*title("TEMP", size(vlarge) color(black))*/ ///
	text(0.12 -4 "Farm {it:i}{subscript: }'s {&Delta}CS{it:{subscript:i}}", place(n) size(vlarge) color(maroon)) ///
	text(3 1.9 "Neighbors'" "{&Sigma}{it:{subscript:j }}{&Delta}CS{it:{subscript:j}}" "(sample)", place(n) size(vlarge) color(green)) ///
	legend(order(2 "10 miles  " 3 "20 miles  " 4 "30 miles") col(3) size(vlarge))
graph export "$dirpath/output/ext_dcs_j.eps", replace

twoway ///
	(kdensity dcs_i if in_kdens, lcolor(maroon) lw(medium))  ///
	(kdensity sum_dcs_j_pos10_upr if in_kdens, lcolor(green) lw(medium) lpattern(solid))  ///
	(kdensity sum_dcs_j_pos20_upr if in_kdens, lcolor(green) lw(medium) lpattern(longdash))  ///
	(kdensity sum_dcs_j_pos30_upr if in_kdens, lcolor(green) lw(medium) lpattern(shortdash))  ///
	, xline(0, lcolor(gs5) lw(thin)) xscale(r(-6,3.2)) xlab(-6(1)3, labsize(vlarge)) ///
	xtitle("$ per AF", size(vlarge)) ///
	ylab(,nogrid nolabels noticks) ytitle("") yscale(lcolor(white)) ///
	graphr(color(white) lc(white)) ///
	/*title("TEMP", size(vlarge) color(black))*/ ///
	text(0.12 -4 "Farm {it:i}{subscript: }'s {&Delta}CS{it:{subscript:i}}", place(n) size(large) color(maroon)) ///
	text(1.1 2.4 "Neighbors'" "{&Sigma}{it:{subscript:j }}{&Delta}CS{it:{subscript:j}}" "(scaled)", place(n) size(vlarge) color(green)) ///
	legend(order(2 "10 miles  " 3 "20 miles  " 4 "30 miles") col(3) size(vlarge))
graph export "$dirpath/output/ext_dcs_j_scaled.eps", replace

twoway ///
	(kdensity dW_10_upr if in_kdens, lcolor(midblue) lw(medium) lpattern(solid))  ///
	(kdensity dW_20_upr if in_kdens, lcolor(midblue) lw(medium) lpattern(longdash))  ///
	(kdensity dW_30_upr if in_kdens, lcolor(midblue) lw(medium) lpattern(shortdash))  ///
	, xline(0, lcolor(gs5) lw(thin)) xscale(r(-6,3.2)) xlab(-6(1)3, labsize(vlarge)) ///
	xtitle("$ per AF", size(vlarge)) ///
	ylab(,nogrid nolabels noticks) ytitle("") yscale(lcolor(white)) ///
	graphr(color(white) lc(white)) ///
	/*title("TEMP", size(vlarge) color(black))*/ ///
	text(.13 -4 "{&Delta}W = {&Delta}CS{it:{subscript:i}} + {&Sigma}{it:{subscript:j }}{&Delta}CS{it:{subscript:j}}" "(scaled)", place(n) size(vlarge) color(midblue)) ///
	legend(order(1 "10 miles  " 2 "20 miles  " 3 "30 miles") col(3) size(vlarge))
graph export "$dirpath/output/ext_dw_scaled.eps", replace


}

************************************************
************************************************

** 5. Histogram of incentive per kWh expected savings
{
use "$dirpath_data/pge_cleaned/pump_test_project_data.dta" , clear
gen subsidy_per_kwh = subsidy_proj / est_savings_kwh_yr
keep pge_badge_nbr date_proj_finish subsidy_proj est_savings_kwh_yr subsidy_per_kwh
duplicates drop
drop pge_badge_nbr
duplicates drop

unique date_proj_finish subsidy_proj
assert r(unique)==r(N)

hist est_savings_kwh_yr
hist subsidy_proj
hist subsidy_per_kwh if subsidy_per_kwh<0.5
replace subsidy_per_kwh = 0.3 if subsidy_per_kwh!=. & subsidy_per_kwh>0.3

sum subsidy_proj, detail
gen subsidy_proj_top15 = min(subsidy_proj,15000)
gen subsidy_proj_top20 = min(subsidy_proj,20000)
gen subsidy_proj_top25 = min(subsidy_proj,25000)

twoway hist subsidy_proj_top20 ///
	, freq fcolor(navy) lcolor(black) lw(thin) s(0) ///
	xscale(r(0,0.3)) xlab(0 "0" 5000 "5" 10000 "10" 15000 "15" 20000 "20", labsize(vlarge)) ///
	xtitle("Total subsidy (thousand $)", size(vlarge)) ///
	ylab(,nogrid angle(0) labsize(vlarge)) ytitle("Number of projects", size(vlarge)) ///
	graphr(color(white) lc(white)) ///
	title("Subsidies for Pump Efficiency Upgrades", size(vlarge) color(black))
graph export "$dirpath/output/hist_project_subsidy.eps", replace


twoway hist subsidy_per_kwh if subsidy_per_kwh<=0.5 ///
	, freq fcolor(navy) lcolor(black) lw(thin) s(0) ///
	xscale(r(0,0.3)) xlab(, labsize(vlarge)) ///
	xtitle("$ subsidy per kWh expected savings", size(vlarge)) ///
	ylab(,nogrid angle(0) labsize(vlarge)) ytitle("Number of projects", size(vlarge)) ///
	graphr(color(white) lc(white)) ///
	title("Subsidies for Pump Efficiency Upgrades", size(vlarge) color(black))
graph export "$dirpath/output/hist_project_subsidy_per_kwh.eps", replace

}

************************************************
************************************************
  
** 6. Hourly prices against hourly usage histogram
{
use "$dirpath_data/merged/sp_hourly_elec_panel_20180719.dta", clear
keep if inlist(month(date),5,6,7,8,9,10)
collapse (sum) kwh (mean) p_kwh, by(sp_uuid hour) fast
*merge m:1 sp_uuid using "$dirpath_data/merged/sp_rate_switchers.dta", nogen ///
*	keep(1 3) keepusing(sp_same_rate_always sp_same_rate_dumbsmart sp_same_rate_in_cat)
	
tabstat kwh, by(hour) s(sum p5 p25 p50 p75 p95)	

*gen group = .
*replace group = 0 if sp_same_rate_dumbsmart==1
*replace group = 1 if sp_same_rate_dumbsmart==0 & sp_same_rate_in_cat==1
*replace group = 2 if sp_same_rate_in_cat==0

egen double kwh_sum = sum(kwh), by(hour) // group)
egen double p_kwh_mean = sum(p_kwh*kwh/kwh_sum), by(hour) //group)
	
keep kwh_sum p_kwh_mean hour //group
duplicates drop
unique hour //group
assert r(unique)==r(N)	
sort /*group*/ hour	

egen double denom = sum(kwh_sum) //, by(group)
gen kwh_pct = kwh_sum / denom
	
twoway ///
	(bar kwh_pct hour /*if group==0*/, lw(medium) lcolor(eltblue) color(eltblue) yaxis(1)) ///
	(scatter p_kwh_mean hour /*if group==0*/, mcolor(dknavy) msize(medlarge) yaxis(2)) ///
	, ///
	xscale(r(0,23)) xlab(0(2)23, labsize(medlarge)) ///
	xtitle("Hour", size(medlarge)) ///
	yscale(r(0,.05) axis(1)) ///
	yscale(r(0,.20) axis(2)) ///
	ylab(0 0.01 0.02 0.03 0.04 0.05,nogrid angle(0) labcolor(eltblue) labsize(medlarge) axis(1)) ///
	ylab(0 0.05 0.10 0.15 0.20,nogrid angle(0) labcolor(dknavy) labsize(medlarge) axis(2)) ///
	ytitle("kWh density", color(eltblue) size(medlarge) axis(1)) ///
	ytitle("Avg marg price ($\kWh)", color(dknavy) size(medlarge) axis(2)) ///
	graphr(color(white) lc(white)) ///
	title("Consumption vs. Mean Hourly Price" "(Summer Months)", size(large) color(black)) ///
	legend(off) ///
	aspectratio(0.6)
graph export "$dirpath/output/hourly_hist_prices_pooled_summer.eps", replace


}

************************************************
************************************************

** 7. Hourly reg coefficient plot
{
use "$dirpath/output/hourly_regs_season.dta", clear
rename hr hour
merge 1:1 hour seas using "$dirpath/output/hourly_kwh_season.dta"

gen hr = hour
replace hr = hr - 0.1 if seas=="summer"
replace hr = hr + 0.1 if seas=="winter"

sum kwh 
replace kwh = kwh/r(sum)

twoway ///
	(rcap uci lci hr if seas=="summer", msize(small) lstyle(ci) lw(medium)) ///
	(rcap uci lci hr if seas=="winter", msize(small) lstyle(ci) lw(medium)) ///
	(scatter beta hr if seas=="summer", msize(medlarge) mc(dknavy) yline(0, lc(gs13))) ///
	(scatter beta hr if seas=="winter", msize(medlarge) mc(eltblue)) ///
	, ///
	xscale(r(0,23)) xlab(0(2)23, labsize(large)) ///
	xtitle("Hour", size(large)) ///
	ylab(-1 -0.8 "-0.8" -0.6 "-0.6" -0.4 "-0.4" -0.2 "-0.2" 0 ,nogrid angle(0) labsize(large)) ///
	///ytitle("Hourly coefficient", size(large)) ///
	graphr(color(white) lc(white)) ///
	/// title("Price Elasticities by Hour/Season ", size(vlarge) color(black)) ///
	legend(order(3 "Summer"	4 "Winter")  col(2) size(medlarge)) ///
	aspectratio(0.55)
graph export "$dirpath/output/hourly_regs.eps", replace

	

sum total_bill_mwh_top50, detail

}

************************************************
************************************************
