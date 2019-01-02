
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
  
** 1. Time series rate changes
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
	(tsline mean_p_kwh4B, color(midblue) lwidth(medthick) lpattern(dash)) ///
	(tsline mean_p_kwh5B, color(dknavy) lwidth(medthick) lpattern(shortdash)), ///
	xscale(r(`xmin',`xmax')) xlab(`xmin'(36)`xlabmax', labsize(medlarge)) ///
	xtitle(" ", size(medlarge)) ///
	ylab(0(0.05)0.25,nogrid angle(0) labsize(medlarge)) ytitle("Avg marginal price ($/kWh)", size(medlarge)) ///
	graphr(color(white) lc(white)) ///
	/*title("Monthly Average Marginal Price", size(vlarge) color(black))*/ ///
	legend(order(1 "AG-4A    " 2 "AG-4B    " 3 "AG-5B")  col(3) size(medlarge))
graph export "$dirpath/output/marg_price_3rates.eps", replace


}

************************************************
************************************************

** 2. Histogram of average annual bill ($)
{
use "$dirpath_data/results/externality_calcs_june2016_rast_dd_mth_2SP.dta", clear
keep if in_regs==1

sum dW_20 if basin_group==122, detail
gen dW_20_hist = max(dW_20, -10)
hist dW_20_hist

sum dW_20_upr if basin_group==122, detail
gen dW_20_upr_hist = max(dW_20_upr,-10)
hist dW_20_upr_hist if n_j_pos20>1000

unique sa_uuid bill_start_dt
assert r(unique)==r(N)

sum total_bill_amount if total_bill_amount>0, detail
gen total_bill_top15k = max(min(total_bill_amount,15000),0)
hist total_bill_top15k

egen max_total_bill = max(total_bill_amount), by(sp_uuid)
sum max_total_bill, detail

*drop if total_bill_kwh<=0
gen days_count = bill_length-1
collapse (sum) total_bill_amount  days_count, by(sp_uuid)
replace total_bill_amount = total_bill_amount/(days_count/365)

hist total_bill_amount
gen total_bill_top50k = max(min(total_bill_amount,50000),0)
hist total_bill_top50k

gen total_bill_top100k = max(min(total_bill_amount,100000),0)
hist total_bill_top100k

gen total_bill_top80k = max(min(total_bill_amount,80000),0)
hist total_bill_top80k

gen total_bill_top60k = max(min(total_bill_amount,60000),0)
hist total_bill_top60k

replace total_bill_top60k = total_bill_top60k/1000

twoway hist total_bill_top60k ///
	, freq fcolor(navy) lcolor(black) lw(thin) w(2.5) s(0) ///
	xscale(r(0,60)) xlab(0(10)60, labsize(vlarge)) ///
	xtitle("Thousand dollars per year", size(vlarge)) ///
	ylab(,nogrid angle(0) labsize(vlarge)) ytitle("Number of service points", size(vlarge)) ///
	graphr(color(white) lc(white)) ///
	title("Average Amount Billed", size(vlarge) color(black))
graph export "$dirpath/output/hist_avg_annual_bill.eps", replace

sum total_bill_top60k, detail

}

************************************************
************************************************
 
** 3. Histogram of average monthly bill (kWh)
{
use "$dirpath_data/merged/sa_bill_elec_panel.dta", clear
keep if pull=="20180719"
keep if regexm(rt_sched_cd,"AG")==1

unique sa_uuid bill_start_dt
assert r(unique)==r(N)

drop if total_bill_kwh<=0
collapse (mean) total_bill_kwh, by(sp_uuid)
replace total_bill_kwh = total_bill_kwh/1000
gen total_bill_mwh_top30 = min(total_bill_kwh,30)
gen total_bill_mwh_top50 = min(total_bill_kwh,50)

twoway hist total_bill_mwh_top50 ///
	, freq fcolor(navy) lcolor(black) lw(thin) w(2.5) s(0) ///
	xscale(r(0,50)) xlab(0(10)50, labsize(vlarge)) ///
	xtitle("MWh per (non-zero) bill", size(vlarge)) ///
	ylab(0(500)2000,nogrid angle(0) labsize(vlarge)) ytitle("Number of service points", size(vlarge)) ///
	graphr(color(white) lc(white)) ///
	title("Average MWh Billed", size(vlarge) color(black))
graph export "$dirpath/output/hist_avg_bill_mwh.eps", replace

sum total_bill_mwh_top50, detail

}

************************************************
************************************************


** 5. Hourly prices against hourly usage histogram
{
use "$dirpath_data/merged/sp_hourly_elec_panel_20180719.dta", clear
*keep if inlist(month(date),5,6,7,8,9,10)
collapse (sum) kwh (mean) p_kwh, by(sp_uuid hour) fast
merge m:1 sp_uuid using "$dirpath_data/merged/sp_rate_switchers.dta", nogen ///
	keep(1 3) keepusing(sp_same_rate_always sp_same_rate_dumbsmart sp_same_rate_in_cat)
	
tabstat kwh, by(hour) s(sum p5 p25 p50 p75 p95)	

gen group = .
replace group = 0 if sp_same_rate_dumbsmart==1
replace group = 1 if sp_same_rate_dumbsmart==0 & sp_same_rate_in_cat==1
replace group = 2 if sp_same_rate_in_cat==0

egen double kwh_sum = sum(kwh), by(hour group)
egen double p_kwh_mean = sum(p_kwh*kwh/kwh_sum), by(hour group)
	
keep kwh_sum p_kwh_mean hour group
duplicates drop
unique hour group
assert r(unique)==r(N)	
sort group hour	

egen double denom = sum(kwh_sum), by(group)
gen kwh_pct = kwh_sum / denom
	
twoway ///
	(bar kwh_pct hour if group==0, lw(medium) lcolor(eltblue) color(eltblue) yaxis(1)) ///
	(scatter p_kwh_mean hour if group==0, mcolor(dknavy) msize(medlarge) yaxis(2)) ///
	, ///
	xscale(r(0,23)) xlab(0(2)23, labsize(vlarge)) ///
	xtitle("Hour", size(vlarge)) ///
	yscale(r(0,.05) axis(1)) ///
	yscale(r(0,.15) axis(2)) ///
	ylab(0 0.01 0.02 0.03 0.04 0.05,nogrid angle(0) labcolor(eltblue) labsize(vlarge) axis(1)) ///
	ylab(0 0.05 0.10 0.15,nogrid angle(0) labcolor(dknavy) labsize(vlarge) axis(2)) ///
	ytitle("kWh density", color(eltblue) size(vlarge) axis(1)) ///
	ytitle("Avg marg price ($\kWh)", color(dknavy) size(vlarge) axis(2)) ///
	graphr(color(white) lc(white)) ///
	title("Stayers", size(huge) color(black)) ///
	legend(off) ///
	//aspectratio(0.55)
graph export "$dirpath/output/hourly_hist_prices_stayers.eps", replace

twoway ///
	(bar kwh_pct hour if group==1, lw(medium) lcolor(eltblue) color(eltblue) yaxis(1)) ///
	(scatter p_kwh_mean hour if group==1, mcolor(dknavy) msize(medlarge) yaxis(2)) ///
	, ///
	xscale(r(0,23)) xlab(0(2)23, labsize(vlarge)) ///
	xtitle("Hour", size(vlarge)) ///
	yscale(r(0,.05) axis(1)) ///
	yscale(r(0,.15) axis(2)) ///
	ylab(0 0.01 0.02 0.03 0.04 0.05,nogrid angle(0) labcolor(eltblue) labsize(vlarge) axis(1)) ///
	ylab(0 0.05 0.10 0.15,nogrid angle(0) labcolor(dknavy) labsize(vlarge) axis(2)) ///
	ytitle("kWh density", color(eltblue) size(vlarge) axis(1)) ///
	ytitle("Avg marg price ($\kWh)", color(dknavy) size(vlarge) axis(2)) ///
	graphr(color(white) lc(white)) ///
	title("Forced Switchers", size(huge) color(black)) ///
	legend(off) ///
	//aspectratio(0.55)
graph export "$dirpath/output/hourly_hist_prices_forcedswitchers.eps", replace

twoway ///
	(bar kwh_pct hour if group==2, lw(medium) lcolor(eltblue) color(eltblue) yaxis(1)) ///
	(scatter p_kwh_mean hour if group==2, mcolor(dknavy) msize(medlarge) yaxis(2)) ///
	, ///
	xscale(r(0,23)) xlab(0(2)23, labsize(vlarge)) ///
	xtitle("Hour", size(vlarge)) ///
	yscale(r(0,.05) axis(1)) ///
	yscale(r(0,.15) axis(2)) ///
	ylab(0 0.01 0.02 0.03 0.04 0.05,nogrid angle(0) labcolor(eltblue) labsize(vlarge) axis(1)) ///
	ylab(0 0.05 0.10 0.15,nogrid angle(0) labcolor(dknavy) labsize(vlarge) axis(2)) ///
	ytitle("kWh density", color(eltblue) size(vlarge) axis(1)) ///
	ytitle("Avg marg price ($\kWh)", color(dknavy) size(vlarge) axis(2)) ///
	graphr(color(white) lc(white)) ///
	title("Choosers", size(huge) color(black)) ///
	legend(off) ///
	//aspectratio(0.55)
graph export "$dirpath/output/hourly_hist_prices_choosers.eps", replace


}

************************************************
************************************************

** 6. Hourly reg coefficient plot
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
