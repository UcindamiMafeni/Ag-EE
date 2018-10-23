
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
 
** 1. Histogram of incentive per kWh expected savings
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
 
** 2. Histogram of average annual bill ($)
{
use "$dirpath_data/merged/sa_bill_elec_panel.dta", clear
keep if pull=="20180719"
keep if regexm(rt_sched_cd,"AG")==1

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

** 4. Hourly reg coefficient plot
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

	
twoway ///
	(bar kwh hour if seas=="summer", lw(medium) lcolor(black) color(dknavy)) ///
	(bar kwh hour if seas=="winter", lw(medium) lcolor(black) color(eltblue)) ///
	, ///
	xscale(r(0,23)) xlab(0(2)23, labsize(large)) ///
	xtitle("Hour", size(large)) ///
	ylab(,nogrid angle(0) labsize(large)) ///
	ytitle("kWh density", size(large)) ///
	graphr(color(white) lc(white)) ///
	/// title("Price Elasticities by Hour/Season ", size(vlarge) color(black)) ///
	legend(order(1 "Summer"	2 "Winter")  col(2) size(medlarge)) ///
	aspectratio(0.55)
graph export "$dirpath/output/hourly_kwh.eps", replace


sum total_bill_mwh_top50, detail

}

************************************************
************************************************
