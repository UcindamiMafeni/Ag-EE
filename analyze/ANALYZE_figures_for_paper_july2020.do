
clear all
version 13
set more off
set matsize 11000

*******************************************************
**** Script to make figures for paper and appendix ****
*******************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

************************************************
************************************************
  
** 1. Bar graph of acreage by crop
{
use "$dirpath_data/cleaned_spatial/cdl_panel_crop_year_full.dta", clear
collapse (sum) landtype_acres, by(year landtype noncrop) fast
drop if noncrop==1
drop noncrop
drop if landtype=="Fallow/Idle Cropland"
egen sum_year = sum(landtype_acres), by(year)
tab year sum_year
sort landtype year
rename landtype_acres acres
tabstat acres if year==2010 & acres>3e5, by(landtype)
tabstat acres if year==2017 & acres>3e5, by(landtype)
gen in_bars10 = inlist(landtype,"Alfalfa","Almonds","Corn","Cotton","Grapes","Rice","Winter Wheat")
gen in_bars17 = inlist(landtype,"Alfalfa","Almonds","Grapes","Pistachios","Rice","Walnuts","Winter Wheat")
gen xbar10 = .
replace xbar10 = 1 if landtype=="Alfalfa"
replace xbar10 = 2 if landtype=="Almonds"
replace xbar10 = 3 if landtype=="Winter Wheat"
replace xbar10 = 4 if landtype=="Rice"
replace xbar10 = 5 if landtype=="Cotton"
replace xbar10 = 6 if landtype=="Corn"
replace xbar10 = 7 if landtype=="Grapes"
replace xbar10 = 8 if landtype=="Walnuts"
replace xbar10 = 9 if landtype=="Pistachios"
gen xbar17 = xbar10+0.4
gen cali_crop = inlist(landtype,"Almonds","Grapes","Pistachios","Walnuts")

gen acresM = acres/1e6
gen acresK = acres/1e3

twoway ///
	(bar acresK xbar10 if (in_bars10 | in_bars17) & year==2010 & cali_crop==20, fi(inten20) color(black) barw(0.4)) ///
	(bar acresK xbar10 if (in_bars10 | in_bars17) & year==2010 & cali_crop==20, fi(inten70) color(black) barw(0.4)) ///
	(bar acresK xbar10 if (in_bars10 | in_bars17) & year==2010 & cali_crop==20, fi(inten0) color(navy) barw(1) lwidth(medium)) ///
	(bar acresK xbar10 if (in_bars10 | in_bars17) & year==2010 & cali_crop==20, fi(inten0) color(midblue) barw(1) lwidth(medium)) ///
	(bar acresK xbar10 if (in_bars10 | in_bars17) & year==2010 & cali_crop==1, fi(inten20) color(midblue) barw(0.4)) ///
	(bar acresK xbar10 if (in_bars10 | in_bars17) & year==2010 & cali_crop==0, fi(inten20) color(navy) barw(0.4)) ///	
	(bar acresK xbar17 if (in_bars10 | in_bars17) & year==2017 & cali_crop==1, color(midblue) barw(0.4)) ///
	(bar acresK xbar17 if (in_bars10 | in_bars17) & year==2017 & cali_crop==0, color(navy) barw(0.4)) ///	
	, ylab(0 500 1000 1500, nogrid angle(0) labsize(medlarge)) ///
	ytitle("Thousand acres planted", size(medlarge)) ///
	xlab("") xtitle("") xscale(r(1 10)) graphr(color(white) lc(white)) ///
	legend(order(1 "2010" 2 "2017" 3 "Low-value crops" 4 "High-value crops" ) rows(2)) /*title("California Acreage by Crop, 2017", size(medlarge) color(black)) */ ///
	text(1310 1.2 "Alfalfa", place(n) angle(45) size(medlarge) color(navy)) ///
	text(1237 2.4 "Almonds", place(n) size(medlarge) color(midblue)) ///
	text(645 3.3 "Winter" "Wheat", place(n) size(medlarge) color(navy)) ///
	text(565 4.3 "Rice", place(n) size(medlarge) color(navy)) ///
	text(390 5.3 "Cotton", place(n) size(medlarge) color(navy)) ///
	text(390 6.4 "Corn", place(n) size(medlarge) color(navy)) ///
	text(705 7.0 "Grapes", place(n) size(medlarge) color(midblue)) ///
	text(385 9.4 "Walnuts", place(n) size(medlarge) color(midblue)) ///
	text(508 8.6 "Pistachios", place(n) size(medlarge) color(midblue)) 
graph export "$dirpath/output/acreage_bars2017_blue.eps", replace
}

************************************************
************************************************

** 2. Map of PGE meters --> ANALYZE_map_meters

************************************************
************************************************

** 3. Time series rate changes, 5 default rates
{
use "$dirpath_data/merged_pge/ag_rates_avg_by_month.dta", clear
replace rt_sched_cd = subinstr(rt_sched_cd,"AG-","",1)
*reg mean_p_kwh i.modate if inlist(rt_sched_cd,"1A","1B","4A","4B","ICE")
*predict resid_, residuals
egen temp1 = group(rt_sched_cd)
gen temp2 = real(subinstr(substr(string(modate,"%tm"),-2,2),"m","",.))
tab temp2
reg mean_p_kwh i.modate i.temp1#i.temp2 if inlist(rt_sched_cd,"1A","1B","4A","4B","ICE")
predict resid2_, residuals
drop temp*
reshape wide *kwh /*resid_*/ resid2_, i(modate) j(rt_sched_cd) string
tsset modate

local xmin = ym(2008,1)
local xmax = ym(2017,9)
local xlabmax = ym(2017,1)
keep if inrange(modate,`xmin',`xmax')

format modate %tmCY

twoway (tsline mean_p_kwh1A, color(eltblue) lwidth(medthick)) ///
	(tsline mean_p_kwh1B, color(eltblue) lwidth(medthick) lpattern(shortdash)) ///
	(tsline mean_p_kwhICE, color(midblue) lwidth(medthick) lpattern(dash_dot)) ///
	(tsline mean_p_kwh4A, color(dknavy) lwidth(medthick)) ///
	(tsline mean_p_kwh4B, color(dknavy) lwidth(medthick)  lpattern(shortdash)) ///
	, ///
	xscale(r(`xmin',`xmax')) xlab(`xmin'(36)`xlabmax', labsize(medlarge)) ///
	xtitle(" ", size(medlarge)) ///
	ylab(0 0.05 0.10 ".10" 0.15 0.20 ".20" 0.25 0.30 ".30",nogrid angle(0) labsize(medlarge)) ytitle("Avg marginal price ($/kWh)", size(medlarge)) ///
	graphr(color(white) lc(white)) ///
	title("Raw marginal prices", size(vlarge) color(black)) ///
	legend(order(1 "AG-1A " 2 "AG-1B " 3 "AG-ICE" 4 "AG-4A " 5 "AG-4B ")  col(3) size(medlarge) nobox region(lstyle(none) lcolor(white)))
graph export "$dirpath/output/marg_price_5_default_rates_raw.eps", replace

/*
twoway (tsline resid_1A, color(eltblue) lwidth(medthick)) ///
	(tsline resid_1B, color(eltblue) lwidth(medthick) lpattern(shortdash)) ///
	(tsline resid_4A, color(dknavy) lwidth(medthick)) ///
	(tsline resid_4B, color(dknavy) lwidth(medthick)  lpattern(shortdash)) ///
	(tsline resid_ICE, color(orange)), ///
	xscale(r(`xmin',`xmax')) xlab(`xmin'(36)`xlabmax', labsize(medlarge)) ///
	xtitle(" ", size(medlarge)) ///
	ylab(,nogrid angle(0) labsize(medlarge)) ytitle("Residualized marginal price ($/kWh)", size(medlarge)) ///
	graphr(color(white) lc(white)) ///
	/*title("Monthly Average Marginal Price", size(vlarge) color(black))*/ ///
	legend(order(1 "AG-1A " 2 "AG-1B " 3 "AG-4A " 4 "AG-$B " 5 "AG-ICE")  col(5) size(medlarge))
*/

twoway (tsline resid2_1A, color(eltblue) lwidth(medthick)) ///
	(tsline resid2_1B, color(eltblue) lwidth(medthick) lpattern(shortdash)) ///
	(tsline resid2_ICE, color(midblue) lwidth(medthick) lpattern(dash_dot)) ///
	(tsline resid2_4A, color(dknavy) lwidth(medthick)) ///
	(tsline resid2_4B, color(dknavy) lwidth(medthick)  lpattern(shortdash)) ///
	, ///
	xscale(r(`xmin',`xmax')) xlab(`xmin'(36)`xlabmax', labsize(medlarge)) ///
	xtitle(" ", size(medlarge)) ///
	ylab(-0.03 -0.02 -0.01 0 0.01 0.02 0.03,nogrid angle(0) labsize(medlarge)) ytitle("Avg marginal price ($/kWh), residuals", size(medlarge)) ///
	graphr(color(white) lc(white)) ///
	title("Residualized marginal prices", size(vlarge) color(black)) ///
	legend(order(1 "AG-1A " 2 "AG-1B " 3 "AG-ICE" 4 "AG-4A " 5 "AG-4B ")  col(3) size(medlarge) nobox region(lstyle(none) lcolor(white)))
graph export "$dirpath/output/marg_price_5_default_rates_resid.eps", replace


}

************************************************
************************************************

** 4. Histrogram of pump horsepower --> where is this code?

************************************************
************************************************

** 5. Hourly prices against hourly usage histogram
{
use "$dirpath_data/merged_pge/sp_hourly_elec_panel_20180719.dta", clear
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
	xscale(r(0,23)) xlab(0(2)23, labsize(medium)) ///
	xtitle("Hour", size(medium)) ///
	yscale(r(0,.05) axis(1)) ///
	yscale(r(0,.20) axis(2)) ///
	ylab(0 0.01 0.02 0.03 0.04 0.05,nogrid angle(0) labcolor(eltblue) labsize(medium) axis(1)) ///
	ylab(0 0.05 0.10 0.15 0.20,nogrid angle(0) labcolor(dknavy) labsize(medium) axis(2)) ///
	ytitle("kWh density", color(eltblue) size(medium) axis(1)) ///
	ytitle("Avg marg price ($\kWh)", color(dknavy) size(medium) axis(2)) ///
	graphr(color(white) lc(white)) ///
	///title("Consumption vs. Mean Hourly Price" "(Summer Months)", size(large) color(black)) ///
	legend(off) ///
	aspectratio(0.6)
graph export "$dirpath/output/hourly_hist_prices_pooled_summer.eps", replace


}

************************************************
************************************************

** 6. Water cost pictures from Power Point

************************************************
************************************************

** 7. Counterfactual maps --> ANALYZE_counterfactual_tax_maps

************************************************
************************************************

** NOT USING. Davis Cost studies lines crossing figure
{
use "$dirpath_data/Davis cost studies/Davis_cost_studies_processed_all.dta", clear

gen profits_less_water = PROFITS + vc_Irrigation
gen oper_profits_less_water = TOTAL_GROSS_RETURNS - TOTAL_OPERATING_COSTS + vc_Irrigation

gen water_share_costs = q_water*p_water / TOTAL_OPERATING_COSTS
gen p_water_af = p_water*12

sum water_share_costs, detail
sum p_water_af, detail

forvalues p = 10(10)300 {
	gen oper_prof_water`p' = TOTAL_GROSS_RETURNS - TOTAL_OPERATING_COSTS + q_water*p_water - q_water*`p'/12
	gen prof_water`p' = PROFITS + q_water*p_water - q_water*(`p'/12)
}

reshape long prof_water oper_prof_water, i(Number Name_of_crop) j(water_price)
/*
tab location if strpos(upper(location),"JOAQUIN") & strpos(upper(location),"SOUTH")
tab Name_of_crop if strpos(upper(location),"JOAQUIN") & strpos(upper(location),"SOUTH")
br if strpos(upper(location),"JOAQUIN") & strpos(upper(location),"SOUTH")
twoway ///
	(line oper_prof_water water_price if Number==0 & water_price<200) ///
	(line oper_prof_water water_price if Number==10 & water_price<200) ///
	(line oper_prof_water water_price if Number==38 & water_price<200) ///
	(line oper_prof_water water_price if Number==74 & water_price<200) ///
	(line oper_prof_water water_price if Number==86 & water_price<200) ///
	(line oper_prof_water water_price if Number==93 & water_price<200) ///
	, legend(order(1 "Alfalfa" 2 "Almonds" 3 "Corn" ///
	4 "Plums" 5 "Tomatoes" 6 "Wheat") c(3)) ///
	title("Operating profits, South San Joaquin")

twoway ///
	(line prof_water water_price if Number==0 & water_price<200) ///
	(line prof_water water_price if Number==10 & water_price<200) ///
	(line prof_water water_price if Number==38 & water_price<200) ///
	(line prof_water water_price if Number==74 & water_price<200) ///
	(line prof_water water_price if Number==86 & water_price<200) ///
	(line prof_water water_price if Number==93 & water_price<200) ///
	, legend(order(1 "Alfalfa" 2 "Almonds" 3 "Corn" ///
	4 "Plums" 5 "Tomatoes" 6 "Wheat") c(3)) ///
	title("Total profits, South San Joaquin")
*/

twoway	 ///
	(line prof_water water_price if Number==0 & water_price<=150, color(eltblue)) ///
	(line prof_water water_price if Number==10 & water_price<=150, color(navy)) ///
	(line prof_water water_price if Number==38 & water_price<=150, color(maroon) lp(shortdash)) ///
	(line prof_water water_price if Number==74 & water_price<=150, color(cranberry) lp(shortdash)) ///
	///(line prof_water water_price if Number==86 & water_price<=150) ///
	(line prof_water water_price if Number==93 & water_price<=150, color(midblue) lp(longdash)) ///
	, ///
	/*xscale(r(`xmin',`xmax'))*/ xlab(/*`xmin'(36)`xlabmax'*/, labsize(medlarge)) ///
	xtitle("Hypothetical water price ($/AF)", size(medlarge)) ///
	ylab(/*0(0.05)0.25*/,nogrid angle(0) labsize(medlarge)) ///
	ytitle("Predicted annual profit per acre ($)", size(medlarge)) ///
	graphr(color(white) lc(white)) ///
	title("Crop budgets for varying water prices", size(large) color(black)) ///
	text(290 75 "Alfalfa", place(n) size(medium) color(eltblue)) ///
	text(1665 55 "Almonds", place(n) size(medium) color(navy)) ///
	text(1425 25 "Plums", place(n) size(medium) color(cranberry)) ///
	text(-330 140 "Corn", place(n) size(medium) color(maroon)) ///
	text(0 25 "Wheat", place(n) size(medium) color(midblue)) ///
	legend(off)	
graph export "$dirpath/output/davis_lines_crossing.eps", replace
	
}

************************************************
************************************************

