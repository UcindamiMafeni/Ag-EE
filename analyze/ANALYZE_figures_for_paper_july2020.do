
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
  
** 1. Time series rate changes
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

** 2. Davis Cost studies lines crossing figure
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



use "$dirpath_data/merged_pge/ag_rates_avg_by_month.dta", clear
replace rt_sched_cd = subinstr(rt_sched_cd,"AG-","",1)
*reg mean_p_kwh i.modate if inlist(rt_sched_cd,"1A","1B","4A","4B","ICE")
*predict resid_, residuals
egen temp1 = group(rt_sched_cd)
gen temp2 = real(subinstr(substr(string(modate,"%tm"),-2,2),"m","",.))
tab temp2
reg mean_p_kwh i.modate i.temp1#i.temp2 //if inlist(rt_sched_cd,"1A","1B","4A","4B","ICE")
predict resid2_, residuals
drop temp*
reshape wide *kwh /*resid_*/ resid2_, i(modate) j(rt_sched_cd) string
tsset modate

local xmin = ym(2008,1)
local xmax = ym(2017,9)
local xlabmax = ym(2017,1)
keep if inrange(modate,`xmin',`xmax')

*twoway tsline resid2*, legend(off)

twoway tsline mean_p_kwhIC*, legend(off)

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
	/*title("Monthly Average Marginal Price", size(vlarge) color(black))*/ ///
	legend(order(1 "AG-1A " 2 "AG-1B " 3 "AG-ICE" 4 "AG-4A " 5 "AG-4B ")  col(3) size(medlarge))
graph export "$dirpath/output/marg_price_5_default_rates_raw.eps", replace
