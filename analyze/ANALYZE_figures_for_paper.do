
clear all
version 13
set more off

************************************************
**** Script to make figures for camp slides ****
************************************************

global dirpath "T:/Projects/Pump Data"
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

** 2. Kernel densities of DWL
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
	(kdensity dcs_i if in_kdens, lcolor(orange_red) lw(medthick))  ///
	(kdensity sum_dcs_j_pos10 if in_kdens, lcolor(green) lw(medthick) lpattern(solid))  ///
	(kdensity sum_dcs_j_pos20 if in_kdens, lcolor(green) lw(medthick) lpattern(longdash))  ///
	(kdensity sum_dcs_j_pos30 if in_kdens, lcolor(green) lw(medthick) lpattern(shortdash))  ///
	, xline(0, lcolor(gs5) lw(thin)) xscale(r(-6,3.2)) xlab(-6(1)3, labsize(medlarge)) ///
	xtitle("$ per AF", size(medlarge)) ///
	ylab(,nogrid nolabels noticks) ytitle("") yscale(lcolor(white)) ///
	graphr(color(white) lc(white)) ///
	title("Marginal Change in Private Surplus and Externality (APEP)", size(medlarge) color(black)) ///
	text(0.2 -4 "Farm {it:i}{subscript: }'s {&Delta}CS{it:{subscript:i}}", place(n) size(medlarge) color(orange_red)) ///
	text(3 1.9 "Neighbors'" "{&Sigma}{it:{subscript:j }}{&Delta}CS{it:{subscript:j}}", place(n) size(medlarge) color(green)) ///
	legend(order(2 "10 miles  " 3 "20 miles  " 4 "30 miles") col(3) size(medlarge)) ///
	aspect(.2)
graph export "$dirpath/output/ext_dcs_j.eps", replace

twoway ///
	(kdensity dcs_i if in_kdens, lcolor(orange_red) lw(medthick))  ///
	(kdensity sum_dcs_j_pos10_upr if in_kdens, lcolor(green) lw(medthick) lpattern(solid))  ///
	(kdensity sum_dcs_j_pos20_upr if in_kdens, lcolor(green) lw(medthick) lpattern(longdash))  ///
	(kdensity sum_dcs_j_pos30_upr if in_kdens, lcolor(green) lw(medthick) lpattern(shortdash))  ///
	, xline(0, lcolor(gs5) lw(thin)) xscale(r(-6,3.2)) xlab(-6(1)3, labsize(medlarge)) ///
	xtitle("$ per AF", size(medlarge)) ///
	ylab(,nogrid nolabels noticks) ytitle("") yscale(lcolor(white)) ///
	graphr(color(white) lc(white)) ///
	title("Marginal Change in Private Surplus and Externality (Scaled)", size(medlarge) color(black)) ///
	text(0.12 -4 "Farm {it:i}{subscript: }'s {&Delta}CS{it:{subscript:i}}", place(n) size(medlarge) color(orange_red)) ///
	text(0.95 2.4 "Neighbors'" "{&Sigma}{it:{subscript:j }}{&Delta}CS{it:{subscript:j}}", place(n) size(medlarge) color(green)) ///
	legend(order(2 "10 miles  " 3 "20 miles  " 4 "30 miles") col(3) size(medlarge)) ///
	aspect(.2)
graph export "$dirpath/output/ext_dcs_j_scaled.eps", replace

twoway ///
	(kdensity dW_10_upr if in_kdens, lcolor(midblue) lw(medthick) lpattern(solid))  ///
	(kdensity dW_20_upr if in_kdens, lcolor(midblue) lw(medthick) lpattern(longdash))  ///
	(kdensity dW_30_upr if in_kdens, lcolor(midblue) lw(medthick) lpattern(shortdash))  ///
	, xline(0, lcolor(gs5) lw(thin)) xscale(r(-6,3.2)) xlab(-6(1)3, labsize(medlarge)) ///
	xtitle("$ per AF", size(medlarge)) ///
	ylab(,nogrid nolabels noticks) ytitle("") yscale(lcolor(white)) ///
	graphr(color(white) lc(white)) ///
	title("Marginal Change in Welfare (Scaled)", size(medlarge) color(black)) ///
	text(.13 -4 "{&Delta}W = {&Delta}CS{it:{subscript:i}} + {&Sigma}{it:{subscript:j }}{&Delta}CS{it:{subscript:j}}", place(n) size(medlarge) color(midblue)) ///
	legend(order(1 "10 miles  " 2 "20 miles  " 3 "30 miles") col(3) size(medlarge)) ///
	aspect(.2)
graph export "$dirpath/output/ext_dw_scaled.eps", replace


}

************************************************
************************************************

** 3. Davis Cost studies lines crossing figure
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
