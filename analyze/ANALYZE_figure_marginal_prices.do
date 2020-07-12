

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

*** versions without titles

twoway  ///
	(tsline mean_p_kwh4D, color(midblue) lwidth(thin) lpattern(solid)) ///
	(tsline mean_p_kwh5A, color(midblue) lwidth(thin) lpattern(solid)) ///
	(tsline mean_p_kwh5D, color(midblue) lwidth(thin) lpattern(solid)) ///
	(tsline mean_p_kwhRA, color(midblue) lwidth(thin) lpattern(solid)) ///
	(tsline mean_p_kwhRD, color(midblue) lwidth(thin) lpattern(solid)) ///
	(tsline mean_p_kwhVA, color(midblue) lwidth(thin) lpattern(solid)) ///
	(tsline mean_p_kwhVD, color(midblue) lwidth(thin) lpattern(solid)) ///
	(tsline mean_p_kwh4E, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline mean_p_kwh5B, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline mean_p_kwh5E, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline mean_p_kwh4C, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline mean_p_kwh4F, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline mean_p_kwh5C, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline mean_p_kwh5F, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline mean_p_kwhRB, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline mean_p_kwhRE, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline mean_p_kwhVB, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline mean_p_kwhVE, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
    (tsline mean_p_kwh1A, color(midblue*0.25) lwidth(medthick) lpattern(solid)) ///
	(tsline mean_p_kwh1B, color(eltblue) lwidth(medthick) lpattern(solid)) ///
	(tsline mean_p_kwhICE, color(navy) lwidth(medthick) lpattern(solid))  ///
	(tsline mean_p_kwh4A, color(midblue) lwidth(medthick) lpattern(solid)) ///
	(tsline mean_p_kwh4B, color(blue*0.75) lwidth(medthick)  lpattern(solid))  ///
	, ///
	xscale(r(`xmin',`xmax')) xlab(`xmin'(36)`xlabmax', labsize(medlarge)) ///
	xtitle(" ", size(medlarge)) ///
	ylab(0 0.05 0.10 ".10" 0.15 0.20 ".20" 0.25 0.30 ".30",nogrid angle(0) labsize(medlarge)) ytitle("Avg marginal price ($/kWh)", size(medlarge)) ///
	graphr(color(white) lc(white)) ///
	/*title("Monthly Average Marginal Price", size(vlarge) color(black))*/ ///
	legend(order(19 "AG-1A " 20 "AG-1B " 22 "AG-4A " 23 "AG-4B " 21 "AG-ICE")  col(3) size(medlarge))
graph export "$dirpath/output/marg_price_all_rates_raw.eps", replace




twoway  ///
	(tsline resid2_4D, color(midblue) lwidth(thin) lpattern(solid)) ///
	(tsline resid2_5A, color(midblue) lwidth(thin) lpattern(solid)) ///
	(tsline resid2_5D, color(midblue) lwidth(thin) lpattern(solid)) ///
	(tsline resid2_RA, color(midblue) lwidth(thin) lpattern(solid)) ///
	(tsline resid2_RD, color(midblue) lwidth(thin) lpattern(solid)) ///
	(tsline resid2_VA, color(midblue) lwidth(thin) lpattern(solid)) ///
	(tsline resid2_VD, color(midblue) lwidth(thin) lpattern(solid)) ///
	(tsline resid2_4E, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline resid2_5B, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline resid2_5E, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline resid2_4C, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline resid2_4F, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline resid2_5C, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline resid2_5F, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline resid2_RB, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline resid2_RE, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline resid2_VB, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline resid2_VE, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
    (tsline resid2_1A, color(midblue*0.25) lwidth(medthick) lpattern(solid)) ///
	(tsline resid2_1B, color(eltblue) lwidth(medthick) lpattern(solid)) ///
	(tsline resid2_ICE, color(navy) lwidth(medthick) lpattern(solid))  ///
	(tsline resid2_4A, color(midblue) lwidth(medthick) lpattern(solid)) ///
	(tsline resid2_4B, color(blue*0.75) lwidth(medthick)  lpattern(solid))  ///
	, ///
	xscale(r(`xmin',`xmax')) xlab(`xmin'(36)`xlabmax', labsize(medlarge)) ///
	xtitle(" ", size(medlarge)) ///
	ylab(-0.03 -0.02 -0.01 0 0.01 0.02 0.03,nogrid angle(0) labsize(medlarge)) ytitle("Avg marginal price ($/kWh), residuals", size(medlarge)) ///
	graphr(color(white) lc(white)) ///
	/*title("Monthly Average Marginal Price", size(vlarge) color(black))*/ ///
	legend(order(19 "AG-1A " 20 "AG-1B " 22 "AG-4A " 23 "AG-4B " 21 "AG-ICE")  col(3) size(medlarge))
graph export "$dirpath/output/marg_price_all_rates_resid.eps", replace

*** versions with titles

twoway  ///
	(tsline mean_p_kwh4D, color(midblue) lwidth(thin) lpattern(solid)) ///
	(tsline mean_p_kwh5A, color(midblue) lwidth(thin) lpattern(solid)) ///
	(tsline mean_p_kwh5D, color(midblue) lwidth(thin) lpattern(solid)) ///
	(tsline mean_p_kwhRA, color(midblue) lwidth(thin) lpattern(solid)) ///
	(tsline mean_p_kwhRD, color(midblue) lwidth(thin) lpattern(solid)) ///
	(tsline mean_p_kwhVA, color(midblue) lwidth(thin) lpattern(solid)) ///
	(tsline mean_p_kwhVD, color(midblue) lwidth(thin) lpattern(solid)) ///
	(tsline mean_p_kwh4E, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline mean_p_kwh5B, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline mean_p_kwh5E, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline mean_p_kwh4C, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline mean_p_kwh4F, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline mean_p_kwh5C, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline mean_p_kwh5F, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline mean_p_kwhRB, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline mean_p_kwhRE, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline mean_p_kwhVB, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline mean_p_kwhVE, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
    (tsline mean_p_kwh1A, color(midblue*0.25) lwidth(medthick) lpattern(solid)) ///
	(tsline mean_p_kwh1B, color(eltblue) lwidth(medthick) lpattern(solid)) ///
	(tsline mean_p_kwhICE, color(navy) lwidth(medthick) lpattern(solid))  ///
	(tsline mean_p_kwh4A, color(midblue) lwidth(medthick) lpattern(solid)) ///
	(tsline mean_p_kwh4B, color(blue*0.75) lwidth(medthick)  lpattern(solid))  ///
	, ///
	xscale(r(`xmin',`xmax')) xlab(`xmin'(36)`xlabmax', labsize(medlarge)) ///
	xtitle(" ", size(medlarge)) ///
	ylab(0 0.05 0.10 ".10" 0.15 0.20 ".20" 0.25 0.30 ".30",nogrid angle(0) labsize(medlarge)) ytitle("Avg marginal price ($/kWh)", size(medlarge)) ///
	graphr(color(white) lc(white)) ///
	title("Raw marginal prices", size(vlarge) color(black)) ///
	legend(order(19 "AG-1A " 20 "AG-1B " 22 "AG-4A " 23 "AG-4B " 21 "AG-ICE")  col(3) size(medlarge))
graph export "$dirpath/output/marg_price_all_rates_raw_wtitle.eps", replace




twoway  ///
	(tsline resid2_4D, color(midblue) lwidth(thin) lpattern(solid)) ///
	(tsline resid2_5A, color(midblue) lwidth(thin) lpattern(solid)) ///
	(tsline resid2_5D, color(midblue) lwidth(thin) lpattern(solid)) ///
	(tsline resid2_RA, color(midblue) lwidth(thin) lpattern(solid)) ///
	(tsline resid2_RD, color(midblue) lwidth(thin) lpattern(solid)) ///
	(tsline resid2_VA, color(midblue) lwidth(thin) lpattern(solid)) ///
	(tsline resid2_VD, color(midblue) lwidth(thin) lpattern(solid)) ///
	(tsline resid2_4E, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline resid2_5B, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline resid2_5E, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline resid2_4C, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline resid2_4F, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline resid2_5C, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline resid2_5F, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline resid2_RB, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline resid2_RE, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline resid2_VB, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
	(tsline resid2_VE, color(blue*0.75) lwidth(thin)  lpattern(solid)) ///
    (tsline resid2_1A, color(midblue*0.25) lwidth(medthick) lpattern(solid)) ///
	(tsline resid2_1B, color(eltblue) lwidth(medthick) lpattern(solid)) ///
	(tsline resid2_ICE, color(navy) lwidth(medthick) lpattern(solid))  ///
	(tsline resid2_4A, color(midblue) lwidth(medthick) lpattern(solid)) ///
	(tsline resid2_4B, color(blue*0.75) lwidth(medthick)  lpattern(solid))  ///
	, ///
	xscale(r(`xmin',`xmax')) xlab(`xmin'(36)`xlabmax', labsize(medlarge)) ///
	xtitle(" ", size(medlarge)) ///
	ylab(-0.03 -0.02 -0.01 0 0.01 0.02 0.03,nogrid angle(0) labsize(medlarge)) ytitle("Avg marginal price ($/kWh), residuals", size(medlarge)) ///
	graphr(color(white) lc(white)) ///
	title("Residualized marginal prices", size(vlarge) color(black)) ///
	legend(order(19 "AG-1A " 20 "AG-1B " 22 "AG-4A " 23 "AG-4B " 21 "AG-ICE")  col(3) size(medlarge))
graph export "$dirpath/output/marg_price_all_rates_resid_wtitle.eps", replace

