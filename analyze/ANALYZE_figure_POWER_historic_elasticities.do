
clear all
set more off

**********************************************************
**** Script to make POWER historic elasticities chart ****
**********************************************************

global path "/Users/louispreonas/Dropbox/Documents/Research/Energy Water Project/slides/figures"

************************************************
************************************************

** Copy-paste in papers from Google sheet

replace authors = subinstr(authors," and "," & ",1)
drop if elast_min==.
drop title
drop context
sort poweryear

replace timestep = "Day" if timestep=="Hourly" | timestep=="Half-hour" | timestep=="Hour"
replace timestep = "Day" if timestep=="Peak events"
replace timestep = "Month" if timestep=="Monthly"

replace customertype = "Res" if customertype=="Residential"

replace elast_min = abs(elast_min)
replace elast_max = abs(elast_max)

cap drop label
gen label = authors + " (" + string(poweryear) + ")"

cap drop id*
gen id_min = _n-1
gen id_max = id_min + 0.4
compress

forvalues i = 1/10 {
	local lab`i' = label[`i']
}
twoway ///
	   (bar elast_min id_min if cust=="Res" & time=="Day" , hor barw(0.4) base(-0.01) color(blue*0.4) lw(none)) ///
       (bar elast_max id_max if cust=="Res" & time=="Day" , hor barw(0.4) base(-0.01) color(blue*0.4) lw(none)) ///
	   (bar elast_min id_min if cust=="Res" & time=="Month", hor barw(0.4) base(-0.01) color(blue*0.9) lw(none)) ///
       (bar elast_max id_max if cust=="Res" & time=="Month", hor barw(0.4) base(-0.01) color(blue*0.9) lw(none)) ///
	   (bar elast_min id_min if cust=="Res" & time=="Year" , hor barw(0.4) base(-0.01) color(blue*1.7) lw(none)) ///
       (bar elast_max id_max if cust=="Res" & time=="Year" , hor barw(0.4) base(-0.01) color(blue*1.7) lw(none)) ///
	   (bar elast_min id_min if cust!="Res" & time=="Day" , hor barw(0.4) base(-0.01) color(orange*0.5) lw(none)) ///
       (bar elast_max id_max if cust!="Res" & time=="Day" , hor barw(0.4) base(-0.01) color(orange*0.5) lw(none)) ///
	   (bar elast_min id_min if cust!="Res" & time=="Month", hor barw(0.4) base(-0.01) color(orange*1) lw(none)) ///
       (bar elast_max id_max if cust!="Res" & time=="Month", hor barw(0.4) base(-0.01) color(orange*1) lw(none)) ///
	   (scatteri  0.25 0.25 (3) "`lab1'" , color(black)  msymbol(i) mlabcolor(black) mlabsize(medium)) ///
	   (scatteri  1.25 0.00 (3) "`lab2'" , color(black)  msymbol(i) mlabcolor(black) mlabsize(medium)) ///
	   (scatteri  2.25 0.262 (3) "`lab3'" , color(black)  msymbol(i) mlabcolor(black) mlabsize(medium)) ///
	   (scatteri  3.25 0.14 (3) "`lab4'" , color(black)  msymbol(i) mlabcolor(black) mlabsize(medium)) ///
	   (scatteri  4.25 0.25 (3) "`lab5'" , color(black)  msymbol(i) mlabcolor(black) mlabsize(medium)) ///
	   (scatteri  5.25 0.29 (3) "`lab6'" , color(black)  msymbol(i) mlabcolor(black) mlabsize(medium)) ///
	   (scatteri  6.25 0.056 (3) "`lab7'" , color(black)  msymbol(i) mlabcolor(black) mlabsize(medium)) ///
	   (scatteri  7.25 0.47 (3) "`lab8'" , color(black)  msymbol(i) mlabcolor(black) mlabsize(medium)) ///
	   (scatteri  8.25 0.29 (3) "`lab9'" , color(black)  msymbol(i) mlabcolor(black) mlabsize(medium)) ///
	   (scatteri  9.25 1.17 (9) "`lab10'", color(black)  msymbol(i) mlabcolor(black) mlabsize(medium)) ///
	   , graphregion(color(white)) plotregion(fcolor(white)) ylabel("")  ///
	   xlabel(0 0.25 0.5 0.75 1, labsize(medium)) xtitle("Demand Elasticity Estimates", size(medium)) ///
	   legend(order(1 "Resid, hour/day  " 3 "Resid, month  " 5 "Resid, year  " 7 "C&I, hour/day  "  9 "C&I, month  ")  ///
	   col(3) size(medium))
graph export "$path/power_archives_bargraph.eps", replace
	   

help twoway
