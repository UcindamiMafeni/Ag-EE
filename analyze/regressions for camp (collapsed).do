

clear all
version 13
set more off

*******************************************************************
**** Script to redo camp regressions using collapsed hourly data **
*******************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

************************************************
************************************************
 
use "$dirpath_data/merged/sp_hour_panel_apep2_CAMP.dta", clear 
gen fwt = 1
drop if log_kwh==.

collapse (sum) fwt (mean) log_kwh, by(log_p sp_id modate hour year month cz log_p_peak log_p_peak_summer peak summer) fast
*gen log_kwh = ln(kwh)
sum fwt, detail

di c(current_time)
reghdfe log_kwh log_p [fw=fwt], absorb(sp_id modate hour) vce(cluster sp_id modate)
di c(current_time)

di c(current_time)
reghdfe log_kwh log_p [fw=fwt], absorb(sp_id##month##hour modate hour) vce(cluster sp_id modate)
di c(current_time)

reghdfe log_kwh log_p, absorb(sp_id##month##hour sp_id##c.year modate hour) vce(cluster sp_id modate)

reghdfe log_kwh log_p, absorb(sp_id##month##hour sp_id##year modate##cz hour) vce(cluster sp_id modate)
reghdfe log_kwh log_p log_p_peak, absorb(sp_id##month##hour modate##cz hour) vce(cluster sp_id modate)
reghdfe log_kwh log_p log_p_peak_summer, absorb(sp_id##month##hour modate##cz hour) vce(cluster sp_id modate)
reghdfe log_kwh c.log_p#i.hour, absorb(sp_id##month##hour modate##cz hour) vce(cluster sp_id modate)


************************************************
************************************************
 
use "$dirpath_data/merged/sp_hour_panel_apep2_CAMP.dta", clear 
keep if substr(sp_uuid,2,1)=="5"
unique sp_uuid
*gen fwt = 1
*drop if log_kwh==.

gen log_wh = ln(1000*kwh)
gen ihs_kwh =  ln(kwh + sqrt(kwh^2+1))
gen ihs_wh =  ln(1000*kwh + sqrt((1000*kwh)^2+1))
gen log_mwh =  ln(1000000*kwh)
gen ihs_mwh =  ln(1000000*kwh + sqrt((1000000*kwh)^2+1))

twoway (scatter log_kwh kwh if abs(kwh)<5, msize(tiny) mcolor(blue)) ///
 (scatter ihs_kwh kwh if abs(kwh)<5, msize(tiny) mcolor(red))

twoway (scatter log_wh kwh if abs(kwh)<5, msize(tiny) mcolor(blue)) ///
 (scatter ihs_wh kwh if abs(kwh)<5, msize(tiny) mcolor(red))
 
twoway (scatter log_kwh kwh if abs(kwh)<5, msize(tiny) mcolor(blue)) ///
 (scatter ihs_kwh kwh if abs(kwh)<5, msize(tiny) mcolor(red)) ///
 (scatter log_wh kwh if abs(kwh)<5, msize(tiny) mcolor(green)) ///
 (scatter ihs_wh kwh if abs(kwh)<5, msize(tiny) mcolor(orange))

twoway (scatter log_kwh kwh if inrange(kwh,-0.2,0.5), msize(tiny) mcolor(blue)) ///
 (scatter ihs_kwh kwh if inrange(kwh,-0.2,0.5), msize(tiny) mcolor(red)) ///
 (scatter log_wh kwh if inrange(kwh,-0.2,0.5), msize(tiny) mcolor(green)) ///
 (scatter ihs_wh kwh if inrange(kwh,-0.2,0.5), msize(tiny) mcolor(orange)) ///
 (scatter log_mwh kwh if inrange(kwh,-0.2,0.5), msize(tiny) mcolor(black)) ///
 (scatter ihs_mwh kwh if inrange(kwh,-0.2,0.5), msize(tiny) mcolor(magenta))
 
 
reghdfe log_kwh log_p, absorb(sp_id modate hour) vce(cluster sp_id modate)
reghdfe log_wh log_p, absorb(sp_id modate hour) vce(cluster sp_id modate)
reghdfe ihs_kwh log_p, absorb(sp_id modate hour) vce(cluster sp_id modate)
reghdfe ihs_wh log_p, absorb(sp_id modate hour) vce(cluster sp_id modate)
reghdfe ihs_mwh log_p, absorb(sp_id modate hour) vce(cluster sp_id modate)

reghdfe log_kwh log_p, absorb(sp_id##month##hour modate##cz hour) vce(cluster sp_id modate)
reghdfe ihs_mwh log_p if kwh>0, absorb(sp_id##month##hour modate##cz hour) vce(cluster sp_id modate)
reghdfe ihs_mwh log_p if kwh>=0, absorb(sp_id##month##hour modate##cz hour) vce(cluster sp_id modate)

drop if kwh<0
gen fwt = 1
collapse (sum) fwt (mean) ihs_mwh, by(log_p sp_id modate hour year month cz log_p_peak log_p_peak_summer peak summer) fast
reghdfe ihs_mwh log_p [fw=fwt], absorb(sp_id##month##hour modate##cz hour) vce(cluster sp_id modate)

****************
****************

use "$dirpath_data/merged/sp_hour_panel_apep2_CAMP.dta", clear 
gen fwt = 1
drop if kwh<0
gen ihs_mwh =  ln(1000000*kwh + sqrt((1000000*kwh)^2+1))

reghdfe log_kwh log_p, absorb(sp_id##month##hour modate##cz hour) vce(cluster sp_id modate)
reghdfe ihs_mwh log_p if kwh>0, absorb(sp_id##month##hour modate##cz hour) vce(cluster sp_id modate)
reghdfe ihs_mwh log_p if kwh>=0, absorb(sp_id##month##hour modate##cz hour) vce(cluster sp_id modate)

collapse (sum) fwt (mean) ihs_mwh, by(log_p sp_id modate hour year month cz log_p_peak log_p_peak_summer peak summer) fast
reghdfe ihs_mwh log_p [fw=fwt], absorb(sp_id##month##hour modate##cz hour) vce(cluster sp_id modate)
reghdfe ihs_mwh log_p [fw=fwt], absorb(sp_id##month##hour modate##cz sp_id##c.year) vce(cluster sp_id modate)
