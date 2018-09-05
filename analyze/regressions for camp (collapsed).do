

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
 
use "$dirpath_data/merged/sp_hour_panel_apep2.dta", clear 
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
