

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

****************
****************

qui use "$dirpath_data/merged/sp_hour_panel_apep2_CAMP.dta", clear 
qui gen fwt = 1
qui drop if kwh<0
qui gen ihs_mwh =  ln(1000000*kwh + sqrt((1000000*kwh)^2+1))
di c(current_time)
reghdfe ihs_mwh log_p, absorb(sp_id##month##hour modate##cz hour) vce(cluster sp_id modate)
di c(current_time)
describe

qui use "$dirpath_data/merged/sp_hour_panel_apep2_CAMP.dta", clear 
qui gen fwt = 1
qui drop if kwh<0
qui gen ihs_mwh =  ln(1000000*kwh + sqrt((1000000*kwh)^2+1))
qui collapse (sum) fwt (mean) ihs_mwh, by(log_p sp_id modate hour year month cz) fast
di c(current_time)
reghdfe ihs_mwh log_p [fw=fwt], absorb(sp_id##month##hour modate##cz hour) vce(cluster sp_id modate)
di c(current_time)
describe

qui use "$dirpath_data/merged/sp_hour_panel_apep2_CAMP.dta", clear 
qui gen fwt = 1
qui drop if kwh<0
qui gen ihs_mwh =  ln(1000000*kwh + sqrt((1000000*kwh)^2+1))
qui collapse (sum) fwt (mean) ihs_mwh log_p, by(sp_id modate hour year month cz) fast
di c(current_time)
reghdfe ihs_mwh log_p [fw=fwt], absorb(sp_id##month##hour modate##cz hour) vce(cluster sp_id modate)
di c(current_time)
describe
*************************
*************************

use "$dirpath_data/merged/sp_month_elec_panel.dta", clear

di c(current_time)
reghdfe ihs_kwh c.log_p_mean#i.summer if pull=="20180719", absorb(sp_group#month modate) vce(cluster sp_group modate)
di c(current_time)
ivreghdfe ihs_kwh (log_p_mean = log_m*_p_kwh_ag_default) if pull=="20180719", absorb(sp_group#month modate) cluster(sp_group modate)
di c(current_time)

ivreghdfe ihs_kwh (log_p_mean = c.log_mean_p_kwh_e20#rt_group) if pull=="20180719", absorb(sp_group#month modate) cluster(sp_group modate)
levelsof rt_group, local(levs)
foreach rt in `levs'{
	gen log_mean_p_kwh_e20_rt`rt' = 0
	replace log_mean_p_kwh_e20_rt`rt' = log_mean_p_kwh_e20 if rt_group==`rt'
}
ivreghdfe ihs_kwh (log_p_mean = log_mean_p_kwh_e20_rt*) if pull=="20180719", ///
	absorb(sp_group#month modate) cluster(sp_group modate)
ivreghdfe ihs_kwh (log_p_mean = log_mean_p_kwh_e20_rt1 log_mean_p_kwh_e20_rt2) ///
	if pull=="20180719" & inlist(rt_group,1,2,3), ///
	absorb(sp_group#month modate) cluster(sp_group modate)
ivreghdfe ihs_kwh (log_p_mean = log_mean_p_kwh_e20_rt1-log_mean_p_kwh_e20_rt22) ///
	if pull=="20180719", absorb(sp_group#month modate) cluster(sp_group modate)
ivreghdfe ihs_kwh (log_p_mean = log_mean_p_kwh_e20_rt3-log_mean_p_kwh_e20_rt20) ///
	if pull=="20180719", absorb(sp_group#month modate) cluster(sp_group modate)

reghdfe ihs_kwh log_p_mean if pull=="20180719", absorb(sp_group#month modate) vce(cluster sp_group modate)
reghdfe ihs_kwh log_p_mean ctrl_fxn_logs if pull=="20180719", absorb(sp_group#month modate) vce(cluster sp_group modate)
reghdfe ihs_kwh log_p_mean ctrl_fxn_logs ctrl_fxn_levs if pull=="20180719", absorb(sp_group#month modate) vce(cluster sp_group modate)
	
reghdfe ihs_kwh log_p_mean if pull=="20180719", absorb(sp_group#month sp_group#year modate) vce(cluster sp_group modate)
reghdfe ihs_kwh log_p_mean ctrl_fxn_logs if pull=="20180719", absorb(sp_group#month sp_group#year modate) vce(cluster sp_group modate)
reghdfe ihs_kwh log_p_mean ctrl_fxn_logs ctrl_fxn_levs if pull=="20180719", absorb(sp_group#month sp_group#year modate) vce(cluster sp_group modate)
	

ivreghdfe ihs_kwh (log_p_mean = log_mean_p_kwh_init) if pull=="20180719", ///
	absorb(sp_group#month modate) cluster(sp_group modate)
ivreghdfe ihs_kwh (log_p_mean = log_m*_p_kwh_init) if pull=="20180719", ///
	absorb(sp_group#month modate) cluster(sp_group modate)

ivreghdfe ihs_kwh (log_p_mean = log_mean_p_kwh_init) if pull=="20180719", ///
	absorb(sp_group#month sp_group#year modate) cluster(sp_group modate)
ivreghdfe ihs_kwh (log_p_mean = log_m*_p_kwh_init) if pull=="20180719", ///
	absorb(sp_group#month sp_group#year modate) cluster(sp_group modate)	
	
ivreghdfe ihs_kwh (log_p_mean = log_mean_p_kwh_init) if pull=="20180719", ///
	absorb(sp_group#month wdist_group#year modate) cluster(sp_group modate)
ivreghdfe ihs_kwh (log_p_mean = log_m*_p_kwh_init) if pull=="20180719", ///
	absorb(sp_group#month wdist_group#year modate) cluster(sp_group modate)		
	
ivreghdfe ihs_kwh (log_p_mean = log_mean_p_kwh_init) if pull=="20180719" & summer==1, ///
	absorb(sp_group modate) cluster(sp_group modate)
ivreghdfe ihs_kwh (log_p_mean = log_m*_p_kwh_init) if pull=="20180719", ///
	absorb(sp_group modate) cluster(sp_group modate)		
	
	
gen sp_same_rate_group = .
replace sp_same_rate_group = 0 if sp_same_rate_group==. & sp_same_rate_dumbsmart==1
replace sp_same_rate_group = 1 if sp_same_rate_group==. & sp_same_rate_dumbsmart==0 & ///
	sp_same_rate_in_cat==1
replace sp_same_rate_group = 2 if sp_same_rate_group==. & sp_same_rate_dumbsmart==0 & ///
	sp_same_rate_in_cat==0
assert sp_same_rate_group!=.
egen temp_tag = tag(sp_uuid)
tab sp_same_rate_group if temp_tag
tab sp_same_rate_group if temp_tag & pull=="20180719"

reghdfe ihs_kwh c.log_p_mean#i.sp_same_rate_group if pull=="20180719", ///
	absorb(sp_group#month sp_group#year modate) vce(cluster sp_group modate)

reghdfe ihs_kwh c.log_p_mean#i.sp_same_rate_group#i.summer if pull=="20180719", ///
	absorb(sp_group#month##c.gw_qtr_bsn_mean2 modate#sp_same_rate_group) vce(cluster sp_group modate)

reghdfe ihs_kwh c.log_p_mean#i.sp_same_rate_group#i.summer if pull=="20180719", ///
	absorb(sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year modate#sp_same_rate_group) vce(cluster sp_group modate)	
	
reghdfe ihs_kwh c.log_p_mean#i.sp_same_rate_group#i.summer if pull=="20180719", ///
	absorb(sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year modate#sp_same_rate_group) vce(cluster sp_group modate)
	
reghdfe ihs_kwh c.log_p_mean#i.sp_same_rate_group#i.summer if pull=="20180719", ///
	absorb(sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year#sp_same_rate_group ///
	wdist_group#year#sp_same_rate_group modate#sp_same_rate_group) vce(cluster sp_group modate)
	
	
ivreghdfe ihs_kwh ( log_p_mean = log_p_m*_lag* ) if pull=="20180719" & sp_same_rate_group==1,  ///
	absorb(sp_group#month modate) cluster(sp_group modate)

	
reghdfe ihs_kwh c.log_p_mean#i.summer if pull=="20180719" & sp_same_rate_group==0, ///
	absorb(sp_group#month modate) vce(cluster sp_group modate)	
reghdfe ihs_kwh c.log_p_mean#i.summer if pull=="20180719" & sp_same_rate_group==0, ///
	absorb(sp_group#month basin_group#year modate) vce(cluster sp_group modate)	
reghdfe ihs_kwh c.log_p_mean#i.summer if pull=="20180719" & sp_same_rate_group==0, ///
	absorb(sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year modate) vce(cluster sp_group modate)
reghdfe ihs_kwh c.log_p_mean#i.summer if pull=="20180719" & sp_same_rate_group==0, ///
	absorb(sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year modate) vce(cluster sp_group modate)
	
	
reghdfe ihs_kwh c.log_p_mean#i.rt_group if pull=="20180719", ///
	absorb(sp_group#month modate) vce(cluster sp_group modate)	
reghdfe ihs_kwh c.log_p_mean#i.rt_group if pull=="20180719", ///
	absorb(sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year modate) vce(cluster sp_group modate)	


gen ln_gw = ln(gw_qtr_bsn_mean2)
reghdfe ihs_kwh log_p_mean if pull=="20180719" & sp_same_rate_group==0,  ///
	absorb(sp_group#month##c.ln_gw basin_group#year wdist_group#year modate) cluster(sp_group modate)
ivreghdfe ihs_kwh ( log_p_mean = log_p_m*_lag* ) if pull=="20180719" & sp_same_rate_group==1,  ///
	absorb(sp_group#month##c.ln_gw basin_group#year wdist_group#year modate) cluster(sp_group modate)
ivreghdfe ihs_kwh ( log_p_mean = log_m*_p_kwh_ag_default ) if pull=="20180719" & sp_same_rate_group==2,  ///
	absorb(sp_group#month##c.ln_gw basin_group#year wdist_group#year modate) cluster(sp_group modate)

reghdfe ihs_kwh log_p_mean if pull=="20180719" & sp_same_rate_group==0,  ///
	absorb(sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year sp_group#c.modate modate) cluster(sp_group modate)
	
	
ivreghdfe ihs_kwh ( log_p_mean = log_p_m*_lag* ) if pull=="20180719" & sp_same_rate_group==1,  ///
	absorb(sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year sp_group#c.modate modate) cluster(sp_group modate)

ivreghdfe ihs_kwh ( log_p_mean = log_m*_p_kwh_ag_default ) if pull=="20180719" & sp_same_rate_group==2,  ///
	absorb(sp_group#month##c.gw_qtr_bsn_mean2 basin_group#year wdist_group#year sp_group#c.modate modate) cluster(sp_group modate)
ivreghdfe ihs_kwh ( log_p_mean = log_m*_p_kwh_ag_default ) if pull=="20180719" & sp_same_rate_group==2,  ///
	absorb(sp_group#month##c.gw_qtr_bsn_mean2 sp_group#year modate) cluster(sp_group modate)
	
	
*************************
*************************

use "$dirpath_data/merged/sp_hourly_elec_panel_20180719.dta", clear
drop if kwh<0
gen log_p = ln(p_kwh)
gen ihs_kwh =  ln(1000000*kwh + sqrt((1000000*kwh)^2+1))
gen modate = ym(year(date), month(date))
format %tm modate
gen year = year(date)
gen month = month(date)
egen sp_group = group(sp_uuid)
reghdfe ihs_kwh log_p, absorb(sp_group#month#hour modate) vce(cluster sp_group modate)

use "$dirpath_data/merged/sp_hourly_elec_panel_collapsed_20180719.dta", clear
reghdfe ihs_kwh log_p [fw=fwt], absorb(sp_group#month#hour modate) vce(cluster sp_group modate)



	