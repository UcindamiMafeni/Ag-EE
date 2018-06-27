
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
 
 
use "$dirpath_data/merged/sp_month_panel_apep.dta", clear 

egen sp_id = group(sp_uuid)
gen log_kwh = log(mnth_bill_kwh)
gen log_pmean = log(mean_p_kwh)
gen log_pmax = log(max_p_kwh)
gen log_pmin = log(min_p_kwh)
egen cz = group(climate_zone_cd)

gen month = real(substr(string(modate,"%tm"),6,2))
gen year =  real(substr(string(modate,"%tm"),1,4))

reghdfe log_kwh log_pmean, absorb(sp_id modate) vce(cluster sp_id modate)
reghdfe log_kwh log_pmean, absorb(i.sp_id##i.month modate) vce(cluster sp_id modate)
reghdfe log_kwh log_pmean, absorb(i.sp_id##i.month i.sp_id##i.year modate) vce(cluster sp_id modate)
reghdfe log_kwh log_pmean, absorb(i.sp_id##i.month i.sp_id##i.year i.cz##i.modate) vce(cluster sp_id modate)

*reghdfe log_kwh log_pmax, absorb(sp_id modate) vce(cluster sp_id modate)
*reghdfe log_kwh log_pmax, absorb(i.sp_id##i.month modate) vce(cluster sp_id modate)
*reghdfe log_kwh log_pmax, absorb(i.sp_id##i.month i.sp_id##i.year modate) vce(cluster sp_id modate)
reghdfe log_kwh log_pmax, absorb(i.sp_id##i.month i.sp_id##i.year i.cz##modate) vce(cluster sp_id modate)

*reghdfe log_kwh log_pmin, absorb(sp_id modate) vce(cluster sp_id modate)
*reghdfe log_kwh log_pmin, absorb(i.sp_id##i.month modate) vce(cluster sp_id modate)
*reghdfe log_kwh log_pmin, absorb(i.sp_id##i.month i.sp_id##i.year modate) vce(cluster sp_id modate)
reghdfe log_kwh log_pmin, absorb(i.sp_id##i.month i.sp_id##i.year i.cz##modate) vce(cluster sp_id modate)

reghdfe log_kwh log_pmin log_pmax, absorb(i.sp_id##i.month i.sp_id##i.year i.cz##modate) vce(cluster sp_id modate)



use "$dirpath_data/merged/sp_hour_panel_apep.dta", clear 

egen sp_id = group(sp_uuid)
gen log_kwh = log(kwh)
gen log_p = log(p_kwh)
gen month = month(date)
gen year =  year(date)
gen modate = ym(year,month)
gen peak = inrange(hour,12,17)
gen summer = inrange(month,5,10)
gen log_p_peak = log_p*peak
gen log_p_peak_summer = log_p*peak*summer

merge m:1 sp_uuid modate using "$dirpath_data/merged/sp_month_panel_apep.dta", keep(3) keepusing(climate_zone_cd)
egen cz = group(climate_zone_cd)
drop climate_zone_cd 

save "$dirpath_data/merged/sp_hour_panel_apep2.dta", replace


use "$dirpath_data/merged/sp_hour_panel_apep2.dta", clear 

reghdfe log_kwh log_p, absorb(sp_id modate hour) vce(cluster sp_id modate)
reghdfe log_kwh log_p, absorb(sp_id##month##hour modate hour) vce(cluster sp_id modate)
reghdfe log_kwh log_p, absorb(sp_id##month##hour sp_id##c.year modate hour) vce(cluster sp_id modate)

reghdfe log_kwh log_p, absorb(sp_id##month##hour sp_id##year modate##cz hour) vce(cluster sp_id modate)
reghdfe log_kwh log_p log_p_peak, absorb(sp_id##month##hour modate##cz hour) vce(cluster sp_id modate)
reghdfe log_kwh log_p log_p_peak_summer, absorb(sp_id##month##hour modate##cz hour) vce(cluster sp_id modate)
reghdfe log_kwh c.log_p#i.hour, absorb(sp_id##month##hour modate##cz hour) vce(cluster sp_id modate)


egen double log_p_max = max(log_p), by(sp_id date)
reghdfe log_kwh log_p log_p_max log_p_peak_summer, absorb(sp_id##month##hour modate##cz hour) vce(cluster sp_id modate)

collapse (mean) log_kwh log_p log_p_peak_summer log_p_max, by(sp_id month hour modate cz) fast
gen summer = inrange(month,5,10)
gen log_p_summer = log_p*summer
gen log_p_winter = log_p*(1-summer)
reghdfe log_kwh c.log_p_winter#i.hour c.log_p_summer#i.hour, absorb(sp_id##month##hour modate##cz hour) vce(cluster sp_id modate)

gen hr = .
gen seas = ""
gen beta = .
gen se = .
local count = 1
foreach sea in winter summer {
	forvalues h = 0/23 {
		replace hr = `h' in `count'
		replace seas = "`sea'" in `count'
		replace beta = _b[c.log_p_`sea'#`h'.hour] in `count'
		replace se = _se[c.log_p_`sea'#`h'.hour] in `count'
		local count = `count'+1
	}
}

local t = abs(invt(e(df_r),0.025))
gen lci = beta - `t'*se
gen uci = beta + `t'*se

keep hr seas beta se lci uci
dropmiss, obs force
compress
save "$dirpath/output/hourly_regs_season.dta", replace

use kwh date hour using "$dirpath_data/merged/sp_hour_panel_apep2.dta", clear 
gen summer = inrange(month(date),5,10)
collapse (count) days=date (sum) kwh, by(summer hour) fast
gen seas = "winter"
replace seas = "summer" if summer==1
drop summer
save "$dirpath/output/hourly_kwh_season.dta", replace

