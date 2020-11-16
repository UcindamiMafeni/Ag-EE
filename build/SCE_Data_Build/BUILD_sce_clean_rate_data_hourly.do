*****************************************************************************
**** Script to merge customer bills and interval data into SCE rate data ****
*****************************************************************************

** This script does two things, ONLY for SA-bills that merge into AMI data: 
**      1) Fix the errors in Chin's code to get accurate SCE price schedules
** 		2) Add hourly marginal prices from SCE data to customer billing data
**		3) Add $/kW fixed charges to each bill

clear

global path_rates = "T:/Projects/Pump Data/data/sce_raw/Rates"
global path_temp = "T:/Projects/Pump Data/data/temp"
global path_out = "T:/Projects/Pump Data/data/sce_cleaned"
global path_noaa = "T:/Projects/Pump Data/data/misc/noaa"

*clean up
cd "$path_temp"
local files: dir . files "*.dta"
qui foreach f in `files' {
	erase "`f'"
}

use "$path_out/sce_ag_rates_compiled.dta", clear

***1) fix errors in Chin's code
drop if rateschedule== "TOU-PA-5" & rate_start_date==td(10jul2011)
replace rate_start_date=td(12apr2019) if rateschedule== "PA-2" & rate_start_date==td(01mar2019) & energycharge_sum>.08 | rateschedule== "PA-2" & rate_start_date==td(01mar2019) & energycharge_sum<.035
replace rate_start_date=td(01jun2011) if rateschedule== "PA-1" & rate_start_date==td(01jan2011) & energycharge<.1 & bundled==1 | rateschedule== "PA-1" & rate_start_date==td(01jan2011) & energycharge>.043 & bundled==0
replace rate_start_date=td(01jun2017) if rateschedule== "PA-1" & rate_start_date==td(01jan2017) & wind_mach_credit>0 & bundled==1 | rateschedule== "PA-1" & rate_start_date==td(01jan2017) & energycharge<.048 & bundled==0
drop if rateschedule== "PA-1" & rate_start_date==td(01apr2013) & bundled==1 &  energycharge>.1 | rateschedule== "PA-1" & rate_start_date==td(01apr2013) & bundled==0 &  energycharge>.04
replace rate_start_date=td(01jun2013) if rateschedule== "PA-2" & rate_start_date==td(01apr2013) & bundled==0 & energycharge_sum>.02 | rateschedule== "PA-2" & rate_start_date==td(01apr2013) & energycharge_sum<.098 & bundled==1
drop if rateschedule== "PA-2" & rate_start_date==td(11aug2014) & bundled==0 &  demandcharge_win==. | rateschedule=="PA-2" & rate_start_date==td(11aug2014) & bundled==1 & voltage_dis_load_2>1.101 // | rateschedule== "PA-2" & rate_start_date==td(01jun2014) & bundled==1 & voltage_cat==3 &  voltage_dis_load_2!=1.1 


replace rate_start_date=td(01jun2009) if rateschedule== "PA-RTP" & rate_start_date==td(04apr2009) & bundled==1 & energycharge_dwr>.0622 | rateschedule== "PA-RTP" & rate_start_date==td(04apr2009) & energycharge<.01080 & bundled==0
replace rate_start_date=td(01jun2010) if rateschedule== "PA-RTP" & rate_start_date==td(01may2010) & bundled==0 & energycharge<.018 
sort rateschedule rate_start_date bundled voltage_cat
expand 2 if rateschedule== "PA-RTP" & rate_start_date==td(01may2010) & bundled==1 
sort rateschedule rate_start_date bundled voltage_cat
gen index=_n if rateschedule== "PA-RTP" & rate_start_date==td(01may2010) & bundled==1
bysort rateschedule rate_start_date bundled voltage_cat: egen rank = rank( index), 
replace rate_start_date=td(01jun2010) if rateschedule== "PA-RTP" & rate_start_date==td(01may2010) & bundled==1  & rank==2
cap drop rank index
replace off_peak_credit=0 if rateschedule== "PA-1" & rate_start_date==td(01jan2017)  & bundled==1 | rateschedule== "PA-1" & rate_start_date==td(01jun2017)  & bundled==1 
replace wind_mach_credit=4.09 if rateschedule== "PA-1" & rate_start_date==td(01jun2017)  & bundled==1 | rateschedule== "PA-1" & rate_start_date==td(01jan2017)  & bundled==1
drop if (rateschedule=="TOU-PA-5" | rateschedule=="TOU-PA-SOP") & voltage_cat==0 // voltage category duplicated for this schedule
replace voltage_cat=0 if (rateschedule=="TOU-PA-5" | rateschedule=="TOU-PA-SOP") & bundled==0 //rateschedule=="TOU-PA-5" & voltage_cat==2 | 
expand 2 if (rateschedule=="TOU-PA-5" | rateschedule=="TOU-PA-SOP") & voltage_cat==3 & bundled==1
expand 3 if (rateschedule=="TOU-PA-5" | rateschedule=="TOU-PA-SOP") & bundled==0 //& rate_start_date<td(31dec2009)

sort rateschedule rate_start_date bundled voltage_cat
gen id = _n
bysort rateschedule rate_start_date: gen index = group(id)
bysort rateschedule rate_start_date bundled: egen rank = rank(index)
replace voltage_cat=2 if (rateschedule=="TOU-PA-5") & rank==2 & bundled==1

replace voltage_dis_energy=0.00299 if rateschedule=="TOU-PA-5" & voltage_cat==2 & bundled==1

replace rate_start_date=td(01aug2012) if rateschedule== "TOU-PA-SOP" & bundled==0 & rate_start_date==td(01jun2012) & energycharge_sum_on_pk>0.01721
replace rate_start_date=td(01aug2012) if rateschedule== "TOU-PA-SOP" & bundled==1 & rate_start_date==td(01jun2012) & energycharge_sum_on_pk<0.072

replace voltage_dis_energy=0.00198 if rateschedule== "TOU-PA-5" & voltage_cat==1 & bundled==1 & rate_start_date<td(30sep2009)
replace voltage_dis_energy=voltage_dis_energy[_n+1] if (rateschedule=="TOU-PA-5" | rateschedule=="TOU-PA-SOP") & voltage_cat==2 & bundled==1 & rate_start_date<td(30sep2009)
replace voltage_cat=rank if bundled==0 & (rateschedule=="TOU-PA-5" | rateschedule=="TOU-PA-SOP")
duplicates drop 
cap drop index

gen odd = mod(rank,2) 
replace rate_start_date=td(01apr2013) if rateschedule== "TOU-PA-A" & bundled==0 & rate_start_date==td(12apr2013) & energycharge_sum_on_pk>0.017
replace rate_start_date=td(01apr2013) if rateschedule== "TOU-PA-A" & odd==1 & bundled==1 & rate_start_date==td(12apr2013)
expand 2 if rate_start_date==td(12apr2013) & rateschedule== "TOU-PA-A" & bundled==1
expand 2 if rate_start_date==td(12apr2013) & rateschedule== "TOU-PA-A" & bundled==0

gen index=_n if rate_start_date==td(12apr2013) & rateschedule== "TOU-PA-A" & bundled==0 | rate_start_date==td(12apr2013) & rateschedule== "TOU-PA-A" & bundled==1
bysort rateschedule rate_start_date bundled rank: egen rank_2 = rank(index)
replace rate_start_date=td(01jan2013) if rateschedule== "TOU-PA-A" & rank_2==2 & rate_start_date==td(12apr2013)
replace rate_start_date=td(01jan2012) if rateschedule== "TOU-PA-B" & energycharge_sum_on_pk>=0.0172 & bundled==0 & rate_start_date==td(01jun2012)
replace rate_start_date=td(01jan2012) if rateschedule== "TOU-PA-B" & odd==0 & bundled==1 & rate_start_date==td(01jun2012)

replace rate_start_date=td(01jun2013) if rateschedule== "TOU-PA2A" & bundled==0 & rate_start_date==td(01oct2013) & energycharge_sum_on_pk>0.0203
replace wind_mach_credit=6 if rateschedule== "TOU-PA2A" & bundled==1 & rate_start_date==td(01oct2013)
replace rate_start_date=td(01jun2013) if rateschedule== "TOU-PA2A" & rank<4 & bundled==1 & rate_start_date==td(01oct2013)

cap drop index rank id
sort rateschedule rate_start_date bundled voltage_cat
gen id = _n
bysort rateschedule rate_start_date bundled: egen rank = rank(id)
bysort rateschedule rate_start_date bundled: egen max = max(rank)

replace voltage_cat=rank if rateschedule== "TOU-PA-SOP" | rateschedule== "TOU-PA2A" & rate_start_date>=td(01jun2013) & rate_start_date<=td(01oct2013)
replace voltage_dis_energy=0.00294 if rateschedule=="TOU-PA-SOP" & voltage_cat==2 & bundled==1 & rate_start_date!=td(01oct2012) & rate_start_date>td(01jun2009)
replace voltage_dis_energy=0.00369 if rateschedule=="TOU-PA2A" & voltage_cat==2 & bundled==1 & rate_start_date==td(01jun2013)
replace voltage_dis_energy=0.00373 if rateschedule=="TOU-PA2A" & voltage_cat==3 & bundled==1 & rate_start_date==td(01jun2013)

replace voltage_dis_energy=0.00153 if rateschedule=="TOU-PA2A" & voltage_cat==1 & bundled==1 & rate_start_date==td(01oct2013)
replace voltage_dis_energy=0.00369 if rateschedule=="TOU-PA2A" & voltage_cat==2 & bundled==1 & rate_start_date==td(01oct2013)

*note: TOU-PA2B should be 3 for dates: oct 2013, oct 2016, oct 2017, jul 2014 (actually jun 2014 in 3 schedule) and Jan 2018

drop rank odd rank_2 id rank max

replace rateschedule="TOU-PA3A" if rateschedule =="TOU-PA3B" & (voltage_dis_energy_3!=. | voltage_dis_energy_2 !=. | voltage_dis_energy_1!=.)
replace rateschedule="TOU-PA3B" if rateschedule =="TOU-PA2B" & (voltage_dis_energy_3!=. | voltage_dis_energy_2 !=. | voltage_dis_energy_1!=.)
drop if rateschedule=="TOU-PA3A" & rate_start_date==td(11aug2013)
replace interruptible_credit=0 if interruptible_credit==.
replace ee_charge=0 if ee_charge==.
replace ee_charge= 10.21374 if rateschedule=="TOU-PA-SOP" & voltage_cat==1 & rate_start_date>=td(01mar2009) & rate_start_date<=td(01jun2009)
replace ee_charge= 9.99551 if rateschedule=="TOU-PA-SOP" & voltage_cat==2 & rate_start_date>=td(01mar2009) & rate_start_date<=td(01jun2009)
replace ee_charge= 9.63234 if rateschedule=="TOU-PA-SOP" & voltage_cat==3 & rate_start_date>=td(01mar2009) & rate_start_date<=td(01jun2009)
replace interruptible_credit=0 if interruptible_credit==.
save "$path_temp/sce_ag_rates_compiled_edit.dta", replace
use "$path_temp/sce_ag_rates_compiled_edit.dta", replace

sort rateschedule rate_start_date bundled voltage_cat
keep rateschedule rate_start_date
duplicates drop
bysort rateschedule: gen rate_end_date=rate_start_date[_n+1]-1
format rate_end_date %td
joinby rateschedule rate_start_date using "$path_temp/sce_ag_rates_compiled_edit.dta"
sort rateschedule rate_start_date bundled voltage_cat

cap drop rate_end_date2
generate rate_end_date2 = string(rate_start_date, "%td") if rate_end_date==.
gen rate_end_date3= "31dec"+substr(rate_end_date2, length(rate_end_date2)-3, length(rate_end_date2))
gen rate_end_date4 = date(rate_end_date3, "DMY") if rate_end_date==.
format rate_end_date4 %d
replace rate_end_date= rate_end_date4 if rate_end_date==.
drop rate_end_date2 rate_end_date3 rate_end_date4
save "$path_temp/sce_ag_rates_compiled_edit_2.dta", replace
drop if voltage_dis_energy_3!=. | voltage_dis_energy_2 !=. | voltage_dis_energy_1!=.

*get rid of observations with voltage categories in wide format
generate rate_start_date_st = string(rate_start_date, "%td")
drop if voltage_cat==. // this drops uneccessary TOU-PA2B and TOU-PA3 observations
gen new_ID=rateschedule+rate_start_date_st
sort rateschedule rate_start_date bundled voltage_cat
cap drop odd rank_2 id rank max rate_start_date_st
save "$path_temp/sce_ag_rates_compiled_categories.dta", replace

*reshape wrt voltage category and append with TOU-PA2E

import excel "$path_rates/Excel/2019/TOU-PA2E_2019.xls", sheet("Sheet1") firstrow clear
format rate_start_date %td
format rate_end_date %td
sort rateschedule rate_start_date bundled voltage_cat
replace ee_charge=0 if ee_charge==.
save "$path_temp/sce_ag_rates_compiled_categories_E.dta", replace
keep rateschedule rate_start_date rate_end_date bundled voltage_cat voltage_dis_energy voltage_dis_load voltage_dis_load_1 voltage_dis_load_2 pf_adjust voltage_dis_load_sop_win_weekd voltage_dis_hrly customercharge new_ID ee_charge
keep if bundled==1
reshape wide voltage_dis_energy voltage_dis_load voltage_dis_load_1 voltage_dis_load_2 pf_adjust voltage_dis_hrly customercharge voltage_dis_load_sop_win_weekd ee_charge, i(new_ID)  j(voltage_cat)
save "$path_temp/sce_ag_rates_compiled_bundled_1.dta", replace

use "$path_temp/sce_ag_rates_compiled_categories_E.dta", clear
keep rateschedule rate_start_date rate_end_date bundled voltage_cat voltage_dis_energy voltage_dis_load voltage_dis_load_1 voltage_dis_load_2 pf_adjust voltage_dis_load_sop_win_weekd voltage_dis_hrly customercharge new_ID ee_charge
keep if bundled==0
reshape wide voltage_dis_energy voltage_dis_load voltage_dis_load_1 voltage_dis_load_2 pf_adjust voltage_dis_hrly customercharge voltage_dis_load_sop_win_weekd ee_charge, i(new_ID)  j(voltage_cat)
save "$path_temp/sce_ag_rates_compiled_unbundled_1.dta", replace

use "$path_temp/sce_ag_rates_compiled_categories.dta", replace
keep rateschedule rate_start_date rate_end_date bundled voltage_cat voltage_dis_energy voltage_dis_load voltage_dis_load_1 voltage_dis_load_2 pf_adjust voltage_dis_load_sop_win_weekd voltage_dis_hrly customercharge new_ID ee_charge
keep if bundled==1
reshape wide voltage_dis_energy voltage_dis_load voltage_dis_load_1 voltage_dis_load_2 pf_adjust voltage_dis_hrly customercharge voltage_dis_load_sop_win_weekd ee_charge, i(new_ID)  j(voltage_cat)
append using "$path_temp/sce_ag_rates_compiled_bundled_1.dta"
save "$path_temp/sce_ag_rates_compiled_bundled.dta", replace

use "$path_temp/sce_ag_rates_compiled_categories.dta", clear
keep rateschedule rate_start_date rate_end_date bundled voltage_cat voltage_dis_energy voltage_dis_load voltage_dis_load_1 voltage_dis_load_2 pf_adjust voltage_dis_load_sop_win_weekd voltage_dis_hrly customercharge new_ID ee_charge
keep if bundled==0
reshape wide voltage_dis_energy voltage_dis_load voltage_dis_load_1 voltage_dis_load_2 pf_adjust voltage_dis_hrly customercharge voltage_dis_load_sop_win_weekd ee_charge, i(new_ID)  j(voltage_cat)
append using "$path_temp/sce_ag_rates_compiled_unbundled_1.dta"
save "$path_temp/sce_ag_rates_compiled_unbundled.dta", replace
append using "$path_temp/sce_ag_rates_compiled_bundled.dta"
drop new_ID
order rateschedule rate_start_date rate_end_date bundled
sort rateschedule rate_start_date rate_end_date bundled
foreach i in voltage_dis_load_1 voltage_dis_load_2 pf_adjust voltage_dis_energy voltage_dis_load voltage_dis_hrly customercharge voltage_dis_load_sop_win_weekd ee_charge {
foreach j in 1 2 3 {
rename `i'`j' `i'_`j'
replace `i'_`j'=0 if `i'_`j'==.
}
}
save "$path_temp/sce_ag_rates_compiled_all.dta", replace
*use "$path_temp/sce_ag_rates_compiled_all.dta", replace

use "$path_temp/sce_ag_rates_compiled_edit_2.dta", clear
append using "$path_temp/sce_ag_rates_compiled_categories_E.dta"
drop pf_adjust voltage_dis_hrly customercharge voltage_cat voltage_dis_energy voltage_dis_load voltage_dis_energy_1 voltage_dis_load_1 voltage_dis_load_1_1 voltage_dis_load_2_1 pf_adjust_1 voltage_dis_energy_2 voltage_dis_load_2 voltage_dis_load_1_2 voltage_dis_load_2_2 pf_adjust_2 voltage_dis_energy_3 voltage_dis_load_1_3 voltage_dis_load_2_3 voltage_dis_load_sop_win_weekd
duplicates drop
joinby rateschedule rate_start_date rate_end_date bundled using "$path_temp/sce_ag_rates_compiled_all.dta"
save "$path_temp/sce_ag_rates_compiled_full.dta", replace

use "$path_temp/sce_ag_rates_compiled_edit_2.dta", clear
keep if voltage_dis_energy_3!=. | voltage_dis_energy_2 !=. | voltage_dis_energy_1!=.
gen pf_adjust_3=0
gen voltage_dis_load_3=0
append using "$path_temp/sce_ag_rates_compiled_full.dta"
duplicates drop
sort rateschedule rate_start_date rate_end_date bundled

foreach i in 1 2 3 { 
lab var voltage_dis_load_sop_win_weekd_`i' "Voltage Discount Summer for v category `i', On Peak and Winter Weekdays (4-9pm) Demand (URG - $/kW)"
lab var voltage_dis_hrly_`i' "Voltage Discount for v cateogory `i' (% of Hourly Rates)"
lab var pf_adjust_`i' "Power Factor Adjustment for v cateogory `i' ($/kVAR)"
lab var voltage_dis_energy_`i' "Voltage Discount for v cateogory `i', Energy ($/kWh)"
lab var voltage_dis_load_`i' "Voltage Discount for v cateogory `i', Connected Load ($/hp)"
lab var voltage_dis_load_1_`i' "Voltage Discount for v cateogory `i', Demand ($/kW)- Facilities related"
lab var voltage_dis_load_2_`i' "Voltage Discount for v cateogory `i', Demand ($/kW)- Time related"
}

replace rateschedule="TOU-PA3B" if rateschedule=="TOU-PA2B" & bundled==1 & rate_start_date==td(07jul2014) & energycharge_sum_on_pk< 0.13931
drop if rateschedule=="TOU-PA2B" & bundled==0 & rate_start_date==td(07jul2014) & energycharge_sum_on_pk>0.023081
expand 2 if rateschedule=="TOU-PA3A" & bundled==0 & rate_start_date==td(07jul2014)

gen index=_n if rateschedule=="TOU-PA3A" & bundled==0 & rate_start_date==td(07jul2014)

bysort rateschedule rate_start_date bundled: egen rank = rank(index)
replace rateschedule="TOU-PA3B" if rank==2
drop index rank
sort rateschedule rate_start_date rate_end_date bundle
drop if rateschedule=="TOU-PA-B" & (rate_start_date==td(01jan2012)) & energycharge_dwr_sum_on_pk<.0047 & bundled==1 | rateschedule=="TOU-PA-B" & (rate_start_date==td(01jun2012)) & energycharge_dwr_sum_on_pk>.0047 & bundled==1 | rateschedule=="TOU-PA-A" & (rate_start_date==td(01jan2013) | rate_start_date==td(01apr2013) | rate_start_date==td(12apr2013)) & energycharge_win_m_pk<.06987 & bundled==1|rateschedule=="TOU-PA2B" & (rate_start_date==td(01oct2013)) & energycharge_sum_on_pk>.0203 & bundled==0 | rateschedule=="TOU-PA2B" & (rate_start_date==td(01oct2013)) & energycharge_sum_on_pk<.1 & bundled==1 |rateschedule=="TOU-PA2B" & (rate_start_date==td(01oct2016)) & energycharge_sum_on_pk>.021 & bundled==0 |rateschedule=="TOU-PA2B" & (rate_start_date==td(01oct2016)) & energycharge_sum_on_pk<.09 & bundled==1 |rateschedule=="TOU-PA2B" & (rate_start_date==td(01oct2017)) & energycharge_sum_on_pk>.023 & bundled==0 |rateschedule=="TOU-PA2B" & (rate_start_date==td(01oct2017)) & energycharge_sum_on_pk<.1 & bundled==1 |rateschedule=="TOU-PA3B" & (rate_start_date==td(01jan2015)) & energycharge_sum_on_pk>.2 & bundled==1 
cap drop new_ID rate_start_date_st

save "$path_temp/sce_ag_rates_compiled_clean.dta", replace

use "$path_temp/sce_ag_rates_compiled_clean.dta", replace
cap drop new_ID rate_start_date_st

keep if rateschedule=="TOU-PA2D" & bundled==0
bysort rateschedule rate_start_date bundled: gen rank = _n
generate rate_start_date_st = string(rate_start_date, "%td")
gen new_ID=rateschedule+rate_start_date_st

reshape wide voltage_dis_load_sop, i(new_ID) j(rank)
drop new_ID rate_start_date_st
sort rateschedule rate_start_date rate_end_date bundle
order rateschedule rate_start_date rate_end_date bundled

save "$path_temp/sce_ag_rates_compiled_schedule_D.dta", replace

use "$path_temp/sce_ag_rates_compiled_clean.dta", replace
drop if rateschedule=="TOU-PA2D" & bundled==0
append using "$path_temp/sce_ag_rates_compiled_schedule_D.dta"
duplicates drop
sort rateschedule rate_start_date rate_end_date bundle

gen super=1 if energycharge_win_sup_off_pk!=0 & energycharge_win_sup_off_pk!=. 
replace super=0 if super==.

*fix more unsystematic errors in price schedules

foreach i in energycharge_dwr_sum_on_pk energycharge_dwr_sum_m_pk energycharge_dwr_sum_off_pk energycharge_dwr_win_m_pk energycharge_dwr_win_off_pk {
foreach j in TOU-PA3A TOU-PA3B {
replace `i'=-.00097 if rateschedule=="`j'" & rate_start_date<td(22nov2013) & bundled==1 
replace `i'=-.00095 if rateschedule=="`j'" & rate_start_date==td(22nov2013) & bundled==1 
replace `i'=-.00037 if rateschedule=="`j'" & rate_start_date>=td(01jan2014) & rate_start_date<td(01jan2015)& bundled==1 
replace `i'=-.00172 if rateschedule=="`j'" & rate_start_date>=td(01jan2015) & rate_start_date<td(01jan2016)& bundled==1 
replace `i'=-.00022 if rateschedule=="`j'" & rate_start_date>=td(01jan2016) & rate_start_date<td(01jan2017)& bundled==1 
replace `i'=0 if rateschedule=="`j'" & rate_start_date>=td(01jan2017) & rate_start_date<td(01jan2018)& bundled==1 
replace `i'=0 if rateschedule=="`j'" & rate_start_date>=td(01jan2018) & rate_start_date<td(01jan2019)& bundled==1 
replace `i'=-.00007 if rateschedule=="`j'" & rate_start_date>=td(01jan2019) & rate_start_date<td(01jan2020)& bundled==1 

}
}

foreach i in energycharge_dwr_sum_on_pk energycharge_dwr_sum_m_pk energycharge_dwr_sum_off_pk energycharge_dwr_win_m_pk energycharge_dwr_win_off_pk {
replace `i'=.08451 if rateschedule=="GS-1" & rate_start_date==td(01jan2009) & bundled==1 
replace `i'=.0623 if rateschedule=="GS-1" & rate_start_date==td(01mar2009) & bundled==1 
replace `i'=.06210 if rateschedule=="GS-1" & rate_start_date==td(04apr2009) & bundled==1 
replace `i'=.06225 if rateschedule=="GS-1" & rate_start_date>=td(01jun2009) & rate_start_date<td(01jan2010)& bundled==1 
replace `i'=.03763 if rateschedule=="GS-1" & rate_start_date>=td(01jan2010) & rate_start_date<td(01jan2011)& bundled==1 
replace `i'=.03952 if rateschedule=="GS-1" & rate_start_date>=td(01jan2011) & rate_start_date<td(01jan2012)& bundled==1 
replace `i'=-.00593 if rateschedule=="GS-1" & rate_start_date==td(01jan2012) & bundled==1 
replace `i'=-.00463 if rateschedule=="GS-1" & rate_start_date>td(01jan2012) & rate_start_date<td(01jan2013)& bundled==1 
replace `i'=-.00097 if rateschedule=="GS-1" & rate_start_date>=td(01jan2013) & rate_start_date<td(01jan2014)& bundled==1 
replace `i'=-.00037 if rateschedule=="GS-1" & rate_start_date>=td(01jan2014) & rate_start_date<td(01jan2015)& bundled==1 
replace `i'=-.00172 if rateschedule=="GS-1" & rate_start_date>=td(01jan2015) & rate_start_date<td(01jan2016)& bundled==1 
replace `i'=-.00022 if rateschedule=="GS-1" & rate_start_date>=td(01jan2016) & rate_start_date<td(01jan2017)& bundled==1 
replace `i'=0 if rateschedule=="GS-1" & rate_start_date>=td(01jan2017) & rate_start_date<td(01jan2018)& bundled==1 
replace `i'=0 if rateschedule=="GS-1" & rate_start_date>=td(01jan2018) & rate_start_date<td(01jan2019)& bundled==1 
replace `i'=-.00007 if rateschedule=="GS-1" & rate_start_date>=td(01jan2019) & rate_start_date<td(01jan2020)& bundled==1 
}

foreach i in energycharge_dwr_win_sup_off_pk energycharge_dwr_sum_sup_off_pk energycharge_dwr_sum_on_pk energycharge_dwr_sum_off_pk energycharge_dwr_win_off_pk {
replace `i'=-.00097 if (rateschedule=="TOU-PA2-SOP-1" | rateschedule=="TOU-PA3-SOP-2" | rateschedule=="TOU-PA3-SOP") & rate_start_date<td(22nov2013)& bundled==1 
replace `i'=-.00095 if (rateschedule=="TOU-PA2-SOP-1" | rateschedule=="TOU-PA3-SOP-2" | rateschedule=="TOU-PA3-SOP") & rate_start_date==td(22nov2013) & bundled==1 
replace `i'=-.00037 if (rateschedule=="TOU-PA2-SOP-1" | rateschedule=="TOU-PA3-SOP-2" | rateschedule=="TOU-PA3-SOP") & rate_start_date>=td(01jan2014) & rate_start_date<td(01jan2015)& bundled==1 
replace `i'=-.00172 if (rateschedule=="TOU-PA2-SOP-1" | rateschedule=="TOU-PA3-SOP-2" | rateschedule=="TOU-PA3-SOP") & rate_start_date>=td(01jan2015) & rate_start_date<td(01jan2016)& bundled==1 
replace `i'=-.00022 if (rateschedule=="TOU-PA2-SOP-1" | rateschedule=="TOU-PA3-SOP-2" | rateschedule=="TOU-PA3-SOP") & rate_start_date>=td(01jan2016) & rate_start_date<td(01jan2017)& bundled==1 
replace `i'=0 if (rateschedule=="TOU-PA2-SOP-1" | rateschedule=="TOU-PA3-SOP-2" | rateschedule=="TOU-PA3-SOP") & rate_start_date>=td(01jan2017) & rate_start_date<td(01jan2018)& bundled==1 
replace `i'=0 if (rateschedule=="TOU-PA2-SOP-1" | rateschedule=="TOU-PA3-SOP-2" | rateschedule=="TOU-PA3-SOP") & rate_start_date>=td(01jan2018) & rate_start_date<td(01jan2019)& bundled==1 
replace `i'=-.00007 if (rateschedule=="TOU-PA2-SOP-1" | rateschedule=="TOU-PA3-SOP-2" | rateschedule=="TOU-PA3-SOP") & rate_start_date>=td(01jan2019) & rate_start_date<td(01jan2020)& bundled==1 
}

***********
*Calculate marginal Prices
***********

***********
*find most disaggregated version of charges
***********

replace energycharge_sum_on_pk=energycharge_sum if energycharge_sum!=0 &  energycharge_sum!=.
replace energycharge_sum_on_pk=energycharge if energycharge!=0 &  energycharge!=.

replace demandcharge_sum_on_pk=demandcharge_sum if demandcharge_sum!=0 &  demandcharge_sum!=.
replace demandcharge_sum_on_pk=demandcharge if demandcharge!=0 &  demandcharge!=.

replace demandcharge_win_on_pk=demandcharge_win if demandcharge_win!=0 &  demandcharge_win!=.
replace demandcharge_win_on_pk=demandcharge if demandcharge!=0 &  demandcharge!=.

foreach i in sum win {
replace energycharge_`i'_m_pk=energycharge_`i' if energycharge_`i'!=. & energycharge_`i'!=0
replace energycharge_`i'_m_pk=energycharge if energycharge!=. & energycharge!=0
replace energycharge_`i'_off_pk=energycharge_`i' if energycharge_`i'!=. & energycharge_`i'!=0
replace energycharge_`i'_off_pk=energycharge if energycharge!=. & energycharge!=0
replace energycharge_`i'_sup_off_pk=energycharge_`i' if energycharge_`i'!=. & energycharge_`i'!=0
replace energycharge_`i'_sup_off_pk=energycharge if energycharge!=. & energycharge!=0
cap replace demandcharge_`i'=demandcharge if demandcharge!=. & demandcharge!=0
cap replace demandcharge_`i'_m_pk=demandcharge_`i' if demandcharge_`i'!=. & demandcharge_`i'!=0
cap replace demandcharge_`i'_m_pk=demandcharge if demandcharge!=. & demandcharge!=0
cap replace demandcharge_`i'_mid_pk=demandcharge_`i' if demandcharge_`i'!=. & demandcharge_`i'!=0
cap replace demandcharge_`i'_mid_pk=demandcharge if demandcharge!=. & demandcharge!=0
replace demandcharge_`i'_off_pk=demandcharge_`i' if demandcharge_`i'!=. & demandcharge_`i'!=0
replace demandcharge_`i'_off_pk=demandcharge if demandcharge!=. & demandcharge!=0
replace demandcharge_`i'_sup_off_pk=demandcharge_`i' if demandcharge_`i'!=. & demandcharge_`i'!=0
replace demandcharge_`i'_sup_off_pk=demandcharge if demandcharge!=. & demandcharge!=0
}

replace energycharge_dwr_sum_on_pk=energycharge_dwr_sum if energycharge_dwr_sum!=0 &  energycharge_dwr_sum!=.
replace energycharge_dwr_sum_on_pk=energycharge_dwr if energycharge_dwr!=0 &  energycharge_dwr!=.

foreach i in sum win {
replace energycharge_dwr_`i'_m_pk=energycharge_dwr_`i' if energycharge_dwr_`i'!=. & energycharge_dwr_`i'!=0
replace energycharge_dwr_`i'_m_pk=energycharge_dwr if energycharge_dwr!=. & energycharge_dwr!=0
replace energycharge_dwr_`i'_off_pk=energycharge_dwr_`i' if energycharge_dwr_`i'!=. & energycharge_dwr_`i'!=0
replace energycharge_dwr_`i'_off_pk=energycharge_dwr if energycharge_dwr!=. & energycharge_dwr!=0
}

rename voltage_dis_load_sop_win_weekd_1 voltage_weekd_1
rename voltage_dis_load_sop_win_weekd_2 voltage_weekd_2
rename voltage_dis_load_sop_win_weekd_3 voltage_weekd_3

foreach i in demandcharge demandcharge_sum demandcharge_win demandcharge_sum_on_pk demandcharge_sum_mid_pk demandcharge_sum_off_pk ///
demandcharge_win_m_pk demandcharge_win_off_pk demandcharge_win_sup_off_pk demandcharge_sum_sup_off_pk demandcharge_win_on_pk voltage_dis_load_1_1 ///
voltage_dis_load_1_2 voltage_dis_load_1_3 voltage_dis_load_2_1 voltage_dis_load_2_2 voltage_dis_load_2_3 demandcharge_win_mid_pk customercharge ///
customercharge_1 customercharge_2 customercharge_3 voltage_dis_load_1 voltage_dis_load_2 voltage_dis_load_3 pf_adjust_1 pf_adjust_2 pf_adjust_3 ///
voltage_weekd_1 voltage_weekd_2 voltage_weekd_3 interruptible_credit minimum_charge_sum minimum_charge_sum{
replace `i'=0 if `i'==.
gen `i'_un= `i' if bundled==0
bysort rateschedule rate_start_date: egen max = max(`i'_un)
drop `i'_un
rename max `i'_un
gen fin_`i'=`i'+`i'_un if bundled==1
replace fin_`i'=`i' if bundled==0
}

foreach i in energycharge_sum_on_pk energycharge_sum_m_pk energycharge_sum_off_pk energycharge_win_m_pk energycharge_win_off_pk energycharge_win_sup_off_pk energycharge_sum_sup_off_pk {
gen `i'_un= `i' if bundled==0
bysort rateschedule rate_start_date: egen max = max(`i'_un)
drop `i'_un
rename max `i'_un
}

foreach i in energycharge_dwr_sum_on_pk energycharge_dwr_sum_m_pk energycharge_dwr_sum_off_pk energycharge_dwr_sum_sup_off_pk energycharge_dwr_win_m_pk energycharge_dwr_win_off_pk energycharge_dwr_win_sup_off_pk {
replace `i'=-`i' if bundled==1 & rate_start_date>td(31dec2011)
replace `i'=0 if bundled==0
}


replace voltage_dis_energy_1=cal_alternative_discount if voltage_dis_energy_1==. & rateschedule=="GS-1"
replace cal_alternative_discount=. if rateschedule=="GS-1" & cal_alternative_discount<1 & bundled==1
replace cal_climate_credit=0 if cal_climate_credit==.

foreach i in 1 2 3 {
replace voltage_dis_energy_`i'=-voltage_dis_energy_`i'
}

*Calculate marginal prices: add charges and subtract credits.

foreach i in sum_on_pk sum_m_pk sum_off_pk win_m_pk win_off_pk {
foreach j in 1 2 3 {
gen final_`i'_`j'=energycharge_`i'+energycharge_dwr_`i'+voltage_dis_energy_`j'+energycharge_`i'_un-cal_climate_credit-fin_interruptible_credit if bundled==1
replace final_`i'_`j'=energycharge_`i'+energycharge_dwr_`i'+voltage_dis_energy_`j'-cal_climate_credit-fin_interruptible_credit if bundled==0
}
}


foreach i in  win_sup_off_pk {
foreach j in 1 2 3 {
gen final_`i'_`j'=energycharge_`i'+energycharge_dwr_`i'+voltage_dis_energy_`j'+energycharge_`i'_un-cal_climate_credit if bundled==1 & super==1
replace final_`i'_`j'=energycharge_`i'+energycharge_dwr_`i'+voltage_dis_energy_`j'-cal_climate_credit if bundled==0 & super==1
}
}

duplicates drop

foreach i in 1 2 3 {
lab var final_sum_on_pk_`i' "Marginal price summer on peak assuming voltage category `i' "
lab var final_sum_m_pk_`i' "Marginal price summer mid peak assuming voltage category `i' "
lab var final_sum_off_pk_`i' "Marginal price summer off peak assuming voltage category `i' "
lab var final_win_m_pk_`i' "Marginal price winter mid peak assuming voltage category `i' "
lab var final_win_off_pk_`i' "Marginal price winter off peak assuming voltage category `i' "
lab var final_win_sup_off_pk_`i' "Marginal price winter super off peak assuming voltage category `i' "

}

keep super rateschedule rate_start_date rate_end_date bundled fin_demandcharge fin_demandcharge_sum fin_demandcharge_win /// 
fin_demandcharge_sum_on_pk fin_demandcharge_sum_mid_pk fin_demandcharge_sum_off_pk fin_demandcharge_win_m_pk ///
fin_demandcharge_win_off_pk fin_demandcharge_win_sup_off_pk fin_demandcharge_sum_sup_off_pk fin_demandcharge_win_on_pk ///
fin_voltage_dis_load_1_1 fin_voltage_dis_load_1_2 fin_voltage_dis_load_1_3 fin_voltage_dis_load_2_1 ///
fin_voltage_dis_load_2_2 fin_voltage_dis_load_2_3 fin_demandcharge_win_mid_pk fin_customercharge fin_customercharge_1 ///
fin_customercharge_2 fin_customercharge_3 final_sum_on_pk_1 final_sum_on_pk_2 final_sum_on_pk_3 final_sum_m_pk_1 ///
final_sum_m_pk_2 final_sum_m_pk_3 final_sum_off_pk_1 final_sum_off_pk_2 final_sum_off_pk_3 final_win_m_pk_1 ///
final_win_m_pk_2 final_win_m_pk_3 final_win_off_pk_1 final_win_off_pk_2 final_win_off_pk_3 final_win_sup_off_pk_1 final_win_sup_off_pk_2 final_win_sup_off_pk_3 /// final_sum_sup_off_pk_1 /// final_sum_sup_off_pk_2 final_sum_sup_off_pk_3
fin_voltage_dis_load_1 fin_voltage_dis_load_2 fin_voltage_dis_load_3 fin_pf_adjust_1 fin_pf_adjust_2 fin_pf_adjust_3 ///
ee_charge_1 ee_charge_2 ee_charge_3 fin_interruptible_credit fin_voltage_weekd_2 fin_voltage_weekd_3 fin_voltage_weekd_1 ///
tou_option_meter_charge tou_RTEM_meter_charge servicecharge off_peak_credit wind_mach_credit fin_minimum_charge_sum fin_minimum_charge_win

save "$path_temp/Marginal_prices_batch_edit_1.dta", replace

use "$path_temp/Marginal_prices_batch_edit_1.dta", replace


*1) find the number of days between start and end date
keep rateschedule rate_end_date rate_start_date
duplicates drop rateschedule rate_end_date rate_start_date, force
gen between = rate_end_date - rate_start_date +1
expand between
sort rateschedule rate_start_date
bysort rateschedule rate_start_date rate_end_date: gen add_day=_n-1
gen date=rate_start_date+add_day
format date %td
*2) generate days of the week
gen dow_num = dow(date)
lab def dow_num 0 "Sunday" 1 "Monday" 2 "Tuesday" 3 "Wednesday" 4 "Thursday" 5 "Friday" 6 "Saturday"
label values dow_num
label values dow_num dow_num
drop between add_day
gen yearly_date = year(date)
gen monthly_date = month(date)
gen daily_date = day(date)

gen season="summer" if monthly_date>=6 & monthly_date<=9
replace season="winter" if season==""
duplicates drop

****generate public holidays

gen holiday = 0

	// New Year's Day
replace holiday = 1 if date==td(01jan2009)
replace holiday = 1 if date==td(01jan2010)
replace holiday = 1 if date==td(31jan2010)
replace holiday = 1 if date==td(02jan2012)
replace holiday = 1 if date==td(01jan2013)
replace holiday = 1 if date==td(01jan2014)
replace holiday = 1 if date==td(01jan2015)
replace holiday = 1 if date==td(01jan2016)
replace holiday = 1 if date==td(02jan2017)
replace holiday = 1 if date==td(01jan2018)
replace holiday = 1 if date==td(01jan2019)


	// President's Day
replace holiday = 1 if date==td(16feb2009)
replace holiday = 1 if date==td(15feb2010)
replace holiday = 1 if date==td(21feb2011)
replace holiday = 1 if date==td(20feb2012)
replace holiday = 1 if date==td(18feb2013)
replace holiday = 1 if date==td(17feb2014)
replace holiday = 1 if date==td(16feb2015)
replace holiday = 1 if date==td(15feb2016)
replace holiday = 1 if date==td(20feb2017)
replace holiday = 1 if date==td(19feb2018)
replace holiday = 1 if date==td(18feb2019)

	// Memorial Day
replace holiday = 1 if date==td(25may2009)
replace holiday = 1 if date==td(31may2010)
replace holiday = 1 if date==td(30may2011)
replace holiday = 1 if date==td(28may2012)
replace holiday = 1 if date==td(27may2013)
replace holiday = 1 if date==td(26may2014)
replace holiday = 1 if date==td(25may2015)
replace holiday = 1 if date==td(30may2016)
replace holiday = 1 if date==td(29may2017)
replace holiday = 1 if date==td(28may2018)
replace holiday = 1 if date==td(27may2019)

	// Independence Day
replace holiday = 1 if date==td(03jul2009)
replace holiday = 1 if date==td(05jul2010)
replace holiday = 1 if date==td(04jul2011)
replace holiday = 1 if date==td(04jul2012)
replace holiday = 1 if date==td(04jul2013)
replace holiday = 1 if date==td(04jul2014)
replace holiday = 1 if date==td(03jul2015)
replace holiday = 1 if date==td(04jul2016)
replace holiday = 1 if date==td(04jul2017)
replace holiday = 1 if date==td(04jul2018)
replace holiday = 1 if date==td(04jul2019)

	// Labor Day
replace holiday = 1 if date==td(07sep2009)
replace holiday = 1 if date==td(06sep2010)
replace holiday = 1 if date==td(05sep2011)
replace holiday = 1 if date==td(03sep2012)
replace holiday = 1 if date==td(02sep2013)
replace holiday = 1 if date==td(01sep2014)
replace holiday = 1 if date==td(07sep2015)
replace holiday = 1 if date==td(05sep2016)
replace holiday = 1 if date==td(04sep2017)
replace holiday = 1 if date==td(03sep2018)
replace holiday = 1 if date==td(02sep2019)


	// Veteran's Day
replace holiday = 1 if date==td(11nov2009)
replace holiday = 1 if date==td(11nov2010)
replace holiday = 1 if date==td(11nov2011)
replace holiday = 1 if date==td(11nov2012)
replace holiday = 1 if date==td(11nov2013)
replace holiday = 1 if date==td(11nov2014)
replace holiday = 1 if date==td(11nov2015)
replace holiday = 1 if date==td(11nov2016)
replace holiday = 1 if date==td(10nov2017)
replace holiday = 1 if date==td(12nov2018)
replace holiday = 1 if date==td(11nov2019)

	// Thanksgiving Day
replace holiday = 1 if date==td(26nov2009)
replace holiday = 1 if date==td(25nov2010)
replace holiday = 1 if date==td(24nov2011)
replace holiday = 1 if date==td(22nov2012)
replace holiday = 1 if date==td(28nov2013)
replace holiday = 1 if date==td(27nov2014)
replace holiday = 1 if date==td(26nov2015)
replace holiday = 1 if date==td(24nov2016)
replace holiday = 1 if date==td(23nov2017)
replace holiday = 1 if date==td(22nov2018)
replace holiday = 1 if date==td(28nov2019)

	// Christmas Day
replace holiday = 1 if date==td(25dec2009)
replace holiday = 1 if date==td(24dec2010)
replace holiday = 1 if date==td(26dec2011)
replace holiday = 1 if date==td(25dec2012)
replace holiday = 1 if date==td(25dec2013)
replace holiday = 1 if date==td(25dec2014)
replace holiday = 1 if date==td(25dec2015)
replace holiday = 1 if date==td(26dec2016)
replace holiday = 1 if date==td(25dec2017)
replace holiday = 1 if date==td(25dec2018)
replace holiday = 1 if date==td(25dec2019)
duplicates drop date rateschedule, force

expand 24
sort rateschedule date
bysort rateschedule date: gen hour=_n-1
joinby rateschedule rate_end_date rate_start_date using "$path_temp/Marginal_prices_batch_edit_1.dta"
sort rateschedule rate_start_date rate_end_date date dow_num season hour bundled

save "$path_temp/Marginal_prices_batch_edit_2.dta", replace

use "$path_temp/Marginal_prices_batch_edit_2.dta", replace

*define applicable hours by season by category
duplicates drop
*For Schedules GS-1, PA-1, PA-2, TOU-PA-B, TOU-PA-A and TOU-PA-5 the definitions of summer and winter, and for on, mid and off peak are taken from TOU-PA-5
*Note: definitions for on, mid, and off peak as well as holidays are identical for TOU-PA-5, TOU-PA-2 and TOU-PA-3. so we assume them for TOU-PA, which isn't stated explicitly until 2011 (identical in 2011 and thereafter)
*On-Peak: Noon to 6:00 p.m. summer weekdays except holidays.
*Mid-Peak: 8:00 a.m. to noon and 6:00 p.m. to 11:00 p.m. summer weekdays except holidays. 8:00 a.m. to 9:00 p.m. winter weekdays except holidays. 
*Off-Peak: All other hours. 

*for schedules with super off peak times, the schedules are as follows:
*On-Peak: 1:00 p.m. to 5:00 p.m. summer weekdays except holidays
*Super Off-Peak: midnight to 6:00 a.m. all year, everyday
*Off-Peak: All other hours - all year, everyday 

*for schedule TOU-PA2D:
*On-Peak: 4 p.m. - 9 p.m. for summer weekdays only
*Mid-Peak: 4 p.m. - 9 p.m. for winter weekdays, and all weekends in both summer and winter (including holidays)
*Off-Peak: All other hours for summer weekdays besides on peak and all other hours for summer weekends besides mid-peak, and 9 p.m. - 8 a.m for all winter days. 
*Super-Off-Peak: 8 a.m. - 4 p.m. all winter
gen weekday=1 if dow_num>=1 & dow_num<=5

foreach i in 1 2 3 {
****Summer
*on Peak
gen Marginal_price_`i'=final_sum_on_pk_`i' if season=="summer" & weekday==1 & hour>=12 & hour<18
*mid Peak
replace Marginal_price_`i'=final_sum_m_pk_`i' if season=="summer" & weekday==1 & (hour>=8 & hour<12 | hour>=18 & hour<23 )
*off Peak
replace Marginal_price_`i'=final_sum_off_pk_`i' if season=="summer" & weekday==1 & (hour<8 | hour==23) | season=="summer" & weekday==0 | season=="summer" & holiday==1


****Winter
*mid Peak
replace Marginal_price_`i'=final_win_m_pk_`i' if season=="winter" & weekday==1 & (hour>=8 & hour<21)
*off Peak
replace Marginal_price_`i'=final_win_off_pk_`i' if season=="winter" & weekday==1 & (hour>=21 | hour<8) | season=="winter" & weekday==0 | season=="winter" & holiday==1

****Super off peak schedules

****Summer
*on Peak
replace Marginal_price_`i'=final_sum_on_pk_`i' if season=="summer" & holiday==0 & weekday==1 & hour>=13 & hour<17 & holiday==0 & super==1
*super off Peak
*replace Marginal_price_`i'=final_sum_sup_off_pk_`i' if (hour>=0 & hour<6) & season=="summer" & super==1
*off Peak
replace Marginal_price_`i'=final_sum_off_pk_`i' if Marginal_price_`i'==. & super==1 & season=="summer"

****Winter
*super off Peak
replace Marginal_price_`i'=final_win_sup_off_pk_`i' if (hour>=0 & hour<6) & season=="winter" & super==1
*off Peak
replace Marginal_price_`i'=final_win_off_pk_`i' if Marginal_price_`i'==. & super==1 & season=="winter"


****TOU-PA2D

**Summer
*on Peak
replace Marginal_price_`i'=final_sum_on_pk_`i' if season=="summer" & weekday==1 & hour>=16 & hour<21 & holiday==0 & rateschedule=="TOU-PA2D"
*mid Peak
replace Marginal_price_`i'=final_sum_m_pk_`i' if hour>=16 & hour<21 & season=="summer" & (weekday==0 | holiday==1) & rateschedule=="TOU-PA2D"
*off Peak
replace Marginal_price_`i'=final_sum_off_pk_`i' if season=="summer" & Marginal_price_`i'==. & rateschedule=="TOU-PA2D"


**Winter
*mid Peak
replace Marginal_price_`i'=final_win_m_pk_`i' if hour>=16 & hour<21 & season=="winter" & rateschedule=="TOU-PA2D"
*off Peak
replace Marginal_price_`i'=final_win_off_pk_`i' if season=="winter" & hour>=21 & hour<8 & rateschedule=="TOU-PA2D"
*super off Peak
replace Marginal_price_`i'=final_win_sup_off_pk_`i' if season=="winter" & hour>=8 & hour<16 & rateschedule=="TOU-PA2D"

lab var Marginal_price_`i' "Marginal Price assuming Voltage Category `i'"
}
*do the same for demand charges

****Summer
*on Peak
gen demandcharge=fin_demandcharge_sum_on_pk if season=="summer" & weekday==1 & hour>=12 & hour<18
*mid Peak
replace demandcharge=fin_demandcharge_sum_on_pk if season=="summer" & weekday==1 & (hour>=8 & hour<12 | hour>=18 & hour<23 )
*off Peak
replace demandcharge=fin_demandcharge_sum_off_pk if season=="summer" & weekday==1 & (hour<8 | hour==23) | season=="summer" & weekday==0 | season=="summer" & holiday==1


****Winter
*mid Peak
replace demandcharge=fin_demandcharge_win_m_pk if season=="winter" & weekday==1 & (hour>=8 & hour<21)
*off Peak
replace demandcharge=fin_demandcharge_win_off_pk if season=="winter" & weekday==1 & (hour>=21 | hour<8) | season=="winter" & weekday==0 | season=="winter" & holiday==1

****Super off peak schedules

****Summer
*on Peak
replace demandcharge=fin_demandcharge_sum_on_pk if season=="summer" & holiday==0 & weekday==1 & hour>=13 & hour<17 & holiday==0 & super==1
*super off Peak
*replace demandcharg=fin_demandcharge_sum_sup_off_pk if (hour>=0 & hour<6) & season=="summer" & super==1
*off Peak
replace demandcharge=fin_demandcharge_sum_off_pk if demandcharge==. & super==1 & season=="summer"

****Winter
*super off Peak
replace demandcharge=fin_demandcharge_win_sup_off_pk if (hour>=0 & hour<6) & season=="winter" & super==1
*off Peak
replace demandcharge=fin_demandcharge_win_off_pk if demandcharge==. & super==1 & season=="winter"


****TOU-PA2D

**Summer
*on Peak
replace demandcharge=fin_demandcharge_sum_on_pk  if season=="summer" & weekday==1 & hour>=16 & hour<21 & holiday==0 & rateschedule=="TOU-PA2D"
*mid Peak
replace demandcharge=fin_demandcharge_sum_on_pk if hour>=16 & hour<21 & season=="summer" & (weekday==0 | holiday==1) & rateschedule=="TOU-PA2D"
*off Peak
replace demandcharge=fin_demandcharge_sum_off_pk if season=="summer" & demandcharge==. & rateschedule=="TOU-PA2D"


**Winter
*mid Peaks
replace demandcharge=fin_demandcharge_win_m_pk if hour>=16 & hour<21 & season=="winter" & rateschedule=="TOU-PA2D"
*off Peak
replace demandcharge=fin_demandcharge_win_off_pk if season=="winter" & hour>=21 & hour<8 & rateschedule=="TOU-PA2D"
*super off Peak
replace demandcharge=fin_demandcharge_win_sup_off_pk if season=="winter" & hour>=8 & hour<16 & rateschedule=="TOU-PA2D"

lab var demandcharge "Demand Charge - $/kW of Billing Demand/Meter/Month"

foreach i in voltage_dis_load_1_1 voltage_dis_load_1_2 voltage_dis_load_1_3 minimum_charge_sum  ///
voltage_dis_load_2_1 voltage_dis_load_2_2 voltage_dis_load_2_3 customercharge_3 minimum_charge_sum ///
customercharge_2 customercharge_1 pf_adjust_1 pf_adjust_2 pf_adjust_3 voltage_dis_load_1 ///
voltage_dis_load_2 voltage_dis_load_3 voltage_weekd_1 voltage_weekd_3 voltage_weekd_2 interruptible_credit{
rename fin_`i' `i'
}

foreach i in 1 2 3 { 
lab var voltage_weekd_`i' "Voltage Discount Summer for v category `i', On Peak and Winter Weekdays (4-9pm) Demand (URG - $/kW)"
lab var pf_adjust_`i' "Power Factor Adjustment for v cateogory `i' ($/kVAR)"
lab var voltage_dis_load_`i' "Voltage Discount for v cateogory `i', Connected Load ($/hp)"
lab var voltage_dis_load_1_`i' "Voltage Discount for v cateogory `i', Demand ($/kW)- Facilities related"
lab var voltage_dis_load_2_`i' "Voltage Discount for v cateogory `i', Demand ($/kW)- Time related"
lab var ee_charge_`i' "Excess Energy Charge for v cateogory `i', $/kWh/Meter/Month"

}
drop customercharge_2 customercharge_3
rename customercharge_1 customercharge
la var minimum_charge_sum "Minimum Charge Summer - $/kW"
la var minimum_charge_win "Minimum Charge Winter - $/kW"
la var wind_mach_credit "Wind Machine Credit- $/hp"
la var interruptible_credit "Interruptible Credit- $/kWh"
la var customercharge "Customer Charge - $/Meter/Month"

keep rateschedule date bundled Marginal_price_1 Marginal_price_2 Marginal_price_3 demandcharge ///
dow_num season hour voltage_dis_load_1_1 voltage_dis_load_1_2 voltage_dis_load_1_3  ///
voltage_dis_load_2_1 voltage_dis_load_2_2 voltage_dis_load_2_3 customercharge ///
 pf_adjust_1 pf_adjust_2 pf_adjust_3 voltage_dis_load_1 minimum_charge_sum minimum_charge_sum ///
voltage_dis_load_2 voltage_dis_load_3 ee_charge_1 ee_charge_2 ee_charge_3 interruptible_credit ///
voltage_weekd_1 voltage_weekd_2 voltage_weekd_3 tou_option_meter_charge tou_RTEM_meter_charge ///
servicecharge off_peak_credit wind_mach_credit minimum_charge_sum minimum_charge_win

duplicates drop

save "$path_temp/Marginal_prices_batch_1.dta", replace
use "$path_temp/Marginal_prices_batch_1.dta", replace

*Deal with RTP: as these prices vary hourly and will be dealt with separately

foreach j in 2009 2010 2011 2012 2013 {
import excel "$path_rates/RTP Excel//`j'.xlsx", sheet("Sheet1") clear
drop A
foreach i in B C D E F G H I J{
egen `i'_2 = sieve(`i'), keep(numeric)
gen `i'_3= "0."+substr(`i'_2, 2, length(`i'_2))
drop `i'_2 `i'
rename `i'_3 `i'
destring `i', replace
}
bysort K: gen hour=_n
replace hour=0 if hour==24
drop if K==.
rename B e_hot_sum_wkday
rename C v_hot_sum_wkday
rename D hot_sum_wkday
rename E mod_sum_wkday
rename F mild_sum_wkday
rename G high_win_wkday
rename H low_win_wkday
rename I high_wkend
rename J low_wkend
rename K Month_count
gen year=`j'
gen rateschedule="PA-RTP"
gen bundled=1
la var e_hot_sum_wkday "Extremely hot summer weekday (>=95)"
la var v_hot_sum_wkday "Very hot summer weekday (91-94)"
la var hot_sum_wkday "Hot summer weekday (85-90)"
la var mod_sum_wkday "Moderate summer weekday (81-84)"
la var mild_sum_wkday "Mild summer weekday (80>=)"
la var high_win_wkday "High cost winter weekday (>90)"
la var low_win_wkday "Low cost winter weekday (90>=)"
la var high_wkend "High cost weekend (>=78)"
la var low_wkend "Low cost weekend (<78)"
save "$path_temp//`j' RTP.dta", replace
}

use "$path_temp/2009 RTP.dta", replace
foreach j in 2010 2011 2012 2013 {
append using "$path_temp//`j' RTP.dta"
}

gen rate_start_date=td(01oct2009) if year==2009 & Month_count==1
replace rate_start_date=td(01jun2009) if year==2009 & Month_count==2
replace rate_start_date=td(04apr2009) if year==2009 & Month_count==3
replace rate_start_date=td(01mar2009) if year==2009 & Month_count==4
replace rate_start_date=td(01mar2010) if year==2010 & Month_count==1
replace rate_start_date=td(01jun2011) if year==2011 & Month_count==1
replace rate_start_date=td(03mar2011) if year==2011 & Month_count==2
replace rate_start_date=td(21oct2012) if year==2012 & Month_count==1
replace rate_start_date=td(01aug2012) if year==2012 & Month_count==2
replace rate_start_date=td(01jun2012) if year==2012 & Month_count==3
replace rate_start_date=td(12apr2013) if year==2013 & Month_count==1
replace rate_start_date=td(01jan2013) if year==2013 & Month_count==2
sort rate_start_date hour
format rate_start_date %td
gen rate_end_date=rate_start_date[_n+24]-1

cap drop rate_end_date2
generate rate_end_date2 = string(rate_start_date, "%td") if rate_end_date==.
gen rate_end_date3= "31dec"+substr(rate_end_date2, length(rate_end_date2)-3, length(rate_end_date2))
gen rate_end_date4 = date(rate_end_date3, "DMY") if rate_end_date==.
format rate_end_date4 %td
replace rate_end_date= rate_end_date4 if rate_end_date==.
drop rate_end_date2 rate_end_date3 rate_end_date4
format rate_end_date %td
save "$path_temp/RTP temp.dta", replace

use "$path_temp/RTP temp.dta", replace

keep rateschedule rate_end_date rate_start_date
duplicates drop
gen between = rate_end_date - rate_start_date +1
expand between
gen obs=1
sort rateschedule rate_start_date
bysort rateschedule rate_start_date rate_end_date: gen add_day = sum(obs)
replace add_day=add_day-1
gen date=rate_start_date+add_day
format date %td
*generate days of the week
gen dow_num = dow(date)
lab def dow_num 0 "Sunday" 1 "Monday" 2 "Tuesday" 3 "Wednesday" 4 "Thursday" 5 "Friday" 6 "Saturday"
label values dow_num
label values dow_num dow_num
drop between add_day
gen yearly_date = year(date)
gen monthly_date = month(date)
gen daily_date = day(date)
gen season="summer" if monthly_date>=6 & monthly_date<=9
replace season="winter" if season==""
drop monthly_date daily_date yearly_date
order rateschedule rate_start_date rate_end_date date
sort date dow_num season
joinby rateschedule rate_end_date rate_start_date using "$path_temp/RTP temp.dta"
order rate_start_date rate_end_date date hour
sort date hour
save "$path_temp/RTP.dta", replace
use "$path_temp/RTP.dta", replace

*Merge marginal prices with temperature data
import delimited "$path_noaa/Temperature Data.csv", varnames(1) clear 
keep date tmax
gen date2 = date( date , "YMD")
format date2 %td
drop date
rename date2 date
replace date=date+1
format date %td
save "$path_temp/Temperature Data.dta", replace  

use "$path_temp/sce_ag_rates_compiled_clean.dta", replace

keep if rateschedule=="PA-RTP"

foreach i in energycharge {
gen `i'_un= `i' if bundled==0
bysort rateschedule rate_start_date: egen max = max(`i'_un)
drop `i'_un
rename max `i'_un
}
keep if bundled==1
replace cal_climate_credit=0 if cal_climate_credit==.
keep rate_start_date rate_end_date energycharge_un energycharge_dwr voltage_dis_hrly_1 voltage_dis_hrly_2 voltage_dis_hrly_3 cal_climate_credit
duplicates drop 

replace voltage_dis_hrly_1=2.38 if rate_start_date==td(01jan2009)
replace voltage_dis_hrly_2=5.19 if rate_start_date==td(01jan2009)
replace voltage_dis_hrly_3=5.19 if rate_start_date==td(01jan2009)

gen between = rate_end_date - rate_start_date +1
expand between
gen obs=1
sort rate_start_date
bysort rate_start_date rate_end_date: gen add_day = sum(obs)
replace add_day=add_day-1
gen date=rate_start_date+add_day
format date %td
drop between add_day
expand 24
bysort date: gen hour = sum(obs)
replace hour=hour-1
drop obs
order rate_start_date rate_end_date date hour
sort date hour
joinby date hour using "$path_temp/RTP.dta"

replace energycharge_dwr=-energycharge_dwr if rate_start_date>td(31dec2011)

*Generate marginal prices

foreach i in  e_hot_sum_wkday v_hot_sum_wkday hot_sum_wkday mod_sum_wkday mild_sum_wkday high_win_wkday low_win_wkday high_wkend low_wkend {
foreach j in 1 2 3 {
gen final_`i'_`j'=`i'+energycharge_dwr-(voltage_dis_hrly_`j'*0.01*`i')+energycharge_un-cal_climate_credit
bysort date: egen final_`i'_`j'_1=mean(final_`i'_`j')
drop final_`i'_`j'
rename final_`i'_`j'_1 final_`i'_`j'
}
}
gen weekday=1 if dow_num>=1 & dow_num<=5
replace weekday=0 if weekday==.
joinby date using "$path_temp/Temperature Data.dta"
foreach i in 1 2 3 {
gen Marginal_price_`i'=.
replace Marginal_price_`i'=final_e_hot_sum_wkday_`i' if Marginal_price_`i'==. & final_e_hot_sum_wkday_`i'!=. & weekday==1 & season=="summer" & tmax>=95 
replace Marginal_price_`i'=final_v_hot_sum_wkday_`i' if Marginal_price_`i'==. & final_v_hot_sum_wkday_`i'!=. & weekday==1 & season=="summer" & tmax>=91 & tmax<=94 
replace Marginal_price_`i'=final_hot_sum_wkday_`i' if Marginal_price_`i'==. & final_hot_sum_wkday_`i'!=. & weekday==1 & season=="summer" & tmax>=85 & tmax<=90 
replace Marginal_price_`i'=final_mod_sum_wkday_`i' if Marginal_price_`i'==. & final_mod_sum_wkday_`i'!=. & weekday==1 & season=="summer" & tmax>=81 & tmax<=84 
replace Marginal_price_`i'=final_mild_sum_wkday_`i' if Marginal_price_`i'==. & final_mild_sum_wkday_`i'!=. & weekday==1 & season=="summer" & tmax<=80 
replace Marginal_price_`i'=final_high_win_wkday_`i' if Marginal_price_`i'==. & final_high_win_wkday_`i'!=. & weekday==1 & season=="winter" & tmax>90 
replace Marginal_price_`i'=final_low_win_wkday_`i' if Marginal_price_`i'==. & final_low_win_wkday_`i'!=. & weekday==1 & season=="winter" & tmax<=90 
replace Marginal_price_`i'=final_high_wkend_`i' if Marginal_price_`i'==. & final_high_wkend_`i'!=. & weekday==0 & tmax>=78 
replace Marginal_price_`i'=final_low_wkend_`i' if Marginal_price_`i'==. & final_high_wkend_`i'!=. & weekday==0 & tmax<78 
}

* e_hot_sum_wkday "Extremely hot summer weekday (>=95)"
*v_hot_sum_wkday "Very hot summer weekday (91-94)"
*hot_sum_wkday "Hot summer weekday (85-90)"
*mod_sum_wkday "Moderate summer weekday (81-84)"
*mild_sum_wkday "Mild summer weekday (80>=)"
*high_win_wkday "High cost winter weekday (>90)"
*low_win_wkday "Low cost winter weekday (90>=)"
*high_wkend "High cost weekend (>=78)"
*low_wkend "Low cost weekend (<78)"

order rateschedule date Marginal_price_1 Marginal_price_2 Marginal_price_3
keep date Marginal_price_1 Marginal_price_2 Marginal_price_3 rateschedule dow_num season hour bundled
duplicates drop 
save "$path_temp/Marginal_prices_batch_2.dta", replace
use "$path_temp/Marginal_prices_batch_1.dta", replace
keep if rateschedule=="PA-RTP" & bundled==1
drop Marginal_price_1 Marginal_price_2 Marginal_price_3
joinby date rateschedule dow_num season hour bundled using "$path_temp/Marginal_prices_batch_2.dta"
save "$path_temp/Marginal_prices_batch_2_2.dta", replace
use "$path_temp/Marginal_prices_batch_1.dta", replace
drop if rateschedule=="PA-RTP" & bundled==1
append using "$path_temp/Marginal_prices_batch_2_2.dta"
sort rateschedule date bundled
sort rateschedule date hour bundled 
foreach i in servicecharge off_peak_credit tou_option_meter_charge wind_mach_credit tou_RTEM_meter_charge ///
 minimum_charge_sum minimum_charge_win ee_charge_1 ee_charge_2 ee_charge_3 {
replace `i'=0 if `i'==.
}
save "$path_out/marginal_prices_hourly_11112020.dta", replace

*clean up
cd "$path_temp"
local files: dir . files "*.dta"
qui foreach f in `files' {
	erase "`f'"
}




