clear all
version 13
set more off

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"


local tag = "20201123"

local tag = "20201222"
use "$dirpath_data/merged_sce/bills_constructed_final_`tag'.dta", replace
duplicates drop sa_uuid bill_start_dt, force
gen year = year(bill_start_dt)
gen month = month(bill_start_dt)

gen diff=total_bill_amount_constr-total_bill_amount
la var diff "Contrusted bill amount - Actual bill amount"
gen diff_prop=diff/ total_bill_amount
la var diff_prop "Contrusted bill amount - Actual bill amount as proportion of true bill"
xtile pctile_diff_prop = diff_prop , nq(100)
xtile pctile_diff = diff , nq(100)

local tag = "20201222"

save "$dirpath_data/merged_sce/bills_constructed_regs_`tag'.dta", replace


local tag = "20201222"
*local tag = "20201123"

use "$dirpath_data/merged_sce/bills_constructed_regs_`tag'.dta", replace
cap drop _merge
merge 1:m sa_uuid bill_start_dt using "T:\Projects\Pump Data\data\merged_sce\bills_demand_response_20201222.dta"
drop _merge
tab prog_name_current
gen dr_ind_current=0 if  prog_name_current=="NEVER ON DR" |  prog_name_current=="NO LONGER ON DR"
replace dr_ind_current=1 if dr_ind_current==.
bysort sa_uuid bill_start_dt: egen dr_ind_current2=max( dr_ind_current )
drop dr_ind_current
rename dr_ind_current2 dr_ind_current
bysort sa_uuid bill_start_dt: gen keep=_n
tab keep
keep if keep==1
drop keep
*Preliminary regression analysus

reghdfe diff dr_ind, a(tariff_sched_text month year) vce(cluster sa_uuid)
reghdfe diff dr_ind total_bill_kwh i.dr_ind#c.total_bill_kwh, a(tariff_sched_text month year) vce(cluster sa_uuid)
reghdfe diff dr_ind monthly_max_kw i.dr_ind#c.monthly_max_kw, a(tariff_sched_text month year) vce(cluster sa_uuid)
reghdfe diff dr_ind total_bill_kwh monthly_max_kw i.dr_ind#c.total_bill_kwh ///
 i.dr_ind#c.monthly_max_kw , a(tariff_sched_text month year) vce(cluster sa_uuid)
reghdfe diff dr_ind tot_kwh_on_peak tot_kwh_mid_peak tot_kwh_off_peak tot_kwh_sup_off_peak ///
i.dr_ind#c.tot_kwh_on_peak i.dr_ind#c.tot_kwh_mid_peak i.dr_ind#c.tot_kwh_off_peak ///
i.dr_ind#c.tot_kwh_sup_off_peak, a(tariff_sched_text month year) vce(cluster sa_uuid)
reghdfe diff dr_ind tot_kwh_on_peak tot_kwh_mid_peak tot_kwh_off_peak tot_kwh_sup_off_peak monthly_max_kw ///
i.dr_ind#c.tot_kwh_on_peak i.dr_ind#c.tot_kwh_mid_peak i.dr_ind#c.tot_kwh_off_peak ///
i.dr_ind#c.tot_kwh_sup_off_peak i.dr_ind#c.monthly_max_kw, a(tariff_sched_text month year) vce(cluster sa_uuid)

reghdfe diff dr_ind_current, a(tariff_sched_text month year) vce(cluster sa_uuid)
reghdfe diff dr_ind_current total_bill_kwh i.dr_ind_current#c.total_bill_kwh, a(tariff_sched_text month year) vce(cluster sa_uuid)
reghdfe diff dr_ind_current monthly_max_kw i.dr_ind_current#c.monthly_max_kw, a(tariff_sched_text month year) vce(cluster sa_uuid)
reghdfe diff dr_ind_current total_bill_kwh monthly_max_kw i.dr_ind_current#c.total_bill_kwh ///
 i.dr_ind_current#c.monthly_max_kw , a(tariff_sched_text month year) vce(cluster sa_uuid)
reghdfe diff dr_ind_current tot_kwh_on_peak tot_kwh_mid_peak tot_kwh_off_peak tot_kwh_sup_off_peak ///
i.dr_ind_current#c.tot_kwh_on_peak i.dr_ind_current#c.tot_kwh_mid_peak i.dr_ind_current#c.tot_kwh_off_peak ///
i.dr_ind_current#c.tot_kwh_sup_off_peak, a(tariff_sched_text month year) vce(cluster sa_uuid)
reghdfe diff dr_ind_current tot_kwh_on_peak tot_kwh_mid_peak tot_kwh_off_peak tot_kwh_sup_off_peak monthly_max_kw ///
i.dr_ind_current#c.tot_kwh_on_peak i.dr_ind_current#c.tot_kwh_mid_peak i.dr_ind_current#c.tot_kwh_off_peak ///
i.dr_ind_current#c.tot_kwh_sup_off_peak i.dr_ind_current#c.monthly_max_kw, a(tariff_sched_text month year) vce(cluster sa_uuid)

*general conclusion: kw, kwh and DR seem to matter for differences between real and reconstructed bill


*Remove dec 2015

reghdfe diff dr_ind_current tot_kwh_on_peak tot_kwh_mid_peak tot_kwh_off_peak tot_kwh_sup_off_peak monthly_max_kw ///
i.dr_ind_current#c.tot_kwh_on_peak i.dr_ind_current#c.tot_kwh_mid_peak i.dr_ind_current#c.tot_kwh_off_peak ///
i.dr_ind_current#c.tot_kwh_sup_off_peak i.dr_ind_current#c.monthly_max_kw if bill_start_dt>td(31dec2015), a(tariff_sched_text month year) vce(cluster sa_uuid)

//Nothing changes

*perform for each month

forvalues m = 1/12 {
	reghdfe diff dr_ind_current tot_kwh_on_peak tot_kwh_mid_peak tot_kwh_off_peak tot_kwh_sup_off_peak monthly_max_kw ///
	i.dr_ind_current#c.tot_kwh_on_peak i.dr_ind_current#c.tot_kwh_mid_peak i.dr_ind_current#c.tot_kwh_off_peak ///
	i.dr_ind_current#c.tot_kwh_sup_off_peak i.dr_ind_current#c.monthly_max_kw if month==`m', a(tariff_sched_text year) vce(cluster sa_uuid)
}

// Heterogeneity, but things generally stay the same.

cd "$dirpath/data/figures"

*perform for each schedule
levelsof tariff_sched_text, local(levs)
foreach rt in `levs' {
	reghdfe diff dr_ind_current tot_kwh_on_peak tot_kwh_mid_peak tot_kwh_off_peak tot_kwh_sup_off_peak monthly_max_kw ///
	i.dr_ind_current#c.tot_kwh_on_peak i.dr_ind_current#c.tot_kwh_mid_peak i.dr_ind_current#c.tot_kwh_off_peak ///
	i.dr_ind_current#c.tot_kwh_sup_off_peak i.dr_ind_current#c.monthly_max_kw if tariff_sched_text=="`rt'", a(month year) vce(cluster sa_uuid)
	
	/*hist diff_prop if tariff_sched_enc==`i', title("Proportional difference- tariff `rt'")
	graph save hist_diff_`i', replace
	graph export hist_diff_`i'.png, replace*/

}

// Heterogeneity, but things generally stay the same.

*Look at extreme values
gen extreme_val_1=1 if pctile_diff>1 & pctile_diff<100
replace extreme_val_1=0 if extreme_val_1==.

reghdfe extreme_val_1 total_bill_kwh monthly_max_kw, a(tariff_sched_text month year) vce(cluster sa_uuid)
reghdfe extreme_val_1 total_bill_volumetric total_bill_kw, a(tariff_sched_text month year) vce(cluster sa_uuid)

gen ndays=bill_end_dt - bill_start_dt + 1
gen long_bill=1 if ndays>33
replace long_bill=0 if long_bill==.

reg extreme_val_1 long_bill

sort year

egen corr_year = corr(total_bill_amount total_bill_amount_constr), ///
	by(year)


	
	
reghdfe extreme_val_1 dr_ind total_bill_kwh monthly_max_kw if year==2015, a(tariff_sched_text month) vce(cluster sa_uuid)


	
if 1==0 {

use "$dirpath_data/merged_sce/bills_constructed_final_`tag'.dta", replace


*figure out what's going on with negative bills

gen neg=1 if total_bill_amount<0
keep if neg==1
sort bill_start_dt
keep sa_uuid bill_start_dt
duplicates drop
save "$dirpath_data/merged_sce/negative_ID.dta", replace
use "$dirpath_data/merged_sce/hourly_with_prices_`tag'.dta", replace
joinby sa_uuid bill_start_dt using "$dirpath_data/merged_sce/negative_ID.dta"
su kwh, d
bysort sa_uuid date: egen day_kwh=sum(kwh)
drop day_kwh
bysort sa_uuid date bundled: egen day_kwh=sum(kwh)
gen zero_day=1 if day_kwh<=0
replace zero_day=0 if zero_day==.
bysort sa_uuid date bundled: egen min_kwh=min(kwh)
gen zero_hour=1 if min_kwh<=0
replace zero_hour=0 if zero_hour==.
save "$dirpath_data/merged_sce/negative_charge.dta", replace
drop bundled p_kwh_1 p_kwh_2 p_kwh_3
duplicates drop
sort sa_uuid bill_start_dt date hour
gen zero_kwh_hour=1 if kwh==0
replace zero_kwh_hour=0 if zero_kwh_hour==.
bysort sa_uuid bill_start_dt date: egen zero_hour_day_count=sum( zero_kwh_hour )
drop hour kwh zero_kwh_hour
duplicates drop
bysort sa_uuid bill_start_dt: egen zero_hour_day_mean=mean( zero_hour_day_count )
bysort sa_uuid bill_start_dt: egen zero_day_count=sum( zero_day )
bysort sa_uuid bill_start_dt: egen zero_day_1=max(zero_day )
keep sa_uuid bill_start_dt zero_day_1 zero_day_count zero_hour_day_mean
duplicates drop
su zero_hour_day_mean
su zero_day_count
su zero_day_count, d
/*
There are 170 bills with negative payable amounts. We need to figure out where this negativity comes from.
Only 1 of the 170 bills has no kwh for every hour of the bill, while 25\% have at least 2 full days of no
kwh activity for every hour. Furthermore, there are only 2 bills with no ma kW demand (i.e. max kW=0) and 
there is an avergae of 6 hours with no kwh demand. However, there are no bills with negative demand. 
This suggests that there is no credible explanation for why we have some negative bills ==> drop them.
70% of the 140 negative bills are DR==1 (demand response option), so clearly DR does play a role in 
explaining the negatives (even though it doesn't explain everything). 11% of all bills are DR==1, 
so we would lose 10% of the sample if we dropped them. When negative bills are not ignored, the correlations are as follows:
DR==1: 0.9813
DR==0:  0.9696
dropping negatives
DR==1: 0.9822
DR==0:  0.9890
*/




drop if total_bill_amount<0 // 170 bill
duplicates drop sa_uuid bill_start_dt, force
xtile pctile = total_bill_amount , nq(100)
xtile pctile = total_bill_amount , nq(100)

}
