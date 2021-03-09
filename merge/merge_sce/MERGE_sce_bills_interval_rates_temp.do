clear all
version 13
set more off

*****************************************************************************
**** Script to merge customer bills and interval data into SCE rate data ****
*****************************************************************************

** This script does two things, ONLY for SA-bills that merge into AMI data: 
** 		1) Add hourly marginal prices to AMI data
**		2) Add $/kW fixed charges to each bill

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"


*******************************************************************************
*******************************************************************************

** 2. Merge in billing/interval data, looping over rates (MARCH DATA)
if 1==0 {

local tag = "20201123"


** Loop over months of sample that overlap with rate data
local YM_min = ym(2015,12) // fully mergeable data only spans from dec 2015-jun 2019
local YM_max = ym(2019,7)
forvalues YM = `YM_min'/`YM_max' {

	qui {
	
	** Load cleaned PGE bililng data
	use "$dirpath_data/sce_cleaned/billing_data.dta", clear
	** Keep if in month
	keep if `YM'==ym(year(bill_start_dt),month(bill_start_dt))

	** Drop observations without good interval data (for purposes of corroborating dollar amounts)
	keep if flag_interval_merge==1

	** Drop flags
	drop flag* sp_uuid interval_bill_corr

	** Expand whole dataset by bill length variable
	expand bill_length, gen(temp_new)
	sort sa_uuid bill_start_dt temp_new
	tab temp_new

	** Construct date variable (duplicated at each bill change-over)
	gen date = bill_start_dt if temp_new==0
	format %td date
	replace date = date[_n-1]+1 if temp_new==1
	assert date==bill_start_dt if temp_new==0
	assert date==bill_end_dt if temp_new[_n+1]==0
	assert date!=.
	unique sa_uuid bill_start_dt date
	assert r(unique)==r(N)

	** Flag duplicate account-dates (bill changeover dates where end=start)
	gen temp_wt = 1
	replace temp_wt = 0.5 if date==date[_n+1] & date==bill_end_dt & ///
		bill_end_dt==bill_start_dt[_n+1] & temp_new==1 & temp_new[_n+1]==0 & ///
		sa_uuid==sa_uuid[_n+1]
	replace temp_wt = 0.5 if date==date[_n-1] & date==bill_end_dt[_n-1] & ///
		bill_end_dt[_n-1]==bill_start_dt & temp_new[_n-1]==1 & temp_new==0 & ///
		sa_uuid==sa_uuid[_n-1]
		// this assigns 50% weight to days that are shared by two bills (i.e. the
		// end_date of the previous bill and the start_date of the current bill)

	** Collapse down to 1 day on cusps (keep start date over end date)
	assert date==bill_end_dt | date==bill_start_dt if temp_wt==0.5
	drop if temp_wt==0.5 & date==bill_end_dt
	drop temp_new temp_wt
	**Get rid of duplicates 
	sort sa_uuid date 
	quietly by sa_uuid date :  gen dup = cond(_N==1,0,_n)
	drop if dup==1
	drop dup
	** Store min and max date, for narrowing down the two subsequent merges
	sum date
	gen year = year(date)
	replace tariff_sched_text="TOU-PA-SOP" if (tariff_sched_text=="TOU-PA-SOP-1" | tariff_sched_text=="TOU-PA-SOP-2") & year<2013
	replace tariff_sched_text="TOU-PA-B" if (tariff_sched_text=="TOU-PA-B-I") 
	replace tariff_sched_text="TOU-PA2-SOP-1" if tariff_sched_text=="TPA2-SOP1"
	replace tariff_sched_text="TOU-PA2-SOP-2" if tariff_sched_text=="TPA2-SOP2"
	replace tariff_sched_text="TOU-PA3-SOP-1" if tariff_sched_text=="TPA3-SOP1"
	replace tariff_sched_text="TOU-PA3-SOP-2" if tariff_sched_text=="TPA3-SOP2"

	local dmin = r(min)
	local dmax = r(max)

	** Merge in hourly interval data
	preserve
	clear
	use "$dirpath_data/sce_cleaned/interval_data_hourly_20190916.dta" if inrange(date,`dmin',`dmax')	
	tempfile temp_interval
	save `temp_interval'
	restore
	merge 1:m sa_uuid date using `temp_interval', keep(3)
	keep if _merge==3
	drop _merge 
	rename hr hour
	
	** Merge in rate data by hour
	preserve
	clear
	use "$dirpath_data/sce_cleaned/marginal_prices_hourly_11112020.dta" if inrange(date,`dmin',`dmax')
	drop dow_num holiday off_peak_credit wind_mach_credit ee_charge_1 ee_charge_2 ee_charge_3 demandcharge_facilities ///
	voltage_dis_load_1_1 voltage_dis_load_1_2 voltage_dis_load_1_3 voltage_dis_load_2_1 voltage_dis_load_2_2 voltage_dis_load_2_3 ///
	voltage_dis_load_1 voltage_dis_load_2 voltage_dis_load_3 pf_adjust_1 pf_adjust_2 pf_adjust_3 voltage_weekd_1 voltage_weekd_2 ///
	voltage_weekd_3 interruptible_credit demandcharge weekday v_weekend_1 dem_time_1 v_weekend_2 dem_time_2 ///
	v_weekend_3 dem_time_3 servicecharge
	tempfile temp_rates
	save `temp_rates'
	restore
	joinby tariff_sched_text date hour using `temp_rates', unmatched(none)

	** Save hourly data with rates and prices
	preserve
	keep sa_uuid date hour kwh p_kwh_1 p_kwh_2 p_kwh_3 bill_start_dt bundled on_peak mid_peak off_peak sup_off_peak
	compress
	save "$dirpath_data/merged_sce/hourly_with_prices_`YM'_`tag'.dta", replace
	restore	
		
	** Check if max kW is missing anywhere where we need it
	count if monthly_max_kw==. 
	local Nmiss_demand = r(N)
	if `Nmiss_demand'>0 {
		gen flag_max_demand_constr = monthly_max_kw==. // & maxdemandcharge!=0 & maxdemandcharge!=.
		la var flag_max_demand_constr "Fixed charge (max) from AMI data, missing in billing"
		egen double temp_max_demand = max(kwh), by(sa_uuid bill_start_dt)
		replace monthly_max_kw = temp_max_demand if flag_max_demand_constr==1
		drop temp*
	}


	** Calculate min/max/mean of marginal price, before collapsing
	foreach i in 1 2 3 {
		egen double p_kwh_min_`i' = min(p_kwh_`i'), by(sa_uuid bill_start_dt bundled)
		egen double p_kwh_max_`i' = max(p_kwh_`i'), by(sa_uuid bill_start_dt bundled)
		egen double p_kwh_mean_`i' = mean(p_kwh_`i'), by(sa_uuid bill_start_dt bundled)
		la var p_kwh_min_`i' "Min marg price ($/kWh) across whole bill"
		la var p_kwh_max_`i' "Max marg price ($/kWh) across whole bill"
		la var p_kwh_mean_`i' "Mean marg price ($/kWh) across whole bill"

	** Calculate total volumetric portion of bill
		gen temp_`i' = kwh*p_kwh_`i'
		egen double total_bill_volumetric_`i' = sum(kwh*p_kwh_`i'), by(sa_uuid bill_start_dt bundled)
		la var total_bill_volumetric_`i' "Total $ of volumetric charges on bill for v category `i' ($/kWh * kWh, constructed)"
		drop temp_`i'
		}
		

		**Calculate total kwh over all time and peak, mid-peak, off-peak and super-off-peak hours for each bill in each rate window
		** We don't do it at the bill level due to overlaps between a change in rate and the same bill
		bysort sa_uuid bill_start_dt: gegen tot_kwh_bill= sum(kwh)
		bysort sa_uuid bill_start_dt rate_start_date: gegen tot_kwh_on_pk= sum(kwh) if on_peak==1
		bysort sa_uuid bill_start_dt rate_start_date: gegen tot_kwh_mid_pk= sum(kwh) if mid_peak==1
		bysort sa_uuid bill_start_dt rate_start_date: gegen tot_kwh_off_pk= sum(kwh) if off_peak==1
		bysort sa_uuid bill_start_dt rate_start_date: gegen tot_kwh_sup_off_pk= sum(kwh) if sup_off_peak==1
		
		bysort sa_uuid bill_start_dt rate_start_date: gegen tot_kwh_sum_on_pk_1= max(kwh) if season=="summer" & on_peak==1
		bysort sa_uuid bill_start_dt rate_start_date: gegen tot_kwh_sum_mid_pk_1= max(kwh) if season=="summer" & mid_peak==1
		bysort sa_uuid bill_start_dt rate_start_date: gegen tot_kwh_sum_off_pk_1= max(kwh) if season=="summer" & off_peak==1
		bysort sa_uuid bill_start_dt rate_start_date: gegen tot_kwh_win_mid_pk_1= max(kwh) if season=="winter" & mid_peak==1
		bysort sa_uuid bill_start_dt rate_start_date: gegen tot_kwh_win_off_pk_1= max(kwh) if season=="winter" & off_peak==1
		bysort sa_uuid bill_start_dt rate_start_date: gegen tot_kwh_sum_on_pk= max(tot_kwh_sum_on_pk_1) 
		bysort sa_uuid bill_start_dt rate_start_date: gegen tot_kwh_sum_mid_pk= max(tot_kwh_sum_mid_pk_1) 
		bysort sa_uuid bill_start_dt rate_start_date: gegen tot_kwh_sum_off_pk= max(tot_kwh_sum_off_pk_1)
		bysort sa_uuid bill_start_dt rate_start_date: gegen tot_kwh_win_mid_pk= max(tot_kwh_win_mid_pk_1) 
		bysort sa_uuid bill_start_dt rate_start_date: gegen tot_kwh_win_off_pk= max(tot_kwh_win_off_pk_1) 


		foreach j in tot_kwh_sum_on_pk tot_kwh_sum_mid_pk tot_kwh_sum_off_pk tot_kwh_win_mid_pk tot_kwh_win_off_pk {
		drop `j'_1
		replace `j'=0 if `j'==.
		}
		

	** Collapse down from hourly observations, to expedite the next few steps
	*drop date hour kwh p_kwh_1 p_kwh_2 p_kwh_3 _merge bill_length on_peak mid_peak off_peak sup_off_peak
	duplicates drop sa_uuid bill_start_dt rate_start_date rate_end_date date bundled, force
		// all I need here is SA-bill-group-offpeak/partpeak/peak to build up
		// group-specific fixed charges
    bysort sa_uuid bill_start_dt rate_start_date bundled: gen days_between=_N
	duplicates drop sa_uuid bill_start_dt rate_start_date bundled, force

	** Calculate fixed charge per kW, for whole bill
	foreach i in 1 2 3 {
		gen total_bill_kw_`i' = KW_p`i'*monthly_max_kw
		lab var total_bill_kw_`i' "Total $ of per-kW charges on bill for v cat `i' ($/kW * max_kW, constructed)"
		}


	** Calculate fixed charge for whole bill
	assert customercharge!=. & tou_option_meter_charge!=. & tou_RTEM_meter_charge!=.
	gen total_bill_fixed = (customercharge + tou_RTEM_meter_charge + tou_option_meter_charge)
		// I'm assuming that the charges apply monthly
	la var total_bill_fixed "Total $ of fixed per-day charges on bill ($/day * days, constructed)"
		
	** Collapse to the SA-bill-group level
	foreach v of varlist *monthly_max_kw {
		egen double temp = max(`v'), by(sa_uuid bill_start_dt bundled)
		replace `v' = temp if `v'==.
		replace `v' = temp if substr("`v'",1,4)=="flag"
		drop temp
	}

	** Add up bill components, except hp demand charges
	foreach i in 1 2 3 {
		assert total_bill_volumetric_`i'!=. & total_bill_kw_`i'!=. & total_bill_fixed!=.
		gen total_bill_amount_nohp_`i' = total_bill_volumetric_`i' + total_bill_kw_`i' + total_bill_fixed
		la var total_bill_amount_nohp_`i' "Total $ on bill for v cat `i', without hp component"
	}
	** Save monthly data of constructed bill components
	compress
	save "$dirpath_data/merged_sce/bills_rates_constructed_`YM'_`tag'.dta", replace 
	
	}
	
	di %tm `YM' "  " c(current_time)
}
*
** Append monthly files (hourly)
clear 
*set obs 1

cd "$dirpath_data/merged_sce"
*local files_hourly : dir "." files "hourly_with_prices_*_`tag'.dta" // for some reason, including the `tag' stopped it from working
local files_hourly : dir "." files "hourly_with_prices_*.dta"
foreach f in `files_hourly' {
	append using "`f'"
}
duplicates drop // for some reason there are a small number of dups...
sort sa_uuid date hour bill_start_dt bundled
cap drop dup
drop if on_peak==. | mid_peak ==. | off_peak ==. | sup_off_peak ==. 
duplicates t sa_uuid date hour bundled, gen(dup) // dups occur on that span months bill cusp dates
assert inlist(dup,0,1)
drop if dup==1 & dup[_n+1]==1 & sa_uuid==sa_uuid[_n+1] & date==date[_n+1] & hour==hour[_n+1] & ///
	 bundled==bundled[_n+1] & bill_start_dt<bill_start_dt[_n+1] // keep later bill date (everything else is identical)
drop dup
sort sa_uuid sa_uuid date hour bundled
quietly by sa_uuid sa_uuid date hour bundled :  gen dup = cond(_N==1,0,_n)
tab dup 
drop if dup>0
drop dup
unique sa_uuid date hour bundled
assert r(unique)==r(N)
compress
save "$dirpath_data/merged_sce/hourly_with_prices_`tag'.dta", replace


** Append monthly files (bills)

clear 
local tag = "20201222"

cd "$dirpath_data/merged_sce/NEW"
*local files_bills : dir "." files "bills_rates_constructed_*_`tag'.dta"
local files_bills : dir "." files "bills_rates_constructed_*.dta"
foreach f in `files_bills' {
	append using "`f'"
}
duplicates drop
sort sa_uuid bill_start_dt rate_start_date bundled
unique sa_uuid bill_start_dt rate_start_date bundled
assert r(unique)==r(N)
compress
*Note: we lose alot of observations in feb 2016 (most likely from interval data)

local tag = "20201222"

destring sa_uuid, gen(sa_uuid_num)
xtile dec = sa_uuid_num , nq(10) // divide into quantiles so that way you can run 10 instances of the following code
drop date hour kwh _merge season on_peak mid_peak off_peak sup_off_peak p_kwh_1 p_kwh_2 p_kwh_3
order sa_uuid bill_start_dt bill_end_dt rate_start_date rate_end_date bundled ///
days_between bill_length tot_kwh_sum_on_pk tot_kwh_sum_mid_pk tot_kwh_sum_off_pk ///
tot_kwh_win_mid_pk tot_kwh_win_off_pk dem_time_sum_on_pk_1 dem_time_sum_mid_pk_1 ///
dem_time_sum_off_pk_1 dem_time_win_mid_pk_1 dem_time_win_off_pk_1

gen bill_weight=days_between/bill_length
rename total_bill_fixed total_bill_fixed_old
gen total_bill_fixed1 = (customercharge)*bill_weight
cap drop total_bill_fixed
bysort sa_uuid bill_start_dt bundled: gegen total_bill_fixed=sum(total_bill_fixed1)
drop total_bill_fixed1

local tag = "20201222"

joinby tariff_sched_text rate_start_date rate_end_date bundled  using "$dirpath_data/merged_sce/facility_charges.dta", unmatched(none)

		
foreach j in 1 2 3{
	gen dem_time_charge_`j'1=(tot_kwh_sum_on_pk*dem_time_sum_on_pk_`j')+ ///
	(tot_kwh_sum_mid_pk*dem_time_sum_mid_pk_`j')+(tot_kwh_sum_off_pk*dem_time_sum_off_pk_`j')+ ///
	(tot_kwh_win_mid_pk*dem_time_win_mid_pk_`j')+(tot_kwh_win_off_pk*dem_time_win_off_pk_`j')* ///
	bill_weight
	bysort sa_uuid bill_start_dt bundled: gegen dem_time_charge_`j'=sum(dem_time_charge_`j'1)
	drop dem_time_charge_`j'
	gen dem_fac_charge_`j'1=(dem_fac_`j'*monthly_max_kw)*bill_weight
	bysort sa_uuid bill_start_dt bundled: gegen dem_fac_charge_`j'=sum(dem_fac_charge_`j'1)
	drop dem_fac_charge_`j'1
	rename total_bill_kw_`j' total_bill_kw_`j'_old
	gen total_bill_kw_`j'=dem_fac_charge_`j'+dem_time_charge_`j'
}

	foreach i in 1 2 3 {
		rename total_bill_amount_nohp_`i' total_bill_amount_nohp_`i'_old
		gen total_bill_amount_`i' = total_bill_volumetric_`i' + total_bill_kw_`i' + total_bill_fixed // ignore hp charges
		la var total_bill_amount_`i' "Total $ on bill for v cat `i'"
	}


duplicates drop sa_uuid bill_start_dt bundled, force


corr total_bill_amount_nohp_1 total_bill_amount_nohp_1_old
local tag = "20201222"

save "$dirpath_data/merged_sce/bills_rates_constructed_`tag'.dta", replace

local tag = "20201222"


use "$dirpath_data/merged_sce/bills_rates_constructed_`tag'.dta", replace
foreach i in 1 2 3 {
		gen diff_`i'=total_bill_amount_`i'-total_bill_amount // for a given start date, customer, v cat, and bundled package, what's the difference between the constructed and true bill
		gen abs_diff_`i'=abs(diff_`i') // absolute value of former difference
		bysort sa_uuid bill_start_dt: egen min_`i'=min(abs_diff_`i') //for a given start date, customer, and window, is the bundled package closer to the true bill than unbundled
		lab var min_`i' "Min absolute difference across bundles - v cat `i' "
		lab var diff_`i'"Bundle-specifc Difference  - v cat `i'"
		lab var abs_diff_`i' "Bundle-specifc absolute difference - v cat `i' "
		sort sa_uuid tariff_sched_text bundled
		egen temp_corr_`i' = corr(total_bill_amount total_bill_amount_`i'), /// find customer and tariff specific correlation across entire bill history
			by(sa_uuid tariff_sched_text bundled)
			
		sort sa_uuid tariff_sched_text 
		egen temp_corr_max_`i' = max(temp_corr_`i'), by(sa_uuid tariff_sched_text) //for a given customer and v cat is the bundled package more correlated to the true bill than unbundled
}

	egen minval = rowmin(min_1 min_2 min_3) //  which voltage category gets you closest to the true bill
	egen max_corr = rowmax(temp_corr_max_1 temp_corr_max_2 temp_corr_max_3) //  which voltage category gets you closest to the true bill

	gen pred_cat=""
	gen pred_bundled=.
	gen best_fit=.
	gen pred_cat_corr=""
	gen pred_bundled_corr=.
	gen best_fit_corr=.


foreach i in 1 2 3 {
	replace pred_bundled=bundled+1 if abs_diff_`i'==minval // which bundling package is consistent with the best fit
	replace pred_bundled_corr=bundled+1 if temp_corr_`i'==max_corr // which bundling package is consistent with the highest correlation
}
replace pred_bundled=0 if pred_bundled==.
replace pred_cat="1" if min_1==minval & min_2!=minval & min_3!=minval // given this window, which voltage category gets you closest to the true bill
replace pred_cat="2" if min_2==minval & min_1!=minval & min_3!=minval 
replace pred_cat="3" if min_3==minval & min_2!=minval & min_1!=minval 
replace pred_cat="1 or 2" if min_1==minval & min_2==minval & min_3!=minval 
replace pred_cat="1 or 3" if min_1==minval & min_2!=minval & min_3==minval 
replace pred_cat="2 or 3" if min_3==minval & min_2==minval & min_1!=minval 
replace pred_cat="Any" if min_3==minval & min_2==minval & min_1==minval //

replace pred_bundled_corr=0 if pred_bundled_corr==.
replace pred_cat_corr="1" if min_corr_1==minval & min_corr_2!=minval & min_corr_3!=minval // which voltage category is most correlated with the true bill
replace pred_cat_corr="2" if min_corr_2==minval & min_corr_1!=minval & min_corr_3!=minval 
replace pred_cat_corr="3" if min_corr_3==minval & min_corr_2!=minval & min_corr_1!=minval 
replace pred_cat_corr="1 or 2" if min_corr_1==minval & min_corr_2==minval & min_corr_3!=minval 
replace pred_cat_corr="1 or 3" if min_corr_1==minval & min_corr_2!=minval & min_corr_3==minval 
replace pred_cat_corr="2 or 3" if min_corr_3==minval & min_corr_2==minval & min_corr_1!=minval 
replace pred_cat_corr="Any" if min_corr_3==minval & min_corr_2==minval & min_corr_1==minval // 

foreach i in 1 2 3 {
	replace best_fit=total_bill_amount_`i' if abs_diff_`i'==minval // what is the predicted bill	
	replace best_fit_corr=total_bill_amount_`i' if temp_corr_`i'==max_corr // what is the predicted bill	
}
bysort sa_uuid bill_start_dt: egen pred_bundled_2=sum(pred_bundled)
bysort sa_uuid bill_start_dt: egen best_fit_2=max(best_fit)
bysort sa_uuid bill_start_dt: egen pred_bundled_corr_2=sum(pred_bundled_corr)
bysort sa_uuid bill_start_dt: egen best_fit_corr_2=max(best_fit_corr)
drop pred_bundled best_fit pred_bundled_corr best_fit_corr

foreach i in pred_bundled pred_bundled_2  best_fit_corr_2 pred_bundled_corr_2 {
rename `i'_2 `i' 
}
replace pred_bundled=pred_bundled-1
replace pred_bundled_corr=pred_bundled_corr-1

lab var pred_cat "V category of best fitting constructed bill "
lab var pred_cat_corr "V category of most correlated constructed bill "

lab var pred_bundled "Bundling of best fitting constructed bill "
lab var pred_bundled_corr "Bundling of most correlated constructed bill "

lab var best_fit "Total $ amount of best fitting constructed bill "
lab var best_fit_corr "Total $ amount of most correlated constructed bill "

lab var minval "Minimum absolute diff for best fitting constructed bill "
lab var max_corr "Maximum correlation for most correlated constructed bill "


gen total_bill_amount_constr= best_fit
gen total_bill_amount_corr_constr= best_fit_corr

lab var total_bill_amount_constr "Total $ on bill, constructed using absolute difference"
lab var total_bill_amount_corr_constr "Total $ on bill, constructed using correlation"

gen total_bill_volumetric=total_bill_volumetric_1 if pred_cat=="1" | pred_cat=="Any"
replace total_bill_volumetric=total_bill_volumetric_2 if pred_cat=="2" | pred_cat=="2 or 3"
replace total_bill_volumetric=total_bill_volumetric_3 if pred_cat=="3"

gen total_bill_kw=total_bill_kw_1 if pred_cat=="1" | pred_cat=="Any"
replace total_bill_kw=total_bill_kw_2 if pred_cat=="2" | pred_cat=="2 or 3"
replace total_bill_kw=total_bill_kw_3 if pred_cat=="3"

drop dem_fac_1 dem_fac_2 dem_fac_3 dem_time_charge_11 dem_fac_charge_1 total_bill_kw_1 dem_time_charge_21 dem_fac_charge_2 total_bill_kw_2 dem_time_charge_31 dem_fac_charge_3 total_bill_kw_3 total_bill_amount_1 total_bill_amount_2 total_bill_amount_3 diff_1 abs_diff_1 min_1 diff_2 abs_diff_2 min_2 diff_3 abs_diff_3 min_3 minval best_fit_2
duplicates drop sa_uuid bill_start_dt, force

local tag = "20201222"

save "$dirpath_data/merged_sce/bills_constructed_final_`tag'.dta", replace


/*
** Delete monthly files (hourly)
cd "$dirpath_data/merged_sce"
local files_hourly : dir "." files "hourly_with_prices_*_`tag'.dta"
foreach f in `files_hourly' {
	erase "`f'"
}

** Delete monthly files (bills)
cd "$dirpath_data/merged_sce"
local files_bills : dir "." files "bills_rates_constructed_*_`tag'.dta"
foreach f in `files_bills' {
	erase "`f'"
}

}*/

*******************************************************************************
*******************************************************************************

** 5. Diagnostics on billing data
if 1==0 {
local tag = "20201222"

use "$dirpath_data/merged_sce/bills_constructed_final_`tag'.dta", replace
drop dem_fac_1 dem_fac_2 dem_fac_3 dem_time_charge_11 dem_fac_charge_1 total_bill_kw_1 dem_time_charge_21 dem_fac_charge_2 total_bill_kw_2 dem_time_charge_31 dem_fac_charge_3 total_bill_kw_3 total_bill_amount_1 total_bill_amount_2 total_bill_amount_3 diff_1 abs_diff_1 min_1 diff_2 abs_diff_2 min_2 diff_3 abs_diff_3 min_3 minval best_fit_2

** Diagnostics
gen temp_bad_bill_match = total_bill_amount==. | total_bill_amount_constr==. | ///
	total_bill_amount/total_bill_amount_constr > 5 | ///
	total_bill_amount_constr/total_bill_amount > 5 
tab temp_bad_bill_match // 1.11% (not great, not terrible)

*generate months and seasons
gen temp_year = year(bill_start_dt)
gen temp_month = month(bill_start_dt)
gen temp_season="summer" if temp_month>=6 & temp_month<=9 
replace temp_season="winter" if temp_season==""
*generate discrepancy between true and reconstructed bill.
gen diff=total_bill_amount_constr-total_bill_amount
gen prop_diff=diff/total_bill_amount
gen zero=0
gen low_winter=1 if diff>0 & temp_season=="winter"
replace  low_winter=0 if  low_winter==.
bysort temp_year sa_uuid: egen max_low_winter=max(low_winter)
bysort temp_year sa_uuid: egen sum_low_winter=sum(low_winter)

gen total_bill_kwh_winter=total_bill_kwh if temp_season=="winter"
gen diff_2=total_bill_amount_constr-total_bill_amount-total_bill_fixed
bysort temp_year sa_uuid: egen max_total_bill_kwh_winter=max(total_bill_kwh_winter)
*Generate variable for total kwh consumption based on kwh consumption in interval data


gen total_bill_kwh_constr= tot_kwh_bill

*merge in DR data
preserve
use "$dirpath_data/merged_sce/backup/bills_constructed_merged_20201222.dta", replace
bys sa_uuid bill_start_dt: egen max_dr_ind_current=max(dr_ind_current)
drop dr_ind_current
rename max_dr_ind_current dr_ind_current
keep sa_uuid bill_start_dt dr_ind_current dr_ind
duplicates drop
tempfile temp_interval
save `temp_interval'
restore
joinby sa_uuid bill_start_dt using `temp_interval'
gen keep=0 if monthly_max_kw>0 & total_bill_kwh==0 | total_bill_kwh>0 & monthly_max_kw==0
replace keep=1 if keep==.
gen keep2=0 if monthly_max_kw>0 & total_bill_kwh_constr==0 | total_bill_kwh_constr>0 & monthly_max_kw==0
replace keep2=1 if keep2==.


local tag = "20201222"
*local tag2 = "old"


save "$dirpath_data/merged_sce/bills_constructed_tests_`tag'.dta", replace
use "$dirpath_data/merged_sce/bills_constructed_tests_`tag'.dta", replace
set scheme s1mono

levelsof tariff_sched_text if tariff_sched_text=="TOU-PA2B", local(levs) //  | tariff_sched_text=="TOU-PA2B" | tariff_sched_text=="TOU-PA3B" /// | tariff_sched_text=="TOU-PA2D"
levelsof temp_year, local(years)
levelsof temp_month, local(months)
levelsof temp_season, local(seasons)

foreach rt in `levs' {
foreach yrs in `years' {
*foreach yr in `seasons' {

	twoway (scatter  diff monthly_max_kw ///
			if temp_year==`yrs'  & total_bill_kwh!=0 & total_bill_kwh!=. & tariff_sched_text=="`rt'" & total_bill_amount>=0 ///
			 & dr_ind_current==0 & monthly_max_kw<210, msize(tiny) ///  & diff>=-2000 & diff<=2000
			) ///
		(line zero monthly_max_kw ///
			if temp_year==`yrs'  & total_bill_kwh!=0 & total_bill_kwh!=. & tariff_sched_text=="`rt'" & total_bill_amount>=0 ///
			& tariff_sched_text=="`rt'"  & dr_ind_current==0 & monthly_max_kw<210 /// & temp_season=="`yr'"
			, msize(tiny) ///  & diff>=-2000 & diff<=2000
			), ///
		title("`rt' `yrs'") legend(label(1 "Energy Usage (KW)") label(2 "No difference") note("Difference bounded between +-2000"))	ytitle("Difference ($)") ///
		xtitle("Energy Usage (KW)")
		
		cd "$dirpath/data/figures" 
		graph save `rt'_`yrs'_kw_trim, replace
		graph export `rt'_`yrs'_kw_trim.pdf, replace
		erase `rt'_`yrs'_kw_trim.gph 
		*erase `rt'_`yr'.gph 
		/*

	twoway (scatter  total_bill_amount_constr total_bill_amount ///
			if temp_year==`yrs'  & total_bill_kwh!=0 & total_bill_kwh!=. & tariff_sched_text=="`rt'" & total_bill_amount>=0 ///
			& total_bill_amount<=10000 & total_bill_amount_constr<=20000 & total_bill_kwh<=100000  /// & temp_season=="`yr'"
			& dr_ind_current==0, msize(tiny) ///  & keep==1
			) ///
		(line total_bill_amount total_bill_amount ///
			if temp_year==`yrs'  & total_bill_kwh!=0 & total_bill_kwh!=. & tariff_sched_text=="`rt'" & total_bill_amount>=0 ///
			& total_bill_amount<=10000 & total_bill_amount_constr<=20000 & total_bill_kwh<=100000  /// & temp_season=="`yr'"
			& dr_ind_current==0, msize(tiny) ///  & keep==1
			), ///
		title("`rt' `yrs'") legend(label(1 "Reconstructed vs True Bills") label(2 "No difference") note("Difference bounded between +-2000"))	ytitle("Reconstructed bills ($)") ///
		xtitle("True bills ($)")
		
		cd "$dirpath/data/figures" 
		graph save `rt'_`yrs'_general, replace
		graph export `rt'_`yrs'_general.pdf, replace
		erase `rt'_`yrs'_general.gph
		*erase `rt'_`yr'.gph 
		

		
	twoway (scatter  diff monthly_max_kw ///
			if temp_year==`yrs'  & total_bill_kwh!=0 & total_bill_kwh!=. & tariff_sched_text=="`rt'" & total_bill_amount>=0 ///
			& diff>=-2000 & diff<=2000 & dr_ind_current==0 & monthly_max_kw<500, msize(tiny) ///  & keep==1
			) ///
		(line zero monthly_max_kw ///
			if temp_year==`yrs'  & total_bill_kwh!=0 & total_bill_kwh!=. & tariff_sched_text=="`rt'" & total_bill_amount>=0 ///
			& tariff_sched_text=="`rt'"  & dr_ind_current==0 & monthly_max_kw<500 /// & temp_season=="`yr'"
			& diff>=-2000 & diff<=2000, msize(tiny) ///  & max_total_bill_kwh_winter<=500  & keep==1
			), ///
		title("`rt' `yrs'") legend(label(1 "Energy Usage (KW)") label(2 "No difference") note("Difference bounded between +-2000"))	ytitle("Difference ($)") ///
		xtitle("Energy Usage (KW)")
		
		cd "$dirpath/data/figures" 
		graph save `rt'_`yrs'_kw_trim, replace
		graph export `rt'_`yrs'_kw_trim.pdf, replace
		erase `rt'_`yrs'_kw_trim.gph 
		*erase `rt'_`yr'.gph 
		
	twoway (scatter  diff total_bill_kwh ///
			if temp_year==`yrs'  & total_bill_kwh!=0 & total_bill_kwh!=. & tariff_sched_text=="`rt'" & total_bill_amount>=0 ///
			& total_bill_kwh<=100000  /// & temp_season=="`yr'" & total_bill_amount<=10000 & total_bill_amount_constr<=20000 
			& diff>=-2000 & diff<=2000 & dr_ind_current==0 , msize(tiny) ///  & keep==1 & monthly_max_kw<500
			) ///
		(line zero total_bill_kwh ///
			if temp_year==`yrs'  & total_bill_kwh!=0 & total_bill_kwh!=. & tariff_sched_text=="`rt'" & total_bill_amount>=0 ///
			& total_bill_kwh<=100000  /// & temp_season=="`yr'" & total_bill_amount<=10000 & total_bill_amount_constr<=20000 
			& diff>=-2000 & diff<=2000 & dr_ind_current==0 , msize(tiny) ///  & keep==1 & monthly_max_kw<500
			), ///
		title("`rt' `yrs'") legend(label(1 "Energy Usage (KWh)") label(2 "No difference") note("Difference bounded between +-2000"))	ytitle("Difference ($)") ///
		xtitle("Energy Usage (KWh)")
		
		cd "$dirpath/data/figures" 
		graph save `rt'_`yrs'_kwh_trim, replace
		graph export `rt'_`yrs'_kwh_trim.pdf, replace
		erase `rt'_`yrs'_kwh_trim.gph
		*erase `rt'_`yr'.gph 
		*/

*}
}
}

local tag = "20201222"
*local tag2 = "old"


use "$dirpath_data/merged_sce/bills_constructed_tests_`tag'.dta", replace

reghdfe diff monthly_max_kw total_bill_kwh , a(tariff_sched_text temp_month temp_year) vce(cluster sa_uuid)
reghdfe diff monthly_max_kw total_bill_kwh_constr , a(tariff_sched_text temp_month temp_year) vce(cluster sa_uuid)
reghdfe diff monthly_max_kw total_bill_kwh , a(tariff_sched_text temp_month temp_year sa_uuid) vce(cluster sa_uuid)
reghdfe diff monthly_max_kw total_bill_kwh_constr , a(tariff_sched_text temp_month temp_year sa_uuid) vce(cluster sa_uuid)

reghdfe diff monthly_max_kw total_bill_kwh if dr_ind_current==0, a(tariff_sched_text temp_month temp_year) vce(cluster sa_uuid)
reghdfe diff monthly_max_kw total_bill_kwh if dr_ind_current==0, a(tariff_sched_text temp_month temp_year sa_uuid) vce(cluster sa_uuid)
reghdfe diff monthly_max_kw total_bill_kwh_constr if dr_ind_current==0, a(tariff_sched_text temp_month temp_year) vce(cluster sa_uuid)
reghdfe diff monthly_max_kw total_bill_kwh_constr if dr_ind_current==0, a(tariff_sched_text temp_month temp_year sa_uuid) vce(cluster sa_uuid)

reghdfe diff monthly_max_kw total_bill_kwh if dr_ind_current==0 & keep==1, a(tariff_sched_text temp_month temp_year) vce(cluster sa_uuid)
reghdfe diff monthly_max_kw total_bill_kwh if dr_ind_current==0 & keep==1, a(tariff_sched_text temp_month temp_year sa_uuid) vce(cluster sa_uuid)
reghdfe diff monthly_max_kw total_bill_kwh_constr if dr_ind_current==0 & keep==1, a(tariff_sched_text temp_month temp_year) vce(cluster sa_uuid)
reghdfe diff monthly_max_kw total_bill_kwh_constr if dr_ind_current==0 & keep==1, a(tariff_sched_text temp_month temp_year sa_uuid) vce(cluster sa_uuid)

reghdfe diff monthly_max_kw total_bill_kwh if dr_ind_current==0 & keep2==1, a(tariff_sched_text temp_month temp_year) vce(cluster sa_uuid)
reghdfe diff monthly_max_kw total_bill_kwh if dr_ind_current==0 & keep2==1, a(tariff_sched_text temp_month temp_year sa_uuid) vce(cluster sa_uuid)
reghdfe diff monthly_max_kw total_bill_kwh_constr if dr_ind_current==0 & keep2==1, a(tariff_sched_text temp_month temp_year) vce(cluster sa_uuid)
reghdfe diff monthly_max_kw total_bill_kwh_constr if dr_ind_current==0 & keep2==1, a(tariff_sched_text temp_month temp_year sa_uuid) vce(cluster sa_uuid)

/*
Main take-aways:

1) bills with nonzero kwh (kw) consumption and zero kw (kwh) consumption don't alter the results much
2) DR somewhat alters the results ==> get rid of it
3) using billing vs interval kwh alters the results. we know billing has measurement error, so don't use it
4) kw is more important for explaining differences than kwh==> focus on this
*/


***Solve systematic problems

*********************
*TOU-PA2A:
*********************

*there appears to be some bills which have a positive relationship between the difference and kw usage
*isolate potential bills
set scheme s1mono
local tag = "20201222"

use "$dirpath_data/merged_sce/bills_constructed_tests_`tag'.dta", replace


*Look at yearly and monthly correlations

levelsof temp_year , local(years) 
foreach i in `years' {
su prop_diff if tariff_sched_text=="TOU-PA2A" & dr_ind_current==0 & temp_year==`i', d
}

*2017-2019 looking strange

levelsof temp_month , local(months) 
foreach i in `months' {
su prop_diff if tariff_sched_text=="TOU-PA2A" & dr_ind_current==0 & temp_year==2019 & temp_month==`i', d
}

*2016: Jan and Dec are looking strange
*2017: Jan-March and Nov-Dec are looking strange
*2018: Jan-Feb + Dec are looking strange
*2019: everything is screwed


levelsof temp_year, local(years)
levelsof temp_month, local(months)
foreach mt in `months' {
foreach yrs in `years' {
gen y_line_`mt'_`yrs' =0
}
}

levelsof temp_month if temp_month<=7, local(months)
foreach mt in `months' {
reg diff monthly_max_kw if diff>100 &  tariff_sched_text=="TOU-PA2A" & dr_ind_current==0 & temp_year==2019 & temp_month==`mt' //diff>100 & 
}

replace y_line_1_2016=-100+(5*monthly_max_kw) &  tariff_sched_text=="TOU-PA2A"
replace y_line_12_2016=-150+(8*monthly_max_kw) &  tariff_sched_text=="TOU-PA2A"

replace y_line_1_2017=-100+(8*monthly_max_kw) &  tariff_sched_text=="TOU-PA2A"
replace y_line_2_2017=-100+(7*monthly_max_kw) &  tariff_sched_text=="TOU-PA2A"
replace y_line_3_2017=-100+(7*monthly_max_kw) &  tariff_sched_text=="TOU-PA2A"
replace y_line_11_2017=-100+(7.5*monthly_max_kw) &  tariff_sched_text=="TOU-PA2A"
replace y_line_12_2017=-100+(7.5*monthly_max_kw) &  tariff_sched_text=="TOU-PA2A"

replace y_line_1_2018=-100+(4.5*monthly_max_kw) &  tariff_sched_text=="TOU-PA2A"
replace y_line_2_2018=-100+(4.5*monthly_max_kw) &  tariff_sched_text=="TOU-PA2A"
replace y_line_10_2018=-100+(7.5*monthly_max_kw) &  tariff_sched_text=="TOU-PA2A"
replace y_line_11_2018=-100+(7.5*monthly_max_kw) &  tariff_sched_text=="TOU-PA2A"
replace y_line_12_2018=-120+(7.5*monthly_max_kw) &  tariff_sched_text=="TOU-PA2A"

replace y_line_1_2019=-100+(7*monthly_max_kw) &  tariff_sched_text=="TOU-PA2A"
replace y_line_2_2019=-100+(7*monthly_max_kw) &  tariff_sched_text=="TOU-PA2A"

*2016: Jan and Dec are looking strange
*2017: weird observations seem to be driving things. strange upward trend early (jan-March) + late in year (Sept-Dec)
*2018: same as above (Jan-Feb) + (Oct-Dec)
*2019: same as above (Jan-Feb) 

levelsof tariff_sched_text if tariff_sched_text=="TOU-PA2A", local(levs) //  | tariff_sched_text=="TOU-PA2B" | tariff_sched_text=="TOU-PA3B" /// | tariff_sched_text=="TOU-PA2D"
levelsof temp_year if temp_year>2015, local(years)
levelsof temp_month, local(months)
levelsof temp_season, local(seasons)

foreach rt in `levs' {
foreach mt in `months' {
foreach yrs in `years' {

	twoway (scatter  diff monthly_max_kw ///
			if temp_year==`yrs' & temp_month==`mt'  & total_bill_kwh!=0 & total_bill_kwh!=. & tariff_sched_text=="`rt'" & total_bill_amount>=0 ///
			& diff>=-2000 & diff<=2000 & dr_ind_current==0 & monthly_max_kw<210, msize(tiny) ///  & keep==1
			) ///
		(line zero monthly_max_kw ///
			if temp_year==`yrs' & temp_month==`mt' & total_bill_kwh!=0 & total_bill_kwh!=. & tariff_sched_text=="`rt'" & total_bill_amount>=0 ///
			& tariff_sched_text=="`rt'"  & dr_ind_current==0 & monthly_max_kw<210 /// & temp_season=="`yr'"
			& diff>=-2000 & diff<=2000, msize(tiny) ///  & max_total_bill_kwh_winter<=500  & keep==1
			), ///
		title("`rt' `mt' `yrs'") legend(label(1 "Energy Usage (KW)") label(2 "No difference") note("Difference bounded between +-2000"))	ytitle("Difference ($)") ///
		xtitle("Energy Usage (KW)")
		
		cd "$dirpath/data/figures" 
		graph save `rt'_`yrs'_`mt'_kw_trim, replace
		graph export `rt'_`yrs'_`mt'_kw_trim.pdf, replace
		erase `rt'_`yrs'_`mt'_kw_trim.gph 
		*erase `rt'_`yr'.gph 
/*
	twoway (scatter  diff monthly_max_kw ///
			if temp_year==`yrs' & temp_month==`mt'  & total_bill_kwh!=0 & total_bill_kwh!=. & tariff_sched_text=="`rt'" & total_bill_amount>=0 ///
			& diff>=50 & diff<=2000 & dr_ind_current==0 & monthly_max_kw<140, msize(tiny) ///  & keep==1
			) ///
		(line y_line_`mt'_`yrs' monthly_max_kw ///
		if temp_year==`yrs' & temp_month==`mt' & total_bill_kwh!=0 & total_bill_kwh!=. & tariff_sched_text=="`rt'" & total_bill_amount>=0 ///
		& tariff_sched_text=="`rt'"  & dr_ind_current==0 & monthly_max_kw<140 /// & temp_season=="`yr'"
		& diff>=50 & diff<=2000, msize(tiny) ///  & max_total_bill_kwh_winter<=500  & keep==1
		) ///
		(line zero monthly_max_kw ///
			if temp_year==`yrs' & temp_month==`mt' & total_bill_kwh!=0 & total_bill_kwh!=. & tariff_sched_text=="`rt'" & total_bill_amount>=0 ///
			& tariff_sched_text=="`rt'"  & dr_ind_current==0 & monthly_max_kw<140 /// & temp_season=="`yr'"
			& diff>=50 & diff<=2000, msize(tiny) ///  & max_total_bill_kwh_winter<=500  & keep==1
			), ///
		title("`rt' `mt' `yrs'") legend(label(1 "Energy Usage (KW)") label(2 "No difference") note("Difference bounded between +-2000"))	ytitle("Difference ($)") ///
		xtitle("Energy Usage (KW)")
		
		cd "$dirpath/data/figures" 
		graph save `rt'_`yrs'_`mt'_kw_fit, replace
		graph export `rt'_`yrs'_`mt'_kw_fit.pdf, replace
		erase `rt'_`yrs'_`mt'_kw_fit.gph 
		*erase `rt'_`yr'.gph 
	*/


}
}
}

gen strange=0

levelsof temp_year, local(years)
levelsof temp_month, local(months)
foreach mt in `months' {
foreach yrs in `years' {
replace strange=1 if diff>=y_line_`mt'_`yrs' & diff>100 &  monthly_max_kw<140 ///
	& temp_year==`yrs' & temp_month==`mt' & y_line_`mt'_`yrs'>0 &  tariff_sched_text=="TOU-PA2A"
	
}
}


*find total amount of strange bills per year per customer per year
bysort temp_year sa_uuid: egen tot_strange_yr_hh=sum(strange) 
tab tot_strange_yr_hh if tot_strange_yr_hh>0
*in a given year, about 50% of households with at least 1 strange bill have multiple strange bills

*find total amount of strange bills per year per customer
bysort temp_year sa_uuid: egen max_strange_yr_hh=max(strange)

preserve
duplicates drop sa_uuid temp_year, force
*for a given customer, find the total number of years with at least one strange bill
bysort sa_uuid : egen tot_strange_sample=sum(max_strange_yr_hh)
keep sa_uuid tot_strange_sample
duplicates drop
tempfile temp_interval
save `temp_interval'
restore
merge m:1 sa_uuid using `temp_interval'
drop _merge

*about 60% of customers with at least 1 strange bill in the sample have a strange bill in multiple years
tab tot_strange_sample if tot_strange_sample>0
tab tot_strange_sample if tot_strange_sample>0 & tot_strange_yr_hh>1

gen strange_multiple=1 if tot_strange_sample>1
replace strange_multiple=0 if strange_multiple==.

*some customers who have at least 1 strange bill ever also have a different tariff at some point in time


preserve
keep if strange_multiple==1
gen switcher=1 if tariff_sched_text!="TOU-PA2A"
replace switcher=0 if switcher==.
bysort sa_uuid: egen ever_switch=max(switcher)
keep ever_switch sa_uuid
duplicates drop
egen mean_switch=mean(ever_switch)
tempfile temp_interval
save `temp_interval'
restore
merge m:1 sa_uuid using `temp_interval'


/*Generally speaking, most strange bills are concentrated among the same customers so it may not be a tariff
problem but a customer problem. Also, strange bills are a very small minority (i.e. .6% of the 
total sample and under 3% of the tariff sample). Dropping customers outright is more costly than dropping bills,
as dropping all customers with strange bills in multiple years is 15% of the tariff and 4% of the total sample.
We opt to drop these guys anyway. Something weird is that 81% of these guys switch tariffs at some point in the 
sample period, so dropping these customers wil reduce observations for other tariffs (but this is inconsequential)
May be worth looking at further at a later date. */

levelsof tariff_sched_text if tariff_sched_text=="TOU-PA2A", local(levs) //  | tariff_sched_text=="TOU-PA2B" | tariff_sched_text=="TOU-PA3B" /// | tariff_sched_text=="TOU-PA2D"
levelsof temp_year if temp_year==2019, local(years)
levelsof temp_month if temp_month==1 | temp_month==2, local(months)
levelsof temp_season, local(seasons)

foreach rt in `levs' {
foreach mt in `months' {
foreach yrs in `years' {

	twoway (scatter  diff monthly_max_kw ///
			if temp_year==`yrs' & temp_month==`mt'  & total_bill_kwh!=0 & total_bill_kwh!=. & tariff_sched_text=="`rt'" & total_bill_amount>=0 ///
			& diff>=-2000 & diff<=2000 & dr_ind_current==0 & monthly_max_kw<210  & tot_strange_sample<=1 , msize(tiny) ///  & keep==1
			) ///
		(line zero monthly_max_kw ///
			if temp_year==`yrs' & temp_month==`mt' & total_bill_kwh!=0 & total_bill_kwh!=. & tariff_sched_text=="`rt'" & total_bill_amount>=0 ///
			& tariff_sched_text=="`rt'"  & dr_ind_current==0 & monthly_max_kw<210 /// & temp_season=="`yr'"
			& diff>=-2000 & diff<=2000  & tot_strange_sample<=1 , msize(tiny) ///  & max_total_bill_kwh_winter<=500  & keep==1
			), ///
		title("`rt' `mt' `yrs'") legend(label(1 "Energy Usage (KW)") label(2 "No difference") note("Difference bounded between +-2000"))	ytitle("Difference ($)") ///
		xtitle("Energy Usage (KW)")
		
		cd "$dirpath/data/figures" 
		graph save `rt'_`yrs'_`mt'_kw_no_strange, replace
		graph export `rt'_`yrs'_`mt'_kw_no_strange.pdf, replace
		erase `rt'_`yrs'_`mt'_kw_no_strange.gph 
		*erase `rt'_`yr'.gph 


}
}
}

*********************
*TOU-PA2B:
*********************

levelsof temp_year , local(years) 
foreach i in `years' {
su prop_diff if tariff_sched_text=="TOU-PA2B" & dr_ind_current==0 & temp_year==`i', d
}

*2018 looking strange

levelsof temp_month , local(months) 
foreach i in `months' {
su prop_diff if tariff_sched_text=="TOU-PA2B" & dr_ind_current==0 & temp_year==2018 & temp_month==`i', d
}

*Jan and May are looking strange

levelsof tariff_sched_text if tariff_sched_text=="TOU-PA2B", local(levs) //  | tariff_sched_text=="TOU-PA2B" | tariff_sched_text=="TOU-PA3B" /// | tariff_sched_text=="TOU-PA2D"
levelsof temp_year if temp_year==2018, local(years)
levelsof temp_month, local(months)
levelsof temp_season, local(seasons)

foreach rt in `levs' {
foreach mt in `months' {
foreach yrs in `years' {

	twoway (scatter  diff monthly_max_kw ///
			if temp_year==`yrs' & temp_month==`mt'  & total_bill_kwh!=0 & total_bill_kwh!=. & tariff_sched_text=="`rt'" & total_bill_amount>=0 ///
			& diff>=-2000 & diff<=2000 & dr_ind_current==0 & monthly_max_kw<210, msize(tiny) ///  & keep==1
			) ///
		(line zero monthly_max_kw ///
			if temp_year==`yrs' & temp_month==`mt' & total_bill_kwh!=0 & total_bill_kwh!=. & tariff_sched_text=="`rt'" & total_bill_amount>=0 ///
			& tariff_sched_text=="`rt'"  & dr_ind_current==0 & monthly_max_kw<210 /// & temp_season=="`yr'"
			& diff>=-2000 & diff<=2000, msize(tiny) ///  & max_total_bill_kwh_winter<=500  & keep==1
			), ///
		title("`rt' `mt' `yrs'") legend(label(1 "Energy Usage (KW)") label(2 "No difference") note("Difference bounded between +-2000"))	ytitle("Difference ($)") ///
		xtitle("Energy Usage (KW)")
		
		cd "$dirpath/data/figures" 
		graph save `rt'_`yrs'_`mt'_kw_trim, replace
		graph export `rt'_`yrs'_`mt'_kw_trim.pdf, replace
		erase `rt'_`yrs'_`mt'_kw_trim.gph 
		*erase `rt'_`yr'.gph 


}
}
}

/*The issue has been found: there was an error in Chin's data whereby a date for this rate in 2018
was stored but didn't actually exist on the pdf. This meant that months Jan-May were messed up.
this has been resolved*/

*********************
*TOU-PA2D:
*********************


levelsof temp_month if temp_month<=7, local(months) 
foreach i in `months' {
su prop_diff if tariff_sched_text=="TOU-PA2D" & dr_ind_current==0 & temp_year==2019 & temp_month==`i', d
}

*July is driving things

levelsof tariff_sched_text if tariff_sched_text=="TOU-PA2D", local(levs) //  | tariff_sched_text=="TOU-PA2B" | tariff_sched_text=="TOU-PA3B" /// | tariff_sched_text=="TOU-PA2D"
levelsof temp_year if temp_year==2019, local(years)
levelsof temp_month if temp_month<=7, local(months) 
levelsof temp_season, local(seasons)

foreach rt in `levs' {
foreach mt in `months' {
foreach yrs in `years' {

	twoway (scatter  diff monthly_max_kw ///
			if temp_year==`yrs' & temp_month==`mt'  & total_bill_kwh!=0 & total_bill_kwh!=. & tariff_sched_text=="`rt'" & total_bill_amount>=0 ///
			& diff>=-2000 & diff<=2000 & dr_ind_current==0 & monthly_max_kw<210, msize(tiny) ///  & keep==1
			) ///
		(line zero monthly_max_kw ///
			if temp_year==`yrs' & temp_month==`mt' & total_bill_kwh!=0 & total_bill_kwh!=. & tariff_sched_text=="`rt'" & total_bill_amount>=0 ///
			& tariff_sched_text=="`rt'"  & dr_ind_current==0 & monthly_max_kw<210 /// & temp_season=="`yr'"
			& diff>=-2000 & diff<=2000, msize(tiny) ///  & max_total_bill_kwh_winter<=500  & keep==1
			), ///
		title("`rt' `mt' `yrs'") legend(label(1 "Energy Usage (KW)") label(2 "No difference") note("Difference bounded between +-2000"))	ytitle("Difference ($)") ///
		xtitle("Energy Usage (KW)")
		
		cd "$dirpath/data/figures" 
		graph save `rt'_`yrs'_`mt'_kw_trim, replace
		graph export `rt'_`yrs'_`mt'_kw_trim.pdf, replace
		erase `rt'_`yrs'_`mt'_kw_trim.gph 
		*erase `rt'_`yr'.gph 
	
}
}
}


/*we are clearly under-estimating bills in July, but this is because there is no interval data beyond July ==>
volumetric charges will be underestimatted for bills with overlap between July and August (same for every tariff)*/


*********************
*TOU-PA2E:
*********************


levelsof temp_month if temp_month<=7, local(months) 
foreach i in `months' {
su prop_diff if tariff_sched_text=="TOU-PA2E" & dr_ind_current==0 & temp_year==2019 & temp_month==`i', d
}

*July is driving things

levelsof tariff_sched_text if tariff_sched_text=="TOU-PA2E", local(levs) //  | tariff_sched_text=="TOU-PA2B" | tariff_sched_text=="TOU-PA3B" /// | tariff_sched_text=="TOU-PA2D"
levelsof temp_year if temp_year==2019, local(years)
levelsof temp_month if temp_month>=3 & temp_month<=7, local(months) 
levelsof temp_season, local(seasons)

foreach rt in `levs' {
foreach mt in `months' {
foreach yrs in `years' {

	twoway (scatter  diff monthly_max_kw ///
			if temp_year==`yrs' & temp_month==`mt'  & total_bill_kwh!=0 & total_bill_kwh!=. & tariff_sched_text=="`rt'" & total_bill_amount>=0 ///
			& diff>=-2000 & diff<=2000 & dr_ind_current==0 & monthly_max_kw<210, msize(tiny) ///  & keep==1
			) ///
		(line zero monthly_max_kw ///
			if temp_year==`yrs' & temp_month==`mt' & total_bill_kwh!=0 & total_bill_kwh!=. & tariff_sched_text=="`rt'" & total_bill_amount>=0 ///
			& tariff_sched_text=="`rt'"  & dr_ind_current==0 & monthly_max_kw<210 /// & temp_season=="`yr'"
			& diff>=-2000 & diff<=2000, msize(tiny) ///  & max_total_bill_kwh_winter<=500  & keep==1
			), ///
		title("`rt' `mt' `yrs'") legend(label(1 "Energy Usage (KW)") label(2 "No difference") note("Difference bounded between +-2000"))	ytitle("Difference ($)") ///
		xtitle("Energy Usage (KW)")
		
		cd "$dirpath/data/figures" 
		graph save `rt'_`yrs'_`mt'_kw_trim, replace
		graph export `rt'_`yrs'_`mt'_kw_trim.pdf, replace
		erase `rt'_`yrs'_`mt'_kw_trim.gph 
		*erase `rt'_`yr'.gph 
	


}
}
}


/*we are clearly under-estimating bills in July, but this is because there is no interval data beyond July ==>
volumetric charges will be underestimatted for bills with overlap between July and August (same for every tariff)*/

*********************
*TOU-PA3B:
*********************


levelsof temp_year , local(years) 
foreach i in `years' {
su prop_diff if tariff_sched_text=="TOU-PA3B" & dr_ind_current==0 & temp_year==`i', d
}

*2019 looking strange

levelsof temp_month if temp_month<=7, local(months) 
foreach i in `months' {
su prop_diff if tariff_sched_text=="TOU-PA3B" & dr_ind_current==0 & temp_year==2019 & temp_month==`i', d
}

*Jun and April are looking strange (ignoring July becuase its the last month) but all are too small to matter

levelsof tariff_sched_text if tariff_sched_text=="TOU-PA3B", local(levs) //  | tariff_sched_text=="TOU-PA2B" | tariff_sched_text=="TOU-PA3B" /// | tariff_sched_text=="TOU-PA2D"
levelsof temp_year if temp_year==2019, local(years)
levelsof temp_month, local(months)
levelsof temp_season, local(seasons)

foreach rt in `levs' {
foreach mt in `months' {
foreach yrs in `years' {

	twoway (scatter  diff monthly_max_kw ///
			if temp_year==`yrs' & temp_month==`mt'  & total_bill_kwh!=0 & total_bill_kwh!=. & tariff_sched_text=="`rt'" & total_bill_amount>=0 ///
			& diff>=-2000 & diff<=2000 & dr_ind_current==0 & monthly_max_kw<500, msize(tiny) ///  & keep==1
			) ///
		(line zero monthly_max_kw ///
			if temp_year==`yrs' & temp_month==`mt' & total_bill_kwh!=0 & total_bill_kwh!=. & tariff_sched_text=="`rt'" & total_bill_amount>=0 ///
			& tariff_sched_text=="`rt'"  & dr_ind_current==0 & monthly_max_kw<500 /// & temp_season=="`yr'"
			& diff>=-2000 & diff<=2000, msize(tiny) ///  & max_total_bill_kwh_winter<=500  & keep==1
			), ///
		title("`rt' `mt' `yrs'") legend(label(1 "Energy Usage (KW)") label(2 "No difference") note("Difference bounded between +-2000"))	ytitle("Difference ($)") ///
		xtitle("Energy Usage (KW)")
		
		cd "$dirpath/data/figures" 
		graph save `rt'_`yrs'_`mt'_kw_trim, replace
		graph export `rt'_`yrs'_`mt'_kw_trim.pdf, replace
		erase `rt'_`yrs'_`mt'_kw_trim.gph 
		*erase `rt'_`yr'.gph 


}
}
}

*This looks like there are no systematic errors and rates appear to be fine

** Create flag for non-zero bills that are off by more than 100% 
gen flag_bill_constr_error100 = total_bill_kwh>0 & total_bill_kwh!=. & ///
	(total_bill_amount/total_bill_amount_constr>2 | total_bill_amount_constr/total_bill_amount>2)
tab flag_bill_constr_error100
tab flag_bill_constr_error100 if temp_regular_bill==1
tab flag_bill_constr_error100 if temp_regular_bill==1 & rt_sched_cd!="AG_4B" 
la var flag_bill_constr_error100 "Flag = 1 for non-zero kWh bills with 100% discrepancy btw $ and $hat"

** Save
drop temp*
sort sa_uuid bill_start_dt
unique sa_uuid bill_start_dt
assert r(unique)==r(N)
compress
save "$dirpath_data/merged_pge/bills_rates_constructed.dta", replace
 
}
}

