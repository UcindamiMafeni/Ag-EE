clear all
version 13
set more off

***********************************************************************************
**** Script to create analysis datasets monthly/hourly electricity regressions ****
***********************************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

** PENDING:

** Files for hourly regressions (at SA and SP levels)
** Add GIS variables

*******************************************************************************
*******************************************************************************

** 1. Monthified billing data with prices, at SA and SP levels
if 1==0{

** Start with cleaned customer + monthified billing data (all three data pulls)
foreach tag in 20180719 20180322 20180827 {
		
	// Load customer data
	use "$dirpath_data/pge_cleaned/pge_cust_detail_`tag'.dta", clear
	
	// Flag which pull these data are from
	gen pull = "`tag'"
	la var pull "Which pull are these data from?"
	
	// Keep relevant variable
	keep prsn_uuid sp_uuid sa_uuid prem_lat prem_long net_mtr_ind dr_ind dr_program ///
		climate_zone_cd sa_sp_start sa_sp_stop in_calif in_pge in_pou pou_name ///
		bad_geocode_flag climate_zone_cd_gis bad_cz_flag missing_geocode_flag ///
		in_billing bill_dt_first bill_dt_last in_interval interval_dt_first ///
		interval_dt_last ee_measure_count pull
		
	// Confirm uniqueness
	duplicates drop
	unique sp_uuid sa_uuid
	assert r(unique)==r(N)
	
	// Drop if not in billing data
	drop if in_billing==0
	drop in_billing
	
	// Merge in monthified billing data
	joinby sa_uuid using "$dirpath_data/pge_cleaned/billing_data_monthified_`tag'.dta", ///
		unmatched(master)
	assert _merge!=1 | bill_dt_first==.
	drop _merge
	
	// Partially de-dupify SA/SPs using SP xwalk
	gen temp_to_keep = sp_uuid==sp_uuid1 | sp_uuid==sp_uuid2 | sp_uuid==sp_uuid3
	egen temp_to_keep_max = max(temp_to_keep), by(sa_uuid modate)
	tab temp*
	drop if temp_to_keep==0 & temp_to_keep_max==1
	drop sp_uuid? temp*

	// Save as temp file
	compress
	tempfile merged`tag'
	save `merged`tag''
	
}

** Merge together monthified billing data across all three data pulls
use `merged20180719', clear
merge 1:1 sp_uuid sa_uuid modate using `merged20180322'
assert pull=="20180322" if _merge==2
assert pull=="20180719" if _merge!=2
drop _merge
merge 1:1 sp_uuid sa_uuid modate using `merged20180827'
assert pull=="20180827" if _merge==2
assert pull!="20180827" if _merge!=2 
drop _merge

** Merge in monthified billing data
rename rt_sched_cd RT_sched_cd 
merge m:1 sa_uuid modate pull using "$dirpath_data/merged/monthified_avg_prices_nomerge"
tab RT_sched_cd if _merge==1 // non-AG rates
drop if _merge==1
drop RT_sched_cd _merge
		
** Deal with duplicates SP/SAs
duplicates t sa_uuid modate, gen(dup)
tab dup
sort sa_uuid modate sp_uuid
//br if dup>0 // Virtually all have identical lat/lons
	
	// Sum up frequencies of SP
preserve 
contract sp_uuid, freq(temp_sp_freq)
tempfile sp_freq
save `sp_freq'
restore
merge m:1 sp_uuid using `sp_freq', nogen

	// Drop duplicates with lower SP frequency
egen temp_sp_freq_max = max(temp_sp_freq) if dup>0, by(sa_uuid modate)
unique sa_uuid modate
local uniq = r(unique)
drop if dup>0 & temp_sp_freq<temp_sp_freq_max
unique sa_uuid modate
assert r(unique)==`uniq'

	// Flag remaining dups
duplicates t sa_uuid modate, gen(dup2)
tab dup2
sort sa_uuid modate sp_uuid
//br sa_uuid modate sp_uuid dup dup2 temp_sp_freq pull if dup2>0

	// Pick an SP at random
unique sa_uuid modate
local uniq = r(unique)
drop if dup2>0 & sa_uuid==sa_uuid[_n-1] & modate==modate[_n-1]
unique sa_uuid modate
assert r(unique)==`uniq'

	// Confirm uniqueness
assert r(unique)==r(N)	
drop dup* temp*

** Save uncollapsed version at the SA-month level (allowing us to play with tariffs in regressions)
sort sp_uuid modate sa_uuid
compress
save "$dirpath_data/merged/sa_month_elec_panel.dta", replace

** Collapse to SP-month level
duplicates t sp_uuid modate, gen(dup)
sort sp_uuid modate sa_sp_start sa_uuid
br if dup>0 // lots of mid-month bill changeovers, also lots of multi-SA SPs

	// Take max of flag variables
foreach v of varlist net_mtr_ind dr_ind in_calif in_pge in_pou bad_geocode_flag ///
	bad_cz_flag missing_geocode_flag in_interval flag_* interval_bill_corr {
	egen temp = max(`v'), by(sp_uuid modate)
	replace `v' = temp if dup>0
	drop temp
}

	// Take min/max of date variables
foreach v of varlist sa_sp_start bill_dt_first interval_dt_first min_p_kwh {
	egen temp = min(`v'), by(sp_uuid modate)
	replace `v' = temp if dup>0
	drop temp
}
foreach v of varlist sa_sp_stop bill_dt_last interval_dt_last max_p_kwh {
	egen temp = max(`v'), by(sp_uuid modate)
	replace `v' = temp if dup>0
	drop temp
}
drop day_first day_last

	// Take mode of strings
foreach v of varlist dr_program pou_name rt_sched_cd {
	egen temp = mode(`v'), by(sp_uuid modate) minmode
	replace `v' = temp if dup>0
	drop temp
}

	// Take weight-averages of mean price variables
foreach v of varlist  mean_p_kwh mean_p_kw_max mean_p_kw_peak mean_p_kw_partpeak {
	egen double temp_num1 = sum(`v'*mnth_bill_kwh) if `v'!=. & mnth_bill_kwh!=. & mnth_bill_kwh>=0, by(sp_uuid modate)
	egen double temp_num2 = mean(temp_num1), by(sp_uuid modate)
	egen double temp_denom1 = sum(mnth_bill_kwh) if `v'!=. & mnth_bill_kwh!=. & mnth_bill_kwh>=0, by(sp_uuid modate)
	egen double temp_denom2 = mean(temp_denom1), by(sp_uuid modate)
	replace `v' = temp_num2/temp_denom2 if temp_denom2!=0 & temp_denom2!=.
	egen temp = mean(`v'), by(sp_uuid modate)
	replace `v' = temp if temp_denom2==0 | temp_denom2==.
	drop temp*
} 

	// Sum  bill kWh and $, and count of EE measures
foreach v of varlist mnth_bill_kwh mnth_bill_amount ee_measure_count {
	egen double temp = sum(`v'), by(sp_uuid modate)
	replace `v' = temp if dup>0
	drop temp
}	

	// Collapse!
drop prsn_uuid sa_uuid days dup
duplicates drop

	// Drop with remaining dups (SPs split across multiple pulls)
duplicates t sp_uuid modate, gen(dup)
egen temp_max = max(pull=="20180322"), by(sp_uuid modate)
egen temp_min = min(pull=="20180322"), by(sp_uuid modate)
unique sp_uuid modate
local uniq = r(unique)
drop if temp_min<temp_max & pull=="20180827" & dup>0
unique sp_uuid modate
assert r(unique)==`uniq'
drop dup temp*

	// Confirm uniqueness
assert r(unique)==r(N)

** Save
sort sp_uuid modate
compress
save "$dirpath_data/merged/sp_month_elec_panel.dta", replace

}

*******************************************************************************
*******************************************************************************	
	
** 2. Billing data without prices, at SA level
if 1==0{

** Start with cleaned customer + monthified billing data (all three data pulls)
foreach tag in 20180719 20180322 20180827 {
		
	// Load customer data
	use "$dirpath_data/pge_cleaned/pge_cust_detail_`tag'.dta", clear
	
	// Flag which pull these data are from
	gen pull = "`tag'"
	la var pull "Which pull are these data from?"
	
	// Keep relevant variable
	keep prsn_uuid sp_uuid sa_uuid prem_lat prem_long net_mtr_ind dr_ind dr_program ///
		climate_zone_cd sa_sp_start sa_sp_stop in_calif in_pge in_pou pou_name ///
		bad_geocode_flag climate_zone_cd_gis bad_cz_flag missing_geocode_flag ///
		in_billing bill_dt_first bill_dt_last in_interval interval_dt_first ///
		interval_dt_last ee_measure_count pull
		
	// Confirm uniqueness
	duplicates drop
	unique sp_uuid sa_uuid
	assert r(unique)==r(N)
	
	// Drop if not in billing data
	drop if in_billing==0
	drop in_billing
	
	// Merge in monthified billing data
	joinby sa_uuid using "$dirpath_data/pge_cleaned/billing_data_`tag'.dta", ///
		unmatched(master)
	assert _merge!=1 | bill_dt_first==.
	drop _merge
	
	// Partially de-dupify SA/SPs using SP xwalk
	gen temp_to_keep = sp_uuid==sp_uuid1 | sp_uuid==sp_uuid2 | sp_uuid==sp_uuid3
	egen temp_to_keep_max = max(temp_to_keep), by(sa_uuid bill_start_dt)
	tab temp*
	drop if temp_to_keep==0 & temp_to_keep_max==1
	drop sp_uuid? temp*

	// Save as temp file
	compress
	tempfile merged`tag'
	save `merged`tag''
	
}

** Merge together monthified billing data across all three data pulls
use `merged20180719', clear
merge 1:1 sp_uuid sa_uuid bill_start_dt using `merged20180322'
assert pull=="20180322" if _merge==2
assert pull=="20180719" if _merge!=2
drop _merge
merge 1:1 sp_uuid sa_uuid bill_start_dt using `merged20180827'
assert pull=="20180827" if _merge==2
assert pull!="20180827" if _merge!=2 
drop _merge

** Deal with duplicates SP/SAs
duplicates t sa_uuid bill_start_dt, gen(dup)
tab dup
sort sa_uuid bill_start_dt sp_uuid
//br if dup>0 // Virtually all have identical lat/lons
	
	// Sum up frequencies of SP
preserve 
contract sp_uuid, freq(temp_sp_freq)
tempfile sp_freq
save `sp_freq'
restore
merge m:1 sp_uuid using `sp_freq', nogen

	// Drop duplicates with lower SP frequency
egen temp_sp_freq_max = max(temp_sp_freq) if dup>0, by(sa_uuid bill_start_dt)
unique sa_uuid bill_start_dt
local uniq = r(unique)
drop if dup>0 & temp_sp_freq<temp_sp_freq_max
unique sa_uuid bill_start_dt
assert r(unique)==`uniq'

	// Flag remaining dups
duplicates t sa_uuid bill_start_dt, gen(dup2)
tab dup2
sort sa_uuid bill_start_dt sp_uuid
//br sa_uuid bill_start_dt sp_uuid dup dup2 temp_sp_freq pull if dup2>0

	// Pick an SP at random
unique sa_uuid bill_start_dt
local uniq = r(unique)
drop if dup2>0 & sa_uuid==sa_uuid[_n-1] & bill_start_dt==bill_start_dt[_n-1]
unique sa_uuid bill_start_dt
assert r(unique)==`uniq'

	// Confirm uniqueness
assert r(unique)==r(N)	
drop dup* temp*
	
** Save panel unique by SA-bill
compress
save "$dirpath_data/merged/sa_bill_elec_panel.dta", replace

}

*******************************************************************************
*******************************************************************************	
	
** 3. Interval data with prices, at SP level
if 1==0{

** Merge with hourly data, for each data pull
foreach tag in "20180719" "20180322" "20180827" {

	** Load full SA-bill panel dataset
	use "$dirpath_data/merged/sa_bill_elec_panel.dta", clear

	** Keep SAs in interval data
	keep if in_interval==1

	** Drop bills before (after) first (last) appearance in interval data
	drop if bill_end_dt<interval_dt_first
	drop if bill_start_dt>interval_dt_last

	** Drop variables not needed for merge
	keep sp_uuid sa_uuid bill_start_dt pull

	** Merge into hourly interval data with prices
	merge 1:m sa_uuid bill_start_dt using "$dirpath_data/merged/hourly_with_prices_`tag'.dta", ///
		keep(2 3)
	assert _merge!=2
	cap drop group
	tab pull
	
	** Drop unnecessary variables 
	drop sa_uuid bill_start_dt _merge pull	
		
	** Collapse to SP-hour level
	foreach v of varlist kwh {
		egen double temp = sum(`v'), by(sp_uuid date hour)
		replace `v' = temp 
		drop temp
	}
	foreach v of varlist p_kwh {
		egen double temp = mean(`v'), by(sp_uuid date hour)
		replace `v' = temp 
		drop temp
	}
	duplicates drop
	unique sp_uuid date hour
	assert r(unique)==r(N)
	la var date "Date"

	** Save
	sort sp_uuid date hour
	compress
	save "$dirpath_data/merged/sp_hourly_elec_panel_`tag'.dta", replace

}

}

*******************************************************************************
*******************************************************************************	

** 4. Transform Q and P, merge in instruments, and collapse SP-hour datasets
if 1==0{

foreach tag in "20180719" "20180322" "20180827" {

	** Load SP-hourly datasets
	use "$dirpath_data/merged/sp_hourly_elec_panel_`tag'.dta", clear

	** Drop negative kWh hours before transform/collapse
	drop if kwh<0
	
	** Log-transform marginal electricity price
	gen log_p = ln(p_kwh)

	** Inverse hyperbolic sine transorm electricity quantity
	gen ihs_kwh =  ln(1000000*kwh + sqrt((1000000*kwh)^2+1))

	** Month and year varialbes
	gen modate = ym(year(date), month(date))
	format %tm modate
	gen year = year(date)
	gen month = month(date)
	la var modate "Year-Month"
	la var year "Year"
	la var month "Month"
	
	** Weekend dummy
	gen weekend = inlist(dow(date),0,6)
	la var weekend "Dummy for weekend days"
	
	** Merge in instrument: E1 tariffs by day
	merge m:1 date using "$dirpath_data/merged/e1_prices_daily.dta", ///
		nogen keep(1 3)
	
	** Merge in instrument: E20 tariffs by hour
	merge m:1 date hour using "$dirpath_data/merged/e20_prices_hourly.dta", ///
		nogen keep(1 3)
	
	** Merge in instrument: default ag rates
	preserve
	keep sp_uuid modate
	duplicates drop
	merge 1:1 sp_uuid modate using "$dirpath_data/merged/sp_month_elec_panel.dta", ///
		nogen keep(1 3) keepusing(rt_sched_cd)
	joinby rt_sched_cd modate using "$dirpath_data/merged/ag_default_prices_hourly.dta", ///
		unmatched(master)
	drop _merge rt_sched_cd rt_default
	duplicates drop sp_uuid date hour, force // not sure why this is necessary; shouldn't be dups!
	tempfile sp_rates
	save `sp_rates'
	restore
	merge 1:1 sp_uuid date hour using `sp_rates', nogen keep(1 3)

	** Collapse
	gen fwt = 1
	collapse (sum) fwt (mean) ihs_kwh, by(sp_uuid hour log_p modate year month ///
		weekend p_kwh_e1_?? p_kwh_e20 p_kwh_ag_default) fast
	
	** Label
	la var log_p "Log of marginal electricity price ($/kWh)"
	la var ihs_kwh "Inverse hyperbolic sine of 1e6*kWh avg elec consumption"
	la var fwt "Frequency weight (post-collapse)"
	
	** Save 
	order sp_uuid modate hour ihs_kwh log_p year month weekend fwt  
	sort *
	compress
	save "$dirpath_data/merged/sp_hourly_elec_panel_collapsed_`tag'.dta", replace
	
}

}

*******************************************************************************
*******************************************************************************	

** 5. Classify SPs based on whether they switch ag rates
if 1==0{

** Start with SP-month panel
use "$dirpath_data/merged/sp_month_elec_panel.dta", clear
	// Note: I checked for cases of multiple rates within an SP-month, and they
	// are EXCEEDINGLY rare. That means that identifying switchers at the SP 
	// level will be virtually identical to identifying switchers at the SA levels
keep sp_uuid modate rt_sched_cd
encode rt_sched_cd, gen(rt_group)

** Flag SPs with the same rate always
egen temp_min1 = min(rt_group), by(sp_uuid)
egen temp_max1 = max(rt_group), by(sp_uuid)
gen sp_same_rate_always = temp_min1==temp_max1
egen sp_tag = tag(sp_uuid)
tab sp_same_rate_always if sp_tag


** Flag SPs who's only rate change is a dumb-to-smart switch within the same rate
gen rt_group_dumbsmart = rt_group

sum rt_group if rt_sched_cd=="AG-4D"
local from = r(mean)
sum rt_group if rt_sched_cd=="AG-4A"
local to = r(mean)
replace rt_group_dumbsmart = `to' if rt_group_dumbsmart==`from'

sum rt_group if rt_sched_cd=="AG-4E"
local from = r(mean)
sum rt_group if rt_sched_cd=="AG-4B"
local to = r(mean)
replace rt_group_dumbsmart = `to' if rt_group_dumbsmart==`from'

sum rt_group if rt_sched_cd=="AG-4F"
local from = r(mean)
sum rt_group if rt_sched_cd=="AG-4C"
local to = r(mean)
replace rt_group_dumbsmart = `to' if rt_group_dumbsmart==`from'

sum rt_group if rt_sched_cd=="AG-5D"
local from = r(mean)
sum rt_group if rt_sched_cd=="AG-5A"
local to = r(mean)
replace rt_group_dumbsmart = `to' if rt_group_dumbsmart==`from'

sum rt_group if rt_sched_cd=="AG-5E"
local from = r(mean)
sum rt_group if rt_sched_cd=="AG-5B"
local to = r(mean)
replace rt_group_dumbsmart = `to' if rt_group_dumbsmart==`from'

sum rt_group if rt_sched_cd=="AG-5F"
local from = r(mean)
sum rt_group if rt_sched_cd=="AG-5C"
local to = r(mean)
replace rt_group_dumbsmart = `to' if rt_group_dumbsmart==`from'

sum rt_group if rt_sched_cd=="AG-RD"
local from = r(mean)
sum rt_group if rt_sched_cd=="AG-RA"
local to = r(mean)
replace rt_group_dumbsmart = `to' if rt_group_dumbsmart==`from'

sum rt_group if rt_sched_cd=="AG-RE"
local from = r(mean)
sum rt_group if rt_sched_cd=="AG-RB"
local to = r(mean)
replace rt_group_dumbsmart = `to' if rt_group_dumbsmart==`from'

sum rt_group if rt_sched_cd=="AG-VD"
local from = r(mean)
sum rt_group if rt_sched_cd=="AG-VA"
local to = r(mean)
replace rt_group_dumbsmart = `to' if rt_group_dumbsmart==`from'

sum rt_group if rt_sched_cd=="AG-VE"
local from = r(mean)
sum rt_group if rt_sched_cd=="AG-VB"
local to = r(mean)
replace rt_group_dumbsmart = `to' if rt_group_dumbsmart==`from'

egen temp_min2 = min(rt_group_dumbsmart), by(sp_uuid)
egen temp_max2 = max(rt_group_dumbsmart), by(sp_uuid)
gen sp_same_rate_dumbsmart = temp_min2==temp_max2

tab sp_same_rate_dumbsmart if sp_tag
assert sp_same_rate_dumbsmart==1 if sp_same_rate_always==1 

** Flag SPs who only change rates ACROSS categories
gen rt_category = 0
replace rt_category = 1 if rt_sched_cd=="AG-1A"
replace rt_category = 2 if rt_sched_cd=="AG-1B"
replace rt_category = 3 if rt_sched_cd=="AG-ICE"
replace rt_category = 4 if inlist(rt_sched_cd,"AG-4A","AG-5A","AG-RA","AG-VA")
replace rt_category = 4 if inlist(rt_sched_cd,"AG-4D","AG-5D","AG-RD","AG-VD")
replace rt_category = 5 if inlist(rt_sched_cd,"AG-4B","AG-5B","AG-RB","AG-VB")
replace rt_category = 5 if inlist(rt_sched_cd,"AG-4C","AG-5C")
replace rt_category = 5 if inlist(rt_sched_cd,"AG-4E","AG-5E","AG-RE","AG-VE")
replace rt_category = 5 if inlist(rt_sched_cd,"AG-4F","AG-5F")
assert rt_category!=0
	// The idea here is that PGE allows virtually no short-run flexibility across
	// 5 rate categories, and what I'm calling "categories" are determined by 
	// type of customer, size of installed motor, and max demand. So, if an SP
	// changes rates BETWEEN categories, this should reflect a structural change
	// in the customer, NOT  choosing a different price willy-nilly.
	
egen temp_min3 = min(rt_group_dumbsmart), by(sp_uuid rt_category)
egen temp_max3 = max(rt_group_dumbsmart), by(sp_uuid rt_category)
egen sp_same_rate_in_cat = min(temp_min3==temp_max3), by(sp_uuid)

tab sp_same_rate_in_cat if sp_tag
assert sp_same_rate_in_cat==1 if sp_same_rate_dumbsmart==1 | ///
	sp_same_rate_always==1

** Keep flags and collapse to SP level
keep sp_uuid sp_same_rate*
duplicates drop
unique sp_uuid
assert r(unique)==r(N)

** Label and save
la var sp_same_rate_always "Flag for SPs with same tariff always"
la var sp_same_rate_dumbsmart "Flag for SPs with same tariff always, incl dumb-to-smart switch"
la var sp_same_rate_in_cat "Flag for SPs with no rate switches w/in category"
sort sp_uuid
compress
save "$dirpath_data/merged/sp_rate_switchers.dta", replace

}

*******************************************************************************
*******************************************************************************

** 6. Transform Q and P, and merge GIS vars in to collapsed SP-month panel
if 1==1{

** Load monthly dataset
use "$dirpath_data/merged/sp_month_elec_panel.dta", clear

** Log-transform marginal electricity price
cap drop log_p_mean log_p_min log_p_max
gen log_p_mean = ln(mean_p_kwh)
gen log_p_min = ln(min_p_kwh)
gen log_p_max = ln(max_p_kwh)
la var log_p_mean "Log of mean marginal electricity price ($/kWh)"
la var log_p_min "Log of min marginal electricity price ($/kWh)"
la var log_p_max "Log of max marginal electricity price ($/kWh)"

** Inverse hyperbolic sine transorm electricity quantity
cap drop ihs_kwh
gen ihs_kwh =  ln(100*mnth_bill_kwh + sqrt((100*mnth_bill_kwh)^2+1))
replace ihs_kwh = . if mnth_bill_kwh<0
la var ihs_kwh "Inverse hyperbolic sine of 100*kWh avg elec consumption"

** Log-transform marginal electricity quantity
cap drop log_kwh log1_kwh 
gen log_kwh = ln(mnth_bill_kwh)
gen log1_kwh = ln(mnth_bill_kwh+1)
replace log1_kwh = . if mnth_bill_kwh<0
gen log_100kwh = ln(100*mnth_bill_kwh)
gen log1_100kwh = ln(100*mnth_bill_kwh+1)
replace log1_100kwh = . if mnth_bill_kwh<0
la var log_kwh "Log of kWh avg elec consumption"
la var log1_kwh "Log+1 of kWh avg elec consumption"
la var log_100kwh "Log of 100*kWh avg elec consumption"
la var log1_100kwh "Log+1 of 100*kWh avg elec consumption"

** Month and year varialbes
cap drop year month
gen year = real(substr(string(modate,"%tm"),1,4))
gen month = real(substr(string(modate,"%tm"),6,2))
drop if year<2008
assert year!=. & month!=.
la var year "Year"
la var month "Month"
		
** Merge in instrument: E1 tariffs by month
cap drop p_kwh_e1_??
merge m:1 modate using "$dirpath_data/merged/e1_prices_monthly.dta", nogen keep(1 3)
	
** Merge in instrument: average E20 tariffs by month
cap drop *_p_kwh_e20
merge m:1 modate using "$dirpath_data/merged/e20_prices_monthly.dta", nogen keep(1 3)
	
** Merge in instrument: default ag rates
cap drop rt_default *_p_kwh_ag_default
merge m:1 rt_sched_cd modate using "$dirpath_data/merged/ag_default_prices_monthly.dta", ///
	nogen keep(1 3)

** Merge in instrument: modal ag rates
cap drop rt_modal *_p_kwh_ag_modal
merge m:1 rt_sched_cd modate using "$dirpath_data/merged/ag_modal_prices_monthly.dta", ///
	nogen keep(1 3)
	
** Merge in GIS variables
cap drop wdist_group
cap drop county_group
cap drop basin_group
merge m:1 sp_uuid using "$dirpath_data/pge_cleaned/sp_premise_gis.dta", ///
	nogen keep(1 3) keepusing(wdist_group county_fips basin_id)
encode county_fips, gen(county_group)
la var county_group "County FIPS (numeric)"
egen basin_group = group(basin_id)
la var basin_group "Groundwater basin (numeric)"
		
** Merge in switchers indicators
cap drop sp_same_rate_*
merge m:1 sp_uuid using "$dirpath_data/merged/sp_rate_switchers.dta", nogen ///
	keep(1 3) keepusing(sp_same_rate_always sp_same_rate_dumbsmart sp_same_rate_in_cat)
		
** Merge in average groundwater depths (basin-by-quarter)
cap drop gw_qtr_bsn_*
gen quarter = .
replace quarter = 1 if inlist(month,1,2,3)
replace quarter = 2 if inlist(month,4,5,6)
replace quarter = 3 if inlist(month,7,8,9)
replace quarter = 4 if inlist(month,10,11,12)
merge m:1 basin_id year quarter using ///
	"$dirpath_data/groundwater/avg_groundwater_depth_basin_quarter_full.dta", ///
	nogen keep(1 3) keepusing(gw_qtr_bsn_mean1 gw_qtr_bsn_mean2)
drop quarter basin_id
	
** Create numeric versions of SP, climate zone, and rate
cap drop sp_group
cap drop cz_group
cap drop rt_group
egen sp_group = group(sp_uuid)
la var sp_group "SP identifier (not global; numeric)"
encode rt_sched_cd, gen(rt_group)
la var rt_group "Ag tariff group (numeric)"
encode climate_zone_cd, gen(cz_group)
la var cz_group "Climate zone (numeric)"

** Combine bad/missing geocode flags
cap drop flag_geocode_badmiss
gen flag_geocode_badmiss = bad_geocode_flag==1 | missing_geocode_flag==1
la var flag_geocode_badmiss "SP geocode either missing or not in California"

** Create flag for irregular bills
cap drop flag_irregular_bill
gen temp = flag_dup_partial_overlap!=0
egen flag_irregular_bill = rowmax(flag_multi_tariff flag_bad_tariff ///
	temp flag_dup_double_overlap flag_dup_bad_kwh flag_dup_overlap_missing ///
	flag_first_bill flag_last_bill flag_long_bill flag_short_bill flag_date_gap)
assert inlist(flag_irregular_bill,0,1)
la var flag_irregular_bill "Flag combining all weird bill indicators"
drop temp

** Create summer dummy
cap drop summer
gen summer = inlist(month,5,6,7,8,9,10)
la var summer "Summer dummy"

** Log-transform prices to be used as instruments
cap drop log_p_kwh_e1*
cap drop log_*p_kwh_e20
cap drop log_*p_kwh_ag_default
cap drop log_*p_kwh_ag_modal
foreach v of varlist p_kwh_e1* *p_kwh_e20 *p_kwh_ag_default *p_kwh_ag_modal {
	local lab: var label `v'
	gen log_`v' = ln(`v')
	la var log_`v' "Log `lab'"
}	

** Construct instruments of 6- and 12-month-lagged prices
cap drop log_p_m*_lag*
tsset sp_group modate 
gen log_p_mean_lag12 = L12.log_p_mean
gen log_p_min_lag12 = L12.log_p_min
gen log_p_max_lag12 = L12.log_p_max
gen log_p_mean_lag6 = L6.log_p_mean
gen log_p_min_lag6 = L6.log_p_min
gen log_p_max_lag6 = L6.log_p_max
la var log_p_mean_lag12 "Avg SP-specific marg elec price (log $/kWh), lagged 12 months"
la var log_p_min_lag12 "Min SP-specific marg elec price (log $/kWh), lagged 12 months"
la var log_p_max_lag12 "Max SP-specific marg elec price (log $/kWh), lagged 12 months"
la var log_p_mean_lag6 "Avg SP-specific marg elec price (log $/kWh), lagged 6 months"
la var log_p_min_lag6 "Min SP-specific marg elec price (log $/kWh), lagged 6 months"
la var log_p_max_lag6 "Max SP-specific marg elec price (log $/kWh), lagged 6 months"

** Construct instruments of 6- and 12-month-lagged DEFAULT prices
cap drop log_p_m*_deflag*
tsset sp_group modate 
gen log_p_mean_deflag12 = L12.log_mean_p_kwh_ag_default
gen log_p_min_deflag12 = L12.log_min_p_kwh_ag_default
gen log_p_max_deflag12 = L12.log_max_p_kwh_ag_default
gen log_p_mean_deflag6 = L6.log_mean_p_kwh_ag_default
gen log_p_min_deflag6 = L6.log_min_p_kwh_ag_default
gen log_p_max_deflag6 = L6.log_max_p_kwh_ag_default
la var log_p_mean_deflag12 "Avg SP-spec default marg elec price (log $/kWh), lagged 12 months"
la var log_p_min_deflag12 "Min SP-spec default marg elec price (log $/kWh), lagged 12 months"
la var log_p_max_deflag12 "Max SP-spec default marg elec price (log $/kWh), lagged 12 months"
la var log_p_mean_deflag6 "Avg SP-spec default marg elec price (log $/kWh), lagged 6 months"
la var log_p_min_deflag6 "Min SP-spec default marg elec price (log $/kWh), lagged 6 months"
la var log_p_max_deflag6 "Max SP-spec default marg elec price (log $/kWh), lagged 6 months"

** Construct instruments of 6- and 12-month-lagged MODAL prices
cap drop log_p_m*_modlag*
tsset sp_group modate 
gen log_p_mean_modlag12 = L12.log_mean_p_kwh_ag_modal
gen log_p_min_modlag12 = L12.log_min_p_kwh_ag_modal
gen log_p_max_modlag12 = L12.log_max_p_kwh_ag_modal
gen log_p_mean_modlag6 = L6.log_mean_p_kwh_ag_modal
gen log_p_min_modlag6 = L6.log_min_p_kwh_ag_modal
gen log_p_max_modlag6 = L6.log_max_p_kwh_ag_modal
la var log_p_mean_modlag12 "Avg SP-spec modal marg elec price (log $/kWh), lagged 12 months"
la var log_p_min_modlag12 "Min SP-spec modal marg elec price (log $/kWh), lagged 12 months"
la var log_p_max_modlag12 "Max SP-spec modal marg elec price (log $/kWh), lagged 12 months"
la var log_p_mean_modlag6 "Avg SP-spec modal marg elec price (log $/kWh), lagged 6 months"
la var log_p_min_modlag6 "Min SP-spec modal marg elec price (log $/kWh), lagged 6 months"
la var log_p_max_modlag6 "Max SP-spec modal marg elec price (log $/kWh), lagged 6 months"

** Create instrument: initial price
cap drop *_init
egen temp_min_date = min(modate), by(sp_uuid)
gen temp_first_rt1 = rt_sched_cd if modate==temp_min_date
egen temp_first_rt2 = mode(temp_first_rt1), by(sp_uuid)
foreach v of varlist rt_sched_cd mean_p_kwh min_p_kwh max_p_kwh {
	rename `v' `v'_TEMP
}
rename temp_first_rt2 rt_sched_cd
merge m:1 rt_sched_cd modate using "$dirpath_data/merged/ag_rates_avg_by_month.dta", ///
	nogen keep(1 3)
foreach v of varlist rt_sched_cd mean_p_kwh min_p_kwh max_p_kwh {
	rename `v' `v'_init
	local vlab1: variable label `v'_init
	local vlab2 = subinstr("`vlab1'","rate","SP's initial rate",1)
	la var `v'_init "`vlab2'"
}
foreach v of varlist *_p_kwh_init {
	gen log_`v' = ln(`v')
	local vlab: variable label `v'
	la var log_`v' "Log `vlab'"
}
foreach v of varlist *_TEMP {
	local v2 = subinstr("`v'","_TEMP","",1)
	rename `v' `v2'
}
la var rt_sched_cd_init "Rate schedule for SP's initial month in dataset"
drop temp*

** Merge in residuals for control function (actual and initial rates)
cap drop ctrl_fxn*
merge m:1 rt_sched_cd modate using "$dirpath_data/merged/ag_rates_ctrl_fxn_monthly.dta", ///
	nogen keep(1 3)
foreach v of varlist rt_sched_cd ctrl_fxn* {
	rename `v' TEMP_`v'
}
rename rt_sched_cd_init rt_sched_cd
merge m:1 rt_sched_cd modate using "$dirpath_data/merged/ag_rates_ctrl_fxn_monthly.dta", ///
	nogen keep(1 3)
foreach v of varlist rt_sched_cd ctrl_fxn* {
	rename `v' `v'_init
	local vlab: variable label `v'_init
	la var `v'_init "`vlab' (SP's initial rate)"
}
foreach v of varlist TEMP_* {
	local v2 = subinstr("`v'","TEMP_","",1) 
	rename `v' `v2'
}
	
** Merge in monthly average temperature at SP level
cap drop degreesC_*
merge 1:1 sp_uuid modate using "$dirpath_data/prism/sp_temperature_monthly.dta", ///
	nogen keep(1 3)

** Define rate categories and large/small ag categories
cap drop rt_category rt_large_ag
gen rt_category = 0
replace rt_category = 1 if rt_sched_cd=="AG-1A"
replace rt_category = 2 if rt_sched_cd=="AG-1B"
replace rt_category = 3 if rt_sched_cd=="AG-ICE"
replace rt_category = 4 if inlist(rt_sched_cd,"AG-4A","AG-5A","AG-RA","AG-VA")
replace rt_category = 4 if inlist(rt_sched_cd,"AG-4D","AG-5D","AG-RD","AG-VD")
replace rt_category = 5 if inlist(rt_sched_cd,"AG-4B","AG-5B","AG-RB","AG-VB")
replace rt_category = 5 if inlist(rt_sched_cd,"AG-4C","AG-5C")
replace rt_category = 5 if inlist(rt_sched_cd,"AG-4E","AG-5E","AG-RE","AG-VE")
replace rt_category = 5 if inlist(rt_sched_cd,"AG-4F","AG-5F")
gen rt_large_ag = 0 if inlist(rt_category,1,4) // small ag rates
replace rt_large_ag = 1 if inlist(rt_category,2,5) // large ag rates
replace rt_large_ag = 2 if inlist(rt_category,3) // ICE
assert rt_category!=. & rt_large_ag!=.
la var rt_category "PGE ag rate category (1 of 5 w/in which choosing is possible)"
la var rt_large_ag "PGE ag rate groups based on motor size/type (0=small, 1=large, 2=ICE)"
	
** Save
order sp_uuid modate
sort sp_uuid modate
unique sp_uuid modate
assert r(unique)==r(N)
compress
save "$dirpath_data/merged/sp_month_elec_panel.dta", replace

}

*******************************************************************************
*******************************************************************************

** 7. Merge GIS vars, SP vars, gw depth, and switchers in to collapsed SP-hour panels
if 1==0{

foreach tag in "20180719" "20180322" "20180827" {
	
	** Load collapsed hourly dataset
	use "$dirpath_data/merged/sp_hourly_elec_panel_collapsed_`tag'.dta", clear
	
	** Merge in variables from PGE customer details
	cap drop flag_nem
	cap drop cz_group
	cap drop rt_group
	cap drop flag_geocode_badmiss
	merge m:1 sp_uuid modate using "$dirpath_data/merged/sp_month_elec_panel.dta", ///
		nogen keep(1 3) keepusing(flag_nem climate_zone_cd rt_sched_cd ///
		bad_geocode_flag missing_geocode_flag)
	encode rt_sched_cd, gen(rt_group)
	la var rt_group "Ag tariff group (numeric)"
	encode climate_zone_cd, gen(cz_group)
	la var cz_group "Climate zone (numeric)"
	gen flag_geocode_badmiss = bad_geocode_flag==1 | missing_geocode_flag==1
	la var flag_geocode_badmiss "SP geocode either missing or not in California"
	drop rt_sched_cd climate_zone_cd bad_geocode_flag missing_geocode_flag

	** Merge in GIS variables
	cap drop wdist_group
	cap drop county_group
	cap drop basin_group
	merge m:1 sp_uuid using "$dirpath_data/pge_cleaned/sp_premise_gis.dta", ///
		nogen keep(1 3) keepusing(wdist_group county_fips basin_id)
	encode county_fips, gen(county_group)
	la var county_group "County FIPS (numeric)"
	egen basin_group = group(basin_id)
	la var basin_group "Groundwater basin (numeric)"
	drop county_fips
	
	** Merge in switchers indicators
	cap drop sp_same_rate_*
	merge m:1 sp_uuid using "$dirpath_data/merged/sp_rate_switchers.dta", nogen ///
		keep(1 3) keepusing(sp_same_rate_always sp_same_rate_dumbsmart sp_same_rate_in_cat)
		
	** Merge in average groundwater depths (basin-by-quarter)
	cap drop gw_qtr_bsn_*
	gen quarter = .
	replace quarter = 1 if inlist(month,1,2,3)
	replace quarter = 2 if inlist(month,4,5,6)
	replace quarter = 3 if inlist(month,7,8,9)
	replace quarter = 4 if inlist(month,10,11,12)
	merge m:1 basin_id year quarter using ///
		"$dirpath_data/groundwater/avg_groundwater_depth_basin_quarter_full.dta", ///
		nogen keep(1 3) keepusing(gw_qtr_bsn_mean1 gw_qtr_bsn_mean2)
	drop quarter basin_id
	
	** Create numeric SP identifier
	cap drop sp_group
	egen sp_group = group(sp_uuid)
	la var sp_group "SP identifier (not global; numeric)"

	** Log price instruments
	cap drop log_p_kwh_e1_* log_p_kwh_e20 log_p_kwh_ag_default
	foreach v of varlist p_kwh_e1_?? p_kwh_e20 p_kwh_ag_default {
		gen log_`v' = ln(`v')
		local vlab: variable label `v'
		la var log_`v' "Log `vlab'"
		drop `v'
	}
	
	** Create summer dummy
	cap drop summer
	gen summer = inlist(month,5,6,7,8,9,10)
	la var summer "Summer dummy"
	
	** Create lagged price instruments
	cap drop log_p_lag*
	preserve
	keep sp_uuid hour weekend modate log_p fwt
	egen double temp_denom = sum(fwt), by(sp_uuid hour weekend modate)
	egen double mean_log_p = sum(log_p*fwt/temp_denom), by(sp_uuid hour weekend modate)
	drop log_p fwt
	duplicates drop
	unique sp_uuid hour weekend modate
	assert r(unique)==r(N)
	egen temp_group = group(sp_uuid hour weekend), missing
	tsset temp_group modate
	gen log_p_lag12 = L12.mean_log_p
	gen log_p_lag6 = L6.mean_log_p
	la var log_p_lag12 "Avg SP-specific marg elec price (log $/kWh), lagged 12 months"
	la var log_p_lag6 "Avg SP-specific marg elec price (log $/kWh), lagged 6 months"
	drop temp* mean_log_p
	tempfile lagged_p
	save `lagged_p'
	restore
	merge m:1 sp_uuid hour weekend modate using `lagged_p', nogen keep(1 3)

	** Merge in flag for interval disparities with billed kWh
	cap drop flag_interval_disp20
	merge m:1 sp_uuid modate using "$dirpath_data/merged/sp_month_elec_panel.dta", nogen keep(1 3) ///
		keepusing(flag_interval_disp20)
		
	** Merge in average SA-wise correlation b/tw billing and interval kWh
	cap drop interval_bill_corr
	preserve
	use sp_uuid interval_bill_corr using "$dirpath_data/merged/sp_month_elec_panel.dta", clear
	duplicates drop
	egen temp = mean(interval_bill_corr), by(sp_uuid)
	replace interval_bill_corr = temp
	drop temp
	duplicates drop
	unique sp_uuid
	assert r(unique)==r(N)
	tempfile bill_corr
	save `bill_corr'
	restore
	merge m:1 sp_uuid using `bill_corr', nogen keep(1 3)
		
	** Save
	unique sp_uuid hour log_p modate year month weekend log_p_kwh_e1_?? log_p_kwh_e20 log_p_kwh_ag_default
	assert r(unique)==r(N)
	compress
	save "$dirpath_data/merged/sp_hourly_elec_panel_collapsed_`tag'.dta", replace
		
}

}

*******************************************************************************
*******************************************************************************


