clear all
version 13
set more off

****************************************************************
**** Script to compare customer data with billing data *********
****************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

*******************************************************************************
*******************************************************************************

** 1. SCE data pull

** Load cleaned PGE bililng data
use "$dirpath_data/sce_cleaned/billing_data_20190916.dta", clear

** Flag first and last bill for each SA (if account begins after Jan 2008 or ends before Aug 2017)
gen month_st = month(bill_start_dt)
gen year_st = year(bill_start_dt)
gen modate_st = ym(year_st,month_st)
gen month_end = month(bill_end_dt)
gen year_end = year(bill_end_dt)
gen modate_end = ym(year_end,month_end)
format %tm modate*

	// flag first bill
tab modate_st if year_st<=2008
egen temp_first_start = min(bill_start_dt), by(sa_uuid)
format %td temp_first_start
tab temp_first_start if year_st<=2008 & temp_first_start==bill_start_dt
gen flag_first_bill = temp_first_start==bill_start_dt & bill_start_dt>date("01jan2008","DMY")
tab flag_first_bill if temp_first_start==bill_start_dt
preserve 
unique sa_uuid
local uniq = r(unique)
keep if temp_first_start==bill_start_dt
unique sa_uuid
assert r(unique)==`uniq'
collapse (count) count=bill_length, by(modate_st flag_first_bill)
twoway ///
	(scatter count modate_st if flag_first_bill==0, color(blue)) ///
	(scatter count modate_st if flag_first_bill==1, color(red))
restore	
unique sa_uuid if flag_first_bill==1
local uniq = r(unique)
unique sa_uuid
di `uniq'/r(unique) // 40% of SAs begin during the sample
	
	// flag last bill
tab modate_end if year_end>=2019
egen temp_last_end = max(bill_end_dt), by(sa_uuid)
format %td temp_last_end
tab temp_last_end if year_end>=2017 & temp_last_end==bill_end_dt
gen flag_last_bill = temp_last_end==bill_end_dt & bill_end_dt<date("30jun2019","DMY")
tab flag_last_bill if temp_last_end==bill_end_dt
preserve 
unique sa_uuid
local uniq = r(unique)
keep if temp_last_end==bill_end_dt
unique sa_uuid
assert r(unique)==`uniq'
collapse (count) count=bill_length, by(modate_end flag_last_bill)
twoway ///
	(scatter count modate_end if flag_last_bill==0 & modate_end>=ym(2019,1), color(blue)) ///
	(scatter count modate_end if flag_last_bill==1 & modate_end>=ym(2019,1), color(red))
restore
unique sa_uuid if flag_last_bill==1
local uniq = r(unique)
unique sa_uuid
di `uniq'/r(unique) // 44% of SAs end during the sample
	
	// plot first and last bill dates
preserve 
keep if temp_first_start==bill_start_dt | temp_last_end==bill_end_dt
egen starter = max(flag_first_bill), by(sa_uuid)
egen ender = max(flag_last_bill), by(sa_uuid)
unique sa_uuid
unique sa_uuid if starter // 64.7% of SAs start during our sample period
unique sa_uuid if ender // 65.2% of SAs end during our sample period
unique sa_uuid if starter & ender // 31.3% of SAs start AND end during our sample period
gen temp_start_month = modate_st if temp_first_start==bill_start_dt
gen temp_end_month = modate_end if temp_last_end==bill_end_dt
egen start_month = mean(temp_start_month), by(sa_uuid)
egen end_month = mean(temp_end_month), by(sa_uuid)
format %tm start_month end_month
keep sa_uuid start_month end_month starter ender
duplicates drop
unique sa_uuid
assert r(unique)==r(N)
gen group = 1 if starter==1 & ender==1
replace group = 2 if starter==1 & ender==0
replace group = 3 if starter==0 & ender==1
replace group = 4 if starter==0 & ender==0
gen labgroup = ""
replace labgroup = "Openers and closers" if group==1
replace labgroup = "Openers only" if group==2
replace labgroup = "Closers only" if group==3
replace labgroup = "Neither" if group==4
tab group
tab labgroup
gen count = _n
collapse (count) count, by(starter ender start_month end_month)
twoway ///
	(scatter end_month start_month if starter==0 & ender==0 [fw=count], msize(vsmall) mfcolor(none) mlcolor(green) mlwidth(vthin)) ///
	(scatter end_month start_month if starter==1 & ender==0 [fw=count], msize(vsmall) mfcolor(none) mlcolor(blue) mlwidth(vthin)) ///
	(scatter end_month start_month if starter==0 & ender==1 [fw=count], msize(vsmall) mfcolor(none) mlcolor(red) mlwidth(vthin)) ///
	(scatter end_month start_month if starter==1 & ender==1 [fw=count], msize(vsmall) mfcolor(none) mlcolor(black) mlwidth(vthin)) ///
	, ///
	xtitle("Start date of first bill", size(vsmall)) xscale(r(564,695)) ///
	xlabel(576 600 624 648 672 696, labsize(vsmall)) ///
	ytitle("End date of last bill", size(vsmall)) ylabel(, nogrid labsize(vsmall)) ///
	graphr(color(white) lc(white)) plotregion(margin(l=0 r=0 t = 0)) ///
	title("PGE Ag Customer Billing Accounts", size(small) color(black)) ///
	legend(order(4 "Openers and closers" 2 "Openers only" 3 "Closers only" 1 "Neither") size(small) c(4) )

restore
	// 33% of accounts are neither openers nor closers!
	// 23% of accounts are openers only
	// 27% of accounts are closers only
	// for 17% of accoutns, we observe the opening and closing!
	
	// label flags
la var flag_first_bill "Flag indicating SA's first bill, when first bill is after start of our sample"	
la var flag_last_bill "Flag indicating SA's last bill, when last bill is before the end of our sample"	


** Keep only essential variables
keep sa_uuid bill_start_dt bill_end_dt bill_length total_bill_kwh ///
	flag_first_bill flag_last_bill
	
** Merge into customer data on SA
joinby sa_uuid using "$dirpath_data/sce_cleaned/sce_cust_detail_20190916.dta", unmatched(both)
tab _merge
unique sa_uuid if _merge==3 //42984 merges
assert _merge!=1 // confirm that all SAs in billing data exist in customer data
unique sa_uuid
local uniq = r(unique)
unique sa_uuid if _merge==2 
di r(unique)/`uniq' // 5% of SAs in customer data do not appear in billing data
gen temp_length = sa_stop - sa_start
egen temp_tag = tag(sa_uuid)
sum temp_length if _merge==3 & temp_tag==1, detail
sum temp_length if _merge==2 & temp_tag==1, detail
count if _merge==2
count if _merge==2 & temp_length==0 // almost half of accounts that don't merge have the same starting and stopping date!
gen temp_year_start = year(sa_start)
gen temp_year_stop = year(sa_stop)
tab temp_year_start _merge if temp_tag==1
tab temp_year_stop _merge if temp_tag==1, missing 
tab temp_year_stop _merge if temp_tag==1 & temp_length!=0, missing 
unique sa_uuid if temp_year_stop>=2008 & temp_length!=0
local uniq = r(unique)
unique sa_uuid if temp_year_stop>=2008 & temp_length!=0 & _merge==2 
di r(unique)/`uniq' // 3% of SAs in customer data that end after 2007 do not appear in billing data!

** Confirm zero bill dups 
unique sa_uuid bill_start_dt
assert r(unique)==r(N)

** Compare billing dates vs. start-stop dates
	// first bill vs. start dates
gen temp_start_diff = bill_start_dt-sa_start if _merge==3 & flag_first_bill==1
br _merge sa_uuid bill_start_dt flag_first_bill sa_start sa_stop temp_start_diff sp_uuid if temp_start_diff!=.
sum temp_start_diff, detail // in >95% of cases, the first bill starts within a few days of SA_start
assert temp_start_diff>0 // first bill never starts before SA_start
	
	// last bill vs. stop dates
gen temp_stop_diff = bill_end_dt-sa_stop if _merge==3 & flag_last_bill==1
br _merge sa_uuid bill_end_dt flag_last_bill sa_start sa_stop temp_stop_diff sp_uuid if temp_stop_diff!=.
sum temp_stop_diff, detail // in >95% of cases, the last bill ends 
sum temp_stop_diff if temp_stop_diff!=0, detail // not a huge issue that 209 of these aren't zero

	// clean up
drop temp_start* temp_stop*	

** NEM flag
egen flag_nem = max(net_mtr_ind), by(sa_uuid bill_start_dt)
egen temp_nem_max = max(flag_nem), by(sa_uuid)
egen temp_nem_min = min(flag_nem), by(sa_uuid)
assert temp_nem_min==temp_nem_max

** Drop duplicates and variables from customer details
drop if _merge==2
drop prsn_uuid* meter_no prem_long prem_lat net_mtr_ind nem_typ climate_zone *naics* cust_name service_zip ///
	cec_subsector cec_sector segment_name ind_subgrp sa_start sa_stop in_calif in_sce in_pou pou_name ///
	bad_geocode_flag climate_zone_gis bad_cz_flag missing_geocode_flag current_tariff nem_start_date ///
	sa_status_code temp* _merge flag_non_ag_tariff
duplicates drop
unique sa_uuid bill_start_dt
assert r(unique)==r(N)

** NEM flag diagnostics
gen temp_neg = total_bill_kwh<0
gen temp_neg_bad = total_bill_kwh<0 & flag_nem==0
count if temp_neg==1
local rN = r(N)
count if temp_neg==1 & flag_nem==1 
di r(N)/`rN' // all 71 negative bills have a NEM flag
drop temp*

** Merge back into billing data
drop bill_length total_bill_kwh flag_first_bill flag_last_bill
merge 1:1 sa_uuid bill_start_dt bill_end_dt using "$dirpath_data/sce_cleaned/billing_data_20190916.dta"
assert _merge==3
drop _merge

** Label
la var flag_nem "Flag for NEM SAs (from customer data; time-invariant, so it's 'ever-NEM')"
la var flag_disct_bill "Flag for discontinuous gap after this bill"

** Save updated version of biling data
order flag_nem, after(interval_bill_corr)
order flag_disct_bill flag_acct , after (flag_short_bill)
order sa_uuid sp_uuid bill_start_dt bill_end_dt
sort sa_uuid bill_start_dt
unique sa_uuid bill_start_dt
assert r(unique)==r(N)
compress
save "$dirpath_data/sce_cleaned/billing_data_20190916.dta", replace


*******************************************************************************
*******************************************************************************







