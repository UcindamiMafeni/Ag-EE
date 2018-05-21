clear all
version 13
set more off

****************************************************************
**** Script to compare customer data with billing data *********
****************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

** Load cleaned PGE bililng data
use "$dirpath_data/pge_cleaned/billing_data.dta", clear

** Keep only essential variables
keep sa_uuid bill_start_dt bill_end_dt bill_length total_bill_kwh ///
	flag_first_bill flag_last_bill
	
** Merge into customer data on SA
joinby sa_uuid using "$dirpath_data/pge_cleaned/pge_cust_detail.dta", unmatched(both)
assert _merge!=1 // confirm that all SAs in billing data exist in customer data
unique sa_uuid
local uniq = r(unique)
unique sa_uuid if _merge==2 
di r(unique)/`uniq' // 22% of SAs in customer data do not appear in billing data!
gen temp_length = sa_sp_stop - sa_sp_start
egen temp_tag = tag(sa_uuid)
sum temp_length if _merge==3 & temp_tag==1, detail
sum temp_length if _merge==2 & temp_tag==1, detail
gen temp_year_start = year(sa_sp_start)
gen temp_year_stop = year(sa_sp_stop)
tab temp_year_start _merge if temp_tag==1
tab temp_year_stop _merge if temp_tag==1, missing
unique sa_uuid if temp_year_stop>=2008 
local uniq = r(unique)
unique sa_uuid if temp_year_stop>=2008 & _merge==2 
di r(unique)/`uniq' // 8% of SAs in customer data that end after 2007 do not appear in billing data!

** Deal with dups
duplicates t sa_uuid bill_start_dt, gen(dup)
unique sa_uuid if _merge==3
local uniq = r(unique)
unique sa_uuid if _merge==3 & dup>0
di r(unique)/`uniq' // 0.2% of merged SAs are dups in customer data
br _merge sa_uuid bill_start_dt bill_end_dt flag_first_bill sa_sp_start ///
	flag_last_bill sa_sp_stop sp_uuid if _merge==3 & dup>0

	// keep the non-dup with the right date range
gen temp_between = inrange(bill_start_dt,sa_sp_start,sa_sp_stop) & ///
	inrange(bill_end_dt,sa_sp_start,sa_sp_stop)
egen temp_between_max = max(temp_between), by(sa_uuid bill_start_dt)
unique sa_uuid bill_start_dt
local uniq = r(unique)
drop if dup>0 & temp_between_max==1 & temp_between==0
unique sa_uuid bill_start_dt
assert r(unique)== `uniq'
 
	// tag remaining dups
duplicates t sa_uuid bill_start_dt, gen(dup2)
unique sa_uuid if _merge==3
local uniq = r(unique)
unique sa_uuid if _merge==3 & dup2>0
di r(unique)/`uniq' // 0.1% of merged SAs are dups in customer data
br _merge sa_uuid bill_start_dt bill_end_dt flag_first_bill sa_sp_start ///
	flag_last_bill sa_sp_stop sp_uuid if _merge==3 & dup2>0

	// keep the non-dup with bill_start_dt in the right date range
gen temp_between_st = inrange(bill_start_dt,sa_sp_start,sa_sp_stop) 
egen temp_between_st_max = max(temp_between_st), by(sa_uuid bill_start_dt)
unique sa_uuid bill_start_dt
local uniq = r(unique)
drop if dup2>0 & temp_between_st_max==1 & temp_between_st==0
unique sa_uuid bill_start_dt
assert r(unique)== `uniq'
	// these are cases where one SP ends and another begins, but the dates don't 
	// exactly match the bill start/end dates
	
	// tag remaining dups
duplicates t sa_uuid bill_start_dt, gen(dup3)
unique sa_uuid if _merge==3
local uniq = r(unique)
unique sa_uuid if _merge==3 & dup3>0
di r(unique)/`uniq' // 0.03% of merged SAs are dups in customer data
br _merge sa_uuid bill_start_dt bill_end_dt flag_first_bill sa_sp_start ///
	flag_last_bill sa_sp_stop sp_uuid dup3 if _merge==3 & dup3>1
	// all remaining dups appear to be SAs with multiple meters, so we want them both
	
	// create sp_uuid variable(s!) to add into billing data, while keeping it unique
sort sa_uuid bill_start_dt sp_uuid
gen sp_uuid1 = ""
replace sp_uuid1 = sp_uuid if dup3==0
replace sp_uuid1 = sp_uuid if dup3>0 & sa_uuid==sa_uuid[_n+1] & bill_start_dt==bill_start_dt[_n+1] & ///
	!(sa_uuid==sa_uuid[_n-1] & bill_start_dt==bill_start_dt[_n-1])
gen sp_uuid2 = ""
replace sp_uuid2 = sp_uuid[_n+1] if dup3>0 & sa_uuid==sa_uuid[_n+1] & bill_start_dt==bill_start_dt[_n+1] & ///
	!(sa_uuid==sa_uuid[_n-1] & bill_start_dt==bill_start_dt[_n-1])
gen sp_uuid3 = ""
replace sp_uuid3 = sp_uuid[_n+2] if dup3>1 & sa_uuid==sa_uuid[_n+2] & bill_start_dt==bill_start_dt[_n+2] & ///
	 sa_uuid==sa_uuid[_n+1] & bill_start_dt==bill_start_dt[_n+1] & !(sa_uuid==sa_uuid[_n-1] & ///
	 bill_start_dt==bill_start_dt[_n-1])
assert dup3<=2

	// clean up
drop dup* temp_between*
	
** Compare billing dates vs. start-stop dates
	// first bill vs. start dates
gen temp_start_diff = bill_start_dt-sa_sp_start if _merge==3 & flag_first_bill==1
gen temp_start_disp = _merge==3 & flag_first_bill==1 & temp_start_diff<-1 // allowing a 1-day buffer
egen temp_start_disp_max = max(temp_start_disp), by(sa_uuid)
br _merge sa_uuid bill_start_dt flag_first_bill sa_sp_start sa_sp_stop temp_start_diff ///
	prsn_uuid sp_uuid pge_badge_nbr if temp_start_disp_max==1
unique sa_uuid 
local uniq = r(unique)
unique sa_uuid if temp_start_disp
di r(unique)/`uniq'	// 137 SAs (0.08%) had bills BEFORE sa_sp_start date (doesn't seem like a huge issue) 
	
	// last bill vs. stop dates
gen temp_stop_diff = bill_end_dt-sa_sp_stop if _merge==3 & flag_last_bill==1
gen temp_stop_disp = _merge==3 & flag_last_bill==1 & temp_stop_diff>34 // allowint 34-day buffer
egen temp_stop_disp_max = max(temp_stop_disp), by(sa_uuid)
br _merge sa_uuid bill_end_dt flag_last_bill sa_sp_start sa_sp_stop temp_stop_diff ///
	prsn_uuid sp_uuid pge_badge_nbr if temp_stop_disp_max==1
unique sa_uuid 
local uniq = r(unique)
unique sa_uuid if temp_stop_disp
di r(unique)/`uniq'	// 3064 SAs (1.8%) had bills end >1 month AFTER sa_sp_stop date (doesn't seem like a huge issue) 

	// clean up
drop temp_start* temp_stop*	

** See if SA/SP lapses are real?
gen temp_mid_lapse = (inrange(bill_start_dt,sa_sp_lapse_start1,sa_sp_lapse_stop1) & ///
	inrange(bill_end_dt,sa_sp_lapse_start1,sa_sp_lapse_stop1) & sa_sp_lapse_start1!=. & ///
	sa_sp_lapse_stop1!=.) | (inrange(bill_start_dt,sa_sp_lapse_start2,sa_sp_lapse_stop2) & ///
	inrange(bill_end_dt,sa_sp_lapse_start2,sa_sp_lapse_stop2) & sa_sp_lapse_start2!=. & ///
	sa_sp_lapse_stop2!=.)
unique sa_uuid if temp_mid_lapse==1 // only 13 out of 162k SAs, so not an issue
br sa_uuid bill_start_dt bill_end_dt sa_sp_start sa_sp_stop sa_sp_lapse* if temp_mid_lapse==1
	
** NEM flag
egen flag_nem = max(net_mtr_ind), by(sa_uuid bill_start_dt)
egen temp_nem_max = max(flag_nem), by(sa_uuid)
egen temp_nem_min = min(flag_nem), by(sa_uuid)
assert temp_nem_min==temp_nem_max

** Drop duplicates and variables from customer details
drop if _merge==2
drop prsn_uuid sp_uuid pge_badge_nbr prem_lat prem_lon net_mtr_ind dr_ind dr_program climate_zone_cd ///
	prsn_naics naics_descr sa_sp_start sa_sp_stop sa_sp_lapse* in_calif in_pge in_pou pou_name ///
	bad_geocode_flag climate_zone_cd_gis bad_cz_flag missing_geocode_flag temp* _merge	
duplicates t sa_uuid bill_start_dt, gen(dup)
unique sa_uuid bill_start_dt
local uniq = r(unique)
drop if dup>0 & sp_uuid1==""
drop dup
unique sa_uuid bill_start_dt
assert r(unique)==`uniq'
assert r(unique)==r(N)

** NEM flag diagnostics
gen temp_neg = total_bill_kwh<0
gen temp_neg_bad = total_bill_kwh<0 & flag_nem==0
count if temp_neg==1
local rN = r(N)
count if temp_neg==1 & flag_nem==1 
di r(N)/`rN' // 99.96% of negative bills have a NEM flag, thank god
egen temp_neg_bad_max = max(temp_neg_bad), by(sa_uuid)
br if temp_neg_bad_max==1 // only 2 SAs: 1 looks like a glitch in kWh, the other looks like definitely NEM
replace flag_nem = 1 if sa_uuid=="3498020821" // manually correct the 1 SA 
drop temp*

** Merge back into billing data
drop bill_length total_bill_kwh flag_first_bill flag_last_bill
merge 1:1 sa_uuid bill_start_dt bill_end_dt using "$dirpath_data/pge_cleaned/billing_data.dta"
assert _merge==3
drop _merge

** Label
la var sp_uuid1 "SP ID for merging SA into customer data"
la var sp_uuid2 "SP ID for merging SA into customer data (cases with 2-3 SPs per SA)"	
la var sp_uuid3 "SP ID for merging SA into customer data (cases with 3 SPs per SA)"	
la var flag_nem "Flag for NEM SAs (from customer data; time-invariant, so it's 'ever-NEM')"

** Save updated version of biling data
order flag_nem sp_uuid?, after(interval_bill_corr)
sort sa_uuid bill_start_dt
unique sa_uuid bill_start_dt
assert r(unique)==r(N)
compress
save "$dirpath_data/pge_cleaned/billing_data.dta", replace











