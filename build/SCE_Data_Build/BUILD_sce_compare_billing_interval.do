clear all
version 13
set more off

**************************************************************************
**** Script to compare SCE daily interval data with billing data *********
**************************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

*******************************************************************************
*******************************************************************************

** 1. Compare billing vs. interval data for both 2019 and 2020 SCE EDRP
{
** Load cleaned SCE bililng data (both data pulls)
use "$dirpath_data/sce_cleaned/billing_data.dta", clear

** Keep only essential variables
drop monthly_max_kw total_bill_amount flag_long_bill flag_short_bill

** Drop observations prior to 2016 (before avaiable smart-meter data)
drop if bill_end_dt<date("01jan2016","DMY")

**for missing bill days when compared to interval data, we check whether these come from discontinuities
sort sa_uuid bill_start_dt
by sa_uuid: gen flag_disct_bill2 = flag_disct_bill[_n+1]
drop flag_disct_bill
rename flag_disct_bill2 flag_disct_bill

** Expand whole dataset by bill length variable
assert bill_length!=.
expand bill_length, gen(temp_new)
sort sa_uuid bill_start_dt temp_new
tab temp_new

** Construct date variable (duplicated at each bill change-over)
gen date = bill_start_dt if temp_new==0
format %td date
replace date = date[_n-1]+1 if temp_new==1
assert date==bill_start_dt if temp_new==0
assert date==bill_end_dt if temp_new[_n+1]==0

br if temp_new[_n+1]==0 & date!=bill_end_dt
assert date!=.
unique sa_uuid bill_start_dt date
assert r(unique)==r(N)

** Flag duplicate account-dates (bill changeover dates where end=start)
sort sa_uuid date bill_start_dt
gen temp_wt = 1
replace temp_wt = 0.5 if date==date[_n+1] &	sa_uuid==sa_uuid[_n+1]
replace temp_wt = 0.5 if date==date[_n-1] & sa_uuid==sa_uuid[_n-1]
	// this assigns 50% weight to days that are shared by two bills (i.e. the
	// end_date of the previous bill and the start_date of the current bill)
	// APPARENTLY not an issue for SCE billing data, except for 4 SAs at the 
	// cusp of the 2019-2020 EDRP merge
tab temp_wt	
tab sa_uuid if temp_wt==0.5
assert flag_overlapping_bill==1 if temp_wt==0.5
drop flag_overlapping_bill
	
** Merge in daily interval data
merge m:1 sa_uuid date using "$dirpath_data/sce_cleaned/interval_data_daily_20190916.dta", gen(_merge2019)
rename kwh kwh2019
merge m:1 sa_uuid date using "$dirpath_data/sce_cleaned/interval_data_daily_20200722.dta", gen(_merge2020)
rename kwh kwh2020

count if kwh2019!=. & kwh2020!=.
local rN = r(N)
count if kwh2019!=. & kwh2020!=. & kwh2019!=kwh2020
di r(N)/`rN' // 1% of merged observations with overlap aren't exactly identical
correlate kwh2019 kwh2020 if kwh2019!=. & kwh2020!=. & kwh2019!=kwh2020 
	// in this 1%, the correlation is 0.95, so fine for the purposes of this exercise
rename kwh2020 kwh
replace kwh = kwh2019 if kwh==.	
drop kwh2019
gen year = year(date)
tab year _merge2019
tab year _merge2020
drop if year==2015 // interval data start in 2016
gen month = month(date)
tab month 
gen modate = ym(year,month)
format %tm modate
count if _merge2019==2
count if _merge2019==2 & kwh!=0
count if _merge2020==2
count if _merge2020==2 & kwh!=0
gen _merge = .
replace _merge = 3 if (_merge2019==3 | _merge2020==3) & bill_length!=.
replace _merge = 2 if _merge==. & (_merge2019==2 | _merge2020==2)
replace _merge = 1 if _merge==. & _merge2019==1 & _merge2020==1
assert _merge!=.
tab _merge2019 _merge2020, missing
tab _merge2019 _merge2020 if _merge==1, missing
tab _merge2019 _merge2020 if _merge==2, missing
tab _merge2019 _merge2020 if _merge==3, missing
drop _merge2019 _merge2020

** Diagnose non-matches (interval data only)
egen temp_max_merge = max(_merge), by(sa_uuid)
egen temp_tag = tag(sa_uuid)
tab temp_max_merge if temp_tag, missing // 13 SAs only exist in AMI data
tab sa_uuid temp_max_merge if temp_max_merge<3 // most SAs that never merge exist for very few AMI days
egen temp_count = count(date), by(sa_uuid)
tab sa_uuid temp_max_merge if temp_max_merge<3 & temp_count>365 // 1 unmatched SA have >1 year of AMI data
tab _merge
tab _merge if temp_count>365
assert _merge==2 if temp_max_merge==2 
tab _merge temp_max_merge 
tab month year if temp_max_merge==3 & _merge==2 // bill gaps in 2017 for a lot of them 
tab _merge temp_max_merge if !(year==2019 & month==12) & kwh>0 & temp_count>365

** Are there werid gaps in billing data in 2017?
egen temp = max(flag_acct), by(sa_uuid)
replace flag_acct = temp
drop temp
egen temp_max_merge_ym = max(_merge), by(sa_uuid modate)
tab temp_max_merge_ym
tab modate temp_max_merge_ym
egen temp1 = max(temp_max_merge_ym) if inlist(modate,ym(2016,7)), by(sa_uuid)
egen temp2 = max(temp_max_merge_ym) if inlist(modate,ym(2017,7)), by(sa_uuid)
egen temp3 = max(temp_max_merge_ym) if inlist(modate,ym(2018,7)), by(sa_uuid)
egen temp4 = mean(temp1), by(sa_uuid)
egen temp5 = mean(temp2), by(sa_uuid)
egen temp6 = mean(temp3), by(sa_uuid)
tab temp5 temp_max_merge if temp_tag, missing
tab temp5 if temp_max_merge==3 & temp_tag, missing
tab temp5 if temp4==3 & temp6==3 & temp_tag, missing
	// 2017 billing data appear to be missing for 4% of SAs (1009 SAs)
tab temp5 flag_acct if temp4==3 & temp6==3 & temp_tag, missing
	// this explains 38% of accounts with billing gaps
tab _merge flag_acct 
tab _merge flag_acct if !(year==2019 & month==12) // 90% of _merge==2's are for flagged accounts
tab _merge flag_acct if !(year==2019 & month==12) & kwh>0 // 91% of _merge==2's are for flagged acocunts
tab _merge flag_acct if !(year==2019 & month==12) & kwh>0 & temp_count>365 // 92% of _merge==2's are for flagged acocunts
	// These missing bills appear to be a problem
drop temp?
	
	
** What about the other type of non-matches (billing data only)?
tab tariff_sched_text _merge // only 3 rates that are systematically _merge==1
tab modate _merge // 43% of missing AMI data is in Feb 2016
tab modate _merge if temp_max_merge==3 // 53% of missing AMI data for ever-merge SAs is in Feb 2016
tab date _merge if modate==ym(2016,2) & temp_max_merge==3 // LEAP DAY!

** Drop _merge==2's, since we're eventually merging back in to billing data
drop if _merge==2
	
** Split interval kwh where temp_wt==0.5
replace kwh = kwh/2 if temp_wt==0.5

** Prepare to collapse back to billing data, summing interval kWh
egen double temp_interval_kwh = sum(kwh) if kwh!=., by(sa_uuid bill_start_dt)
egen double interval_kwh = mean(temp_interval_kwh), by(sa_uuid bill_start_dt)
egen interval_merge_max = max(_merge), by(sa_uuid bill_start_dt)
egen interval_merge_min = min(_merge), by(sa_uuid bill_start_dt)
drop temp* date kwh _merge year month modate

** Collapse back to SA-bill level
duplicates drop
unique sa_uuid bill_start_dt
assert r(unique)==r(N)

** Correlations
correlate total_bill_kwh interval_kwh
correlate total_bill_kwh interval_kwh if interval_merge_min==3
correlate total_bill_kwh interval_kwh if interval_merge_min==3 & year(bill_start_dt)==2016 //0.999
correlate total_bill_kwh interval_kwh if interval_merge_min==3 & year(bill_start_dt)==2017 //0.998
correlate total_bill_kwh interval_kwh if interval_merge_min==3 & year(bill_start_dt)==2018 //0.923
correlate total_bill_kwh interval_kwh if interval_merge_min==3 & year(bill_start_dt)==2019 //0.998

** Percent and level differences 
gen pct_diff_kwh = (interval_kwh-total_bill_kwh)/total_bill_kwh
sum pct_diff_kwh if interval_merge_min==3, detail
br if pct_diff_kwh>.8 & pct_diff_kwh!=. & interval_merge_min==3
gen lev_diff_kwh = interval_kwh-total_bill_kwh
sum lev_diff_kwh if interval_merge_min==3, detail
sum lev_diff_kwh if interval_merge_min==3 & pct_diff_kwh!=., detail
sum lev_diff_kwh if interval_merge_min==3 & pct_diff_kwh>0.8 & pct_diff_kwh!=., detail

** SA-specific correlations
sort sa_uuid bill_start_dt
egen bill_int_corr = corr(total_bill_kwh interval_kwh) if interval_merge_min==3, by(sa_uuid)
egen temp_sa_tag = tag(sa_uuid) if interval_merge_min==3
sum bill_int_corr if temp_sa_tag==1, detail

** Interval-specific flags
gen flag_interval_merge = interval_merge_min==3
replace flag_interval_merge = 0.5 if interval_merge_max==3 & interval_merge_min==1
gen flag_interval_disp20 = abs(pct_diff_kwh)>0.2 & interval_kwh!=.
replace flag_interval_disp20 = 0 if interval_kwh==0 & interval_merge_max==3 & ///
	pct_diff_kwh==. & lev_diff_kwh==0
rename bill_int_corr interval_bill_corr
	
** Merge back into billing data
keep sa_uuid bill_start_dt bill_end_dt flag* interval_bill_corr
unique sa_uuid bill_start_dt
assert r(unique)==r(N)
merge 1:1 sa_uuid bill_start_dt bill_end_dt using "$dirpath_data/sce_cleaned/billing_data.dta"
assert _merge!=1
replace flag_interval_merge = 0 if _merge==2
replace flag_interval_disp20 = 1 if _merge==2
drop _merge

** Label
la var interval_bill_corr "SA-wise correlation b/tw billing & interval kWh, where fully merged"
la var flag_interval_merge "Flag = 1 (0.5) if interval data full (partially) merge, for a given SA-bill"
la var flag_interval_disp20 "Flag = 1 for >20% disparity b/tw billing & interval kWh (missing = 1)"
la var flag_disct_bill "Flag = 1 if bill is followed by a gap"

** Save updated version of biling data
order flag_interval* interval_bill_corr, after(flag_short_bill)
sort sa_uuid bill_start_dt
unique sa_uuid bill_start_dt
assert r(unique)==r(N)
compress
save "$dirpath_data/sce_cleaned/billing_data.dta", replace
}

