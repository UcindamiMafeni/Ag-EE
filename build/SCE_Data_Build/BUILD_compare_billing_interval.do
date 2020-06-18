clear all
version 13
set more off

**********************************************************************
**** Script to compare daily interval data with billing data *********
**********************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

*******************************************************************************
*******************************************************************************

** Load cleaned SCE bililng data
use "$dirpath_data/sce_cleaned/billing_data_20190916.dta", clear

** Keep only essential variables
drop monthly_max_kw total_bill_amount flag_acct flag_long_bill flag_short

** Drop observations prior to 2011 (before avaiable smart-meter data)
drop if bill_start_dt<date("01jan2016","DMY")

**for missing bill days when compared to interval data, we check whether these come from discontinuities
sort sa_uuid bill_start_dt
by sa_uuid: gen flag_disct_bill2= flag_disct_bill[_n+1]
drop flag_disct_bill
rename flag_disct_bill2 flag_disct_bill

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

** Flag duplicate account-dates (bill changeover dates where end=start) this doesn't happen so far in the SCE sample
gen temp_wt = 1
replace temp_wt = 0.5 if date==date[_n+1] & date==bill_end_dt & ///
	bill_end_dt==bill_start_dt[_n+1] & temp_new==1 & temp_new[_n+1]==0 & ///
	sa_uuid==sa_uuid[_n+1]
replace temp_wt = 0.5 if date==date[_n-1] & date==bill_end_dt[_n-1] & ///
	bill_end_dt[_n-1]==bill_start_dt & temp_new[_n-1]==1 & temp_new==0 & ///
	sa_uuid==sa_uuid[_n-1]
	// this assigns 50% weight to days that are shared by two bills (i.e. the
	// end_date of the previous bill and the start_date of the current bill)
	
** Merge in daily interval data
merge m:1 sa_uuid date using "$dirpath_data/sce_cleaned/interval_data_daily_20190916.dta"
gen year = year(date)
tab year _merge
gen month = month(date)
tab month _merge if year==2017
count if _merge==2

//figure out how many SA's are matched and to what extent
sort sa_uuid date 
gen onlyusing= _merge==2
by sa_uuid: egen countmiss= total(onlyusing)
by sa_uuid: gen fracmiss= countmiss/_N if _n==1 //only one entry per sa_uuid

sum fracmiss, det // median is 0.24, of the 30195 unique sauuids, only 1096 have fully accounted for interval data

sum fracmiss if flag_disct_bill!=1, det
stop

local m2 = r(N)
count if _merge==2 & ((year==2011 & inrange(month,1,2)) | (year==2017 & inrange(month,8,9)))
di r(N)/`m2' // 99% of _merge==2 occur in first or last sample month
drop if _merge==2
egen temp_max_merge = max(_merge), by(sa_uuid)
egen temp_max_year = max(year), by(sa_uuid)
preserve
keep sa_uuid temp_max_year temp_max_merge
duplicates drop
tab temp_max_year temp_max_merge 
	// 89% of billed accounts appear somewhere in interval data
	// 53% of accounts that never appear in interval data didn't exist past 2013
	// 6% of accounts that exist through 2017 donpt appear anywhere in interval data!
restore

** Split interval kwh where temp_wt==0.5
replace kwh = kwh/2 if temp_wt==0.5

** Prepare to collapse back to billing data, summing interval kWh
egen double temp_interval_kwh = sum(kwh) if kwh!=., by(sa_uuid bill_start_dt)
egen double interval_kwh = mean(temp_interval_kwh), by(sa_uuid bill_start_dt)
egen interval_merge_max = max(_merge), by(sa_uuid bill_start_dt)
egen interval_merge_min = min(_merge), by(sa_uuid bill_start_dt)
drop temp* date kwh _merge year month

** Collapse back to SA-bill level
duplicates drop
unique sa_uuid bill_start_dt
assert r(unique)==r(N)

** Correlations
correlate total_bill_kwh interval_kwh
correlate total_bill_kwh interval_kwh if interval_merge_min==3
correlate total_bill_kwh interval_kwh if interval_merge_min==3 & year(bill_start_dt)==2011
correlate total_bill_kwh interval_kwh if interval_merge_min==3 & year(bill_start_dt)==2012
correlate total_bill_kwh interval_kwh if interval_merge_min==3 & year(bill_start_dt)==2013
correlate total_bill_kwh interval_kwh if interval_merge_min==3 & year(bill_start_dt)==2014
correlate total_bill_kwh interval_kwh if interval_merge_min==3 & year(bill_start_dt)==2015
correlate total_bill_kwh interval_kwh if interval_merge_min==3 & year(bill_start_dt)==2016
correlate total_bill_kwh interval_kwh if interval_merge_min==3 & year(bill_start_dt)==2017

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
merge 1:1 sa_uuid bill_start_dt bill_end_dt using "$dirpath_data/pge_cleaned/billing_data_20180322.dta"
assert _merge!=1
replace flag_interval_merge = 0 if _merge==2
replace flag_interval_disp20 = 1 if _merge==2
drop _merge

** Label
la var interval_bill_corr "SA-wise correlation b/tw billing & interval kWh, where fully merged"
la var flag_interval_merge "Flag = 1 (0.5) if interval data full (partially) merge, for a given SA-bill"
la var flag_interval_disp20 "Flag = 1 for >20% disparity b/tw billing & interval kWh (missing = 1)"

** Save updated version of biling data
order flag_interval* interval_bill_corr, after(flag_short_bill)
sort sa_uuid bill_start_dt
unique sa_uuid bill_start_dt
assert r(unique)==r(N)
compress
save "$dirpath_data/pge_cleaned/billing_data_20180322.dta", replace
