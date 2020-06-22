clear all
version 13
set more off

***************************************************************************
**** Script to import and clean raw SCE data -- billing data file *********
***************************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"


** Potentially questionable choices I've meade:
// When (2) bills had same start dates but end dates differed by a day, everything else being same,///
//							 I dropped one observation which aligned less with timeline
// I flag the 


*******************************************************************************
*******************************************************************************

** 1. Sept 2019 data pull


** Load raw SCE billing data
use "$dirpath_data/sce_raw/bill_data_20190916.dta", clear

** Service agreement ID
assert serv_acct_num!=""
unique serv_acct_num // 42984 unique service acc no
la var serv_acct_num "Service Account Number"
rename serv_acct_num sa_uuid

** Start and end dates
assert statl_yr_mo_dt!="" & meter_read_dt!=""
gen bill_end_dt = date(meter_read_dt,"YMD")
destring num_days, replace
rename num_days bill_length
gen bill_start_dt = bill_end_dt- bill_length+1
format %td bill_start_dt bill_end_dt
assert bill_start_dt!=. & bill_end_dt!=. // no missings
assert bill_start_dt<=bill_end_dt // end date never prior to start date
tab bill_length // 0.5% of observations have bill period > 34 days! (97% b/tw 28-34 days)
drop meter_read_dt
order sa_uuid bill_start_dt bill_end_dt bill_length
la var bill_start_dt "Bill period start date"
la var bill_end_dt "Bill period end date"
la var bill_length "Length of bill period (in days)"
drop statl_yr_mo_dt

unique sa_uuid bill_start_dt // 3481573 unique customer-service point-service aggrements
duplicates r sa_uuid bill_start_dt

gsort sa_uuid bill_end_dt
by sa_uuid: gen new_start_dt= bill_end_dt[_n-1]+1

gen diff= bill_start_dt-new_start_dt
//assert bill_start_dt==new_start_dt | missing(new_start_dt) ** 4,378 contradictions, 0.13 percent
tab diff
gen flag_disct_bill=0
replace flag_disct_bill=1 if !missing(new_start_dt) & diff>0
gen flag_overlap_bill= diff<-1
drop diff new_start_dt
la var flag_disct_bill "Flag for a gap in billing cyle"
la var flag_overlap_bill "Flag for overlap with previous bill"

** Rate schedule
count if tariff_sched_text=="" // no missing
sort sa_uuid bill_start_dt
la var tariff_sched_text "Rate schedule at end of billing cycle"

** Usage and demand
destring kwh_usage bill_amount monthly_max_kw, replace
count if kwh_usage==. // no missings
count if bill_amount==. // no missings
count if monthly_max_kw==. // 956,712 missings out of 3.5M

** Bill amount
gen perkwh = bill_amount/kwh_usage
sum perkwh, detail // p5 = 8.6 cents/kwh, p95 = 4.7 $/kWh
drop perkwh

** Labels
rename (kwh_usage bill_amount) (total_bill_kwh total_bill_amount)
la var total_bill_kwh "Total billed electric usage (kWh)"
la var monthly_max_kw "Max demand in bill period (kW)" 
la var total_bill_amount "Total bill amount ($)"

** Duplicate bills: everything matches except bill amount and usage: NONE	
duplicates t sa_uuid bill_start_dt bill_end_dt tariff_sched_text, gen(dup)
tab dup
drop dup

** Duplicate bills: same account, same start/end dates: NONE
duplicates t sa_uuid bill_start_dt bill_end_dt, gen(dup)
tab dup
drop dup


** Since the overlapping bills are basically the same ones, we drop one instance
drop if flag_overlap_bill
drop flag_overlap_bill

sort sa_uuid bill_start_dt
by sa_uuid: egen flag_acct= max(flag_disct_bill)
la var flag_acct "Flag for accounts that ever have a gap in the billing cycle"

sort flag_acct sa_uuid bill_start_dt

** Confirm bill uniqueness for SA, start date
unique sa_uuid bill_start_dt 
assert r(unique)==r(N)


** Deal with bill length
tab bill_length 
tab bill_length if total_bill_kwh!=.
tab bill_length if total_bill_kwh!=. & total_bill_kwh!=0
tab bill_length if total_bill_kwh!=. & total_bill_kwh!=0 & total_bill_kwh>0	
assert bill_length!=.

gen flag_long_bill = bill_length>34
gen flag_short_bill = bill_length<28
la var flag_long_bill "Flag for bills longer than 34 days"
la var flag_short_bill "Flag for bills shorter than 28 days"
tab flag_long_bill flag_short_bill

count if flag_long_bill==0 
di 1 - r(N)/_N // 0.002% of bills are longer than 34 days
count if flag_long_bill==0 & flag_short_bill==0
di 1 - r(N)/_N // 0.02% of bills are shorter than 28 days

count if total_bill_kwh!=. & total_bill_kwh!=0 & total_bill_kwh>0	
local N = r(N)
count if flag_long_bill==0 & total_bill_kwh!=. & total_bill_kwh!=0 & total_bill_kwh>0
di 1 - r(N)/`N' // 0.002% of positive bills are longer than 34 days
count if flag_long_bill==0 & flag_short_bill==0 & total_bill_kwh!=. & total_bill_kwh!=0 & total_bill_kwh>0
di 1 - r(N)/`N' // 0.02% of positive bills are shorter than 28 days

tab flag_long_bill 
tab flag_long_bill if total_bill_kwh==. | total_bill_kwh==0
tab flag_short_bill 
tab flag_short_bill if total_bill_kwh==. | total_bill_kwh==0

	// Compress and save
compress
save "$dirpath_data/sce_cleaned/billing_data_20190916.dta", replace
