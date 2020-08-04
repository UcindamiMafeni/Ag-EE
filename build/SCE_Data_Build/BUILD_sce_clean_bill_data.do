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


*******************************************************************************
*******************************************************************************

** 1. Sept 2019 data pull
{
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

unique sa_uuid bill_start_dt // 3481573 unique SA-bills
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
}

*******************************************************************************
*******************************************************************************

** 2. July 2020 data pull
{
** Load raw SCE billing data
use "$dirpath_data/sce_raw/bill_data_20200722.dta", clear

** Service agreement ID
assert serv_acct_num!=""
unique serv_acct_num // 25120 unique service acc no
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
tab bill_length // 0.05% of observations have bill period > 34 days! (97% b/tw 28-34 days)
drop meter_read_dt
order sa_uuid bill_start_dt bill_end_dt bill_length
la var bill_start_dt "Bill period start date"
la var bill_end_dt "Bill period end date"
la var bill_length "Length of bill period (in days)"
drop statl_yr_mo_dt

unique sa_uuid bill_start_dt // 150869 unique SA-bills
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
count if monthly_max_kw==. // 29,878 missings out of 151K

** Bill amount
gen perkwh = bill_amount/kwh_usage
sum perkwh, detail // p5 = 7.6 cents/kwh, p95 = 4.7 $/kWh
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
di 1 - r(N)/_N // 0.04% of bills are longer than 34 days
count if flag_long_bill==0 & flag_short_bill==0
di 1 - r(N)/_N // 0.5% of bills are shorter than 28 days

count if total_bill_kwh!=. & total_bill_kwh!=0 & total_bill_kwh>0	
local N = r(N)
count if flag_long_bill==0 & total_bill_kwh!=. & total_bill_kwh!=0 & total_bill_kwh>0
di 1 - r(N)/`N' // 0.003% of positive bills are longer than 34 days
count if flag_long_bill==0 & flag_short_bill==0 & total_bill_kwh!=. & total_bill_kwh!=0 & total_bill_kwh>0
di 1 - r(N)/`N' // 0.5% of positive bills are shorter than 28 days

tab flag_long_bill 
tab flag_long_bill if total_bill_kwh==. | total_bill_kwh==0
tab flag_short_bill 
tab flag_short_bill if total_bill_kwh==. | total_bill_kwh==0

	// Compress and save
compress
save "$dirpath_data/sce_cleaned/billing_data_20200722.dta", replace
}

*******************************************************************************
*******************************************************************************

** 3. Combine billing data into a single unified dataset
{

	// unlike with the multiple PGE data pulls, it will be easiest to just combine 
	// SCE pulls from the get-go, since they're the same set of customers
	
** Load cleaned SCE bililng data (both data pulls)
use "$dirpath_data/sce_cleaned/billing_data_20190916.dta", clear
gen pull = "20190916"
append using "$dirpath_data/sce_cleaned/billing_data_20200722.dta"
replace pull = "20200722" if pull==""
tab pull

** Remove duplicates (all variables)
duplicates t sa_uuid-flag_short_bill, gen(dup)
tab dup
assert dup<2
tab bill_start_dt if dup==1
unique sa_uuid-flag_short_bill
local uniq = r(unique)
drop if dup==1 & pull=="20200722"
unique sa_uuid-flag_short_bill
assert r(unique)==r(N)
assert r(unique)==`uniq'
drop dup

** Resolve dups from SA-specific flags
duplicates t sa_uuid bill_start_dt, gen(dup)
tab dup
sort sa_uuid bill_start_dt
br if dup>0
egen temp = max(flag_acct), by(sa_uuid)
replace flag_acct = temp
drop temp dup
unique sa_uuid bill_start_dt
local uniq = r(unique)
duplicates drop sa_uuid bill_start_dt, force
unique sa_uuid bill_start_dt
assert r(unique)==`uniq'

** Resolve remaining dups by defaulting to the 2020 pull
duplicates t sa_uuid bill_start_dt, gen(dup)
tab dup
unique sa_uuid bill_start_dt
local uniq = r(unique)
drop if dup>1 & pull=="20190916"
unique sa_uuid bill_start_dt
assert r(unique)==`uniq'
assert r(unique)==r(N)
drop dup

** Assess coverage across pulls
egen sa_in_2019 = max(pull=="20190916"), by(sa_uuid)
egen sa_in_2020 = max(pull=="20200722"), by(sa_uuid)	
egen temp_tag = tag(sa_uuid)
tab sa_in_2019 sa_in_2020 if temp_tag==1, missing
unique sa_uuid

unique sa_uuid if sa_in_2019==1 & sa_in_2020==0
hist bill_end_dt if sa_in_2019==1 & sa_in_2020==0
hist bill_end_dt if sa_in_2019==1 & sa_in_2020==0 & year(bill_end_dt)>=2018, freq
egen temp_max = max(bill_end_dt), by(sa_uuid)
format %td temp_max
hist temp_max if sa_in_2019==1 & sa_in_2020==0 & temp_tag==1 & year(bill_end_dt)>=2017, freq
unique sa_uuid if sa_in_2019==1 & sa_in_2020==0 & bill_end_dt>mdy(5,31,2019)
	// 260 SAs that we see in June/July 2019 don't show up in the 2020 data pull
	// (a bit more mass than we'd naturally expect)
egen flag_nobill_2020 = max(sa_in_2019==1 & sa_in_2020==0 & bill_end_dt>mdy(5,31,2019)), by(sa_uuid)
	
unique sa_uuid if sa_in_2019==0 & sa_in_2020==1	
egen temp_min = min(bill_start_dt), by(sa_uuid)
format %td temp_min
hist temp_min if sa_in_2019==0 & sa_in_2020==1 & temp_tag==1, freq
	// 153 SAs that we see in the 2020 data pull that don't show up in the 2019 data pull
	// (this distribution looks smoother)
egen flag_nobill_2019 = max(sa_in_2019==0 & sa_in_2020==1), by(sa_uuid)

** Clean up and label
drop sa_in_2019 sa_in_2020 temp*
la var pull "Which SCE data pull is this bill from?"
la var flag_nobill_2020 "Flag for SAs that should be in 2020 data pull, but aren't"
la var flag_nobill_2019 "Flag for SAs that newly appear in 2020 data pull"

** Fix bill discontinuity flag
gsort sa_uuid bill_end_dt
by sa_uuid: gen new_start_dt= bill_end_dt[_n-1]+1
gen diff = bill_start_dt-new_start_dt
tab diff flag_disct_bill
tab diff flag_disct_bill if diff>0
tab diff pull if diff>0 & flag_disct_bill==0
assert pull=="20200722" if diff>0 & flag_disct_bill==0 & diff!=.
replace flag_disct_bill = 1 if !missing(new_start_dt) & diff>0 & diff!=. & pull=="20200722" 
egen temp = max(flag_disct_bill), by(sa_uuid)
egen temp_tag = tag(sa_uuid)
tab temp flag_acct if temp_tag
replace flag_acct = temp
drop diff new_start_dt temp*

** Save
sort sa_uuid bill_start_dt
unique sa_uuid bill_start_dt
assert r(unique)==r(N)
compress
save "$dirpath_data/sce_cleaned/billing_data.dta", replace

** Erase pull-specific files, which are now redundant
erase "$dirpath_data/sce_cleaned/billing_data_20190916.dta"	
erase "$dirpath_data/sce_cleaned/billing_data_20200722.dta"	
	
}

*******************************************************************************
*******************************************************************************
