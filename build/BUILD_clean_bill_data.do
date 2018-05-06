clear all
version 13
set more off

*******************************************************************************
**** Script to import and clean raw PGE data -- customer details file *********
*******************************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

** Load raw PGE customer data
use "$dirpath_data/pge_raw/bill_data.dta", clear

** Service agreement ID
assert sa_uuid!=""
assert length(sa_uuid)==10
unique sa_uuid // 162809 unique service agreements
unique sa_uuid bseg_start_dt // 163160 unique customer-service point-service aggrements
duplicates r sa_uuid bseg_start_dt
duplicates t sa_uuid bseg_start_dt, gen(dup)
*br if dup>0 // a BUNCH of dups (almost 1% of account-bills), which will be tricky to parse!
drop dup
la var sa_uuid "Service Agreement ID (anonymized, 10-digit)"

** Start and end dates
assert bseg_start_dt!="" & bseg_end_dt!=""
gen bill_start_dt = date(bseg_start_dt,"DMY")
gen bill_end_dt = date(bseg_end_dt,"DMY")
format %td bill_start_dt bill_end_dt
assert bill_start_dt!=. & bill_end_dt!=. // no missings
assert bill_start_dt<=bill_end_dt // end date never prior to start date
gen bill_length = bill_end_dt-bill_start_dt+1
tab bill_length // 0.5% of observations have bill period > 34 days! (97% b/tw 28-34 days)
drop bseg_start_dt bseg_end_dt
order sa_uuid bill_start_dt bill_end_dt bill_length
la var bill_start_dt "Bill period start date"
la var bill_end_dt "Bill period end date"
la var bill_length "Length of bill period (in days)"

** Rate schedule
count if rt_sched_cd=="" // only 1 observation missing
sort sa_uuid bill_start_dt
egen temp1 = mode(rt_sched_cd), by(sa_uuid)
egen temp2 = max(rt_sched_cd==""), by(sa_uuid)
assert (rt_sched_cd==temp1 | rt_sched_cd=="") if temp2==1 // missing is unambiguous
replace rt_sched_cd = temp1 if rt_sched_cd=="" // populate missing
drop temp1 temp2
la var rt_sched_cd "Rate schedule at end of billing cycle"

** Usage and demand
destring total_electric_usage max_demand peak_demand partial_peak_demand, replace
count if total_electric_usage==. // 813 missings out of 5.27M
count if max_demand==. // 2.97M missings out of 5.27M
count if peak_demand==. // 5.13M missings out of 5.72M
count if partial_peak_demand==. // 5.17M missings out of 5.27M

** Bill amount
destring total_bill_amount, replace
assert total_bill_amount!=. // never missing!
gen perkwh = total_bill_amount/total_electric_usage
sum perkwh, detail // p5 = 11 cents/kwh, p95 = 8.5 cents/kWh
drop perkwh

** Labels
rename total_electric_usage total_bill_kwh
la var total_bill_kwh "Total billed electric usage (kWh)"
la var max_demand "Max demand in bill period (kWh)" 
la var peak_demand "Peak demand in bill period (kWh)"
la var partial_peak_demand "Partial peak demand in bill period (kWh)"
la var total_bill_amount "Total bill amount ($)"

** Duplicate bills (everything matches except bill amount)
duplicates t sa_uuid bill_start_dt bill_end_dt rt_sched_cd total_bill_kwh, gen(dup)
foreach v of varlist total_bill_amount max_demand peak_demand partial_peak_demand {
	egen double temp = mean(`v'), by(sa_uuid bill_start_dt bill_end_dt rt_sched_cd total_bill_kwh)
	replace `v' = temp if dup>0
	drop temp
}
drop dup
duplicates drop
unique sa_uuid bill_start_dt bill_end_dt rt_sched_cd total_bill_kwh
assert r(unique)==r(N)
	
** Duplicate bills (everything matches except bill amount and usage, and there's a zero)	
duplicates t sa_uuid bill_start_dt bill_end rt_sched_cd, gen(dup)
tab dup
sort sa_uuid bill_start_dt bill_end_dt rt_sched_cd
br if dup>0
unique sa_uuid bill_start_dt bill_end_dt rt_sched_cd if dup>0 // 23814 dups
unique sa_uuid bill_start_dt bill_end_dt rt_sched_cd if dup>0 & total_bill_kwh==0
	// 2/3 of dups have a 0 kWh as at least 1 of the dups
egen temp = max(total_bill_kwh==0), by(sa_uuid bill_start_dt bill_end_dt rt_sched_cd)
foreach v of varlist total_bill_kwh total_bill_amount {
	egen double temp2 = sum(`v'), by(sa_uuid bill_start_dt bill_end_dt rt_sched_cd)
	replace `v' = temp2 if dup>0 & temp==1
	drop temp2
}
foreach v of varlist max_demand peak_demand partial_peak_demand {
	egen double temp2 = max(`v'), by(sa_uuid bill_start_dt bill_end_dt rt_sched_cd)
	replace `v' = temp2 if dup>0 & temp==1
	drop temp2
}
drop dup temp
duplicates drop



sort sa_uuid bill_start_dt bill_end_dt 
br if dup>0
br if dup2>0

gen month_st = month(bill_start_dt)
gen year_st = year(bill_start_dt)
gen modate = ym(year,month)
format %tm modate



compress

** Potentially questionable choices
// Dups where everything's identical except $ amount: collapse to avg $ amount
// Dups where everything's identical except $ amount and kWh, and 1 dup is 0 kWh: collapse and sum $ amount 


** Pending
// Deal with duplicates!
// Deal with bill length > 34 days
// Deal with bills with negative kWh
// Monthify bills!
