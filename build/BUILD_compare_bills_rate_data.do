clear all
version 13
set more off

***********************************************************************************
**** Script to compare customer bills to constructed bills using PGE rate data ****
***********************************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

** Load cleaned PGE bililng data
use "$dirpath_data/pge_cleaned/billing_data.dta", clear

** Drop observations prior to 2011 (before avaiable smart-meter data)
drop if bill_start_dt<date("01jan2011","DMY")

** Drop observations without good interval data (for purposes of corroborating dollar amounts)
keep if flag_interval_merge==1
drop if flag_interval_disp20==1

** Drop NEM customers (for purpsoes of corroborating dollar amounts)
drop if flag_nem==1

** Drop first bills, last bills, long bills, short bills
drop if flag_first_bill==1
drop if flag_last_bill==1
drop if flag_long_bill==1
drop if flag_short_bill==1

** Drop bills with multiple/bad tariffs, or with overlapping windows, or bad kwh
drop if flag_multi_tariff==1
drop if flag_bad_tariff==1
drop if flag_dup_partial_overlap>0
drop if flag_dup_double_overlap==1
drop if flag_dup_bad_kwh==1
drop if flag_dup_overlap_missing==1

** Drop flags
drop flag* sp_uuid?

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

