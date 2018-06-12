clear all
version 13
set more off

**************************************************************************
**** Script to build calendar of historic event days (2011-2017) *********
**************************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

** Smart Days for RESIDENTIAL
// Scraped from: 
// https://www.pge.com/en_US/residential/rate-plans/rate-plan-options/smart-rate-add-on/smart-day-history/smart-day-history.page
// (residential event days go back to 2011)
import excel using "$dirpath_data/pge_raw/rates/event_days_residential.xlsx", clear firstrow
dropmiss, obs force
dropmiss, force
rename event_day_residential date_temp
gen event_day_res = 1
replace date_temp = trim(itrim(date_temp))
replace date_temp = subinstr(date_temp,word(date_temp,1),"",1)
gen date = date(date_temp,"MDY")
format %td date
assert date!=.
drop date_temp
tempfile event_days_res
save `event_days_res'

** Smart Days for BUSINESS
// Scraped from: 
// https://www.pge.com/en_US/business/rate-plans/rate-plans/peak-day-pricing/event-day-history.page
// (busienss event days go back to 2013, even though the ag rates suggest that
// even days existed as early as 2010 for ag......)
import excel using "$dirpath_data/pge_raw/rates/event_days_business.xlsx", clear firstrow
dropmiss, obs force
dropmiss, force
rename event_day_business date_temp
gen event_day_biz = 1
replace date_temp = trim(itrim(date_temp))
replace date_temp = subinstr(date_temp,word(date_temp,1),"",1)
gen date = date(date_temp,"MDY")
format %td date
assert date!=.
drop date_temp
tempfile event_days_biz
save `event_days_biz'

** Combine
use `event_days_res', clear
merge 1:1 date using `event_days_biz'
tab _merge if year(date)>=2013
	// 58 event days in common, 3 each not in common
drop _merge
order date 

** Label
la var date "Date"
la var event_day_res "Indicator for residential Event Days"
la var event_day_biz "Indicator for business Event Days"

** Save
sort date
compress
save "$dirpath_data/pge_cleaned/event_days.dta", replace

