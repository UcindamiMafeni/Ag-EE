clear all
version 13
set more off

***********************************************************************
**** Script to clean raw PGE data -- pump test project data file ******
***********************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

** Load raw PGE customer data
use "$dirpath_data/pge_raw/pump_test_project_data.dta", clear
duplicates drop
rename *, lower

foreach var of varlist * {
 label variable `var' ""
}

** PGE meter badge number
rename meteronprojectrecord pge_badge_nbr
assert pge_badge_nbr!="" 
order pge_badge_nbr
unique pge_badge_nbr // 720 (out of 977 total)
la var pge_badge_nbr "PG&E meter badge number"
assert pge_badge_nbr==meterontestrecord
drop meterontestrecord

** Dates
rename indicatedprojectfinish date_proj_finish
rename preprojecttestdateonproject date_test_pre
rename postprojecttestdateonprojec date_test_post
rename subsidizedpumptestdate date_test_subs
la var date_proj_finish "Date of project finish"
la var date_test_pre "Date of pre-project pump test (for project)"
la var date_test_post "Date of post-project pump test (for project)"
la var date_test_subs "Date of subsidized pump test"

count if date_test_pre>date_test_post // 3, which seems wrong
count if date_test_pre>date_proj_finish // 9, which seems wrong
gen flag_date_problem = 0
replace flag_date_problem = 1 if date_test_pre>date_test_post
replace flag_date_problem = 1 if date_test_pre>date_proj_finish
count if date_test_post<date_proj_finish // 102, weirdly
la var flag_date_problem "Date inconsistency (pre after post; pre after finish)"

count if date_test_post<date_test_subs // 215 where subsidy happened (likely) too late to influence project
gen flag_subs_after_proj = date_test_post<date_test_subs 
la var flag_subs_after_proj "Subsidized pump test AFTER project, likely not compliers"
count if date_test_pre>date_test_subs // 184 tests where subsidy may have 
gen flag_subs_before_proj = date_test_pre>date_test_subs
la var flag_subs_before_proj "Subsidized pump test BEFORE project, maybe indirect influence on project"

order pge_badge_nbr date* flag*
sort *

** Savings and subsidy
rename gross est_savings_kwh_yr 
la var est_savings_kwh_yr "Engineering estimate(?) of gross kWh savings in first year"
rename incentive subsidy 
la var subsidy "Subsidy offered for project"
assert est_savings_kwh_yr!=.
assert subsidy!=.

** Is well?
tab iswell
assert iswell!=.
la var iswell "Is well?"

** Run
tab run
assert run!=.
la var run "Run number"

** Save
compress
save "$dirpath_data/pge_cleaned/pump_test_projectdata.dta", replace

