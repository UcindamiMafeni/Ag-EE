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
format %td date*

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
rename incentive subsidy_proj 
la var subsidy_proj "Subsidy offered for project"
assert est_savings_kwh_yr!=.
assert subsidy_proj!=.

** Is well?
tab iswell
assert iswell!=.
la var iswell "Is well?"

** Run
tab run
assert run!=.
la var run "Run number"

** Merge into full APEP dataset
preserve
use "$dirpath_data/pge_cleaned/pump_test_data.dta", clear
gen idU = _n
keep idU test_date_stata pge_badge_nbr subsidy
tempfile temp
save `temp'
restore
gen idM = _n
joinby pge_badge_nbr using `temp', unmatched(both)

unique pge_badge_nbr if _merge==1 // 11 meters with projects don't merge
unique pge_badge_nbr if _merge==2 // 14496 tested meters don't have projects
unique pge_badge_nbr if _merge==3 // 709 meters with projects merge

duplicates t idU, gen(dup)
assert _merge==3 if dup>0 & idU!=.
br id* pge_badge_nbr *date* dup if dup>0 & idU!=.

gen temp_date_match = test_date_stata==date_test_pre | test_date_stata==date_test_post ///
	| test_date_stata==date_test_subs 
egen temp_match_max = max(temp_date_match), by(idM)	
tab temp_date_match temp_match_max if _merge==3
br id* pge_badge_nbr *date* dup temp_date_match temp_match_max subsidy* ///
	if dup>0 & idU!=. & temp_match_max==0

	// Flag observations that either don't merge, or don't have the right date
gen flag_apep_mismatch = _merge==1 | temp_match_max==0
la var flag_apep_mismatch "Flag for project meters that (i) don't match to APEP, or (ii) match at wrong date"

keep if idM!=.
drop _merge dup temp* idU test_date_stata pge_badge_nbr subsidy
duplicates drop
unique idM
assert r(unique)==r(N)
drop idM
	
	// Pump test project id
gen apep_proj_id = _n
order apep_proj_id	
la var apep_proj_id "APEP project identifier"
	
** Save
compress
save "$dirpath_data/pge_cleaned/pump_test_project_data.dta", replace

