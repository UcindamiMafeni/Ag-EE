clear all
version 13
set more off

**************************************************************
**** Script to import and clean NAICS code descriptions ******
**************************************************************

** Downloaded at https://www.census.gov/eos/www/naics/downloadables/downloadables.html

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

** Import NAICS codes
import excel using "$dirpath_data/misc/6-digit_2017_codes.xlsx", firstrow allstring clear

** Trim all variables
foreach v of varlist * {
	replace `v' = trim(itrim(`v'))
}

** Drop missings
dropmiss, force
dropmiss, obs force

** Keep only NAICS 111
keep if substr(NAICSCode,1,3)=="111"

** Rename and label variables
rename NAICSCode naics
rename NAICSTitle naics_descr
la var naics "6-digit NAICS code"
la var naics_descr "NAICS code description"

** Compress and save
unique naics
assert r(unique)==r(N)
compress
save "$dirpath_data/misc/naics6_descr.dta", replace
