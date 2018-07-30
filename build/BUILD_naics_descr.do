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
import excel using "$dirpath_data/misc/2017_NAICS_Descriptions.xlsx", firstrow allstring clear

** Trim all variables
foreach v of varlist * {
	replace `v' = trim(itrim(`v'))
}

** Drop missings
dropmiss, force
dropmiss, obs force

** Drop descriptions (too much information)
drop Description

//** Keep only NAICS 111
//keep if substr(Code,1,3)=="111"

** Clean up NAICS titles and compress
replace Title = substr(Title,1,length(Title)-1) if substr(Title,-1,1)=="T"
compress

** Add trailing zeros and drop dups
replace Code = Code + "0" if length(Code)<6
replace Code = Code + "0" if length(Code)<6
replace Code = Code + "0" if length(Code)<6
duplicates drop
duplicates drop Code, force
unique Code
assert r(unique)==r(N)

** Rename and label variables
rename Code naics
rename Title naics_descr
la var naics "NAICS code (3-6 digits)"
la var naics_descr "NAICS code description"

** Compress and save
unique naics
assert r(unique)==r(N)
sort naics
compress
save "$dirpath_data/misc/naics_descr.dta", replace
