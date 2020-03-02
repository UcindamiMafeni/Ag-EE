global path_in= "C:\Users\clohani\OneDrive\Desktop\Check_WRIMS_Matches"
global path_new= "C:\Users\clohani\Dropbox\New_Water_Districts"


use "$path_in/Final_matches.dta", clear
duplicates drop name_PO latitude longitude, force
save "$path_in/Unique_final_matches.dta", replace
import delimited using  "$path_in/haggerty_wr70_cleaned.csv", clear varnames(1)


duplicates drop primary_owner latitude longitude, force
rename primary_owner name_PO
merge m:1 name_PO latitude longitude using "$path_in/Unique_final_matches.dta"
//merge m:1 name_PO 
keep name_PO latitude longitude reported_diversion_total* reported_use_total* reported_diversion_year _merge
drop if missing(reported_diversion_year)

drop reported_diversion_year
destring reported*, replace force

rename (reported_diversion_total reported_diversion_total_1 reported_diversion_total_2 reported_diversion_total_3) (final_2010 final_2011 final_2012 final_2013)
keep latitude longitude name_PO final* _merge

gen name_2= strupper(name_PO)
//preserve

keep name_2 
duplicates drop
sort name_2
local phrases `" "WATER DISTRICT" "IRRIGATION DISTRICT" "WATER COMPANY" "CITY" "DISTRICT" "'
save "$path_in/Old_WRIMS_matches_new_PO_names", replace
stop

//restore

gen avg_alloc=0
forvalues i=2010(1)2013 {
	replace final_`i'= 0 if missing(final_`i')
	replace avg_alloc= final_`i' + avg_alloc
}

replace avg_alloc= avg_alloc/4

sum avg_alloc, det

local vals 5 10 25 50 75 90 95
foreach val of local vals {
	local pt_`val'= r(p`val')
}

gen pt=0
foreach i of local vals{
	replace pt=`i' if avg_alloc>= `pt_`i''
}

sort pt
by pt: tab _merge

gsort -_merge -avg_alloc
keep if pt>=90
export delimited "$path_in/Names_contracts_90pt.csv", replace

keep if pt>=95
export delimited "$path_in/Names_contracts_95pt.csv", replace
