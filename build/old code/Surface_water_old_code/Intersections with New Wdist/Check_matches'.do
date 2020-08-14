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
keep name_PO latitude longitude reported_diversion_total* reported_use_total* reported_diversion_year _merge name_DBF
drop if missing(reported_diversion_year)

drop reported_diversion_year
destring reported*, replace force

rename (reported_diversion_total reported_diversion_total_1 reported_diversion_total_2 reported_diversion_total_3) (final_2010 final_2011 final_2012 final_2013)
keep latitude longitude name_PO final* _merge name_DBF

gen name_2= strupper(name_PO)
preserve

keep name_2 
duplicates drop
sort name_2
save "$path_in/Old_WRIMS_matches_new_PO_names", replace

restore

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



*** This piece of code generates datasets of names of contracts with large allocation percentiles

/*
gsort -_merge -avg_alloc
keep if pt>=90
export delimited "$path_in/Names_contracts_90pt.csv", replace

keep if pt>=95
export delimited "$path_in/Names_contracts_95pt.csv", replace
*/




**** This piece of code gives summary stats of matches with and without phrases


local phrases_1 `" "CITY" "DISTRICT" "COUNTY" " DIST " "'
local phrases_2 `" "ASSOCIATION" " BASIN " "DEPARTMENT" " FARM" " FARMS" " FARMING" "FARMING COMPANY" " INDIAN" " INDIANS" "IRRIGATION DISTRICT" "IRRIGATION COMPANY" "IRRIGATION CO" "MUTUAL WATER" "NATIONAL PARK" "NATIONAL FOREST" "NATL FOREST" " RIVER " " RANCH" " SERVICE" "WATER DISTRICT" "WATER COMPANY" "WATER CO" "WATER AGENCY" "WATER COMMITTEE" "WATER RESOURCE"   "'
local phrases_3 `" " WD " " ID " " I D " " W D " " R D " " F C " " W C D " "W C D" "'

gen has_phrases=0
forvalues i=1(1)3 {
	foreach ph of local phrases_`i' {
		replace has_phrases=`i' if strpos(name_2, "`ph'")>0
	}
}

gsort -has_phrases -avg_alloc

*** This piece of code generates share of avg allocation by whether or not relevant phrases are present
/*
preserve
replace has_phrases=1 if has_phrases>=1
sort has_phrases

by has_phrases: egen share_type= total(avg_alloc)
egen share_total= total(avg_alloc)
gen share_percentage= share_type/share_total


keep has_phrases share*
duplicates drop
log using "$path_in/Allocation_share_type.log", replace
tab has_phrases share_percentage
log close
restore
drop share*
*/

sort pt

log using "$path_in/WRIMS_Matches_logs_matchRate_wPhrases.log", replace
dis "Matches, all"
tab _merge
dis "***"

dis "Matches, with phrases"
tab _merge if has_phrases>0
dis "***"

foreach i of local vals {
	dis "Matches of allocation percentile `i', with phrases"
	tab _merge if pt==`i' & has_phrases>0 
	dis "Matches of allocation percentile `i', all"
	tab _merge if pt==`i'
	dis "***"
}
log close



*** This piece of code generates stats for figuring out if we want to use same name matches to pivot more matches, what are improvements like
** Two separate issues: how many new matches do we get, are there any cases of one contract_name matching to multiple shapefile names

//keep if has_phrases>0
gen is_matched= _merge
drop _merge
sort name_PO is_matched name_DBF
by name_PO: egen secondary_match= max(is_matched)
//now make it a flag variable 
replace secondary_match=0 if secondary_match==1
replace secondary_match=1 if secondary_match==3
gen temp=0
by name_PO is_matched name_DBF: replace temp=1 if _n==1
by name_PO is_matched: egen no_of_shapes= total(temp)
drop temp
//assert no_of_shapes<=1
//create ids for the various Shapefile matched to a given contract
//by name_PO is_matched: egen id_Shp_matched= group(name_DBF)
//assert id_Shp_matched<=1 

sort pt

log using "$path_in/WRIMS_2nd_Matches_logs_matchRate_wPhrases.log", replace

dis "Matches, all"
tab secondary_match
dis "***"

dis "Matches, with phrases"
tab secondary_match if has_phrases>0
dis "***"

foreach i of local vals {
	dis "Matches of allocation percentile `i', with phrases"
	tab secondary_match if pt==`i' & has_phrases>0 
	dis "Matches of allocation percentile `i', all"
	tab secondary_match if pt==`i'
	dis "***"
}
log close

preserve
//keep if pt>=95
gsort -secondary_match -avg_alloc
//export delimited "$path_in/Secondary_matches_contracts_95pt.csv", replace

keep if secondary_match==0
export delimited "$path_in/Old_unmatched_post_2ndary_matches_contracts.csv", replace

keep if secondary_match==0 & has_phrases>0
export delimited "$path_in/Unmatched_after_Secondary_matches_contracts_95pt.csv", replace
restore
