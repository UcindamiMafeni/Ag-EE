do 0_Set_Path.do

global path_in= "$path_master/Cut_3"
global path_out= "$path_master/Cut_3"
global path_temp= "$path_master/Temp"

use "$path_master/Cut_1/Matched.dta", clear
keep name_DBF name_PO
gen string_matched=1
save "$path_temp/Matched_1.dta", replace

use "$path_master/Cut_2/Matched.dta", clear
keep name_DBF name_PO
gen string_matched=1
save "$path_temp/Matched_2.dta", replace

use "$path_in/Geo_matches_contained.dta", replace
gen geo_matched=1
duplicates drop

joinby name_DBF name_PO using "$path_temp/Matched_1.dta", unmatched(both)
rename _merge merge1
joinby name_DBF name_PO using "$path_temp/Matched_2.dta", unmatched(both)
rename _merge merge2

replace string_matched=1 if merge2==2
replace string_matched=0 if missing(string_matched)
replace geo_matched=0 if missing(geo_matched)

gen both_valid= geo_matched*string_matched

//now see if the matches are atleast nearest
keep if string_matched==1 & geo_matched==0

//joinby name_DBF name_PO using "$path_in/Geo_matches_nearest.dta", unmatched(master)
save "$path_temp/String_No_Geo.dta", replace

import delimited using "$path_master/haggerty_wr70_cleaned.csv", clear
keep primary_owner latitude longitude
rename primary_owner name_PO
save "$path_temp/PO_coordinates.dta", replace

use "$path_temp/String_No_Geo.dta", clear
drop latitude longitude
joinby name_PO using "$path_temp/PO_coordinates.dta"
drop merge*
duplicates drop
save "$path_out/Mysterious_string_matches.dta", replace
bysort name_DBF: keep if _n==1
rename (latitude longitude) (lat lon)
keep name_DBF name_PO lat lon
export delimited "$path_out/Mysterious_string_matches.csv", replace
