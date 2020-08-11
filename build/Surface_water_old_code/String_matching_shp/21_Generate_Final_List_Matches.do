do 0_Set_Path.do

global path_in= "$path_master/Cut_3"
global path_out= "$path_master/Cut_3"
global path_temp= "$path_master/Temp"
global path_names= "$path_master/Names"

use "$path_master/Geo_and_string_matches.dta", clear

//carry only those names forward from this list that have been geo-matched
keep if geo_matched

keep name_DBF name_PO latitude longitude
save "$path_temp/Final_matches.dta", replace

keep name_DBF name_PO
duplicates drop

save "$path_temp/Final_matches_ND.dta", replace

use "$path_in/Match_with_distances.dta", clear
local lim= 50 

//limit the distance upto which string matches are considered valid
keep if distance<= `lim'

//save the version of all matches, where either district contains a contract or where string match is close enough, with duplicates
keep name_DBF name_PO latitude longitude
save "$path_temp/Approved_strings.dta", replace
append using "$path_temp/Final_matches.dta"
save "$path_master/Final_matches.dta", replace

//save the version with no duplicate name pairs
use "$path_temp/Approved_strings.dta", replace
keep name_DBF name_PO
duplicates drop
append using "$path_temp/Final_matches_ND.dta"
save "$path_master/Final_matches_ND.dta", replace
