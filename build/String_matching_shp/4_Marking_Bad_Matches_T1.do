*** DENOTE IF MATCHES ARE BAD ***
/*

Input:
Tier-splitted matches from previous code.
	$path_out/Tier_1.dta

Output:
Tier splitted matches, with indicator for whether match is good or not.
	$path_out/Tier_1.dta

*/

// list of good matches for Tier_3
do 0_Set_Path.do

global path_in= "$path_master/Cut_1"
global path_out= "$path_master/Cut_1"


#delimit ;
local bad_matches 5 7 8 15 16 19 301;
#delimit cr

use "$path_in/Tier_1.dta", clear
replace bad_match=0
gsort -similscore

foreach i of local bad_matches {
	replace bad_match=1 in `i'
}

save "$path_out/Tier_1.dta", replace
