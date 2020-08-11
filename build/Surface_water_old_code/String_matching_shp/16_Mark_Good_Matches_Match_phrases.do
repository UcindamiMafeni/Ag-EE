do 0_Set_Path.do

global path_in= "$path_master/Cut_2"
global path_out= "$path_master/Cut_2"
global path_temp= "$path_master/Temp"

#delimit ;
local good_matches 5 6 54 57 88 94 96 106 144 278 286 353 364 365 366 395 420 439 458 462 470 479 483 741 742 765 856 1185 1186;
#delimit cr
	
use "$path_in/Match_phrases_Cut2.dta", clear
cap gen bad_match=1

gsort -similscore
foreach i of local good_matches {
	replace bad_match=0 in `i'
}
save "$path_out/Match_phrases_Cut2_rest.dta", replace
