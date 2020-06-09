
do 0_Set_Path.do

global path_in= "$path_master/Cut_2"
global path_out= "$path_master/Cut_2"
global path_temp= "$path_master/Temp"

#delimit ;
local good_matches 44 69 127 198 339 386 390 415 420 436 453 455 500 647 650 1011 1048 1291;
#delimit cr
	
use "$path_in/Containment_phrases_Cut2_rest.dta", clear
cap gen bad_match=1

foreach i of local good_matches {
	replace bad_match=0 in `i'
}
save "$path_out/Containment_phrases_Cut2_rest.dta", replace

// exclude the ones matched well from the list of excised phrases

keep if bad_match==0
keep id_DBF
duplicates drop
save "$path_temp/DBF_matched_on_Excised_Phrases.dta", replace

use "$path_in/Match_phrases_Cut2.dta", clear
merge m:1 id_DBF using "$path_temp/DBF_matched_on_Excised_Phrases.dta"
drop if _merge==3

gsort id_DBF -similscore
save "$path_out/Match_phrases_Cut2.dta", replace
//now we don't have to trawl through the list for shapefiles that are already matched


use "$path_in/Matchit_leftover_top5_post_containment.dta", clear
merge m:1 id_DBF using "$path_temp/DBF_matched_on_Excised_Phrases.dta"
drop if _merge==3

gsort id_DBF -similscore
save "$path_out/Matchit_leftover_top5_post_containment.dta", replace

