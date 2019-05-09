do 0_Set_Path.do

global path_in= "$path_master/Cut_2"
global path_names= "$path_master/Names"
global path_temp= "$path_master/Temp"

use "$path_in/Containment_match_Cut2.dta", clear
keep if bad_match!=1
keep id_DBF
duplicates drop
save "$path_temp/Cut2_DBF_matches.dta", replace

use "$path_names/Master_DBF_after_Cut1.dta", clear
cap drop _merge
merge 1:1 id_DBF using "$path_temp/Cut2_DBF_matches.dta"
//assert no using entry missing from master
assert _merge!=2
drop if _merge==3
drop _merge
save "$path_in/Intmdt_Master_DBF_post_containment.dta", replace

use "$path_in/Matchit_Post1Cut.dta", clear
merge m:1 id_DBF using "$path_temp/Cut2_DBF_matches.dta"
assert _merge!=2
drop if _merge==3
drop _merge
save "$path_in/Matchit_leftover_post_containment.dta", replace

use "$path_in/Matchit_leftover_post_containment.dta", clear
gsort id_DBF -similscore
by id_DBF: keep if _n<=5

save "$path_in/Matchit_leftover_top5_post_containment.dta", replace
