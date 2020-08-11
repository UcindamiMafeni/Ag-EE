// list of good matches for Tier_3
do 0_Set_Path.do

global path_in= "$path_master/Cut_1"
global path_out= "$path_master/Cut_1"


#delimit ;
local good_matches 66 72 88 89 90 98 100 101 117 118 119 137 138 169 170 178 182 195 200 229 237 238 245 247 
	260 261 262 343 375 410 503 505 569 570 579 581 586 587 588 589 590 591 596 631 632 677 690 721 769 770 796 
	797 798 808 809 810 972 1125 1126 1140 1156 1158 1159 1162 1226 1345 1346 1347 1424 1438 1439 1440 1581 1582;
#delimit cr

use "$path_in/Tier_3.dta", clear
replace bad_match=1
gsort -similscore

foreach i of local good_matches {
	replace bad_match=0 in `i'
}

save "$path_out/Tier_3.dta", replace

/*
//name_DBF has some commonly found names
gen common=0
replace common=1 if strpos(name_DBF,"calif")>0 & (strpos(name_DBF,"service")>0 | strpos(name_DBF,"amenties")>0) 
replace common=1 if strpos(name_DBF,"golden state")>0
replace common=1 if strpos(name_DBF,"sweetwater")>0
replace common=1 if strpos(name_DBF,"los angeles county")>0
keep if common==1
gsort -similscore

save "$path_out/Common_phrases.dta", replace
