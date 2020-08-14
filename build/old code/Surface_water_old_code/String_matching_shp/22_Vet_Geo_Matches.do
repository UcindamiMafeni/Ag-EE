do 0_Set_Path.do

global path_in= "$path_master/Cut_3"
global path_out= "$path_master/Cut_3"
global path_temp= "$path_master/Temp"
global path_names= "$path_master/Names"

use "$path_master/Geo_and_string_matches.dta", clear

keep if geo_matched
keep if !both_valid

save "$path_master/Vetted_matches.dta", replace

keep name_DBF name_PO
duplicates drop

gen name_DBF_dup= ustrlower(name_DBF)
gen name_PO_dup= ustrlower(name_PO)
strdist name_DBF_dup name_PO_dup, gen(dist)

sort dist

gen bad_matches=1
replace bad_matches= 0 if _n<=30

local bad 12 21 22 24 26 28 29 30
foreach b of local bad {
	replace bad_match=1 in `b'
}

local good_matches 32 33 37 40 42 53 54 55 68 75 88 120 130 136 153 155 174 180 218 231 ///
	237 242 258 259 333 346 366 367 376 379 389 400 408 411 418 426 434 464 466 468 477 ///
	489 494 511 513 517 526 529 533 537 564 570 573 612 622 633 669 688 691 706 720 723 ///
	724 822 824 858 872 896 903 942 949 953 994 1000 1023 1056 1085 1093 1095 1109 1138 ///
	1170 1182 1205 1213 1225 1362 1401 1531 1550 1667 1668 1669 1710 1714 1715 1736 1753 ///
	1761 1791 1866 1893 1944 1980 1983 1991 2086 2092 2112 2146 2346 2480 2492 2547 2673 ///
	2910 2995 3031 3269 3310 3385 3767 3773 3789 3816 3916 3984 3994 4001 4150 4203 4411 ///
	4445 4484 4486 4576 4603 4992 5022 5178 5495 5496 5498 5527 5874 6168 6252 6316 6322
	
foreach match of local good_matches {
	replace bad_match= 0 in `match'

}
keep if bad_match==0
drop bad_match
save "$path_temp/okay_names_geo.dta", replace
//now we need to merge these matches with the ones that are a) okay distance_wise b)are contained and string matched

use "$path_master/Vetted_matches.dta", clear
merge m:1 name_DBF name_PO using "$path_temp/okay_names_geo.dta"
keep if _merge==3
drop _merge
save "$path_temp/Final_matches.dta", replace

use "$path_master/Geo_and_string_matches.dta", clear
keep if both_valid==1

append using "$path_temp/Final_matches.dta"
save "$path_temp/Final_matches.dta", replace

use "$path_in/Match_with_distances.dta", clear
local lim= 5

//limit the distance upto which string matches are considered valid
keep if distance<= `lim'
append using "$path_temp/Final_matches.dta"
cap drop name_DBF_* name_PO_* dist* merge*
save "$path_master/Final_matches.dta", replace
keep name_DBF name_PO
duplicates drop
save "$path_master/Final_matches_ND.dta", replace
