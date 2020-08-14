*** MARK SHAPEFILES WHICH HAVEN'T BEEN MATCHED ***
/*
	Input:
	Tier 1,2,3 matches which have been marked so far, and containment match.
		
	Output:
	An output master which only contains the shapefiles that havent been matched.
		$path_master/Names/Master_DBF_after_Cut1.dta
*/

do 0_Set_Path.do

global path_in= "$path_master/Cut_1"
global path_out= "$path_master/Cut_1"

local files Tier_1 Tier_2 Tier_3

clear
set obs 1
gen id_DBF=.

save "$path_out/Matched_DBFs_in_Cut1.dta", replace
foreach f of local files {
	use "$path_in/`f'.dta", clear
	drop if bad_match==1
	keep id_DBF 
	append using "$path_out/Matched_DBFs_in_Cut1.dta"
	save "$path_out/Matched_DBFs_in_Cut1.dta", replace
	//
}
use "$path_in/Containment_match_T3.dta", clear
keep if name_DBF_contains==1 | name_PO_contains==1
keep id_DBF
append using "$path_out/Matched_DBFs_in_Cut1.dta"
//this is the empty observation used to append on
drop if _n==_N

duplicates drop
save "$path_out/Matched_DBFs_in_Cut1.dta", replace

use "$path_master/Names/Entity_Names_DBF.dta", clear
merge 1:1 id_DBF using "$path_in/Matched_DBFs_in_Cut1.dta"
//assert no using entry missing from master
assert _merge!=2
drop if _merge==3
//keep only the ones that have not been matched for now
save "$path_master/Names/Master_DBF_after_Cut1.dta", replace
