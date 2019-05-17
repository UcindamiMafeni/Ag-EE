do 0_Set_Path.do

global path_in= "$path_master/Cut_1"
global path_out= "$path_master/Cut_1"
global path_temp= "$path_master/Temp"
global path_names= "$path_master/Names"


//The three tiers have matched data, which has names as id_DBF and id_PO

local files Tier_1 Tier_2 Tier_3 

clear
set obs 1
gen id_DBF=.

save "$path_out/Matched.dta", replace
foreach f of local files {
	use "$path_in/`f'.dta", clear
	drop if bad_match==1
	keep id_DBF id_PO
	append using "$path_out/Matched.dta"
	save "$path_out/Matched.dta", replace
}

use "$path_in/Containment_match_T3.dta", clear
keep if name_DBF_contains==1 | name_PO_contains==1
keep id_DBF id_PO
append using "$path_out/Matched.dta"
drop if _n==_N
duplicates drop

save "$path_out/Matched.dta", replace
use "$path_names/Entity_Names_DBF.dta", clear
joinby id_DBF using "$path_out/Matched.dta", unmatched(using)
assert _merge!=2
keep id_DBF id_PO name
rename name name_DBF 
save "$path_out/Matched.dta", replace

//to match the WRIMS file is a two step procedure, get a matched id_PO and original names file in temp, then match on that
use "$path_names/First_Cut_PO.dta", clear
rename name dup_name
save "$path_temp/Merge_PO_step0.dta", replace
use "$path_names/Primary_Owner_names_CleanedFile_NDD.dta", clear
keep name dup_name
merge m:1 dup_name using "$path_temp/Merge_PO_step0.dta"
assert _merge!=2
keep if _merge==3 //only those WRIMS we have used for first cut
drop _merge
save "$path_temp/Merge_PO_step1.dta", replace

use "$path_out/Matched.dta", clear
joinby id_PO using "$path_temp/Merge_PO_step1.dta", unmatched(master)
rename name name_PO
order name_PO, after(name_DBF)
drop dup_name
save "$path_out/Matched.dta", replace

*** Now for those cut out in Cut_2 ***
* here the procedure is that we made new indices for the entire primary owner WRIMS set
* then match them naively, use that to get a first cut containment match
* then excise common phrases to redo some matches


global path_in= "$path_master/Cut_2"
global path_out= "$path_master/Cut_2"

local files Containment_match_Cut2 Containment_phrases_Cut2_rest Match_phrases_Cut2_rest

clear
set obs 1
gen id_DBF=.

save "$path_out/Matched.dta", replace
foreach f of local files {
	use "$path_in/`f'.dta", clear
	drop if bad_match==1
	keep id_DBF id_PO
	append using "$path_out/Matched.dta"
	save "$path_out/Matched.dta", replace
}

drop if _n==_N
duplicates drop

save "$path_out/Matched.dta", replace

use "$path_names/Entity_Names_DBF.dta", clear
joinby id_DBF using "$path_out/Matched.dta", unmatched(using) 
assert _merge!=2
keep id_DBF id_PO name
rename name name_DBF 
save "$path_out/Matched.dta", replace

//now we have the DBF files matched

//again, 2 step procedure to match WRIMS data
use "$path_names/Primary_Owner_Post1Cut.dta", clear
rename name_PO dup_name
save "$path_temp/Merge_PO_step0.dta", replace
use "$path_names/Primary_Owner_names_CleanedFile_NDD.dta", clear
keep name dup_name
merge m:1 dup_name using "$path_temp/Merge_PO_step0.dta"
assert _merge!=2
keep if _merge==3 //only those WRIMS that are matched now
drop _merge
save "$path_temp/Merge_PO_step1.dta", replace

use "$path_out/Matched.dta", clear
joinby id_PO using "$path_temp/Merge_PO_step1.dta", unmatched(master) 
rename name name_PO
order name_PO, after(name_DBF)
drop dup_name
save "$path_out/Matched.dta", replace
