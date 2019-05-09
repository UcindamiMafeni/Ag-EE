do 0_Set_Path.do

global path_in= "$path_master/Cut_2"
global path_names= "$path_master/Names"
global path_temp= "$path_master/Temp"

use "$path_in/Intmdt_Master_DBF_post_containment.dta", clear


local phrases water irrigation district county storage community services service conservation municipal utility utilities city town of area company public park system project resort ranches ranch inn

cap drop has*
replace name=ustrlower(name)
gen name_ex=name
gen excised=0
//set trace on 
foreach phrase of local phrases {
	gen has_`phrase'=0
	replace has_`phrase'=1 if strpos(name,"`phrase'")>0
	replace name_ex= subinstr(name_ex, "`phrase'","",.)
	replace excised= 1 if strpos(name,"`phrase'")>0
}

keep if excised==1
keep name name_ex id_DBF

save "$path_temp/DBF_excised_PostCut2.dta", replace


use "$path_names/Primary_Owner_Post1Cut.dta", clear
gen name_PO_ex=name_PO
gen excised=0
//set trace on 
foreach phrase of local phrases {
	gen has_`phrase'=0
	replace has_`phrase'=1 if strpos(name_PO,"`phrase'")>0
	replace name_PO_ex= subinstr(name_PO_ex, "`phrase'","",.)
	replace excised= 1 if strpos(name_PO,"`phrase'")>0
}
keep if excised==1
keep name_PO name_PO_ex id_PO
save "$path_temp/PO_excised_PostCut2.dta", replace

use "$path_temp/DBF_excised_PostCut2.dta", clear
matchit id_DBF name_ex using "$path_temp/PO_excised_PostCut2.dta", idusing(id_PO) txtusing(name_PO_ex) override

save "$path_in/Match_phrases_Cut2.dta", replace

merge m:1 id_DBF using "$path_in/Intmdt_Master_DBF_post_containment.dta"
keep if _merge==3
drop _merge

merge m:1 id_PO using "$path_names/Primary_Owner_Post1Cut.dta"
keep if _merge==3
drop _merge

order name, after(name_PO)
gsort id_DBF -similscore

save "$path_in/Match_phrases_Cut2.dta", replace
