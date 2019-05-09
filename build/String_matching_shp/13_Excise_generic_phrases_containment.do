do 0_Set_Path.do

global path_in= "$path_master/Cut_2"
global path_names= "$path_master/Names"
global path_temp= "$path_master/Temp"

use "$path_in/Intmdt_Master_DBF_post_containment.dta", clear


local phrases water irrigation district county storage community services service conservation municipal utility utilities city town of area company public park system project resort ranch inn

replace name=ustrlower(name)
gen name_ex=name
gen excised=0
cap drop has*
//set trace on 
foreach phrase of local phrases {
	gen has_`phrase'=0
	replace has_`phrase'=1 if strpos(name,"`phrase'")>0
	replace name_ex= subinstr(name_ex, "`phrase'","",.)
	replace excised= 1 if strpos(name,"`phrase'")>0
}

keep if excised==1
keep name name_ex id_DBF

save "$path_temp/Containment_with_DBF_only.dta", replace

matchit id_DBF name_ex using "$path_names/Primary_Owner_Post1Cut.dta", idusing(id_PO) txtusing(name_PO) override  threshold(0.33)

local names name_DBF name_PO

local master_n name_PO
local split_n name_ex

		if "`master_n'"=="`split_n'" continue
		
		split `master_n'
		local masters "`r(varlist)'"

		split `split_n'
		local splits "`r(varlist)'"
		local n_splits `r(nvars)'
		
		local counter=1
		foreach split_v of local splits {
			gen flag_`counter'=0
			foreach master_v of local masters {
				replace flag_`counter'=1 if strpos(`master_v',`split_v')>0
			}
			local counter= `counter'+1
		}
		//has flag raised if that word is contained in the master
		gen `master_n'_contains=1
		forvalues i=1(1)`n_splits' {
			//if flag is ever lowered, that word isn't contained and containment fails
			replace `master_n'_contains=0 if flag_`i'==0
		
		}
		drop flag_*
		drop `masters' `splits'


save "$path_in/Containment_phrases_Cut2.dta", replace
keep if name_PO_contains==1
save "$path_in/Containment_phrases_Cut2_rest.dta", replace

use "$path_in/Containment_phrases_Cut2_rest.dta", clear
merge m:1 id_DBF using "$path_in/Intmdt_Master_DBF_post_containment.dta"
keep if _merge==3
drop _merge

merge m:1 id_PO using "$path_names/Primary_Owner_Post1Cut.dta"
keep if _merge==3
drop _merge

order name, after(name_PO)

gsort id_DBF -similscore
save "$path_in/Containment_phrases_Cut2_rest.dta", replace
