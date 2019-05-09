//marking matches that have full containment one way or the other
//we split on spaces, for both name_DBF and name_PO

do 0_Set_Path.do

global path_in= "$path_master/Cut_1"
global path_out= "$path_master/Cut_1"


local names name_DBF name_PO
 
//set trace on
//Matchit_Post1Cut

//use "$path_in/Matchit_Post1Cut.dta", clear
use "$path_in/Tier_3.dta", clear
foreach split_n of local names {
	foreach master_n of local names {
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
	}
}

save "$path_out/Containment_match_T3.dta", replace
keep if name_PO_contains==1 | name_DBF_contains==1
save "$path_out/Containment_match_T3.dta", replace

/*
save "$path_out/Containment_match_Cut2.dta", replace
keep if name_PO_contains==1 | name_DBF_contains==1
save "$path_out/Containment_match_Cut2.dta", replace
*/
