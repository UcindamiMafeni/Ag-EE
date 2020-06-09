global path_in= "C:\Users\clohani\Dropbox\New_Water_Districts"
global path_matched_orig= "C:\Users\clohani\OneDrive\Desktop\Temp"

import delimited "$path_in/New_Wdist_WRIMS.csv", clear varnames(1)
rename in_wdistgid gid


//drop _merge
//gid seems to be the ID for the new waterdistricts (Calhealth)
merge m:1 gid using "$path_in/Wdist_large_area.dta"
gen matched_coincident_wdist= 0
replace matched_coincident_wdist=1 if _merge==3
rename _merge merge1
// here we now have an indicator for whether the given match is on a water 
// district that had a significant overlap with one from the original set


//now we will make an indicator for the WRIMS contracts that were matched themselves

rename (name_po copy_lat copy_lon) (name_PO latitude longitude)
//destring latitude longitude, replace force
//merge m:1 name_PO latitude longitude using "$path_matched_orig/Contracts_matched.dta"
keep if merge1==3 | merge1==2
// drop if merge1==3 | merge1==2

format %30s in_wdistname name_PO
gen flag_okay=0
order in_wdistname, after(name_PO)
order flag_okay, after(in_wdistname)
order in_wdistact*, after(flag_okay)
/*
drop if missing(name_PO)
destring latitude longitude, replace
merge m:1 name_PO latitude longitude using "Matched_WRIMS_Orig_names.dta"
*/

