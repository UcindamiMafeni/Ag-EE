global path_in= "C:\Users\clohani\Dropbox\New_Water_Districts"

import delimited using "$path_in/Intersected_data_Wdis_Cal.csv", clear varnames(1)
keep name
duplicates drop
save "$path_in/New_WD_which_intersect_with_old.dta", replace

import delimited using "$path_in/New_Wdist_WRIMS.csv", clear varnames(1)
rename in_wdistname name
merge m:1 name using "$path_in/New_WD_which_intersect_with_old.dta"
keep if _merge==1

