global path_in= "C:\Users\clohani\Desktop\Code_Chinmay"

import delimited using "$path_in/Intersected_CLU_basin.csv", clear varnames(1)
destring tot_int_area, replace force
gsort clu_id -tot_int_area
by clu_id: keep if _n==1
save "$path_in/CLU merge Basin County/Merged_CLU_basin.dta", replace

import delimited using "$path_in/Intersected_CLU_county.csv", clear varnames(1)
destring tot_int_area, replace force
gsort clu_id -tot_int_area
by clu_id: keep if _n==1
save "$path_in/CLU merge Basin County/Merged_CLU_county.dta", replace
