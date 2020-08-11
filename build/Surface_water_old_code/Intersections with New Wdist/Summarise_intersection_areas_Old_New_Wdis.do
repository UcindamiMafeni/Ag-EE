global path_in= "C:\Users\clohani\Dropbox\New_Water_Districts"

import delimited "$path_in/Wdist.csv", clear
tostring objectid, replace
drop if missing(objectid)
duplicates drop objectid, force
save "$path_in/Wdist.dta", replace

import delimited using "$path_in/Intersected_data_Wdis_Cal.csv", clear
merge m:1 objectid using "$path_in/Wdist.dta"

** merge==1 means only in master, which means no intersections for these shapefiles, 
** merge==2 means no intersections for these water districts"

gsort objectid -inter_area
gen frac_area= inter_area/tot_area
by objectid: egen max_inter= max(frac_area)
gen largestInter=0
by objectid: replace largestInter=1 if _n==1

/* this block is to match up with one of the datasets and see how much of the new matched wdis have intersections*/
drop _merge
merge m:1 gid using "C:\Users\clohani\Dropbox\New_Water_Districts\Acceptable_matches_new_Wdis/Temp_overlapping_wdis_matches.dta"
keep if _merge==3

sum frac_area, det

sum max_inter if largestInter==1, det

stop
local cutoff= 0.8

keep if frac_area>= `cutoff'

save "$path_in/Wdist_large_area.dta", replace
