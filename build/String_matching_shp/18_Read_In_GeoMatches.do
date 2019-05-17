do 0_Set_Path.do

global path_in= "C:/Users/Chimmay Lohani/Dropbox/Water_Districts/misc"
global path_out= "$path_master/Cut_3"

import delimited "$path_in/pge_prem_coord_polygon_wdist_2.txt", clear

keep primary_owner in_wdistagencyname wdist in_wdist nearestwdist latitude longitude

save "$path_out/Geo_matches.dta", replace

keep if in_wdist=="1"
keep primary_owner in_wdistagencyname latitude longitude

rename (primary_owner in_wdistagencyname) (name_PO name_DBF)
save "$path_out/Geo_matches_contained.dta", replace

use "$path_out/Geo_matches.dta", clear

keep if in_wdist=="0"
keep primary_owner in_wdistagencyname latitude longitude

rename (primary_owner in_wdistagencyname) (name_PO name_DBF)
save "$path_out/Geo_matches_nearest.dta", replace
