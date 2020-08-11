do 0_Set_Path.do

global path_in= "$path_master/Cut_3"
global path_out= "$path_master/Cut_3"
global path_temp= "$path_master/Temp"

import delimited using "$path_in/Match_with_distances.csv", clear

cap drop v1
cap rename (name_po name_dbf) (name_PO name_DBF)
sort distance 

save "$path_out/Match_with_distances.dta", replace

