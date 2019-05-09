//now we use the remaining unmatched DBF files to find their matches

do 0_Set_Path.do

global path_in= "$path_master/Names"
global path_out= "$path_master/Cut_2"
global path_temp= "$path_master/Temp"

use "$path_in/Master_DBF_after_Cut1.dta", clear
cap drop _merge
gen name_DBF= ustrlower(name)
drop name
replace name_DBF= strtrim(name_DBF)
replace name_DBF=stritrim(name_DBF)

save "$path_temp/Master_Post1Cut.dta", replace

//at this stage, the other list isnt numbered. We number it for further analysis. Again, dup_name becomes name_PO, change back to match
use "$path_in/Primary_Owner_names_CleanedFile.dta", clear
gen id_PO= _n
keep dup_name id_PO
rename dup_name name_PO
replace name_PO=ustrlower(name_PO)
save "$path_in/Primary_Owner_Post1Cut.dta", replace

use "$path_temp/Master_Post1Cut.dta", clear
matchit id_DBF name_DBF using "$path_in/Primary_Owner_Post1Cut.dta", idusing(id_PO) txtusing(name_PO) override

save "$path_out/Matchit_Post1Cut.dta", replace
