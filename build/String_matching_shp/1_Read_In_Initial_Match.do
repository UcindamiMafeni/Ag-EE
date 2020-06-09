*** READ IN FILES AND CREATE MASTERS ***
/*
Aim:
Take in files, find if they have certain phrases, create master with 
and without duplicates.

Inputs: Database of water districts, input of WRIMS file.
Outputs: Cleaned name masters with dummies for farm related phrases.

*/

do 0_Set_Path.do

global path_in= "$path_master"
global path_names= "$path_master/Names"
global path_temp= "$path_master/Temp"

import dbase "$path_in/Water_Districts.dbf", clear

keep AGENCYNAME 
rename AGENCYNAME name
duplicates drop 

save "$path_names/Entity_Names_DBF.dta", replace

keep name
gen dup_name= ustrlower(name)
replace dup_name= strtrim(dup_name)
replace dup_name=strltrim(dup_name)
gen has_farm=0
replace has_farm=1 if strpos(dup_name, "farm")>0 

gen has_water=0
replace has_water=1 if strpos(dup_name, "water")>0

gen has_distr=0
replace has_distr=1 if strpos(dup_name,"district")>0

gen has_irrigation=0
replace has_irrigation=1 if strpos(dup_name,"irrigation")>0
cap gen id_DBF=_n
save "$path_names/Entity_Names_DBF.dta", replace


use "$path_in/Primary_Owner_names_CleanedFile.dta", clear
keep name
gen dup_name= ustrlower(name)
replace dup_name= strtrim(dup_name)
replace dup_name=strltrim(dup_name)
gen has_farm=0
replace has_farm=1 if strpos(dup_name, "farm")>0

gen has_farm_like=0
replace has_farm_like=1 if strpos(dup_name, "acreage")>0 | strpos(dup_name, "field")>0 | strpos(dup_name,"homestead")>0 | strpos(dup_name,"lawn")>0 | strpos(dup_name,"meadow")>0 | strpos(dup_name,"nursery")>0 | strpos(dup_name,"orchard")>0 | strpos(dup_name,"pasture")>0 | strpos(dup_name,"plantation")>0 | strpos(dup_name,"ranch")>0 | strpos(dup_name,"acres")>0

gen has_water=0
replace has_water=1 if strpos(dup_name, "water")>0

gen has_distr=0
replace has_distr=1 if strpos(dup_name,"district")>0

gen has_irrigation=0
replace has_irrigation=1 if strpos(dup_name,"irrigation")>0

local haves has_farm has_farm_like has_water has_distr has_irrigation

gen has_sth=0
foreach have of local haves {
	replace has_sth= max(has_sth,`have')
}

//no duplicates dropped here, "original"
save "$path_names/Primary_Owner_names_CleanedFile_NDD.dta", replace

use "$path_names/Primary_Owner_names_CleanedFile_NDD.dta", clear
drop name
duplicates drop
save "$path_names/Primary_Owner_names_CleanedFile.dta", replace

keep if has_sth==1
gen id_PO=_n
save "$path_names/First_Cut_PO.dta", replace

*** Fuzzy matching on first cut

// matchit idmaster txtmaster using filename.dta , idusing(varname) txtusing(varname) [options]

//NOTE: here I change dup_name to name, original still has a variable named name, change name back to dup_name to match
use "$path_names/First_Cut_PO.dta", clear
cap drop name_PO
rename dup_name name
keep name id_PO
//HERE THERE WAS A TEMP IDK
save "$path_names/First_Cut_PO.dta", replace

/*
use "$path_names/Entity_Names_DBF.dta", clear
keep dup_name
cap gen id_master= _n
rename dup_name name_master
replace name_master=ustrlower(name_master)

matchit id_master name_master using "$path_temp/First_Cut_PO.dta", idusing(id_PO) txtusing(name_PO) override

save "$path_in/Cut_1/Matchit_Naive.dta", replace
