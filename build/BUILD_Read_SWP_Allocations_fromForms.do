global path_in= "C:/Users/Chimmay Lohani/Desktop/Ag_Data/SWP/Allocations"
global path_out= "C:/Users/Chimmay Lohani/Desktop/Ag_Data/SWP/temp"
global path_swp= "C:/Users/Chimmay Lohani/Desktop/Ag_Data/SWP"

//form data is super badly formatted and quickest read into dta files by hand
//path_in contains these hand fed forms in dta format
clear
set obs 1
gen name=""
gen region=""
save "$path_out/Matches.dta", replace

cd "$path_in"
local files: dir . files "*.dta"

foreach f of local files {
	use "$path_in/`f'", clear
	keep name region
	append using "$path_out/Matches.dta"
	save "$path_out/Matches.dta", replace
}
duplicates drop
drop if missing(name)
save "$path_out/Matches.dta", replace


cd "$path_out"
local filez: dir . files "*.dta"

use "$path_swp/Matches.dta", clear

foreach f of local filez {
merge 1:1 name region using "$path_out/`f'"
drop _merge
}

save "$path_swp/Modern_allocations.dta", replace
