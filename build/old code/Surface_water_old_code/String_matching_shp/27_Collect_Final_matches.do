do 0_Set_Path.do

global path_in="C:/Users/Chimmay Lohani/Dropbox/EW_Downloads/Code/Deliverables/Rest"
global path_out="$path_in"
global path_temp= "$path_in/Temp"

local stubs Colorado CVP
local files Naive_match Collected_phrase_matches

foreach stub of local stubs {
	clear
	set obs 1
	gen delete=""
	save "$path_in/`stub'/Collected_Final_Matches.dta", replace
	foreach file of local files {
		use "$path_in/`stub'/`file'.dta", clear
		keep if bad_match==0
		keep index_shp index
		merge m:1 index using "$path_in/`stub'/Contracts_cleaned.dta"
		keep if _merge==3
		drop _merge
		joinby index_shp using "$path_master/Water_Districts.dta", unmatched(both)
		keep if _merge==3
		drop _merge
		append using "$path_in/`stub'/Collected_Final_Matches.dta"
		save "$path_in/`stub'/Collected_Final_Matches.dta", replace
	}
	drop if _n==_N
	drop delete
	save "$path_in/`stub'/Collected_Final_Matches.dta", replace
}
