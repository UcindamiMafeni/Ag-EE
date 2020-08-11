*** Code to identify matched already picked up in a given file, to be not re-looked at later ***

do 0_Set_Path.do

global path_in="C:/Users/Chimmay Lohani/Dropbox/EW_Downloads/Code/Deliverables/Rest"
global path_out="$path_in"
global path_temp= "$path_in/Temp"

//file to input matches from

local stubs Colorado CVP
local fname_in Collected_phrase_matches
local fname_out Contracts_cleaned
local match_on index

foreach stub of local stubs {
	use "$path_in/`stub'/`fname_in'.dta", clear
	keep if bad_match==0
	keep `match_on'
	duplicates drop
	save "$path_temp/`fname_in'_`stub'.dta", replace
	
	use "$path_in/`stub'/`fname_out'.dta", clear
	merge m:1 `match_on' using "$path_temp/`fname_in'_`stub'.dta"
	assert _merge!=2
	drop if _merge==3
	drop _merge
	save "$path_out/`stub'/`fname_out'_excised.dta", replace
	
}
