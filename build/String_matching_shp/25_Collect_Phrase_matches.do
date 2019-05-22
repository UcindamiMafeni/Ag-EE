do 0_Set_Path.do

global path_in="C:/Users/Chimmay Lohani/Dropbox/EW_Downloads/Code/Deliverables/Rest"
global path_out="$path_in"
global path_temp= "$path_in/Temp"

local stubs Colorado CVP

*** Colorado ***
local stub Colorado
use "$path_in/`stub'/Collected_phrase_matches.dta", replace
gsort -similscore
gen bad_match=1
	
local good_matches 23 24 25 26 27 28 32 51 52
forvalues i=1(1)19 {
	local good_matches `good_matches' `i'
}

foreach match of local good_matches {
	replace bad_match=0 in `match'
}
save "$path_in/`stub'/Collected_phrase_matches.dta", replace


*** CVP ***
local stub CVP 
use "$path_in/`stub'/Collected_phrase_matches.dta", clear
gsort -similscore
drop similscore
duplicates drop
gen bad_match=1

local good_matches 117 118 119 122 123 124 125 126 127 128 130 131 134 135 136 138 139 ///
	142 143 144 147 148 149 154 159 163 164 169 172 173 176 178 202 205 238 248 250 272 ///
	312 390 442 268 550 551 552 559 610 661 797 869 964 965 966 967 974 977 1042

forvalues i=1(1)115 {
	local good_matches `good_matches' `i'
}

foreach match of local good_matches {
	replace bad_match=0 in `match'
}
save "$path_in/`stub'/Collected_phrase_matches.dta", replace
