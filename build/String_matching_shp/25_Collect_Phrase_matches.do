do 0_Set_Path.do

global path_in="C:/Users/Chimmay Lohani/Dropbox/EW_Downloads/Code/Deliverables/Rest"
global path_out="$path_in"
global path_temp= "$path_in/Temp"

local stubs Colorado CVP

*** Colorado ***
local stub Colorado
use "$path_in/`stub'/Collected_phrase_matches.dta", replace
gsort -similscore
cap gen bad_match=1
	
local good_matches 23 24 25 26 27 28 32 51 52
forvalues i=1(1)19 {
	local good_matches `good_matches' `i'
}

foreach match of local good_matches {
	replace bad_match=0 in `match'
}
save "$path_in/`stub'/Collected_phrase_matches.dta", replace

use "$path_in/`stub'/Naive_match.dta", replace
gsort -similscore
cap gen bad_match=1

local good_matches 32 431 432 525 526 527 528 595 619 620 621 739 875 1216 1993 ///
	1994 4330 4331 4427
	
forvalues i=1(1)13 {
	local good_matches `good_matches' `i'
}

foreach match of local good_matches {
	replace bad_match=0 in `match'
}

save "$path_in/`stub'/Naive_match.dta", replace

*** CVP ***
local stub CVP 
use "$path_in/`stub'/Collected_phrase_matches.dta", clear
gsort -similscore
drop similscore
duplicates drop
cap gen bad_match=1

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

local stub CVP
use "$path_in/`stub'/Naive_match.dta", clear
cap gen bad_match=1

local good_matches 59 61 68 69 70 71 75 76 77 78 89 90 91 92 97 100 102 107 122 123 124 125 ///
	151 152 159 192 200 229 282 287 291 322 324 329 336 337 338 350 409 597 648 682 835 836 ///
	953 1042 1115 1235 1236 1271 1376
	
forvalues i=1(1)31 {
	local good_matches `good_matches' `i'
}

forvalues i=33(1)54 {
	local good_matches `good_matches' `i'
}

foreach match of local good_matches {
	replace bad_match=0 in `match'
}
save "$path_in/`stub'/Naive_match.dta", replace
