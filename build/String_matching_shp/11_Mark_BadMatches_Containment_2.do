
do 0_Set_Path.do

global path_in= "$path_master/Cut_2"
global path_out= "$path_master/Cut_2"

#delimit ;
local bad_matches 1 13 36 41 43 44 45 55 75 79 80 82 83 85 107 144 145 155 157 158 163 165 169 170
		171 189 193 200 201 205 223 229 231 232 233 234 235 236 237 238 239 240 247 256 266 267 268 277 278 281 311;
#delimit cr

#delimit ;
local bad_scale 48 49 57 85 86 88 89 91 92 93 96 97 98 101 106 110 113 113 114 115 118
		119 141 146 147 148 150 151 152 153 154 156 161 168 173 174 176 178 179 180 183 184 203 207 
		212 214 216 292 293 297 298 299 312;
#delimit cr
	
use "$path_in/Containment_match_Cut2.dta", clear
gen bad_match=.

foreach i of local bad_matches {
	replace bad_match=1 in `i'
}

foreach i of local bad_scale {
	replace bad_match=1 in `i'
}

save "$path_out/Containment_match_Cut2.dta", replace
