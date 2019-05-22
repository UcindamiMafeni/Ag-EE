do 0_Set_Path.do

global path_in= "C:/Users/Chimmay Lohani/Dropbox/Water_Data"
global path_out="C:/Users/Chimmay Lohani/Dropbox/EW_Downloads/Code/Deliverables/Rest"
global path_temp= "$path_master/Temp"

//we will input files from the three locations, generate a key and clean up the names to make them ready for matching

local file_Colorado Colorado_Contracts_Mod
local file_CVP CVP_Contracts
local file_SWP Modern_allocations

local stubs Colorado CVP SWP

foreach stub of local stubs {
	use "$path_in/`stub'/`file_`stub''.dta", clear
	gen index= _n
	gen name_dup= name
	
	//bogus spaces around brackets
	replace name_dup= subinstr(name_dup,"( ","(",.)
	replace name_dup= subinstr(name_dup," )",")",.)
	
	
	qui ds
	local N=_N
	set trace on
	
	//remove the part under brackets if its more than length 20 chars
	/*
	forvalues i=1(1)`N' {
		local str= name_dup[`i']
		//if there is an open bracket
		if strpos("`str'","(")>0 {
			local pos_op= strpos("`str'","(")
		//if there is a closed bracket
			if strpos("`str'",")")>0 {
				local pos_cl= strpos("`str'",")")
		//if they are far apart
				if `pos_cl'-`pos_op'>20 {
		//insuring against lazy evaluation. Does stata lazy evaluate? who knows!
					local str_2= substr("`str'",1,`pos_op') + substr("`str'",`pos_cl',.)
					replace name_dup= "`str_2'" in `i'
				}
			}
		}
	}
	*/
	
	split name_dup, p(( ))
	local vars `r(varlist)'
	local index=1
	foreach var of local vars {
		//the even stubs are the ones enclosed within brackets
		if mod(`index',2)==0 {
			replace `var'= "" if strlen(`var')>20
		}
		
		local index= `index' + 1
	}
	replace name_dup= ""
	foreach var of local vars {
		replace name_dup= name_dup + " " + `var'
	}
	
	replace name_dup= subinstr(name_dup, " wd", " Water District",.)
	replace name_dup= subinstr(name_dup, " WD", " Water District",.)
	replace name_dup= subinstr(name_dup, " Wd", " Water District",.)
	
	replace name_dup= stritrim(name_dup)
	replace name_dup= strtrim(name_dup)
	
	//there's some hand-cleaning required for the Colorado data
	if "`stub'"=="Colorado" {
		replace name_dup = "Palo Verde Irrigation District" in 103
		replace name_dup = "Yuma Project Reservation Division" in 104
		replace name_dup = "Imperial Irrigation District" in 105
		replace name_dup = "Metropolitan Water District City of Los Angeles" in 107
		replace name_dup = "Metropolitan Water District City of Los Angeles" in 108
		replace name_dup = "Imperial Irrigation District Coachella Valley" in 110
		replace name_dup = "Palo Verde Irrigation District" in 111
	}
	
	replace name_dup= ustrlower(name_dup)
	rename name_dup name_du
	drop name_dup*
	rename name_du name_dup
	save "$path_out/`stub'/Contracts_cleaned.dta", replace
}


import dbase "$path_master/Water_Districts.dbf", clear
keep OBJECTID AGENCYNAME
gen name_shp= ustrlower(AGENCYNAME)
egen index_shp= group(name_shp)

save "$path_master/Water_Districts.dta", replace
