global path_in= "C:/Users/Chimmay Lohani/Desktop/Ag_Data/CVP/Fulfilment/Yearwise"
global path_out= "C:/Users/Chimmay Lohani/Desktop/Ag_Data/CVP/Fulfilment/Stata"
global path_root= "C:/Users/Chimmay Lohani/Desktop/Ag_Data/CVP/Fulfilment"

//Path in contains yearwise fulfilment data. These yearwise files were created by splitting the consolidated excel sheet by hand
cd "$path_in"
local files: dir . files "*.xlsx"
foreach f of local files {
	import excel using "$path_in/`f'", clear

	drop if missing(B)
	missings dropvars, force
	local yr= substr("`f'",1,4)
	
	qui ds
	local vars `r(varlist)'
	local index=1
	foreach var of local vars {
		if `index'==1 rename `var' group
		if `index'==2 rename `var' contractors
		if `index'==3 rename `var' supply_`yr'
		local index= `index'+1
	}
	drop in 1
	replace group= "Misc" if missing(group)
	destring supply_`yr',replace
	//for these years, the name of the contractor read "Settlement/Water rights" as opposed to "Sacramento. exchange", I was unsure if I had moved it around by mistake so I double check that here
	if "`yr'"=="2009" | "`yr'"=="2008" | "`yr'"=="2007" | "`yr'"=="2006" | "`yr'"=="2005" | "`yr'"=="2004" | "`yr'"=="2003" | "`yr'"=="2002" | "`yr'"=="2001" | "`yr'"=="2000" | "`yr'"=="1999" | "`yr'"=="1998" | "`yr'"=="1997" | "`yr'"=="1996" {
		local N=_N
		forvalues i=1(1)`N' {
			if contractors[`i']=="Settlement Contractors/Water Rights" {
				replace contractors= "Sacramento River water rights and San Joaquin River exchange" in `i'
			}
		}
	
	}
	local name= substr("`f'",1, 15)
	save "$path_out/`name'.dta", replace
}


//creating a roster of names to match on, for a consolidated list of fulfilments
clear
set obs 1
gen group=""
gen contractors=""
save "$path_out/Matches.dta", replace

cd "$path_out"
local files: dir . files "*.dta"

foreach f of local files {
	use "$path_out/`f'", clear
	keep group contractors
	append using "$path_out/Matches.dta"
	save "$path_out/Matches.dta", replace
}
duplicates drop
save "$path_root/Matches.dta", replace
//move matches out of this folder to another directory 

//matching on names and creating a consolidated list

cd "$path_out"
local files: dir . files "*.dta"

use "$path_root/Matches.dta", clear

foreach f of local files {
	merge 1:1 group contractors using "$path_in/Stata/`f'"
	drop _merge
}
drop in 1
drop G

//the entire commitment was met in this period
forvalues i=1978(1)1989 {
	gen supply_`i'=1
}

save "$path_root/CVP_fulfilment.dta", replace
