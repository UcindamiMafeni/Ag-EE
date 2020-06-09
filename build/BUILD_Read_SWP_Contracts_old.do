global path_in= "C:/Users/Chimmay Lohani/Desktop/Ag_Data/SWP/Contracts"
global path_out= "C:/Users/Chimmay Lohani/Desktop/Ag_Data/SWP/temp"
global path_swp= "C:/Users/Chimmay Lohani/Desktop/Ag_Data/SWP"
//The excel files are split into four pages, just like in the original report, and then read in
cd "$path_in"
local filez: dir . files "*.xlsx"

 
foreach f of local filez {
import excel using "$path_in/`f'", clear
drop if missing(A)
missings dropvars, force
set trace on

qui ds
local vars `r(varlist)'
foreach var of local vars {
	local entry= `var'[1]
	local entry= subinstr("`entry'"," ","",.)
	local entry= subinstr("`entry'","-","_",.)
	rename `var' `entry'
}
drop in 1
cap rename Name Year
destring *, replace
local name= substr("`f'",1,5)
set trace off
save "$path_out/`name'.dta", replace
}

//put all of these together to get full sample
use "$path_out/con_1.dta", clear
forvalues i=2(1)4 {
	merge 1:1 Year using "$path_out/con_`i'.dta"
}

save "$path_swp/Old_Contracts_LF.dta", replace
