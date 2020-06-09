global path_in= "C:\Users\clohani\Downloads"
global path_out= "C:\Users\clohani\Dropbox\California_Crop_TS"

local yrs /* 200708cactb00 200810cactb00 200910cactb00 201010cactb00 201112cactb00 201212cactb00 */ 2013cropyearcactb00 2014cropyearcactb00 ///
		2015cropyearcactb00 2016cropyearDetail 2017CropYearDetail 2018CropYearDetail

local year= 2013
foreach yr of local yrs {

import excel using "$path_in/`yr'.xlsx", clear

drop if _n==1

keep A B C D E F
qui des, varlist
local vars `r(varlist)'
dis "`vars'"
foreach var of local vars{
	local str= `var'[1]
	dis "`str'"
	local name= subinstr("`str'"," ","_",.)
	rename `var' `name'
}

drop if _n<=3

gen marked_for_deletion=0
gen crop= ""
local N= _N

local crop_current= County[1]
replace marked_for_deletion=1 in 1

forvalues i=1(1)`N' {
	replace marked_for_deletion=1 if missing(County[`i'-1]) & missing(County[`i'-2]) & _n==`i'
	if marked_for_deletion[`i']==1 {
		local crop_current= County[`i']
	}
	replace crop= "`crop_current'" in `i'
}

drop if marked_for_deletion==1
drop if missing(County)

drop if strpos(County,"Counties Reporting")>0
drop if strpos(County,"STATE")>0

save "$path_out/`year'_County_Crop_details.dta", replace
local year= `year' + 1

}
