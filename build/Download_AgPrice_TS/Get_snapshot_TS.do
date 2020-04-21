global path_in= "C:\Users\clohani\OneDrive\Desktop"
global path_out= "C:\Users\clohani\Dropbox\California_Crop_Snapshots\Temp"


//import excel "$path_in/Master_list_files.xlsx", clear firstrow

import delimited "$path_in/AG_TS_output.csv", clear
rename (v1 v2 v3 v4) (filename name A subfolder)

//this segment checks for phrase States and United States
replace name= subinstr(name, ":", "",.)
replace name= subinstr(name, "–", "",.)
split name, parse("States")
keep if !missing(name3)

//this segment gives us the crop
replace name1= subinstr(name1, "Prices Received for", "", .)
replace name1= strtrim(name1)
replace name1= subinstr(name1, " ", "_",.)
rename name1 crop

//this segment gives us the month-year
replace name3= substr(name3,strpos(name3," ")+1,.)
split name3, parse(" ")
rename (name31 name32) (month yr)
drop name2 name3*

//read files

gen filepath= "C:\Users\clohani\Dropbox\California_Crop_Snapshots\Zip\" + subfolder + "/" + filename
drop if !(missing(real(substr(month,1,1))) | !missing(real(substr(yr,1,1))))

save "$path_in/List_master_Ag_crops.dta", replace

local M= _N
local count_missing=0
forvalues j=1(1)`M' {

use "$path_in/List_master_Ag_crops.dta", clear

local file= filepath[`j']
local mnth= substr(month[`j'],1,3)
local yr= substr(yr[`j'],3,2)
local crop= crop[`j']

capture confirm new file `file'
if _rc==0 {
    dis "FLAG FLAG FLAG"
	continue
}

import delimited using "`file'", clear
local N= _N


//finding rownumber of units
forvalues i=1(1)`N' {
 if v2[`i']=="u" {
	local row_units `i'
	break
 }
}

qui des, varlist
local vars= r(varlist)

//finding variable which has price of the month we want; name of
local price_not_found=1
local state_not_found=1
foreach var of local vars {
	forvalues i=1(1)`N' {
		local current= `var'[`i']
		local next= `var'[`i'+1]
		if "`current'"== "United States" {
			local state_var `var'
			local state_not_found=0
		}
		
		if strpos("`current'", "`mnth'")>0 {
			if strpos("`current'", "`yr'")>0 | strpos("`next'", "`yr'")>0 {
				local price_var `var'
				local price_not_found=0
			}
		}
	}
}

if `price_not_found'==1 | `state_not_found'==1 {
	dis "MISSING MISSING MISSING"
	continue
	local count_missing= `count_missing'+1
}

keep v2 `state_var' `price_var'
local units= `price_var'[`row_units']
keep `state_var' `price_var'

qui des, varlist
local varlist= r(varlist)
local cnt=0
foreach var of local varlist {
	local cnt= `cnt' +1
}
if `cnt'<2 {
	dis "MISSING MISSING MISSING"
	local count_missing= `count_missing'+1
	continue
}

rename (`state_var' `price_var') (state price)
keep if state== "California" | state=="United States"
gen crop= "`crop'"
gen units= "`units'"
gen month= "`mnth'"
gen yr= "`yr'"


save "$path_out/Temp_`yr'_`mnth'_`j'.dta", replace
}

dis "`count_missing'"
