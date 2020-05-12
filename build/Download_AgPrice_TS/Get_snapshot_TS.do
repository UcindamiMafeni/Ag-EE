global path_in= "C:\Users\clohani\OneDrive\Desktop"
global path_out= "C:\Users\clohani\Dropbox\California_Crop_Snapshots\Temp"


//import excel "$path_in/Master_list_files.xlsx", clear firstrow

import delimited "$path_in/AG_TS_output.csv", clear
rename (v1 v2 v3 v4) (filename name A subfolder)

//this segment checks for phrase States and United States
replace name= subinstr(name, ":", "",.)
replace name= subinstr(name, "–", "",.)



**** Part I: 2007-2008 ****

preserve

keep if strpos(subfolder,"2007")>0 | strpos(subfolder,"2008")>0

replace name= subinstr(name, " Â", "",.)
keep if strpos(name, "Price")>0
drop if strpos(name, "Average")>0
keep if strpos(name, "by State")>0
gen prices_leading=0
replace prices_leading=1 if strpos(name,"Prices Received ")==2

**lets extract the year(s) of the data
gen yr= ""
split subfolder, p("-")
replace yr= subfolder4
drop subfolder?
gen yr1= substr(yr,1,4)
destring yr1, replace

** we now extract the name of the crop, 
** first, for the ones with prices_leading

gen crop=""
gen index=0
replace index= strpos(name, ",")
replace crop= substr(name,1,index-1) if index>0 & prices_leading==1
replace crop= subinstr(crop,"Prices Received ","",.)
replace crop= strtrim(crop)
replace crop= stritrim(crop)

** for ones without prices_leading
replace index=0
replace index= strpos(name, "  Prices")
replace index= strpos(name,"  Monthly") if index==0 & prices_leading==0
replace index= strpos(name, ",")-1 if (strpos(name, ",")<index | index==0)  & prices_leading==0
replace crop= substr(name,1, index) if index>0 & prices_leading==0

** read files 
gen filepath= "C:\Users\clohani\Dropbox\California_Crop_Snapshots\Zip\" + subfolder + "/" + filename
destring yr1, replace
drop if missing(yr1)


drop if missing(yr1)
save "$path_in/List_2007-08_crops.dta", replace

local M= _N
local count_missing_2=0
forvalues j=1(1)`M' {

use "$path_in/List_2007-08_crops.dta", clear
local file= filepath[`j']
local yr1= yr1[`j']
local crop= crop[`j']

capture confirm new file `file'
if _rc==0 {
    dis "FLAG FLAG FLAG"
	continue
}

import delimited using "`file'", clear
drop if v2=="t"
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
gen f_mnth=0
gen f_yr=0

//finding variable which has price of the month we want; name of
local mnth_not_found=1
local state_not_found=1 
local month_vars
local n_month_vars=0
foreach var of local vars {
	forvalues i=1(1)`N' {
		local current= `var'[`i']
		local next= `var'[`i'+1]
		local cond_mnth strpos("`current'","Jan")>0 | strpos("`current'","Feb")>0 | (strpos("`current'","Mar")>0 & strpos("`current'","Mary")==0) | strpos("`current'","Apr")>0 | strpos("`current'","May")>0 | strpos("`current'","Jun")>0 | strpos("`current'","Jul")>0 | strpos("`current'","Aug")>0 | strpos("`current'","Sep")>0 | strpos("`current'","Oct")>0 | strpos("`current'","Nov")>0 | strpos("`current'","Dec")>0 
		local cond_yr strpos("`current'","2007")>0 | strpos("`current'","2008")>0 | strpos("`current'","2009")>0 | strpos("`current'","2010")>0 | strpos("`current'","2011")>0 | strpos("`current'","2012")>0 | strpos("`current'","2013")>0 | strpos("`current'","2014")>0 | strpos("`current'","2015")>0 | strpos("`current'","2016")>0 | strpos("`current'","2017")>0 | strpos("`current'","2018")>0

		if "`current'"== "United States" | "`current'"== "US" | "`current'"== "USA" {
			local state_var `var'
			local state_not_found=0
		}
		
		//if strpos("`current'","Jan")>0 | strpos("`current'","Feb")>0 | strpos("`current'","Mar")>0 | strpos("`current'","Apr")>0 | strpos("`current'","May")>0 | strpos("`current'","Jun")>0 | strpos("`current'","Jul")>0 | strpos("`current'","Aug")>0 | strpos("`current'","Sep")>0 | strpos("`current'","Oct")>0 | strpos("`current'","Nov")>0 | strpos("`current'","Dec")>0 {

		if `cond_mnth' {
			replace f_mnth=1 in `i'
			local mnth_not_found=0
			local month_vars `month_vars' `var'
		}
		
		replace f_yr=1 in `i' if `cond_yr'
	}
}

if `mnth_not_found'==1 | `state_not_found'==1 {
	dis "MISSING MISSING MISSING"
	local count_missing= `count_missing_2'+1
	continue
	
}


//find unique instances of the variables we have saved, otherwise we can double count
local uniq_month_vars 

foreach var of local month_vars {
	local found =0
	foreach u_var of local uniq_month_vars {
		if "`u_var'"=="`var'" {
			local found=1 
			break
		}
	}
	if `found'==0 {
		local uniq_month_vars `uniq_month_vars' `var'
	}
}

dis "`uniq_month_vars'"

local month_vars `uniq_month_vars'

local except `state_var'
local varlist `uniq_month_vars'
local month_vars: list varlist - except

local n_month_vars=0
foreach var of local month_vars {
	local n_month_vars= `n_month_vars' + 1
}

dis "`n_month_vars'"
dis "`month_vars'"

keep v2 `state_var' `month_vars' f_mnth f_yr
foreach var of local month_vars {
	local units= `var'[`row_units']
}
keep `state_var' `month_vars' f_mnth f_yr
rename (`state_var') (state)
drop if !(missing(state) | state=="State" | state=="state" | state=="United States" | state=="California" | state== "CA" | state== "US" | state== "USA")


keep if state== "California" | state== "CA" | state=="United States" | state== "US" | state== "USA" | f_mnth==1 | f_yr==1

gen crop= "`crop'"
gen units= "`units'"
gen month= ""
gen price=""
gen yr= .

forvalues i=1(1)`n_month_vars' {
	gen mnth_`i'=""
	gen yr_`i'=""
}

local N= _N
forvalues i=1(1)`N' {
	// get months as locals
	if f_mnth[`i']==1 {
		local k=1
		foreach var of local month_vars {
			replace mnth_`k'= `var'[`i'] if _n>`i'
			local k= `k'+1
		}
	}
	
	if f_yr[`i']==1 {
		local k=1
		foreach var of local month_vars {
			replace yr_`k'= `var'[`i'] if _n>`i'
			local k= `k'+1
		}
	}
}


//clean up years
forvalues k=1(1)`n_month_vars' {
	replace yr_`k'= substr(yr_`k',1,strpos(yr_`k',"-")-1) if strpos(yr_`k',"-")>0
}
destring yr_*, replace


gen order= _n
gen to_expand= 0
replace to_expand= `n_month_vars' if f_yr==0 & f_mnth==0
expand to_expand

drop if f_yr==1 | f_mnth==1
drop to_expand

sort state order

local k=1
foreach var of local month_vars {
	rename `var' price_`k'
	local k= `k' + 1
}

local N= _N
forvalues i=1(1)`N' {
	local k= mod(`i',`n_month_vars')
	if `k'==0 local k= `n_month_vars'
	replace month= mnth_`k'[`i'] in `i'
	replace yr= yr_`k'[`i'] in `i'
	replace price= price_`k'[`i'] in `i'
}

drop mnth_* price_*

gen mnth_code=1
replace mnth_code= 2 if substr(month,1,3)=="Feb"
replace mnth_code= 3 if substr(month,1,3)=="Mar"
replace mnth_code= 4 if substr(month,1,3)=="Apr"
replace mnth_code= 5 if substr(month,1,3)=="May"
replace mnth_code= 6 if substr(month,1,3)=="Jun"
replace mnth_code= 7 if substr(month,1,3)=="Jul"
replace mnth_code= 8 if substr(month,1,3)=="Aug"
replace mnth_code= 9 if substr(month,1,3)=="Sep"
replace mnth_code= 10 if substr(month,1,3)=="Oct"
replace mnth_code= 11 if substr(month,1,3)=="Nov"
replace mnth_code= 12 if substr(month,1,3)=="Dec"



local yr= `yr1'
local new_state=0
//take year from data if present
if !missing(yr[1]) {
	local yr1= yr[1]
	local yr=yr[1]
}
replace yr= `yr' if _n==1 & missing(yr[1])
forvalues k=2(1)`N'{
	if state[`k']!= state[`k'-1] {
		local new_state=1
		local yr= `yr1'
		//take year from data if present
		if !missing(yr[`k']) {
			local yr=yr[`k']
		}
		//replace yr= `yr' if _n==`k'
	}
	if state[`k']== state[`k'-1] {
		if mnth_code[`k']== mnth_code[`k'-1]+1 {
			//replace yr= `yr' if _n==`k'
		}
		if strpos(month[`k'],"1/")>0 {
			local yr= `yr' + 1
			//replace yr= `yr' if _n==`k'
		}
		//if the current entry isnt a successor of previous one, reset
		if strpos(month[`k'],"1/")==0 & !(mnth_code[`k']== mnth_code[`k'-1]+1) {
			local yr= `yr1'
			//replace yr= `yr' if _n==`k'
		}
	}
	replace yr= `yr' if _n==`k'
	local new_state=0
}
keep state crop units month price yr mnth_code
gen file= "`file'"
save "$path_out/Files_2007_08/Temp_`yr1'_`j'.dta", replace

}

dis "`count_missing_2'"

restore




**** Part II: Multimonth entries ****

replace name= subinstr(name, " Â", "",.)
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
destring yr, replace force

preserve

replace name= subinstr(name, " Â", "",.)
keep if missing(yr) & strpos(name, "Price")>0
drop if strpos(name, "Average")>0
gen prices_leading=0
replace prices_leading=1 if strpos(name,"Prices Received for")==2

**lets extract the year(s) of the data
drop yr month
gen yr= .
replace name= subinstr(name, " - ", "-",.)
replace name= subinstr(name, "- ", "-",.)
replace name= subinstr(name, " -", "-",.)

replace name= subinstr(name,"(continued)","",.)
replace name= subinstr(name,"Non-Oil","Non_Oil",.)
replace name= strtrim(name)
replace name= stritrim(name)
split name, p("-")
gen yr1= substr(name1,-4,4)
gen yr2= substr(name2,-4,4) if !missing(name3)
drop name?
replace yr1= substr(name,-4,4) if missing(yr1)

** we now extract the name of the crop, 
** first, for the ones with prices_leading

gen index=0
replace index= strpos(name, "by Month")
replace index= strpos(name, "States") if strpos(name, "States")>0 & (strpos(name, "States")<index | index==0)
replace crop= substr(name,1,index-1) if index>0 & prices_leading==1
replace crop= subinstr(crop,"Prices Received for","",.)
replace crop= strtrim(crop)
replace crop= stritrim(crop)

split crop, p(" ")
replace crop= crop1 + " " + crop2
replace crop= strtrim(crop)
drop crop?

** for ones without prices_leading
replace index=0
replace index= strpos(name, "Prices Received")
replace crop= substr(name,1, index-1) if index>0 & prices_leading==0


** read files 
gen filepath= "C:\Users\clohani\Dropbox\California_Crop_Snapshots\Zip\" + subfolder + "/" + filename
destring yr1 yr2, replace
drop if missing(yr1)
save "$path_in/List_problematic_Ag_crops.dta", replace

local M= _N
local count_missing_1=0
forvalues j=1(1)`M' {

use "$path_in/List_problematic_Ag_crops.dta", clear
local file= filepath[`j']
local yr1= yr1[`j']
local yr2= yr2[`j']
local crop= crop[`j']

capture confirm new file `file'
if _rc==0 {
    dis "FLAG FLAG FLAG"
	continue
}

import delimited using "`file'", clear
drop if v2=="t"
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
gen f_mnth=0
gen f_yr=0

//finding variable which has price of the month we want; name of
local mnth_not_found=1
local state_not_found=1 
local month_vars
local n_month_vars=0
foreach var of local vars {
	forvalues i=1(1)`N' {
		local current= `var'[`i']
		local next= `var'[`i'+1]
		local cond_mnth strpos("`current'","Jan")>0 | strpos("`current'","Feb")>0 | (strpos("`current'","Mar")>0 & strpos("`current'","Mary")==0) | strpos("`current'","Apr")>0 | strpos("`current'","May")>0 | strpos("`current'","Jun")>0 | strpos("`current'","Jul")>0 | strpos("`current'","Aug")>0 | strpos("`current'","Sep")>0 | strpos("`current'","Oct")>0 | strpos("`current'","Nov")>0 | strpos("`current'","Dec")>0 
		local cond_yr strpos("`current'","2007")>0 | strpos("`current'","2008")>0 | strpos("`current'","2009")>0 | strpos("`current'","2010")>0 | strpos("`current'","2011")>0 | strpos("`current'","2012")>0 | strpos("`current'","2013")>0 | strpos("`current'","2014")>0 | strpos("`current'","2015")>0 | strpos("`current'","2016")>0 | strpos("`current'","2017")>0 | strpos("`current'","2018")>0

		if "`current'"== "United States" | "`current'"== "US" | "`current'"== "USA" {
			local state_var `var'
			local state_not_found=0
		}
		
		//if strpos("`current'","Jan")>0 | strpos("`current'","Feb")>0 | strpos("`current'","Mar")>0 | strpos("`current'","Apr")>0 | strpos("`current'","May")>0 | strpos("`current'","Jun")>0 | strpos("`current'","Jul")>0 | strpos("`current'","Aug")>0 | strpos("`current'","Sep")>0 | strpos("`current'","Oct")>0 | strpos("`current'","Nov")>0 | strpos("`current'","Dec")>0 {

		if `cond_mnth' {
			replace f_mnth=1 in `i'
			local mnth_not_found=0
			local month_vars `month_vars' `var'
		}
		
		replace f_yr=1 in `i' if `cond_yr'
	}
}

if `mnth_not_found'==1 | `state_not_found'==1 {
	dis "MISSING MISSING MISSING"
	local count_missing= `count_missing_1'+1
	continue
	
}


//find unique instances of the variables we have saved, otherwise we can double count
local uniq_month_vars 

foreach var of local month_vars {
	local found =0
	foreach u_var of local uniq_month_vars {
		if "`u_var'"=="`var'" {
			local found=1 
			break
		}
	}
	if `found'==0 {
		local uniq_month_vars `uniq_month_vars' `var'
	}
}

dis "`uniq_month_vars'"

local month_vars `uniq_month_vars'

local except `state_var'
local varlist `uniq_month_vars'
local month_vars: list varlist - except

local n_month_vars=0
foreach var of local month_vars {
	local n_month_vars= `n_month_vars' + 1
}

dis "`n_month_vars'"
dis "`month_vars'"

keep v2 `state_var' `month_vars' f_mnth f_yr
foreach var of local month_vars {
	local units= `var'[`row_units']
}
keep `state_var' `month_vars' f_mnth f_yr
rename (`state_var') (state)
drop if !(missing(state) | state=="State" | state=="state" | state=="United States" | state=="California" | state== "CA" | state== "US" | state== "USA")
/*
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
*/


keep if state== "California" | state== "CA" | state=="United States" | state== "US" | state== "USA" | f_mnth==1 | f_yr==1

gen crop= "`crop'"
gen units= "`units'"
gen month= ""
gen price=""
gen yr= .

forvalues i=1(1)`n_month_vars' {
	gen mnth_`i'=""
	gen yr_`i'=""
}

local N= _N
forvalues i=1(1)`N' {
	// get months as locals
	if f_mnth[`i']==1 {
		local k=1
		foreach var of local month_vars {
			replace mnth_`k'= `var'[`i'] if _n>`i'
			local k= `k'+1
		}
	}
	
	if f_yr[`i']==1 {
		local k=1
		foreach var of local month_vars {
			replace yr_`k'= `var'[`i'] if _n>`i'
			local k= `k'+1
		}
	}
}


//clean up years
forvalues k=1(1)`n_month_vars' {
	replace yr_`k'= substr(yr_`k',1,strpos(yr_`k',"-")-1) if strpos(yr_`k',"-")>0
}
destring yr_*, replace


gen order= _n
gen to_expand= 0
replace to_expand= `n_month_vars' if f_yr==0 & f_mnth==0
expand to_expand

drop if f_yr==1 | f_mnth==1
drop to_expand

sort state order

local k=1
foreach var of local month_vars {
	rename `var' price_`k'
	local k= `k' + 1
}

local N= _N
forvalues i=1(1)`N' {
	local k= mod(`i',`n_month_vars')
	if `k'==0 local k= `n_month_vars'
	replace month= mnth_`k'[`i'] in `i'
	replace yr= yr_`k'[`i'] in `i'
	replace price= price_`k'[`i'] in `i'
}

drop mnth_* price_*

gen mnth_code=1
replace mnth_code= 2 if substr(month,1,3)=="Feb"
replace mnth_code= 3 if substr(month,1,3)=="Mar"
replace mnth_code= 4 if substr(month,1,3)=="Apr"
replace mnth_code= 5 if substr(month,1,3)=="May"
replace mnth_code= 6 if substr(month,1,3)=="Jun"
replace mnth_code= 7 if substr(month,1,3)=="Jul"
replace mnth_code= 8 if substr(month,1,3)=="Aug"
replace mnth_code= 9 if substr(month,1,3)=="Sep"
replace mnth_code= 10 if substr(month,1,3)=="Oct"
replace mnth_code= 11 if substr(month,1,3)=="Nov"
replace mnth_code= 12 if substr(month,1,3)=="Dec"



local yr= `yr1'
local new_state=0
//take year from data if present
if !missing(yr[1]) {
	local yr1= yr[1]
	local yr=yr[1]
}
replace yr= `yr' if _n==1 & missing(yr[1])
forvalues k=2(1)`N'{
	if state[`k']!= state[`k'-1] {
		local new_state=1
		local yr= `yr1'
		//take year from data if present
		if !missing(yr[`k']) {
			local yr=yr[`k']
		}
		//replace yr= `yr' if _n==`k'
	}
	if state[`k']== state[`k'-1] {
		if mnth_code[`k']== mnth_code[`k'-1]+1 {
			//replace yr= `yr' if _n==`k'
		}
		if strpos(month[`k'],"1/")>0 {
			local yr= `yr' + 1
			//replace yr= `yr' if _n==`k'
		}
		//if the current entry isnt a successor of previous one, reset
		if strpos(month[`k'],"1/")==0 & !(mnth_code[`k']== mnth_code[`k'-1]+1) {
			local yr= `yr1'
			//replace yr= `yr' if _n==`k'
		}
	}
	replace yr= `yr' if _n==`k'
	local new_state=0
}
keep state crop units month price yr mnth_code
gen file= "`file'"
save "$path_out/Multimonth/Temp_`yr1'_`j'.dta", replace

}

dis "`count_missing_1'"


***





**** Part III: Post 2010 Single Month entries ****
restore

//read files

gen filepath= "C:\Users\clohani\Dropbox\California_Crop_Snapshots\Zip\" + subfolder + "/" + filename
tostring yr, replace
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
		if "`current'"== "United States" | "`current'"== "US" | "`current'"== "USA" {
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
keep if state== "California" | state== "CA" | state=="United States" | state== "US" | state== "USA"
gen crop= "`crop'"
gen units= "`units'"
gen month= "`mnth'"
gen yr= "`yr'"


save "$path_out/Temp_`yr'_`mnth'_`j'.dta", replace
}


dis "`count_missing_2'"

dis "`count_missing_1'"

dis "`count_missing'"
