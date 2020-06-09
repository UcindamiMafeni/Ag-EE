global path_in= "C:\Users\clohani\Dropbox\Edison_Rates\Processed_Excel"

import excel using "$path_in/TOU-PA2B.xlsx", clear

sxpose, clear

qui ds
local vars `r(varlist)'
dis `vars'

foreach var of local vars {
	local nname= `var'[1]
	rename `var' `nname'

}

drop if _n==1

split Date, p(.)

destring Date1 Date2 Date3, replace
gen date_2= mdy(Date2,Date1,Date3)
drop Date*
format date_2 %td


qui ds
local vars `r(varlist)'
destring `vars', ignore("( ) R I N / A ") replace 

local N= _N 

gen negs= 1
set tracedepth 1
set trace on
foreach var of local vars {
	replace negs = -1 if `var'<0
	replace `var'= `var'*negs
	replace negs= 1 
}
drop negs

gen bundled =1
replace bundled= 0 if Index==1

gen energycharge_dwr_sum_on_pk= 0
gen energycharge_dwr_sum_m_pk= 0
gen energycharge_dwr_sum_off_pk= 0
gen energycharge_dwr_win_on_pk= 0
gen energycharge_dwr_win_m_pk= 0
gen energycharge_dwr_win_off_pk= 0

replace energycharge_dwr_sum_on_pk= energycharge_sum_on_pk if Index==3
replace energycharge_dwr_sum_m_pk= energycharge_sum_m_pk if Index==3
replace energycharge_dwr_sum_off_pk= energycharge_sum_off_pk if Index==3

replace energycharge_dwr_win_on_pk= energycharge_win_on_pk if Index==3
replace energycharge_dwr_win_m_pk= energycharge_win_m_pk if Index==3
replace energycharge_dwr_win_off_pk= energycharge_win_off_pk if Index==3

replace energycharge_sum_on_pk= 0 if Index==3
replace energycharge_sum_m_pk= 0 if Index==3
replace energycharge_sum_off_pk= 0 if Index==3

replace energycharge_win_on_pk= 0 if Index==3
replace energycharge_win_m_pk= 0 if Index==3
replace energycharge_win_off_pk= 0 if Index==3


local all `vars'
local except "Index bundled date_2"
local to_collapse: list all - except
collapse (max) `to_collapse', by(bundled date_2)
