clear

*******************************************************************
** Code to build SCE agricultural tariffs up from raw data files **
*******************************************************************

global path_in = "T:/Projects/Pump Data/data/sce_raw/Rates/Processed_Excel"
global path_temp = "T:/Projects/Pump Data/data/temp"
global path_out = "T:/Projects/Pump Data/data/sce_cleaned"

**********************************************************************
**********************************************************************

** 1. Import rate histories in raw excel files (which have been extracted from PDFs)
{
	// This chunk of code reads in processed XLSX files and creates 
cd "$path_in"	
local files : dir . files "*"
foreach f in `files' {

	import excel using "$path_in/`f'", clear
	sxpose, clear

	qui ds
	local vars `r(varlist)'
	dis `vars'

	foreach var of local vars {
		local nname= `var'[1]
		rename `var' `nname'

	}

	drop if _n==1
	
	cap rename date Date
	cap rename index Index
	
	split Date, p(.)
	destring Date1 Date2 Date3, replace
	gen date_2= mdy(Date2,Date1,Date3)
	drop Date*
	format date_2 %td

	qui ds
	local vars `r(varlist)'
	foreach v in `vars' {
		noi noi cap {
			replace `v' = subinstr(`v',"(R)","",.)
			replace `v' = subinstr(`v',"(R","",.)
			replace `v' = subinstr(`v',"(I)","",.)
			replace `v' = subinstr(`v',"(N","",.)
			replace `v' = subinstr(`v',"N/A","",.)
			replace `v' = subinstr(`v',")","",.)
			replace `v' = subinstr(`v',"*","",.)
			replace `v' = trim(itrim(`v'))
			replace `v' = word(`v'[_n-1],2) if date_2==date_2[_n-1] & `v'==""
			replace `v' = word(`v',1) if wordcount(`v')>1
			replace `v' = subinstr(`v',"(","-",1) if substr(`v',1,1)=="("
		}		
	}
	destring `vars', replace
	
	

	local N= _N 

	gen negs= 1
	set tracedepth 1
	set trace on
	foreach var of local vars {
		cap {
			replace negs = -1 if `var'<0
			replace `var'= `var'*negs
			replace negs= 1 
		}
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

	cap replace energycharge_dwr_sum_on_pk= energycharge_sum_on_pk if Index==3
	cap replace energycharge_dwr_sum_m_pk= energycharge_sum_m_pk if Index==3
	cap replace energycharge_dwr_sum_off_pk= energycharge_sum_off_pk if Index==3

	cap replace energycharge_dwr_win_on_pk= energycharge_win_on_pk if Index==3
	cap replace energycharge_dwr_win_m_pk= energycharge_win_m_pk if Index==3
	cap replace energycharge_dwr_win_off_pk= energycharge_win_off_pk if Index==3

	cap replace energycharge_sum_on_pk= 0 if Index==3
	cap replace energycharge_sum_m_pk= 0 if Index==3
	cap replace energycharge_sum_off_pk= 0 if Index==3

	cap replace energycharge_win_on_pk= 0 if Index==3
	cap replace energycharge_win_m_pk= 0 if Index==3
	cap replace energycharge_win_off_pk= 0 if Index==3


	local all `vars'
	local except "Index bundled date_2"
	local to_collapse: list all - except
	collapse (max) `to_collapse', by(bundled date_2)
	
	gen ratename = subinstr("`f'",".xlsx","",1)
	order ratename
	
	compress
	local f2 = subinstr("`f'",".xlsx",".dta",1)
	save "$path_temp/sce_rate_`f2'", replace
	
}	

}

**********************************************************************
**********************************************************************

** 2. Hand code up rate histories for the rates that were impossible to extract from PDFs
{

set trace on
//set paths here

//generate the variables to keep track

//denote number of categories in each schedule type
clear

local cats= 3*2
local N=0

local N= `N' + `cats'
set obs `N'

gen rateschedule= ""
gen rate_start_date= .
gen voltage_cat= mod(_n,3) + 1
gen bundled= mod(_n,2) 
gen energycharge= .
gen energycharge_dwr=.
gen customercharge= .
gen servicecharge= .
gen off_peak_credit= .
gen tou_option_meter_charge= .
gen voltage_dis_energy = .
gen voltage_dis_load= .
format rate_start_date %td

label variable voltage_cat "1: 2-50 kV, 2: 50-220 kV, 3- 220 kV"
label variable bundled "1 if bundled, 0 if not"
label variable energycharge "variable charge $/kWh/Meter/month"
label variable energycharge_dwr "variable charge applicable selectively"
label variable customercharge "fixed $/meter/month"
label variable servicecharge "fixed $/hp/month, atleast 2 hp for single phase, 3 for 3 phase"
label variable off_peak_credit "fixed $/hp/month"
label variable tou_option_meter_charge "fixed $/month"
label variable voltage_dis_energy "discount $/kWh"
label variable voltage_dis_load "discount $/hp"

//while updating, we will keep track of new observations in each step by defining new
gen old=.

*************************
*** 1. Schedule- PA-1 ***
*************************

replace rateschedule = "PA-1" if missing(old)

** 2009 Jan

// define conditions
local cond1 " missing(old) & bundled==1"
local cond2 " missing(old) & bundled==0"

//start date is common to everyone
replace rate_start_date = mdy(1,1,2009) if missing(old)

// bundled
replace energycharge = 0.11884 if `cond1'
replace energycharge_dwr = 0.08451 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 0 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00199 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00433 if `cond1' & voltage_cat>1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat>1

// non-bundled
replace energycharge = 0.02537 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 30.02 if `cond2'
replace servicecharge = 2.23 if `cond2'
replace off_peak_credit = 2.01 if `cond2'
replace tou_option_meter_charge = 18.53 if `cond2'
replace voltage_dis_energy= 0.00199 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00433 if `cond2' & voltage_cat>1
replace voltage_dis_load= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond2' & voltage_cat>1

//now these are no longer old
replace old= 0 if missing(old)

local N= `N' + `cats'
set obs `N'


** 2009 March

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(3,1,2009) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.12783 if `cond1'
replace energycharge_dwr = 0.06231 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 0 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00199 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00433 if `cond1' & voltage_cat>1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat>1

// non-bundled
replace energycharge = 0.01951 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 29.29 if `cond2'
replace servicecharge = 2.36 if `cond2'
replace off_peak_credit = 1.96 if `cond2'
replace tou_option_meter_charge = 18.08 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat>1
replace voltage_dis_load= 0.14 if `cond2' & voltage_cat==1
replace voltage_dis_load= 4.66 if `cond2' & voltage_cat>1

replace old= 0 if missing(old)


** 2009 April 4

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(4,4,2009) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.12980 if `cond1'
replace energycharge_dwr = 0.06210 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 0 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00201 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00438 if `cond1' & voltage_cat>1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat>1

// non-bundled
replace energycharge = 0.02051 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 34.0 if `cond2'
replace servicecharge = 2.67 if `cond2'
replace off_peak_credit = 2.27 if `cond2'
replace tou_option_meter_charge = 20.98 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat>1
replace voltage_dis_load= 0.14 if `cond2' & voltage_cat==1
replace voltage_dis_load= 4.66 if `cond2' & voltage_cat>1

replace old= 0 if missing(old)


** 2009 June 1

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(6,1,2009) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.12769 if `cond1'
replace energycharge_dwr = 0.06225 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 0 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00198 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00432 if `cond1' & voltage_cat>1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat>1

// non-bundled
replace energycharge = 0.02109 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 34.0 if `cond2'
replace servicecharge = 2.67 if `cond2'
replace off_peak_credit = 2.27 if `cond2'
replace tou_option_meter_charge = 20.98 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat>1
replace voltage_dis_load= 0.14 if `cond2' & voltage_cat==1
replace voltage_dis_load= 4.66 if `cond2' & voltage_cat>1

replace old= 0 if missing(old)


** 2009 Oct 1

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(10,1,2009) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.11735 if `cond1'
replace energycharge_dwr = 0.06225 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 1.72 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00210 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00468 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00473 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3

// non-bundled
replace energycharge = 0.03120 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 40.80 if `cond2'
replace servicecharge = 1.92 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.95 if `cond2' & voltage_cat==2
replace voltage_dis_load= 0.96 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Jan 1 2010

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(1,1,2010) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.11735 if `cond1'
replace energycharge_dwr = 0.03763 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 1.72 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00210 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00468 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00473 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3

// non-bundled
replace energycharge = 0.03210 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 40.80 if `cond2'
replace servicecharge = 1.92 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.95 if `cond2' & voltage_cat==2
replace voltage_dis_load= 0.96 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Mar 1 2010

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(3,1,2010) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.11298 if `cond1'
replace energycharge_dwr = 0.03763 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 2.0 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00210 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00468 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00473 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3

// non-bundled
replace energycharge = 0.04273 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 44.09 if `cond2'
replace servicecharge = 2.2 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.95 if `cond2' & voltage_cat==2
replace voltage_dis_load= 0.96 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// June 1 2010

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(6,1,2010) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.11298 if `cond1'
replace energycharge_dwr = 0.03763 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 2.0 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00210 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00468 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00473 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3

// non-bundled
replace energycharge = 0.04219 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 44.09 if `cond2'
replace servicecharge = 2.2 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.95 if `cond2' & voltage_cat==2
replace voltage_dis_load= 0.96 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Jan 1 2011

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(1,1,2011) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.11298 if `cond1'
replace energycharge_dwr = 0.03952 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 2.0 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00210 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00468 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00473 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3

// non-bundled
replace energycharge = 0.04248 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 44.09 if `cond2'
replace servicecharge = 2.2 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.95 if `cond2' & voltage_cat==2
replace voltage_dis_load= 0.96 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Mar 1 2011

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(3,1,2011) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.11298 if `cond1'
replace energycharge_dwr = 0.03952 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 2.03 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00210 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00468 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00473 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3

// non-bundled
replace energycharge = 0.04248 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 44.09 if `cond2'
replace servicecharge = 2.23 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.95 if `cond2' & voltage_cat==2
replace voltage_dis_load= 0.96 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Jun 1 2011

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(1,1,2011) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.09689 if `cond1'
replace energycharge_dwr = 0.03952 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 2.02 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00210 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00468 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00473 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3

// non-bundled
replace energycharge = 0.04407 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 44.41 if `cond2'
replace servicecharge = 2.22 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.95 if `cond2' & voltage_cat==2
replace voltage_dis_load= 0.96 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Jan 1 2012

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(1,1,2012) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.09689 if `cond1'
replace energycharge_dwr = 0.00593 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 2.09 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00210 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00468 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00473 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3

// non-bundled
replace energycharge = 0.04394 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 44.41 if `cond2'
replace servicecharge = 2.29 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.95 if `cond2' & voltage_cat==2
replace voltage_dis_load= 0.96 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Jun 1 2012

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(6,1,2012) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.09689 if `cond1'
replace energycharge_dwr = 0.00463 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 2.09 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00210 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00468 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00473 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3

// non-bundled
replace energycharge = 0.04398 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 44.41 if `cond2'
replace servicecharge = 2.29 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.95 if `cond2' & voltage_cat==2
replace voltage_dis_load= 0.96 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Aug 1 2012

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(8,1,2012) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.09424 if `cond1'
replace energycharge_dwr = 0.00463 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 2.18 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00210 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00468 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00473 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3

// non-bundled
replace energycharge = 0.04431 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 45.35 if `cond2'
replace servicecharge = 2.38 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.95 if `cond2' & voltage_cat==2
replace voltage_dis_load= 0.96 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Oct 1 2012

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(10,1,2012) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.09424 if `cond1'
replace energycharge_dwr = 0.00463 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 2.14 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00210 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00468 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00473 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3

// non-bundled
replace energycharge = 0.04436 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 45.35 if `cond2'
replace servicecharge = 2.34 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.95 if `cond2' & voltage_cat==2
replace voltage_dis_load= 0.96 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

sort rateschedule bundled voltage_cat rate_start_date

// Apr 1 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(4,1,2013) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.09656 if `cond1'
replace energycharge_dwr = 0.00097 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 2.58 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00210 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00468 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00473 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3

// non-bundled
replace energycharge = 0.03987 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 47.91 if `cond2'
replace servicecharge = 2.78 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.95 if `cond2' & voltage_cat==2
replace voltage_dis_load= 0.96 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Jun 1 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(6,1,2013) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.09628 if `cond1'
replace energycharge_dwr = 0.00097 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 2.58 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00210 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00468 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00473 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3

// non-bundled
replace energycharge = 0.04058 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 47.85 if `cond2'
replace servicecharge = 2.78 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.95 if `cond2' & voltage_cat==2
replace voltage_dis_load= 0.96 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Oct 1 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(10,1,2013) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.09628 if `cond1'
replace energycharge_dwr = 0.00097 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 2.58 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00210 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00468 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00473 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3

// non-bundled
replace energycharge = 0.04050 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 47.85 if `cond2'
replace servicecharge = 2.68 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.95 if `cond2' & voltage_cat==2
replace voltage_dis_load= 0.96 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Nov 22 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(11,22,2013) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.09811 if `cond1'
replace energycharge_dwr = 0.00095 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 2.55 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00210 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00472 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00477 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3

// non-bundled
replace energycharge = 0.04212 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 49.52 if `cond2'
replace servicecharge = 2.75 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.97 if `cond2' & voltage_cat==2
replace voltage_dis_load= 0.98 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Jan 1 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(1,1,2013) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.09811 if `cond1'
replace energycharge_dwr = 0.00095 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 2.55 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00210 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00472 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00477 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3

// non-bundled
replace energycharge = 0.04212 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 49.52 if `cond2'
replace servicecharge = 2.75 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.97 if `cond2' & voltage_cat==2
replace voltage_dis_load= 0.98 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Apr 1 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(4,1,2013) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.10313 if `cond1'
replace energycharge_dwr = 0.00097 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 2.26 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00210 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00468 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00473 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3

// non-bundled
replace energycharge = 0.04397 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 48.20 if `cond2'
replace servicecharge = 2.46 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.95 if `cond2' & voltage_cat==2
replace voltage_dis_load= 0.96 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Apr 12 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(4,12,2013) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.10313 if `cond1'
replace energycharge_dwr = 0.00097 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 2.26 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00210 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00468 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00473 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3

// non-bundled
replace energycharge = 0.04397 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 48.20 if `cond2'
replace servicecharge = 2.46 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.95 if `cond2' & voltage_cat==2
replace voltage_dis_load= 0.96 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Jan 1 2014

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(1,1,2014) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.09607 if `cond1'
replace energycharge_dwr = 0.00037 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 2.81 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00202 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00456 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00461 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3

// non-bundled
replace energycharge = 0.04267 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 50.58 if `cond2'
replace servicecharge = 3.01 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.98 if `cond2' & voltage_cat==2
replace voltage_dis_load= 0.99 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Apr 1 2014

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(4,1,2014) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.10559 if `cond1'
replace energycharge_dwr = 0.00037 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 2.81 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00222 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00501 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00506 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3

// non-bundled
replace energycharge = 0.04267 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 50.58 if `cond2'
replace servicecharge = 3.01 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.98 if `cond2' & voltage_cat==2
replace voltage_dis_load= 0.99 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Jun 1 2014

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(6,1,2014) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.12043 if `cond1'
replace energycharge_dwr = 0.00037 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 2.75 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00239 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00536 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00542 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3

// non-bundled
replace energycharge = 0.04320 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 49.13 if `cond2'
replace servicecharge = 2.95 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.95 if `cond2' & voltage_cat==2
replace voltage_dis_load= 0.96 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Jul 7 2014

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(7,7,2014) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.12043 if `cond1'
replace energycharge_dwr = 0.00037 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 2.75 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00239 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00536 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00542 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3

// non-bundled
replace energycharge = 0.04384 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 49.13 if `cond2'
replace servicecharge = 2.95 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.95 if `cond2' & voltage_cat==2
replace voltage_dis_load= 0.96 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Aug 11 2014

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(8,11,2014) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.12043 if `cond1'
replace energycharge_dwr = 0.00037 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 2.75 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00237 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00532 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00537 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3

// non-bundled
replace energycharge = 0.04383 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 49.13 if `cond2'
replace servicecharge = 2.95 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.95 if `cond2' & voltage_cat==2
replace voltage_dis_load= 0.96 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Aug 11 2014

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(8,11,2014) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.12043 if `cond1'
replace energycharge_dwr = 0.00037 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 2.75 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00237 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00532 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00537 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3

// non-bundled
replace energycharge = 0.04383 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 49.13 if `cond2'
replace servicecharge = 2.95 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.95 if `cond2' & voltage_cat==2
replace voltage_dis_load= 0.96 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Jan 1 2015

** new variable: california climate credit
gen cal_climate_credit= 0
label variable cal_climate_credit "$/kWh"

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(1,1,2015) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.12178 if `cond1'
replace energycharge_dwr = 0.00172 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 3.10 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00220 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00495 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00537 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3
replace cal_climate_credit= 0 if `cond1'

// non-bundled
replace energycharge = 0.04490 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 50.26 if `cond2'
replace servicecharge = 3.3 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.97 if `cond2' & voltage_cat==2
replace voltage_dis_load= 0.98 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00731 if `cond1'

replace old= 0 if missing(old)


// Mar 2 2015

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(3,2,2015) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.12178 if `cond1'
replace energycharge_dwr = 0.00172 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 3.10 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00216 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00486 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00491 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3
replace cal_climate_credit= 0 if `cond1'

// non-bundled
replace energycharge = 0.04489 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 50.26 if `cond2'
replace servicecharge = 3.3 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.97 if `cond2' & voltage_cat==2
replace voltage_dis_load= 0.98 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00680 if `cond1'

replace old= 0 if missing(old)



// Jun 1 2015


local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(6,1,2015) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.12178 if `cond1'
replace energycharge_dwr = 0.00172 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 3.10 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00216 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00486 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00491 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3
replace cal_climate_credit= 0 if `cond1'

// non-bundled
replace energycharge = 0.04534 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 50.26 if `cond2'
replace servicecharge = 3.3 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.97 if `cond2' & voltage_cat==2
replace voltage_dis_load= 0.98 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00680 if `cond1'

replace old= 0 if missing(old)



// Oct 1 2015

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(10,1,2015) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.12178 if `cond1'
replace energycharge_dwr = 0.00172 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 3.10 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00216 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00486 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00491 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3
replace cal_climate_credit= 0 if `cond1'

// non-bundled
replace energycharge = 0.04537 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 50.26 if `cond2'
replace servicecharge = 3.3 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.97 if `cond2' & voltage_cat==2
replace voltage_dis_load= 0.98 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00680 if `cond1'

replace old= 0 if missing(old)



// Nov 24 2015


local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(11,24,2015) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.11809 if `cond1'
replace energycharge_dwr = 0.00172 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 3.10 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00211 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00476 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00481 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3
replace cal_climate_credit= 0 if `cond1'

// non-bundled
replace energycharge = 0.04527 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 50.26 if `cond2'
replace servicecharge = 3.3 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.97 if `cond2' & voltage_cat==2
replace voltage_dis_load= 0.98 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00680 if `cond1'

replace old= 0 if missing(old)


// Jun 1 2016


local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(6,1,2016) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.08331 if `cond1'
replace energycharge_dwr = 0.00022 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 3.89 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00159 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00360 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00364 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3
replace cal_climate_credit= 0 if `cond1'

// non-bundled
replace energycharge = 0.04527 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 50.26 if `cond2'
replace servicecharge = 3.3 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 0.97 if `cond2' & voltage_cat==2
replace voltage_dis_load= 0.98 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00680 if `cond1'

replace old= 0 if missing(old)

// Oct 1 2016

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(8,1,2016) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.08331 if `cond1'
replace energycharge_dwr = 0.00022 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 3.89 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00159 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00360 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00364 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3
replace cal_climate_credit= 0 if `cond1'

// non-bundled
replace energycharge = 0.04632 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 59.96 if `cond2'
replace servicecharge = 4.09 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 1.08 if `cond2' & voltage_cat==2
replace voltage_dis_load= 1.09 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00378 if `cond1'

replace old= 0 if missing(old)


// Jan 1 2016

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(1,1,2016) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.08703 if `cond1'
replace energycharge_dwr = 0.00022 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 3.73 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00159 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00360 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00364 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3
replace cal_climate_credit= 0 if `cond1'

// non-bundled
replace energycharge = 0.04644 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 56.11 if `cond2'
replace servicecharge = 3.93 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 1.08 if `cond2' & voltage_cat==2
replace voltage_dis_load= 1.09 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00378 if `cond1'

replace old= 0 if missing(old)


// Jan 1 2016

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(1,1,2017) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.08309 if `cond1'
replace energycharge_dwr = 0.0000 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 4.09 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00180 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00408 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00412 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3
replace cal_climate_credit= 0 if `cond1'

// non-bundled
replace energycharge = 0.04845 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 59.08 if `cond2'
replace servicecharge = 4.29 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 1.05 if `cond2' & voltage_cat==2
replace voltage_dis_load= 1.06 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00414 if `cond1'

replace old= 0 if missing(old)


// Jan 1 2017

** new variable: wind machine credit
gen wind_mach_credit= 0
label variable wind_mach_credit "$/hp"

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(1,1,2017) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.08309 if `cond1'
replace energycharge_dwr = 0.0000 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 0.0 if `cond1'
replace wind_mach_credit = 4.09 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00180 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00408 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00412 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3
replace cal_climate_credit= 0 if `cond1'

// non-bundled
replace energycharge = 0.04845 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 59.08 if `cond2'
replace servicecharge = 4.29 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace wind_mach_credit = 0.0 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 1.05 if `cond2' & voltage_cat==2
replace voltage_dis_load= 1.06 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00414 if `cond1'

replace old= 0 if missing(old)


// Jun 1 2017

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(1,1,2017) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.08309 if `cond1'
replace energycharge_dwr = 0.0000 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 0.0 if `cond1'
replace wind_mach_credit = 4.09 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00180 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00408 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00412 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3
replace cal_climate_credit= 0 if `cond1'

// non-bundled
replace energycharge = 0.04755 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 59.08 if `cond2'
replace servicecharge = 4.29 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace wind_mach_credit = 0.00 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 1.05 if `cond2' & voltage_cat==2
replace voltage_dis_load= 1.06 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00414 if `cond1'

replace old= 0 if missing(old)


// Sep 23 2017

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(9,23,2017) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.08309 if `cond1'
replace energycharge_dwr = 0.0000 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 0 if `cond1'
replace wind_mach_credit = 0.00 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00180 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00408 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00412 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3
replace cal_climate_credit= 0 if `cond1'

// non-bundled
replace energycharge = 0.04755 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 59.08 if `cond2'
replace servicecharge = 4.29 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace wind_mach_credit = 0.00 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 1.05 if `cond2' & voltage_cat==2
replace voltage_dis_load= 1.06 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00414 if `cond1'

replace old= 0 if missing(old)


// Oct 1 2017

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(10,1,2017) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.08309 if `cond1'
replace energycharge_dwr = 0.0000 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 0 if `cond1'
replace wind_mach_credit = 0.00 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00180 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00408 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00412 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3
replace cal_climate_credit= 0 if `cond1'

// non-bundled
replace energycharge = 0.04697 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 59.08 if `cond2'
replace servicecharge = 4.29 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace wind_mach_credit = 0.00 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 1.05 if `cond2' & voltage_cat==2
replace voltage_dis_load= 1.06 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00414 if `cond1'

replace old= 0 if missing(old)


// Jan 1 2018

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(1,1,2018) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.09544 if `cond1'
replace energycharge_dwr = 0.0000 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 0 if `cond1'
replace wind_mach_credit = 0.00 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00207 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00469 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00474 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3
replace cal_climate_credit= 0 if `cond1'

// non-bundled
replace energycharge = 0.04455 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 61.23 if `cond2'
replace servicecharge = 4.30 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace wind_mach_credit = 0.00 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 1.08 if `cond2' & voltage_cat==2
replace voltage_dis_load= 1.09 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00400 if `cond1'

replace old= 0 if missing(old)


// Jun 1 2018

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(6,1,2018) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.09544 if `cond1'
replace energycharge_dwr = 0.0000 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 0 if `cond1'
replace wind_mach_credit = 0.00 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00207 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00469 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00474 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3
replace cal_climate_credit= 0 if `cond1'

// non-bundled
replace energycharge = 0.04524 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 61.23 if `cond2'
replace servicecharge = 4.30 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace wind_mach_credit = 0.00 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 1.08 if `cond2' & voltage_cat==2
replace voltage_dis_load= 1.09 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00400 if `cond1'

replace old= 0 if missing(old)


// Aug 27 2018

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(8,27,2018) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.07222 if `cond1'
replace energycharge_dwr = 0.0000 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 0 if `cond1'
replace wind_mach_credit = 0.00 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00157 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00355 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00359 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3
replace cal_climate_credit= 0 if `cond1'

// non-bundled
replace energycharge = 0.04498 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 61.23 if `cond2'
replace servicecharge = 4.30 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace wind_mach_credit = 0.00 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 1.08 if `cond2' & voltage_cat==2
replace voltage_dis_load= 1.09 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00400 if `cond1'

replace old= 0 if missing(old)


// Sep 15 2018

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(9,15,2018) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.09544 if `cond1'
replace energycharge_dwr = 0.0000 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 0 if `cond1'
replace wind_mach_credit = 0.00 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3
replace cal_climate_credit= 0 if `cond1'

// non-bundled
replace energycharge = 0.04524 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 61.23 if `cond2'
replace servicecharge = 4.30 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace wind_mach_credit = 0.00 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 1.08 if `cond2' & voltage_cat==2
replace voltage_dis_load= 1.09 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00400 if `cond1'

replace old= 0 if missing(old)


// Oct 1 2018

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(10,1,2018) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.09411 if `cond1'
replace energycharge_dwr = 0.0000 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 0 if `cond1'
replace wind_mach_credit = 0.00 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00205 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00463 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00468 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3
replace cal_climate_credit= 0 if `cond1'

// non-bundled
replace energycharge = 0.04521 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 61.23 if `cond2'
replace servicecharge = 4.30 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace wind_mach_credit = 0.00 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 1.08 if `cond2' & voltage_cat==2
replace voltage_dis_load= 1.09 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00400 if `cond1'

replace old= 0 if missing(old)


// Jan 1 2019

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(1,1,2019) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.09411 if `cond1'
replace energycharge_dwr = 0.00007 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 0 if `cond1'
replace wind_mach_credit = 0.00 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00205 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00463 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00468 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3
replace cal_climate_credit= 0 if `cond1'

// non-bundled
replace energycharge = 0.04530 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 61.23 if `cond2'
replace servicecharge = 4.30 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace wind_mach_credit = 0.00 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 1.08 if `cond2' & voltage_cat==2
replace voltage_dis_load= 1.09 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00400 if `cond1'

replace old= 0 if missing(old)


// Mar 1 2019

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(3,1,2019) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.06865 if `cond1'
replace energycharge_dwr = 0.00007 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 0 if `cond1'
replace wind_mach_credit = 0.00 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00113 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00463 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00468 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3
replace cal_climate_credit= 0 if `cond1'

// non-bundled
replace energycharge = 0.04530 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 61.23 if `cond2'
replace servicecharge = 4.30 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace wind_mach_credit = 0.00 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 1.08 if `cond2' & voltage_cat==2
replace voltage_dis_load= 1.09 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00400 if `cond1'

replace old= 0 if missing(old)


// Apr 12 2019

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-1" if missing(old)
replace rate_start_date = mdy(4,12,2019) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge = 0.07286 if `cond1'
replace energycharge_dwr = 0.00007 if `cond1'
replace customercharge = 0 if `cond1'
replace servicecharge = 0 if `cond1'
replace off_peak_credit = 0 if `cond1'
replace wind_mach_credit = 3.71 if `cond1'
replace tou_option_meter_charge = 0 if `cond1'
replace voltage_dis_energy= 0.00120 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00264 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00267 if `cond1' & voltage_cat==3
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==1
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==2
replace voltage_dis_load= 0.00 if `cond1' & voltage_cat==3
replace cal_climate_credit= 0 if `cond1'

// non-bundled
replace energycharge = 0.04210 if `cond2'
replace energycharge_dwr = 0 if `cond2'
replace customercharge = 40.46 if `cond2'
replace servicecharge = 3.91 if `cond2'
replace off_peak_credit = 0.0 if `cond2'
replace wind_mach_credit = 0.00 if `cond2'
replace tou_option_meter_charge = 0 if `cond2'
replace voltage_dis_energy= 0.00051 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.01932 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.02445 if `cond2' & voltage_cat==3
replace voltage_dis_load= 0.03 if `cond2' & voltage_cat==1
replace voltage_dis_load= 1.03 if `cond2' & voltage_cat==2
replace voltage_dis_load= 1.04 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00400 if `cond1'

replace old= 0 if missing(old)

sort rateschedule bundled voltage_cat rate_start_date




*************************
*** 2. Schedule- PA-2 ***
*************************

// We have summer and winter charges now, so we'll define new variables to keep energycharge and demandcharge seperately
// in later parts of the code, when timeseries is being made by day, replace the energycharge with one of these accordingly
// for the demand charge, there is a common one plus two separate winter/summer charges  so add them accordingly

gen energycharge_sum= .
gen energycharge_win= .
gen energycharge_dwr_sum=.
gen energycharge_dwr_win=.
gen tou_RTEM_meter_charge=.
gen demandcharge= .
gen demandcharge_sum= .
gen demandcharge_win= .
gen voltage_dis_load_1= .
gen voltage_dis_load_2= .

label variable energycharge_sum "Summer variable charge $/kWh/Meter/month"
label variable energycharge_win "Winter variable charge $/kWh/Meter/month"
label variable energycharge_dwr_sum "variable charge applicable selectively"
label variable energycharge_dwr_win "variable charge applicable selectively"
label variable demandcharge "$/kW of Billing Demand/Meter/Month"
label variable demandcharge_sum "For summer, $/kW of Billing Demand/Meter/Month"
label variable demandcharge_win "For winter, $/kW of Billing Demand/Meter/Month"
label variable tou_RTEM_meter_charge "fixed $/month"
label variable voltage_dis_load_1 "Facilities related: discount $/hp"
label variable voltage_dis_load_2 "Time-related: discount $/hp"

// Jan 1 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(1,1,2009) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.06864 if `cond1'
replace energycharge_win= 0.06614 if `cond1'
replace energycharge_dwr_sum= 0.08451 if `cond1'
replace energycharge_dwr_win= 0.08451 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 0 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00199 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00433 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00433 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.01538 if `cond2'
replace energycharge_win= 0.01538 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 58.91 if `cond2'
replace demandcharge= 8.82 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond2'
replace tou_option_meter_charge= 18.43 if `cond2'
replace tou_RTEM_meter_charge= 158.83 if `cond2'
replace voltage_dis_load_1= 0.19 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 6.25 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.25 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Mar 1 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(3,1,2009) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.07725 if `cond1'
replace energycharge_win= 0.07434 if `cond1'
replace energycharge_dwr_sum= 0.06231 if `cond1'
replace energycharge_dwr_win= 0.06231 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 0 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00199 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00433 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00433 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.01130 if `cond2'
replace energycharge_win= 0.01130 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 57.47 if `cond2'
replace demandcharge= 8.97 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond2'
replace tou_option_meter_charge= 17.98 if `cond2'
replace tou_RTEM_meter_charge= 154.95 if `cond2'
replace voltage_dis_load_1= 0.19 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 6.25 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.25 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Apr 4 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(4,4,2009) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.07859 if `cond1'
replace energycharge_win= 0.07605 if `cond1'
replace energycharge_dwr_sum= 0.06210 if `cond1'
replace energycharge_dwr_win= 0.06210 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 0 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00201 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00438 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00438 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.01112 if `cond2'
replace energycharge_win= 0.01112 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 66.70 if `cond2'
replace demandcharge= 10.24 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond2'
replace tou_option_meter_charge= 20.87 if `cond2'
replace tou_RTEM_meter_charge= 179.85 if `cond2'
replace voltage_dis_load_1= 0.19 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 6.25 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.25 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Jun 1 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(6,1,2009) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.07716 if `cond1'
replace energycharge_win= 0.07465 if `cond1'
replace energycharge_dwr_sum= 0.06225 if `cond1'
replace energycharge_dwr_win= 0.06225 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 0 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00198 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00432 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00432 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.01159 if `cond2'
replace energycharge_win= 0.01159 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 66.70 if `cond2'
replace demandcharge= 10.24 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond2'
replace tou_option_meter_charge= 20.87 if `cond2'
replace tou_RTEM_meter_charge= 179.85 if `cond2'
replace voltage_dis_load_1= 0.19 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 6.25 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.25 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Oct 1 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(10,1,2009) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.11973 if `cond1'
replace energycharge_win= 0.06201 if `cond1'
replace energycharge_dwr_sum= 0.06225 if `cond1'
replace energycharge_dwr_win= 0.06225 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 3.5 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.35 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.96 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.97 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00140 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00312 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00316 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.01280 if `cond2'
replace energycharge_win= 0.01280 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 80.04 if `cond2'
replace demandcharge= 7.00 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond2'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 86.51 if `cond2'
replace voltage_dis_load_1= 0.09 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 2.98 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.18 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Oct 1 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(10,1,2009) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.11973 if `cond1'
replace energycharge_win= 0.06201 if `cond1'
replace energycharge_dwr_sum= 0.06225 if `cond1'
replace energycharge_dwr_win= 0.06225 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 3.5 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.35 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.96 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.97 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00140 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00312 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00316 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.01280 if `cond2'
replace energycharge_win= 0.01280 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 80.04 if `cond2'
replace demandcharge= 7.00 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond2'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 86.51 if `cond2'
replace voltage_dis_load_1= 0.09 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 2.98 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.18 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Jan 1 2010

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(1,1,2010) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.11973 if `cond1'
replace energycharge_win= 0.06201 if `cond1'
replace energycharge_dwr_sum= 0.03763 if `cond1'
replace energycharge_dwr_win= 0.03763 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 3.5 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.35 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.96 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.97 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00140 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00312 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00316 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.01368 if `cond2'
replace energycharge_win= 0.01368 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 80.04 if `cond2'
replace demandcharge= 7.02 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond2'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 86.51 if `cond2'
replace voltage_dis_load_1= 0.09 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 2.98 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.18 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Mar 1 2010

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(3,1,2010) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.11528 if `cond1'
replace energycharge_win= 0.05944 if `cond1'
replace energycharge_dwr_sum= 0.03763 if `cond1'
replace energycharge_dwr_win= 0.03763 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 3.39 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.35 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.96 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.97 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00140 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00312 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00316 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.02046 if `cond2'
replace energycharge_win= 0.02046 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 86.50 if `cond2'
replace demandcharge= 8.04 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond2'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 93.49 if `cond2'
replace voltage_dis_load_1= 0.09 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 2.98 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.18 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3


replace old= 0 if missing(old)

// Jun 1 2010

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(6,1,2010) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.11528 if `cond1'
replace energycharge_win= 0.05944 if `cond1'
replace energycharge_dwr_sum= 0.03763 if `cond1'
replace energycharge_dwr_win= 0.03763 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 3.39 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.35 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.96 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.97 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00140 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00312 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00316 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.01992 if `cond2'
replace energycharge_win= 0.01992 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 86.50 if `cond2'
replace demandcharge= 8.06 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond2'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 93.49 if `cond2'
replace voltage_dis_load_1= 0.09 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 2.98 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.18 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Jan 1 2010

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(1,1,2011) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.11528 if `cond1'
replace energycharge_win= 0.05944 if `cond1'
replace energycharge_dwr_sum= 0.03952 if `cond1'
replace energycharge_dwr_win= 0.03952 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 3.39 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.35 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.96 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.97 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00140 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00312 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00316 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.02020 if `cond2'
replace energycharge_win= 0.02020 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 86.50 if `cond2'
replace demandcharge= 8.06 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond2'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 93.49 if `cond2'
replace voltage_dis_load_1= 0.09 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 2.98 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.18 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Mar 1 2010

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(3,1,2011) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.11528 if `cond1'
replace energycharge_win= 0.05944 if `cond1'
replace energycharge_dwr_sum= 0.03952 if `cond1'
replace energycharge_dwr_win= 0.03952 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 3.39 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.35 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.96 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.97 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00140 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00312 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00316 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.02020 if `cond2'
replace energycharge_win= 0.02020 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 86.50 if `cond2'
replace demandcharge= 8.15 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond2'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 93.49 if `cond2'
replace voltage_dis_load_1= 0.09 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 2.98 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.18 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Mar 1 2011

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(3,1,2011) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.11528 if `cond1'
replace energycharge_win= 0.05944 if `cond1'
replace energycharge_dwr_sum= 0.03952 if `cond1'
replace energycharge_dwr_win= 0.03952 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 3.39 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.35 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.96 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.97 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00140 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00312 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00316 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.02020 if `cond2'
replace energycharge_win= 0.02020 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 86.50 if `cond2'
replace demandcharge= 8.15 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond2'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 93.49 if `cond2'
replace voltage_dis_load_1= 0.09 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 2.98 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.18 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Jun 1 2011

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(6,1,2011) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.09881 if `cond1'
replace energycharge_win= 0.05028 if `cond1'
replace energycharge_dwr_sum= 0.03952 if `cond1'
replace energycharge_dwr_win= 0.03952 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 2.95 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.35 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.96 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.97 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00140 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00312 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00316 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.02107 if `cond2'
replace energycharge_win= 0.02107 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 87.13 if `cond2'
replace demandcharge= 8.13 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond2'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 94.18 if `cond2'
replace voltage_dis_load_1= 0.09 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 2.98 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.18 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Jan 1 2012

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(1,1,2012) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.09881 if `cond1'
replace energycharge_win= 0.05028 if `cond1'
replace energycharge_dwr_sum= 0.00593 if `cond1'
replace energycharge_dwr_win= 0.00593 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 2.95 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.35 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.96 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.97 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00140 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00312 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00316 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.02094 if `cond2'
replace energycharge_win= 0.02094 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 87.13 if `cond2'
replace demandcharge= 8.12 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond2'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 94.18 if `cond2'
replace voltage_dis_load_1= 0.09 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 2.98 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.18 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Jun 1 2012

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(6,1,2012) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.09881 if `cond1'
replace energycharge_win= 0.05028 if `cond1'
replace energycharge_dwr_sum= 0.00463 if `cond1'
replace energycharge_dwr_win= 0.00463 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 2.95 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.35 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.96 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.97 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00140 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00312 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00316 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.02098 if `cond2'
replace energycharge_win= 0.02098 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 87.13 if `cond2'
replace demandcharge= 8.12 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond2'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 94.18 if `cond2'
replace voltage_dis_load_1= 0.09 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 2.98 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.18 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3


replace old= 0 if missing(old)

// Aug 1 2012

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(8,1,2012) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.09610 if `cond1'
replace energycharge_win= 0.04897 if `cond1'
replace energycharge_dwr_sum= 0.00463 if `cond1'
replace energycharge_dwr_win= 0.00463 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 2.87 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.35 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.96 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.97 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00140 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00312 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00316 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.02148 if `cond2'
replace energycharge_win= 0.02148 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 88.98 if `cond2'
replace demandcharge= 8.40 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond2'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 96.18 if `cond2'
replace voltage_dis_load_1= 0.09 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 2.98 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.18 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Oct 1 2012

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(10,1,2012) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.09610 if `cond1'
replace energycharge_win= 0.04897 if `cond1'
replace energycharge_dwr_sum= 0.00463 if `cond1'
replace energycharge_dwr_win= 0.00463 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 2.87 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.35 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.96 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.97 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00140 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00312 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00316 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.02153 if `cond2'
replace energycharge_win= 0.02153 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 88.98 if `cond2'
replace demandcharge= 8.87 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond2'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 96.18 if `cond2'
replace voltage_dis_load_1= 0.09 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 2.98 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.18 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Apr 1 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(4,1,2013) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.09827 if `cond1'
replace energycharge_win= 0.05508 if `cond1'
replace energycharge_dwr_sum= 0.00097 if `cond1'
replace energycharge_dwr_win= 0.00097 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 3.32 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.35 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.96 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.97 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00140 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00312 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00316 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.01975 if `cond2'
replace energycharge_win= 0.01975 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 93.99 if `cond2'
replace demandcharge= 8.33 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond2'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 101.59 if `cond2'
replace voltage_dis_load_1= 0.09 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 2.98 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.18 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Apr 12 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(4,12,2013) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.10525 if `cond1'
replace energycharge_win= 0.05152 if `cond1'
replace energycharge_dwr_sum= 0.00097 if `cond1'
replace energycharge_dwr_win= 0.00097 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 3.27 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.35 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.96 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.97 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00140 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00312 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00316 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.02052 if `cond2'
replace energycharge_win= 0.02052 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 94.57 if `cond2'
replace demandcharge= 9.31 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond2'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 102.23 if `cond2'
replace voltage_dis_load_1= 0.09 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 2.98 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.18 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Jun 1 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(4,1,2013) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.09798 if `cond1'
replace energycharge_win= 0.05492 if `cond1'
replace energycharge_dwr_sum= 0.00097 if `cond1'
replace energycharge_dwr_win= 0.00097 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 3.31 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.35 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.96 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.97 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00140 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00312 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00316 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.02049 if `cond2'
replace energycharge_win= 0.02049 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 93.88 if `cond2'
replace demandcharge= 8.32 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond2'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 101.48 if `cond2'
replace voltage_dis_load_1= 0.09 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 2.98 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.18 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Oct 1 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(10,1,2013) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.09798 if `cond1'
replace energycharge_win= 0.05492 if `cond1'
replace energycharge_dwr_sum= 0.00097 if `cond1'
replace energycharge_dwr_win= 0.00097 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 3.31 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.35 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.96 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.97 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00140 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00312 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00316 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.02041 if `cond2'
replace energycharge_win= 0.02041 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 93.88 if `cond2'
replace demandcharge= 9.28 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond2'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 101.48 if `cond2'
replace voltage_dis_load_1= 0.09 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 2.98 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.18 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Nov 22 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(11,22,2013) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.09985 if `cond1'
replace energycharge_win= 0.05595 if `cond1'
replace energycharge_dwr_sum= 0.00095 if `cond1'
replace energycharge_dwr_win= 0.00095 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 3.37 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.35 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.96 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.97 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00140 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00312 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00316 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.02132 if `cond2'
replace energycharge_win= 0.02132 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 97.17 if `cond2'
replace demandcharge= 9.53 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond2'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 105.03 if `cond2'
replace voltage_dis_load_1= 0.09 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.05 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.32 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Jan 1 2014

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(1,1,2014) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.09778 if `cond1'
replace energycharge_win= 0.05454 if `cond1'
replace energycharge_dwr_sum= 0.00037 if `cond1'
replace energycharge_dwr_win= 0.00037 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 3.32 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.34 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.94 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.95 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00134 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00303 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00306 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.02141 if `cond2'
replace energycharge_win= 0.02141 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 99.26 if `cond2'
replace demandcharge= 9.31 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond2'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 107.28 if `cond2'
replace voltage_dis_load_1= 0.09 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.08 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.38 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Apr 1 2014

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(4,1,2014) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.10747 if `cond1'
replace energycharge_win= 0.05993 if `cond1'
replace energycharge_dwr_sum= 0.00037 if `cond1'
replace energycharge_dwr_win= 0.00037 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 3.65 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.37 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 1.03 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 1.04 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00147 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00333 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00336 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.02141 if `cond2'
replace energycharge_win= 0.02141 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 99.26 if `cond2'
replace demandcharge= 9.31 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond2'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 107.28 if `cond2'
replace voltage_dis_load_1= 0.09 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.08 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.38 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Jun 1 2014

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(6,1,2014) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.12257 if `cond1'
replace energycharge_win= 0.06833 if `cond1'
replace energycharge_dwr_sum= 0.00037 if `cond1'
replace energycharge_dwr_win= 0.00037 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 4.16 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.40 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 1.10 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 1.11 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00158 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00356 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00360 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.02255 if `cond2'
replace energycharge_win= 0.02255 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 96.41 if `cond2'
replace demandcharge= 9.09 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond2'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 104.20 if `cond2'
replace voltage_dis_load_1= 0.09 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 2.99 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.19 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Jul 7 2014

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(7,7,2014) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.12257 if `cond1'
replace energycharge_win= 0.06833 if `cond1'
replace energycharge_dwr_sum= 0.00037 if `cond1'
replace energycharge_dwr_win= 0.00037 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 4.16 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.40 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 1.10 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 1.11 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00158 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00356 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00360 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.02319 if `cond2'
replace energycharge_win= 0.02319 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 96.41 if `cond2'
replace demandcharge= 9.09 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond2'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 104.20 if `cond2'
replace voltage_dis_load_1= 0.09 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 2.99 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.19 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00681 if `cond2'

replace old= 0 if missing(old)

// Aug 11 2014

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(8,11,2014) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.12257 if `cond1'
replace energycharge_win= 0.06833 if `cond1'
replace energycharge_dwr_sum= 0.00037 if `cond1'
replace energycharge_dwr_win= 0.00037 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 4.16 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.40 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 1.09 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 1.11 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00157 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00354 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00358 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.02318 if `cond2'
replace energycharge_win= 0.02318 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 96.41 if `cond2'
replace demandcharge= 9.09 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond2'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 104.20 if `cond2'
replace voltage_dis_load_1= 0.09 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 2.99 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.19 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00681 if `cond2'

replace old= 0 if missing(old)

// Aug 11 2014

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(8,11,2014) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.12257 if `cond1'
replace energycharge_win= 0.06833 if `cond1'
replace energycharge_dwr_sum= 0.00037 if `cond1'
replace energycharge_dwr_win= 0.00037 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 4.16 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.40 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 1.09 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 1.10 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00157 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00354 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00358 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.02318 if `cond2'
replace energycharge_win= 0.02318 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 96.41 if `cond2'
replace demandcharge= 9.09 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 104.20 if `cond2'
replace voltage_dis_load_1= 0.09 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 2.99 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.19 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00731 if `cond2'


replace old= 0 if missing(old)

// Jan 1 2015

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(1,1,2015) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.12392 if `cond1'
replace energycharge_win= 0.06968 if `cond1'
replace energycharge_dwr_sum= 0.00172 if `cond1'
replace energycharge_dwr_win= 0.00172 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 4.16 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.37 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 1.01 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 1.02 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00146 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00329 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00332 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.02378 if `cond2'
replace energycharge_win= 0.02378 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 98.62 if `cond2'
replace demandcharge= 9.25 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 106.59 if `cond2'
replace voltage_dis_load_1= 0.09 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.06 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.33 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00731 if `cond2'

replace old= 0 if missing(old)

// Mar 2 2015

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(3,2,2015) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.12392 if `cond1'
replace energycharge_win= 0.06968 if `cond1'
replace energycharge_dwr_sum= 0.00172 if `cond1'
replace energycharge_dwr_win= 0.00172 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 4.16 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.36 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.99 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 1.00 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00143 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00323 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00326 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.02377 if `cond2'
replace energycharge_win= 0.02377 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 98.62 if `cond2'
replace demandcharge= 9.25 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 106.59 if `cond2'
replace voltage_dis_load_1= 0.09 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.06 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.33 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00680 if `cond2'

replace old= 0 if missing(old)

// Jun 1 2015

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(6,1,2015) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.12392 if `cond1'
replace energycharge_win= 0.06968 if `cond1'
replace energycharge_dwr_sum= 0.00172 if `cond1'
replace energycharge_dwr_win= 0.00172 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 4.16 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.36 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.99 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 1.00 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00143 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00323 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00326 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.02422 if `cond2'
replace energycharge_win= 0.02422 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 98.62 if `cond2'
replace demandcharge= 9.25 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 106.59 if `cond2'
replace voltage_dis_load_1= 0.09 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.06 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.33 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00680 if `cond2'

replace old= 0 if missing(old)

// Oct 1 2015

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(10,1,2015) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.12392 if `cond1'
replace energycharge_win= 0.06968 if `cond1'
replace energycharge_dwr_sum= 0.00172 if `cond1'
replace energycharge_dwr_win= 0.00172 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 4.16 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.36 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.99 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 1.00 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00143 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00323 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00326 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.02425 if `cond2'
replace energycharge_win= 0.02425 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 98.62 if `cond2'
replace demandcharge= 9.25 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 106.59 if `cond2'
replace voltage_dis_load_1= 0.09 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.06 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.33 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00680 if `cond2'

replace old= 0 if missing(old)

// Jan 1 2016

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(1,1,2016) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.08858 if `cond1'
replace energycharge_win= 0.04936 if `cond1'
replace energycharge_dwr_sum= 0.00022 if `cond1'
replace energycharge_dwr_win= 0.00022 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 3.01 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.26 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.73 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.74 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00106 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00240 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00242 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.02286 if `cond2'
replace energycharge_win= 0.02286 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 110.11 if `cond2'
replace demandcharge= 10.66 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 119.01 if `cond2'
replace voltage_dis_load_1= 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.40 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 7.03 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00378 if `cond2'

replace old= 0 if missing(old)

// Jun 1 2016

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(6,1,2016) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.08480 if `cond1'
replace energycharge_win= 0.04726 if `cond1'
replace energycharge_dwr_sum= 0.00022 if `cond1'
replace energycharge_dwr_win= 0.00022 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 2.88 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.26 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.73 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.74 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00106 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00240 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00242 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.02056 if `cond2'
replace energycharge_win= 0.02056 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 117.67 if `cond2'
replace demandcharge= 11.24 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 127.18 if `cond2'
replace voltage_dis_load_1= 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.40 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 7.03 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00378 if `cond2'

replace old= 0 if missing(old)

// Oct 1 2016

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(10,1,2016) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.08480 if `cond1'
replace energycharge_win= 0.04726 if `cond1'
replace energycharge_dwr_sum= 0.00022 if `cond1'
replace energycharge_dwr_win= 0.00022 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 2.88 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.26 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.73 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.74 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00106 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00240 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00242 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.02112 if `cond2'
replace energycharge_win= 0.02112 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 117.67 if `cond2'
replace demandcharge= 11.24 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 127.18 if `cond2'
replace voltage_dis_load_1= 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.40 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 7.03 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00378 if `cond2'

replace old= 0 if missing(old)

// Jan 1 2017

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(1,1,2017) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.08458 if `cond1'
replace energycharge_win= 0.04704 if `cond1'
replace energycharge_dwr_sum= 0.000 if `cond1'
replace energycharge_dwr_win= 0.000 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 2.88 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.29 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.83 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.84 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00120 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00272 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00275 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.02362 if `cond2'
replace energycharge_win= 0.02362 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 115.95 if `cond2'
replace demandcharge= 11.43 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 125.32 if `cond2'
replace voltage_dis_load_1= 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.32 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.86 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00414 if `cond2'

replace old= 0 if missing(old)

// Jun 1 2017

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(6,1,2017) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.08458 if `cond1'
replace energycharge_win= 0.04704 if `cond1'
replace energycharge_dwr_sum= 0.000 if `cond1'
replace energycharge_dwr_win= 0.000 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 2.88 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.29 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.83 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.84 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00120 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00272 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00275 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.02272 if `cond2'
replace energycharge_win= 0.02272 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 115.95 if `cond2'
replace demandcharge= 11.43 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 125.32 if `cond2'
replace voltage_dis_load_1= 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.32 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.86 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00414 if `cond2'

replace old= 0 if missing(old)

// Oct 1 2017

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(10,1,2017) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.08458 if `cond1'
replace energycharge_win= 0.04704 if `cond1'
replace energycharge_dwr_sum= 0.000 if `cond1'
replace energycharge_dwr_win= 0.000 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 2.88 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.29 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.83 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.84 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00120 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00272 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00275 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.02214 if `cond2'
replace energycharge_win= 0.02214 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 115.95 if `cond2'
replace demandcharge= 11.43 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 125.32 if `cond2'
replace voltage_dis_load_1= 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.32 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.86 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00414 if `cond2'

replace old= 0 if missing(old)

// Oct 1 2018

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(10,1,2018) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.09579 if `cond1'
replace energycharge_win= 0.05327 if `cond1'
replace energycharge_dwr_sum= 0.000 if `cond1'
replace energycharge_dwr_win= 0.000 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 3.26 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.33 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.94 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.95 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00136 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00308 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00311 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.01948 if `cond2'
replace energycharge_win= 0.01948 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 120.18 if `cond2'
replace demandcharge= 11.65 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 129.89 if `cond2'
replace voltage_dis_load_1= 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.41 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 7.06 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00400 if `cond2'

replace old= 0 if missing(old)


// Jan 1 2018

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(1,1,2018) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.09715 if `cond1'
replace energycharge_win= 0.05403 if `cond1'
replace energycharge_dwr_sum= 0.000 if `cond1'
replace energycharge_dwr_win= 0.000 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 3.31 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.33 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.95 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.96 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00138 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00312 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00315 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.01882 if `cond2'
replace energycharge_win= 0.01882 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 120.18 if `cond2'
replace demandcharge= 11.65 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 129.89 if `cond2'
replace voltage_dis_load_1= 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.41 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 7.06 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00400 if `cond2'

replace old= 0 if missing(old)

// Jun 1 2018

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(6,1,2018) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.09715 if `cond1'
replace energycharge_win= 0.05403 if `cond1'
replace energycharge_dwr_sum= 0.000 if `cond1'
replace energycharge_dwr_win= 0.000 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 3.31 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.33 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.95 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.96 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00138 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00312 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00315 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.01951 if `cond2'
replace energycharge_win= 0.01951 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 120.18 if `cond2'
replace demandcharge= 11.65 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 129.89 if `cond2'
replace voltage_dis_load_1= 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.41 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 7.06 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00400 if `cond2'

replace old= 0 if missing(old)


// Aug 27 2018

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(8,27,2018) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.07351 if `cond1'
replace energycharge_win= 0.04088 if `cond1'
replace energycharge_dwr_sum= 0.000 if `cond1'
replace energycharge_dwr_win= 0.000 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 2.5 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.25 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.72 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.73 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00104 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00236 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00238 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.01925 if `cond2'
replace energycharge_win= 0.01925 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 120.18 if `cond2'
replace demandcharge= 11.65 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 129.89 if `cond2'
replace voltage_dis_load_1= 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.41 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 7.06 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00400 if `cond2'

replace old= 0 if missing(old)


// Sep 15 2018

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(9,15,2018) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.09715 if `cond1'
replace energycharge_win= 0.05403 if `cond1'
replace energycharge_dwr_sum= 0.000 if `cond1'
replace energycharge_dwr_win= 0.000 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 3.31 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.33 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.95 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.96 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00138 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00312 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00315 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.01951 if `cond2'
replace energycharge_win= 0.01951 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 120.18 if `cond2'
replace demandcharge= 11.65 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 129.89 if `cond2'
replace voltage_dis_load_1= 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.41 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 7.06 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00400 if `cond2'

replace old= 0 if missing(old)


// Sep 15 2018

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(9,15,2018) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.09715 if `cond1'
replace energycharge_win= 0.05403 if `cond1'
replace energycharge_dwr_sum= 0.000 if `cond1'
replace energycharge_dwr_win= 0.000 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 3.31 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.33 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.95 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.96 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00138 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00312 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00315 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.01951 if `cond2'
replace energycharge_win= 0.01951 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 120.18 if `cond2'
replace demandcharge= 11.65 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 129.89 if `cond2'
replace voltage_dis_load_1= 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.41 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 7.06 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00400 if `cond2'

replace old= 0 if missing(old)


// Jan 1 2019

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(1,1,2019) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.09579 if `cond1'
replace energycharge_win= 0.05327 if `cond1'
replace energycharge_dwr_sum= 0.00007 if `cond1'
replace energycharge_dwr_win= 0.00007 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 3.26 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.33 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.94 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.95 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00136 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00308 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00311 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.01957 if `cond2'
replace energycharge_win= 0.01957 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 120.18 if `cond2'
replace demandcharge= 11.29 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 129.89 if `cond2'
replace voltage_dis_load_1= 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.41 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 7.06 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.00 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00400 if `cond2'

replace old= 0 if missing(old)


// Mar 1 2019

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(3,1,2019) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.07658 if `cond1'
replace energycharge_win= 0.05355 if `cond1'
replace energycharge_dwr_sum= 0.00007 if `cond1'
replace energycharge_dwr_win= 0.00007 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 3.06 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.05 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.14 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.14 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00105 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00232 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00235 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.03617 if `cond2'
replace energycharge_win= 0.02801 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 43.15 if `cond2'
replace demandcharge= 8.44 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 0 if `cond2'
replace voltage_dis_load_1= 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 2.69 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.43 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00019 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00535 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.01270 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00400 if `cond2'

replace old= 0 if missing(old)

// Apr 12 2019

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(3,1,2019) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.08126 if `cond1'
replace energycharge_win= 0.05682 if `cond1'
replace energycharge_dwr_sum= 0.00007 if `cond1'
replace energycharge_dwr_win= 0.00007 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 3.25 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.05 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.15 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.15 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00112 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00247 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00249 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.03373 if `cond2'
replace energycharge_win= 0.02608 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 40.46 if `cond2'
replace demandcharge= 8.04 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 0 if `cond2'
replace voltage_dis_load_1= 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 2.52 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.03 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00018 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00502 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.01191 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00400 if `cond2'

replace old= 0 if missing(old)


// June 1 2019

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-2" if missing(old)
replace rate_start_date = mdy(6,1,2019) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_sum= 0.08799 if `cond1'
replace energycharge_win= 0.06152 if `cond1'
replace energycharge_dwr_sum= 0.00007 if `cond1'
replace energycharge_dwr_win= 0.00007 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace demandcharge_sum= 3.52 if `cond1'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond1'
replace tou_RTEM_meter_charge= 0 if `cond1'
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.05 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.16 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.16 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00121 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00267 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00270 if `cond1' & voltage_cat==3


// unbundled
replace energycharge_sum= 0.03424 if `cond2'
replace energycharge_win= 0.02642 if `cond2'
replace energycharge_dwr_sum= 0 if `cond2'
replace energycharge_dwr_win= 0 if `cond2'
replace customercharge= 41.34 if `cond2'
replace demandcharge= 8.17 if `cond2'
replace demandcharge_sum= 0 if `cond2'
replace demandcharge_win= 0 if `cond1'
replace tou_option_meter_charge= 0 if `cond2'
replace tou_RTEM_meter_charge= 0 if `cond2'
replace voltage_dis_load_1= 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 2.57 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.16 if `cond2' & voltage_cat==3
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2= 0 if `cond2' & voltage_cat==3
replace voltage_dis_energy= 0.00018 if `cond2' & voltage_cat==1
replace voltage_dis_energy= 0.00513 if `cond2' & voltage_cat==2
replace voltage_dis_energy= 0.01217 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00251 if `cond2'
replace old= 0 if missing(old)


***************************
*** 3. Schedule- PA-RTP ***
***************************

// new variables Power Factor Adjustment, hourly volatge discount

gen pf_adjust= 0
gen voltage_dis_hrly= 0

label variable pf_adjust "$/kVAR"
label variable voltage_dis_hrly " Hourly Rates- %"

// Jan 1 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-RTP" if missing(old)
replace rate_start_date = mdy(1,1,2009) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled

replace energycharge_dwr= 0.08451 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace voltage_dis_hrly= 0 if `cond1' & voltage_cat==1
replace voltage_dis_hrly= 0 if `cond1' & voltage_cat==2
replace voltage_dis_hrly= 0 if `cond1' & voltage_cat==3
replace pf_adjust= 0 if `cond1'& voltage_cat==1
replace pf_adjust= 0 if `cond1'& voltage_cat==2
replace pf_adjust= 0 if `cond1'& voltage_cat==3

// unbundled
replace energycharge= 0.01349 if `cond2'
replace energycharge_dwr= 0 if `cond2'
replace customercharge= 83.73 if `cond2'
replace demandcharge= 4.46 if `cond2'
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==1
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==2
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==3
replace pf_adjust= 0.18 if `cond2'& voltage_cat==1
replace pf_adjust= 0.20 if `cond2'& voltage_cat==2
replace pf_adjust= 0.20 if `cond2'& voltage_cat==3

replace old= 0 if missing(old)


// Mar 1 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-RTP" if missing(old)
replace rate_start_date = mdy(3,1,2009) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled

replace energycharge_dwr= 0.01063 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace voltage_dis_hrly= 2.38 if `cond1' & voltage_cat==1
replace voltage_dis_hrly= 5.19 if `cond1' & voltage_cat==2
replace voltage_dis_hrly= 5.19 if `cond1' & voltage_cat==3
replace pf_adjust= 0 if `cond1'& voltage_cat==1
replace pf_adjust= 0 if `cond1'& voltage_cat==2
replace pf_adjust= 0 if `cond1'& voltage_cat==3

// unbundled
replace energycharge= 0.01063 if `cond2'
replace energycharge_dwr= 0 if `cond2'
replace customercharge= 81.68 if `cond2'
replace demandcharge= 4.35 if `cond2'
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==1
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==2
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==3
replace pf_adjust= 0.18 if `cond2'& voltage_cat==1
replace pf_adjust= 0.20 if `cond2'& voltage_cat==2
replace pf_adjust= 0.20 if `cond2'& voltage_cat==3

replace old= 0 if missing(old)


// Apr 4 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-RTP" if missing(old)
replace rate_start_date = mdy(4,4,2009) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled

replace energycharge_dwr= 0.06210 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace voltage_dis_hrly= 2.38 if `cond1' & voltage_cat==1
replace voltage_dis_hrly= 5.19 if `cond1' & voltage_cat==2
replace voltage_dis_hrly= 5.19 if `cond1' & voltage_cat==3
replace pf_adjust= 0 if `cond1'& voltage_cat==1
replace pf_adjust= 0 if `cond1'& voltage_cat==2
replace pf_adjust= 0 if `cond1'& voltage_cat==3

// unbundled
replace energycharge= 0.01042 if `cond2'
replace energycharge_dwr= 0 if `cond2'
replace customercharge= 94.8 if `cond2'
replace demandcharge= 5.05 if `cond2'
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==1
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==2
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==3
replace pf_adjust= 0.18 if `cond2'& voltage_cat==1
replace pf_adjust= 0.20 if `cond2'& voltage_cat==2
replace pf_adjust= 0.20 if `cond2'& voltage_cat==3

replace old= 0 if missing(old)


// June 1 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-RTP" if missing(old)
replace rate_start_date = mdy(4,4,2009) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled

replace energycharge_dwr= 0.06225 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace voltage_dis_hrly= 2.38 if `cond1' & voltage_cat==1
replace voltage_dis_hrly= 5.19 if `cond1' & voltage_cat==2
replace voltage_dis_hrly= 5.19 if `cond1' & voltage_cat==3
replace pf_adjust= 0 if `cond1'& voltage_cat==1
replace pf_adjust= 0 if `cond1'& voltage_cat==2
replace pf_adjust= 0 if `cond1'& voltage_cat==3

// unbundled
replace energycharge= 0.01081 if `cond2'
replace energycharge_dwr= 0 if `cond2'
replace customercharge= 94.8 if `cond2'
replace demandcharge= 5.05 if `cond2'
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==1
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==2
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==3
replace pf_adjust= 0.18 if `cond2'& voltage_cat==1
replace pf_adjust= 0.20 if `cond2'& voltage_cat==2
replace pf_adjust= 0.20 if `cond2'& voltage_cat==3

replace old= 0 if missing(old)


// Sept 4 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-RTP" if missing(old)
replace rate_start_date = mdy(9,4,2009) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled

replace energycharge_dwr= 0.06225 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace voltage_dis_hrly= 2.38 if `cond1' & voltage_cat==1
replace voltage_dis_hrly= 5.19 if `cond1' & voltage_cat==2
replace voltage_dis_hrly= 5.19 if `cond1' & voltage_cat==3
replace pf_adjust= 0 if `cond1'& voltage_cat==1
replace pf_adjust= 0 if `cond1'& voltage_cat==2
replace pf_adjust= 0 if `cond1'& voltage_cat==3

// unbundled
replace energycharge= 0.01081 if `cond2'
replace energycharge_dwr= 0 if `cond2'
replace customercharge= 94.8 if `cond2'
replace demandcharge= 5.05 if `cond2'
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==1
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==2
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==3
replace pf_adjust= 0.18 if `cond2'& voltage_cat==1
replace pf_adjust= 0.20 if `cond2'& voltage_cat==2
replace pf_adjust= 0.20 if `cond2'& voltage_cat==3

replace old= 0 if missing(old)


// Oct 1 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-RTP" if missing(old)
replace rate_start_date = mdy(10,1,2009) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled

replace energycharge_dwr= 0.06225 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace voltage_dis_hrly= 2.38 if `cond1' & voltage_cat==1
replace voltage_dis_hrly= 5.19 if `cond1' & voltage_cat==2
replace voltage_dis_hrly= 5.19 if `cond1' & voltage_cat==3
replace pf_adjust= 0 if `cond1'& voltage_cat==1
replace pf_adjust= 0 if `cond1'& voltage_cat==2
replace pf_adjust= 0 if `cond1'& voltage_cat==3

// unbundled
replace energycharge= 0.01207 if `cond2'
replace energycharge_dwr= 0 if `cond2'
replace customercharge= 113.76 if `cond2'
replace demandcharge= 6.82 if `cond2'
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==1
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==2
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==3
replace voltage_dis_load_1= 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.08 if `cond2' & voltage_cat==3
replace pf_adjust= 0.27 if `cond2'& voltage_cat==1
replace pf_adjust= 0.32 if `cond2'& voltage_cat==2
replace pf_adjust= 0.32 if `cond2'& voltage_cat==3

replace old= 0 if missing(old)


// Oct 1 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-RTP" if missing(old)
replace rate_start_date = mdy(10,1,2009) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled

replace energycharge_dwr= 0.06225 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace voltage_dis_hrly= 2.38 if `cond1' & voltage_cat==1
replace voltage_dis_hrly= 5.19 if `cond1' & voltage_cat==2
replace voltage_dis_hrly= 5.19 if `cond1' & voltage_cat==3
replace pf_adjust= 0 if `cond1'& voltage_cat==1
replace pf_adjust= 0 if `cond1'& voltage_cat==2
replace pf_adjust= 0 if `cond1'& voltage_cat==3

// unbundled
replace energycharge= 0.01207 if `cond2'
replace energycharge_dwr= 0 if `cond2'
replace customercharge= 113.76 if `cond2'
replace demandcharge= 6.82 if `cond2'
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==1
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==2
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==3
replace voltage_dis_load_1= 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.08 if `cond2' & voltage_cat==3
replace pf_adjust= 0.27 if `cond2'& voltage_cat==1
replace pf_adjust= 0.32 if `cond2'& voltage_cat==2
replace pf_adjust= 0.32 if `cond2'& voltage_cat==3

replace old= 0 if missing(old)


// Jan 1 2010

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-RTP" if missing(old)
replace rate_start_date = mdy(1,1,2010) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled

replace energycharge_dwr= 0.03763 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace voltage_dis_hrly= 2.39 if `cond1' & voltage_cat==1
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==2
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==3
replace pf_adjust= 0 if `cond1'& voltage_cat==1
replace pf_adjust= 0 if `cond1'& voltage_cat==2
replace pf_adjust= 0 if `cond1'& voltage_cat==3

// unbundled
replace energycharge= 0.01294 if `cond2'
replace energycharge_dwr= 0 if `cond2'
replace customercharge= 113.76 if `cond2'
replace demandcharge= 6.83 if `cond2'
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==1
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==2
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==3
replace voltage_dis_load_1= 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.08 if `cond2' & voltage_cat==3
replace pf_adjust= 0.27 if `cond2'& voltage_cat==1
replace pf_adjust= 0.32 if `cond2'& voltage_cat==2
replace pf_adjust= 0.32 if `cond2'& voltage_cat==3

replace old= 0 if missing(old)


// Mar 1 2010

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-RTP" if missing(old)
replace rate_start_date = mdy(3,1,2010) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled

replace energycharge_dwr= 0.03763 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace voltage_dis_hrly= 2.39 if `cond1' & voltage_cat==1
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==2
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==3
replace pf_adjust= 0 if `cond1'& voltage_cat==1
replace pf_adjust= 0 if `cond1'& voltage_cat==2
replace pf_adjust= 0 if `cond1'& voltage_cat==3

// unbundled
replace energycharge= 0.01829 if `cond2'
replace energycharge_dwr= 0 if `cond2'
replace customercharge= 122.94 if `cond2'
replace demandcharge= 7.81 if `cond2'
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==1
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==2
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==3
replace voltage_dis_load_1= 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.08 if `cond2' & voltage_cat==3
replace pf_adjust= 0.27 if `cond2'& voltage_cat==1
replace pf_adjust= 0.32 if `cond2'& voltage_cat==2
replace pf_adjust= 0.32 if `cond2'& voltage_cat==3

replace old= 0 if missing(old)


// May 1 2010

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-RTP" if missing(old)
replace rate_start_date = mdy(5,1,2010) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled

replace energycharge_dwr= 0.03763 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace voltage_dis_hrly= 2.39 if `cond1' & voltage_cat==1
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==2
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==3
replace pf_adjust= 0 if `cond1'& voltage_cat==1
replace pf_adjust= 0 if `cond1'& voltage_cat==2
replace pf_adjust= 0 if `cond1'& voltage_cat==3

// unbundled
replace energycharge= 0.01829 if `cond2'
replace energycharge_dwr= 0 if `cond2'
replace customercharge= 122.94 if `cond2'
replace demandcharge= 7.81 if `cond2'
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==1
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==2
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==3
replace voltage_dis_load_1= 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.08 if `cond2' & voltage_cat==3
replace pf_adjust= 0.27 if `cond2'& voltage_cat==1
replace pf_adjust= 0.32 if `cond2'& voltage_cat==2
replace pf_adjust= 0.32 if `cond2'& voltage_cat==3

replace old= 0 if missing(old)


// Jun 1 2010

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-RTP" if missing(old)
replace rate_start_date = mdy(5,1,2010) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled

replace energycharge_dwr= 0.03763 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace voltage_dis_hrly= 2.39 if `cond1' & voltage_cat==1
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==2
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==3
replace pf_adjust= 0 if `cond1'& voltage_cat==1
replace pf_adjust= 0 if `cond1'& voltage_cat==2
replace pf_adjust= 0 if `cond1'& voltage_cat==3

// unbundled
replace energycharge= 0.01775 if `cond2'
replace energycharge_dwr= 0 if `cond2'
replace customercharge= 122.94 if `cond2'
replace demandcharge= 7.83 if `cond2'
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==1
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==2
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==3
replace voltage_dis_load_1= 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.08 if `cond2' & voltage_cat==3
replace pf_adjust= 0.27 if `cond2'& voltage_cat==1
replace pf_adjust= 0.32 if `cond2'& voltage_cat==2
replace pf_adjust= 0.32 if `cond2'& voltage_cat==3

replace old= 0 if missing(old)


// Jan 1 2011

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-RTP" if missing(old)
replace rate_start_date = mdy(1,1,2011) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled

replace energycharge_dwr= 0.03952 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace voltage_dis_hrly= 2.39 if `cond1' & voltage_cat==1
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==2
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==3
replace pf_adjust= 0 if `cond1'& voltage_cat==1
replace pf_adjust= 0 if `cond1'& voltage_cat==2
replace pf_adjust= 0 if `cond1'& voltage_cat==3

// unbundled
replace energycharge= 0.01800 if `cond2'
replace energycharge_dwr= 0 if `cond2'
replace customercharge= 122.94 if `cond2'
replace demandcharge= 7.83 if `cond2'
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==1
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==2
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==3
replace voltage_dis_load_1= 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.08 if `cond2' & voltage_cat==3
replace pf_adjust= 0.27 if `cond2'& voltage_cat==1
replace pf_adjust= 0.32 if `cond2'& voltage_cat==2
replace pf_adjust= 0.32 if `cond2'& voltage_cat==3

replace old= 0 if missing(old)


// Mar 1 2011

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-RTP" if missing(old)
replace rate_start_date = mdy(3,1,2011) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled

replace energycharge_dwr= 0.03952 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace voltage_dis_hrly= 2.39 if `cond1' & voltage_cat==1
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==2
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==3
replace pf_adjust= 0 if `cond1'& voltage_cat==1
replace pf_adjust= 0 if `cond1'& voltage_cat==2
replace pf_adjust= 0 if `cond1'& voltage_cat==3

// unbundled
replace energycharge= 0.01800 if `cond2'
replace energycharge_dwr= 0 if `cond2'
replace customercharge= 122.94 if `cond2'
replace demandcharge= 7.90 if `cond2'
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==1
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==2
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==3
replace voltage_dis_load_1= 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.08 if `cond2' & voltage_cat==3
replace pf_adjust= 0.27 if `cond2'& voltage_cat==1
replace pf_adjust= 0.32 if `cond2'& voltage_cat==2
replace pf_adjust= 0.32 if `cond2'& voltage_cat==3

replace old= 0 if missing(old)


// Jun 1 2011

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-RTP" if missing(old)
replace rate_start_date = mdy(6,1,2011) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled

replace energycharge_dwr= 0.03952 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace voltage_dis_hrly= 2.39 if `cond1' & voltage_cat==1
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==2
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==3
replace pf_adjust= 0 if `cond1'& voltage_cat==1
replace pf_adjust= 0 if `cond1'& voltage_cat==2
replace pf_adjust= 0 if `cond1'& voltage_cat==3

// unbundled
replace energycharge= 0.01727 if `cond2'
replace energycharge_dwr= 0 if `cond2'
replace customercharge= 123.84 if `cond2'
replace demandcharge= 7.87 if `cond2'
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==1
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==2
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==3
replace voltage_dis_load_1= 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.08 if `cond2' & voltage_cat==3
replace pf_adjust= 0.27 if `cond2'& voltage_cat==1
replace pf_adjust= 0.32 if `cond2'& voltage_cat==2
replace pf_adjust= 0.32 if `cond2'& voltage_cat==3

replace old= 0 if missing(old)


// Jan 1 2012

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-RTP" if missing(old)
replace rate_start_date = mdy(1,1,2012) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled

replace energycharge_dwr= 0.00593 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace voltage_dis_hrly= 2.39 if `cond1' & voltage_cat==1
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==2
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==3
replace pf_adjust= 0 if `cond1'& voltage_cat==1
replace pf_adjust= 0 if `cond1'& voltage_cat==2
replace pf_adjust= 0 if `cond1'& voltage_cat==3

// unbundled
replace energycharge= 0.01716 if `cond2'
replace energycharge_dwr= 0 if `cond2'
replace customercharge= 123.84 if `cond2'
replace demandcharge= 7.80 if `cond2'
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==1
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==2
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==3
replace voltage_dis_load_1= 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.08 if `cond2' & voltage_cat==3
replace pf_adjust= 0.27 if `cond2'& voltage_cat==1
replace pf_adjust= 0.32 if `cond2'& voltage_cat==2
replace pf_adjust= 0.32 if `cond2'& voltage_cat==3

replace old= 0 if missing(old)


// Jun 1 2012

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-RTP" if missing(old)
replace rate_start_date = mdy(6,1,2012) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
********************************************************** the hourly discount rate here dropped exactly by a factor of 100, bad input?
replace energycharge_dwr= 0.00463 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
//replace voltage_dis_hrly= 0.0239 if `cond1' & voltage_cat==1
replace voltage_dis_hrly= 2.39 if `cond1' & voltage_cat==1
//replace voltage_dis_hrly= 0.0532 if `cond1' & voltage_cat==2
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==2
//replace voltage_dis_hrly= 0.0532 if `cond1' & voltage_cat==3
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==3
replace pf_adjust= 0 if `cond1'& voltage_cat==1
replace pf_adjust= 0 if `cond1'& voltage_cat==2
replace pf_adjust= 0 if `cond1'& voltage_cat==3

// unbundled
replace energycharge= 0.01720 if `cond2'
replace energycharge_dwr= 0 if `cond2'
replace customercharge= 123.84 if `cond2'
replace demandcharge= 7.80 if `cond2'
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==1
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==2
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==3
replace voltage_dis_load_1= 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.08 if `cond2' & voltage_cat==3
replace pf_adjust= 0.27 if `cond2'& voltage_cat==1
replace pf_adjust= 0.32 if `cond2'& voltage_cat==2
replace pf_adjust= 0.32 if `cond2'& voltage_cat==3

replace old= 0 if missing(old)


// Aug 1 2012

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-RTP" if missing(old)
replace rate_start_date = mdy(8,1,2012) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_dwr= 0.00463 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace voltage_dis_hrly= 2.39 if `cond1' & voltage_cat==1
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==2
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==3
replace pf_adjust= 0 if `cond1'& voltage_cat==1
replace pf_adjust= 0 if `cond1'& voltage_cat==2
replace pf_adjust= 0 if `cond1'& voltage_cat==3

// unbundled
replace energycharge= 0.01724 if `cond2'
replace energycharge_dwr= 0 if `cond2'
replace customercharge= 126.47 if `cond2'
replace demandcharge= 8.07 if `cond2'
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==1
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==2
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==3
replace voltage_dis_load_1= 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.08 if `cond2' & voltage_cat==3
replace pf_adjust= 0.27 if `cond2'& voltage_cat==1
replace pf_adjust= 0.32 if `cond2'& voltage_cat==2
replace pf_adjust= 0.32 if `cond2'& voltage_cat==3

replace old= 0 if missing(old)


// Oct 1 2012

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-RTP" if missing(old)
replace rate_start_date = mdy(10,1,2012) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_dwr= 0.00463 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace voltage_dis_hrly= 2.39 if `cond1' & voltage_cat==1
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==2
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==3
replace pf_adjust= 0 if `cond1'& voltage_cat==1
replace pf_adjust= 0 if `cond1'& voltage_cat==2
replace pf_adjust= 0 if `cond1'& voltage_cat==3

// unbundled
replace energycharge= 0.01729 if `cond2'
replace energycharge_dwr= 0 if `cond2'
replace customercharge= 126.47 if `cond2'
replace demandcharge= 8.40 if `cond2'
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==1
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==2
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==3
replace voltage_dis_load_1= 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.08 if `cond2' & voltage_cat==3
replace pf_adjust= 0.27 if `cond2'& voltage_cat==1
replace pf_adjust= 0.32 if `cond2'& voltage_cat==2
replace pf_adjust= 0.32 if `cond2'& voltage_cat==3

replace old= 0 if missing(old)


// Oct 1 2012

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-RTP" if missing(old)
replace rate_start_date = mdy(10,1,2012) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_dwr= 0.00463 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace voltage_dis_hrly= 2.39 if `cond1' & voltage_cat==1
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==2
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==3
replace pf_adjust= 0 if `cond1'& voltage_cat==1
replace pf_adjust= 0 if `cond1'& voltage_cat==2
replace pf_adjust= 0 if `cond1'& voltage_cat==3

// unbundled
replace energycharge= 0.01729 if `cond2'
replace energycharge_dwr= 0 if `cond2'
replace customercharge= 126.47 if `cond2'
replace demandcharge= 8.40 if `cond2'
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==1
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==2
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==3
replace voltage_dis_load_1= 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.08 if `cond2' & voltage_cat==3
replace pf_adjust= 0.27 if `cond2'& voltage_cat==1
replace pf_adjust= 0.32 if `cond2'& voltage_cat==2
replace pf_adjust= 0.32 if `cond2'& voltage_cat==3

replace old= 0 if missing(old)


// Jan 1 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-RTP" if missing(old)
replace rate_start_date = mdy(1,1,2013) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_dwr= 0.00097 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace voltage_dis_hrly= 2.39 if `cond1' & voltage_cat==1
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==2
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==3
replace pf_adjust= 0 if `cond1'& voltage_cat==1
replace pf_adjust= 0 if `cond1'& voltage_cat==2
replace pf_adjust= 0 if `cond1'& voltage_cat==3

// unbundled
replace energycharge= 0.01691 if `cond2'
replace energycharge_dwr= 0 if `cond2'
replace customercharge= 134.42 if `cond2'
replace demandcharge= 8.83 if `cond2'
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==1
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==2
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==3
replace voltage_dis_load_1= 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.08 if `cond2' & voltage_cat==3
replace pf_adjust= 0.27 if `cond2'& voltage_cat==1
replace pf_adjust= 0.32 if `cond2'& voltage_cat==2
replace pf_adjust= 0.32 if `cond2'& voltage_cat==3

replace old= 0 if missing(old)


// Apr 1 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-RTP" if missing(old)
replace rate_start_date = mdy(4,1,2013) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_dwr= 0.00097 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace voltage_dis_hrly= 2.39 if `cond1' & voltage_cat==1
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==2
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==3
replace pf_adjust= 0 if `cond1'& voltage_cat==1
replace pf_adjust= 0 if `cond1'& voltage_cat==2
replace pf_adjust= 0 if `cond1'& voltage_cat==3

// unbundled
replace energycharge= 0.01708 if `cond2'
replace energycharge_dwr= 0 if `cond2'
replace customercharge= 134.42 if `cond2'
replace demandcharge= 8.83 if `cond2'
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==1
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==2
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==3
replace voltage_dis_load_1= 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.08 if `cond2' & voltage_cat==3
replace pf_adjust= 0.51 if `cond2'& voltage_cat==1
replace pf_adjust= 0.34 if `cond2'& voltage_cat==2
replace pf_adjust= 0.34 if `cond2'& voltage_cat==3

replace old= 0 if missing(old)


// Apr 12 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-RTP" if missing(old)
replace rate_start_date = mdy(4,12,2013) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_dwr= 0.00097 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace voltage_dis_hrly= 2.39 if `cond1' & voltage_cat==1
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==2
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==3
replace pf_adjust= 0 if `cond1'& voltage_cat==1
replace pf_adjust= 0 if `cond1'& voltage_cat==2
replace pf_adjust= 0 if `cond1'& voltage_cat==3

// unbundled
replace energycharge= 0.01691 if `cond2'
replace energycharge_dwr= 0 if `cond2'
replace customercharge= 134.42 if `cond2'
replace demandcharge= 8.83 if `cond2'
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==1
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==2
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==3
replace voltage_dis_load_1= 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.08 if `cond2' & voltage_cat==3
replace pf_adjust= 0.32 if `cond2'& voltage_cat==1
replace pf_adjust= 0.27 if `cond2'& voltage_cat==2
replace pf_adjust= 0.27 if `cond2'& voltage_cat==3

replace old= 0 if missing(old)


// Oct 1 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "PA-RTP" if missing(old)
replace rate_start_date = mdy(9,1,2013) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

// bundled
replace energycharge_dwr= 0.00097 if `cond1'
replace customercharge= 0 if `cond1'
replace demandcharge= 0 if `cond1'
replace voltage_dis_hrly= 2.39 if `cond1' & voltage_cat==1
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==2
replace voltage_dis_hrly= 5.32 if `cond1' & voltage_cat==3
replace voltage_dis_energy= 0.00153 if `cond1' & voltage_cat==1
replace voltage_dis_energy= 0.00369 if `cond1' & voltage_cat==2
replace voltage_dis_energy= 0.00373 if `cond1' & voltage_cat==3

// unbundled
replace energycharge= 0.02028 if `cond2'
replace energycharge_dwr= 0 if `cond2'
replace customercharge= 39.50 if `cond2'
replace demandcharge= 7.87 if `cond2'
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==1
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==2
replace voltage_dis_hrly= 0 if `cond2' & voltage_cat==3
replace voltage_dis_load_1= 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load_1= 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1= 6.08 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

***************************
*** 4. Schedule- TOU-PA ***
***************************

** TOU-PA-A

** Note: winter on peak has NA. across its prices, we code it as -999

gen energycharge_sum_on_pk= 0
gen energycharge_sum_m_pk= 0
gen energycharge_sum_off_pk= 0
gen energycharge_win_on_pk= 0
gen energycharge_win_m_pk= 0
gen energycharge_win_off_pk= 0
gen energycharge_dwr_sum_on_pk= 0
gen energycharge_dwr_sum_m_pk= 0
gen energycharge_dwr_sum_off_pk= 0
gen energycharge_dwr_win_on_pk= 0
gen energycharge_dwr_win_m_pk= 0
gen energycharge_dwr_win_off_pk= 0

label variable energycharge_dwr_win_on_pk "variable charge, when NA is labelled with -999"

// Oct 1 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-A" if missing(old)
replace rate_start_date = mdy(10,1,2009) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.23789 if `cond1'
replace energycharge_sum_m_pk = 0.09618 if `cond1'
replace energycharge_sum_off_pk = 0.03825 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.08100 if `cond1'
replace energycharge_win_off_pk= 0.03616 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.06225 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.06225 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.06225 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.06225 if `cond1'
replace energycharge_dwr_win_off_pk= 0.06225 if `cond1'
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01207 if `cond2'
replace energycharge_sum_m_pk = 0.01207 if `cond2'
replace energycharge_sum_off_pk = 0.01207 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01207 if `cond2'
replace energycharge_win_off_pk= 0.01207 if `cond2'
replace customercharge = 113.76 if `cond2'
replace servicecharge = 5.11 if `cond2'
replace voltage_dis_load = 0.07 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.28 if `cond2' & voltage_cat==2
replace voltage_dis_load = 2.31 if `cond2' & voltage_cat==3
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Jun 1 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-A" if missing(old)
replace rate_start_date = mdy(6,1,2009) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.11697 if `cond1'
replace energycharge_sum_m_pk = 0.09480 if `cond1'
replace energycharge_sum_off_pk = 0.02358 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.11057 if `cond1'
replace energycharge_win_off_pk= 0.02358 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.06225 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.06225 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.06225 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.06225 if `cond1'
replace energycharge_dwr_win_off_pk= 0.06225 if `cond1'
replace servicecharge = 1.03 if `cond1'
replace voltage_dis_energy = 0.00198 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00431 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00431 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01081 if `cond2'
replace energycharge_sum_m_pk = 0.01081 if `cond2'
replace energycharge_sum_off_pk = 0.01081 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01081 if `cond2'
replace energycharge_win_off_pk= 0.01081 if `cond2'
replace customercharge = 94.80 if `cond2'
replace servicecharge = 4.50 if `cond2'
replace voltage_dis_load = 0.14 if `cond2' & voltage_cat==1
replace voltage_dis_load = 4.68 if `cond2' & voltage_cat==2
replace voltage_dis_load = 4.68 if `cond2' & voltage_cat==3
replace pf_adjust = 0.18 if `cond2' & voltage_cat==1
replace pf_adjust = 0.20 if `cond2' & voltage_cat==2
replace pf_adjust = 0.20 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Apr 4 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-A" if missing(old)
replace rate_start_date = mdy(4,4,2009) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.11893 if `cond1'
replace energycharge_sum_m_pk = 0.09647 if `cond1'
replace energycharge_sum_off_pk = 0.02428 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.11244 if `cond1'
replace energycharge_win_off_pk= 0.02428 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.06210 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.06210 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.06210 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.06210 if `cond1'
replace energycharge_dwr_win_off_pk= 0.06210 if `cond1'
replace servicecharge = 1.04 if `cond1'
replace voltage_dis_energy = 0.00201 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00437 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00437 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01042 if `cond2'
replace energycharge_sum_m_pk = 0.01042 if `cond2'
replace energycharge_sum_off_pk = 0.01042 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01042 if `cond2'
replace energycharge_win_off_pk= 0.01042 if `cond2'
replace customercharge = 94.80 if `cond2'
replace servicecharge = 4.50 if `cond2'
replace voltage_dis_load = 0.14 if `cond2' & voltage_cat==1
replace voltage_dis_load = 4.68 if `cond2' & voltage_cat==2
replace voltage_dis_load = 4.68 if `cond2' & voltage_cat==3
replace pf_adjust = 0.18 if `cond2' & voltage_cat==1
replace pf_adjust = 0.20 if `cond2' & voltage_cat==2
replace pf_adjust = 0.20 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Mar 1 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-A" if missing(old)
replace rate_start_date = mdy(3,1,2009) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.11710 if `cond1'
replace energycharge_sum_m_pk = 0.09490 if `cond1'
replace energycharge_sum_off_pk = 0.02360 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.11068 if `cond1'
replace energycharge_win_off_pk= 0.02360 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.06231 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.06231 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.06231 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.06231 if `cond1'
replace energycharge_dwr_win_off_pk= 0.06231 if `cond1'
replace servicecharge = 1.03 if `cond1'
replace voltage_dis_energy = 0.00199 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00432 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00432 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01063 if `cond2'
replace energycharge_sum_m_pk = 0.01063 if `cond2'
replace energycharge_sum_off_pk = 0.01063 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01063 if `cond2'
replace energycharge_win_off_pk= 0.01063 if `cond2'
replace customercharge = 81.68 if `cond2'
replace servicecharge = 3.98 if `cond2'
replace voltage_dis_load = 0.14 if `cond2' & voltage_cat==1
replace voltage_dis_load = 4.68 if `cond2' & voltage_cat==2
replace voltage_dis_load = 4.68 if `cond2' & voltage_cat==3
replace pf_adjust = 0.18 if `cond2' & voltage_cat==1
replace pf_adjust = 0.20 if `cond2' & voltage_cat==2
replace pf_adjust = 0.20 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Jan 1 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-A" if missing(old)
replace rate_start_date = mdy(1,1,2009) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.10818 if `cond1'
replace energycharge_sum_m_pk = 0.08618 if `cond1'
replace energycharge_sum_off_pk = 0.01539 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.10182 if `cond1'
replace energycharge_win_off_pk= 0.01539 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.08451 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.08451 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.08451 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.08451 if `cond1'
replace energycharge_dwr_win_off_pk= 0.08451 if `cond1'
replace servicecharge = 1.03 if `cond1'
replace voltage_dis_energy = 0.00199 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00432 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00432 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01349 if `cond2'
replace energycharge_sum_m_pk = 0.01349 if `cond2'
replace energycharge_sum_off_pk = 0.01349 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01349 if `cond2'
replace energycharge_win_off_pk= 0.01349 if `cond2'
replace customercharge = 83.73 if `cond2'
replace servicecharge = 3.74 if `cond2'
replace voltage_dis_load = 0.14 if `cond2' & voltage_cat==1
replace voltage_dis_load = 4.68 if `cond2' & voltage_cat==2
replace voltage_dis_load = 4.68 if `cond2' & voltage_cat==3
replace pf_adjust = 0.18 if `cond2' & voltage_cat==1
replace pf_adjust = 0.20 if `cond2' & voltage_cat==2
replace pf_adjust = 0.20 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Jun 1 2010

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-A" if missing(old)
replace rate_start_date = mdy(6,1,2010) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.22958 if `cond1'
replace energycharge_sum_m_pk = 0.09250 if `cond1'
replace energycharge_sum_off_pk = 0.03646 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.03763 if `cond1'
replace energycharge_win_off_pk= 0.03763 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.03763 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.03763 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.03763 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.03763 if `cond1'
replace energycharge_dwr_win_off_pk= 0.03763 if `cond1'
replace servicecharge = 0.0 if `cond1'
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01775 if `cond2'
replace energycharge_sum_m_pk = 0.01775 if `cond2'
replace energycharge_sum_off_pk = 0.01775 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01775 if `cond2'
replace energycharge_win_off_pk= 0.01775 if `cond2'
replace customercharge = 122.94 if `cond2'
replace servicecharge = 4.89 if `cond2'
replace voltage_dis_load = 0.07 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.28 if `cond2' & voltage_cat==2
replace voltage_dis_load = 2.31 if `cond2' & voltage_cat==3
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Mar 1 2010

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-A" if missing(old)
replace rate_start_date = mdy(3,1,2010) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.22958 if `cond1'
replace energycharge_sum_m_pk = 0.09250 if `cond1'
replace energycharge_sum_off_pk = 0.03646 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.03763 if `cond1'
replace energycharge_win_off_pk= 0.03763 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.03763 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.03763 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.03763 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.03763 if `cond1'
replace energycharge_dwr_win_off_pk= 0.03763 if `cond1'
replace servicecharge = 0.0 if `cond1'
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01829 if `cond2'
replace energycharge_sum_m_pk = 0.01829 if `cond2'
replace energycharge_sum_off_pk = 0.01829 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01829 if `cond2'
replace energycharge_win_off_pk= 0.01829 if `cond2'
replace customercharge = 122.94 if `cond2'
replace servicecharge = 5.86 if `cond2'
replace voltage_dis_load = 0.07 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.28 if `cond2' & voltage_cat==2
replace voltage_dis_load = 2.31 if `cond2' & voltage_cat==3
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Jan 1 2010

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-A" if missing(old)
replace rate_start_date = mdy(1,1,2010) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.23789 if `cond1'
replace energycharge_sum_m_pk = 0.09618 if `cond1'
replace energycharge_sum_off_pk = 0.03825 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.08100 if `cond1'
replace energycharge_win_off_pk= 0.03616 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.03763 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.03763 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.03763 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.03763 if `cond1'
replace energycharge_dwr_win_off_pk= 0.03763 if `cond1'
replace servicecharge = 0.0 if `cond1'
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01294 if `cond2'
replace energycharge_sum_m_pk = 0.01294 if `cond2'
replace energycharge_sum_off_pk = 0.01294 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01294 if `cond2'
replace energycharge_win_off_pk= 0.01294 if `cond2'
replace customercharge = 113.76 if `cond2'
replace servicecharge = 5.12 if `cond2'
replace voltage_dis_load = 0.07 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.28 if `cond2' & voltage_cat==2
replace voltage_dis_load = 2.31 if `cond2' & voltage_cat==3
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Jun 1 2011

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-A" if missing(old)
replace rate_start_date = mdy(6,1,2011) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.19976 if `cond1'
replace energycharge_sum_m_pk = 0.07970 if `cond1'
replace energycharge_sum_off_pk = 0.03063 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.03952 if `cond1'
replace energycharge_win_off_pk= 0.03952 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.03952 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.03952 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.03952 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.03952 if `cond1'
replace energycharge_dwr_win_off_pk= 0.03952 if `cond1'
replace servicecharge = 0.0 if `cond1'
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01727 if `cond2'
replace energycharge_sum_m_pk = 0.01727 if `cond2'
replace energycharge_sum_off_pk = 0.01727 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01727 if `cond2'
replace energycharge_win_off_pk= 0.01727 if `cond2'
replace customercharge = 123.84 if `cond2'
replace servicecharge = 5.91 if `cond2'
replace voltage_dis_load = 0.07 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.28 if `cond2' & voltage_cat==2
replace voltage_dis_load = 2.31 if `cond2' & voltage_cat==3
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Mar 1 2011

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-A" if missing(old)
replace rate_start_date = mdy(3,1,2011) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.22958 if `cond1'
replace energycharge_sum_m_pk = 0.09250 if `cond1'
replace energycharge_sum_off_pk = 0.03646 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.03952 if `cond1'
replace energycharge_win_off_pk= 0.03952 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.03952 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.03952 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.03952 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.03952 if `cond1'
replace energycharge_dwr_win_off_pk= 0.03952 if `cond1'
replace servicecharge = 0.0 if `cond1'
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01800 if `cond2'
replace energycharge_sum_m_pk = 0.01800 if `cond2'
replace energycharge_sum_off_pk = 0.01800 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01800 if `cond2'
replace energycharge_win_off_pk= 0.01800 if `cond2'
replace customercharge = 122.94 if `cond2'
replace servicecharge = 5.93 if `cond2'
replace voltage_dis_load = 0.07 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.28 if `cond2' & voltage_cat==2
replace voltage_dis_load = 2.31 if `cond2' & voltage_cat==3
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Jan 1 2011

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-A" if missing(old)
replace rate_start_date = mdy(1,1,2011) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.22958 if `cond1'
replace energycharge_sum_m_pk = 0.09250 if `cond1'
replace energycharge_sum_off_pk = 0.03646 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.07782 if `cond1'
replace energycharge_win_off_pk= 0.03444 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.03952 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.03952 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.03952 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.03952 if `cond1'
replace energycharge_dwr_win_off_pk= 0.03952 if `cond1'
replace servicecharge = 0.0 if `cond1'
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01800 if `cond2'
replace energycharge_sum_m_pk = 0.01800 if `cond2'
replace energycharge_sum_off_pk = 0.01800 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01800 if `cond2'
replace energycharge_win_off_pk= 0.01800 if `cond2'
replace customercharge = 122.94 if `cond2'
replace servicecharge = 5.87 if `cond2'
replace voltage_dis_load = 0.07 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.28 if `cond2' & voltage_cat==2
replace voltage_dis_load = 2.31 if `cond2' & voltage_cat==3
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Oct 1 2012

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-A" if missing(old)
replace rate_start_date = mdy(10,1,2012) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.19418 if `cond1'
replace energycharge_sum_m_pk = 0.07755 if `cond1'
replace energycharge_sum_off_pk = 0.02989 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.06507 if `cond1'
replace energycharge_win_off_pk= 0.02817 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00463 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00463 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00463 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00463 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00463 if `cond1'
replace servicecharge = 0.0 if `cond1'
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01729 if `cond2'
replace energycharge_sum_m_pk = 0.01729 if `cond2'
replace energycharge_sum_off_pk = 0.01729 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01729 if `cond2'
replace energycharge_win_off_pk= 0.01729 if `cond2'
replace customercharge = 126.47 if `cond2'
replace servicecharge = 6.31 if `cond2'
replace voltage_dis_load = 0.07 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.28 if `cond2' & voltage_cat==2
replace voltage_dis_load = 2.31 if `cond2' & voltage_cat==3
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Aug 1 2012

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-A" if missing(old)
replace rate_start_date = mdy(8,1,2012) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.19418 if `cond1'
replace energycharge_sum_m_pk = 0.07755 if `cond1'
replace energycharge_sum_off_pk = 0.02989 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.06507 if `cond1'
replace energycharge_win_off_pk= 0.02817 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00463 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00463 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00463 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00463 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00463 if `cond1'
replace servicecharge = 0.0 if `cond1'
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01724 if `cond2'
replace energycharge_sum_m_pk = 0.01724 if `cond2'
replace energycharge_sum_off_pk = 0.01724 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01724 if `cond2'
replace energycharge_win_off_pk= 0.01724 if `cond2'
replace customercharge = 126.47 if `cond2'
replace servicecharge = 6.05 if `cond2'
replace voltage_dis_load = 0.07 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.28 if `cond2' & voltage_cat==2
replace voltage_dis_load = 2.31 if `cond2' & voltage_cat==3
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Jun 1 2012

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-A" if missing(old)
replace rate_start_date = mdy(6,1,2012) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.19976 if `cond1'
replace energycharge_sum_m_pk = 0.07970 if `cond1'
replace energycharge_sum_off_pk = 0.03063 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.06685 if `cond1'
replace energycharge_win_off_pk= 0.02886 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00463 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00463 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00463 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00463 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00463 if `cond1'
replace servicecharge = 0.0 if `cond1'
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01720 if `cond2'
replace energycharge_sum_m_pk = 0.01720 if `cond2'
replace energycharge_sum_off_pk = 0.01720 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01720 if `cond2'
replace energycharge_win_off_pk= 0.01720 if `cond2'
replace customercharge = 123.84 if `cond2'
replace servicecharge = 5.85 if `cond2'
replace voltage_dis_load = 0.07 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.28 if `cond2' & voltage_cat==2
replace voltage_dis_load = 2.31 if `cond2' & voltage_cat==3
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Jan 1 2012

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-A" if missing(old)
replace rate_start_date = mdy(1,1,2012) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.19976 if `cond1'
replace energycharge_sum_m_pk = 0.07970 if `cond1'
replace energycharge_sum_off_pk = 0.03063 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.06685 if `cond1'
replace energycharge_win_off_pk= 0.02886 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00593 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00593 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00593 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00593 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00593 if `cond1'
replace servicecharge = 0.0 if `cond1'
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01716 if `cond2'
replace energycharge_sum_m_pk = 0.01716 if `cond2'
replace energycharge_sum_off_pk = 0.01716 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01716 if `cond2'
replace energycharge_win_off_pk= 0.01716 if `cond2'
replace customercharge = 123.84 if `cond2'
replace servicecharge = 5.85 if `cond2'
replace voltage_dis_load = 0.07 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.28 if `cond2' & voltage_cat==2
replace voltage_dis_load = 2.31 if `cond2' & voltage_cat==3
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Apr 12 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-A" if missing(old)
replace rate_start_date = mdy(4,12,2013) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.21709 if `cond1'
replace energycharge_sum_m_pk = 0.08411 if `cond1'
replace energycharge_sum_off_pk = 0.02977 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.06685 if `cond1'
replace energycharge_win_off_pk= 0.02781 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00097 if `cond1'
replace servicecharge = 0.0 if `cond1'
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01691 if `cond2'
replace energycharge_sum_m_pk = 0.01691 if `cond2'
replace energycharge_sum_off_pk = 0.01691 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01691 if `cond2'
replace energycharge_win_off_pk= 0.01691 if `cond2'
replace customercharge = 134.42 if `cond2'
replace servicecharge = 6.64 if `cond2'
replace voltage_dis_load = 0.07 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.28 if `cond2' & voltage_cat==2
replace voltage_dis_load = 2.31 if `cond2' & voltage_cat==3
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Jan 1 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-A" if missing(old)
replace rate_start_date = mdy(4,12,2013) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.21709 if `cond1'
replace energycharge_sum_m_pk = 0.08411 if `cond1'
replace energycharge_sum_off_pk = 0.02977 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.06988 if `cond1'
replace energycharge_win_off_pk= 0.02781 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00097 if `cond1'
replace servicecharge = 0.0 if `cond1'
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01691 if `cond2'
replace energycharge_sum_m_pk = 0.01691 if `cond2'
replace energycharge_sum_off_pk = 0.01691 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01691 if `cond2'
replace energycharge_win_off_pk= 0.01691 if `cond2'
replace customercharge = 134.42 if `cond2'
replace servicecharge = 6.64 if `cond2'
replace voltage_dis_load = 0.07 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.28 if `cond2' & voltage_cat==2
replace voltage_dis_load = 2.31 if `cond2' & voltage_cat==3
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Apr 1 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-A" if missing(old)
replace rate_start_date = mdy(4,12,2013) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.21709 if `cond1'
replace energycharge_sum_m_pk = 0.08411 if `cond1'
replace energycharge_sum_off_pk = 0.02977 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.06988 if `cond1'
replace energycharge_win_off_pk= 0.02781 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00097 if `cond1'
replace servicecharge = 0.0 if `cond1'
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01708 if `cond2'
replace energycharge_sum_m_pk = 0.01708 if `cond2'
replace energycharge_sum_off_pk = 0.01708 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01708 if `cond2'
replace energycharge_win_off_pk= 0.01708 if `cond2'
replace customercharge = 134.42 if `cond2'
replace servicecharge = 6.64 if `cond2'
replace voltage_dis_load = 0.07 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.28 if `cond2' & voltage_cat==2
replace voltage_dis_load = 2.31 if `cond2' & voltage_cat==3
replace pf_adjust = 0.51 if `cond2' & voltage_cat==1
replace pf_adjust = 0.34 if `cond2' & voltage_cat==2
replace pf_adjust = 0.34 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)



** TOU-PA-B
gen demandcharge_sum_on_pk= 0
gen demandcharge_sum_mid_pk= 0
gen demandcharge_sum_off_pk= 0

// Oct 1 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-B" if missing(old)
replace rate_start_date = mdy(10,1,2009) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.11163 if `cond1'
replace energycharge_sum_m_pk = 0.06413 if `cond1'
replace energycharge_sum_off_pk = 0.03827 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.06357 if `cond1'
replace energycharge_win_off_pk= 0.03618 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.06225 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.06225 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.06225 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.06225 if `cond1'
replace energycharge_dwr_win_off_pk= 0.06225 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 10.60 if `cond1'
replace demandcharge_sum_mid_pk = 2.53 if `cond1'
replace demandcharge_sum_off_pk = -999 if `cond1'
replace voltage_dis_load_2 = 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2 = 0.54 if `cond1' & voltage_cat==2
replace voltage_dis_load_2 = 0.55 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01207 if `cond2'
replace energycharge_sum_m_pk = 0.01207 if `cond2'
replace energycharge_sum_off_pk = 0.01207 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01207 if `cond2'
replace energycharge_win_off_pk= 0.01207 if `cond2'
replace customercharge = 113.76 if `cond2'
replace servicecharge = 6.82 if `cond2'
replace voltage_dis_load_1 = 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.08 if `cond2' & voltage_cat==3
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Jun 1 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-B" if missing(old)
replace rate_start_date = mdy(6,1,2009) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.11398 if `cond1'
replace energycharge_sum_m_pk = 0.08932 if `cond1'
replace energycharge_sum_off_pk = 0.02359 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.10463 if `cond1'
replace energycharge_win_off_pk= 0.02359 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.06225 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.06225 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.06225 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.06225 if `cond1'
replace energycharge_dwr_win_off_pk= 0.06225 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 9.75 if `cond1'
replace demandcharge_sum_mid_pk = 0.00 if `cond1'
replace demandcharge_sum_off_pk = -999 if `cond1'
replace voltage_dis_load_2 = 0.38 if `cond1' & voltage_cat==1
replace voltage_dis_load_2 = 1.04 if `cond1' & voltage_cat==2
replace voltage_dis_load_2 = 1.04 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00198 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00431 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00431 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01081 if `cond2'
replace energycharge_sum_m_pk = 0.01081 if `cond2'
replace energycharge_sum_off_pk = 0.01081 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01081 if `cond2'
replace energycharge_win_off_pk= 0.01081 if `cond2'
replace customercharge = 94.80 if `cond2'
replace servicecharge = 6.01 if `cond2'
replace voltage_dis_load_1 = 0.19 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 6.25 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.25 if `cond2' & voltage_cat==3
replace pf_adjust = 0.18 if `cond2' & voltage_cat==1
replace pf_adjust = 0.20 if `cond2' & voltage_cat==2
replace pf_adjust = 0.20 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Apr 4 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-B" if missing(old)
replace rate_start_date = mdy(4,4,2009) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.11589 if `cond1'
replace energycharge_sum_m_pk = 0.09092 if `cond1'
replace energycharge_sum_off_pk = 0.02429 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.10642 if `cond1'
replace energycharge_win_off_pk= 0.02429 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.06210 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.06210 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.06210 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.06210 if `cond1'
replace energycharge_dwr_win_off_pk= 0.06210 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 9.88 if `cond1'
replace demandcharge_sum_mid_pk = 0.00 if `cond1'
replace demandcharge_sum_off_pk = -999 if `cond1'
replace voltage_dis_load_2 = 0.39 if `cond1' & voltage_cat==1
replace voltage_dis_load_2 = 1.05 if `cond1' & voltage_cat==2
replace voltage_dis_load_2 = 1.05 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00201 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00437 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00437 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01042 if `cond2'
replace energycharge_sum_m_pk = 0.01042 if `cond2'
replace energycharge_sum_off_pk = 0.01042 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01042 if `cond2'
replace energycharge_win_off_pk= 0.01042 if `cond2'
replace customercharge = 94.80 if `cond2'
replace servicecharge = 6.01 if `cond2'
replace voltage_dis_load_1 = 0.19 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 6.25 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.25 if `cond2' & voltage_cat==3
replace pf_adjust = 0.18 if `cond2' & voltage_cat==1
replace pf_adjust = 0.20 if `cond2' & voltage_cat==2
replace pf_adjust = 0.20 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Mar 1 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-B" if missing(old)
replace rate_start_date = mdy(3,1,2009) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.11409 if `cond1'
replace energycharge_sum_m_pk = 0.08942 if `cond1'
replace energycharge_sum_off_pk = 0.02361 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.10473 if `cond1'
replace energycharge_win_off_pk= 0.02361 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.06231 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.06231 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.06231 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.06231 if `cond1'
replace energycharge_dwr_win_off_pk= 0.06231 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 9.76 if `cond1'
replace demandcharge_sum_mid_pk = 0.00 if `cond1'
replace demandcharge_sum_off_pk = -999 if `cond1'
replace voltage_dis_load_2 = 0.39 if `cond1' & voltage_cat==1
replace voltage_dis_load_2 = 1.04 if `cond1' & voltage_cat==2
replace voltage_dis_load_2 = 1.04 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00199 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00432 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00432 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01063 if `cond2'
replace energycharge_sum_m_pk = 0.01063 if `cond2'
replace energycharge_sum_off_pk = 0.01063 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01063 if `cond2'
replace energycharge_win_off_pk= 0.01063 if `cond2'
replace customercharge = 81.68 if `cond2'
replace servicecharge = 5.31 if `cond2'
replace voltage_dis_load_1 = 0.19 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 6.25 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.25 if `cond2' & voltage_cat==3
replace pf_adjust = 0.18 if `cond2' & voltage_cat==1
replace pf_adjust = 0.20 if `cond2' & voltage_cat==2
replace pf_adjust = 0.20 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Jan 1 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-B" if missing(old)
replace rate_start_date = mdy(1,1,2009) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.10520 if `cond1'
replace energycharge_sum_m_pk = 0.08073 if `cond1'
replace energycharge_sum_off_pk = 0.01540 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.09592 if `cond1'
replace energycharge_win_off_pk= 0.01540 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.08451 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.08451 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.08451 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.08451 if `cond1'
replace energycharge_dwr_win_off_pk= 0.08451 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 9.77 if `cond1'
replace demandcharge_sum_mid_pk = 0.00 if `cond1'
replace demandcharge_sum_off_pk = -999 if `cond1'
replace voltage_dis_load_2 = 0.39 if `cond1' & voltage_cat==1
replace voltage_dis_load_2 = 1.04 if `cond1' & voltage_cat==2
replace voltage_dis_load_2 = 1.04 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00199 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00432 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00432 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01349 if `cond2'
replace energycharge_sum_m_pk = 0.01349 if `cond2'
replace energycharge_sum_off_pk = 0.01349 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01349 if `cond2'
replace energycharge_win_off_pk= 0.01349 if `cond2'
replace customercharge = 83.73 if `cond2'
replace servicecharge = 5.00 if `cond2'
replace voltage_dis_load_1 = 0.19 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 6.25 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.25 if `cond2' & voltage_cat==3
replace pf_adjust = 0.18 if `cond2' & voltage_cat==1
replace pf_adjust = 0.20 if `cond2' & voltage_cat==2
replace pf_adjust = 0.20 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Jun 1 2010

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-B" if missing(old)
replace rate_start_date = mdy(6,1,2010) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.10745 if `cond1'
replace energycharge_sum_m_pk = 0.06150 if `cond1'
replace energycharge_sum_off_pk = 0.03648 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.06096 if `cond1'
replace energycharge_win_off_pk= 0.03446 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.03763 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.03763 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.03763 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.03763 if `cond1'
replace energycharge_dwr_win_off_pk= 0.03763 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 10.25 if `cond1'
replace demandcharge_sum_mid_pk = 2.45 if `cond1'
replace demandcharge_sum_off_pk = -999 if `cond1'
replace voltage_dis_load_2 = 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2 = 0.54 if `cond1' & voltage_cat==2
replace voltage_dis_load_2 = 0.55 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01775 if `cond2'
replace energycharge_sum_m_pk = 0.01775 if `cond2'
replace energycharge_sum_off_pk = 0.01775 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01775 if `cond2'
replace energycharge_win_off_pk= 0.01775 if `cond2'
replace customercharge = 122.94 if `cond2'
replace demandcharge = 7.83 if `cond2'
replace voltage_dis_load_1 = 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.08 if `cond2' & voltage_cat==3
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Mar 1 2010

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-B" if missing(old)
replace rate_start_date = mdy(3,1,2010) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.10745 if `cond1'
replace energycharge_sum_m_pk = 0.06150 if `cond1'
replace energycharge_sum_off_pk = 0.03648 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.06096 if `cond1'
replace energycharge_win_off_pk= 0.03446 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.03763 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.03763 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.03763 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.03763 if `cond1'
replace energycharge_dwr_win_off_pk= 0.03763 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 10.25 if `cond1'
replace demandcharge_sum_mid_pk = 2.45 if `cond1'
replace demandcharge_sum_off_pk = -999 if `cond1'
replace voltage_dis_load_2 = 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2 = 0.54 if `cond1' & voltage_cat==2
replace voltage_dis_load_2 = 0.55 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01829 if `cond2'
replace energycharge_sum_m_pk = 0.01829 if `cond2'
replace energycharge_sum_off_pk = 0.01829 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01829 if `cond2'
replace energycharge_win_off_pk= 0.01829 if `cond2'
replace customercharge = 122.94 if `cond2'
replace demandcharge = 7.81 if `cond2'
replace voltage_dis_load_1 = 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.08 if `cond2' & voltage_cat==3
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Jan 1 2010

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-B" if missing(old)
replace rate_start_date = mdy(1,1,2010) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.11163 if `cond1'
replace energycharge_sum_m_pk = 0.06413 if `cond1'
replace energycharge_sum_off_pk = 0.03827 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.06357 if `cond1'
replace energycharge_win_off_pk= 0.03618 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.03763 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.03763 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.03763 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.03763 if `cond1'
replace energycharge_dwr_win_off_pk= 0.03763 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 10.60 if `cond1'
replace demandcharge_sum_mid_pk = 2.53 if `cond1'
replace demandcharge_sum_off_pk = -999 if `cond1'
replace voltage_dis_load_2 = 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2 = 0.54 if `cond1' & voltage_cat==2
replace voltage_dis_load_2 = 0.55 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01294 if `cond2'
replace energycharge_sum_m_pk = 0.01294 if `cond2'
replace energycharge_sum_off_pk = 0.01294 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01294 if `cond2'
replace energycharge_win_off_pk= 0.01294 if `cond2'
replace customercharge = 113.76 if `cond2'
replace demandcharge = 6.83 if `cond2'
replace voltage_dis_load_1 = 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.08 if `cond2' & voltage_cat==3
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Jun 1 2011

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-B" if missing(old)
replace rate_start_date = mdy(6,1,2011) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.09280 if `cond1'
replace energycharge_sum_m_pk = 0.05256 if `cond1'
replace energycharge_sum_off_pk = 0.03064 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.05208 if `cond1'
replace energycharge_win_off_pk= 0.02887 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.03952 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.03952 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.03952 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.03952 if `cond1'
replace energycharge_dwr_win_off_pk= 0.03952 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 8.98 if `cond1'
replace demandcharge_sum_mid_pk = 2.15 if `cond1'
replace demandcharge_sum_off_pk = -999 if `cond1'
replace voltage_dis_load_2 = 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2 = 0.54 if `cond1' & voltage_cat==2
replace voltage_dis_load_2 = 0.55 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01727 if `cond2'
replace energycharge_sum_m_pk = 0.01727 if `cond2'
replace energycharge_sum_off_pk = 0.01727 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01727 if `cond2'
replace energycharge_win_off_pk= 0.01727 if `cond2'
replace customercharge = 123.84 if `cond2'
replace demandcharge = 7.87 if `cond2'
replace voltage_dis_load_1 = 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.08 if `cond2' & voltage_cat==3
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Mar 1 2011

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-B" if missing(old)
replace rate_start_date = mdy(3,1,2011) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.10745 if `cond1'
replace energycharge_sum_m_pk = 0.06150 if `cond1'
replace energycharge_sum_off_pk = 0.03648 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.06096 if `cond1'
replace energycharge_win_off_pk= 0.03446 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.03952 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.03952 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.03952 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.03952 if `cond1'
replace energycharge_dwr_win_off_pk= 0.03952 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 10.25 if `cond1'
replace demandcharge_sum_mid_pk = 2.45 if `cond1'
replace demandcharge_sum_off_pk = -999 if `cond1'
replace voltage_dis_load_2 = 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2 = 0.54 if `cond1' & voltage_cat==2
replace voltage_dis_load_2 = 0.55 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01800 if `cond2'
replace energycharge_sum_m_pk = 0.01800 if `cond2'
replace energycharge_sum_off_pk = 0.01800 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01800 if `cond2'
replace energycharge_win_off_pk= 0.01800 if `cond2'
replace customercharge = 122.94 if `cond2'
replace demandcharge = 7.90 if `cond2'
replace voltage_dis_load_1 = 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.08 if `cond2' & voltage_cat==3
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Jan 1 2011

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-B" if missing(old)
replace rate_start_date = mdy(1,1,2011) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.10745 if `cond1'
replace energycharge_sum_m_pk = 0.06150 if `cond1'
replace energycharge_sum_off_pk = 0.03648 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.03952 if `cond1'
replace energycharge_win_off_pk= 0.03952 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.03952 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.03952 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.03952 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.03952 if `cond1'
replace energycharge_dwr_win_off_pk= 0.03952 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 10.25 if `cond1'
replace demandcharge_sum_mid_pk = 2.45 if `cond1'
replace demandcharge_sum_off_pk = -999 if `cond1'
replace voltage_dis_load_2 = 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2 = 0.54 if `cond1' & voltage_cat==2
replace voltage_dis_load_2 = 0.55 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01800 if `cond2'
replace energycharge_sum_m_pk = 0.01800 if `cond2'
replace energycharge_sum_off_pk = 0.01800 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01800 if `cond2'
replace energycharge_win_off_pk= 0.01800 if `cond2'
replace customercharge = 122.94 if `cond2'
replace demandcharge = 7.83 if `cond2'
replace voltage_dis_load_1 = 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.08 if `cond2' & voltage_cat==3
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Oct 1 2012

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-B" if missing(old)
replace rate_start_date = mdy(10,1,2012) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.09028 if `cond1'
replace energycharge_sum_m_pk = 0.05119 if `cond1'
replace energycharge_sum_off_pk = 0.02990 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.05072 if `cond1'
replace energycharge_win_off_pk= 0.02818 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00463 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00463 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00463 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00463 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00463 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 8.72 if `cond1'
replace demandcharge_sum_mid_pk = 2.09 if `cond1'
replace demandcharge_sum_off_pk = -999 if `cond1'
replace voltage_dis_load_2 = 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2 = 0.54 if `cond1' & voltage_cat==2
replace voltage_dis_load_2 = 0.55 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01729 if `cond2'
replace energycharge_sum_m_pk = 0.01729 if `cond2'
replace energycharge_sum_off_pk = 0.01729 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01729 if `cond2'
replace energycharge_win_off_pk= 0.01729 if `cond2'
replace customercharge = 126.47 if `cond2'
replace demandcharge = 8.40 if `cond2'
replace voltage_dis_load_1 = 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.08 if `cond2' & voltage_cat==3
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Aug 1 2012

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-B" if missing(old)
replace rate_start_date = mdy(8,1,2012) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.09028 if `cond1'
replace energycharge_sum_m_pk = 0.05119 if `cond1'
replace energycharge_sum_off_pk = 0.02990 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.05072 if `cond1'
replace energycharge_win_off_pk= 0.02818 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00463 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00463 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00463 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00463 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00463 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 8.72 if `cond1'
replace demandcharge_sum_mid_pk = 2.09 if `cond1'
replace demandcharge_sum_off_pk = -999 if `cond1'
replace voltage_dis_load_2 = 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2 = 0.54 if `cond1' & voltage_cat==2
replace voltage_dis_load_2 = 0.55 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01724 if `cond2'
replace energycharge_sum_m_pk = 0.01724 if `cond2'
replace energycharge_sum_off_pk = 0.01724 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01724 if `cond2'
replace energycharge_win_off_pk= 0.01724 if `cond2'
replace customercharge = 126.47 if `cond2'
replace demandcharge = 8.07 if `cond2'
replace voltage_dis_load_1 = 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.08 if `cond2' & voltage_cat==3
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Jun 1 2012

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-B" if missing(old)
replace rate_start_date = mdy(6,1,2012) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.09028 if `cond1'
replace energycharge_sum_m_pk = 0.05256 if `cond1'
replace energycharge_sum_off_pk = 0.03064 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.05208 if `cond1'
replace energycharge_win_off_pk= 0.02887 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00463 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00463 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00463 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00463 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00463 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 8.98 if `cond1'
replace demandcharge_sum_mid_pk = 2.15 if `cond1'
replace demandcharge_sum_off_pk = -999 if `cond1'
replace voltage_dis_load_2 = 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2 = 0.54 if `cond1' & voltage_cat==2
replace voltage_dis_load_2 = 0.55 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01720 if `cond2'
replace energycharge_sum_m_pk = 0.01720 if `cond2'
replace energycharge_sum_off_pk = 0.01720 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01720 if `cond2'
replace energycharge_win_off_pk= 0.01720 if `cond2'
replace customercharge = 123.84 if `cond2'
replace demandcharge = 7.80 if `cond2'
replace voltage_dis_load_1 = 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.08 if `cond2' & voltage_cat==3
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Jan 1 2012

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-B" if missing(old)
replace rate_start_date = mdy(6,1,2012) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.09028 if `cond1'
replace energycharge_sum_m_pk = 0.05256 if `cond1'
replace energycharge_sum_off_pk = 0.03064 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.05208 if `cond1'
replace energycharge_win_off_pk= 0.02887 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00593 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00593 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00593 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00593 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00593 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 8.98 if `cond1'
replace demandcharge_sum_mid_pk = 2.15 if `cond1'
replace demandcharge_sum_off_pk = -999 if `cond1'
replace voltage_dis_load_2 = 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2 = 0.54 if `cond1' & voltage_cat==2
replace voltage_dis_load_2 = 0.55 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01716 if `cond2'
replace energycharge_sum_m_pk = 0.01716 if `cond2'
replace energycharge_sum_off_pk = 0.01716 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01716 if `cond2'
replace energycharge_win_off_pk= 0.01716 if `cond2'
replace customercharge = 123.84 if `cond2'
replace demandcharge = 7.80 if `cond2'
replace voltage_dis_load_1 = 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.08 if `cond2' & voltage_cat==3
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Apr 1 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-B" if missing(old)
replace rate_start_date = mdy(4,1,2013) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.09862 if `cond1'
replace energycharge_sum_m_pk = 0.05406 if `cond1'
replace energycharge_sum_off_pk = 0.02978 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.05352 if `cond1'
replace energycharge_win_off_pk= 0.02782 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00097 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 9.94 if `cond1'
replace demandcharge_sum_mid_pk = 2.38 if `cond1'
replace demandcharge_sum_off_pk = -999 if `cond1'
replace voltage_dis_load_2 = 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2 = 0.54 if `cond1' & voltage_cat==2
replace voltage_dis_load_2 = 0.55 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01708 if `cond2'
replace energycharge_sum_m_pk = 0.01708 if `cond2'
replace energycharge_sum_off_pk = 0.01708 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01708 if `cond2'
replace energycharge_win_off_pk= 0.01708 if `cond2'
replace customercharge = 134.42 if `cond2'
replace demandcharge = 8.83 if `cond2'
replace voltage_dis_load_1 = 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.08 if `cond2' & voltage_cat==3
replace pf_adjust = 0.51 if `cond2' & voltage_cat==1
replace pf_adjust = 0.34 if `cond2' & voltage_cat==2
replace pf_adjust = 0.34 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Jan 1 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-B" if missing(old)
replace rate_start_date = mdy(1,1,2013) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.09862 if `cond1'
replace energycharge_sum_m_pk = 0.05406 if `cond1'
replace energycharge_sum_off_pk = 0.02978 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.05352 if `cond1'
replace energycharge_win_off_pk= 0.02782 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00097 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 9.94 if `cond1'
replace demandcharge_sum_mid_pk = 2.38 if `cond1'
replace demandcharge_sum_off_pk = -999 if `cond1'
replace voltage_dis_load_2 = 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2 = 0.54 if `cond1' & voltage_cat==2
replace voltage_dis_load_2 = 0.55 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01691 if `cond2'
replace energycharge_sum_m_pk = 0.01691 if `cond2'
replace energycharge_sum_off_pk = 0.01691 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01691 if `cond2'
replace energycharge_win_off_pk= 0.01691 if `cond2'
replace customercharge = 134.42 if `cond2'
replace demandcharge = 8.83 if `cond2'
replace voltage_dis_load_1 = 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.08 if `cond2' & voltage_cat==3
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

***************************
*** 5. Schedule- TOU-PA2 ***
***************************

********* Rate A

// Nov 22 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2A" if missing(old)
replace rate_start_date = mdy(11,22,2013) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.32478 if `cond1'
replace energycharge_sum_m_pk = 0.09822 if `cond1'
replace energycharge_sum_off_pk = 0.03727 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.06735 if `cond1'
replace energycharge_win_off_pk= 0.04338 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00095 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00095 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00095 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00095 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00095 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace wind_mach_credit = 6.12 if `cond1'
replace voltage_dis_energy = 0.00152 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00372 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00376 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02119 if `cond2'
replace energycharge_sum_m_pk = 0.02119 if `cond2'
replace energycharge_sum_off_pk = 0.02119 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02119 if `cond2'
replace energycharge_win_off_pk= 0.02119 if `cond2'
replace customercharge = 40.88 if `cond2'
replace demandcharge = 8.11 if `cond2'
replace voltage_dis_load = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.60 if `cond2' & voltage_cat==2
replace voltage_dis_load = 7.17 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Oct 1 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2A" if missing(old)
replace rate_start_date = mdy(10,1,2013) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.31871 if `cond1'
replace energycharge_sum_m_pk = 0.09639 if `cond1'
replace energycharge_sum_off_pk = 0.03660 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.06610 if `cond1'
replace energycharge_win_off_pk= 0.04259 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00097 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace wind_mach_credit = 6.12 if `cond1'
replace voltage_dis_energy = 0.00153 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00369 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00373 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02028 if `cond2'
replace energycharge_sum_m_pk = 0.02028 if `cond2'
replace energycharge_sum_off_pk = 0.02028 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02028 if `cond2'
replace energycharge_win_off_pk= 0.02028 if `cond2'
replace customercharge = 39.50 if `cond2'
replace demandcharge = 7.87 if `cond2'
replace voltage_dis_load = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.54 if `cond2' & voltage_cat==2
replace voltage_dis_load = 6.93 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Jun 1 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2A" if missing(old)
replace rate_start_date = mdy(10,1,2013) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.31871 if `cond1'
replace energycharge_sum_m_pk = 0.09639 if `cond1'
replace energycharge_sum_off_pk = 0.03660 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.06610 if `cond1'
replace energycharge_win_off_pk= 0.04259 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00097 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace wind_mach_credit = 6.00 if `cond1'
replace voltage_dis_energy = 0.00153 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00369 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00373 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02036 if `cond2'
replace energycharge_sum_m_pk = 0.02036 if `cond2'
replace energycharge_sum_off_pk = 0.02036 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02036 if `cond2'
replace energycharge_win_off_pk= 0.02036 if `cond2'
replace customercharge = 39.50 if `cond2'
replace demandcharge = 8.00 if `cond2'
replace voltage_dis_load = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.54 if `cond2' & voltage_cat==2
replace voltage_dis_load = 6.93 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

// Apr 1 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2A" if missing(old)
replace rate_start_date = mdy(4,1,2013) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.31901 if `cond1'
replace energycharge_sum_m_pk = 0.09645 if `cond1'
replace energycharge_sum_off_pk = 0.03664 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.06617 if `cond1'
replace energycharge_win_off_pk= 0.04263 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00097 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace wind_mach_credit = 6.01 if `cond1'
replace voltage_dis_energy = 0.00153 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00369 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00373 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01962 if `cond2'
replace energycharge_sum_m_pk = 0.01962 if `cond2'
replace energycharge_sum_off_pk = 0.01962 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01962 if `cond2'
replace energycharge_win_off_pk= 0.01962 if `cond2'
replace customercharge = 39.50 if `cond2'
replace demandcharge = 8.01 if `cond2'
replace voltage_dis_load = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.54 if `cond2' & voltage_cat==2
replace voltage_dis_load = 6.94 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Nov 22 2014

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2A" if missing(old)
replace rate_start_date = mdy(11,22,2014) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.39796 if `cond1'
replace energycharge_sum_m_pk = 0.11970 if `cond1'
replace energycharge_sum_off_pk = 0.04496 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.08189 if `cond1'
replace energycharge_win_off_pk= 0.05246 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00037 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00037 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00037 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00037 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00037 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace wind_mach_credit = 6.00 if `cond1'
replace voltage_dis_energy = 0.00170 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00420 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00424 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02318 if `cond2'
replace energycharge_sum_m_pk = 0.02318 if `cond2'
replace energycharge_sum_off_pk = 0.02318 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02318 if `cond2'
replace energycharge_win_off_pk= 0.02318 if `cond2'
replace customercharge = 40.56 if `cond2'
replace demandcharge = 8.34 if `cond2'
replace voltage_dis_load = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.54 if `cond2' & voltage_cat==2
replace voltage_dis_load = 7.11 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00731

replace old= 0 if missing(old)


// Aug 11 2014

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2A" if missing(old)
replace rate_start_date = mdy(8,11,2014) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.39796 if `cond1'
replace energycharge_sum_m_pk = 0.11970 if `cond1'
replace energycharge_sum_off_pk = 0.04496 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.08189 if `cond1'
replace energycharge_win_off_pk= 0.05246 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00037 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00037 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00037 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00037 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00037 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace wind_mach_credit = 6.00 if `cond1'
replace voltage_dis_energy = 0.00170 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00420 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00424 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02318 if `cond2'
replace energycharge_sum_m_pk = 0.02318 if `cond2'
replace energycharge_sum_off_pk = 0.02318 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02318 if `cond2'
replace energycharge_win_off_pk= 0.02318 if `cond2'
replace customercharge = 40.56 if `cond2'
replace demandcharge = 8.34 if `cond2'
replace voltage_dis_load = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.54 if `cond2' & voltage_cat==2
replace voltage_dis_load = 7.11 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00731

replace old= 0 if missing(old)


// Jul 7 2014

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2A" if missing(old)
replace rate_start_date = mdy(7,7,2014) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.40049 if `cond1'
replace energycharge_sum_m_pk = 0.12054 if `cond1'
replace energycharge_sum_off_pk = 0.04525 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.08242 if `cond1'
replace energycharge_win_off_pk= 0.05280 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00037 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00037 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00037 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00037 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00037 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace wind_mach_credit = 6.00 if `cond1'
replace voltage_dis_energy = 0.00171 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00422 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00426 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02308 if `cond2'
replace energycharge_sum_m_pk = 0.02308 if `cond2'
replace energycharge_sum_off_pk = 0.02308 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02308 if `cond2'
replace energycharge_win_off_pk= 0.02308 if `cond2'
replace customercharge = 40.56 if `cond2'
replace demandcharge = 8.34 if `cond2'
replace voltage_dis_load = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.54 if `cond2' & voltage_cat==2
replace voltage_dis_load = 7.11 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00681

replace old= 0 if missing(old)


// June 1 2014

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2A" if missing(old)
replace rate_start_date = mdy(6,1,2014) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.40049 if `cond1'
replace energycharge_sum_m_pk = 0.12054 if `cond1'
replace energycharge_sum_off_pk = 0.04525 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.08242 if `cond1'
replace energycharge_win_off_pk= 0.05280 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00037 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00037 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00037 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00037 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00037 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace wind_mach_credit = 6.00 if `cond1'
replace voltage_dis_energy = 0.00171 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00422 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00426 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02244 if `cond2'
replace energycharge_sum_m_pk = 0.02244 if `cond2'
replace energycharge_sum_off_pk = 0.02244 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02244 if `cond2'
replace energycharge_win_off_pk= 0.02244 if `cond2'
replace customercharge = 40.56 if `cond2'
replace demandcharge = 8.34 if `cond2'
replace voltage_dis_load = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.54 if `cond2' & voltage_cat==2
replace voltage_dis_load = 7.11 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00681

replace old= 0 if missing(old)


// Apr 1 2014

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2A" if missing(old)
replace rate_start_date = mdy(4,1,2014) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.35096 if `cond1'
replace energycharge_sum_m_pk = 0.10570 if `cond1'
replace energycharge_sum_off_pk = 0.03970 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.07228 if `cond1'
replace energycharge_win_off_pk= 0.04632 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00037 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00037 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00037 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00037 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00037 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace wind_mach_credit = 6.00 if `cond1'
replace voltage_dis_energy = 0.00160 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00394 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00398 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02130 if `cond2'
replace energycharge_sum_m_pk = 0.02130 if `cond2'
replace energycharge_sum_off_pk = 0.02130 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02130 if `cond2'
replace energycharge_win_off_pk= 0.02130 if `cond2'
replace customercharge = 41.76 if `cond2'
replace demandcharge = 8.55 if `cond2'
replace voltage_dis_load = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.62 if `cond2' & voltage_cat==2
replace voltage_dis_load = 7.32 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00681

replace old= 0 if missing(old)


// Jan 1 2014

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2A" if missing(old)
replace rate_start_date = mdy(1,1,2014) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.31927 if `cond1'
replace energycharge_sum_m_pk = 0.09615 if `cond1'
replace energycharge_sum_off_pk = 0.03614 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.06577 if `cond1'
replace energycharge_win_off_pk= 0.04216 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00037 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00037 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00037 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00037 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00037 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace wind_mach_credit = 6.00 if `cond1'
replace voltage_dis_energy = 0.00145 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00358 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00362 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02130 if `cond2'
replace energycharge_sum_m_pk = 0.02130 if `cond2'
replace energycharge_sum_off_pk = 0.02130 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02130 if `cond2'
replace energycharge_win_off_pk= 0.02130 if `cond2'
replace customercharge = 41.76 if `cond2'
replace demandcharge = 8.55 if `cond2'
replace voltage_dis_load = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.62 if `cond2' & voltage_cat==2
replace voltage_dis_load = 7.32 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Nov 24 2015

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2A" if missing(old)
replace rate_start_date = mdy(11,24,2015) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.35084 if `cond1'
replace energycharge_sum_m_pk = 0.10638 if `cond1'
replace energycharge_sum_off_pk = 0.04087 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.07330 if `cond1'
replace energycharge_win_off_pk= 0.04746 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00172 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00172 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00172 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00172 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00172 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace wind_mach_credit = 6.00 if `cond1'
replace voltage_dis_energy = 0.00155 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00377 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00381 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02414 if `cond2'
replace energycharge_sum_m_pk = 0.02414 if `cond2'
replace energycharge_sum_off_pk = 0.02414 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02414 if `cond2'
replace energycharge_win_off_pk= 0.02414 if `cond2'
replace customercharge = 41.49 if `cond2'
replace demandcharge = 8.91 if `cond2'
replace voltage_dis_load = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.60 if `cond2' & voltage_cat==2
replace voltage_dis_load = 7.27 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00680

replace old= 0 if missing(old)


// Oct 1 2015

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2A" if missing(old)
replace rate_start_date = mdy(10,1,2015) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.35084 if `cond1'
replace energycharge_sum_m_pk = 0.10638 if `cond1'
replace energycharge_sum_off_pk = 0.04087 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.07330 if `cond1'
replace energycharge_win_off_pk= 0.04746 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00172 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00172 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00172 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00172 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00172 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace wind_mach_credit = 6.00 if `cond1'
replace voltage_dis_energy = 0.00155 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00377 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00381 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02414 if `cond2'
replace energycharge_sum_m_pk = 0.02414 if `cond2'
replace energycharge_sum_off_pk = 0.02414 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02414 if `cond2'
replace energycharge_win_off_pk= 0.02414 if `cond2'
replace customercharge = 41.49 if `cond2'
replace demandcharge = 8.91 if `cond2'
replace voltage_dis_load = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.60 if `cond2' & voltage_cat==2
replace voltage_dis_load = 7.27 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00680

replace old= 0 if missing(old)


// Jun 1 2015

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2A" if missing(old)
replace rate_start_date = mdy(6,1,2015) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.36187 if `cond1'
replace energycharge_sum_m_pk = 0.10974 if `cond1'
replace energycharge_sum_off_pk = 0.04211 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.07557 if `cond1'
replace energycharge_win_off_pk= 0.04891 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00172 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00172 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00172 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00172 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00172 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace wind_mach_credit = 6.00 if `cond1'
replace voltage_dis_energy = 0.00157 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00384 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00388 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02421 if `cond2'
replace energycharge_sum_m_pk = 0.02421 if `cond2'
replace energycharge_sum_off_pk = 0.02421 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02421 if `cond2'
replace energycharge_win_off_pk= 0.02421 if `cond2'
replace customercharge = 41.49 if `cond2'
replace demandcharge = 8.91 if `cond2'
replace voltage_dis_load = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.60 if `cond2' & voltage_cat==2
replace voltage_dis_load = 7.27 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00680

replace old= 0 if missing(old)


// Mar 2 2015

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2A" if missing(old)
replace rate_start_date = mdy(3,2,2015) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.36187 if `cond1'
replace energycharge_sum_m_pk = 0.10974 if `cond1'
replace energycharge_sum_off_pk = 0.04211 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.07557 if `cond1'
replace energycharge_win_off_pk= 0.04891 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00172 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00172 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00172 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00172 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00172 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace wind_mach_credit = 6.00 if `cond1'
replace voltage_dis_energy = 0.00157 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00384 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00388 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02376 if `cond2'
replace energycharge_sum_m_pk = 0.02376 if `cond2'
replace energycharge_sum_off_pk = 0.02376 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02376 if `cond2'
replace energycharge_win_off_pk= 0.02376 if `cond2'
replace customercharge = 41.49 if `cond2'
replace demandcharge = 8.91 if `cond2'
replace voltage_dis_load = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.60 if `cond2' & voltage_cat==2
replace voltage_dis_load = 7.27 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00680

replace old= 0 if missing(old)


// Jan 1 2015

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2A" if missing(old)
replace rate_start_date = mdy(1,1,2015) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.36789 if `cond1'
replace energycharge_sum_m_pk = 0.11155 if `cond1'
replace energycharge_sum_off_pk = 0.04278 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.07679 if `cond1'
replace energycharge_win_off_pk= 0.04969 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00172 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00172 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00172 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00172 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00172 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace wind_mach_credit = 6.00 if `cond1'
replace voltage_dis_energy = 0.00159 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00391 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00395 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02377 if `cond2'
replace energycharge_sum_m_pk = 0.02377 if `cond2'
replace energycharge_sum_off_pk = 0.02377 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02377 if `cond2'
replace energycharge_win_off_pk= 0.02377 if `cond2'
replace customercharge = 41.49 if `cond2'
replace demandcharge = 8.91 if `cond2'
replace voltage_dis_load = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.60 if `cond2' & voltage_cat==2
replace voltage_dis_load = 7.27 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00731

replace old= 0 if missing(old)



// Jan 1 2015

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2A" if missing(old)
replace rate_start_date = mdy(1,1,2015) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.36789 if `cond1'
replace energycharge_sum_m_pk = 0.11155 if `cond1'
replace energycharge_sum_off_pk = 0.04278 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.07679 if `cond1'
replace energycharge_win_off_pk= 0.04969 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00172 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00172 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00172 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00172 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00172 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace wind_mach_credit = 6.00 if `cond1'
replace voltage_dis_energy = 0.00159 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00391 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00395 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02377 if `cond2'
replace energycharge_sum_m_pk = 0.02377 if `cond2'
replace energycharge_sum_off_pk = 0.02377 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02377 if `cond2'
replace energycharge_win_off_pk= 0.02377 if `cond2'
replace customercharge = 41.49 if `cond2'
replace demandcharge = 8.91 if `cond2'
replace voltage_dis_load = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.60 if `cond2' & voltage_cat==2
replace voltage_dis_load = 7.27 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00731

replace old= 0 if missing(old)


// Oct 1 2016

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2A" if missing(old)
replace rate_start_date = mdy(10,1,2016) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.27011 if `cond1'
replace energycharge_sum_m_pk = 0.06887 if `cond1'
replace energycharge_sum_off_pk = 0.03004 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.04333 if `cond1'
replace energycharge_win_off_pk= 0.03453 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00022 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00022 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00022 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00022 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00022 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace wind_mach_credit = 8.30 if `cond1'
replace voltage_dis_energy = 0.00158 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00377 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00381 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02099 if `cond2'
replace energycharge_sum_m_pk = 0.02099 if `cond2'
replace energycharge_sum_off_pk = 0.02099 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02099 if `cond2'
replace energycharge_win_off_pk= 0.02099 if `cond2'
replace customercharge = 42.25 if `cond2'
replace demandcharge = 11.07 if `cond2'
replace voltage_dis_load = 0.12 if `cond2' & voltage_cat==1
replace voltage_dis_load = 3.96 if `cond2' & voltage_cat==2
replace voltage_dis_load = 8.91 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00378

replace old= 0 if missing(old)


// Jun 1 2016

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2A" if missing(old)
replace rate_start_date = mdy(6,1,2016) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.27011 if `cond1'
replace energycharge_sum_m_pk = 0.06887 if `cond1'
replace energycharge_sum_off_pk = 0.03004 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.04333 if `cond1'
replace energycharge_win_off_pk= 0.03453 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00022 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00022 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00022 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00022 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00022 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace wind_mach_credit = 8.30 if `cond1'
replace voltage_dis_energy = 0.00158 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00377 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00381 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02043 if `cond2'
replace energycharge_sum_m_pk = 0.02043 if `cond2'
replace energycharge_sum_off_pk = 0.02043 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02043 if `cond2'
replace energycharge_win_off_pk= 0.02043 if `cond2'
replace customercharge = 42.25 if `cond2'
replace demandcharge = 11.07 if `cond2'
replace voltage_dis_load = 0.12 if `cond2' & voltage_cat==1
replace voltage_dis_load = 3.96 if `cond2' & voltage_cat==2
replace voltage_dis_load = 8.91 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00378

replace old= 0 if missing(old)


// Jan 1 2016

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2A" if missing(old)
replace rate_start_date = mdy(1,1,2016) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.26068 if `cond1'
replace energycharge_sum_m_pk = 0.07828 if `cond1'
replace energycharge_sum_off_pk = 0.02943 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.05362 if `cond1'
replace energycharge_win_off_pk= 0.03434 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00022 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00022 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00022 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00022 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00022 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace wind_mach_credit = 6.00 if `cond1'
replace voltage_dis_energy = 0.00116 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00284 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00287 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02277 if `cond2'
replace energycharge_sum_m_pk = 0.02277 if `cond2'
replace energycharge_sum_off_pk = 0.02277 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02277 if `cond2'
replace energycharge_win_off_pk= 0.02277 if `cond2'
replace customercharge = 46.32 if `cond2'
replace demandcharge = 10.28 if `cond2'
replace voltage_dis_load = 0.09 if `cond2' & voltage_cat==1
replace voltage_dis_load = 2.89 if `cond2' & voltage_cat==2
replace voltage_dis_load = 8.12 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00378

replace old= 0 if missing(old)


// Oct 1 2017

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2A" if missing(old)
replace rate_start_date = mdy(10,1,2017) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.29299 if `cond1'
replace energycharge_sum_m_pk = 0.07450 if `cond1'
replace energycharge_sum_off_pk = 0.03238 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.04681 if `cond1'
replace energycharge_win_off_pk= 0.03725 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.000 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.000 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.000 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.000 if `cond1'
replace energycharge_dwr_win_off_pk= 0.000 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace wind_mach_credit = 8.30 if `cond1'
replace voltage_dis_energy = 0.00179 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00427 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00431 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02223 if `cond2'
replace energycharge_sum_m_pk = 0.02223 if `cond2'
replace energycharge_sum_off_pk = 0.02223 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02223 if `cond2'
replace energycharge_win_off_pk= 0.02223 if `cond2'
replace customercharge = 41.63 if `cond2'
replace demandcharge = 11.26 if `cond2'
replace voltage_dis_load = 0.12 if `cond2' & voltage_cat==1
replace voltage_dis_load = 3.86 if `cond2' & voltage_cat==2
replace voltage_dis_load = 8.78 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00414

replace old= 0 if missing(old)


// June 1 2017

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2A" if missing(old)
replace rate_start_date = mdy(6,1,2017) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.29299 if `cond1'
replace energycharge_sum_m_pk = 0.07450 if `cond1'
replace energycharge_sum_off_pk = 0.03238 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.04681 if `cond1'
replace energycharge_win_off_pk= 0.03725 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.000 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.000 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.000 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.000 if `cond1'
replace energycharge_dwr_win_off_pk= 0.000 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace wind_mach_credit = 8.30 if `cond1'
replace voltage_dis_energy = 0.00179 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00427 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00431 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02281 if `cond2'
replace energycharge_sum_m_pk = 0.02281 if `cond2'
replace energycharge_sum_off_pk = 0.02281 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02281 if `cond2'
replace energycharge_win_off_pk= 0.02281 if `cond2'
replace customercharge = 41.63 if `cond2'
replace demandcharge = 11.26 if `cond2'
replace voltage_dis_load = 0.12 if `cond2' & voltage_cat==1
replace voltage_dis_load = 3.86 if `cond2' & voltage_cat==2
replace voltage_dis_load = 8.78 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00414

replace old= 0 if missing(old)


// Jan 1 2017

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2A" if missing(old)
replace rate_start_date = mdy(1,1,2017) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.29299 if `cond1'
replace energycharge_sum_m_pk = 0.07450 if `cond1'
replace energycharge_sum_off_pk = 0.03238 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.04681 if `cond1'
replace energycharge_win_off_pk= 0.03725 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.000 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.000 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.000 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.000 if `cond1'
replace energycharge_dwr_win_off_pk= 0.000 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace wind_mach_credit = 8.30 if `cond1'
replace voltage_dis_energy = 0.00179 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00427 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00431 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02371 if `cond2'
replace energycharge_sum_m_pk = 0.02371 if `cond2'
replace energycharge_sum_off_pk = 0.02371 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02371 if `cond2'
replace energycharge_win_off_pk= 0.02371 if `cond2'
replace customercharge = 41.63 if `cond2'
replace demandcharge = 11.26 if `cond2'
replace voltage_dis_load = 0.12 if `cond2' & voltage_cat==1
replace voltage_dis_load = 3.86 if `cond2' & voltage_cat==2
replace voltage_dis_load = 8.78 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00414

replace old= 0 if missing(old)


// Oct 1 2018

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2A" if missing(old)
replace rate_start_date = mdy(10,1,2018) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.33179 if `cond1'
replace energycharge_sum_m_pk = 0.08444 if `cond1'
replace energycharge_sum_off_pk = 0.03667 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.05302 if `cond1'
replace energycharge_win_off_pk= 0.04219 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.000 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.000 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.000 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.000 if `cond1'
replace energycharge_dwr_win_off_pk= 0.000 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace wind_mach_credit = 8.30 if `cond1'
replace voltage_dis_energy = 0.00203 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00485 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00490 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01954 if `cond2'
replace energycharge_sum_m_pk = 0.01954 if `cond2'
replace energycharge_sum_off_pk = 0.01954 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01954 if `cond2'
replace energycharge_win_off_pk= 0.01954 if `cond2'
replace customercharge = 43.15 if `cond2'
replace demandcharge = 11.47 if `cond2'
replace voltage_dis_load = 0.12 if `cond2' & voltage_cat==1
replace voltage_dis_load = 3.97 if `cond2' & voltage_cat==2
replace voltage_dis_load = 9.10 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00400

replace old= 0 if missing(old)


// Sep 15 2018

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2A" if missing(old)
replace rate_start_date = mdy(9,15,2018) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.33646 if `cond1'
replace energycharge_sum_m_pk = 0.08563 if `cond1'
replace energycharge_sum_off_pk = 0.03719 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.05377 if `cond1'
replace energycharge_win_off_pk= 0.04279 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.000 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.000 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.000 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.000 if `cond1'
replace energycharge_dwr_win_off_pk= 0.000 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace wind_mach_credit = 8.30 if `cond1'
replace voltage_dis_energy = 0.00205 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00491 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00496 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01957 if `cond2'
replace energycharge_sum_m_pk = 0.01957 if `cond2'
replace energycharge_sum_off_pk = 0.01957 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01957 if `cond2'
replace energycharge_win_off_pk= 0.01957 if `cond2'
replace customercharge = 43.15 if `cond2'
replace demandcharge = 11.47 if `cond2'
replace voltage_dis_load = 0.12 if `cond2' & voltage_cat==1
replace voltage_dis_load = 3.97 if `cond2' & voltage_cat==2
replace voltage_dis_load = 9.10 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00400

replace old= 0 if missing(old)


// Aug 27 2018

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2A" if missing(old)
replace rate_start_date = mdy(8,27,2018) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.25457 if `cond1'
replace energycharge_sum_m_pk = 0.06477 if `cond1'
replace energycharge_sum_off_pk = 0.02814 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.04069 if `cond1'
replace energycharge_win_off_pk= 0.03238 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.000 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.000 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.000 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.000 if `cond1'
replace energycharge_dwr_win_off_pk= 0.000 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace wind_mach_credit = 8.30 if `cond1'
replace voltage_dis_energy = 0.00156 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00371 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00375 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01931 if `cond2'
replace energycharge_sum_m_pk = 0.01931 if `cond2'
replace energycharge_sum_off_pk = 0.01931 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01931 if `cond2'
replace energycharge_win_off_pk= 0.01931 if `cond2'
replace customercharge = 43.15 if `cond2'
replace demandcharge = 11.47 if `cond2'
replace voltage_dis_load = 0.12 if `cond2' & voltage_cat==1
replace voltage_dis_load = 3.97 if `cond2' & voltage_cat==2
replace voltage_dis_load = 9.10 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00400

replace old= 0 if missing(old)


// Jun 1 2018

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2A" if missing(old)
replace rate_start_date = mdy(6,1,2018) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.33646 if `cond1'
replace energycharge_sum_m_pk = 0.08563 if `cond1'
replace energycharge_sum_off_pk = 0.03719 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.05377 if `cond1'
replace energycharge_win_off_pk= 0.04279 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.000 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.000 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.000 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.000 if `cond1'
replace energycharge_dwr_win_off_pk= 0.000 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace wind_mach_credit = 8.30 if `cond1'
replace voltage_dis_energy = 0.00205 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00491 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00496 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01957 if `cond2'
replace energycharge_sum_m_pk = 0.01957 if `cond2'
replace energycharge_sum_off_pk = 0.01957 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01957 if `cond2'
replace energycharge_win_off_pk= 0.01957 if `cond2'
replace customercharge = 43.15 if `cond2'
replace demandcharge = 11.47 if `cond2'
replace voltage_dis_load = 0.12 if `cond2' & voltage_cat==1
replace voltage_dis_load = 3.97 if `cond2' & voltage_cat==2
replace voltage_dis_load = 9.10 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00400

replace old= 0 if missing(old)


// Jan 1 2018

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2A" if missing(old)
replace rate_start_date = mdy(1,1,2018) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.33646 if `cond1'
replace energycharge_sum_m_pk = 0.08563 if `cond1'
replace energycharge_sum_off_pk = 0.03719 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.05377 if `cond1'
replace energycharge_win_off_pk= 0.04279 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.000 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.000 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.000 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.000 if `cond1'
replace energycharge_dwr_win_off_pk= 0.000 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace wind_mach_credit = 8.30 if `cond1'
replace voltage_dis_energy = 0.00205 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00491 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00496 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01888 if `cond2'
replace energycharge_sum_m_pk = 0.01888 if `cond2'
replace energycharge_sum_off_pk = 0.01888 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01888 if `cond2'
replace energycharge_win_off_pk= 0.01888 if `cond2'
replace customercharge = 43.15 if `cond2'
replace demandcharge = 11.47 if `cond2'
replace voltage_dis_load = 0.12 if `cond2' & voltage_cat==1
replace voltage_dis_load = 3.97 if `cond2' & voltage_cat==2
replace voltage_dis_load = 9.10 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00400

replace old= 0 if missing(old)


// Jan 1 2019

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2A" if missing(old)
replace rate_start_date = mdy(1,1,2019) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.33179 if `cond1'
replace energycharge_sum_m_pk = 0.08444 if `cond1'
replace energycharge_sum_off_pk = 0.03667 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.05302 if `cond1'
replace energycharge_win_off_pk= 0.04219 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00007 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00007 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00007 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00007 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00007 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace wind_mach_credit = 8.30 if `cond1'
replace voltage_dis_energy = 0.00203 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00485 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00490 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01963 if `cond2'
replace energycharge_sum_m_pk = 0.01963 if `cond2'
replace energycharge_sum_off_pk = 0.01963 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01963 if `cond2'
replace energycharge_win_off_pk= 0.01963 if `cond2'
replace customercharge = 43.15 if `cond2'
replace demandcharge = 11.11 if `cond2'
replace voltage_dis_load = 0.12 if `cond2' & voltage_cat==1
replace voltage_dis_load = 3.97 if `cond2' & voltage_cat==2
replace voltage_dis_load = 9.10 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00400

replace old= 0 if missing(old)



****** Rate B ******

// Nov 22 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2B" if missing(old)
replace rate_start_date = mdy(11,22,2013) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.11340 if `cond1'
replace energycharge_sum_m_pk = 0.06028 if `cond1'
replace energycharge_sum_off_pk = 0.03727 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.06735 if `cond1'
replace energycharge_win_off_pk= 0.04338 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00095 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00095 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00095 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00095 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00095 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 11.76 if `cond1'
replace demandcharge_sum_mid_pk = 3.15 if `cond1'
replace wind_mach_credit = 6.12 if `cond1'
replace voltage_dis_load_2= 0.18 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.51 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.52 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00099 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00221 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00223 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02119 if `cond2'
replace energycharge_sum_m_pk = 0.02119 if `cond2'
replace energycharge_sum_off_pk = 0.02119 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02119 if `cond2'
replace energycharge_win_off_pk= 0.02119 if `cond2'
replace customercharge = 40.88 if `cond2'
replace demandcharge = 8.11 if `cond2'
replace voltage_dis_load_1 = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 2.60 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 7.17 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Oct 1 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2B" if missing(old)
replace rate_start_date = mdy(10,1,2013) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.11128 if `cond1'
replace energycharge_sum_m_pk = 0.05917 if `cond1'
replace energycharge_sum_off_pk = 0.03660 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.06610 if `cond1'
replace energycharge_win_off_pk= 0.04259 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00097 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 11.54 if `cond1'
replace demandcharge_sum_mid_pk = 3.09 if `cond1'
replace wind_mach_credit = 6.00 if `cond1'
replace voltage_dis_load_2= 0.18 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.51 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.52 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00099 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00219 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00222 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02028 if `cond2'
replace energycharge_sum_m_pk = 0.02028 if `cond2'
replace energycharge_sum_off_pk = 0.02028 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02028 if `cond2'
replace energycharge_win_off_pk= 0.02028 if `cond2'
replace customercharge = 39.50 if `cond2'
replace demandcharge = 7.87 if `cond2'
replace voltage_dis_load_1 = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 2.54 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.93 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Jun 1 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2B" if missing(old)
replace rate_start_date = mdy(6,1,2013) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.11128 if `cond1'
replace energycharge_sum_m_pk = 0.05917 if `cond1'
replace energycharge_sum_off_pk = 0.03660 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.06610 if `cond1'
replace energycharge_win_off_pk= 0.04259 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00097 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 11.54 if `cond1'
replace demandcharge_sum_mid_pk = 3.09 if `cond1'
replace wind_mach_credit = 6.00 if `cond1'
replace voltage_dis_load_2= 0.18 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.51 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.52 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00099 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00219 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00222 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02036 if `cond2'
replace energycharge_sum_m_pk = 0.02036 if `cond2'
replace energycharge_sum_off_pk = 0.02036 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02036 if `cond2'
replace energycharge_win_off_pk= 0.02036 if `cond2'
replace customercharge = 39.50 if `cond2'
replace demandcharge = 8.00 if `cond2'
replace voltage_dis_load_1 = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 2.54 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.93 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Apr 1 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2B" if missing(old)
replace rate_start_date = mdy(4,1,2013) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.11141 if `cond1'
replace energycharge_sum_m_pk = 0.05923 if `cond1'
replace energycharge_sum_off_pk = 0.03664 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.06617 if `cond1'
replace energycharge_win_off_pk= 0.04263 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00097 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 11.55 if `cond1'
replace demandcharge_sum_mid_pk = 3.09 if `cond1'
replace wind_mach_credit = 6.01 if `cond1'
replace voltage_dis_load_2= 0.18 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.51 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.52 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00099 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00219 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00222 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01962 if `cond2'
replace energycharge_sum_m_pk = 0.01962 if `cond2'
replace energycharge_sum_off_pk = 0.01962 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01962 if `cond2'
replace energycharge_win_off_pk= 0.01962 if `cond2'
replace customercharge = 39.50 if `cond2'
replace demandcharge = 8.01 if `cond2'
replace voltage_dis_load_1 = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 2.54 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.94 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Aug 11 2014

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2B" if missing(old)
replace rate_start_date = mdy(8,11,2014) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.13841 if `cond1'
replace energycharge_sum_m_pk = 0.07321 if `cond1'
replace energycharge_sum_off_pk = 0.04496 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.08189 if `cond1'
replace energycharge_win_off_pk= 0.05246 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00037 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00037 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00037 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00037 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00037 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 14.44 if `cond1'
replace demandcharge_sum_mid_pk = 3.86 if `cond1'
replace wind_mach_credit = 6.00 if `cond1'
replace voltage_dis_load_2= 0.20 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.58 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.59 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00111 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00248 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00251 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02318 if `cond2'
replace energycharge_sum_m_pk = 0.02318 if `cond2'
replace energycharge_sum_off_pk = 0.02318 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02318 if `cond2'
replace energycharge_win_off_pk= 0.02318 if `cond2'
replace customercharge = 40.56 if `cond2'
replace demandcharge = 8.34 if `cond2'
replace voltage_dis_load_1 = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 2.54 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 7.11 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00731 if `cond2'

replace old= 0 if missing(old)


// July 7 2014

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2B" if missing(old)
replace rate_start_date = mdy(7,7,2014) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.13932 if `cond1'
replace energycharge_sum_m_pk = 0.07369 if `cond1'
replace energycharge_sum_off_pk = 0.04525 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.08242 if `cond1'
replace energycharge_win_off_pk= 0.05280 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00037 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00037 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00037 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00037 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00037 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 14.53 if `cond1'
replace demandcharge_sum_mid_pk = 3.89 if `cond1'
replace wind_mach_credit = 6.00 if `cond1'
replace voltage_dis_load_2= 0.20 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.58 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.59 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00112 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00250 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00253 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02308 if `cond2'
replace energycharge_sum_m_pk = 0.02308 if `cond2'
replace energycharge_sum_off_pk = 0.02308 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02308 if `cond2'
replace energycharge_win_off_pk= 0.02308 if `cond2'
replace customercharge = 40.56 if `cond2'
replace demandcharge = 8.34 if `cond2'
replace voltage_dis_load_1 = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 2.54 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 7.11 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00681 if `cond2'

replace old= 0 if missing(old)


// June 1 2014

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2B" if missing(old)
replace rate_start_date = mdy(6,1,2014) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.13932 if `cond1'
replace energycharge_sum_m_pk = 0.07369 if `cond1'
replace energycharge_sum_off_pk = 0.04525 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.08242 if `cond1'
replace energycharge_win_off_pk= 0.05280 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00037 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00037 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00037 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00037 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00037 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 14.53 if `cond1'
replace demandcharge_sum_mid_pk = 3.89 if `cond1'
replace wind_mach_credit = 6.00 if `cond1'
replace voltage_dis_load_2= 0.20 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.58 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.59 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00112 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00250 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00253 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02244 if `cond2'
replace energycharge_sum_m_pk = 0.02244 if `cond2'
replace energycharge_sum_off_pk = 0.02244 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02244 if `cond2'
replace energycharge_win_off_pk= 0.02244 if `cond2'
replace customercharge = 40.56 if `cond2'
replace demandcharge = 8.34 if `cond2'
replace voltage_dis_load_1 = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 2.54 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 7.11 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00681 if `cond2'

replace old= 0 if missing(old)


// Apr 1 2014

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2B" if missing(old)
replace rate_start_date = mdy(4,1,2014) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.12215 if `cond1'
replace energycharge_sum_m_pk = 0.06463 if `cond1'
replace energycharge_sum_off_pk = 0.03970 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.07228 if `cond1'
replace energycharge_win_off_pk= 0.04632 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00037 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00037 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00037 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00037 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00037 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 12.73 if `cond1'
replace demandcharge_sum_mid_pk = 3.41 if `cond1'
replace wind_mach_credit = 6.00 if `cond1'
replace voltage_dis_load_2= 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.54 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.55 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00104 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00234 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00236 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02130 if `cond2'
replace energycharge_sum_m_pk = 0.02130 if `cond2'
replace energycharge_sum_off_pk = 0.02130 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02130 if `cond2'
replace energycharge_win_off_pk= 0.02130 if `cond2'
replace customercharge = 41.76 if `cond2'
replace demandcharge = 8.34 if `cond2'
replace voltage_dis_load_1 = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 2.62 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 7.32 if `cond2' & voltage_cat==3
replace cal_climate_credit= 0.00681 if `cond2'

replace old= 0 if missing(old)


// Jan 1 2014

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2B" if missing(old)
replace rate_start_date = mdy(1,1,2014) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.11113 if `cond1'
replace energycharge_sum_m_pk = 0.05881 if `cond1'
replace energycharge_sum_off_pk = 0.03614 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.06577 if `cond1'
replace energycharge_win_off_pk= 0.04216 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00037 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00037 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00037 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00037 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00037 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 11.58 if `cond1'
replace demandcharge_sum_mid_pk = 3.10 if `cond1'
replace wind_mach_credit = 6.00 if `cond1'
replace voltage_dis_load_2= 0.17 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.49 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.50 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00095 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00213 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00215 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02130 if `cond2'
replace energycharge_sum_m_pk = 0.02130 if `cond2'
replace energycharge_sum_off_pk = 0.02130 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02130 if `cond2'
replace energycharge_win_off_pk= 0.02130 if `cond2'
replace customercharge = 41.76 if `cond2'
replace demandcharge = 8.55 if `cond2'
replace voltage_dis_load_1 = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 2.62 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 7.32 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


// Nov 24 2015

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2B" if missing(old)
replace rate_start_date = mdy(11,24,2015) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.12292 if `cond1'
replace energycharge_sum_m_pk = 0.06567 if `cond1'
replace energycharge_sum_off_pk = 0.04097 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.07330 if `cond1'
replace energycharge_win_off_pk= 0.04746 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00172 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00172 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00172 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00172 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00172 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 12.68 if `cond1'
replace demandcharge_sum_mid_pk = 3.38 if `cond1'
replace wind_mach_credit = 6.00 if `cond1'
replace voltage_dis_load_2= 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.52 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.53 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00099 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00223 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00225 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02414 if `cond2'
replace energycharge_sum_m_pk = 0.02414 if `cond2'
replace energycharge_sum_off_pk = 0.02414 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02414 if `cond2'
replace energycharge_win_off_pk= 0.02414 if `cond2'
replace customercharge = 41.49 if `cond2'
replace demandcharge = 8.91 if `cond2'
replace voltage_dis_load_1 = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 2.60 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 7.27 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00680 if `cond2'

replace old= 0 if missing(old)


// Oct 1 2015

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2B" if missing(old)
replace rate_start_date = mdy(10,1,2015) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.12676 if `cond1'
replace energycharge_sum_m_pk = 0.06770 if `cond1'
replace energycharge_sum_off_pk = 0.04211 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.07557 if `cond1'
replace energycharge_win_off_pk= 0.04891 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00172 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00172 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00172 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00172 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00172 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 13.08 if `cond1'
replace demandcharge_sum_mid_pk = 3.49 if `cond1'
replace wind_mach_credit = 6.00 if `cond1'
replace voltage_dis_load_2= 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.53 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.54 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00101 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00227 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00229 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02424 if `cond2'
replace energycharge_sum_m_pk = 0.02424 if `cond2'
replace energycharge_sum_off_pk = 0.02424 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02424 if `cond2'
replace energycharge_win_off_pk= 0.02424 if `cond2'
replace customercharge = 41.49 if `cond2'
replace demandcharge = 8.91 if `cond2'
replace voltage_dis_load_1 = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 2.60 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 7.27 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00680 if `cond2'

replace old= 0 if missing(old)


// Jun 1 2015

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2B" if missing(old)
replace rate_start_date = mdy(6,1,2015) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.12676 if `cond1'
replace energycharge_sum_m_pk = 0.06770 if `cond1'
replace energycharge_sum_off_pk = 0.04211 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.07557 if `cond1'
replace energycharge_win_off_pk= 0.04891 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00172 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00172 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00172 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00172 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00172 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 13.08 if `cond1'
replace demandcharge_sum_mid_pk = 3.49 if `cond1'
replace wind_mach_credit = 6.00 if `cond1'
replace voltage_dis_load_2= 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.53 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.54 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00101 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00227 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00229 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02421 if `cond2'
replace energycharge_sum_m_pk = 0.02421 if `cond2'
replace energycharge_sum_off_pk = 0.02421 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02421 if `cond2'
replace energycharge_win_off_pk= 0.02421 if `cond2'
replace customercharge = 41.49 if `cond2'
replace demandcharge = 8.91 if `cond2'
replace voltage_dis_load_1 = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 2.60 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 7.27 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00680 if `cond2'

replace old= 0 if missing(old)



// Mar 2 2015

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2B" if missing(old)
replace rate_start_date = mdy(3,2,2015) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.12676 if `cond1'
replace energycharge_sum_m_pk = 0.06770 if `cond1'
replace energycharge_sum_off_pk = 0.04211 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.07557 if `cond1'
replace energycharge_win_off_pk= 0.04891 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00172 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00172 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00172 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00172 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00172 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 13.08 if `cond1'
replace demandcharge_sum_mid_pk = 3.49 if `cond1'
replace wind_mach_credit = 6.00 if `cond1'
replace voltage_dis_load_2= 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.53 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.54 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00101 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00227 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00229 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02376 if `cond2'
replace energycharge_sum_m_pk = 0.02376 if `cond2'
replace energycharge_sum_off_pk = 0.02376 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02376 if `cond2'
replace energycharge_win_off_pk= 0.02376 if `cond2'
replace customercharge = 41.49 if `cond2'
replace demandcharge = 8.91 if `cond2'
replace voltage_dis_load_1 = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 2.60 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 7.27 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00680 if `cond2'

replace old= 0 if missing(old)


// Jan 1 2015

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2B" if missing(old)
replace rate_start_date = mdy(1,1,2015) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.12883 if `cond1'
replace energycharge_sum_m_pk = 0.06879 if `cond1'
replace energycharge_sum_off_pk = 0.04278 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.07679 if `cond1'
replace energycharge_win_off_pk= 0.04969 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00172 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00172 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00172 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00172 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00172 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 13.30 if `cond1'
replace demandcharge_sum_mid_pk = 3.55 if `cond1'
replace wind_mach_credit = 6.00 if `cond1'
replace voltage_dis_load_2= 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.54 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.55 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00103 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00231 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00233 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02377 if `cond2'
replace energycharge_sum_m_pk = 0.02377 if `cond2'
replace energycharge_sum_off_pk = 0.02377 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02377 if `cond2'
replace energycharge_win_off_pk= 0.02377 if `cond2'
replace customercharge = 41.49 if `cond2'
replace demandcharge = 8.91 if `cond2'
replace voltage_dis_load_1 = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 2.60 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 7.27 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00731 if `cond2'

replace old= 0 if missing(old)


// Oct 1 2016

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2B" if missing(old)
replace rate_start_date = mdy(10,1,2016) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.09705 if `cond1'
replace energycharge_sum_m_pk = 0.04821 if `cond1'
replace energycharge_sum_off_pk = 0.03004 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.04333 if `cond1'
replace energycharge_win_off_pk= 0.03453 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00022 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00022 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00022 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00022 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00022 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 9.82 if `cond1'
replace demandcharge_sum_mid_pk = 1.77 if `cond1'
replace wind_mach_credit = 8.30 if `cond1'
replace voltage_dis_load_2= 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.53 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.54 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00104 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00230 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00232 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02099 if `cond2'
replace energycharge_sum_m_pk = 0.02099 if `cond2'
replace energycharge_sum_off_pk = 0.02099 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02099 if `cond2'
replace energycharge_win_off_pk= 0.02099 if `cond2'
replace customercharge = 42.25 if `cond2'
replace demandcharge = 11.07 if `cond2'
replace voltage_dis_load_1 = 0.12 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.96 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 8.91 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00378 if `cond2'

replace old= 0 if missing(old)


// Oct 1 2016

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2B" if missing(old)
replace rate_start_date = mdy(10,1,2016) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.09705 if `cond1'
replace energycharge_sum_m_pk = 0.04821 if `cond1'
replace energycharge_sum_off_pk = 0.03004 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.04333 if `cond1'
replace energycharge_win_off_pk= 0.03453 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00022 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00022 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00022 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00022 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00022 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 9.82 if `cond1'
replace demandcharge_sum_mid_pk = 1.77 if `cond1'
replace wind_mach_credit = 8.30 if `cond1'
replace voltage_dis_load_2= 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.53 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.54 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00104 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00230 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00232 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02099 if `cond2'
replace energycharge_sum_m_pk = 0.02099 if `cond2'
replace energycharge_sum_off_pk = 0.02099 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02099 if `cond2'
replace energycharge_win_off_pk= 0.02099 if `cond2'
replace customercharge = 42.25 if `cond2'
replace demandcharge = 11.07 if `cond2'
replace voltage_dis_load_1 = 0.12 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.96 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 8.91 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00378 if `cond2'

replace old= 0 if missing(old)


// Jun 1 2016

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2B" if missing(old)
replace rate_start_date = mdy(6,1,2016) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.09705 if `cond1'
replace energycharge_sum_m_pk = 0.04821 if `cond1'
replace energycharge_sum_off_pk = 0.03004 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.04333 if `cond1'
replace energycharge_win_off_pk= 0.03453 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00022 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00022 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00022 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00022 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00022 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 9.82 if `cond1'
replace demandcharge_sum_mid_pk = 1.77 if `cond1'
replace wind_mach_credit = 8.30 if `cond1'
replace voltage_dis_load_2= 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.53 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.54 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00104 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00230 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00232 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02043 if `cond2'
replace energycharge_sum_m_pk = 0.02043 if `cond2'
replace energycharge_sum_off_pk = 0.02043 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02043 if `cond2'
replace energycharge_win_off_pk= 0.02043 if `cond2'
replace customercharge = 42.25 if `cond2'
replace demandcharge = 11.07 if `cond2'
replace voltage_dis_load_1 = 0.12 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.96 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 8.91 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00378 if `cond2'

replace old= 0 if missing(old)


// Jan 1 2016

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2B" if missing(old)
replace rate_start_date = mdy(1,1,2016) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.09064 if `cond1'
replace energycharge_sum_m_pk = 0.04793 if `cond1'
replace energycharge_sum_off_pk = 0.02943 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.05362 if `cond1'
replace energycharge_win_off_pk= 0.03434 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00022 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00022 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00022 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00022 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00022 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 9.46 if `cond1'
replace demandcharge_sum_mid_pk = 2.52 if `cond1'
replace wind_mach_credit = 6.00 if `cond1'
replace voltage_dis_load_2= 0.14 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.39 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.39 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00075 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00169 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00171 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02277 if `cond2'
replace energycharge_sum_m_pk = 0.02277 if `cond2'
replace energycharge_sum_off_pk = 0.02277 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02277 if `cond2'
replace energycharge_win_off_pk= 0.02277 if `cond2'
replace customercharge = 46.32 if `cond2'
replace demandcharge = 10.28 if `cond2'
replace voltage_dis_load_1 = 0.09 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 2.89 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 8.12 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00378 if `cond2'

replace old= 0 if missing(old)

// Oct 1 2017

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2B" if missing(old)
replace rate_start_date = mdy(10,1,2017) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.10513 if `cond1'
replace energycharge_sum_m_pk = 0.05210 if `cond1'
replace energycharge_sum_off_pk = 0.03238 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.04681 if `cond1'
replace energycharge_win_off_pk= 0.03725 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.0000 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.0000 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.0000 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.0000 if `cond1'
replace energycharge_dwr_win_off_pk= 0.0000 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 10.66 if `cond1'
replace demandcharge_sum_mid_pk = 1.92 if `cond1'
replace wind_mach_credit = 8.30 if `cond1'
replace voltage_dis_load_2= 0.22 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.60 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.61 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00118 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00261 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00264 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02223 if `cond2'
replace energycharge_sum_m_pk = 0.02223 if `cond2'
replace energycharge_sum_off_pk = 0.02223 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02223 if `cond2'
replace energycharge_win_off_pk= 0.02223 if `cond2'
replace customercharge = 41.63 if `cond2'
replace demandcharge = 11.26 if `cond2'
replace voltage_dis_load_1 = 0.12 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.86 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 8.78 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00414 if `cond2'

replace old= 0 if missing(old)


// Jun 1 2017

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2B" if missing(old)
replace rate_start_date = mdy(6,1,2017) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.10513 if `cond1'
replace energycharge_sum_m_pk = 0.05210 if `cond1'
replace energycharge_sum_off_pk = 0.03238 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.04681 if `cond1'
replace energycharge_win_off_pk= 0.03725 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.0000 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.0000 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.0000 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.0000 if `cond1'
replace energycharge_dwr_win_off_pk= 0.0000 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 10.66 if `cond1'
replace demandcharge_sum_mid_pk = 1.92 if `cond1'
replace wind_mach_credit = 8.30 if `cond1'
replace voltage_dis_load_2= 0.22 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.60 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.61 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00118 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00261 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00264 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02281 if `cond2'
replace energycharge_sum_m_pk = 0.02281 if `cond2'
replace energycharge_sum_off_pk = 0.02281 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02281 if `cond2'
replace energycharge_win_off_pk= 0.02281 if `cond2'
replace customercharge = 41.63 if `cond2'
replace demandcharge = 11.26 if `cond2'
replace voltage_dis_load_1 = 0.12 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.86 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 8.78 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00414 if `cond2'

replace old= 0 if missing(old)


// Jan 1 2017

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2B" if missing(old)
replace rate_start_date = mdy(1,1,2017) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.10513 if `cond1'
replace energycharge_sum_m_pk = 0.05210 if `cond1'
replace energycharge_sum_off_pk = 0.03238 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.04681 if `cond1'
replace energycharge_win_off_pk= 0.03725 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.0000 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.0000 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.0000 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.0000 if `cond1'
replace energycharge_dwr_win_off_pk= 0.0000 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 10.66 if `cond1'
replace demandcharge_sum_mid_pk = 1.92 if `cond1'
replace wind_mach_credit = 8.30 if `cond1'
replace voltage_dis_load_2= 0.22 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.60 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.61 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00118 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00261 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00264 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.02371 if `cond2'
replace energycharge_sum_m_pk = 0.02371 if `cond2'
replace energycharge_sum_off_pk = 0.02371 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02371 if `cond2'
replace energycharge_win_off_pk= 0.02371 if `cond2'
replace customercharge = 41.63 if `cond2'
replace demandcharge = 11.26 if `cond2'
replace voltage_dis_load_1 = 0.12 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.86 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 8.78 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00414 if `cond2'

replace old= 0 if missing(old)


// Oct 1 2018

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2B" if missing(old)
replace rate_start_date = mdy(10,1,2018) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.11908 if `cond1'
replace energycharge_sum_m_pk = 0.05900 if `cond1'
replace energycharge_sum_off_pk = 0.03667 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.05302 if `cond1'
replace energycharge_win_off_pk= 0.04219 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.0000 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.0000 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.0000 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.0000 if `cond1'
replace energycharge_dwr_win_off_pk= 0.0000 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 12.07 if `cond1'
replace demandcharge_sum_mid_pk = 2.18 if `cond1'
replace wind_mach_credit = 8.30 if `cond1'
replace voltage_dis_load_2= 0.25 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.68 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.69 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00134 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00296 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00299 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01954 if `cond2'
replace energycharge_sum_m_pk = 0.01954 if `cond2'
replace energycharge_sum_off_pk = 0.01954 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01954 if `cond2'
replace energycharge_win_off_pk= 0.01954 if `cond2'
replace customercharge = 43.15 if `cond2'
replace demandcharge = 11.47 if `cond2'
replace voltage_dis_load_1 = 0.12 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.97 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 9.10 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00400 if `cond2'

replace old= 0 if missing(old)


// Sep 15 2018

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2B" if missing(old)
replace rate_start_date = mdy(9,15,2018) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.12076 if `cond1'
replace energycharge_sum_m_pk = 0.05984 if `cond1'
replace energycharge_sum_off_pk = 0.03719 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.05377 if `cond1'
replace energycharge_win_off_pk= 0.04279 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.0000 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.0000 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.0000 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.0000 if `cond1'
replace energycharge_dwr_win_off_pk= 0.0000 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 12.24 if `cond1'
replace demandcharge_sum_mid_pk = 2.21 if `cond1'
replace wind_mach_credit = 8.30 if `cond1'
replace voltage_dis_load_2= 0.25 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.69 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.70 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00136 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00300 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00303 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01957 if `cond2'
replace energycharge_sum_m_pk = 0.01957 if `cond2'
replace energycharge_sum_off_pk = 0.01957 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01957 if `cond2'
replace energycharge_win_off_pk= 0.01957 if `cond2'
replace customercharge = 43.15 if `cond2'
replace demandcharge = 11.47 if `cond2'
replace voltage_dis_load_1 = 0.12 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.97 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 9.10 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00400 if `cond2'

replace old= 0 if missing(old)


// Aug 27 2018

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2B" if missing(old)
replace rate_start_date = mdy(8,27,2018) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.09138 if `cond1'
replace energycharge_sum_m_pk = 0.04528 if `cond1'
replace energycharge_sum_off_pk = 0.02814 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.04069 if `cond1'
replace energycharge_win_off_pk= 0.03238 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.0000 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.0000 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.0000 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.0000 if `cond1'
replace energycharge_dwr_win_off_pk= 0.0000 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 9.26 if `cond1'
replace demandcharge_sum_mid_pk = 1.67 if `cond1'
replace wind_mach_credit = 8.30 if `cond1'
replace voltage_dis_load_2= 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.52 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.53 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00103 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00227 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00229 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01931 if `cond2'
replace energycharge_sum_m_pk = 0.01931 if `cond2'
replace energycharge_sum_off_pk = 0.01931 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01931 if `cond2'
replace energycharge_win_off_pk= 0.01931 if `cond2'
replace customercharge = 43.15 if `cond2'
replace demandcharge = 11.47 if `cond2'
replace voltage_dis_load_1 = 0.12 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.97 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 9.10 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00400 if `cond2'

replace old= 0 if missing(old)


// Jun 1 2018

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2B" if missing(old)
replace rate_start_date = mdy(6,1,2018) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.12076 if `cond1'
replace energycharge_sum_m_pk = 0.05984 if `cond1'
replace energycharge_sum_off_pk = 0.03719 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.05377 if `cond1'
replace energycharge_win_off_pk= 0.04279 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.0000 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.0000 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.0000 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.0000 if `cond1'
replace energycharge_dwr_win_off_pk= 0.0000 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 12.24 if `cond1'
replace demandcharge_sum_mid_pk = 2.21 if `cond1'
replace wind_mach_credit = 8.30 if `cond1'
replace voltage_dis_load_2= 0.25 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.69 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.70 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00136 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00300 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00303 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01957 if `cond2'
replace energycharge_sum_m_pk = 0.01957 if `cond2'
replace energycharge_sum_off_pk = 0.01957 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01957 if `cond2'
replace energycharge_win_off_pk= 0.01957 if `cond2'
replace customercharge = 43.15 if `cond2'
replace demandcharge = 11.47 if `cond2'
replace voltage_dis_load_1 = 0.12 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.97 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 9.10 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00400 if `cond2'

replace old= 0 if missing(old)


// Jan 1 2018

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2B" if missing(old)
replace rate_start_date = mdy(1,1,2018) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.12076 if `cond1'
replace energycharge_sum_m_pk = 0.05984 if `cond1'
replace energycharge_sum_off_pk = 0.03719 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.05377 if `cond1'
replace energycharge_win_off_pk= 0.04279 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.0000 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.0000 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.0000 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.0000 if `cond1'
replace energycharge_dwr_win_off_pk= 0.0000 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 12.24 if `cond1'
replace demandcharge_sum_mid_pk = 2.21 if `cond1'
replace wind_mach_credit = 8.30 if `cond1'
replace voltage_dis_load_2= 0.25 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.69 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.70 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00136 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00300 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00303 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01888 if `cond2'
replace energycharge_sum_m_pk = 0.01888 if `cond2'
replace energycharge_sum_off_pk = 0.01888 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01888 if `cond2'
replace energycharge_win_off_pk= 0.01888 if `cond2'
replace customercharge = 43.15 if `cond2'
replace demandcharge = 11.47 if `cond2'
replace voltage_dis_load_1 = 0.12 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.97 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 9.10 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00400 if `cond2'

replace old= 0 if missing(old)


// Jun 1 2019

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2B" if missing(old)
replace rate_start_date = mdy(6,1,2019) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.06410 if `cond1'
replace energycharge_sum_m_pk = 0.05927 if `cond1'
replace energycharge_sum_off_pk = 0.05770 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.07755 if `cond1'
replace energycharge_win_off_pk= 0.05095 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00007 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00007 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00007 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00007 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00007 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 9.33 if `cond1'
replace demandcharge_sum_mid_pk = 2.85 if `cond1'
replace wind_mach_credit = 6.33 if `cond1'
replace voltage_dis_load_2= 0.14 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.36 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.36 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00092 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00202 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00205 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01781 if `cond2'
replace energycharge_sum_m_pk = 0.01781 if `cond2'
replace energycharge_sum_off_pk = 0.01781 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01781 if `cond2'
replace energycharge_win_off_pk= 0.01781 if `cond2'
replace customercharge = 41.34 if `cond2'
replace demandcharge = 10.98 if `cond2'
replace voltage_dis_load_1 = 0.12 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.76 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 8.97 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00251 if `cond2'

replace old= 0 if missing(old)


// Apr 12 2019

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2B" if missing(old)
replace rate_start_date = mdy(4,12,2019) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.05921 if `cond1'
replace energycharge_sum_m_pk = 0.05475 if `cond1'
replace energycharge_sum_off_pk = 0.05330 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.07160 if `cond1'
replace energycharge_win_off_pk= 0.04706 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00007 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00007 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00007 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00007 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00007 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 8.62 if `cond1'
replace demandcharge_sum_mid_pk = 2.63 if `cond1'
replace wind_mach_credit = 6.33 if `cond1'
replace voltage_dis_load_2= 0.12 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.34 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.34 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00085 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00187 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00189 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01765 if `cond2'
replace energycharge_sum_m_pk = 0.01765 if `cond2'
replace energycharge_sum_off_pk = 0.01765 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01765 if `cond2'
replace energycharge_win_off_pk= 0.01765 if `cond2'
replace customercharge = 40.46 if `cond2'
replace demandcharge = 10.79 if `cond2'
replace voltage_dis_load_1 = 0.12 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.68 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 8.78 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00400 if `cond2'

replace old= 0 if missing(old)


// Mar 1 2019

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2B" if missing(old)
replace rate_start_date = mdy(3,1,2019) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.05581 if `cond1'
replace energycharge_sum_m_pk = 0.05160 if `cond1'
replace energycharge_sum_off_pk = 0.05023 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.06743 if `cond1'
replace energycharge_win_off_pk= 0.04435 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00007 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00007 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00007 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00007 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00007 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 8.12 if `cond1'
replace demandcharge_sum_mid_pk = 2.48 if `cond1'
replace wind_mach_credit = 6.33 if `cond1'
replace voltage_dis_load_2= 0.11 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.31 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.31 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00080 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00177 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00178 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.01902 if `cond2'
replace energycharge_sum_m_pk = 0.01902 if `cond2'
replace energycharge_sum_off_pk = 0.01902 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01902 if `cond2'
replace energycharge_win_off_pk= 0.01902 if `cond2'
replace customercharge = 43.15 if `cond2'
replace demandcharge = 11.37 if `cond2'
replace voltage_dis_load_1 = 0.13 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.93 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 9.36 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00400 if `cond2'

replace old= 0 if missing(old)



/*
*****************************
*** 6. Schedule- TOU-PA3B ***
*****************************

gen minimum_charge_sum= .
gen minimum_charge_win= .
label variable minimum_charge_sum "$/kWh"
// Nov 22 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2B" if missing(old)
replace rate_start_date = mdy(11,22,2019) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.09448 if `cond1'
replace energycharge_sum_m_pk = 0.05006 if `cond1'
replace energycharge_sum_off_pk = 0.03244 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.05398 if `cond1'
replace energycharge_win_off_pk= 0.03662 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00095 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00095 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00095 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00095 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00095 if `cond1'

replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 8.12 if `cond1'
replace demandcharge_sum_mid_pk = 2.48 if `cond1'
replace voltage_dis_load_2= 0.11 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.31 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.31 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00080 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00177 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00178 if `cond1' & voltage_cat==3


//unbundled 
replace energycharge_sum_on_pk = 0.01902 if `cond2'
replace energycharge_sum_m_pk = 0.01902 if `cond2'
replace energycharge_sum_off_pk = 0.01902 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01902 if `cond2'
replace energycharge_win_off_pk= 0.01902 if `cond2'
replace customercharge = 43.15 if `cond2'
replace demandcharge = 11.37 if `cond2'
replace pf_adjust = 0.51 if `cond2' & voltage_cat==1
replace pf_adjust = 0.34 if `cond2' & voltage_cat==2
replace pf_adjust = 0.34 if `cond2' & voltage_cat==3
replace voltage_dis_load_1 = 0.13 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.93 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 9.36 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

replace demandcharge_win_mid

*/


*****************************
*** 6. Schedule- TOU-PA2D ***
*****************************

gen minimum_charge_sum= .
gen minimum_charge_win= .
label variable minimum_charge_sum "$/kWh"

gen voltage_dis_load_sop_win_weekd=0
gen voltage_dis_load_sop=0
gen energycharge_dwr_win_sup_off_pk=0
gen energycharge_win_sup_off_pk= 0
gen demandcharge_win_m_pk= 0

label variable voltage_dis_load_sop_win_weekd "$/kWh"
// Nov 22 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2D" if missing(old)
replace rate_start_date = mdy(6,1,2019) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.08720 if `cond1'
replace energycharge_sum_m_pk = 0.07842 if `cond1'
replace energycharge_sum_off_pk = 0.05289 if `cond1'
replace energycharge_win_m_pk = 0.06681 if `cond1'
replace energycharge_win_off_pk = 0.05318 if `cond1'
replace energycharge_win_sup_off_pk= 0.04542 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00007 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00007 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00007 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00007 if `cond1'
replace energycharge_dwr_win_off_pk = 0.00007 if `cond1'
replace energycharge_dwr_win_sup_off_pk= 0.00007 if `cond1'

replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 12.36 if `cond1'
replace demandcharge_win_m_pk = 2.18 if `cond1'

replace wind_mach_credit = 6.33 if `cond1'
replace voltage_dis_load_sop_win_weekd = 0.11 if `cond1' & voltage_cat==1 
replace voltage_dis_load_sop_win_weekd = 0.29 if `cond1' & voltage_cat==2
replace voltage_dis_load_sop_win_weekd = 0.29 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00092 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00202 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00205 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.04422 if `cond2'
replace energycharge_sum_m_pk = 0.04422 if `cond2'
replace energycharge_sum_off_pk = 0.04422 if `cond2'
replace energycharge_win_m_pk = 0.02863 if `cond2'
replace energycharge_win_off_pk = 0.02642 if `cond2'
replace energycharge_win_sup_off_pk= 0.02516 if `cond2'
replace customercharge = 41.34 if `cond2'
replace demandcharge = 8.17 if `cond2'
replace demandcharge_sum_on_pk = 2.31 if `cond2'
replace demandcharge_win_m_pk = 0.00 if `cond2'

replace voltage_dis_load_1 = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 2.57 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.16 if `cond2' & voltage_cat==3
replace voltage_dis_load_sop= 0.04 if `cond2' & voltage_cat==1
replace voltage_dis_load_sop= 0.98 if `cond2' & voltage_cat==2
replace voltage_dis_load_sop= 2.31 if `cond2' & voltage_cat==3
replace voltage_dis_energy = 0.00013 if `cond2' & voltage_cat==1
replace voltage_dis_energy = 0.00369 if `cond2' & voltage_cat==2
replace voltage_dis_energy = 0.00877 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00251 if `cond2'

replace old= 0 if missing(old)

*** Apr 12 2019 ***

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2D" if missing(old)
replace rate_start_date = mdy(4,12,2019) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.08053 if `cond1'
replace energycharge_sum_m_pk = 0.07243 if `cond1'
replace energycharge_sum_off_pk = 0.04885 if `cond1'
replace energycharge_win_m_pk = 0.06170 if `cond1'
replace energycharge_win_off_pk = 0.04912 if `cond1'
replace energycharge_win_sup_off_pk= 0.04195 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00007 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00007 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00007 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00007 if `cond1'
replace energycharge_dwr_win_off_pk = 0.00007 if `cond1'
replace energycharge_dwr_win_sup_off_pk= 0.00007 if `cond1'

replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 11.41 if `cond1'
replace demandcharge_win_m_pk = 2.01 if `cond1'

replace wind_mach_credit = 6.33 if `cond1'
replace voltage_dis_load_sop_win_weekd = 0.10 if `cond1' & voltage_cat==1 
replace voltage_dis_load_sop_win_weekd = 0.27 if `cond1' & voltage_cat==2
replace voltage_dis_load_sop_win_weekd = 0.27 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00085 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00187 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00189 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.04350 if `cond2'
replace energycharge_sum_m_pk = 0.04350 if `cond2'
replace energycharge_sum_off_pk = 0.02215 if `cond2'
replace energycharge_win_m_pk = 0.02824 if `cond2'
replace energycharge_win_off_pk = 0.02608 if `cond2'
replace energycharge_win_sup_off_pk= 0.02484 if `cond2'
replace customercharge = 40.46 if `cond2'
replace demandcharge = 8.04 if `cond2'
replace demandcharge_sum_on_pk = 2.26 if `cond2'
replace demandcharge_win_m_pk = 0.00 if `cond2'

replace voltage_dis_load_1 = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 2.52 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.03 if `cond2' & voltage_cat==3
replace voltage_dis_load_sop= 0.04 if `cond2' & voltage_cat==1
replace voltage_dis_load_sop= 0.96 if `cond2' & voltage_cat==2
replace voltage_dis_load_sop= 2.26 if `cond2' & voltage_cat==3
replace voltage_dis_energy = 0.00013 if `cond2' & voltage_cat==1
replace voltage_dis_energy = 0.00361 if `cond2' & voltage_cat==2
replace voltage_dis_energy = 0.00858 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00400 if `cond2'

replace old= 0 if missing(old)


*** Mar 1 2019 ***

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA2D" if missing(old)
replace rate_start_date = mdy(3,1,2019) if missing(old)
replace voltage_cat= mod(_n,3) + 1 if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.07589 if `cond1'
replace energycharge_sum_m_pk = 0.06826 if `cond1'
replace energycharge_sum_off_pk = 0.04604 if `cond1'
replace energycharge_win_m_pk = 0.05814 if `cond1'
replace energycharge_win_off_pk = 0.04628 if `cond1'
replace energycharge_win_sup_off_pk= 0.03953 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00007 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00007 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00007 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00007 if `cond1'
replace energycharge_dwr_win_off_pk = 0.00007 if `cond1'
replace energycharge_dwr_win_sup_off_pk= 0.00007 if `cond1'

replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 10.75 if `cond1'
replace demandcharge_win_m_pk = 1.89 if `cond1'

replace wind_mach_credit = 6.33 if `cond1'
replace voltage_dis_load_sop_win_weekd = 0.09 if `cond1' & voltage_cat==1 
replace voltage_dis_load_sop_win_weekd = 0.25 if `cond1' & voltage_cat==2
replace voltage_dis_load_sop_win_weekd = 0.25 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00080 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00177 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00178 if `cond1' & voltage_cat==3

//unbundled 
replace energycharge_sum_on_pk = 0.04659 if `cond2'
replace energycharge_sum_m_pk = 0.04659 if `cond2'
replace energycharge_sum_off_pk = 0.02382 if `cond2'
replace energycharge_win_m_pk = 0.03031 if `cond2'
replace energycharge_win_off_pk = 0.02801 if `cond2'
replace energycharge_win_sup_off_pk= 0.02669 if `cond2'
replace customercharge = 43.15 if `cond2'
replace demandcharge = 8.44 if `cond2'
replace demandcharge_sum_on_pk = 2.41 if `cond2'
replace demandcharge_win_m_pk = 0.00 if `cond2'

replace voltage_dis_load_1 = 0.08 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 2.69 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.43 if `cond2' & voltage_cat==3
replace voltage_dis_load_sop= 0.04 if `cond2' & voltage_cat==1
replace voltage_dis_load_sop= 1.02 if `cond2' & voltage_cat==2
replace voltage_dis_load_sop= 2.41 if `cond2' & voltage_cat==3
replace voltage_dis_energy = 0.00014 if `cond2' & voltage_cat==1
replace voltage_dis_energy = 0.00385 if `cond2' & voltage_cat==2
replace voltage_dis_energy = 0.00915 if `cond2' & voltage_cat==3
replace cal_climate_credit = 0.00400 if `cond2'

replace old= 0 if missing(old)


*******************************
*** 7. Schedule- TOU-PA-SOP ***
*******************************

//new variables:
gen energycharge_sum_sup_off_pk=0
gen energycharge_dwr_sum_sup_off_pk=0
gen demandcharge_win_off_pk=0
gen demandcharge_win_sup_off_pk=0
gen demandcharge_sum_sup_off_pk=0
gen demandcharge_win_on_pk=0
gen interruptible_credit=0
gen ee_charge=0

//now we have a new category 0-2 kV for the excess energy charge
local cats=4


** Oct 1 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-SOP" if missing(old)
replace rate_start_date = mdy(10,1,2009) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.08865 if `cond1'
replace energycharge_sum_off_pk = 0.05167 if `cond1'
replace energycharge_sum_sup_off_pk = 0.02034 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_off_pk = 0.06300 if `cond1'
replace energycharge_win_sup_off_pk= 0.01983 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.06225 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.06225 if `cond1'
replace energycharge_dwr_sum_sup_off_pk = 0.06225 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_off_pk = 0.06225 if `cond1'
replace energycharge_dwr_win_sup_off_pk= 0.06225 if `cond1'

replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 24.34 if `cond1'
replace demandcharge_sum_off_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'
replace demandcharge_win_sup_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.01207 if `cond2'
replace energycharge_sum_off_pk = 0.01207 if `cond2'
replace energycharge_sum_sup_off_pk = 0.01207 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_off_pk = 0.01207 if `cond2'
replace energycharge_win_sup_off_pk= 0.01207 if `cond2'
replace customercharge = 113.76 if `cond2'
replace demandcharge = 6.82 if `cond2'

** second piece

//bundled
replace voltage_dis_load_2= 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.54 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.55 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled
replace pf_adjust= 0.32 if `cond2' & voltage_cat==3
replace pf_adjust= 0.32 if `cond2' & voltage_cat==2
replace pf_adjust= 0.27 if `cond2' & voltage_cat==1
replace pf_adjust= 0.27 if `cond2' & voltage_cat==0
replace voltage_dis_load_1 = 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.08 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


** Jun 1 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-SOP" if missing(old)
replace rate_start_date = mdy(6,1,2009) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.08830 if `cond1'
replace energycharge_sum_off_pk = 0.05374 if `cond1'
replace energycharge_sum_sup_off_pk = 0.02210 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_off_pk = 0.05398 if `cond1'
replace energycharge_win_sup_off_pk= 0.02159 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.06225 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.06225 if `cond1'
replace energycharge_dwr_sum_sup_off_pk = 0.06225 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_off_pk = 0.06225 if `cond1'
replace energycharge_dwr_win_sup_off_pk= 0.06225 if `cond1'

replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 24.34 if `cond1'
replace demandcharge_sum_off_pk = 0.00 if `cond1'
replace demandcharge_sum_sup_off_pk = 0.00 if `cond1'
replace demandcharge_win_on_pk = -999 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'
replace demandcharge_win_sup_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.01081 if `cond2'
replace energycharge_sum_off_pk = 0.01081 if `cond2'
replace energycharge_sum_sup_off_pk = 0.01081 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_off_pk = 0.01081 if `cond2'
replace energycharge_win_sup_off_pk= 0.01081 if `cond2'
replace customercharge = 94.80 if `cond2'
replace demandcharge = 6.01 if `cond2'

** second piece

//bundled
replace voltage_dis_load_2= 0.38 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 1.04 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 1.04 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00198 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00431 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00431 if `cond1' & voltage_cat==3

//unbundled
replace pf_adjust= 0.20 if `cond2' & voltage_cat==3
replace pf_adjust= 0.20 if `cond2' & voltage_cat==2
replace pf_adjust= 0.18 if `cond2' & voltage_cat==1
replace pf_adjust= 0.18 if `cond2' & voltage_cat==0
replace voltage_dis_load_1 = 0.19 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 6.25 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.25 if `cond2' & voltage_cat==3
replace interruptible_credit = 0.00933 if `cond2'
replace ee_charge = 10.21374 if `cond2' * voltage_cat==0
replace ee_charge = 9.99551 if `cond2' * voltage_cat==1
replace ee_charge = 9.63234 if `cond2' * voltage_cat==2
replace ee_charge = 9.63234 if `cond2' * voltage_cat==3

replace old= 0 if missing(old)


** Apr 4 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-SOP" if missing(old)
replace rate_start_date = mdy(4,4,2009) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.08988 if `cond1'
replace energycharge_sum_off_pk = 0.05486 if `cond1'
replace energycharge_sum_sup_off_pk = 0.02278 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_off_pk = 0.05510 if `cond1'
replace energycharge_win_sup_off_pk= 0.02228 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.06210 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.06210 if `cond1'
replace energycharge_dwr_sum_sup_off_pk = 0.06210 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_off_pk = 0.06210 if `cond1'
replace energycharge_dwr_win_sup_off_pk= 0.06210 if `cond1'

replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 18.77 if `cond1'
replace demandcharge_sum_off_pk = 0.00 if `cond1'
replace demandcharge_sum_sup_off_pk = 0.00 if `cond1'
replace demandcharge_win_on_pk = -999 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'
replace demandcharge_win_sup_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.01042 if `cond2'
replace energycharge_sum_off_pk = 0.01042 if `cond2'
replace energycharge_sum_sup_off_pk = 0.01042 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_off_pk = 0.01042 if `cond2'
replace energycharge_win_sup_off_pk= 0.01042 if `cond2'
replace customercharge = 94.80 if `cond2'
replace demandcharge = 6.01 if `cond2'

** second piece

//bundled
replace voltage_dis_load_2= 0.39 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 1.05 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 1.05 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00201 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00437 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00437 if `cond1' & voltage_cat==3

//unbundled
replace pf_adjust= 0.20 if `cond2' & voltage_cat==3
replace pf_adjust= 0.20 if `cond2' & voltage_cat==2
replace pf_adjust= 0.18 if `cond2' & voltage_cat==1
replace pf_adjust= 0.18 if `cond2' & voltage_cat==0
replace voltage_dis_load_1 = 0.19 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 6.25 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.25 if `cond2' & voltage_cat==3
replace interruptible_credit = 0.00933 if `cond2'
replace ee_charge = 10.21374 if `cond2' * voltage_cat==0
replace ee_charge = 9.99551 if `cond2' * voltage_cat==1
replace ee_charge = 9.63234 if `cond2' * voltage_cat==2
replace ee_charge = 9.63234 if `cond2' * voltage_cat==3

replace old= 0 if missing(old)


** Mar 1 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-SOP" if missing(old)
replace rate_start_date = mdy(3,1,2009) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.08840 if `cond1'
replace energycharge_sum_off_pk = 0.05380 if `cond1'
replace energycharge_sum_sup_off_pk = 0.02213 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_off_pk = 0.05404 if `cond1'
replace energycharge_win_sup_off_pk= 0.02163 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.06231 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.06231 if `cond1'
replace energycharge_dwr_sum_sup_off_pk = 0.06231 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_off_pk = 0.06231 if `cond1'
replace energycharge_dwr_win_sup_off_pk= 0.06231 if `cond1'

replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 18.54 if `cond1'
replace demandcharge_sum_off_pk = 0.00 if `cond1'
replace demandcharge_sum_sup_off_pk = 0.00 if `cond1'
replace demandcharge_win_on_pk = -999 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'
replace demandcharge_win_sup_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.01063 if `cond2'
replace energycharge_sum_off_pk = 0.01063 if `cond2'
replace energycharge_sum_sup_off_pk = 0.01063 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_off_pk = 0.01063 if `cond2'
replace energycharge_win_sup_off_pk= 0.01063 if `cond2'
replace customercharge = 81.68 if `cond2'
replace demandcharge = 5.31 if `cond2'

** second piece

//bundled
replace voltage_dis_load_2= 0.39 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 1.04 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 1.04 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00199 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00432 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00432 if `cond1' & voltage_cat==3

//unbundled
replace pf_adjust= 0.20 if `cond2' & voltage_cat==3
replace pf_adjust= 0.20 if `cond2' & voltage_cat==2
replace pf_adjust= 0.18 if `cond2' & voltage_cat==1
replace pf_adjust= 0.18 if `cond2' & voltage_cat==0
replace voltage_dis_load_1 = 0.19 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 6.25 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.25 if `cond2' & voltage_cat==3
replace interruptible_credit = 0.00933 if `cond2'
replace ee_charge = 10.21374 if `cond2' * voltage_cat==0
replace ee_charge = 9.99551 if `cond2' * voltage_cat==1
replace ee_charge = 9.99551 if `cond2' * voltage_cat==2
replace ee_charge = 9.63234 if `cond2' * voltage_cat==3

replace old= 0 if missing(old)


** Jan 1 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-SOP" if missing(old)
replace rate_start_date = mdy(1,1,2009) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.07971 if `cond1'
replace energycharge_sum_off_pk = 0.04537 if `cond1'
replace energycharge_sum_sup_off_pk = 0.01393 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_off_pk = 0.04560 if `cond1'
replace energycharge_win_sup_off_pk= 0.01343 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.08451 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.08451 if `cond1'
replace energycharge_dwr_sum_sup_off_pk = 0.08451 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_off_pk = 0.08451 if `cond1'
replace energycharge_dwr_win_sup_off_pk= 0.08451 if `cond1'

replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 18.56 if `cond1'
replace demandcharge_sum_off_pk = 0.00 if `cond1'
replace demandcharge_sum_sup_off_pk = 0.00 if `cond1'
replace demandcharge_win_on_pk = -999 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'
replace demandcharge_win_sup_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.01349 if `cond2'
replace energycharge_sum_off_pk = 0.01349 if `cond2'
replace energycharge_sum_sup_off_pk = 0.01349 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_off_pk = 0.01349 if `cond2'
replace energycharge_win_sup_off_pk= 0.01349 if `cond2'
replace customercharge = 83.73 if `cond2'
replace demandcharge = 5.00 if `cond2'

** second piece

//bundled
replace voltage_dis_load_2= 0.39 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 1.04 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 1.04 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00199 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00432 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00432 if `cond1' & voltage_cat==3

//unbundled
replace pf_adjust= 0.20 if `cond2' & voltage_cat==3
replace pf_adjust= 0.20 if `cond2' & voltage_cat==2
replace pf_adjust= 0.18 if `cond2' & voltage_cat==1
replace pf_adjust= 0.18 if `cond2' & voltage_cat==0
replace voltage_dis_load_1 = 0.19 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 6.25 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.25 if `cond2' & voltage_cat==3
/*
replace interruptible_credit = 0.00933 if `cond2'
replace ee_charge = 10.21374 if `cond2' * voltage_cat==0
replace ee_charge = 9.99551 if `cond2' * voltage_cat==1
replace ee_charge = 9.99551 if `cond2' * voltage_cat==2
replace ee_charge = 9.63234 if `cond2' * voltage_cat==3
*/

replace old= 0 if missing(old)


** Jun 1 2010

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-SOP" if missing(old)
replace rate_start_date = mdy(6,1,2010) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.08522 if `cond1'
replace energycharge_sum_off_pk = 0.04944 if `cond1'
replace energycharge_sum_sup_off_pk = 0.01914 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_off_pk = 0.06040 if `cond1'
replace energycharge_win_sup_off_pk= 0.01865 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.03763 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.03763 if `cond1'
replace energycharge_dwr_sum_sup_off_pk = 0.03763 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_off_pk = 0.03763 if `cond1'
replace energycharge_dwr_win_sup_off_pk= 0.03763 if `cond1'

replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 23.54 if `cond1'
replace demandcharge_sum_off_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'
replace demandcharge_win_sup_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.01775 if `cond2'
replace energycharge_sum_off_pk = 0.01775 if `cond2'
replace energycharge_sum_sup_off_pk = 0.01775 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_off_pk = 0.01775 if `cond2'
replace energycharge_win_sup_off_pk= 0.01775 if `cond2'
replace customercharge = 122.94 if `cond2'
replace demandcharge = 7.83 if `cond2'

** second piece

//bundled
replace voltage_dis_load_2= 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 1.04 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 1.04 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled
replace pf_adjust= 0.32 if `cond2' & voltage_cat==3
replace pf_adjust= 0.32 if `cond2' & voltage_cat==2
replace pf_adjust= 0.27 if `cond2' & voltage_cat==1
replace pf_adjust= 0.27 if `cond2' & voltage_cat==0
replace voltage_dis_load_1 = 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.08 if `cond2' & voltage_cat==3
/*
replace interruptible_credit = 0.00933 if `cond2'
replace ee_charge = 10.21374 if `cond2' * voltage_cat==0
replace ee_charge = 9.99551 if `cond2' * voltage_cat==1
replace ee_charge = 9.99551 if `cond2' * voltage_cat==2
replace ee_charge = 9.63234 if `cond2' * voltage_cat==3
*/

replace old= 0 if missing(old)


** Mar 1 2010

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-SOP" if missing(old)
replace rate_start_date = mdy(3,1,2010) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.08522 if `cond1'
replace energycharge_sum_off_pk = 0.04944 if `cond1'
replace energycharge_sum_sup_off_pk = 0.01914 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_off_pk = 0.06040 if `cond1'
replace energycharge_win_sup_off_pk= 0.01865 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.03763 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.03763 if `cond1'
replace energycharge_dwr_sum_sup_off_pk = 0.03763 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_off_pk = 0.03763 if `cond1'
replace energycharge_dwr_win_sup_off_pk= 0.03763 if `cond1'

replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 23.54 if `cond1'
replace demandcharge_sum_off_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'
replace demandcharge_win_sup_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.01829 if `cond2'
replace energycharge_sum_off_pk = 0.01829 if `cond2'
replace energycharge_sum_sup_off_pk = 0.01829 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_off_pk = 0.01829 if `cond2'
replace energycharge_win_sup_off_pk= 0.01829 if `cond2'
replace customercharge = 122.94 if `cond2'
replace demandcharge = 7.81 if `cond2'

** second piece

//bundled
replace voltage_dis_load_2= 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.54 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.55 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled
replace pf_adjust= 0.32 if `cond2' & voltage_cat==3
replace pf_adjust= 0.32 if `cond2' & voltage_cat==2
replace pf_adjust= 0.27 if `cond2' & voltage_cat==1
replace pf_adjust= 0.27 if `cond2' & voltage_cat==0
replace voltage_dis_load_1 = 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.08 if `cond2' & voltage_cat==3
/*
replace interruptible_credit = 0.00933 if `cond2'
replace ee_charge = 10.21374 if `cond2' * voltage_cat==0
replace ee_charge = 9.99551 if `cond2' * voltage_cat==1
replace ee_charge = 9.99551 if `cond2' * voltage_cat==2
replace ee_charge = 9.63234 if `cond2' * voltage_cat==3
*/

replace old= 0 if missing(old)


** Jan 1 2010

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-SOP" if missing(old)
replace rate_start_date = mdy(1,1,2010) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.08865 if `cond1'
replace energycharge_sum_off_pk = 0.05167 if `cond1'
replace energycharge_sum_sup_off_pk = 0.02034 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_off_pk = 0.06300 if `cond1'
replace energycharge_win_sup_off_pk= 0.01983 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.03763 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.03763 if `cond1'
replace energycharge_dwr_sum_sup_off_pk = 0.03763 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_off_pk = 0.03763 if `cond1'
replace energycharge_dwr_win_sup_off_pk= 0.03763 if `cond1'

replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 23.54 if `cond1'
replace demandcharge_sum_off_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'
replace demandcharge_win_sup_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.01294 if `cond2'
replace energycharge_sum_off_pk = 0.01294 if `cond2'
replace energycharge_sum_sup_off_pk = 0.01294 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_off_pk = 0.01294 if `cond2'
replace energycharge_win_sup_off_pk= 0.01294 if `cond2'
replace customercharge = 113.76 if `cond2'
replace demandcharge = 6.83 if `cond2'

** second piece

//bundled
replace voltage_dis_load_2= 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.54 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.55 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled
replace pf_adjust= 0.32 if `cond2' & voltage_cat==3
replace pf_adjust= 0.32 if `cond2' & voltage_cat==2
replace pf_adjust= 0.27 if `cond2' & voltage_cat==1
replace pf_adjust= 0.27 if `cond2' & voltage_cat==0
replace voltage_dis_load_1 = 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.08 if `cond2' & voltage_cat==3
/*
replace interruptible_credit = 0.00933 if `cond2'
replace ee_charge = 10.21374 if `cond2' * voltage_cat==0
replace ee_charge = 9.99551 if `cond2' * voltage_cat==1
replace ee_charge = 9.99551 if `cond2' * voltage_cat==2
replace ee_charge = 9.63234 if `cond2' * voltage_cat==3
*/

replace old= 0 if missing(old)


** Jun 1 2011

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-SOP" if missing(old)
replace rate_start_date = mdy(6,1,2011) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.07333 if `cond1'
replace energycharge_sum_off_pk = 0.04199 if `cond1'
replace energycharge_sum_sup_off_pk = 0.01546 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_off_pk = 0.05159 if `cond1'
replace energycharge_win_sup_off_pk= 0.01503 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.03952 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.03952 if `cond1'
replace energycharge_dwr_sum_sup_off_pk = 0.03952 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_off_pk = 0.03952 if `cond1'
replace energycharge_dwr_win_sup_off_pk= 0.03952 if `cond1'

replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 20.62 if `cond1'
replace demandcharge_sum_off_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'
replace demandcharge_win_sup_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.01727 if `cond2'
replace energycharge_sum_off_pk = 0.01727 if `cond2'
replace energycharge_sum_sup_off_pk = 0.01727 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_off_pk = 0.01727 if `cond2'
replace energycharge_win_sup_off_pk= 0.01727 if `cond2'
replace customercharge = 123.84 if `cond2'
replace demandcharge = 7.87 if `cond2'

** second piece

//bundled
replace voltage_dis_load_2= 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.54 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.55 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled
replace pf_adjust= 0.32 if `cond2' & voltage_cat==3
replace pf_adjust= 0.32 if `cond2' & voltage_cat==2
replace pf_adjust= 0.27 if `cond2' & voltage_cat==1
replace pf_adjust= 0.27 if `cond2' & voltage_cat==0
replace voltage_dis_load_1 = 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.08 if `cond2' & voltage_cat==3
/*
replace interruptible_credit = 0.00933 if `cond2'
replace ee_charge = 10.21374 if `cond2' * voltage_cat==0
replace ee_charge = 9.99551 if `cond2' * voltage_cat==1
replace ee_charge = 9.99551 if `cond2' * voltage_cat==2
replace ee_charge = 9.63234 if `cond2' * voltage_cat==3
*/

replace old= 0 if missing(old)


** Mar 1 2011

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-SOP" if missing(old)
replace rate_start_date = mdy(3,1,2011) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.08522 if `cond1'
replace energycharge_sum_off_pk = 0.04944 if `cond1'
replace energycharge_sum_sup_off_pk = 0.01914 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_off_pk = 0.06040 if `cond1'
replace energycharge_win_sup_off_pk= 0.01865 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.03952 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.03952 if `cond1'
replace energycharge_dwr_sum_sup_off_pk = 0.03952 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_off_pk = 0.03952 if `cond1'
replace energycharge_dwr_win_sup_off_pk= 0.03952 if `cond1'

replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 23.54 if `cond1'
replace demandcharge_sum_off_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'
replace demandcharge_win_sup_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.01800 if `cond2'
replace energycharge_sum_off_pk = 0.01800 if `cond2'
replace energycharge_sum_sup_off_pk = 0.01800 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_off_pk = 0.01800 if `cond2'
replace energycharge_win_sup_off_pk= 0.01800 if `cond2'
replace customercharge = 122.94 if `cond2'
replace demandcharge = 7.90 if `cond2'

replace old= 0 if missing(old)


** Jan 1 2011

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-SOP" if missing(old)
replace rate_start_date = mdy(1,1,2011) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.08522 if `cond1'
replace energycharge_sum_off_pk = 0.04944 if `cond1'
replace energycharge_sum_sup_off_pk = 0.01914 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_off_pk = 0.06040 if `cond1'
replace energycharge_win_sup_off_pk= 0.01865 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.03952 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.03952 if `cond1'
replace energycharge_dwr_sum_sup_off_pk = 0.03952 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_off_pk = 0.03952 if `cond1'
replace energycharge_dwr_win_sup_off_pk= 0.03952 if `cond1'

replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 23.54 if `cond1'
replace demandcharge_sum_off_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'
replace demandcharge_win_sup_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.01800 if `cond2'
replace energycharge_sum_off_pk = 0.01800 if `cond2'
replace energycharge_sum_sup_off_pk = 0.01800 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_off_pk = 0.01800 if `cond2'
replace energycharge_win_sup_off_pk= 0.01800 if `cond2'
replace customercharge = 122.94 if `cond2'
replace demandcharge = 7.83 if `cond2'

** second piece

//bundled
replace voltage_dis_load_2= 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.54 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.55 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled
replace pf_adjust= 0.32 if `cond2' & voltage_cat==3
replace pf_adjust= 0.32 if `cond2' & voltage_cat==2
replace pf_adjust= 0.27 if `cond2' & voltage_cat==1
replace pf_adjust= 0.27 if `cond2' & voltage_cat==0
replace voltage_dis_load_1 = 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.08 if `cond2' & voltage_cat==3
/*
replace interruptible_credit = 0.00933 if `cond2'
replace ee_charge = 10.21374 if `cond2' * voltage_cat==0
replace ee_charge = 9.99551 if `cond2' * voltage_cat==1
replace ee_charge = 9.99551 if `cond2' * voltage_cat==2
replace ee_charge = 9.63234 if `cond2' * voltage_cat==3
*/

replace old= 0 if missing(old)


** Oct 1 2012

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-SOP" if missing(old)
replace rate_start_date = mdy(10,1,2012) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.07137 if `cond1'
replace energycharge_sum_off_pk = 0.04092 if `cond1'
replace energycharge_sum_sup_off_pk = 0.01515 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_off_pk = 0.05025 if `cond1'
replace energycharge_win_sup_off_pk= 0.01473 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00463 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00463 if `cond1'
replace energycharge_dwr_sum_sup_off_pk = 0.00463 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_off_pk = 0.00463 if `cond1'
replace energycharge_dwr_win_sup_off_pk= 0.00463 if `cond1'

replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 20.03 if `cond1'
replace demandcharge_sum_off_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'
replace demandcharge_win_sup_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.01729 if `cond2'
replace energycharge_sum_off_pk = 0.01729 if `cond2'
replace energycharge_sum_sup_off_pk = 0.01729 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_off_pk = 0.01729 if `cond2'
replace energycharge_win_sup_off_pk= 0.01729 if `cond2'
replace customercharge = 126.47 if `cond2'
replace demandcharge = 8.40 if `cond2'

** second piece
/*
//bundled
replace voltage_dis_load_2= 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.54 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.55 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled
replace pf_adjust= 0.32 if `cond2' & voltage_cat==3
replace pf_adjust= 0.32 if `cond2' & voltage_cat==2
replace pf_adjust= 0.27 if `cond2' & voltage_cat==1
replace pf_adjust= 0.27 if `cond2' & voltage_cat==0
replace voltage_dis_load_1 = 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.08 if `cond2' & voltage_cat==3

replace interruptible_credit = 0.00933 if `cond2'
replace ee_charge = 10.21374 if `cond2' * voltage_cat==0
replace ee_charge = 9.99551 if `cond2' * voltage_cat==1
replace ee_charge = 9.99551 if `cond2' * voltage_cat==2
replace ee_charge = 9.63234 if `cond2' * voltage_cat==3
*/

replace old= 0 if missing(old)

** Aug 1 2012

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-SOP" if missing(old)
replace rate_start_date = mdy(6,1,2012) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.07137 if `cond1'
replace energycharge_sum_off_pk = 0.04092 if `cond1'
replace energycharge_sum_sup_off_pk = 0.01515 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_off_pk = 0.05025 if `cond1'
replace energycharge_win_sup_off_pk= 0.01473 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00463 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00463 if `cond1'
replace energycharge_dwr_sum_sup_off_pk = 0.00463 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_off_pk = 0.00463 if `cond1'
replace energycharge_dwr_win_sup_off_pk= 0.00463 if `cond1'

replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 20.03 if `cond1'
replace demandcharge_sum_off_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'
replace demandcharge_win_sup_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.01724 if `cond2'
replace energycharge_sum_off_pk = 0.01724 if `cond2'
replace energycharge_sum_sup_off_pk = 0.01724 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_off_pk = 0.01724 if `cond2'
replace energycharge_win_sup_off_pk= 0.01724 if `cond2'
replace customercharge = 123.47 if `cond2'
replace demandcharge = 8.07 if `cond2'

** second piece

//bundled
replace voltage_dis_load_2= 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.54 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.55 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled
replace pf_adjust= 0.32 if `cond2' & voltage_cat==3
replace pf_adjust= 0.32 if `cond2' & voltage_cat==2
replace pf_adjust= 0.27 if `cond2' & voltage_cat==1
replace pf_adjust= 0.27 if `cond2' & voltage_cat==0
replace voltage_dis_load_1 = 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.08 if `cond2' & voltage_cat==3

/*
replace interruptible_credit = 0.00933 if `cond2'
replace ee_charge = 10.21374 if `cond2' * voltage_cat==0
replace ee_charge = 9.99551 if `cond2' * voltage_cat==1
replace ee_charge = 9.99551 if `cond2' * voltage_cat==2
replace ee_charge = 9.63234 if `cond2' * voltage_cat==3
*/

replace old= 0 if missing(old)


** Jun 1 2012

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-SOP" if missing(old)
replace rate_start_date = mdy(6,1,2012) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.07333 if `cond1'
replace energycharge_sum_off_pk = 0.04199 if `cond1'
replace energycharge_sum_sup_off_pk = 0.01546 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_off_pk = 0.05159 if `cond1'
replace energycharge_win_sup_off_pk= 0.01503 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00463 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00463 if `cond1'
replace energycharge_dwr_sum_sup_off_pk = 0.00463 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_off_pk = 0.00463 if `cond1'
replace energycharge_dwr_win_sup_off_pk= 0.00463 if `cond1'

replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 20.62 if `cond1'
replace demandcharge_sum_off_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'
replace demandcharge_win_sup_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.01720 if `cond2'
replace energycharge_sum_off_pk = 0.01720 if `cond2'
replace energycharge_sum_sup_off_pk = 0.01720 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_off_pk = 0.01720 if `cond2'
replace energycharge_win_sup_off_pk= 0.01720 if `cond2'
replace customercharge = 123.84 if `cond2'
replace demandcharge = 7.80 if `cond2'

** second piece

//bundled
replace voltage_dis_load_2= 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.54 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.55 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled
replace pf_adjust= 0.32 if `cond2' & voltage_cat==3
replace pf_adjust= 0.32 if `cond2' & voltage_cat==2
replace pf_adjust= 0.27 if `cond2' & voltage_cat==1
replace pf_adjust= 0.27 if `cond2' & voltage_cat==0
replace voltage_dis_load_1 = 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.08 if `cond2' & voltage_cat==3

/*
replace interruptible_credit = 0.00933 if `cond2'
replace ee_charge = 10.21374 if `cond2' * voltage_cat==0
replace ee_charge = 9.99551 if `cond2' * voltage_cat==1
replace ee_charge = 9.99551 if `cond2' * voltage_cat==2
replace ee_charge = 9.63234 if `cond2' * voltage_cat==3
*/

replace old= 0 if missing(old)


** Jan 1 2012

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-SOP" if missing(old)
replace rate_start_date = mdy(1,1,2012) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.07333 if `cond1'
replace energycharge_sum_off_pk = 0.04199 if `cond1'
replace energycharge_sum_sup_off_pk = 0.01546 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_off_pk = 0.05159 if `cond1'
replace energycharge_win_sup_off_pk= 0.01503 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00593 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00593 if `cond1'
replace energycharge_dwr_sum_sup_off_pk = 0.00593 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_off_pk = 0.00593 if `cond1'
replace energycharge_dwr_win_sup_off_pk= 0.00593 if `cond1'

replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 20.62 if `cond1'
replace demandcharge_sum_off_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'
replace demandcharge_win_sup_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.01716 if `cond2'
replace energycharge_sum_off_pk = 0.01716 if `cond2'
replace energycharge_sum_sup_off_pk = 0.01716 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_off_pk = 0.01716 if `cond2'
replace energycharge_win_sup_off_pk= 0.01716 if `cond2'
replace customercharge = 123.84 if `cond2'
replace demandcharge = 7.80 if `cond2'

** second piece

//bundled
replace voltage_dis_load_2= 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.54 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.55 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled
replace pf_adjust= 0.32 if `cond2' & voltage_cat==3
replace pf_adjust= 0.32 if `cond2' & voltage_cat==2
replace pf_adjust= 0.27 if `cond2' & voltage_cat==1
replace pf_adjust= 0.27 if `cond2' & voltage_cat==0
replace voltage_dis_load_1 = 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.08 if `cond2' & voltage_cat==3

/*
replace interruptible_credit = 0.00933 if `cond2'
replace ee_charge = 10.21374 if `cond2' * voltage_cat==0
replace ee_charge = 9.99551 if `cond2' * voltage_cat==1
replace ee_charge = 9.99551 if `cond2' * voltage_cat==2
replace ee_charge = 9.63234 if `cond2' * voltage_cat==3
*/

replace old= 0 if missing(old)

** Apr 12 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-SOP" if missing(old)
replace rate_start_date = mdy(4,12,2013) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.07706 if `cond1'
replace energycharge_sum_off_pk = 0.04235 if `cond1'
replace energycharge_sum_sup_off_pk = 0.01296 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_off_pk = 0.05298 if `cond1'
replace energycharge_win_sup_off_pk= 0.01249 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_sup_off_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_off_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_sup_off_pk= 0.00097 if `cond1'

replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 22.84 if `cond1'
replace demandcharge_sum_off_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'
replace demandcharge_win_sup_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.01691 if `cond2'
replace energycharge_sum_off_pk = 0.01691 if `cond2'
replace energycharge_sum_sup_off_pk = 0.01691 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_off_pk = 0.01691 if `cond2'
replace energycharge_win_sup_off_pk= 0.01691 if `cond2'
replace customercharge = 134.42 if `cond2'
replace demandcharge = 8.83 if `cond2'

** second piece

//bundled
replace voltage_dis_load_2= 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.54 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.55 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled
replace pf_adjust= 0.32 if `cond2' & voltage_cat==3
replace pf_adjust= 0.32 if `cond2' & voltage_cat==2
replace pf_adjust= 0.27 if `cond2' & voltage_cat==1
replace pf_adjust= 0.27 if `cond2' & voltage_cat==0
replace voltage_dis_load_1 = 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.08 if `cond2' & voltage_cat==3

/*
replace interruptible_credit = 0.00933 if `cond2'
replace ee_charge = 10.21374 if `cond2' * voltage_cat==0
replace ee_charge = 9.99551 if `cond2' * voltage_cat==1
replace ee_charge = 9.99551 if `cond2' * voltage_cat==2
replace ee_charge = 9.63234 if `cond2' * voltage_cat==3
*/

replace old= 0 if missing(old)

** Apr 1 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-SOP" if missing(old)
replace rate_start_date = mdy(4,1,2013) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.07706 if `cond1'
replace energycharge_sum_off_pk = 0.04235 if `cond1'
replace energycharge_sum_sup_off_pk = 0.01296 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_off_pk = 0.05298 if `cond1'
replace energycharge_win_sup_off_pk= 0.01249 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_sup_off_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_off_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_sup_off_pk= 0.00097 if `cond1'

replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 22.84 if `cond1'
replace demandcharge_sum_off_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'
replace demandcharge_win_sup_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.01708 if `cond2'
replace energycharge_sum_off_pk = 0.01708 if `cond2'
replace energycharge_sum_sup_off_pk = 0.01708 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_off_pk = 0.01708 if `cond2'
replace energycharge_win_sup_off_pk= 0.01708 if `cond2'
replace customercharge = 134.42 if `cond2'
replace demandcharge = 8.83 if `cond2'

** second piece

//bundled
replace voltage_dis_load_2= 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.54 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.55 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled
replace pf_adjust= 0.34 if `cond2' & voltage_cat==3
replace pf_adjust= 0.34 if `cond2' & voltage_cat==2
replace pf_adjust= 0.51 if `cond2' & voltage_cat==1
replace pf_adjust= 0.51 if `cond2' & voltage_cat==0
replace voltage_dis_load_1 = 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.08 if `cond2' & voltage_cat==3

/*
replace interruptible_credit = 0.00933 if `cond2'
replace ee_charge = 10.21374 if `cond2' * voltage_cat==0
replace ee_charge = 9.99551 if `cond2' * voltage_cat==1
replace ee_charge = 9.99551 if `cond2' * voltage_cat==2
replace ee_charge = 9.63234 if `cond2' * voltage_cat==3
*/

replace old= 0 if missing(old)


** Jan 1 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-SOP" if missing(old)
replace rate_start_date = mdy(1,1,2013) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.07706 if `cond1'
replace energycharge_sum_off_pk = 0.04235 if `cond1'
replace energycharge_sum_sup_off_pk = 0.01296 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_off_pk = 0.05298 if `cond1'
replace energycharge_win_sup_off_pk= 0.01249 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_sup_off_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_off_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_sup_off_pk= 0.00097 if `cond1'

replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 22.84 if `cond1'
replace demandcharge_sum_off_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'
replace demandcharge_win_sup_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.01691 if `cond2'
replace energycharge_sum_off_pk = 0.01691 if `cond2'
replace energycharge_sum_sup_off_pk = 0.01691 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_off_pk = 0.01691 if `cond2'
replace energycharge_win_sup_off_pk= 0.01691 if `cond2'
replace customercharge = 134.42 if `cond2'
replace demandcharge = 8.83 if `cond2'

** second piece

//bundled
replace voltage_dis_load_2= 0.19 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.54 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.55 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00132 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00294 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00298 if `cond1' & voltage_cat==3

//unbundled
replace pf_adjust= 0.32 if `cond2' & voltage_cat==3
replace pf_adjust= 0.32 if `cond2' & voltage_cat==2
replace pf_adjust= 0.27 if `cond2' & voltage_cat==1
replace pf_adjust= 0.27 if `cond2' & voltage_cat==0
replace voltage_dis_load_1 = 0.10 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 3.04 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.08 if `cond2' & voltage_cat==3

/*
replace interruptible_credit = 0.00933 if `cond2'
replace ee_charge = 10.21374 if `cond2' * voltage_cat==0
replace ee_charge = 9.99551 if `cond2' * voltage_cat==1
replace ee_charge = 9.99551 if `cond2' * voltage_cat==2
replace ee_charge = 9.63234 if `cond2' * voltage_cat==3
*/

replace old= 0 if missing(old)


*****************************
*** 8. Schedule- TOU-PA-5 ***
*****************************


** Oct 1 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-5" if missing(old)
replace rate_start_date = mdy(10,1,2009) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.10756 if `cond1'
replace energycharge_sum_m_pk = 0.06544 if `cond1'
replace energycharge_sum_off_pk = 0.03371 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.05384 if `cond1'
replace energycharge_win_off_pk= 0.03070 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.06225 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.06225 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.06225 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.06225 if `cond1'
replace energycharge_dwr_win_off_pk= 0.06225 if `cond1'

replace demandcharge = 0.0 if `cond1'
replace minimum_charge_sum = 29.46 if `cond1'
replace minimum_charge_win = 11.99 if `cond1'
replace demandcharge_sum_on_pk = 14.28 if `cond1'
replace demandcharge_sum_mid_pk = 4.01 if `cond1'
replace demandcharge_win_m_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.01221 if `cond2'
replace energycharge_sum_m_pk = 0.01221 if `cond2'
replace energycharge_sum_off_pk = 0.01221 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01221 if `cond2'
replace energycharge_win_off_pk= 0.01221 if `cond2'
replace customercharge = 115.57 if `cond2'
replace minimum_charge_sum = 10.39 if `cond2'
replace minimum_charge_win = 8.88 if `cond2'
replace demandcharge = 9.35 if `cond2'
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.27 if `cond2' & voltage_cat==0

** second piece

//bundled
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.31 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.86 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.87 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00134 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00299 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00302 if `cond1' & voltage_cat==3

//unbundled
replace voltage_dis_load_1 = 0.14 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 4.50 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 7.76 if `cond2' & voltage_cat==3
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==3
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

** Jun 1 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-5" if missing(old)
replace rate_start_date = mdy(6,1,2009) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.06734 if `cond1'
replace energycharge_sum_m_pk = 0.04952 if `cond1'
replace energycharge_sum_off_pk = 0.01888 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.05365 if `cond1'
replace energycharge_win_off_pk= 0.02138 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.06225 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.06225 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.06225 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.06225 if `cond1'
replace energycharge_dwr_win_off_pk= 0.06225 if `cond1'

replace minimum_charge_sum = 29.46 if `cond1'
replace minimum_charge_win = 11.99 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 10.47 if `cond1'
replace demandcharge_sum_mid_pk = 4.01 if `cond1'
replace demandcharge_sum_off_pk = -999 if `cond1'
replace demandcharge_win_on_pk = -999 if `cond1'
replace demandcharge_win_m_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.01089 if `cond2'
replace energycharge_sum_m_pk = 0.01089 if `cond2'
replace energycharge_sum_off_pk = 0.01089 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01089 if `cond2'
replace energycharge_win_off_pk= 0.01089 if `cond2'
replace customercharge = 96.31 if `cond2'
replace minimum_charge_sum = 10.68 if `cond2'
replace minimum_charge_win = 9.16 if `cond2'
replace demandcharge = 11.86 if `cond2'
replace pf_adjust = 0.20 if `cond2' & voltage_cat==3
replace pf_adjust = 0.20 if `cond2' & voltage_cat==2
replace pf_adjust = 0.18 if `cond2' & voltage_cat==1
replace pf_adjust = 0.18 if `cond2' & voltage_cat==0

** second piece

//bundled
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.38 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 1.04 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 1.04 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00198 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00431 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00431 if `cond1' & voltage_cat==3

//unbundled
replace voltage_dis_load_1 = 0.19 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 6.24 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.24 if `cond2' & voltage_cat==3
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==3
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


** Apr 4 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-5" if missing(old)
replace rate_start_date = mdy(4,4,2009) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.06864 if `cond1'
replace energycharge_sum_m_pk = 0.05057 if `cond1'
replace energycharge_sum_off_pk = 0.01953 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.05476 if `cond1'
replace energycharge_win_off_pk= 0.02206 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.06210 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.06210 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.06210 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.06210 if `cond1'
replace energycharge_dwr_win_off_pk= 0.06210 if `cond1'

replace minimum_charge_sum = 29.46 if `cond1'
replace minimum_charge_win = 11.99 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 10.61 if `cond1'
replace demandcharge_sum_mid_pk = 0.00 if `cond1'
replace demandcharge_sum_off_pk = -999 if `cond1'
replace demandcharge_win_on_pk = -999 if `cond1'
replace demandcharge_win_m_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.01038 if `cond2'
replace energycharge_sum_m_pk = 0.01038 if `cond2'
replace energycharge_sum_off_pk = 0.01038 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01038 if `cond2'
replace energycharge_win_off_pk= 0.01038 if `cond2'
replace customercharge = 96.31 if `cond2'
replace minimum_charge_sum = 10.68 if `cond2'
replace minimum_charge_win = 9.16 if `cond2'
replace demandcharge = 11.86 if `cond2'
replace pf_adjust = 0.20 if `cond2' & voltage_cat==3
replace pf_adjust = 0.20 if `cond2' & voltage_cat==2
replace pf_adjust = 0.18 if `cond2' & voltage_cat==1
replace pf_adjust = 0.18 if `cond2' & voltage_cat==0

** second piece

//bundled
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.39 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 1.05 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 1.05 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00201 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00437 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00437 if `cond1' & voltage_cat==3

//unbundled
replace voltage_dis_load_1 = 0.19 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 6.24 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.24 if `cond2' & voltage_cat==3
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==3
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


** Mar 1 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-5" if missing(old)
replace rate_start_date = mdy(3,1,2009) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.06741 if `cond1'
replace energycharge_sum_m_pk = 0.04956 if `cond1'
replace energycharge_sum_off_pk = 0.01890 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.05371 if `cond1'
replace energycharge_win_off_pk= 0.02140 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.06231 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.06231 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.06231 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.06231 if `cond1'
replace energycharge_dwr_win_off_pk= 0.06231 if `cond1'

replace minimum_charge_sum = 29.46 if `cond1'
replace minimum_charge_win = 11.99 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 10.48 if `cond1'
replace demandcharge_sum_mid_pk = 0.00 if `cond1'
replace demandcharge_sum_off_pk = -999 if `cond1'
replace demandcharge_win_on_pk = -999 if `cond1'
replace demandcharge_win_m_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.01058 if `cond2'
replace energycharge_sum_m_pk = 0.01058 if `cond2'
replace energycharge_sum_off_pk = 0.01058 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01058 if `cond2'
replace energycharge_win_off_pk= 0.01058 if `cond2'
replace customercharge = 82.98 if `cond2'
replace minimum_charge_sum = 10.68 if `cond2'
replace minimum_charge_win = 9.16 if `cond2'
replace demandcharge = 10.50 if `cond2'
replace pf_adjust = 0.20 if `cond2' & voltage_cat==3
replace pf_adjust = 0.20 if `cond2' & voltage_cat==2
replace pf_adjust = 0.18 if `cond2' & voltage_cat==1
replace pf_adjust = 0.18 if `cond2' & voltage_cat==0

** second piece

//bundled
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.39 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 1.04 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 1.04 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00199 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00432 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00432 if `cond1' & voltage_cat==3

//unbundled
replace voltage_dis_load_1 = 0.19 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 6.24 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.24 if `cond2' & voltage_cat==3
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==3
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


** Jan 1 2009

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-5" if missing(old)
replace rate_start_date = mdy(1,1,2009) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.05887 if `cond1'
replace energycharge_sum_m_pk = 0.04115 if `cond1'
replace energycharge_sum_off_pk = 0.01073 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.04528 if `cond1'
replace energycharge_win_off_pk= 0.01321 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.08451 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.08451 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.08451 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.08451 if `cond1'
replace energycharge_dwr_win_off_pk= 0.08451 if `cond1'

replace minimum_charge_sum = 29.46 if `cond1'
replace minimum_charge_win = 11.99 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 10.49 if `cond1'
replace demandcharge_sum_mid_pk = 0.00 if `cond1'
replace demandcharge_sum_off_pk = -999 if `cond1'
replace demandcharge_win_on_pk = -999 if `cond1'
replace demandcharge_win_m_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.01349 if `cond2'
replace energycharge_sum_m_pk = 0.01349 if `cond2'
replace energycharge_sum_off_pk = 0.01349 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01349 if `cond2'
replace energycharge_win_off_pk= 0.01349 if `cond2'
replace customercharge = 85.06 if `cond2'
replace minimum_charge_sum = 9.93 if `cond2'
replace minimum_charge_win = 8.40 if `cond2'
replace demandcharge = 9.93 if `cond2'
replace pf_adjust = 0.20 if `cond2' & voltage_cat==3
replace pf_adjust = 0.20 if `cond2' & voltage_cat==2
replace pf_adjust = 0.18 if `cond2' & voltage_cat==1
replace pf_adjust = 0.18 if `cond2' & voltage_cat==0

** second piece

//bundled
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.39 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 1.04 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 1.04 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00199 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00432 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00432 if `cond1' & voltage_cat==3

//unbundled
replace voltage_dis_load_1 = 0.19 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 6.24 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 6.24 if `cond2' & voltage_cat==3
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==3
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


** Jun 1 2010

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-5" if missing(old)
replace rate_start_date = mdy(6,1,2010) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.10352 if `cond1'
replace energycharge_sum_m_pk = 0.06277 if `cond1'
replace energycharge_sum_off_pk = 0.03208 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.05155 if `cond1'
replace energycharge_win_off_pk= 0.02916 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.03763 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.03763 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.03763 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.03763 if `cond1'
replace energycharge_dwr_win_off_pk= 0.03763 if `cond1'

replace minimum_charge_sum = 29.46 if `cond1'
replace minimum_charge_win = 11.99 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 13.81 if `cond1'
replace demandcharge_sum_mid_pk = 3.88 if `cond1'
replace demandcharge_win_m_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.01758 if `cond2'
replace energycharge_sum_m_pk = 0.01758 if `cond2'
replace energycharge_sum_off_pk = 0.01758 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01758 if `cond2'
replace energycharge_win_off_pk= 0.01758 if `cond2'
replace customercharge = 124.89 if `cond2'
replace minimum_charge_sum = 11.34 if `cond2'
replace minimum_charge_win = 9.84 if `cond2'
replace demandcharge = 10.96 if `cond2'
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.27 if `cond2' & voltage_cat==0

** second piece

//bundled
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.31 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.86 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.87 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00134 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00299 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00302 if `cond1' & voltage_cat==3

//unbundled
replace voltage_dis_load_1 = 0.14 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 4.50 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 7.76 if `cond2' & voltage_cat==3
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==3
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


** Jan 1 2010

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-5" if missing(old)
replace rate_start_date = mdy(1,1,2010) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.10756 if `cond1'
replace energycharge_sum_m_pk = 0.06544 if `cond1'
replace energycharge_sum_off_pk = 0.03371 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.05384 if `cond1'
replace energycharge_win_off_pk= 0.03070 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.03763 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.03763 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.03763 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.03763 if `cond1'
replace energycharge_dwr_win_off_pk= 0.03763 if `cond1'

replace minimum_charge_sum = 29.46 if `cond1'
replace minimum_charge_win = 11.99 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 14.28 if `cond1'
replace demandcharge_sum_mid_pk = 4.01 if `cond1'
replace demandcharge_win_m_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.01309 if `cond2'
replace energycharge_sum_m_pk = 0.01309 if `cond2'
replace energycharge_sum_off_pk = 0.01309 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01309 if `cond2'
replace energycharge_win_off_pk= 0.01309 if `cond2'
replace customercharge = 115.57 if `cond2'
replace minimum_charge_sum = 10.42 if `cond2'
replace minimum_charge_win = 8.90 if `cond2'
replace demandcharge = 9.37 if `cond2'
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.27 if `cond2' & voltage_cat==0

** second piece

//bundled
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.31 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.86 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.87 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00134 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00299 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00302 if `cond1' & voltage_cat==3

//unbundled
replace voltage_dis_load_1 = 0.14 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 4.50 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 7.76 if `cond2' & voltage_cat==3
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==3
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


** Mar 1 2010

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-5" if missing(old)
replace rate_start_date = mdy(3,1,2010) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.10352 if `cond1'
replace energycharge_sum_m_pk = 0.06277 if `cond1'
replace energycharge_sum_off_pk = 0.03208 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.05155 if `cond1'
replace energycharge_win_off_pk= 0.02916 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.03763 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.03763 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.03763 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.03763 if `cond1'
replace energycharge_dwr_win_off_pk= 0.03763 if `cond1'

replace minimum_charge_sum = 29.46 if `cond1'
replace minimum_charge_win = 11.99 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 13.81 if `cond1'
replace demandcharge_sum_mid_pk = 3.88 if `cond1'
replace demandcharge_win_m_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.01812 if `cond2'
replace energycharge_sum_m_pk = 0.01812 if `cond2'
replace energycharge_sum_off_pk = 0.01812 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01812 if `cond2'
replace energycharge_win_off_pk= 0.01812 if `cond2'
replace customercharge = 124.89 if `cond2'
replace minimum_charge_sum = 11.31 if `cond2'
replace minimum_charge_win = 9.81 if `cond2'
replace demandcharge = 10.92 if `cond2'
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.27 if `cond2' & voltage_cat==0

** second piece

//bundled
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.31 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.86 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.87 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00134 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00299 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00302 if `cond1' & voltage_cat==3

//unbundled
replace voltage_dis_load_1 = 0.14 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 4.50 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 7.76 if `cond2' & voltage_cat==3
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==3
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


** Jun 1 2011

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-5" if missing(old)
replace rate_start_date = mdy(6,1,2011) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.07177 if `cond1'
replace energycharge_sum_m_pk = 0.04237 if `cond1'
replace energycharge_sum_off_pk = 0.02022 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.03427 if `cond1'
replace energycharge_win_off_pk= 0.01811 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.03952 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.03952 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.03952 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.03952 if `cond1'
replace energycharge_dwr_win_off_pk= 0.03952 if `cond1'

replace minimum_charge_sum = 29.46 if `cond1'
replace minimum_charge_win = 11.99 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 9.97 if `cond1'
replace demandcharge_sum_mid_pk = 2.80 if `cond1'
replace demandcharge_win_m_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.02601 if `cond2'
replace energycharge_sum_m_pk = 0.02601 if `cond2'
replace energycharge_sum_off_pk = 0.02601 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02601 if `cond2'
replace energycharge_win_off_pk= 0.02601 if `cond2'
replace customercharge = 125.81 if `cond2'
replace minimum_charge_sum = 11.49 if `cond2'
replace minimum_charge_win = 10.00 if `cond2'
replace demandcharge = 10.83 if `cond2'
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.27 if `cond2' & voltage_cat==0

** second piece

//bundled
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.31 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.86 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.87 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00134 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00299 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00302 if `cond1' & voltage_cat==3

//unbundled
replace voltage_dis_load_1 = 0.14 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 4.50 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 7.76 if `cond2' & voltage_cat==3
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==3
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


** Jul 10 2011

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-5" if missing(old)
replace rate_start_date = mdy(7,10,2011) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

** second piece

//bundled
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.31 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.86 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.87 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00134 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00299 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00302 if `cond1' & voltage_cat==3

//unbundled
replace voltage_dis_load_1 = 0.14 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 4.50 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 7.76 if `cond2' & voltage_cat==3
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==3
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


** Jan 1 2011

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-5" if missing(old)
replace rate_start_date = mdy(1,1,2011) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.10352 if `cond1'
replace energycharge_sum_m_pk = 0.06277 if `cond1'
replace energycharge_sum_off_pk = 0.03208 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.05155 if `cond1'
replace energycharge_win_off_pk= 0.02916 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.03952 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.03952 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.03952 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.03952 if `cond1'
replace energycharge_dwr_win_off_pk= 0.03952 if `cond1'

replace minimum_charge_sum = 29.46 if `cond1'
replace minimum_charge_win = 11.99 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 13.81 if `cond1'
replace demandcharge_sum_mid_pk = 3.88 if `cond1'
replace demandcharge_win_m_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.01785 if `cond2'
replace energycharge_sum_m_pk = 0.01785 if `cond2'
replace energycharge_sum_off_pk = 0.01785 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.01785 if `cond2'
replace energycharge_win_off_pk= 0.01785 if `cond2'
replace customercharge = 124.89 if `cond2'
replace minimum_charge_sum = 11.34 if `cond2'
replace minimum_charge_win = 9.84 if `cond2'
replace demandcharge = 10.96 if `cond2'
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.27 if `cond2' & voltage_cat==0

** second piece

//bundled
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.31 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.86 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.87 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00134 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00299 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00302 if `cond1' & voltage_cat==3

//unbundled
replace voltage_dis_load_1 = 0.14 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 4.50 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 7.76 if `cond2' & voltage_cat==3
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==3
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


** Oct 1 2012

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-5" if missing(old)
replace rate_start_date = mdy(10,1,2012) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.06960 if `cond1'
replace energycharge_sum_m_pk = 0.04115 if `cond1'
replace energycharge_sum_off_pk = 0.01972 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.03331 if `cond1'
replace energycharge_win_off_pk= 0.01767 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00463 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00463 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00463 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00463 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00463 if `cond1'

replace minimum_charge_sum = 29.46 if `cond1'
replace minimum_charge_win = 11.99 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 9.65 if `cond1'
replace demandcharge_sum_mid_pk = 2.71 if `cond1'
replace demandcharge_win_m_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.02640 if `cond2'
replace energycharge_sum_m_pk = 0.02640 if `cond2'
replace energycharge_sum_off_pk = 0.02640 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02640 if `cond2'
replace energycharge_win_off_pk= 0.02640 if `cond2'
replace customercharge = 128.48 if `cond2'
replace minimum_charge_sum = 8.72 if `cond2'
replace minimum_charge_win = 7.17 if `cond2'
replace demandcharge = 10.66 if `cond2'
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.27 if `cond2' & voltage_cat==0

** second piece

//bundled
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.31 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.86 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.87 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00134 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00299 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00302 if `cond1' & voltage_cat==3

//unbundled
replace voltage_dis_load_1 = 0.14 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 4.50 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 7.76 if `cond2' & voltage_cat==3
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==3
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


** Aug 1 2012

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-5" if missing(old)
replace rate_start_date = mdy(8,1,2012) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.06960 if `cond1'
replace energycharge_sum_m_pk = 0.04115 if `cond1'
replace energycharge_sum_off_pk = 0.01972 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.03331 if `cond1'
replace energycharge_win_off_pk= 0.01767 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00463 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00463 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00463 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00463 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00463 if `cond1'

replace minimum_charge_sum = 29.46 if `cond1'
replace minimum_charge_win = 11.99 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 9.65 if `cond1'
replace demandcharge_sum_mid_pk = 2.71 if `cond1'
replace demandcharge_win_m_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.02640 if `cond2'
replace energycharge_sum_m_pk = 0.02640 if `cond2'
replace energycharge_sum_off_pk = 0.02640 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02640 if `cond2'
replace energycharge_win_off_pk= 0.02640 if `cond2'
replace customercharge = 128.48 if `cond2'
replace minimum_charge_sum = 8.72 if `cond2'
replace minimum_charge_win = 7.17 if `cond2'
replace demandcharge = 10.66 if `cond2'
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.27 if `cond2' & voltage_cat==0

** second piece

//bundled
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.31 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.86 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.87 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00134 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00299 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00302 if `cond1' & voltage_cat==3

//unbundled
replace voltage_dis_load_1 = 0.14 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 4.50 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 7.76 if `cond2' & voltage_cat==3
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==3
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

** Jun 1 2012

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-5" if missing(old)
replace rate_start_date = mdy(6,1,2012) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.07177 if `cond1'
replace energycharge_sum_m_pk = 0.04237 if `cond1'
replace energycharge_sum_off_pk = 0.02022 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.03427 if `cond1'
replace energycharge_win_off_pk= 0.01811 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00463 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00463 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00463 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00463 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00463 if `cond1'

replace minimum_charge_sum = 29.46 if `cond1'
replace minimum_charge_win = 11.99 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 9.97 if `cond1'
replace demandcharge_sum_mid_pk = 2.80 if `cond1'
replace demandcharge_win_m_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.02594 if `cond2'
replace energycharge_sum_m_pk = 0.02594 if `cond2'
replace energycharge_sum_off_pk = 0.02594 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02594 if `cond2'
replace energycharge_win_off_pk= 0.02594 if `cond2'
replace customercharge = 125.81 if `cond2'
replace minimum_charge_sum = 8.72 if `cond2'
replace minimum_charge_win = 7.17 if `cond2'
replace demandcharge = 10.04 if `cond2'
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.27 if `cond2' & voltage_cat==0

** second piece

//bundled
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.31 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.86 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.87 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00134 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00299 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00302 if `cond1' & voltage_cat==3

//unbundled
replace voltage_dis_load_1 = 0.14 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 4.50 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 7.76 if `cond2' & voltage_cat==3
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==3
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


** Jan 1 2012

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-5" if missing(old)
replace rate_start_date = mdy(1,1,2012) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.07177 if `cond1'
replace energycharge_sum_m_pk = 0.04237 if `cond1'
replace energycharge_sum_off_pk = 0.02022 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.03427 if `cond1'
replace energycharge_win_off_pk= 0.01811 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00593 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00593 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00593 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00593 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00593 if `cond1'

replace minimum_charge_sum = 29.46 if `cond1'
replace minimum_charge_win = 11.99 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 9.97 if `cond1'
replace demandcharge_sum_mid_pk = 2.80 if `cond1'
replace demandcharge_win_m_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.02590 if `cond2'
replace energycharge_sum_m_pk = 0.02590 if `cond2'
replace energycharge_sum_off_pk = 0.02590 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02590 if `cond2'
replace energycharge_win_off_pk= 0.02590 if `cond2'
replace customercharge = 125.81 if `cond2'
replace minimum_charge_sum = 8.72 if `cond2'
replace minimum_charge_win = 7.17 if `cond2'
replace demandcharge = 10.04 if `cond2'
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.27 if `cond2' & voltage_cat==0

** second piece

//bundled
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.31 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.86 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.87 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00134 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00299 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00302 if `cond1' & voltage_cat==3

//unbundled
replace voltage_dis_load_1 = 0.14 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 4.50 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 7.76 if `cond2' & voltage_cat==3
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==3
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


** Apr 12 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-5" if missing(old)
replace rate_start_date = mdy(4,12,2013) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.07479 if `cond1'
replace energycharge_sum_m_pk = 0.04247 if `cond1'
replace energycharge_sum_off_pk = 0.01812 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.03356 if `cond1'
replace energycharge_win_off_pk= 0.01579 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00097 if `cond1'

replace minimum_charge_sum = 29.46 if `cond1'
replace minimum_charge_win = 11.99 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 10.97 if `cond1'
replace demandcharge_sum_mid_pk = 3.08 if `cond1'
replace demandcharge_win_m_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.02490 if `cond2'
replace energycharge_sum_m_pk = 0.02490 if `cond2'
replace energycharge_sum_off_pk = 0.02490 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02490 if `cond2'
replace energycharge_win_off_pk= 0.02490 if `cond2'
replace customercharge = 136.56 if `cond2'
replace minimum_charge_sum = 8.72 if `cond2'
replace minimum_charge_win = 7.17 if `cond2'
replace demandcharge = 12.18 if `cond2'
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.27 if `cond2' & voltage_cat==0

** second piece

//bundled
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.31 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.86 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.87 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00134 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00299 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00302 if `cond1' & voltage_cat==3

//unbundled
replace voltage_dis_load_1 = 0.14 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 4.50 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 7.76 if `cond2' & voltage_cat==3
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==3
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


** Apr 1 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-5" if missing(old)
replace rate_start_date = mdy(4,1,2013) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.07479 if `cond1'
replace energycharge_sum_m_pk = 0.04247 if `cond1'
replace energycharge_sum_off_pk = 0.01812 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.03356 if `cond1'
replace energycharge_win_off_pk= 0.01579 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00097 if `cond1'

replace minimum_charge_sum = 29.46 if `cond1'
replace minimum_charge_win = 11.99 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 10.97 if `cond1'
replace demandcharge_sum_mid_pk = 3.08 if `cond1'
replace demandcharge_win_m_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.02507 if `cond2'
replace energycharge_sum_m_pk = 0.02507 if `cond2'
replace energycharge_sum_off_pk = 0.02507 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02507 if `cond2'
replace energycharge_win_off_pk= 0.02507 if `cond2'
replace customercharge = 136.56 if `cond2'
replace minimum_charge_sum = 8.72 if `cond2'
replace minimum_charge_win = 7.17 if `cond2'
replace demandcharge = 12.18 if `cond2'
replace pf_adjust = 0.34 if `cond2' & voltage_cat==3
replace pf_adjust = 0.34 if `cond2' & voltage_cat==2
replace pf_adjust = 0.51 if `cond2' & voltage_cat==1
replace pf_adjust = 0.51 if `cond2' & voltage_cat==0

** second piece

//bundled
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.31 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.86 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.87 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00134 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00299 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00302 if `cond1' & voltage_cat==3

//unbundled
replace voltage_dis_load_1 = 0.14 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 4.50 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 7.76 if `cond2' & voltage_cat==3
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==3
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)


** Jan 1 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-5" if missing(old)
replace rate_start_date = mdy(1,1,2013) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.07479 if `cond1'
replace energycharge_sum_m_pk = 0.04247 if `cond1'
replace energycharge_sum_off_pk = 0.01812 if `cond1'
replace energycharge_win_on_pk = -999 if `cond1'
replace energycharge_win_m_pk = 0.03356 if `cond1'
replace energycharge_win_off_pk= 0.01579 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_m_pk = 0.00097 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_on_pk = -999 if `cond1'
replace energycharge_dwr_win_m_pk = 0.00097 if `cond1'
replace energycharge_dwr_win_off_pk= 0.00097 if `cond1'

replace minimum_charge_sum = 29.46 if `cond1'
replace minimum_charge_win = 11.99 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demandcharge_sum_on_pk = 10.97 if `cond1'
replace demandcharge_sum_mid_pk = 3.08 if `cond1'
replace demandcharge_win_m_pk = 0.00 if `cond1'
replace demandcharge_win_off_pk = 0.00 if `cond1'

//unbundled 
replace energycharge_sum_on_pk = 0.02490 if `cond2'
replace energycharge_sum_m_pk = 0.02490 if `cond2'
replace energycharge_sum_off_pk = 0.02490 if `cond2'
replace energycharge_win_on_pk = -999 if `cond2'
replace energycharge_win_m_pk = 0.02490 if `cond2'
replace energycharge_win_off_pk= 0.02490 if `cond2'
replace customercharge = 136.56 if `cond2'
replace minimum_charge_sum = 8.72 if `cond2'
replace minimum_charge_win = 7.17 if `cond2'
replace demandcharge = 12.18 if `cond2'
replace pf_adjust = 0.32 if `cond2' & voltage_cat==3
replace pf_adjust = 0.32 if `cond2' & voltage_cat==2
replace pf_adjust = 0.27 if `cond2' & voltage_cat==1
replace pf_adjust = 0.27 if `cond2' & voltage_cat==0

** second piece

//bundled
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==1
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==2
replace voltage_dis_load_1= 0.0 if `cond1' & voltage_cat==3
replace voltage_dis_load_2= 0.31 if `cond1' & voltage_cat==1
replace voltage_dis_load_2= 0.86 if `cond1' & voltage_cat==2
replace voltage_dis_load_2= 0.87 if `cond1' & voltage_cat==3
replace voltage_dis_energy = 0.00134 if `cond1' & voltage_cat==1
replace voltage_dis_energy = 0.00299 if `cond1' & voltage_cat==2
replace voltage_dis_energy = 0.00302 if `cond1' & voltage_cat==3

//unbundled
replace voltage_dis_load_1 = 0.14 if `cond2' & voltage_cat==1
replace voltage_dis_load_1 = 4.50 if `cond2' & voltage_cat==2
replace voltage_dis_load_1 = 7.76 if `cond2' & voltage_cat==3
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_load_2 = 0.0 if `cond2' & voltage_cat==3
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==1
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==2
replace voltage_dis_energy = 0.0 if `cond2' & voltage_cat==3

replace old= 0 if missing(old)

/*
*********************************
*** 8. Schedule- TOU-PA-3-SOP ***
*********************************

** Nov 22 2013

local N= `N' + `cats'
set obs `N'

replace rateschedule = "TOU-PA-5" if missing(old)
replace rate_start_date = mdy(11,22,2013) if missing(old)
replace voltage_cat= mod(_n,4) if missing(old)
replace bundled= mod(_n,2) if missing(old)

//bundled
replace energycharge_sum_on_pk = 0.07962 if `cond1'
replace energycharge_sum_off_pk = 0.04597 if `cond1'
replace energycharge_sum_sup_off_pk = 0.02033 if `cond1'
replace energycharge_win_off_pk = 0.05137 if `cond1'
replace energycharge_win_sup_off_pk= 0.02147 if `cond1'
replace energycharge_dwr_sum_on_pk = 0.00095 if `cond1'
replace energycharge_dwr_sum_off_pk = 0.00095 if `cond1'
replace energycharge_dwr_sum_sup_off_pk = 0.00095 if `cond1'
replace energycharge_dwr_win_off_pk = 0.00095 if `cond1'
replace energycharge_dwr_win_sup_off_pk= 0.00095 if `cond1'
replace demandcharge = 0.0 if `cond1'
replace demand
*/

compress
save "$path_temp/sce_rate_hand_coded.dta", replace

}

**********************************************************************
**********************************************************************

** 3. Combine all SCE rates into a single file, and standardize 
{
clear
cd "$path_temp"	
local files : dir . files "sce_rate_*.dta"
foreach f in `files' {
	append using `f'
}
duplicates drop

replace rateschedule = ratename if rateschedule==""
assert ratename==rateschedule | ratename==""
drop ratename
order rateschedule
replace rateschedule = upper(rateschedule)
tab rateschedule
la var rateschedule "SCE ag rate schedule"

replace rate_start_date = date_2 if rate_start_date==.
assert date_2==rate_start_date | date_2==.
drop date_2
order rate_start_date, after(rateschedule)
la var rate_start_date "Rate effective date"

br ratesch energycharge_*
order bundled energycharge_*, after(rate_start_date)

tab old, missing
drop old

br ratesche rate_start_date pf*
replace pf_adjust_1 = pfa_1 if pfa_1!=. & pf_adjust_1==.
replace pf_adjust_2 = pfa_2 if pfa_2!=. & pf_adjust_2==. & pfa_2==pfa_3 & pfa_2!=pfa_1
replace pf_adjust_2 = pfa_3 if pfa_3!=. & pf_adjust_2==. & pfa_2!=pfa_3 & pfa_2==pfa_1
drop pfa_?


br ratesched rate_start_date bundled voltage_cat voltage_dis_energy*

// this is still a mess, but I think we can extract most of what we need in for marginal prices as is
// need to come back and clean further to make any sense of the fixed charges

sort rateschedule bundled rate_start_date voltage_cat
unique rateschedule rate_start_date bundled voltage_cat
	// this should be unique!
compress
save "$path_out/sce_ag_rates_compiled.dta", replace

}

**********************************************************************
**********************************************************************
