clear all
version 13
set more off

*******************************************************************************
**** Script to import and clean raw PGE data -- acoount-level EE data ********
*******************************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

*******************************************************************************
*******************************************************************************

** 1. March 2018 data pull
if 1==0{

** Load raw PGE customer data
use "$dirpath_data/pge_raw/energy_efficiency_data_20180322.dta", clear
duplicates drop

** Service agreement ID
assert sa_uuid!=""
assert length(sa_uuid)==10
unique sa_uuid // 3730 unique service agreements
la var sa_uuid "Service Agreement ID (anonymized, 10-digit)"

** Issue date
gen ee_measure_date = date(chk_issue_date,"MDY")
format %td ee_measure_date
count if ee_measure_date==. // 50 missings out of 5,612 observations
assert chk_issue_date=="" if ee_measure_date==.
la var ee_measure_date "Date that EE application was paid"
drop chk_issue_date

** Label EE variables
la var eega_code "Energy efficiency sub-program code"
rename eega_description eega_desc
la var eega_desc "Energy efficiency sub-program description"
la var measure_code "Energy efficiency measure code"
la var measure_desc "Energy efficiency measure description"

** Standardize sub-program descriptions
replace eega_desc = "Industrial Calculated Incentives" if eega_desc=="IND CALCULATED INCENTIVES"
replace eega_desc = "Commercial Deemed Incentives" if eega_desc=="Commercial Programs - Deemed"
replace eega_desc = "Agricultural Deemed Incentives" if eega_desc=="Agricultural Programs - Deemed"
replace eega_desc = "Agricultural Energy Advisor" if eega_desc=="AGRICULTURAL ENERGY ADVISOR"
replace eega_desc = "Agricultural Calculated Incentives" if eega_desc=="AG CALCULATED INCENTIVES"
	// 2 remain that are not obviously the same thing, but have the same code

** Confirm uniqueness and save
unique sa_uuid eega_code measure_code ee_measure_date
assert r(unique)==r(N)
compress
save "$dirpath_data/pge_cleaned/pge_ee_programs_20180322.dta", replace	

}

*******************************************************************************
*******************************************************************************

** 2. July 2018 data pull
{

** Load raw PGE customer data
use "$dirpath_data/pge_raw/energy_efficiency_data_20180719.dta", clear
duplicates drop

** Service agreement ID
assert sa_uuid!=""
assert length(sa_uuid)==10
unique sa_uuid // 2575 unique service agreements
la var sa_uuid "Service Agreement ID (anonymized, 10-digit)"

** Issue date
gen ee_measure_date = date(chk_issue_date,"MDY")
format %td ee_measure_date
count if ee_measure_date==. // 186 missings out of 4,534 observations
assert chk_issue_date=="" if ee_measure_date==.
la var ee_measure_date "Date that EE application was paid"
drop chk_issue_date

** Label EE variables
la var eega_code "Energy efficiency sub-program code"
rename eega_description eega_desc
la var eega_desc "Energy efficiency sub-program description"
la var measure_code "Energy efficiency measure code"
la var measure_desc "Energy efficiency measure description"

** Standardize sub-program descriptions
replace eega_desc = "Industrial Calculated Incentives" if eega_desc=="IND CALCULATED INCENTIVES"
replace eega_desc = "Commercial Deemed Incentives" if eega_desc=="Commercial Programs - Deemed"
replace eega_desc = "Agricultural Deemed Incentives" if eega_desc=="Agricultural Programs - Deemed"
replace eega_desc = "Agricultural Energy Advisor" if eega_desc=="AGRICULTURAL ENERGY ADVISOR"
replace eega_desc = "Agricultural Calculated Incentives" if eega_desc=="AG CALCULATED INCENTIVES"
	// 2 remain that are not obviously the same thing, but have the same code

** Confirm uniqueness and save
unique sa_uuid eega_code measure_code ee_measure_date
assert r(unique)==r(N)
compress
save "$dirpath_data/pge_cleaned/pge_ee_programs_20180719.dta", replace	

}

*******************************************************************************
*******************************************************************************

