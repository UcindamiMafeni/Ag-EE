clear all
version 13
set more off

*******************************************************************************
**** Script to import and clean raw PGE data -- acoount-level EE data ********
*******************************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

*******************************************************************************
*******************************************************************************

** 1. March 2018 data pull

** Load raw PGE customer data
use "$dirpath_data/sce_raw/energy_efficiency_data_20190916.dta", clear
duplicates drop

** Service agreement ID
rename iouserviceaccountid sa_uuid
assert sa_uuid!=""
unique sa_uuid // 5069 unique service agreements


** Installation date
split iouinstallationdate, p(":")
rename iouinstallationdate1 dt
drop iouinstallationdate?
gen ee_install_date = date(dt,"DMY")
format %td ee_install_date
count if ee_install_date==. // 1 missings out of 8841 observations
la var ee_install_date "Installation date"
drop dt

** what is paid date?
** ioexantquant? iogrsavekw and iogrsavekwh dont seem to be consistently related?
**

/*
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
*/
