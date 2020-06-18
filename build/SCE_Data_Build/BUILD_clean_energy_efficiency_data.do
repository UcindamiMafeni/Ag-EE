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
drop iouinstallationdate*
gen ee_install_date = date(dt,"DMY")
format %td ee_install_date
count if ee_install_date==. // 1 missings out of 8841 observations
la var ee_install_date "Installation date"
drop dt

** Paid date
split ioupaiddate, p(":")
rename ioupaiddate1 dt
drop ioupaiddate*
gen ee_paid_date = date(dt,"DMY")
format %td ee_paid_date
count if ee_paid_date==. // 6,841 missings out of 8841 observations
la var ee_paid_date "Date incentive was paid"
drop dt


unique claimid
assert r(unique)==r(N)
la var claimid "EE claim ID"

tab iouprgname
rename iouprgname eega_desc 
la var eega_desc "Energy efficiency program description"

tab iouprgelement 
rename iouprgelement eega_desc_sub
la var eega_desc_sub "Energy efficiency element descrption"

tab ioumeaname
rename ioumeaname measure_desc
la var measure_desc "Energy efficiency measure description"

tab ioumeaelecenduseshape
rename ioumeaelecenduseshape eega_code
la var eega_code "Energy efficiency measure code/category"

drop iouprgid iouprgelementdescription iouclaimyearquarter 

tab iouenduse
rename iouenduse ee_enduse
la var ee_enduse "Energy efficiency end use"

tab iouunits
replace iouunits = "kWh" if iouunits=="kwh" | iouunits=="KWH"
replace iouunits = "Ton" if iouunits=="TON"
rename iouunits ee_units
la var ee_units "EE units"

rename iouexantequantity quantity_ex_ante
la var quantity_ex_ante "Quantity ex ante (of units)"

rename totalgrsavkw savings_kw
la var savings_kw "Total gross savings (kW)"

rename totalgrsavkwh savings_kwh
la var savings_kwh "Total gross savings (kWh)"

rename iougrincentive ee_incentive
la var ee_incentive "EE incentive to take-up measure"

tab iousector
replace iousector = substr(upper(iousector),1,3)
replace iousector = "AGR" if iousector=="AG"
tab iousector
rename iousector sector
la var sector "Customer sector"

destring year, replace
br if year!=year(ee_install_date) & year!=year(ee_paid_date)
drop year // not adding any relevant infomration

** Clean up and save
order sa_uuid ee_install_date claimid sector ee_enduse eega_desc eega_code eega_desc_sub measure_desc ///
	ee_incentive ee_paid_date ee_units quantity_ex_ante savings_kw savings_kwh
sort *
compress
save "$dirpath_data/pge_cleaned/sce_ee_programs_20190916.dta", replace	

