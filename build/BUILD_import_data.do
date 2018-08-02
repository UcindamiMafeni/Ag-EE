************************************************
* Import PG&E data from excel / csv.
************************************************
**** SETUP:
clear all
memory clear
set more off, perm
set excelxlsxlargefile on
version 12

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"
global dirpath_raw "S:/Raw Data/PumpData"

** additional directory paths to make things easier

************************************************
************************************************

** MARCH 22 2018 DATA

*** load customer data and save as dta file
import excel using "$dirpath_raw/Data03212018/PGE_03222018_rn1765829728_1/PGE_03222018_rn1765829728_2.xlsx", sheet("Sheet1") firstrow allstring clear
save "$dirpath_data/pge_raw/customer_data_20180322.dta", replace

*** load monthly billing data and save as dta file
import delim using "$dirpath_raw/Data03212018/PGE_03222018_rn1765829728_1/PGE_03222018_rn1765829728_3.csv", varn(1) stringc(_all) clear
save "$dirpath_data/pge_raw/bill_data_20180322.dta", replace

*** load energy efficiency data and save as dta file
import excel using "$dirpath_raw/Data03212018/PGE_03222018_rn1765829728_1/PGE_03222018_rn1765829728_4.xlsx", firstrow allstring clear
save "$dirpath_data/pge_raw/energy_efficiency_data_20180322.dta", replace

*** load interval data and save as dta file
import delim using "$dirpath_raw/Data03212018/PGE_03222018_rn1765829728_1/PGE_03222018_rn1765829728_5.csv", clear
save "$dirpath_data/pge_raw/interval_data_20180322.dta", replace

*** load pump test project data and save as dta file
import excel using "$dirpath_raw/Data03212018/PGE_03222018_rn1765829728_1/PGE_03222018_rn1765829728_6.xlsx", cellrange(A16:J1038) firstrow clear
save "$dirpath_data/pge_raw/pump_test_project_data_20180322.dta", replace

*** load badge number data and save as dta file
import excel using "$dirpath_raw/Data03212018/PGE_03222018_rn1765829728_1/PGE_03222018_rn1765829728_7.xlsx", firstrow clear
save "$dirpath_data/pge_raw/meter_badge_number_data_20180322.dta", replace

*** load pump test data and save as dta file
import excel using "$dirpath_raw/Data03212018/PGE_03222018_rn1765829728_1/APEPPumpTests Berkley excel.xlsx", firstrow clear
save "$dirpath_data/pge_raw/pump_test_data_20180322.dta", replace

************************************************
************************************************

** JULY 19 2018 DATA (where PGE fixed the sampling criteria for all non-APEP data)

*** load customer data and save as dta file
import excel using "$dirpath_raw/Data 07192018/Customer Detail 201807.xlsx", firstrow allstring clear
save "$dirpath_data/pge_raw/customer_data_20180719.dta", replace

*** load monthly billing data and save as dta file
import excel using "$dirpath_raw/Data 07192018/Customer Billing 201807.xlsx", firstrow allstring clear
save "$dirpath_data/pge_raw/bill_data_20180719.dta", replace

*** load energy efficiency data and save as dta file
import excel using "$dirpath_raw/Data 07192018/Customer Energy Efficiency 201807.xlsx", firstrow allstring clear
save "$dirpath_data/pge_raw/energy_efficiency_data_20180719.dta", replace

*** load interval data and save as dta file
insheet using "$dirpath_raw/Data 07192018/Interval Electric 201807/Interval Electric 201807.csv", clear
save "$dirpath_data/pge_raw/interval_data_20180719.dta", replace

*** load badge number data and save as dta file
import excel using "$dirpath_raw/Data 07192018/SP PGE BADGE Number History 201807.xlsx", firstrow clear
save "$dirpath_data/pge_raw/meter_badge_number_data_20180719.dta", replace


************************************************
************************************************

** AUGUST 2 2018 DATA (billing data file only, which was inadvertently truncated in the July 19 pull)

*** load monthly billing data and save as dta file
import delim using "$dirpath_raw/Data08022018/Customer Billing 201808.csv", varn(1) stringc(_all) clear
save "$dirpath_data/pge_raw/bill_data_20180802.dta", replace

************************************************
************************************************









**** FIRST WAVE OF DATA -- NOT CURRENTLY BEING USED
/*
*** load monthly billing datasets and save as dta files
import delim using "$dirpath_raw/PGE_01312018_rn1765829728_3.csv"
save "$dirpath_data/old/bill_data_1516.dta", replace
import delim using "$dirpath_raw/EDRP UC Berkeley Ag - Billing 2008 - 2010.csv"
save "$dirpath_data/old/bill_data_0810.dta", replace
import delim using "$dirpath_raw/EDRP UC Berkeley Ag - Billing 2011 - 2014 2017.csv"
save "$dirpath_data/old/bill_data_111417.dta", replace

*** append monthly billing datasets into one dta file
use "$dirpath_data/old/bill_data_0810", clear
append using "$dirpath_data/old/bill_data_111417"
append using "$dirpath_data/old/bill_data_1516"
save "$dirpath_data/old/bill_data.dta", replace

*** load interval data and save as dta file
import delim using "$dirpath_raw/PGE_01312018_rn1765829728_5.csv"
save "$dirpath_data/old/PGE_01312018_rn1765829728_5.dta", replace

*/
