************************************************
* Import PG&E data from excel / csv.
************************************************
**** SETUP:
clear all
memory clear
set more off, perm
version 12

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"
global dirpath_raw "S:/Raw Data/PumpData"

** additional directory paths to make things easier

************************************************
*** load monthly billing data and save as dta file
import delim "$dirpath_raw/Data03212018/PGE_03222018_rn1765829728_1/PGE_03222018_rn1765829728_3.csv", varn(1) stringc(_all) clear
*import delim using "$dirpath_raw/Data03212018/PGE_03222018_rn1765829728_1/PGE_03222018_rn1765829728_3.csv"
save "$dirpath_data/pge_raw/bill_data.dta", replace

*** load customer data and save as dta file
import excel "$dirpath_raw/Data03212018/PGE_03222018_rn1765829728_1/PGE_03222018_rn1765829728_2.xlsx", sheet("Sheet1") firstrow allstring clear
*import delim using "$dirpath_raw/Data03212018/PGE_03222018_rn1765829728_1/PGE_03222018_rn1765829728_2.csv"
save "$dirpath_data/pge_raw/customer_data.dta", replace

*** load interval data and save as dta file
import delim using "$dirpath_raw/Data03212018/PGE_03222018_rn1765829728_1/PGE_03222018_rn1765829728_5.csv"
save "$dirpath_data/pge_raw/interval_data.dta", replace


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
