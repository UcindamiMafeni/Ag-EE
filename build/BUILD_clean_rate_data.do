************************************************
* Build and clean PGE agricultural rate data.
************************************************
**** SETUP:
clear all
memory clear
set more off, perm
version 12

global dirpath "S:/Matt/ag_pump"
** additional directory paths to make things easier
global dirpath_data "$dirpath/data"
global dirpath_data_temp "$dirpath/data/temp"
global dirpath_data_pge_raw "$dirpath_data/pge_raw"

************************************************
// all rates downloaded from https://www.pge.com/tariffs/electric.shtml on 5/8/18


**** STEP 1: IMPORT ALL EXCEL DATA TO STATA
cd "$dirpath_data_pge_raw/rates/excel"

local files: dir . files "*.xls"

foreach file in `files' {
 import excel "`file'", firstrow clear
 gen file = "`file'"
 replace file = subinstr(file, ".xls", "", .)
 local fname = file
 missings dropvars, force
 save "$dirpath_data_temp/`fname'.dta", replace 
}



