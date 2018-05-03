clear all
version 13
set more off

*******************************************************************************
**** Script to import and clean raw PGE data -- customer details file *********
*******************************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

** Load raw PGE customer data
use "$dirpath_data/pge_raw/customer_data.dta", clear
