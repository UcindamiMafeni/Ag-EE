clear all
version 13
set more off

*****************************************************************************
**** Script to merge and analyse temperature, electricity and water data ****
*****************************************************************************

** This script does two things: 
** 		1) Collapses hourly customer electricity data to daily data
**		2) Merges electricity data with daily temperature data
**		3) Merges in monthly customer data on geographic factors 
**		4) Implements regression analysis to investigate the effect of temperature on electricity consumption


global dirpath "T:\Projects\Pump Data"
global dirpath_data "$dirpath\data"

*Append yearly temperature datasets
set obs 1

forvalues year = 2008/2019 {
append using "$dirpath_data\prism\cleaned_daily\pge_sp_temperature_daily_`year'.dta"
}

save "$dirpath_data\prism\cleaned_daily\pge_sp_temperature_daily.dta"

use "$dirpath_data\merged_pge\sp_hourly_elec_panel_20180719.dta", clear

*aggregate kwh to daily level for temp data merge
bysort sp_uuid date: gegen day_kwh=sum(kwh)
drop hour p_kwh
duplicates drop // reduce dimensions to daily level
lab var day_kwh "kwh consumption aggregated to daily level"
*merge in temp data

merge 1:1 sp_uuid date using "$dirpath_data\prism\cleaned_daily\pge_sp_temperature_daily.dta"
gen modate=mofd(date) // create month-year variable for potential future merge with monthly panel
format modate %tm





/*
drop _merge
merge 1:1 sp_uuid modate "$dirpath_data\merged_pge\sp_month_water_panel.dta"

drop _merge

use "$dirpath_data\merged_pge\sp_month_water_panel.dta", clear

