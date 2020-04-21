clear all
version 13
set more off

*******************************************************************************
**** Script to create analysis datasets for annual electricity regressions ****
*******************************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

** PENDING:

** Re-make this dataset from raw data rather than collapsing the existing monthly dataset

*******************************************************************************
*******************************************************************************

** Start with monthly electricity dataset
use "$dirpath_data/merged/sp_month_elec_panel.dta"

** Create a flag for SP-years with < 12 months
egen months = count(modate), by(sp_group year)
gen flag_partial_year = (months < 12)
drop months

** Create summer and winter electricity consumption variables
gen summer_kwh = mnth_bill_kwh if month >= 5 & month <= 10
gen winter_kwh = mnth_bill_kwh if month <= 4 | month >= 11
gen summer_bill_amount = mnth_bill_amount if month >= 5 & month <= 10
gen winter_bill_amount = mnth_bill_amount if month <= 4 | month >= 11

** Create summer and winter electricity price variables
gen mean_p_kwh_summer = mean_p_kwh if month >= 5 & month <= 10
gen mean_p_kwh_winter = mean_p_kwh if month <= 4 | month >= 11
gen mean_p_kwh_summer_ag_default = mean_p_kwh_ag_default if month >= 5 & month <= 10
gen mean_p_kwh_winter_ag_default = mean_p_kwh_ag_default if month <= 4 | month >= 11

** Collpase to annual dataset for relevant variables
collapse (sum) ann_kwh=mnth_bill_kwh summer_kwh winter_kwh ///
               ann_bill_amount=mnth_bill_amount summer_bill_amount winter_bill_amount ///
	     (max) flag_* ///
         (mean) mean_p_kwh mean_p_kwh_summer mean_p_kwh_winter ///
		        mean_p_kwh_ag_default mean_p_kwh_summer_ag_default mean_p_kwh_winter_ag_default ///
         (median) merge_sp_water_panel rt_large_ag basin_group wdist_group ///
		          alfalfa almonds fallow grapes grass no_crop annual perennial ///
				  annual_always perennial_always perennial_ever annual_ever ann_per_switcher ///
	     (firstnm) pull, ///
		 by(sp_group year)

** Create additional electricity consumption variables
gen ihs_kwh = ln(100 * ann_kwh + sqrt((100 * ann_kwh)^2 + 1))
replace ihs_kwh = . if ann_kwh < 0
gen ihs_kwh_summer = ln(100 * summer_kwh + sqrt((100 * summer_kwh)^2 + 1))
replace ihs_kwh_summer = . if summer_kwh < 0
gen ihs_kwh_winter = ln(100 * winter_kwh + sqrt((100 * winter_kwh)^2 + 1))
replace ihs_kwh_winter = . if winter_kwh < 0
gen elec_binary = (ann_kwh > 0)
gen elec_binary_summer = (summer_kwh > 0)
gen elec_binary_winter = (winter_kwh > 0)
egen elec_binary_frac = mean(elec_binary), by(sp_group)

** Create logged electricity price variables
gen log_p_mean = ln(mean_p_kwh)
gen log_p_mean_summer = ln(mean_p_kwh_summer)
gen log_p_mean_winter = ln(mean_p_kwh_winter)
gen log_mean_p_kwh_ag_default = ln(mean_p_kwh_ag_default)
gen log_mean_p_kwh_summer_ag_default = ln(mean_p_kwh_summer_ag_default)
gen log_mean_p_kwh_winter_ag_default = ln(mean_p_kwh_winter_ag_default)

** Create lagged electricity price varibales
tsset sp_group year
gen log_p_mean_lag = L1.log_p_mean
gen log_p_mean_summer_lag = L1.log_p_mean_summer
gen log_p_mean_winter_lag = L1.log_p_mean_winter
gen log_p_mean_deflag = L1.log_mean_p_kwh_ag_default
gen log_p_mean_summer_deflag = L1.log_mean_p_kwh_summer_ag_default
gen log_p_mean_winter_deflag = L1.log_mean_p_kwh_winter_ag_default

** Save
sort sp_group year
compress
save "$dirpath_data/merged/sp_annual_elec_panel.dta", replace

*******************************************************************************
*******************************************************************************


