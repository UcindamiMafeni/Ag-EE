************************************************
* Perform initial SCE data checks
************************************************
**** SETUP:
clear all
memory clear
set more off, perm
set excelxlsxlargefile on
version 12

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"
global dirpath_data_sce "$dirpath/data/sce_raw"

global dirpath_output_sce "$dirpath/output/sce_checks"

*** CHANGE ME LATER
global dirpath_raw "T:/Projects/Pump Data/data/TEMP2"

** additional directory paths to make things easier

************************************************
************************************************

******* BILLING DATA

** Plot number of customers by month 
use "$dirpath_data_sce/bill_data_20190916.dta", clear

keep serv_acct_num statl_yr_mo_dt
duplicates drop
gen ones = 1
collapse (sum) count = ones, by(statl)
gen year = substr(statl, 1, 4)
gen month = substr(statl, 5, 2)

destring year month, replace

gen ym_stata = ym(year, month)
format ym_stata %tm

*twoway line count ym_stata, xtitle("month") ytitle("Service Accounts")
twoway line count ym_stata if year > 2007, xtitle("month") ytitle("Service accounts (nr)")
graph export "$dirpath_output_sce/sce_customers_monthly.png", replace


** Plot of consumption by month 
use "$dirpath_data_sce/bill_data_20190916.dta", clear
destring kwh_usage, replace

keep kwh_usage statl_yr_mo_dt
collapse (sum) kwh_usage, by(statl)
gen year = substr(statl, 1, 4)
gen month = substr(statl, 5, 2)

destring year month, replace

gen ym_stata = ym(year, month)
format ym_stata %tm

twoway line kwh ym_stata if year > 2007, xtitle("Month") ytitle("Total kWh")
graph export "$dirpath_output_sce/sce_total_kwh_monthly.png", replace


** Plot of consumption per customer by month 
use "$dirpath_data_sce/bill_data_20190916.dta", clear
destring kwh_usage, replace

keep kwh_usage statl_yr_mo_dt

gen counter = 1 
collapse (sum) kwh_usage counter, by(statl)
gen year = substr(statl, 1, 4)
gen month = substr(statl, 5, 2)

destring year month, replace

gen ym_stata = ym(year, month)
format ym_stata %tm

gen kwh_per_customer = kwh/counter

twoway line kwh_per ym_stata if year > 2007, xtitle("Month") ytitle("kWh per customer")
graph export "$dirpath_output_sce/sce_kwh_per_customer_monthly.png", replace


** Plot of average consumption by month 
use "$dirpath_data_sce/bill_data_20190916.dta", clear
destring kwh_usage, replace

keep kwh_usage statl_yr_mo_dt

collapse (mean) kwh_usage, by(statl)
gen year = substr(statl, 1, 4)
gen month = substr(statl, 5, 2)

destring year month, replace

gen ym_stata = ym(year, month)
format ym_stata %tm


twoway line kwh ym_stata if year > 2007, xtitle("Month") ytitle("Mean kWh")
graph export "$dirpath_output_sce/sce_mean_kwh_monthly.png", replace




******* CUSTOMER DATA

** Plot of accounts by opening month
use "$dirpath_data_sce/customer_data_20190916.dta", clear

keep serv_acct_num sa_estab_date

duplicates drop

gen acct_open_date = date(sa_estab_date, "DMY")
format acct_open %td

gen acct_open_month = month(acct_open)
gen acct_open_year = year(acct_open_date)
gen acct_open_ym = ym(acct_open_year, acct_open_month)

format acct_open_ym %tm
gen counter = 1
collapse(sum) counter, by(acct_open_ym acct_open_year)

twoway line counter acct_open_ym, xtitle("Month") ytitle("Number of accounts opened")
graph export "$dirpath_output_sce/sce_acct_open_month_raw.png", replace



** Plot of accounts by closing month
use "$dirpath_data_sce/customer_data_20190916.dta", clear

keep serv_acct_num sa_close_date

duplicates drop

gen acct_cl_date = date(sa_close_date, "DMY")
format acct_cl %td

gen acct_cl_month = month(acct_cl)
gen acct_cl_year = year(acct_cl_date)
gen acct_cl_ym = ym(acct_cl_year, acct_cl_month)

format acct_cl_ym %tm
gen counter = 1
collapse(sum) counter, by(acct_cl_ym acct_cl_year)

twoway line counter acct_cl_ym if acct_cl_ym !=., xtitle("Month") ytitle("Number of accounts closed") 
graph export "$dirpath_output_sce/sce_acct_close_month_raw.png", replace


****** MERGE BETWEEN CUSTOMER & BILLING DATA
use "$dirpath_data_sce/customer_data_20190916.dta", clear
gen acct_cl_date = date(sa_close_date, "DMY")
format acct_cl %td
gen acct_cl_year = year(acct_cl_date)

tempfile customer_temp
save "`customer_temp'"

use "$dirpath_data_sce/bill_data_20190916.dta", clear
keep serv_acct_num
duplicates drop
merge 1:1 serv_acct_num using "`customer_temp'"

tab acct_cl_year if _merge !=3

/*
. tab acct_cl_year if _merge !=3

acct_cl_yea |
          r |      Freq.     Percent        Cum.
------------+-----------------------------------
       2008 |        290       13.79       13.79
       2009 |        223       10.60       24.39
       2010 |        189        8.99       33.38
       2011 |        197        9.37       42.75
       2012 |        255       12.13       54.87
       2013 |        195        9.27       64.15
       2014 |        185        8.80       72.94
       2015 |        128        6.09       79.03
       2016 |        125        5.94       84.97
       2017 |        124        5.90       90.87
       2018 |        128        6.09       96.96
       2019 |         64        3.04      100.00
------------+-----------------------------------
      Total |      2,103      100.00
*/


** LOOK FOR MISSINGS IN BILLING DATA

use "$dirpath_data_sce/bill_data_20190916.dta", clear

 count if kwh_usage == "0"
  652,590

di 652590/3517751
.18551341


count if kwh_usage == "0" & monthly_max != . & monthly_max != 0
  20,855

gen year_str = substr(statl, 1, 4)

destring year_str, replace
year_str: all characters numeric; replaced as int

tab year_str if kwh_usage == "0"

/*
   year_str |      Freq.     Percent        Cum.
------------+-----------------------------------
       2007 |          1        0.00        0.00
       2008 |     64,978        9.96        9.96
       2009 |     61,466        9.42       19.38
       2010 |     66,143       10.14       29.51
       2011 |     66,171       10.14       39.65
       2012 |     49,552        7.59       47.24
       2013 |     46,577        7.14       54.38
       2014 |     47,495        7.28       61.66
       2015 |     50,173        7.69       69.35
       2016 |     54,186        8.30       77.65
       2017 |     57,978        8.88       86.54
       2018 |     51,831        7.94       94.48
       2019 |     36,039        5.52      100.00
------------+-----------------------------------
      Total |    652,590      100.00
*/


******* EE DATA

use "$dirpath_data/sce_raw/energy_efficiency_data_20190916.dta", clear

