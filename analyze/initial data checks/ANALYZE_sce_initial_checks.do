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


** Meters assigned to multiple SAs
use "$dirpath_data_sce/customer_data_20190916.dta", clear
replace meter_no = strtrim(meter_no)
drop if meter_no == "" | meter_no == "COMBO"

replace sa_close_date = "31DEC2019" if sa_status_code == "A"
rename sa_estab_date sa_estab_date_str
rename sa_close_date sa_close_date_str
gen sa_estab_date = date(sa_estab_date_str, "DMY")
gen sa_close_date = date(sa_close_date_str, "DMY")
drop sa_*_str
format sa_estab_date %td
format sa_close_date %td

gen duration = sa_close_date - sa_estab_date + 1
expand duration
bysort serv_acct_num : gen date = sa_estab_date[1] + _n - 1  
format date %td

gen ones = 1
collapse (count) count=ones, by(meter_no date)
tab count
/*
    (count) |
       ones |      Freq.     Percent        Cum.
------------+-----------------------------------
          1 |204,442,483       99.93       99.93
          2 |    141,687        0.07      100.00
          3 |        135        0.00      100.00
------------+-----------------------------------
      Total |204,584,305      100.00

*/

keep if count > 1
egen meter_id = group(meter_no)
tsset meter_id date
tsspell meter_id
collapse (max) _seq, by(meter_id _spell)
tab _seq
/*
 (max) _seq |      Freq.     Percent        Cum.
------------+-----------------------------------
          1 |      8,767       99.12       99.12
          2 |         16        0.18       99.30
          4 |          1        0.01       99.31
          6 |          1        0.01       99.32
          7 |          1        0.01       99.33
         14 |          1        0.01       99.34
         15 |          1        0.01       99.36
         23 |          1        0.01       99.37
         30 |          1        0.01       99.38
         32 |          1        0.01       99.39
         36 |          1        0.01       99.40
         64 |          1        0.01       99.41
         86 |          1        0.01       99.42
        115 |          1        0.01       99.43
        120 |          1        0.01       99.45
        121 |          1        0.01       99.46
        124 |          1        0.01       99.47
        128 |          1        0.01       99.48
        134 |          1        0.01       99.49
        144 |          1        0.01       99.50
        169 |          1        0.01       99.51
        185 |          1        0.01       99.53
        217 |          1        0.01       99.54
        227 |          1        0.01       99.55
        233 |          1        0.01       99.56
        248 |          1        0.01       99.57
        276 |          1        0.01       99.58
        295 |          1        0.01       99.59
        449 |          1        0.01       99.60
       1002 |          1        0.01       99.62
       1065 |          1        0.01       99.63
       1400 |          1        0.01       99.64
       1540 |          1        0.01       99.65
       1568 |          1        0.01       99.66
       1576 |          1        0.01       99.67
       1729 |          1        0.01       99.68
       1948 |          1        0.01       99.69
       2032 |          1        0.01       99.71
       2148 |          1        0.01       99.72
       2413 |          1        0.01       99.73
       2474 |          1        0.01       99.74
       2562 |          1        0.01       99.75
       2705 |          1        0.01       99.76
       2730 |          1        0.01       99.77
       2898 |          1        0.01       99.79
       3132 |          1        0.01       99.80
       3165 |          1        0.01       99.81
       3180 |          1        0.01       99.82
       3404 |          1        0.01       99.83
       3768 |          1        0.01       99.84
       3872 |          1        0.01       99.85
       4085 |          1        0.01       99.86
       4608 |          1        0.01       99.88
       4656 |          1        0.01       99.89
       4776 |          1        0.01       99.90
       4906 |          1        0.01       99.91
       5234 |          1        0.01       99.92
       5973 |          1        0.01       99.93
       6313 |          1        0.01       99.94
       6626 |          1        0.01       99.95
       7101 |          1        0.01       99.97
       7160 |          1        0.01       99.98
       7687 |          1        0.01       99.99
       8085 |          1        0.01      100.00
------------+-----------------------------------
      Total |      8,845      100.00

*/

count if _seq > 365
* 36

****** MERGE BETWEEN CUSTOMER & BILLING DATA

** Missing SAs (in billing data) by account close year
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


** No usage or negative bills
use "$dirpath_data_sce/customer_data_20190916.dta", clear
rename nem_*_date nem_*_date_str
gen nem_start_date = date(nem_start_date_str, "DMY")
gen nem_end_date = date(nem_end_date_str, "DMY")
format nem_start_date %td
format nem_end_date %td

tempfile customer_temp
save "`customer_temp'"

use "$dirpath_data_sce/bill_data_20190916.dta", clear
merge m:1 serv_acct_num using "`customer_temp'"
rename meter_read_dt meter_read_dt_str
gen meter_read_dt = date(meter_read_dt_str, "YMD")
format meter_read_dt %td

count if kwh_usage == "0" & nem_pro != "" & meter_read_dt >= nem_start_date & meter_read_dt <= nem_end_date
*291

count if bill_amount == 0
*134

count if serv_acct_num == "29281167"
*139

count if bill_amount < 0
*200

count if bill_amount < 0 & nem_pro != "" & meter_read_dt >= nem_start_date & meter_read_dt <= nem_end_date


****** LOOK FOR MISSINGS IN BILLING DATA

use "$dirpath_data_sce/bill_data_20190916.dta", clear
count if kwh_usage == "0"
*652,590
di 652590/3517751
*.18551341

count if kwh_usage == "0" & monthly_max != . & monthly_max != 0
*20,855

gen year = substr(statl, 1, 4)
gen mo = substr(statl, 5, 6)
destring year, replace
destring mo, replace

tab year if kwh_usage == "0"
/*
       year |      Freq.     Percent        Cum.
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

tab mo if kwh_usage == "0"
/*
         mo |      Freq.     Percent        Cum.
------------+-----------------------------------
          1 |     69,641       10.67       10.67
          2 |     74,031       11.34       22.02
          3 |     63,739        9.77       31.78
          4 |     55,855        8.56       40.34
          5 |     49,228        7.54       47.89
          6 |     46,685        7.15       55.04
          7 |     45,939        7.04       62.08
          8 |     42,019        6.44       68.52
          9 |     43,704        6.70       75.21
         10 |     48,803        7.48       82.69
         11 |     54,683        8.38       91.07
         12 |     58,263        8.93      100.00
------------+-----------------------------------
      Total |    652,590      100.00
*/

****** INTERVAL DATA

/*
** Append into one file
tempfile interval_temp
local years "2016 2017 2018 2019"
foreach y in `years' {
	local reset = 1
	local interval_files : dir "$dirpath_data_sce" files "interval_data_`y'*.dta"
	foreach file in `interval_files' {
		di "`file'"
		if `reset' == 1 {
			use "$dirpath_data_sce/`file'", clear
			local reset = 0
		}
		else {
			append using "$dirpath_data_sce/`file'"
		}
	}
	gen date = date(substr(interval_date, 1, 9), "DMY")
	gen double interval_start = clock(substr(interval_start_dttm, 1, 18), "DMYhms")
	gen double interval_end = clock(substr(interval_end_dttm, 1, 18), "DMYhms")
	drop interval_date interval*dttm
	cap append using `interval_temp'
	save `interval_temp', replace
}
format date %td
format interval_start %tc
format interval_end %tc
sort service_account_id interval_start
save "$dirpath_raw/interval_data"
*/


** Negative usage
use "$dirpath_raw/interval_data", clear
keep if usage < 0
tostring service_account_id, gen(serv_acct_num)
merge m:1 serv_acct_num using "$dirpath_data_sce/customer_data_20190916.dta"

count
*801

count if nem_proatr_id == ""
*801


** Zero usage
use "$dirpath_raw/interval_data", clear
keep if usage == 0

count
*1,560,409,149

tostring service_account_id, gen(serv_acct_num)
keep serv_acct_num date usage
collapse (count) count=usage, by(serv_acct_num date)

tempfile interval_temp
save `interval_temp'

gen year = year(date)
collapse (sum) count, by(year)
tab year, summarize(count) means
/*
            | Summary of
            | (sum) count
       year |        Mean
------------+------------
       2016 |   4.326e+08
       2017 |   4.423e+08
       2018 |   4.286e+08
       2019 |   2.568e+08
------------+------------
      Total |   3.901e+08
*/

use `interval_temp', clear
gen month = month(date)
collapse (sum) count, by(month)
tab month, summarize(count) means
/*
            | Summary of
            | (sum) count
      month |        Mean
------------+------------
          1 |   1.656e+08
          2 |   1.452e+08
          3 |   1.600e+08
          4 |   1.445e+08
          5 |   1.424e+08
          6 |   1.284e+08
          7 |   1.281e+08
          8 |    97196505
          9 |   1.020e+08
         10 |   1.114e+08
         11 |   1.141e+08
         12 |   1.215e+08
------------+------------
      Total |   1.300e+08
*/

use "$dirpath_data_sce/customer_data_20190916.dta", clear
rename nem_*_date nem_*_date_str
gen nem_start_date = date(nem_start_date_str, "DMY")
gen nem_end_date = date(nem_end_date_str, "DMY")
format nem_start_date %td
format nem_end_date %td
keep serv_acct_num nem_proatr_id nem_start_date nem_end_date
merge 1:m serv_acct_num using `interval_temp'

summ count if nem_proatr_id != ""
di r(sum)
*14,592,315

summ count if nem_proatr_id != "" & date >= nem_start_date & date <= nem_end_date
di r(sum)
*9,502,377


****** MERGE BETWEEN CUSTOMER & INTERVAL DATA

** Missing SAs (in interval data) by account close year
use "$dirpath_data_sce/customer_data_20190916.dta", clear
count
*

gen acct_cl_date = date(sa_close_date, "DMY")
format acct_cl %td
gen acct_cl_year = year(acct_cl_date)

tempfile customer_temp
save "`customer_temp'"

use "$dirpath_raw/interval_data", clear
keep service_account_id
duplicates drop
count
*

tostring service_account_id, gen(serv_acct_num)
keep serv_acct_num
merge 1:1 serv_acct_num using "`customer_temp'"

tab acct_cl_year if _merge !=3


****** MERGE BETWEEN BILLING & INTERVAL DATA

** Compare billing and interval data by month
use "$dirpath_data_sce/bill_data_20190916.dta", clear
destring serv_acct_num, gen(service_account_id) 
destring kwh_usage, gen(kwh_bill)

gen year = substr(statl, 1, 4)
gen month = substr(statl, 5, 2)
destring year month, replace
keep if year >= 2016
gen mo_yr = ym(year, month)
format mo_yr %tm


keep service_account_id kwh_bill mo_yr
collapse (mean) kwh_bill, by(service_account_id mo_yr)

tempfile billing_temp
save `billing_temp'

use "$dirpath_raw/interval_data", clear
gen mo_yr = mofd(date)
format mo_yr %tm
collapse (sum) kwh_interval=usage, by(service_account_id mo_yr)

merge 1:1 service_account_id mo_yr using `billing_temp'
/*
Result                           # of obs.
-----------------------------------------
not matched                        22,062
    from master                    21,335  (_merge==1)
    from using                        727  (_merge==2)

matched                         1,067,012  (_merge==3)
-----------------------------------------
*/

corr kwh_bill kwh_interval
/*
             | kwh_bill kwh_in~l
-------------+------------------
    kwh_bill |   1.0000
kwh_interval |   0.9376   1.0000
*/

count if (kwh_interval > 1.5 * kwh_bill) | (kwh_bill > 1.5 * kwh_interval)
* 297,368


****** PUMP TEST DATA

/*
** Fix Excel formatting
import excel pump_ref customer_name zip_code pump_name pump_location service_acct sce_meter test_date pump_mfg pump_serial disch_psi ///
             standing_level drawdown suction_head suction_lift discharge_head pumping_level total_head gpm gpm_foot_drawdown acre_feet_24_hours ///
			 kw_input_to_motor hp_input_to_motor motor_load pump_rpm kwh_per_acre_foor overall_plant_efficiency customer_meter_gpm ///
			 overall_plant_efficiency_existin overall_plant_efficiency_improve pump_kwh_existing pump_kwh_improved kw_input_existing ///
			 kw_input_improved kwh_acre_foot_existing kwh_acre_foot_improved acre_foot_per_year_1 acre_foot_per_year_2 ///
			 average_cost_per_acre_foot_exist average_cost_per_acre_foot_impro total_annual_cost_existing total_annual_cost_improved ///
       using "T:\Raw Data\PumpData\Data09132019\Pump Test Data Extract.xlsx", cellrange(A3:AP40613) clear
save "$dirpath_data/sce_raw/pump_test_data_20190916"
*/


** Missing data
use "$dirpath_data/sce_raw/pump_test_data_20190916.dta", clear

count if service_acct == "SA999999999"
* 65

count if sce_meter == ""
* 66

count if overall_plant_efficiency_improve == 0
* 18,420

** Merge between pump test data and customer data
use "$dirpath_data/sce_raw/pump_test_data_20190916.dta", clear
rename service_acct serv_acct_num
keep serv_acct_num
replace serv_acct_num = subinstr(serv_acct_num, "SA", "", .)
duplicates drop

merge 1:1 serv_acct_num using "$dirpath_data_sce/customer_data_20190916.dta"
/*
Result                           # of obs.
-----------------------------------------
not matched                        36,604
    from master                     1,463  (_merge==1)
    from using                     35,141  (_merge==2)

matched                            10,102  (_merge==3)
-----------------------------------------
*/

keep if _merge == 1
keep serv_acct_num
rename serv_acct_num service_acct
replace service_acct = "SA" + service_acct
merge 1:m service_acct using "$dirpath_data/sce_raw/pump_test_data_20190916.dta"
keep if _merge == 3

use "$dirpath_data/sce_raw/pump_test_data_20190916.dta", clear
rename service_acct serv_acct_num
replace serv_acct_num = subinstr(serv_acct_num, "SA", "", .)

merge m:1 serv_acct_num using "$dirpath_data_sce/customer_data_20190916.dta"
/*
Result                           # of obs.
-----------------------------------------
not matched                        41,595
    from master                     6,454  (_merge==1)
    from using                     35,141  (_merge==2)

matched                            34,157  (_merge==3)
-----------------------------------------
*/

keep if _merge == 3
sort serv_acct_num test_date sce_meter meter_no
br serv_acct_num test_date sce_meter meter_no *


****** PUMP OVERHAUL DATA

/*
** Fix Excel formatting and append
tempfile pump_temp
local years ""2011 " 2012 2013 2014 2015 2016 2017 2018"
foreach y in `years' {
	if inlist("`y'", "2011 ", "2012", "2013", "2015", "2016") {
		import excel application_received_date contract_description invoice_received_date customer_name serv_acct_css_num ///
		             sa_address sa_city contact_zip product_description product_status product_substatus installed_quantity ///
					 kwh_savings kw_savings net_incentive_amount ///
			   using "T:\Raw Data\PumpData\Data09132019\Pump Overhauls 2011 to 2018_Rebate.xlsx", sheet("`y'") cellrange(A3) clear
	}
	else if "`y'" == "2014" {
		import excel application_received_date contract_description invoice_received_date customer_name serv_acct_css_num ///
		             sa_address sa_city contact_zip product_description product_status installed_quantity ///
					 kwh_savings kw_savings net_incentive_amount ///
               using "T:\Raw Data\PumpData\Data09132019\Pump Overhauls 2011 to 2018_Rebate.xlsx", sheet("`y'") cellrange(A3) clear
	}
	else if "`y'" == "2017" {
		import excel application_received_date contract_description invoice_received_date customer_name customer_address ///
		             customer_city customer_zip serv_acct_css_num sa_address sa_city contact_zip product_description ///
					 product_status product_substatus installed_quantity kwh_savings kw_savings net_incentive_amount ///
			   using "T:\Raw Data\PumpData\Data09132019\Pump Overhauls 2011 to 2018_Rebate.xlsx", sheet("`y'") cellrange(A2) clear
	}
	else if "`y'" == "2018" {
		import excel application_received_date contract_description invoice_received_date customer_name serv_acct_css_num ///
		             sa_address sa_city contact_zip product_description product_status product_substatus installed_quantity ///
					 kwh_savings kw_savings net_incentive_amount ///
			   using "T:\Raw Data\PumpData\Data09132019\Pump Overhauls 2011 to 2018_Rebate.xlsx", sheet("`y'") cellrange(A2) clear
		tostring product_substatus, replace
		replace product_substatus = "" if product_substatus == "."
	}
	cap append using `pump_temp'
	save `pump_temp', replace
}
drop if application_received_date == . & contract_description == ""
save "$dirpath_data/sce_raw/pump_overhaul_data_20190916"
*/

** Merge between pump overhaul data and customer data
use "$dirpath_data/sce_raw/pump_overhaul_data_20190916.dta", clear
rename serv_acct_css_num serv_acct_num
replace serv_acct_num = subinstr(serv_acct_num, "SA", "", .)
replace serv_acct_num = subinstr(serv_acct_num, "CU", "", .)

tempfile pump_merge_temp
save `pump_merge_temp'

keep serv_acct_num
duplicates drop

merge 1:1 serv_acct_num using "$dirpath_data_sce/customer_data_20190916.dta"
/*
Result                           # of obs.
-----------------------------------------
not matched                        44,721
    from master                       235  (_merge==1)
    from using                     44,486  (_merge==2)

matched                               757  (_merge==3)
-----------------------------------------
*/

keep if _merge == 1
keep serv_acct_num
merge 1:m serv_acct_num using `pump_merge_temp'
keep if _merge == 3

tab product_description
/*
                    product_description |      Freq.     Percent        Cum.
----------------------------------------+-----------------------------------
      Agricultural pump system overhaul |        368       87.83       87.83
Agricultural pump system overhaul - r.. |         32        7.64       95.47
        Industrial pump system overhaul |         13        3.10       98.57
Industrial pump system overhaul - ret.. |          6        1.43      100.00
----------------------------------------+-----------------------------------
                                  Total |        419      100.00
*/


******* EE DATA

** Merge between EE data and customer data
use "$dirpath_data/sce_raw/energy_efficiency_data_20190916.dta", clear
rename iouserviceaccountid serv_acct_num
keep serv_acct_num
duplicates drop

merge 1:1 serv_acct_num using "$dirpath_data_sce/customer_data_20190916.dta"
/*
Result                           # of obs.
-----------------------------------------
not matched                        40,174
    from master                         0  (_merge==1)
    from using                     40,174  (_merge==2)

matched                             5,069  (_merge==3)
-----------------------------------------
*/

******* DR DATA

** Merge between DR data and customer data
use "$dirpath_data/sce_raw/demand_response_data_20190916.dta", clear
keep serv_acct_num
duplicates drop

merge 1:1 serv_acct_num using "$dirpath_data_sce/customer_data_20190916.dta"
/*
Result                           # of obs.
-----------------------------------------
not matched                        41,026
    from master                         0  (_merge==1)
    from using                     41,026  (_merge==2)

matched                             4,217  (_merge==3)
-----------------------------------------
*/
