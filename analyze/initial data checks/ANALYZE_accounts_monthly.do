*** load monthly billing datasets and save as dta files
import delim using "S:/Raw Data/PumpData/PGE_01312018_rn1765829728_3.csv"
save "S:/Matt/ag_pump/data/old/bill_data_1516"
import delim using "S:/Raw Data/PumpData/EDRP UC Berkeley Ag - Billing 2008 - 2010.csv"
save "S:/Matt/ag_pump/data/old/bill_data_0810"
import delim using "S:/Raw Data/PumpData/EDRP UC Berkeley Ag - Billing 2011 - 2014 2017.csv"
save "S:/Matt/ag_pump/data/old/bill_data_111417"

*** append monthly billing datasets into one dta file
use "S:/Matt/ag_pump/data/old/bill_data_0810", clear
append using "S:/Matt/ag_pump/data/old/bill_data_111417"
append using "S:/Matt/ag_pump/data/old/bill_data_1516"
save "S:/Matt/ag_pump/data/old/bill_data"

*** plot number of accounts by month
use "S:/Matt/ag_pump/data/old/bill_data", clear
gen bill_end_date = date(bseg_end_dt, "MDY")
gen bill_end_month = mofd(bill_end_date)
format bill_end_month %tm
keep sa_uuid bill_end_month
duplicates drop
collapse (count) count = sa_uuid, by(bill_end_month)
twoway line count bill, xtitle("month") ytitle("accounts")
graph export "S:/Matt/ag_pump/output/accounts_monthly.png"
