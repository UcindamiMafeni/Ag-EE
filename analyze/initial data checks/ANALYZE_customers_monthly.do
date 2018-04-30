
*** plot number of customers by month
use "S:/Matt/ag_pump/data/pge/bill_data", clear
gen bill_end_date = date(bseg_end_dt, "DMY")
gen bill_end_month = mofd(bill_end_date)
format bill_end_month %tm
keep sa_uuid bill_end_month
duplicates drop
merge m:m sa_uuid using "S:/Matt/ag_pump/data/pge/customer_data", keepusing(prsn_uuid) nogen keep(3)
keep prsn_uuid bill_end_month
duplicates drop
collapse (count) count = prsn_uuid, by(bill_end_month)
twoway line count bill, xtitle("month") ytitle("customers")
graph export "S:/Matt/ag_pump/output/customers_monthly.png"
