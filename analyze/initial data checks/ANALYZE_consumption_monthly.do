*** plot aggregate consumption by month
use "S:/Matt/ag_pump/data/pge/bill_data", clear
gen bill_end_date = date(bseg_end_dt, "DMY")
gen bill_end_month = mofd(bill_end_date)
format bill_end_month %tm
keep bill_end_month total_electric_usage
collapse (sum) usage = total, by(bill)
replace usage = usage / 10^6
twoway line usage bill, xtitle("month") ytitle("aggregate consumption (GWh)")
graph export "S:/Matt/ag_pump/output/consumption_monthly.png"
