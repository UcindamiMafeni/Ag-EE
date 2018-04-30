
*** plot number of customers by day
use "S:/Matt/ag_pump/data/pge/interval_data", clear
keep sa_uuid usg_dt
duplicates drop
merge m:m sa_uuid using "S:/Matt/ag_pump/data/pge/customer_data", keepusing(prsn_uuid) nogen keep(3)
keep prsn_uuid usg_dt
duplicates drop
gen date = date(usg_dt, "DMY")
format date %td
keep prsn_uuid date
collapse (count) count = prsn_uuid, by(date)
twoway line count date, xtitle("date") ytitle("customers")
graph export "S:/Matt/ag_pump/output/customers_interval.png"
