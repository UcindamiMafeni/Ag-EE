*** plot aggregate consumption by day
use "S:/Matt/ag_pump/data/pge/interval_data", clear
keep usg_dt kwh
collapse (sum) usage = kwh, by(usg_dt)
gen date = date(usg_dt, "DMY")
keep date usage
format date %td
replace usage = usage / 10^6
sort date
twoway line usage date, xtitle("date") ytitle("aggregate consumption (GWh)")
graph export "S:/Matt/ag_pump/output/consumption_interval.png"
