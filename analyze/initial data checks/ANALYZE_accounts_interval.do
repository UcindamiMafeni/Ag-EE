*** load interval data and save as dta file
import delim using "S:/Raw Data/PumpData/PGE_01312018_rn1765829728_5.csv"
save "S:/Matt/ag_pump/data/old/PGE_01312018_rn1765829728_5"

*** plot number of accounts by day
use "S:/Matt/ag_pump/data/old/PGE_01312018_rn1765829728_5", clear
keep sa_uuid usg_dt
duplicates drop
gen date = date(usg_dt, "DMY")
format date %td
keep sa_uuid date
collapse (count) count = sa_uuid, by(date)
twoway line count date, xtitle("date") ytitle("accounts")
graph export "S:/Matt/ag_pump/output/accounts_interval.png"
