

use "S:\Matt\ag_pump\data\pge_cleaned\billing_data_monthified.dta" , clear
joinby sa_uuid using "S:\Matt\ag_pump\data\pge_cleaned\pge_cust_detail.dta" , unmatched(both)
tab _merge
keep if _merge==2 | (sp_uuid==sp_uuid1 | sp_uuid==sp_uuid2 | sp_uuid==sp_uuid3)
unique sp_uuid sa_uuid modate if _merge==3
assert r(unique)==r(N) 
gen temp = ym(year(sa_sp_start),month(sa_sp_start))
egen first_SP_start = min(temp), by(sp_uuid)
gen temp2 = modate if _merge==3
egen first_SP_bill = min(temp2), by(sp_uuid)
format %tm first_SP_start first_SP_bill
drop if _merge==2
assert modate!=.
di _N
collapse (sum) mnth_bill_kwh, by(sp_uuid modate first_SP_start first_SP_bill) fast
di _N
sort sp_uuid modate
unique sp_uuid modate
assert r(unique)==r(N)

egen sp_tag = tag(sp_uuid) 
hist first_SP_start if sp_tag==1
hist first_SP_start if sp_tag==1 & first_SP_start>=ym(2008,1)
hist first_SP_bill if sp_tag==1
hist first_SP_bill if sp_tag==1 & first_SP_start>=ym(2008,1)
twoway scatter first_SP_bill first_SP_start if sp_tag, msize(vtiny) jitter(3)

egen n_mths = count(modate), by(sp_uuid)
hist n_mths if sp_tag 


