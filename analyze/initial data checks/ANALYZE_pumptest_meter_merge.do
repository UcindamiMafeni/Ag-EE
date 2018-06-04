

use "S:\Matt\ag_pump\data\pge_cleaned\pump_test_data.dta" , clear
unique pge_badge_nbr
keep pge_badge_nbr test_date_stata apeptestid pumplatnew pumplongnew
unique apeptestid pge_badge_nbr
assert r(unique)==r(N)
duplicates drop
joinby pge_badge_nbr using "S:\Matt\ag_pump\data\pge_cleaned\xwalk_sp_meter_date.dta" , unmatched(both)
tab _merge
unique apeptestid 
local uniq = r(unique)
unique apeptestid if _merge==3
di r(unique)/`uniq'

unique pge_badge_nbr if apeptestid!=.

unique pge_badge_nbr if apeptestid!=. & _merge==3

sort _merge pge_badge_nbr
br if _merge!=2

hist test_date_stata if _merge==3
hist test_date_stata if _merge==1

duplicates t pge_badge_nbr, gen(dup)
tab dup
gen temp_date_match = inrange(test_date_stata,mtr_install_date, mtr_remove_date)
egen temp_date_match_max = max(temp_date_match), by(pge_badge_nbr)
egen temp_date_match_min = min(temp_date_match), by(pge_badge_nbr)
drop if temp_date_match==0 & temp_date_match_min<temp_date_match_max & _merge==3
gen temp_date_match2 = inrange(test_date_stata,sa_sp_start, sa_sp_stop)
egen temp_date_match2_max = max(temp_date_match2), by(pge_badge_nbr)
egen temp_date_match2_min = min(temp_date_match2), by(pge_badge_nbr)
drop if temp_date_match2==0 & temp_date_match2_min<temp_date_match2_max & _merge==3
duplicates t sa_uuid sp_uuid, gen(dup2)
br if dup2>0 & _merge==3
duplicates drop sa_uuid sp_uuid pge_badge_nbr, force
duplicates t sa_uuid sp_uuid, gen(dup3)
br if dup3>0 & _merge==3
sort sp_uuid sa_uuid pge_badge_nbr
drop temp* dup*
rename _merge merge_pumptest

rename pge_badge_nbr pge_badge_nbrM
drop sa_sp_start sa_sp_stop


joinby sa_uuid sp_uuid using "S:\Matt\ag_pump\data\pge_cleaned\pge_cust_detail.dta" , unmatched(both)
tab _merge

correlate pumplatnew prem_lat
correlate pumplongnew prem_lon
geodist pumplatnew pumplongnew prem_lat prem_lon, gen(distance) miles
hist distance if _merge==3
sum distance if _merge==3, detail

tab _merge merge, missing

twoway ///
(scatter pumplatnew pumplongnew if merge_pumptest==3, msize(vtiny)) ///
(scatter pumplatnew pumplongnew if merge_pumptest==1, msize(vtiny))


**** same thing with pump upgrade project data (a more important subset of meters)
use "S:\Matt\ag_pump\data\pge_raw\pump_test_project_data.dta", clear
rename MeteronProjectRecord pge_badge_nbr
duplicates drop pge_badge_nbr, force
joinby pge_badge_nbr using "S:\Matt\ag_pump\data\pge_cleaned\xwalk_sp_meter_date.dta" , unmatched(both)
tab _merge if _merge!=2


***********REDO full merge, keeping covariates to check for systematic differences
use "S:\Matt\ag_pump\data\pge_cleaned\pump_test_data.dta" , clear
unique pge_badge_nbr
unique apeptestid pge_badge_nbr
assert r(unique)==r(N)
duplicates drop
joinby pge_badge_nbr using "S:\Matt\ag_pump\data\pge_cleaned\xwalk_sp_meter_date.dta" , unmatched(both)
tab _merge
drop if _merge==2
duplicates t pge_badge_nbr, gen(dup)
tab dup
gen temp_date_match = inrange(test_date_stata,mtr_install_date, mtr_remove_date)
egen temp_date_match_max = max(temp_date_match), by(pge_badge_nbr)
egen temp_date_match_min = min(temp_date_match), by(pge_badge_nbr)
drop if temp_date_match==0 & temp_date_match_min<temp_date_match_max & _merge==3
gen temp_date_match2 = inrange(test_date_stata,sa_sp_start, sa_sp_stop)
egen temp_date_match2_max = max(temp_date_match2), by(pge_badge_nbr)
egen temp_date_match2_min = min(temp_date_match2), by(pge_badge_nbr)
drop if temp_date_match2==0 & temp_date_match2_min<temp_date_match2_max & _merge==3
duplicates t sa_uuid sp_uuid, gen(dup2)
br if dup2>0 & _merge==3
duplicates drop sa_uuid sp_uuid pge_badge_nbr, force
duplicates t sa_uuid sp_uuid, gen(dup3)
br if dup3>0 & _merge==3
sort sp_uuid sa_uuid pge_badge_nbr
drop temp* dup*
rename _merge merge_pumptest
drop sa_sp_start sa_sp_stop
joinby sa_uuid sp_uuid using "S:\Matt\ag_pump\data\pge_cleaned\pge_cust_detail.dta" , unmatched(both)
tab _merge
drop if _merge==2
duplicates drop apeptestid pge_badge_nbr, force

gen merge = merge_==3
drop merge_

	// CUSTOMER TYPE
tab customertype 
	// 6% of pumps are irrigation districts
tab customertype if merge==1
	// 15% of unmatched pumps are irrigation districts
tab customertype if merge==3
	// only 55/1041 irrigation district pumps matched
tab merge if customertype=="Individ Farms"
	// merge rate for ONLY farms is 63% (compared to 59% pooled)
	
	// WATER END USE
tab waterenduse customertype
tab merge if waterenduse=="agriculture"	
tab merge if waterenduse=="agriculture"	& customertype=="Individ Farms"
	// merge rate for ONLY ag end use id 63% (compared to 59% pooled)
	
	// FARM TYPE
tab farmtype, missing	
replace farmtype = "other" if farmtype=="dother"
replace farmtype = "other" if farmtype=="oth"
replace farmtype = "other" if farmtype=="oother"
replace farmtype = "other" if farmtype=="oher"
replace farmtype = "other" if farmtype=="othero"
replace farmtype = "other" if farmtype=="otherq"
replace farmtype = "other" if farmtype=="othr"
replace farmtype = "other" if farmtype=="othre"
tab farmtype, missing
tab farmtype merge, missing
tabstat merge, by(farmtype) s(count mean) missing
tabstat merge if waterenduse=="agriculture"	& customertype=="Individ Farms", ///
	by(farmtype) s(count mean) missing
	
	// TARIFF
replace ratesch = trim(itrim(upper(ratesch)))
tab ratesch, missing
	// WOW what a mess this variable!

	// TIMING
gen year = year(test_date_stata)	
tab year
tab year merge
tabstat merge, by(year)
	// nothing changing over time in terms of merge rate

	
	
*************
**** CONFIRM THAT PUMP TEST PROGRAM DATA MERGE INTO PUMP TEST DATA
use "S:\Matt\ag_pump\data\pge_raw\pump_test_project_data.dta", clear
rename MeteronProjectRecord pge_badge_nbr
duplicates drop pge_badge_nbr, force
gen id = _n
joinby pge_badge_nbr using "S:\Matt\ag_pump\data\pge_cleaned\pump_test_data.dta" , unmatched(both)
tab _merge
unique id 
local uniq = r(unique)
unique id if _merge==3
di r(unique)/`uniq'
keep if id!=.
br id pge_badge_nbr *TestDate* test_date_stata
gen date_diff = test_date_stata - SubsidizedPumpTestDate
gen date_diff2 = test_date_stata - PreProjectTestDate
gen date_diff3 = test_date_stata - PostProjectTestDate
gen date_match = date_diff==0 | date_diff2==0 | date_diff3==0
tab date_match if _merge==3, missing
egen date_match_min = min(date_match), by(pge_badge_nbr)
egen date_match_max = min(date_match), by(pge_badge_nbr)
br id pge_badge_nbr *TestDate* test_date_stata date* if date_match_min==0 & ///
	date_match_max==1

br if _merge==1
br if pge_badge_nbr=="1003718784" | test_date_stata==20187

	


	
	
	

