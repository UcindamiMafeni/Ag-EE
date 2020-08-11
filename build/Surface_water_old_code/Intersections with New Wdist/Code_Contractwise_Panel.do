global path_contract_matches = "C:\Users\clohani\Dropbox\EW_Downloads\Code\Deliverables\Matches"
global path_contract_alloc= "C:\Users\clohani\Dropbox\Water_Data"
global path_out= "C:\Users\clohani\OneDrive\Desktop\Create Panel"
global path_temp= "C:\Users\clohani\OneDrive\Desktop\Create Panel/Temp"
global path_downloads= "C:\Users\clohani\Downloads"
//we will read each of the matches and merge the allocations on them in order to generate a yearly panel of water for each type of contract


*** Colorado
//there are multiple contracts with the same ID over different dates, we will select the latest one
use "$path_contract_alloc/Colorado/Colorado_Contracts_Mod.dta", clear
duplicates tag name, generate(dupes)
sort dupes name contract_id date
gen to_drop=0
local drops 78 80 81 84 86 88 90 91 93 95 97 99 101 103 106 107 109 110 111 112 113 114 115 116
foreach drop of local drops {
	replace to_drop=1 in `drop'
}
drop if to_drop==1
drop dupes to_drop


duplicates tag contract_id, generate(dupes)
sort dupes name contract_id date
//stop
gen to_drop=0
local drops 79 81 83 84 90
foreach drop of local drops {
	replace to_drop=1 in `drop'
}
drop if to_drop==1
drop dupes to_drop 

save "$path_temp/Colorado_allocations.dta", replace

use "$path_contract_matches/Colorado/Collected_Final_Matches.dta", clear
replace name= "Coachella Valley Water District (6a)" if name== "Coachella Valley Water District (3a)5"
replace name= "Metropolitan Water District of Southern California" if name== "The Metropolitan Water District of Southern California"
// Coachella Valley Water District (6a) <- Coachella Valley Water District (3a)5
 // The Metropolitan Water District of Southern California -> Metropolitan Water District of Southern California
merge m:1 name using "$path_temp/Colorado_allocations.dta"
keep if _merge==3
gen water=0
replace water= water + entitlement if !missing(entitlement)
replace water= water + consumptive if !missing(consumptive)

collapse (sum) water, by(AGENCYNAME)
label variable water "in acre-feet"
//expand 
//save "$path_out/


*** CVP

*** SWP
use "$path_downloads/Modern_allocations.dta", clear
collapse (sum) initial* final* (mean) percent*, by(name)
save "$path_temp/SWP_allocations_new.dta", replace

use "$path_downloads/Old_Contracts_WF.dta", clear
rename (allocation*) (final*)
forvalues i=1996(1)2035 {
	drop final_`i'
}

rename names name
replace name= "Antelope Valley-East Kern WA" if name=="AVEK"
replace name= "Alameda County WD" if name=="AlamedaCounty"
replace name= "Alameda County FC&WCD, Zone 7" if name=="Alameda_Zone7"
replace name= "County of Butte " if name=="Butte"
replace name= "Castaic Lake WA" if name== "Castaic_Lake"
replace name= "Coachella Valley WD" if name== "Coachella"
replace name= "Crestline-Lake Arrowhead WA" if name== "Crestline"
replace name= "Desert WA" if name== "Desert"
replace name= "Dudley Ridge WD" if name== "DudleyRidge"
replace name= "Empire West Side ID" if name== "Empire"
replace name= "Kern County WA" if name== "KernAg"
replace name= "Kern County WA" if name== "KernM_I"
replace name= "County of Kings" if name== "Kings"
replace name= "Littlerock Creek ID" if name== "Littlerock"
replace name= "Metropolitan WDSC" if name== "Metropolitan"
replace name= "Mojave WA" if name== "Mojave"
replace name= "Napa County FC&WCD " if name== "Napab"
replace name= "Oak Flat WD" if name== "OakFlat"
replace name= "Palmdale WO" if name== "Palmdale"
replace name= "Plumas County FC&WCD" if name== "Plumas"
replace name= "San Gorgonio Pass WA" if name== "SanGorgonio"
replace name= "San Luis Obispo County FC&WCD" if name== "SanLuisObispo"
replace name= "San Bernardino Valley MWD" if name== "San_Bernardino"
replace name= "San Gabriel Valley MWD" if name== "San_Gabriel"
replace name= "Santa Barbara County FC&WCD" if name== "SantaBarbara"
replace name= "Santa Clara Valley WD" if name== "SantaClara"
replace name= "Solano County WA" if name== "Solano"
replace name= "Tulare Lake Basin WSD" if name== "Tulare"
replace name= "Ventura_County FCD" if name== "Ventura"
drop if name=="SouthBayAreaFutureContractor"
collapse (sum) final* , by(name)

save "$path_temp/SWP_allocations_old.dta", replace

use "$path_contract_matches/SWP/Collected_Final_Matches.dta", clear
merge m:1 name using "$path_temp/SWP_allocations_new.dta"
drop _merge
merge m:1 name using "$path_temp/SWP_allocations_old.dta"
keep if _merge==3
drop _merge
collapse (sum) final*, by(AGENCYNAME)
save "$path_out/SWP_Panel.dta", replace


*** WRIMS

import delimited "$path_downloads/haggerty_wr70_cleaned.csv", varnames(1) clear
keep primary_owner latitude longitude reported_diversion_total* reported_use_total* reported_diversion_year
drop if missing(reported_diversion_year)
drop if reported_diversion_year=="NA"
drop reported_diversion_year
destring reported*, replace force
local vars reported_use_total reported_use_total_1 reported_use_total_2 reported_use_total_3
foreach var of local vars {
	replace `var'=0 if missing(`var')
}
rename primary_owner name_PO
collapse (sum) reported*, by(name_PO latitude longitude)
save "$path_temp/WRIMS_allocations.dta", replace

use "$path_contract_matches/WRIMS/Final_Matches.dta", clear
merge m:1 name_PO latitude longitude using "$path_temp/WRIMS_allocations.dta"
rename name_DBF AGENCYNAME
collapse (sum) reported*, by(AGENCYNAME)
