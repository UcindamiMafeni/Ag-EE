************************************************
* Build and clean PGE pump test data.
************************************************
**** SETUP:
clear all
memory clear
set more off, perm
version 12

global dirpath "S:/Matt/ag_pump"
** additional directory paths to make things easier
global dirpath_data "$dirpath/data"
global dirpath_data_pge_raw "$dirpath_data/pge_raw"
global dirpath_data_pge_cleaned "$dirpath_data/pge_cleaned"

************************************************

***** PUMP TEST DATA
use "$dirpath_data_pge_raw/pump_test_data_20180322.dta", clear

rename *, lower

foreach var of varlist * {
 label variable `var' ""
}

** drop variables that are always missing
drop yield text859 list527 text857 frmbustype txthoursafter

** drop variables that are identical to other variables always
drop text620 text561

** drop check-box variables (1:1 mapping with other variables in the data)
drop check*


label variable acreage "total farm acreage"
label variable acreageservedbypump "acreage served by pump"
label variable farmtype "type of farm"
label variable crop "crop type"
label variable saidhours "annual pump use hours (default 1000)"
label variable waterenduse "water use"
label variable apeptestid "unique test identifier"
label variable testdate "pump test date"
label variable pumplatnew "pump latitude"
label variable pumplongnew "pump longitude"
label variable mtrmake "motor make"
label variable mtrsn "motor serial nr"
label variable mtrv "motor volts"
label variable mtra "motor amps"
label variable hp "motor horsepower"
label variable mtreff "motor efficiency"
label variable meterno "pge meter badge number"
label variable meterkh "pge meter kh"
label variable metercon "pge meter constant"
label variable ratesch "rate schedule"
label variable avgcost "average cost ($ per kWh)"
label variable watrsrc "water source"
label variable pumpmke "pump make"
label variable gearheadmake "gear head make"
label variable drvtype "drive type"
label variable pumptyp "pump type"
label variable testsectiondiameter "test section diameter"
label variable rpmattach "rpm at tachometer"
label variable rpmatgh "rpm at gear head"
label variable namepltrpm "nameplate rpm"
label variable economicanalysis "use this run for economic analysis?"
label variable run "run number"
label variable ofruns "total number of runs"
label variable metrdskrevs "meter disk revolutions"
label variable metrdsktime "meter disk time (seconds)"
label variable volts12 "volts 1-2"
label variable volts13 "volts 1-3"
label variable volts23 "volts 2-3"
label variable amp1 "amps 1"
label variable amp2 "amps 2"
label variable amp3 "amps 3"
label variable pf "power factor"
label variable pwl "pumping water level (ft)"
label variable swl "wells: standing water level (ft)"
label variable rwl "wells: recovered water level (ft)"
label variable dchpres "discharge pressure (psi)"
label variable dchlvl "discharge level (ft)"
label variable drwdwn "drawdown (ft)"
label variable ggecor "gauge correction (ft)"
label variable gaugeheight "gauge height (ft)"
label variable otherlosses "other losses (ft)"
label variable totlift "total lift (ft)"
label variable phgpm "tester's gallons per minute"
label variable customergpm "customer's gallons per minute"
label variable kwi "kw input"
label variable kwik "disk read kw"
label variable hpi "horsepower input"
label variable hpi2 "horsepower input 2"
label variable mtrload "motor load (%)"
label variable kwhaf "kwh per acre-foot"
label variable khmg "kwh per million gallons"
label variable af24hrs "acre feet per 24h"
label variable mg24hrs "million gallons per 24h"
label variable whp "water hp"
label variable ope "overall pump efficiency (%)"
label variable crosssection "cross-section square ft"
label variable tstgpm "tested gallons per minute"
label variable flow "flow velocity (ft/sec)"
label variable notes "notes"
label variable idealope "ideal overall pump efficiency (%)"
label variable cubicft "tested gallons per minute (cubic ft)"
*label variable total_lift "total lift (ft)"
label variable hpi3 "horsepower input 3"
label variable txtopeafter "overall pump efficiency after project (%, assumed)"
label variable txtflowafter "flow after project (gallons per minute, assumed)"
label variable txtdpafter "discharge pressure after project (psi)"
label variable txtpwlafter "pumping water level after project (ft)"
label variable cmbmotorhilo "???"
label variable txtope "user set ideal overall pump efficiency (%)"
label variable txtafpumped "acre-feet pumped"
label variable txthpafter "nameplate hp after project"
label variable txttdhafter "tdh after project (discharge pressure + pwl, ft)"
label variable txtafafter "acre-feet pumped after project"
label variable txttdh "tdh (ft)"
label variable txthp "nameplate horsepower"

drop text842 text597 text889 combo891 combo642  combo903 text913 combo915 ///
 text866  text886  text564 text552 text901 text579 text583 combo638 text583 txtdpafter ///
 text888 text566


rename text616 subsidy
label variable subsidy "pump test subsidy ($)"

rename combo698 customertype
label variable customertype "customer type"

rename combo573 metertype 
label variable metertype "pge meter type"

rename text624 directreadkw
label variable directreadkw "direct read of kw"

rename text847 measuredpowerfactor
label variable measuredpowerfactor "measured power factor"

rename frame864 holddpconstant
label variable holddpconstant "hold discharge pressure constant (Y/N)"

rename cmbmotorhilo afterloadoutofrange
label variable afterloadoutofrange "reason why after load >115% or <80%"

rename text862 memoforreport
label variable memoforreport "memo for report"

rename text567 annualcost
label variable annualcost "annual cost ($)"

rename text569 annualcostafter
label variable annualcostafter "annual cost ($) after upgrade"

rename ope ope_numeric
rename hp hp_numeric

rename txt* *
rename *after *_after

******** CLEAN ACTUAL DATA

** DROP ACREAGE DATA - MISSING ALMOST EVERYWHERE
drop acreage acreageserved

replace farmtype = lower(farmtype)
replace farmtype = trim(farmtype)

replace farmtype = "vineyard" if farmtype == "bin"
replace farmtype = "cut flowers" if farmtype == "flowers"
replace farmtype = "other" if farmtype == "food processor"
replace farmtype = "fruit" if farmtype == "fui"
replace farmtype = "" if farmtype == "g"
replace farmtype = "general row crops" if strpos(farmtype, "general row") | farmtype == "genneral row crops"
replace farmtype = "other" if farmtype == "ither" | farmtype == "misc"
replace farmtype = "nuts" if farmtype == "nurts" | farmtype == "nut" | ///
  farmtype == "nuts/citrus" | farmtype == "nuts/fruit" | farmtype == "nuts/row crop"
  
replace farmtype = "other" if farmtype == "open" | farmtype == "sod"
replace farmtype = "trees" if strpos(farmtype, "tre")
replace farmtype = "other" if strpos(farmtype, "vario")
replace farmtype = "vineyard" if strpos(farmtype, "vin") | strpos(farmtype, "wine")
replace farmtype = "other" if farmtype == "water"  

** come back to me?
replace crop = lower(crop)
replace crop = trim(crop)

replace waterenduse = lower(waterenduse)
replace waterenduse = "" if waterenduse == "." | waterenduse == "1800"
replace waterenduse = "agriculture" if strpos(waterenduse, "agri")
replace waterenduse = "district" if strpos(waterenduse, "district")
replace waterenduse = "irrigation" if strpos(waterenduse, "ir") 
replace waterenduse = "" if waterenduse == "stock"


split testdate, p("/")
destring testdate1 testdate2 testdate3, replace
gen date_stata = mdy(testdate1, testdate2, testdate3)
 
drop testdate*

replace mtrmake = lower(mtrmake)

// motors don't have no amps or no volts, this is a function of the form
replace mtrv = . if mtrv == 0 | mtrv > 4970
replace mtra = . if mtra == 0
replace mtreff = . if mtreff == 0

// consistent with cleaning in the customer file
rename meterno pge_badge_nbr

replace meterkh = . if meterkh == 0

//not useful
drop metertype metercon

// COME BACK AND CLEAN THE RATE SCHEDULE

replace avgcost = . if avgcost == 0

replace testsectiondiameter = . if testsection == 0


replace directreadkw = . if directreadkw == 0
replace measuredpowerfactor = . if measuredpowerfactor ==.

replace rpmattach = . if rpmattach == 0
replace rpmatgh = . if rpmatgh == 0
replace namepltrpm = . if namepltrpm == 0
replace namepltrpm = . if namepltrpm > 3600 // one extreme outlier here

// about 4 issues with this
replace ofruns = run if run <= ofruns

// should be in 0-1 units, a few are in 0-100
replace measuredpower = measuredpower/100 if measuredpower > 1
replace measuredpower = . if measuredpower == 0


replace metrdskrevs = . if metrdskrevs == 0
replace metrdsktime = . if metrdsktime == 0

replace volts12 = . if volts12 == 0
replace volts13 = . if volts13 == 0
replace volts23 = . if volts23 == 0

replace amp1 = . if amp1 == 0
replace amp2 = . if amp2 == 0
replace amp3 = . if amp3 == 0

replace pf = pf / 100 if pf > 1
replace pf = . if pf == 0


// water level variables should be in feet down (all the same sign)
replace pwl = -pwl if pwl < 0
replace swl = -swl if swl < 0
replace rwl = -rwl if rwl < 0

// psi can't be negative
replace dchpres = -dchpres if dchpres <0

//discharge level shouldn't be negative
destring dchlvl, replace
replace dchlvl = -dchlvl if dchlvl < 0

// drawdown level should not be negative
destring drwdwn, replace
replace drwdwn = -drwdwn if drwdwn < 0

// other losses should not be negative
replace otherlosses = -otherlosses if otherlosses < 0

destring totlift, replace

replace customergpm = . if customergpm == 0

destring kwi kwik hpi hpi2, replace

replace kwi = . if kwi == 0
replace kwik = . if kwik == 0
replace hpi = . if hpi == 0
replace hpi2 = . if hpi2 == 0

// some of the motor load percentages are over 100, but everything seems to be
// in the same units, so....?

destring khmg af24hrs mg24hrs whp, replace
replace khmg = . if khmg == 0


destring crosssection, replace

// a few extreme positive outliers here
destring flow, replace

// put ideal ope in the same units as ope_numeric
replace idealope = idealope * 100 if idealope < 1


destring cubicft hpi3, replace

replace hpi3 = . if hpi3 == 0

// a few of these are over 100? that seems odd
replace ope_after = . if ope_after == 0

replace flow_after = . if flow_after == 0

// CHECK ON HOLDDP CONSTANT - WHAT DOES 2 MEAN?

replace pwl_after = -pwl_after if pwl_after <0

destring ope, replace

replace afpumped = subinstr(afpumped, ",", "", .)
destring afpumped, replace

replace hp_after = . if hp_after == 0
replace tdh_after = . if tdh_after == 0
replace af_after = . if af_after == 0

replace tdh = subinstr(tdh, ",", "", .)
destring tdh, replace

replace  annualcost = subinstr(annualcost, ",", "", .)
replace  annualcost = subinstr(annualcost, "$", "", .)
destring annualcost, replace
replace annualcost = . if annualcost == 0



replace  annualcost_after = subinstr(annualcost_after, ",", "", .)
replace  annualcost_after = subinstr(annualcost_after, "$", "", .)
destring annualcost_after, replace
replace annualcost_after = . if annualcost_after == 0

rename date_stata test_date_stata
format test_date_stata %td

foreach v of varlist * {
	cap replace `v' = trim(itrim(`v'))
}

compress
save "$dirpath_data_pge_cleaned/pump_test_data.dta", replace

