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

************************************************
***** 1: PUMP TEST PROJECT DATA
*use "$dirpath_data_pge/pump_test_project_data.dta", clear


***** 2: PUMP TEST DATA
use "$dirpath_data_pge_raw/pump_test_data.dta", clear

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

rename text569 annualcost
label variable annualcost "annual cost ($)"

rename text567 annualcostafter
label variable annualcostafter "annual cost ($) after upgrade"

rename ope ope_numeric
rename hp hp_numeric

rename txt* *
rename *after *_after





