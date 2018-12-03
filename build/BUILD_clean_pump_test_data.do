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
assert mi(yield) 
assert mi(text859)
assert mi(list527)
assert mi(text857)
assert mi(frmbustype)
assert mi(txthoursafter)
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

// SOME BETTER LABELING!
gen TEMP_SORT = _n


assert phgpm==tstgpm
drop phgpm
rename tstgpm flow_gpm
la var flow_gpm "Pump flow rate, gallons per minute (measured)"
rename customergpm flow_cust_gpm 
la var flow_cust_gpm "Pump flow rate, gallons per minute (per customer's flow meter)"
rename flow_after flow_gpm_after 
la var flow_gpm_after "Pump flow rate after project, gallons per minute (assumed)"
rename flow flow_veloc_fts
la var flow_veloc_fts "Flow velocity (ft/sec), or how fast water moves in discharge pipe"

la var pwl "Pumping water level (ft), stabilized under constant pumping conditions"
la var pwl_after "Pumping water level after project (ft; assumed)"

la var swl "Standing water level (ft), when the pump is not running"
la var rwl "Recovered water level (ft), ~15 min after shutting off pump"
la var drwdwn "Drawdown (ft), or the difference between RWL and PWL"
gen temp = pwl-swl
gen temp2 = pwl-rwl
count if temp==drwdwn // drawdown is supposed to be PWL-SWL
count if temp2==drwdwn // drawdown is actually PWL-RWL
assert temp==temp2 if temp==drwdwn
assert swl==rwl if temp==drwdwn 
br swl pwl drwdwn temp2 rwl
br swl pwl drwdwn temp2 rwl if round(temp2,0.001)!=round(drwdwn,0.001) & rwl!=0
gen flag_bad_drwdwn = round(temp2,0.001)!=round(drwdwn,0.001) & rwl!=0
la var flag_bad_drwdwn "Flag for drawdown inconsistent with PWL/RWL, b/c PWL < RWL!"
assert pwl<rwl if flag_bad_drwdwn==1
br swl pwl drwdwn temp2 rwl if temp2!=drwdwn & rwl==0
count if swl!=0 & swl==rwl
count if swl!=0 & swl!=rwl // RWL=SWL almost always, except where 0
replace swl = rwl if swl==0 // a few cases where RWL!=0 but SWL==0
replace swl = . if swl==0
replace rwl = . if rwl==0
assert drwdwn==. | drwdwn==0 if rwl==.
replace drwdwn = . if rwl==.
drop temp*

la var dchpres "Discharge pressure at gauge (psi; 1 psi = 2.31 feet of water head)"
rename dchpres dchpres_psi
la var dchlvl "Discharge level at gauge (feet; 1 psi = 2.31 feet of water head)"
rename dchlvl dchlvl_ft
gen temp = dchlvl_ft/dchpres_psi
sum temp, det // not an exact conversion, but very close
drop temp

la var ggecor "Gauge correction (ft), an input to calculating total lift"
rename ggecor gaugecor_ft
rename gaugeheight gaugeheight_ft 
la var gaugeheight_ft "Gauge height (ft), always zero if gaugecor_ft>0"
assert gaugecor_ft==0 | gaugeheight_ft==0

gen temp1 = pwl + dchlvl_ft + gaugecor_ft
gen temp2 = temp1 + gaugeheight_ft
gen temp3 = temp2 + otherlosses
correlate temp1 totlift // great!
correlate temp2 totlift // great!
correlate temp3 totlift // great!
br pwl dchpres_psi dchlvl_ft gaugecor_ft gaugeheight_ft otherlosses temp1 temp2 temp3 totlift
gen temp1_diff = abs(totlift - temp1)
gen temp2_diff = abs(totlift - temp2)
gen temp3_diff = abs(totlift - temp3)
count if temp1_diff<1 // 19593 out of 21851
count if temp2_diff<1 // 19719 out of 21851
count if temp3_diff<1 // 20529 out of 20851
count if temp2_diff>1 & temp1_diff<1 // temp2 (almost) strictly dominates temp1
count if temp3_diff>1 & temp2_diff<1 // temp3 (almost) strictly dominates temp2
sort temp3_diff
br pwl dchpres_psi dchlvl_ft gaugecor_ft gaugeheight_ft otherlosses temp1 temp2 temp3 totlift ///
	temp*diff if otherlosses==0
br pwl dchpres_psi dchlvl_ft gaugecor_ft gaugeheight_ft otherlosses temp1 temp2 temp3 totlift ///
	temp*diff if otherlosses!=0
gen temp4 = temp3_diff<1
tab flag_bad_drwdwn temp4
tab temp4 if notes!=""
tab temp4 if notes==""
rename otherlosses otherlosses_ft 
la var otherlosses_ft "Other losses (ft), or minor chnages to total lift calcuation"
gen totlift_gap = totlift - temp3
la var totlift_gap "Total lift (reported) - derived value (pwl+dchlvl+gaugecor+gaugeheight+otherlosses)"
la var totlift "Total lift (ft, reported)"
sum totlift_gap, det
replace totlift_gap = round(totlift_gap,0.0001)
sort totlift_gap
br pwl dchlvl_ft gaugecor_ft gaugeheight_ft otherlosses temp3 totlift totlift_gap
drop temp*

gen temp = tdh - totlift
sum temp, det
assert inrange(temp,-0.5,0.5)
assert round(tdh,1)==tdh
la var tdh "Total dynamic head (ft), equalivalent to total lift, but integer-valued for APEP calcs"
la var tdh_after "Total dynamic head (ft), after project (assumed)"
drop temp

la var af24hrs "Acre-feet per 24 hours, at the measured flow rate"
gen temp = flow_gpm*60*24/325900 - af24hrs
sum temp, detail 
assert inrange(temp,-0.55,0.5)
gen temp2 = flow_gpm*60*24/325900
br af24hrs temp2 temp // same thing, with more sig figs!
replace af24hrs = temp2
gen af24hrs_after = flow_gpm_after*60*24/325900
la var af24hrs_after "Acre-feet per 24 hours, after project (derived from flow_gpm_after)"
drop temp*

correlate cubicft flow_gpm
la var cubicft "Pump flow rate, cubic feet per second (measured)"
rename cubicft flow_cfps

br *hp*  // almost certainly mislabeled, b/c non-nameplate is the one that's round numbers
gen temp = flow_gpm*tdh/(hp*ope)
gen temp2 = flow_gpm*totlift/(hp*ope)
gen temp3 = flow_gpm*tdh/(hp*ope_numeric)
gen temp4 = flow_gpm*totlift/(hp*ope_numeric)
sum temp*, detail
	// HORSEPOWER FORMULA IS HP = (   FLOW    * TOTLIFT) / (39.60 * OPE)
    //                              gall/min     feet                % 
	//
	// I can't figure out where 39.60 comes from!
	// 1 gallon of watter weighs 8.34 lbs, and 1 HP = 550 ft-lb/sec
	//
	// so, the OPE*HP should equal:
	// FLOW (gall/min) * TOTLIFT (ft) * (8.34 lb/ft) * (1 min/60 sec) * (1/550)
gen temp5 = flow_gpm*totlift*8.34/60/550	
gen temp6 = hp*ope_numeric/100
br temp5 temp6
correlate temp5 temp6 // But that works?!
di (8.34/60/550)^(-1)/100 // AHA I figured it out! Take that physics!
gen temp7 = flow_gpm_after*tdh_after*8.34/60/550
gen temp8 = hp_after*ope_after/100	
br temp5 temp6 temp7 temp8
correlate temp7 temp8 //  Also works!
rename hpi hpi1
la var hpi1 "Horsepower input 1 (of up to 3)"	
la var hpi2 "Horsepower input 2 (of up to 3)"
la var hpi3 "Horsepower input 3 (of up to 3)"
rename hp_numeric hp_nameplate
la var hp_nameplate "Nameplate horsepower (all round numbers"
la var hp "Horsepower input to motor (measured)"
la var hp_after "Horsepower input to motor after project (assumed)"
drop temp*

rename whp water_hp 
la var water_hp "Water horsepower output of motor (measured)"
gen temp = 100*water_hp/hp 
br water_hp hp temp ope 
count if abs(temp-ope)>1 // only 11 of 21851
	// OPE is defined as water HP / HP, or HP out / HP in
gen water_hp_after = hp_after*ope_after
la var water_hp_after "Water horsepower output after project (assumed, from hp_afer ope_after)"
drop temp*	
	
la var mtreff "Motor efficiency (%)"	
la var mtrload "Motor load (%), or HP input * motor efficiency / nameplate HP"
gen temp = hp/hp_nameplate*mtreff
br water_hp hp temp ope hp_nameplate mtrload mtreff
correlate temp mtrload // not right!
reg temp mtrload, nocons
gen temp2 = abs(temp-mtrload)
sum temp2, detail
drop temp*

rename kwi kw_input
la var kw_input "Kilowatt input to motor (1 HP = 0.746 kW)"
gen temp = kw_input/hp
sum temp, detail
sort temp
br hp kw_input temp // this variable is a mess, so i'm gonna fix it
replace kw_input = hp*0.7457 if !inrange(temp,0.745,0.747)
gen temp2 = kw_input/hp
sort temp2 temp
br hp kw_input temp temp2 
gen kw_input_after = hp_after*0.7457
la var kw_input_after "Kilowatt input to motor after project (assumed, derived from hp_after)"
drop temp*

la var kwhaf "Kilowatt-hours per acre-foot, given test conditions"
gen temp = kwhaf * af24hrs/24
br kwhaf af24hrs kw_input temp
correlate kw_input temp // perfectly correlated
reg temp kw_input, nocons // dead on, except rounding!
gen temp2 = kw_input - temp
sort temp2
gen kwhaf_after = kw_input_after * 24/af24hrs
br kw_input kw_input_after af24hrs af24hrs_after kwhaf kwhaf_after temp2
la var kwhaf_after "Kilowatt-hours per acre-foot after project (assumed, derived from kw_input_after af24hrs_after)"
drop temp*

rename avgcost avgcost_kwh
la var avgcost_kwh "Average cost of electricity ($/kWh)"
count if avgcost_kwh==. | avgcost_kwh==0

rename namepltrpm rpm_nameplate
rename rpmatgh rpm_gearhead
rename rpmattach rpm_tachometer
la var rpm_nameplate "RPM, nameplate"
la var rpm_gearhead "RPM, at gearhead (almost always missing)"
la var rpm_tachometer "RPM, at tachometer (almost always missing)"
br rpm*
count if rpm_gearhead!=.
count if rpm_tachometer!=.

correlate *ope*
rename ope ope_round
rename ope_numeric ope
rename idealope ope_ideal
la var ope "Operating pump efficiency (%, measured)"
la var ope_round "Operating pump efficiency (%, measured, few sig figs)"
la var ope_ideal "Operating pump efficiency (%, ideal)"
la var ope_after "Operating pump efficiency after project (%, assumed)"
count if ope==. | ope==0

rename afpumped afperyr
rename af_after afperyr_after
la var afperyr "Acre-feet pumped per year"
la var afperyr_after "Acre-feet pumped per year, after project (assumed)"
br af24hrs afperyr saidhours
gen temp = af24hrs/24*saidhours
correlate temp afperyr
gen temp2 = abs(afperyr - temp)
sum temp2, detail // thumbs up emoji
drop temp*
rename saidhours hrs_per_year
la var hrs_per_year "Hours pumping per year (assumed/elicited, pre-project)"

gen kwhperyr = afperyr*kwhaf
gen kwhperyr_after = afperyr_after*kwhaf_after
/*br afperyr afperyr_after kwhaf kwhaf_after kwhperyr kwhperyr_after hrs_per_year ///
	kw_input kw_input_after hp hp_after ope ope_after flow_gpm flow_gpm_after ///
	tdh tdh_after if ope_after-ope>10 & abs(flow_gpm_after-flow_gpm)<1 & ///
	abs(tdh_after-tdh)<1
*/
la var kwhperyr "KWH per year, derived from afperyer*kwhaf"
la var kwhperyr_after "KWH per year after projct, derived from afperyer_after*kwhaf_after"

la var annualcost "Annual cost ($), under current operating conditions/assumptions"
la var annualcost_after "Annual cost ($) after upgrades (assumed)"
gen temp1 = annualcost/kwhperyr
gen temp2 = annualcost_after/kwhperyr_after
gen temp3 = temp1 - avgcost_kwh
gen temp4 = temp2 - avgcost_kwh
br annualcost annualcost_after kwhperyr kwhperyr_after avgcost_kwh temp1 temp3 temp2 temp4
correlate temp1 avgcost_kwh // GREAT!
correlate temp2 avgcost_kwh // TERRIBLE!
rename temp2 avgcost_kwh_after 
la var avgcost_kwh_after "Avg elec cost ($/kWh) after project (derived as annualcost_after/kwhperyr_after)"
drop temp*

la var directreadkw "Direct-read kW input to motor (often missing)"
la var kwik "Disk-read kW input to motor (often missing)"
rename directreadkw kw_input_direct
rename kwik kw_input_disk

replace crop = subinstr(crop,"tomatos","tomatoes",1)
replace crop = "tree" if crop=="trees"
replace crop = subinstr(crop,"potatos","potatoes",1)
replace crop = subinstr(crop,"potato","potatoes",1) if !regexm(crop,"potatoes")
replace crop = subinstr(crop,"/ ","/",1)
replace crop = "pomegranate" if inlist(crop,"pomegranate","pomegranantes","pomegranats", ///
	"pomegranets","pomegranite","pomgranates")
replace crop = "pistachio" if crop=="pistachios"
replace crop = "pears" if crop=="pear"
replace crop = "peaches" if crop=="peach"
replace crop = "onions" if crop=="onion"
replace crop = "other" if inlist(crop,"pth","othr","othert","othere")
replace crop = "oranges" if crop=="orange"
replace crop = subinstr(crop,"strabberries","strawberries",1)
replace crop = "" if crop=="m"
replace crop = subinstr(crop,"luttce","lettuce",1)
replace crop = subinstr(crop,"avacados","avocados",1)
replace crop = "" if crop=="g"
replace crop = "other" if crop=="ither"
replace crop = subinstr(crop,"alflafa","alfalfa",1)
replace crop = subinstr(crop,"aldalfa","alfalfa",1)
replace crop = subinstr(crop,"cellery","celery",1)
replace crop = subinstr(crop,"califlower","cauliflower",1)
replace crop = "carrots" if crop=="carrot"
replace crop = subinstr(crop," and ","/",1)
replace crop = subinstr(crop," & ","/", 1)
replace crop = subinstr(crop,"cherrys","cherries",1)
replace crop = "" if crop=="4"
replace crop = "almonds" if inlist(crop,"alm","alma")
la var crop "Crop type"

replace farmtype = "other" if farmtype=="dother"
replace farmtype = "other" if farmtype=="oher"
replace farmtype = "other" if farmtype=="oother"
replace farmtype = "other" if farmtype=="oter"
replace farmtype = "other" if farmtype=="oth"
replace farmtype = "other" if farmtype=="othero"
replace farmtype = "other" if farmtype=="otherq"
replace farmtype = "other" if farmtype=="othr"
replace farmtype = "other" if farmtype=="othre"
la var farmtype "Type of farm"

replace pumpmke = "" if inlist(pumpmke,"NA","NONE","No Name Plate","Other")
replace pumpmke = upper(trim(itrim(pumpmke)))
la var pumpmke "Pump make (as reported, missings standardized to missing)"

replace mtrmake = "" if inlist(mtrmake,"no name plate","other","none","na")
replace mtrmake = upper(trim(itrim(mtrmake)))
la var mtrmake "Motor make (as reported, missings standardized to missing)"

replace mtrsn = "" if regexm(mtrsn,"[0-9]")==0
la var mtrsn "Motor serial number (as reported, missings standardized to missing)"

la var waterenduse "Water end use"
rename watrsrc watersource
la var watersource "Water source"
la var apeptestid "Unique APEP test identifier (not quite unique by observation)"
la var economicanalysis "Use this run for economic anlaysis?"
la var pumptyp "Pump type"
rename pumptyp pumptype
la var pumplatnew "Pump latitude"
la var pumplongnew "Pump longitude"
la var customertype "Customer type"
rename subsidy subsidy_for_test 
la var subsidy_for_test "Subisdy ($) received for pump test"
la var run "Run number"
la var ofruns "Total number of runs"
rename ofruns nbr_of_runs
rename testsectiondiameter pump_diameter 
la var pump_diameter "Diameter of pump (inches; section of pump tested)"
la var test_date_stata "Date of APEP pump test"

sort TEMP_SORT
drop TEMP_SORT

compress
save "$dirpath_data_pge_cleaned/pump_test_data.dta", replace

