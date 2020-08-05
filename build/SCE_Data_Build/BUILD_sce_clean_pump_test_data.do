************************************************
* Build and clean SCE pump test data.
************************************************
**** SETUP:
clear all
memory clear
set more off, perm
version 12

global dirpath "T:/Projects/Pump Data"
** additional directory paths to make things easier
global dirpath_data "$dirpath/data"
global dirpath_data_sce_raw "$dirpath_data/sce_raw"
global dirpath_data_sce_cleaned "$dirpath_data/sce_cleaned"

************************************************
************************************************

***** PUMP TEST DATA
use "$dirpath_data_sce_raw/pump_test_data_20190916.dta", clear
gen pull = 2019
append using  "$dirpath_data_sce_raw/pump_test_data_20200722.dta"
replace pull = 2020 if pull==.
duplicates drop

	// harmonize variable names (sigh)
replace pump_ref = real(PumpRef) if pump_ref==. & pull==2020	
count if pump_ref==.
drop if pump_ref==.
drop PumpRef

replace customer_name = CustomerName if customer_name=="" & pull==2020
assert customer_name!=""
drop CustomerName

replace zip_code = ZipCode if zip_code=="" & pull==2020
assert zip_code!=""
drop ZipCode

replace pump_name = PumpName if pump_name=="" & pull==2020
count if pump_name==""
drop PumpName

replace pump_serial = PumpSerial if pump_serial=="" & pull==2020
count if pump_serial==""
drop PumpSerial

replace pump_location = PumpLocation if pump_location=="" & pull==2020
count if pump_location==""
drop PumpLocation

replace service_acct = ServiceAcct if service_acct=="" & pull==2020
assert service_acct!=""
drop ServiceAcct

replace sce_meter = SCEMeter if sce_meter=="" & pull==2020
count if sce_meter==""
drop SCEMeter

gen temp = date(TestDate,"MDY")
br test_date TestDate temp
format %td test_date temp
replace test_date = temp if test_date==. & pull==2020
assert test_date!=.
drop TestDate temp

replace pump_mfg = PumpMfg if pump_mfg=="" & pull==2020
count if pump_mfg==""
drop PumpMfg

replace disch_psi = real(DischPSI) if disch_psi==. & pull==2020
assert disch_psi!=.
drop DischPSI

replace standing_level = real(StandingLevel) if standing_level==. & pull==2020
tab pull if standing_level==.
drop StandingLevel

replace drawdown = real(Drawdown) if drawdown==. & pull==2020
tab pull if drawdown==.
drop Drawdown

replace suction_head = real(SuctionHead) if suction_head==. & pull==2020
tab pull if suction_head==.
drop SuctionHead

replace suction_lift = real(SuctionLift) if suction_lift==. & pull==2020
assert suction_lift!=.
drop SuctionLift

replace discharge_head = real(DischargeHead) if discharge_head==. & pull==2020
assert discharge_head!=.
drop DischargeHead

replace pumping_level = real(PumpingLevel) if pumping_level==. & pull==2020
tab pull if pumping_level==.
drop PumpingLevel

replace total_head = real(TotalHead) if total_head==. & pull==2020
assert total_head!=.
drop TotalHead

replace gpm = real(GPM) if gpm==. & pull==2020
assert gpm!=.
drop GPM

replace gpm_foot_drawdown = real(GPMFootDrawdown) if gpm_foot_drawdown==. & pull==2020
tab pull if gpm_foot_drawdown==.
drop GPMFootDrawdown

replace acre_feet_24_hours = real(AcreFeet24Hours) if acre_feet_24_hours==. & pull==2020
assert acre_feet_24_hours!=.
drop AcreFeet24Hours

replace kw_input_to_motor = real(kWInputtoMotor) if kw_input_to_motor==. & pull==2020
assert kw_input_to_motor!=.
drop kWInputtoMotor

replace hp_input_to_motor = real(HPInputtoMotor) if hp_input_to_motor==. & pull==2020
assert hp_input_to_motor!=.
drop HPInputtoMotor

replace motor_load = real(MotorLoad) if motor_load==. & pull==2020
assert motor_load!=.
drop MotorLoad

replace pump_rpm = real(PumpRPM) if pump_rpm==. & pull==2020
tab pull if pump_rpm==.
drop PumpRPM

rename kwh_per_acre_foor kwh_per_acre_foot
replace kwh_per_acre_foot = real(kWhperAcreFoot) if kwh_per_acre_foot==. & pull==2020
assert kwh_per_acre_foot!=.
drop kWhperAcreFoot

replace overall_plant_efficiency = real(OverallPlantEfficiency) if overall_plant_efficiency==. & pull==2020
assert overall_plant_efficiency!=.
drop OverallPlantEfficiency

replace customer_meter_gpm = real(CustomerMeterGPM) if customer_meter_gpm==. & pull==2020
tab pull if customer_meter_gpm==.
drop CustomerMeterGPM

replace overall_plant_efficiency_existin = real(AC) if overall_plant_efficiency_existin==. & pull==2020
assert overall_plant_efficiency_existin!=.
drop AC

replace overall_plant_efficiency_improve = real(AD) if overall_plant_efficiency_improve==. & pull==2020
assert overall_plant_efficiency_improve!=.
drop AD

replace pump_kwh_existing = real(PumpkWh) if pump_kwh_existing==. & pull==2020
assert pump_kwh_existing!=.
drop PumpkWh

replace pump_kwh_improved = real(AF) if pump_kwh_improved==. & pull==2020
assert pump_kwh_improved!=.
drop AF

replace kw_input_existing = real(kWInput) if kw_input_existing==. & pull==2020
assert kw_input_existing!=.
drop kWInput

replace kw_input_improved = real(AH) if kw_input_improved==. & pull==2020
assert kw_input_improved!=.
drop AH

replace kwh_acre_foot_existing = real(kWhAcreFoot) if kwh_acre_foot_existing==. & pull==2020
assert kwh_acre_foot_existing!=.
drop kWhAcreFoot

replace kwh_acre_foot_improved = real(AJ) if kwh_acre_foot_improved==. & pull==2020
assert kwh_acre_foot_improved!=.
drop AJ

replace acre_foot_per_year_1 = real(AcreFootperYear) if acre_foot_per_year_1==. & pull==2020
assert acre_foot_per_year_1!=.
drop AcreFootperYear

replace acre_foot_per_year_2 = real(AL) if acre_foot_per_year_2==. & pull==2020
assert acre_foot_per_year_2!=.
drop AL

replace average_cost_per_acre_foot_exist = real(AverageCostperAcreFoot) if average_cost_per_acre_foot_exist==. & pull==2020
assert average_cost_per_acre_foot_exist!=.
drop AverageCostperAcreFoot

replace average_cost_per_acre_foot_impro = real(AN) if average_cost_per_acre_foot_impro==. & pull==2020
assert average_cost_per_acre_foot_impro!=.
drop AN

replace total_annual_cost_existing = real(TotalAnnualCost) if total_annual_cost_existing==. & pull==2020
assert total_annual_cost_existing!=.
drop TotalAnnualCost

replace total_annual_cost_improved = real(AP) if total_annual_cost_improved==. & pull==2020
assert total_annual_cost_improved!=.
drop AP

foreach var of varlist * {
 label variable `var' ""
}
br

	// drop *exact* duplicates across the 2 pulls
duplicates drop
order pull pump_ref test_date
sort pump_ref test_date pull
duplicates t pump_ref-total_annual_cost_improved, gen(dup)
tab dup
unique pump_ref-total_annual_cost_improved
local uniq = r(unique)
drop if dup==1 & pull==2020
unique pump_ref-total_annual_cost_improved
assert r(unique)==`uniq'
drop dup

	// evaluate remaining dups across/within the 2 pulls
duplicates t pump_ref test_date pull, gen(dup1)
duplicates t pump_ref test_date, gen(dup2)
tab dup1 dup2 // 774 dups 
br if dup1==0 & dup2>0 // a few variables are causing this, and differences may
	// come out in the wash as we clean, and we'll circle back to this
count if dup1>0 
	// only 99 dups within-pulls
drop dup*

	// establish a unique identifier
gen uniq_id = _n
order uniq_id test_date pump_ref
hist test_date
la var pump_ref "Pump reference number (identifies pump)"
la var test_date "Date of pump test"
format %td test_date

la var customer_name "Customer name" // not gonna clean this now
assert customer_name!=""
tab customer_name // many of these are actually individual people

la var zip_code "ZIP code" 
assert zip_code!=""
count if length(zip_code)>5 // many of these are ZIP9s

la var pump_name "Pump name" // NOT completely redundant, since variants will help disambiguate wells vs boosters
unique pump_ref
unique pump_ref pump_name 
replace pump_name = trim(itrim(upper(pump_name)))
replace pump_name = subinstr(pump_name,"# ","#",1)
replace pump_name = subinstr(pump_name,"- ","-",1)
replace pump_name = subinstr(pump_name,"- ","-",1)
replace pump_name = subinstr(pump_name," -","-",1)
replace pump_name = subinstr(pump_name," -","-",1)
replace pump_name = itrim(subinstr(pump_name,"#"," #",1))
replace pump_name = itrim(subinstr(pump_name,".","",1))
order pump_name, after(pump_ref)

rename pump_location pump_address
la var pump_address "Pump street address"

rename service_acct sa_uuid
la var sa_uuid "SCE service account number"
rename sce_meter meter_no
la var meter_no "SCE meter number"
assert sa_uuid!="" 
tab sa_uuid if substr(sa_uuid,1,2)!="SA"
replace sa_uuid = substr(sa_uuid,3,100) if substr(sa_uuid,1,2)=="SA"
assert real(sa_uuid)!=.
unique sa_uuid
duplicates r sa_uuid
count if meter_no=="" // 82 missing meter numbers, which hopefully doesn't matter

la var pump_mfg "Pump manufacturer"
tab pump_mfg, missing
replace pump_mfg = "" if pump_mfg=="0"
replace pump_mfg = "" if pump_mfg=="00000EY"
replace pump_mfg = subinstr(pump_mfg,"&","",.)
replace pump_mfg = subinstr(pump_mfg,"-","",.)
replace pump_mfg = subinstr(pump_mfg,"/","",.)
replace pump_mfg = subinstr(pump_mfg,".","",.)
replace pump_mfg = trim(itrim(upper(pump_mfg)))
tab pump_mfg, missing
replace pump_mfg = "" if pump_mfg=="NA"
replace pump_mfg = "" if pump_mfg=="NO"
replace pump_mfg = "" if pump_mfg=="NO DATA PLATE"
replace pump_mfg = "" if pump_mfg=="NONE"
replace pump_mfg = "" if pump_mfg=="NO INFO"
replace pump_mfg = "" if pump_mfg=="NO NAME PLATE"
replace pump_mfg = "" if pump_mfg=="NO PLATE"
replace pump_mfg = "" if pump_mfg=="UNKNOWN"
replace pump_mfg = "" if pump_mfg=="UNREADABLE"
	// lots of obvious cleaning ot be done here, if we ever want to

la var pump_serial "Pump serial number"	
replace pump_serial = trim(itrim(pump_serial))
tab pump_serial if strpos(upper(pump_serial),"N")
replace pump_serial = "" if pump_serial=="N/A"
replace pump_serial = "" if pump_serial=="NONE"
replace pump_serial = "" if pump_serial=="NO PLATE"
replace pump_serial = "" if pump_serial=="BLANK"
replace pump_serial = "" if pump_serial=="CAN'T READ"
replace pump_serial = "" if pump_serial=="CANT READ"
replace pump_serial = "" if pump_serial=="MISSING PLATE"	
replace pump_serial = "" if pump_serial=="NA"
replace pump_serial = "" if pump_serial=="NO #"
replace pump_serial = "" if pump_serial=="NO DATA PLATE"
replace pump_serial = "" if pump_serial=="NO NANE PLATE"
replace pump_serial = "" if pump_serial=="NO NUMBER"
replace pump_serial = "" if pump_serial=="NO PATE"
replace pump_serial = "" if pump_serial=="NO PLANT"
replace pump_serial = "" if pump_serial=="NO PLATE1"
replace pump_serial = "" if pump_serial=="NO PLATE5"
replace pump_serial = "" if pump_serial=="NO PLATE6"
replace pump_serial = "" if pump_serial=="NO Plate"
replace pump_serial = "" if pump_serial=="NOE"
replace pump_serial = "" if pump_serial=="NO SERIAL"
replace pump_serial = "" if pump_serial=="NOPLATE"
replace pump_serial = "" if pump_serial=="N0 PLATE"
replace pump_serial = "" if pump_serial=="No #"
replace pump_serial = "" if pump_serial=="No PLate"
replace pump_serial = "" if pump_serial=="No Plant"
replace pump_serial = "" if pump_serial=="No Plate"
replace pump_serial = "" if pump_serial=="No Serial"
replace pump_serial = "" if pump_serial=="No plate"
replace pump_serial = "" if pump_serial=="None"
replace pump_serial = "" if pump_serial=="PAIINTED"
replace pump_serial = "" if pump_serial=="PAINTED"
replace pump_serial = "" if pump_serial=="Painted"
replace pump_serial = "" if pump_serial=="UNREADABLE"
replace pump_serial = "" if pump_serial=="n/a"
replace pump_serial = "" if pump_serial=="no plant"
replace pump_serial = "" if pump_serial=="no plate"
replace pump_serial = "" if pump_serial=="none"
replace pump_serial = "" if pump_serial=="painted"
replace pump_serial = "" if pump_serial=="worn off"
replace pump_serial = "" if pump_serial=="WORN OFF"
replace pump_serial = "" if pump_serial=="covered"
replace pump_serial = "" if pump_serial=="COVERED"
replace pump_serial = "" if pump_serial=="PLATE"
sort pump_serial
sort uniq_id

rename standing_level swl
la var swl "Standing water level (ft), when the pump is not running"
assert swl>=0
hist swl
sum swl, detail // zeros seem obviously wrong to me, so i'm replaing with missings
sum swl if swl!=0, detail
count if swl==. // super high missing rate
count if swl==0 // which gets higher when we add in the zeros
order swl, after(pump_serial)

rename pumping_level pwl
la var pwl "Pumping water level (ft), stabilized under constant pumping conditions"
hist pwl
assert pwl>=0
sum pwl, detail
count if pwl==. // same number of missings as swl
assert pwl==. if swl==.
assert swl==. if pwl==.
twoway scatter pwl swl, msize(tiny)
assert pwl>=swl
twoway (kdensity pwl if swl!=0) (kdensity pwl if swl==0) // swl zeros much more likely for small pwl
order pwl, after(swl)

rename drawdown drwdwn
assert drwdwn==. if swl==.
assert drwdwn>=0
sum drwdwn if swl!=0, detail
assert drwdwn==0 if swl==0
gen temp = drwdwn - (pwl-swl) if swl!=0 & swl!=.
sum temp, detail // turns out these are pretty darn good
count if abs(temp)>0.001 & swl!=0 & swl!=. // on 6 bad ones
la var drwdwn "Drawdown (ft), or the difference between SWL and PWL"
gen flag_bad_drwdwn = abs(temp)>0.001
la var flag_bad_drwdwn "Flag for drawdown inconsistent with SWL/PWL (or missing)"
drop temp
order drwdwn, after(pwl)
replace swl = . if swl==0

rename disch_psi dchpres_psi
la var dchpres "Discharge pressure at gauge (psi; 1 psi = 2.31 feet of water head)"
hist dchpres_psi
sum dchpres_psi, detail
sum dchpres_psi if dchpres<0, detail
gen temp = discharge_head/dchpres_psi
sum temp, detail
sum temp if dchpres_psi>0, detail
sum temp if dchpres_psi<0, detail
rename discharge_head dchlvl_ft
la var dchlvl_ft "Discharge level at gauge (feet; 1 psi = 2.31 feet of water head)"
drop temp
order dchpres_ps dchlvl_ft, after(drwdwn)
// COME BACK AND FIX NEGATIVE VALUES?
twoway  ///
	(kdensity dchlvl_ft if pwl!=. & dchlvl_ft!=0 & dchlvl_ft<1000) ///
	(kdensity dchlvl_ft if pwl==. & dchlvl_ft!=0 & dchlvl_ft<1000)
gen temp1 = dchlvl_ft+pwl	
twoway  ///
	(kdensity temp1 if pwl!=. & temp1!=0 & dchlvl_ft<1000) ///
	(kdensity dchlvl_ft if pwl==. & dchlvl_ft!=0 & dchlvl_ft<1000)
drop temp1
	// It really looks like there are two types of pumps here
	
// CONFIRMED (see p.18) http://www.lmphotonics.com/gepman/Edison%20Pumping%20Guide%2012-27.pdf
// Pumps where pwl is populated are vertical well pumps
// Pumps where pwl is missing are booster pumps
gen booster_pump = pwl==.
la var booster_pump "Flag for booster pumps (which we probably want to drop)"
egen temp_min = min(booster_pump), by(pump_ref)
egen temp_max = max(booster_pump), by(pump_ref)
tab temp_min temp_max
sort pump_ref
br if temp_min<temp_max
	// will deal with these below, it looks like it should be reasonably straightforward

	// total lift for vertical wells
rename total_head totlift
la var totlift "Total lift (pwl+dchlvl_ft for vertical wells)"	
assert totlift>=0
assert totlift!=.
gen temp = pwl + dchlvl_ft
gen temp2 = abs(totlift - temp)
gen flag_bad_totlift = 0
replace flag_bad_totlift = 1 if temp2>1 & temp2!=. & booster_pump==0
la var flag_bad_totlift "Flag for total lift not equal to sum of components"
drop temp temp2

	// total lift for booster pumps
sum suction_head, detail
sum suction_lift, detail
assert suction_head==. if pwl!=.
assert suction_head!=. if pwl==.
assert pwl==. if suction_head!=.
assert pwl!=. if suction_head==.
gen temp1 = dchlvl_ft - suction_head 
gen temp2 = dchlvl_ft + suction_lift
gen temp_booster_ok = booster==1 & abs(totlift - temp1)<1
replace temp_booster_ok = 1 if abs(totlift - temp2)<1 & booster==1
assert temp_booster_ok==1 | totlift==0 if booster_pump==1
replace flag_bad_totlift = 1 if temp_booster_ok==0 & totlift==0 & booster_pump==1
drop temp? temp_booster_ok

	// disambiguate pumps with both vertical and booster tests
egen temp_min2 = min(flag_bad_totlift), by(pump_ref booster_pump)
egen temp_max2 = max(flag_bad_totlift), by(pump_ref booster_pump)	
br uniq_id test_date pump_ref pump_name swl-totlift booster_pump temp* if temp_min<temp_max

	// group 1: all vertical tests are good, all booster tests are bad
egen temp1 = max(temp_max2) if booster_pump==0, by(pump_ref)
egen temp2 = min(temp_min2) if booster_pump==1, by(pump_ref)	
egen temp3 = mean(temp1), by(pump_ref)
egen temp4 = mean(temp2), by(pump_ref)	
br uniq_id test_date pump_ref pump_name swl-totlift booster_pump temp* if temp_min<temp_max & temp3==0 & temp4==1
gen flag_test_to_drop = 0
replace flag_test_to_drop = 1 if temp_min<temp_max & temp3==0 & temp4==1 & booster_pump==1
replace booster_pump = . if flag_test_to_drop==1
drop temp?

	// group 2: all vertical tests are bad, all booster tests are good
egen temp1 = max(flag_test_to_drop), by(pump_ref)
br uniq_id test_date pump_ref swl-totlift booster_pump temp* if temp_min<temp_max & temp1==0
egen temp2 = min(drwdwn==0 & pwl<20 & swl==.) if booster_pump==0, by(pump_ref)	
egen temp3 = max(flag_bad_totlift) if booster_pump==1, by(pump_ref)	
egen temp4 = mean(temp2), by(pump_ref)
egen temp5 = mean(temp3), by(pump_ref)	
br uniq_id test_date pump_ref pump_name swl-totlift booster_pump temp* if temp_min<temp_max & temp1==0 & temp4==1 & temp5==0
		// these shouldn't get dropped, just reclassified
gen booster_pump_reclass = temp_min<temp_max & temp1==0 & temp4==1 & temp5==0 & booster_pump==0
replace booster_pump = 1 if temp_min<temp_max & temp1==0 & temp4==1 & temp5==0	
drop temp*
	// assess where we're at
egen temp_min = min(booster_pump), by(pump_ref)
egen temp_max = max(booster_pump), by(pump_ref)
tab temp_min temp_max
br uniq_id test_date pump_ref pump_name swl-totlift booster_pump flag_bad_totlift temp* if temp_min<temp_max
	
	// group 3: all vertical tests are good, all booster tests are zeros
egen temp1 = min(totlift==0) if booster_pump==1, by(pump_ref)
egen temp2 = max(flag_bad_totlift) if booster_pump==0, by(pump_ref)	
egen temp3 = mean(temp1), by(pump_ref)
egen temp4 = mean(temp2), by(pump_ref)	
br uniq_id test_date pump_ref pump_name swl-totlift booster_pump flag_bad_totlift temp* if temp_min<temp_max & temp3==1 & temp4==0
replace flag_test_to_drop = 1 if temp_min<temp_max & temp3==1 & temp4==0 & totlift==0
replace booster_pump = . if flag_test_to_drop==1
drop temp*
	// assess where we're at
egen temp_min = min(booster_pump), by(pump_ref)
egen temp_max = max(booster_pump), by(pump_ref)
tab temp_min temp_max
br uniq_id test_date pump_ref pump_name swl-totlift booster_pump flag_bad_totlift temp* if temp_min<temp_max
	
	// group 4: most vertical tests are good, all booster tests are zeros
egen temp1 = min(totlift==0) if booster_pump==1, by(pump_ref)
egen temp2 = mean(flag_bad_totlift) if booster_pump==0, by(pump_ref)	
egen temp3 = mean(temp1), by(pump_ref)
egen temp4 = mean(temp2), by(pump_ref)	
br uniq_id test_date pump_ref pump_name swl-totlift booster_pump flag_bad_totlift temp* if temp_min<temp_max & temp3==1 & temp4<0.5
replace flag_test_to_drop = 1 if temp_min<temp_max & temp3==1 & temp4<0.5 & totlift==0
replace booster_pump = . if flag_test_to_drop==1
drop temp*
	// assess where we're at
egen temp_min = min(booster_pump), by(pump_ref)
egen temp_max = max(booster_pump), by(pump_ref)
tab temp_min temp_max
br uniq_id test_date pump_ref pump_name swl-totlift booster_pump flag_bad_totlift temp* if temp_min<temp_max

	// group 5: all vertical tests have zero drawdown, all booster tests are good, and "WELL" isn't in the name
egen temp1 = max(flag_test_to_drop), by(pump_ref)
br uniq_id test_date pump_ref swl-totlift booster_pump temp* if temp_min<temp_max & temp1==0
egen temp2 = min(drwdwn==0 & swl==.) if booster_pump==0, by(pump_ref)	
egen temp3 = max(flag_bad_totlift) if booster_pump==1, by(pump_ref)	
egen temp4 = mean(temp2), by(pump_ref)
egen temp5 = mean(temp3), by(pump_ref)	
egen temp6 = max(regexm(pump_name,"WELL")), by(pump_ref)
br uniq_id test_date pump_ref pump_name swl-totlift booster_pump temp* if temp_min<temp_max & temp1==0 & temp4==1 & temp5==0
		// these shouldn't get dropped, just reclassified
replace booster_pump_reclass = 1 if temp_min<temp_max & temp1==0 & temp4==1 & temp5==0 & temp6==0 & booster_pump==0
replace booster_pump = 1 if temp_min<temp_max & temp1==0 & temp4==1 & temp5==0 & temp6==0	
drop temp*
	// assess where we're at
egen temp_min = min(booster_pump), by(pump_ref)
egen temp_max = max(booster_pump), by(pump_ref)
tab temp_min temp_max
br uniq_id test_date pump_ref pump_name swl-totlift booster_pump flag_bad_totlift temp* if temp_min<temp_max

	// group 6: split into multiple identifiers, since all but one are unambiguous based on pump name
egen pump_refNEW = group(pump_ref booster_pump) // reindex pump ID to split well/booster combos on the same ID
egen temp_sd = sd(pump_refNEW), by(pump_ref)
unique pump_ref if temp_sd>0 & temp_sd!=.
unique pump_ref if temp_min<temp_max
drop temp*
		
	// gut check: compare pump names to booster assignment
gen tempB = max(regexm(pump_name,"BOOSTER"),regexm(pump_name,"BST"),regexm(pump_name,"BSTR")) //, substr(word(pump_name,wordcount(pump_name)),1,1)=="B")
gen tempW = max(regexm(pump_name,"WELL"),regexm(pump_name,"WL")) //, substr(word(pump_name,wordcount(pump_name)),1,1)=="W")	
tab tempB tempW
tab pump_name if tempB==1 & tempW==1
gen temp1 = suction_head==.
tab pump_name temp1 if tempB==1 & tempW==1 & flag_test_to_drop!=1
	// pretty solid
drop temp*

	// see if dropping bad tests would make us lose any SAs in doing so
assert totlift==0 & overall_plant_efficiency==0 if flag_test_to_drop==1	
egen temp_min = min(flag_test_to_drop), by(sa_uuid)
tab temp_min
tab temp_min if flag_test_to_drop==1 
	// the answer is yes, so i will replace everything with missings instead of dropping observations
drop temp*

	// populate missing pump_refNEWs 
egen temp1 = min(pump_refNEW), by(pump_ref)	
egen temp2 = max(pump_refNEW), by(pump_ref)	
replace pump_refNEW = temp1 if temp1==temp2 & pump_refNEW==.
assert pump_refNEW!=.
replace pump_ref = pump_refNEW
egen temp_min = min(booster_pump), by(pump_ref)
egen temp_max = max(booster_pump), by(pump_ref)
assert temp_min==temp_max
drop temp* pump_refNEW

	// label suction variables for booster pumps
la var suction_lift "Distance (ft) btw pump discharge head and water lvel (booster pumps!)"
la var suction_head "Feet above center line of pump suction intake (booster pumps!)"

	// resume cleaning: flow variables
rename gpm flow_gpm
la var flow_gpm "Pump flow rate, gallons per minute (measured)"
hist flow_gpm	
assert flow_gpm!=.
assert flow_gpm>=0
count if flow_gpm==0 & flag_test_to_drop!=1
br if flow_gpm==0 // almost all are boosters
tab booster_pump flag_bad_drwdwn if flow_gpm==0 // these tests are expendible, most are boosters anyway
replace flag_test_to_drop = 1 if flow_gpm==0

rename gpm_foot_drawdown flow_gpm_drwdwn_ft
la var flow_gpm_drwdwn_ft "Flow (gpm) / drawdown (ft), a measure of well (not pump) performance"
gen temp = flow_gpm/drwdwn
br drwdwn flow_gpm flow_gpm_drwdwn_ft temp if booster_pump==0 & flag_test_to_drop!=1 & flag_bad_drwdwn!=1
br drwdwn flow_gpm flow_gpm_drwdwn_ft temp if booster_pump==0 & flag_test_to_drop!=1 & flag_bad_drwdwn!=1 ///
	& abs(flow_gpm_drwdwn_ft - temp)>=0.001
br swl pwl drwdwn flow_gpm flow_gpm_drwdwn_ft temp booster_pump flag_test_to_drop flag_bad_drwdwn ///
	if !(abs(flow_gpm_drwdwn_ft - temp)<0.001 | drwdwn==0 | drwdwn==.)
gen temp2 = flow_gpm/flow_gpm_drwdwn_ft
gen temp3 = pwl-swl
	// cannot fix these bad drawdowns
drop temp*
	
rename acre_feet_24_hours af24hrs
la var af24hrs "Acre-feet per 24 hours, at the measured flow rate"
assert af24hrs!=.
assert af24hrs>=0
gen temp = flow_gpm*60*24/325900 - af24hrs
sum temp, detail 
count if !inrange(temp,-0.25,0.25) // only 9 bad ones
gen temp2 = flow_gpm*60*24/325900
br flow_gpm af24hrs temp* flag* booster* if !inrange(temp,-0.25,0.25) 
	// all are either already flagged as bad totlift or booster pumps
	// af24hrs are the ones that need updating, clustered on 3 discrete values
replace af24hrs = temp2 if !inrange(temp,-0.25,0.25) 
assert af24hrs!=0 if flow_gpm!=0
drop temp*

rename customer_meter_gpm flow_cust_gpm
la var flow_cust_gpm "Pump flow rate, gallons per minute (per customer's flow meter)"
assert flow_cust_gpm!=.
sum flow_cust_gpm, detail
sum flow_cust_gpm if flow_cust_gpm!=0, detail // zeros are almost certainly missings
correlate flow_cust_gpm flow_gpm if flow_cust_gpm!=0
replace flow_cust_gpm = . if flow_cust_gpm==0
order flow_cust_gpm, after(flow_gpm_drwdwn_ft)

	// power variables
rename hp_input_to_motor hp
la var hp "Horsepower input to motor (measured)"
hist hp
assert hp>=0
assert hp!=.
br if hp==0 & flag_test_to_drop!=1
replace hp = . if hp==0

rename kw_input_to_motor kw_input
la var kw_input "Kilowatt input to motor (1 HP = 0.746 kW)"
hist kw_input
assert kw_input>=0
assert kw_input!=.
replace kw_input = . if kw_input==0

gen temp = kw_input/hp
sum temp, detail
sort temp
br test_date booster_pump flag* totlift flow_gpm hp kw_input kw_input_existing temp ///
	if round(temp,0.01)!=0.75
replace hp = kw_input/r(p50) if hp==0 & kw_input!=0
gen flag_hp_kw_to_fix =  round(temp,0.01)!=0.75
assert round(temp,0.01)==0.75 | temp==. if kw_input!=kw_input_existing
count if kw_input_existing!=kw_input
br if kw_input_existing!=kw_input
tab kw_input_existing kw_input if kw_input_existing!=kw_input
gen temp1 = kw_input_existing/hp
gen temp2 = kw_input/hp
tab temp1 if kw_input_existing!=kw_input
tab temp2 if kw_input_existing!=kw_input // kw_input_existing makes some key corrections
replace kw_input = kw_input_existing 
replace kw_input = . if kw_input==0
replace flag_hp_kw_to_fix = 0 if round(temp,0.01)==0.75
drop temp* kw_input_existing


rename motor_load mtrload
la var mtrload "Motor load (%), or HP input * motor efficiency / nameplate HP"
sum mtrload, detail
assert mtrload!=.
assert mtrload>=0

sum pump_rpm, detail
sum pump_rpm if booster_pump==0, detail
assert pump_rpm!=.
replace pump_rpm = . if pump_rpm<=0
rename pump_rpm rpm_tachometer
la var rpm_tachometer "RPM, at tachometer (often missing)"
hist test_date if rpm_tachometer!=.
hist test_date if rpm_tachometer==.

rename kwh_per_acre_foot kwhaf
la var kwhaf "Kilowatt-hours per acre-foot, given test conditions"
assert kwhaf==kwh_acre_foot_existing
drop kwh_acre_foot_existing
gen temp = kwhaf * af24hrs/24
br kwhaf af24hrs kw_input temp booster* flag*
correlate kw_input temp // perfectly correlated
reg temp kw_input, nocons // dead on, except rounding!
gen temp2 = kw_input - temp
sort temp2
replace kwhaf = . if kwhaf==0 
br kwhaf af24hrs kw_input temp booster* flag* temp* if abs(temp2)>1
gen flag_kwhaf_to_fix = abs(temp2)>1
drop temp*

	// finally made it to OPE
rename overall_plant_efficiency* ope*	
la var ope "Operating pump efficiency (%, measured)"
assert ope==ope_existin
drop ope_existin
hist ope
hist ope if flag_test_to_drop!=1
sum ope if flag_test_to_drop!=1, detail
br if ope==0 & flag_test_to_drop!=1
sum ope if flag_test_to_drop!=1 & booster_pump==0, detail
sum ope if flag_test_to_drop!=1 & booster_pump==1, detail
tab booster_pump if flag_test_to_drop!=1 & ope==0 // all but 4 are booster pumps
br if ope==0 & totlift!=0 // 18 observations
gen flag_ope_to_fix = ope==0 & flag_test_to_drop!=1
tab flag_ope_to_fix
replace ope = . if ope==0 & (flag_ope_to_fix==1 | flag_test_to_drop==1)
replace totlift = . if totlift==0 & ope==. & (flag_ope_to_fix==1 | flag_test_to_drop==1)
replace flow_gpm = . if flow_gpm==0 & (kwhaf==. | ope==.)
replace flow_gpm_drwdwn_ft = . if flow_gpm_drwdwn_ft==0 & flow_gpm==.
replace af24hrs = . if af24hrs==0 & flow_gpm==.

	// now, it's time to apply some physics
gen tdh = totlift
la var tdh "Total dynamic head (ft), equalivalent to total lift"
order tdh, after(totlift)
gen temp = flow_gpm*tdh/(hp*ope)
sum temp, detail
	// HORSEPOWER FORMULA IS HP = (   FLOW    * TOTLIFT) / (39.60 * OPE)
    //                              gall/min     feet                % 
	// so, the OPE*HP should equal:
	// FLOW (gall/min) * TOTLIFT (ft) * (8.34 lb/ft) * (1 min/60 sec) * (1/550)
gen temp2 = flow_gpm*totlift*8.34/60/550	
gen temp3 = hp*ope/100
correlate temp2 temp3
di (8.34/60/550)^(-1)/100 // BOOM! physics!
drop temp*

	// revisit the bad hp tests i've flagged
assert inlist(flag_hp_kw_to_fix,0,1)
br pwl totlift flow_gpm kw_input hp ope booster_pump if flag_hp_kw_to_fix==1
gen temp = kw_input/hp
sum temp, detail
	// break ties using ope, flow, and lift
gen temp1 = flow_gpm*totlift/(39.60*ope)
gen temp2 = temp1*r(p50)
gen temp3 = abs(hp-temp1)
gen temp4 = abs(kw_input-temp2)
sum temp3 temp4 if flag_hp_kw_to_fix==0, detail
sum temp3 temp4 if flag_hp_kw_to_fix==1, detail
	// clearly  kw_input dominates on internal consistency
sum temp, detail
replace hp = kw_input/r(p50) if flag_hp_kw_to_fix==1
replace flag_hp_kw_to_fix = 0 if abs(kw_input/hp - r(p50))<0.00001
drop temp*

gen water_hp = flow_gpm*tdh/3960
la var water_hp "Water horsepower output of motor (measured)"
gen temp = 100*water_hp/hp 
br water_hp hp temp ope 
count if abs(temp-ope)>1 // only 76 of 21851
	// OPE is defined as water HP / HP, or HP out / HP in
drop temp*	
order water_hp, after(ope)

rename pump_kwh_existing kwhperyr
rename average_cost_per_acre_foot_exist avgcost_af
rename acre_foot_per_year_1 afperyr
correlate afperyr acre_foot_per_year_2 //????
rename total_annual_cost_existing annualcost

	// validate kwhperyr and afperyr
gen temp1 = kwhaf*afperyr
gen temp2 = temp1 - kwhperyr
sum temp2, detail
sum temp2 if booster_pump==0 & temp1!=0, detail // pretty bang on
gen temp3 = kwhperyr/kw_input 
sum temp3, detail // plausible for numbers of hours per year (not all 8760)
count if temp3>8760
la var kwhperyr "KWH per year, assumed, consistent with afperyr*kwhaf"
la var afperyr "Acre-feet pumped per year"
order kwhperyr afperyr, after(water_hp)

	// validate kwhaf 
gen temp4 = (kw_input*325851)/(flow_gpm*60)
correlate kwhaf temp4
br if abs(kwhaf-temp4)>=0.001 // all previously flagged tests, so we're good here!

	// create hours per year
gen temp6 = afperyr/af24hrs*24
sum temp6, detail
twoway scatter temp3 temp6
correlate temp3 temp6 // YES hours calculated via flow or via kw are basically the same 
reg temp6 temp3, nocons
br temp3 temp6 kwhperyr kw_input afperyr af24hrs
count if temp3>8760 & temp3!=.
count if temp6>8760 & temp6!=.
	// gonna use the afperyr one, for consistency the PGE APEP build
rename temp6 hrs_per_year
la var hrs_per_year "Hours pumping per year (assumed, derived from afperyr/af24hrs*24)"
order hrs_per_year, after(afperyr)	
	
la var avgcost_af "Average cost per acre-foot ($/af)"
sum avgcost_af, detail	
sum avgcost_af if booster_pump==0 & flag_test_to_drop==0, detail	
br if avgcost_af<=0
replace avgcost_af = . if avgcost_af<=0
br if avgcost_af>10000 & avgcost_af!=.
replace avgcost_af = . if avgcost_af>10000
order avgcost_af, after(hrs_per_year)
	
	// create average cost per kwh
gen temp6 = avgcost_af/kwhaf
sum temp6, detail
br if temp6<0 | temp6>100
rename temp6 avgcost_kwh
la var avgcost_kwh "Avg elec cost ($/kWh), derived from avgcost_af/kwhaf"
order avgcost_kwh, after(avgcost_af)

la var annualcost "Annual cost ($), under current opetating conditions/assumptions"
sum annualcost, detail
replace annualcost = . if annualcost<=0
sum annualcost if avgcost_af==., detail
order annualcost, after(avgcost_kwh)
gen temp6 = annualcost/avgcost_af
correlate temp6 afperyr
reg temp6 afperyr
gen temp7 = afperyr-temp6
correlate temp7 acre_foot_per_year_2
sum acre_foot_per_year_2 if booster_pump, detail
la var acre_foot_per_year_2 "No idea what this variable means..."
drop temp*

	// consolidate bad tests
br if flag_bad_totlift | flag_hp_kw_to_fix | flag_kwhaf_to_fix | flag_ope_to_fix | flag_test_to_drop	
br if (flag_bad_totlift | flag_hp_kw_to_fix | flag_kwhaf_to_fix | flag_ope_to_fix | flag_test_to_drop) ///
	& booster_pump==0
gen flag_bad_test = 0
replace flag_bad_test = 1 if flag_test_to_drop==1
replace flag_bad_test = 1 if flag_ope_to_fix==1
replace flag_bad_test = 1 if flag_kwhaf_to_fix==1
replace flag_bad_test = 1 if flag_hp_kw_to_fix==1
replace flag_bad_test = 1 if flag_bad_totlift==1
tab booster_pump flag_bad_test , missing
tab booster_pump flag_bad_test if ope!=., missing // missing OPE essentially grabs all bad tests
*drop flag_bad_totlift flag_hp_kw_to_fix flag_kwhaf_to_fix flag_ope_to_fix flag_test_to_drop
la var flag_bad_test "Flag for tests where essential components are missing or internally inconsistent"
sum kwhaf if flag_bad_test==0, detail
replace flag_bad_test = 1 if kwhaf>30000 // absurdly large outliers
tab booster_pump*, missing
drop booster_pump_reclass

	// improvement variables
rename ope_improve ope_after
la var ope_after "Operating pump efficiency after project (%, assumed)"
replace ope_after = . if ope_after==0	
twoway scatter ope_after ope, msize(tiny)	
gen temp = ope_after - ope
sum temp, detail
drop temp

rename kwh_acre_foot_improved kwhaf_after
la var kwhaf_after "Kilowatt-hours per acre-foot after proejct (assumed)"
replace kwhaf_after = . if kwhaf_after==0
twoway scatter kwhaf_after kwhaf if flag_bad_test==0 & booster_pump==0, msize(tiny)	

rename kw_input_improved kw_input_after
la var kw_input_after "Kilowatt input to motor after project (assumed)"
replace kw_input_after = . if kw_input_after==0
twoway scatter kw_input_after kw_input, msize(tiny)

gen hp_after = kw_input_after/0.7457
la var hp_after "Horsepower input to motor after project (assumed, derived from kw_input_after)"
twoway scatter hp_after hp if flag_bad_test==0, msize(tiny)	
	
gen water_hp_after = hp_after*ope_after
la var water_hp_after "Water horsepower output after project (assumed, from hp_afer ope_after)"
twoway scatter water_hp_after water_hp if flag_bad_test==0, msize(tiny)	

rename pump_kwh_improved kwhperyr_after
la var kwhperyr_after "KWH per year after project (assumed)"
twoway scatter kwhperyr_after kwhperyr, msize(tiny)
replace kwhperyr_after = . if kwhperyr_after==0 & kwhperyr!=0	
	
rename average_cost_per_acre_foot_impro avgcost_af_after
la var avgcost_af_after "Average cost per acre-foot after project (assumed, $/af)"	
replace avgcost_af_after = . if avgcost_af_after==0
sum avgcost_af_after, detail
gen temp = avgcost_af_after/avgcost_af
sum temp, detail
replace avgcost_af_after = . if temp>3
twoway scatter avgcost_af_after avgcost_af, msize(tiny)
drop temp

gen avgcost_kwh_after = avgcost_af_after/kwhaf_after
la var avgcost_kwh_after "Avg elec cost ($/kWh) after project (assumed: avgcost_af_after/kwhaf_after)"
twoway scatter avgcost_kwh_after avgcost_kwh, msize(tiny)
gen temp = avgcost_kwh_after - avgcost_kwh
sum temp, detail
drop temp

rename total_annual_cost_improved annualcost_after
la var annualcost_after "Annual cost ($) after upgrades (assumed)"
replace annualcost_after = . if annualcost_after==0
twoway scatter annualcost_after annualcost, msize(tiny)

gen afperyr_after = annualcost_after/avgcost_af_after
la var afperyr_after "Acre-feet pumped per year, after project (annualcost_after/avgcost_af_after)"
twoway scatter afperyr_after afperyr, msize(tiny)
gen temp = afperyr_after - afperyr
sum temp, detail
sort temp
count if abs(temp)>1 & temp!=. // ehh
drop temp

	// check to see if FLOW*TOTLIFT is assumed to be the same
*HORSEPOWER FORMULA IS HP = (   FLOW    * TOTLIFT) / (39.60 * OPE)
gen temp1 = flow_gpm * tdh
gen temp2 = hp*39.568345*ope
gen temp3 = hp_after*39.568345*ope_after
gen temp4 = temp1 - temp2
sum temp4 if flag_bad_test==0, detail
br if temp4>10 & flag_bad_test==0 & temp4!=. // all booster pumps, but still, want to flag as bad
replace flag_bad_test = 1 if temp4>10 & temp4!=.
sum temp4 if flag_bad_test==0, detail // bang on except rounding
gen temp5 = temp3 - temp1
sum temp5 if flag_bad_test==0, detail 
twoway scatter temp3 temp1 if flag_bad_test==0, msize(tiny)
reg temp3 temp1 if flag_bad_test==0, nocons // well that clearly works pretty well
gen temp6 = temp3/temp1
sum temp6 if flag_bad_test==0, detail // bang on! 
drop temp*

gen flow_gpm_after = flow_gpm 
replace flow_gpm_after = . if ope_after==. | hp_after==.
la var flow_gpm_after "Pump flow rate after project, gallons per minute (assumed to be unchaged)"

gen tdh_after = tdh
replace tdh_after = . if flow_gpm_after==.
la var tdh_after "Total dynamic head (ft) after project (assumed to be unchanged"

gen af24hrs_after = flow_gpm_after*60*24/325900
la var af24hrs_after "Acre-feet per 24 hours, after project (derived from flow_gpm_after)"

order tdh_after flow_gpm_after af24hrs_after kw_input_after hp_after kwhaf_after ope_after ///
	water_hp_after kwhperyr_after afperyr_after avgcost_af_after avgcost_kwh_after ///
	annualcost_after, after(annualcost)

	// standardize pump names
preserve 
keep pump_ref pump_name
duplicates drop
duplicates t pump_ref, gen(dup)
list if dup>0 // these are all variant spellings, and I want to harrmonize them
replace pump_name = trim(itrim(upper(pump_name)))
replace pump_name = subinstr(pump_name,"# ","#",1)
replace pump_name = subinstr(pump_name,"- ","-",1)
replace pump_name = subinstr(pump_name,"- ","-",1)
replace pump_name = subinstr(pump_name," -","-",1)
replace pump_name = subinstr(pump_name," -","-",1)
replace pump_name = itrim(subinstr(pump_name,"#"," #",1))
duplicates drop
drop dup
duplicates t pump_ref, gen(dup)
tab dup
gen temp = length(pump_name)
egen temp2 = max(temp), by(pump_ref)
egen temp3 = mode(pump_name) if temp==temp2, by(pump_ref) minmode
egen temp4 = mode(temp3), by(pump_ref)
replace pump_name = temp4 if temp4!=""
drop temp* dup
duplicates drop
unique pump_ref
assert r(unique)==r(N)
tempfile pumpnames
save `pumpnames'
restore
drop pump_name
merge m:1 pump_ref using `pumpnames'
assert _merge==3
drop _merge

	// circle back to resolve dups across pulls (exact dups)
duplicates t test_date pump_ref customer_name-pump_name, gen(dup)
tab dup	
unique test_date pump_ref customer_name-pump_name
local uniq = r(unique)
drop if dup==1 & pull==2020
duplicates drop test_date pump_ref customer_name-pump_name, force
unique test_date pump_ref customer_name-pump_name
assert r(unique)==`uniq'
drop dup

	// evaluate remaining dups across/within the 2 pulls
duplicates t pump_ref test_date pull, gen(dup1)
duplicates t pump_ref test_date, gen(dup2)
sort test_date pump_ref pull
tab dup1 dup2 // 774 dups 
br if dup1==0 & dup2>0 
gen mismatch = ""
gen disp = ""
foreach v of varlist customer_name-annualcost {
	replace mismatch = mismatch + " " + "`v'" if dup1==0 & dup2==1 & ///
		`v'!=`v'[_n+1] & pump_ref==pump_ref[_n+1] & test_date==test_date[_n+1]
	cap replace disp = disp + " " + "`v'" if dup1==0 & dup2==1 & ///
		round(`v',0.00001)!=round(`v'[_n+1],0.00001) & pump_ref==pump_ref[_n+1] & test_date==test_date[_n+1]		
}
tab mismatch
tab disp
assert disp=="" if mismatch==" suction_lift"
	// all suction_lift mismatches are induced by Stata rounding issues
tab disp if mismatch==" hp"	 // same for all but 13 of the hp mismatches
unique test_date pump_ref
local uniq = r(unique)
drop if dup1==0 & dup2==1 & pull==2019 & inlist(mismatch," suction_lift"," hp") & disp==""
unique test_date pump_ref
assert r(unique)==`uniq'
drop dup* mismatch disp

	// evaluate remaining dups across/within the 2 pulls (iterate above step again)
duplicates t pump_ref test_date pull, gen(dup1)
duplicates t pump_ref test_date, gen(dup2)
sort test_date pump_ref pull
tab dup1 dup2 
br if dup1==0 & dup2>0 
gen mismatch = ""
gen disp = ""
foreach v of varlist customer_name-annualcost {
	replace mismatch = mismatch + " " + "`v'" if dup1==0 & dup2==1 & ///
		`v'!=`v'[_n+1] & pump_ref==pump_ref[_n+1] & test_date==test_date[_n+1]
	cap replace disp = disp + " " + "`v'" if dup1==0 & dup2==1 & ///
		round(`v',0.00001)!=round(`v'[_n+1],0.00001) & pump_ref==pump_ref[_n+1] & test_date==test_date[_n+1]		
}
tab mismatch
tab disp
br test_date pump_ref pull mismatch disp drwdwn dchlvl_ft suction_head totlift tdh ///
	flow_gpm hp mtrload flag* if dup1==0 & dup2>0 
	// 2020 pull has fewer bad flags, consistently, so we'll keep those
unique test_date pump_ref
local uniq = r(unique)
drop if dup1==0 & dup2==1 & pull==2019 & mismatch!=""
unique test_date pump_ref
assert r(unique)==`uniq'
drop dup* mismatch disp

	// resolve double dups
duplicates t pump_ref test_date pull, gen(dup1)
duplicates t pump_ref test_date, gen(dup2)
sort test_date pump_ref pull
tab dup1 dup2
egen temp_dup = min(dup1), by(pump_ref test_date)
br if temp_dup==0 & dup2>0 
egen temp1 = max(hp*(pull==2020)), by(pump_ref test_date)
gen temp2 = abs(hp-temp1) if pull==2019
egen temp3 = max(kw_input*(pull==2020)), by(pump_ref test_date)
gen temp4 = abs(kw_input-temp3) if pull==2019
egen temp5 = max(drwdwn*(pull==2020)), by(pump_ref test_date)
gen temp6 = abs(drwdwn-temp5) if pull==2019
egen temp7 = rowtotal(temp2 temp4 temp6)
egen temp8 = min(temp7) if pull==2019, by(pump_ref test_date)
unique test_date pump_ref
local uniq = r(unique)
drop if temp_dup==0 & dup2>0 & pull==2019 & temp7>temp8 & temp7>1
unique test_date pump_ref
assert r(unique)==`uniq'
drop dup* temp*

	// resolve last dups (across pulls)
duplicates t pump_ref test_date pull, gen(dup1)
duplicates t pump_ref test_date, gen(dup2)
sort test_date pump_ref pull
tab dup1 dup2
egen temp_dup = min(dup1), by(pump_ref test_date)
br if temp_dup==0 & dup2>0  // kepe 2020 values
unique test_date pump_ref
local uniq = r(unique)
drop if temp_dup==0 & dup2>0 & pull==2019 
unique test_date pump_ref
assert r(unique)==`uniq'
drop dup* temp*
drop pull

	// save
rename test_date test_date_stata
la var test_date_stata "Date of SCE pump test"
la var uniq_id "Unique SCE pump test identifier"
order uniq_id test_date_stata pump_ref pump_name
unique test_date_stata pump_ref
sort *
compress
save "$dirpath_data_sce_cleaned/sce_pump_test_data.dta", replace


************************************************
************************************************
