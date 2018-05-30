************************************************
* Build and clean PGE agricultural rate data.
************************************************
**** SETUP:
clear all
memory clear
set more off, perm
version 13

global dirpath "S:/Matt/ag_pump"
** additional directory paths to make things easier
global dirpath_data "$dirpath/data"
global dirpath_data_temp "$dirpath/data/temp"
global dirpath_data_pge_raw "$dirpath_data/pge_raw"

************************************************
// all rates downloaded from https://www.pge.com/tariffs/electric.shtml on 5/8/18


** DELETE OLD TEMP FILES
clear
cd "$dirpath_data_temp"
local files: dir . files "*.dta"
qui foreach f in `files' {
	erase "`f'"
}

clear
cd "$dirpath_data_temp/cleaned"
local files: dir . files "*.dta"
qui foreach f in `files' {
	erase "`f'"
}
**** STEP 1: IMPORT ALL EXCEL DATA TO STATA
cd "$dirpath_data_pge_raw/rates/excel"

local files: dir . files "*.xls"

foreach file in `files' {
 import excel "`file'", firstrow clear
 gen file = "`file'"
 replace file = subinstr(file, ".xls", "", .)
 local fname = file
 missings dropvars, force
 rename *, lower
 save "$dirpath_data_temp/`fname'.dta", replace 
}

*/

**** LARGE AG RATES


**** BEGINNING THROUGH 100301-100430 
foreach dates in "080101-080229" "080301-080430" "080501-080930" "081001-081231" ///
   "090101-090228" "090301-090930" "091001-091231" ///
   "100101-100228" "100301-100430" {
   
// read dataset in
use "$dirpath_data_temp/lgag_`dates'.dta", clear
rename *, lower

ds
foreach var in `r(varlist)' {
capture confirm string variable `var'
if !_rc {
  replace `var' = lower(`var')
}
}

// get rid of the footnotes
drop if timeofuseperiod == ""

// split the rate schedules into two
split rateschedule, p("2/")
drop rateschedule2 rateschedule3 rateschedule
rename rateschedule1 rateschedule

// fill in common variables across the rate schedule
foreach var in rateschedule ratedesign customercharge ///
  averagetotalrate1perkwh season otherchargeconditions {
 capture confirm numeric variable `var'
 if !_rc {
replace `var' = `var'[_n-1] if `var' == .
 }
 else {
replace `var' = `var'[_n-1] if `var' == ""
 }

}

// grab customer charge & meter charge
gen rownumber = _n

expand 2 if strpos(rateschedule, " and ")
bys rownumber: gen dupes = _n

split rateschedule, p(" and ")
replace rateschedule = ""
replace rateschedule = rateschedule1 if dupes == 1
replace rateschedule = rateschedule2 if dupes == 2
drop rateschedule1 rateschedule2


split customercharge, p(" plus ")
drop customercharge
rename customercharge1 customercharge
rename customercharge2 metercharge


split metercharge, p("   ")

replace metercharge = ""
replace metercharge = metercharge1 if dupes == 1
replace metercharge = metercharge2 if dupes == 2
drop metercharge1 metercharge2


replace metercharge = subinstr(metercharge, rateschedule, "", .)


foreach l in `c(alpha)' "/" "$" {
 replace customercharge = subinstr(customercharge, "`l'", "", .)
 replace metercharge = subinstr(metercharge, "`l'", "", .)
}
replace customercharge = trim(itrim(customercharge))
replace metercharge = trim(itrim(metercharge))

replace customercharge = "0" if customercharge == ""
replace metercharge = "0" if metercharge == ""

replace demandcharge = "0" if demandcharge == "-"
replace energycharge = "0" if energycharge == "-"


destring customercharge metercharge demandcharge energycharge, replace

replace rateschedule = upper(rateschedule)
drop rownumber dupes

// get the start & end date for the rates
rename file dates
replace dates = subinstr(dates, "lgag_", "", .)
split dates, p("-")
replace dates1 = "20" + dates1
replace dates2 = "20" + dates2

gen rate_start_date = date(dates1, "YMD") 
gen rate_end_date = date(dates2, "YMD") 

format rate_start rate_end %td

drop dates dates1 dates2

// create a tou flag
gen tou = 0
replace tou = 1 if strpos(ratedesign, "tou")
replace tou = 1 if strpos(ratedesign, "time-of-use")
replace tou = 1 if strpos(ratedesign, "time of use")

drop ratedesign

egen peaktype_group = group(otherchargeconditions)


// create an indicator for peak, off peak, partial peak for each hour
gen rownr = _n if tou == 1 & timeofuseperiod != "maximum" 
expand 24 if tou == 1 & timeofuseperiod != "maximum" 
bys rownr: gen hour = _n
replace hour = hour - 1
expand 2 if tou == 1 & timeofuseperiod != "maximum" 
bys rownr hour: gen minute = _n
replace minute = 0 if minute == 1
replace minute = 30 if minute == 2
expand 7 if tou == 1 & timeofuseperiod != "maximum"
bys rownr hour minute: gen dow_num = _n
replace dow_num = dow_num - 1
// sunday = 0, monday = 1, ... saturday = 6

drop rownr

// create a peak flag
gen offpeak = 0 if tou ==1 & timeofuseperiod != "maximum"
gen partpeak = 0 if tou == 1 & timeofuseperiod != "maximum"
gen peak = 0 if tou==1 & timeofuseperiod != "maximum"

replace peaktype_group = . if tou == 0




/* peak group 1:
customer chooses summer peak period:  noon to 4:00 pm weekdays or 1:00 pm to 5:00 pm weekday
 s or 2:00 pm to 6:00 pm weekdays.  winter partial peak: 8:30 am to 9:30 pm weekdays.  all 
 other hours and holidays are off peak for both summer and winter.
*/

// 2 == "possible peak"
replace peak = 2 if timeofuseperiod == "max peak" & season == "summer" /// 
  & peaktype_group == 1 & hour >= 12 & hour <= 18 ///
  & (dow_num >= 1 & dow_num <= 5) 
  
replace peak = 2 if timeofuseperiod == "max peak" & season == "summer" /// 
  & peaktype_group == 1 & hour >= 14 & hour <= 18 ///
  & (dow_num >= 1 & dow_num <= 5) 
    
replace partpeak = 1 if timeofuseperiod == "part-peak" /// 
  & peaktype_group == 1 & hour >= 8 & hour <= 21 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
  & peaktype_group == 1 & hour == 8 & minute == 0 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
  & peaktype_group == 1 & hour == 21 & minute == 30 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace offpeak = 1 if peak == 0 & partpeak ==0 & peaktype_group == 1


** peak group 2:
/*customer chooses summer peak periods: noon to 6:00 pm mtw, or noon to 6:00 pm wthf   winter 
 partial peak: 8:30 am to 9:30 pm weekdays.  all other hours and holidays are off peak for 
 both summer and winter.
*/

// 2 == "possible peak"
replace peak = 2 if timeofuseperiod == "max peak" /// 
  & peaktype_group == 2 & hour >= 12 & hour <= 17 ///
  & (dow_num == 1 | dow_num == 2 | dow_num == 3) & season == "summer"
  
replace peak = 2 if timeofuseperiod == "max peak" /// 
  & peaktype_group == 2 & hour >= 12 & hour <= 17 ///
  & (dow_num == 3 | dow_num == 4 | dow_num == 5) & season == "summer"
  

replace partpeak = 1 if timeofuseperiod == "part-peak" /// 
  & peaktype_group == 2 & hour >= 8 & hour <= 21 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
  & peaktype_group == 2 & hour == 8 & minute == 0 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
  & peaktype_group == 2 & hour == 21 & minute == 30 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace offpeak = 1 if peak == 0 & partpeak ==0 & peaktype_group == 2 




/* peak group 3:
summer peak: noon to 6:00 pm weekdays,  partial peak 8:30 am to noon; 6:00 pm to 9:30 pm  we
> ekdays.  winter partial peak: 8:30 am to 9:30 pm weekdays.  all other hours and holidays a
> re off peak for both summer and winter.
*/

replace peak = 1 if timeofuseperiod == "max peak" & season == "summer" /// 
  & peaktype_group == 3 & hour >= 12 & hour <= 18 ///
  & (dow_num >= 1 & dow_num <= 5) 
    
replace partpeak = 1 if timeofuseperiod == "part-peak" /// 
  & peaktype_group == 3 & hour >= 8 & hour <= 12 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "summer"
  
replace partpeak = 1 if timeofuseperiod == "part-peak" /// 
  & peaktype_group == 3 & hour >= 18 & hour <= 21 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "summer"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
  & peaktype_group == 3 & hour == 8 & minute == 0 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "summer"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
  & peaktype_group == 3 & hour == 21 & minute == 30 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "summer"

replace partpeak = 1 if timeofuseperiod == "part-peak" /// 
  & peaktype_group == 3 & hour >= 8 & hour <= 21 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
  & peaktype_group == 3 & hour == 8 & minute == 0 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
  & peaktype_group == 3 & hour == 21 & minute == 30 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace offpeak = 1 if peak == 0 & partpeak ==0 & peaktype_group == 3 
  

/* peak group 4:
summer peak: noon to 6:00 pm weekdays.  winter partial peak: 8:30 am to 9:30 pm weekdays.  a
  ll other hours and holidays are off peak for both summer and winter.
*/

replace peak = 1 if timeofuseperiod == "max peak" & season == "summer" /// 
  & peaktype_group == 4 & hour >= 12 & hour <= 18 ///
  & (dow_num >= 1 & dow_num <= 5) 

    
replace partpeak = 1 if timeofuseperiod == "part-peak" /// 
  & peaktype_group == 4 & hour >= 8 & hour <= 21 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
  & peaktype_group == 4 & hour == 8 & minute == 0 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
  & peaktype_group == 4 & hour == 21 & minute == 30 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  

replace offpeak = 1 if peak == 0 & partpeak ==0 & peaktype_group == 4 

// set up the energy charge for all hours of the day
egen peakenergy_prelim = mean(energycharge) if timeofuse == "max peak", by(rateschedule season)
egen offpeakenergy_prelim = mean(energycharge) if timeofuse == "off-peak", by(rateschedule season)
egen partialpeakenergy_prelim = mean(energycharge) if timeofuse == "part-peak", by(rateschedule season)


egen peakenergy = mean(peakenergy_prelim), by(rateschedule season)
egen offpeakenergy = mean(offpeakenergy_prelim), by(rateschedule season)
egen partialpeakenergy = mean(partialpeakenergy_prelim), by(rateschedule season)


replace energycharge = peakenergy if peak == 1 & tou == 1
replace energycharge = offpeakenergy if offpeak == 1 & tou == 1
replace energycharge = partialpeakenergy if partpeak == 1 & tou == 1

drop peakenergy offpeakenergy partialpeakenergy *_prelim


// NOT SURE IF DEMAND CHARGES ARE ADDITIVE?
gen maxdemandcharge = .
replace maxdemandcharge = demandcharge if timeofuse == "maximum"

egen maxdemand = mean(maxdemandcharge), by(rateschedule)

replace maxdemandcharge = maxdemand
drop maxdemand

drop if timeofuse == "maximum"

drop otherchargeconditions peaktype_group 

// organize the dataset
order rateschedule rate_start rate_end tou season  dow_num hour minute ///
  offpeak partpeak peak demandcharge maxdemandcharge energycharge customercharge metercharge
  
drop averagetotalrate  

rename demandcharge3kw demandcharge
rename energychargekwh energycharge

label variable rateschedule "rate name"
label variable rate_start_date "rate period start date"
label variable rate_end_date "rate period end date"
label variable tou "tou rate? (1/0)"
label variable season "summer vs winter? summer: may-oct, winter = nov-apr"
label variable dow_num "day of week (following stata dow numbering)"
lab define dowl 0 "sunday" 1 "monday" 2 "tuesday" 3 "wednesday" ///
  4 "thursday" 5 "friday" 6 "saturday" 
label values dow_num dowl

label variable hour "hour of day"
label variable minute "minute of day"
label variable offpeak "is off peak? (1/0)"
label variable partpeak "is partial peak? (1/0)"
label variable peak "is peak? 1: yes 2: choice 0: no"

lab define peakl 0 "no" 1 "yes" 2 "choice"
label values peak peakl

label variable demandcharge "demand charge ($/kW)"
label variable maxdemandcharge "base demand charge ($/kW)"
label variable energycharge "energy charge ($/kWh)"
label variable customercharge "customer charge ($/day)"
label variable metercharge "meter charge ($/day)"

drop timeofuseperiod
duplicates drop

save "$dirpath_data_temp/cleaned/CLEANED_lgag_`dates'.dta", replace
}
  




  
  
  
  


  
foreach dates in "100501-100531" "100601-101231" "110101-110228" "110301-111231" ///
  "120101-120229" "120301-120630" "120701-121231" "130101-130430" "130501-130930" ///
  "131001-131231" "140101-140228" "140301-140430" "140501-140930" "141001-141231" ///
  "150101-150228" "150301-150831" "150901-151231" "160101-160229" "160301-160323" ///
  "160324-160731" "160801-160930" "161001-161231" "170101-170228" "170301-171231" ///
  "180101-180228" {
// read dataset in
use "$dirpath_data_temp/lgag_`dates'.dta", clear
rename *, lower

ds
foreach var in `r(varlist)' {
capture confirm string variable `var'
if !_rc {
  replace `var' = lower(`var')
}
}

// get rid of the footnotes
drop if timeofuseperiod == ""

// split the rate schedules into two
split rateschedule, p("4/")
drop rateschedule2 rateschedule3 rateschedule
rename rateschedule1 rateschedule

// fill in common variables across the rate schedule
foreach var in rateschedule ratedesign customercharge ///
  averagetotalrate3perkwh season {
 capture confirm numeric variable `var'
 if !_rc {
replace `var' = `var'[_n-1] if `var' == .
 }
 else {
replace `var' = `var'[_n-1] if `var' == ""
 }

}

foreach var in demandcharge energycharge pdp1 pdp2 {
 replace `var' = subinstr(`var', "-", "", .)
 destring `var', replace
}
replace pdp2 = -pdp2

egen pdp_mean = mean(pdp1charges), by(rateschedule)
replace pdp1 = pdp_mean
drop pdp_mean

foreach var in demandcharge energycharge pdp1 pdp2 {
replace `var' = 0 if `var' == .
}
// grab customer charge & meter charge
gen rownumber = _n

expand 2 if strpos(rateschedule, " and ")
bys rownumber: gen dupes = _n

split rateschedule, p(" and ")
replace rateschedule = ""
replace rateschedule = rateschedule1 if dupes == 1
replace rateschedule = rateschedule2 if dupes == 2
drop rateschedule1 rateschedule2


split customercharge, p(" plus ")
drop customercharge
rename customercharge1 customercharge
rename customercharge2 metercharge


split metercharge, p("   ")

replace metercharge = ""
replace metercharge = metercharge1 if dupes == 1
replace metercharge = metercharge2 if dupes == 2
drop metercharge1 metercharge2


replace metercharge = subinstr(metercharge, rateschedule, "", .)


foreach l in `c(alpha)' "/" "$" {
 replace customercharge = subinstr(customercharge, "`l'", "", .)
 replace metercharge = subinstr(metercharge, "`l'", "", .)
}
replace customercharge = trim(itrim(customercharge))
replace metercharge = trim(itrim(metercharge))

replace customercharge = "0" if customercharge == ""
replace metercharge = "0" if metercharge == ""


destring customercharge metercharge, replace

drop rownumber dupes

// get the start & end date for the rates
rename file dates
replace dates = subinstr(dates, "lgag_", "", .)
split dates, p("-")
replace dates1 = "20" + dates1
replace dates2 = "20" + dates2

gen rate_start_date = date(dates1, "YMD") 
gen rate_end_date = date(dates2, "YMD") 

format rate_start rate_end %td

drop dates dates1 dates2

// create a tou flag
gen tou = 0
replace tou = 1 if strpos(ratedesign, "tou")
replace tou = 1 if strpos(ratedesign, "time-of-use")
replace tou = 1 if strpos(ratedesign, "time of use")

drop ratedesign

// create an indicator for peak, off peak, partial peak for each hour
gen rownr = _n if tou == 1 & timeofuseperiod != "maximum" 
expand 24 if tou == 1 & timeofuseperiod != "maximum" 
bys rownr: gen hour = _n
replace hour = hour - 1
expand 2 if tou == 1 & timeofuseperiod != "maximum" 
bys rownr hour: gen minute = _n
replace minute = 0 if minute == 1
replace minute = 30 if minute == 2
expand 7 if tou == 1 & timeofuseperiod != "maximum"
bys rownr hour minute: gen dow_num = _n
replace dow_num = dow_num - 1
// sunday = 0, monday = 1, ... saturday = 6

drop rownr

// create a peak flag
gen offpeak = 0 if tou ==1 & timeofuseperiod != "maximum"
gen partpeak = 0 if tou == 1 & timeofuseperiod != "maximum"
gen peak = 0 if tou==1 & timeofuseperiod != "maximum"



/* ag-r rates: (group 1: summer - 12 noon to 6p, mtw)
 group 2: summer - 12 noon to 6 pm w th f
 off peak all other days/hours
 
 winter: partial peak 830a-930p m-f
 off peak all other days/hours
*/

// 2 == "possible peak"
replace peak = 2 if timeofuseperiod == "max peak" & season == "summer" /// 
  & (rateschedule == "ag-rb" | rateschedule == "ag-re")  & hour >= 12 & hour <= 18 ///
  & (dow_num >= 1 & dow_num <= 5) 
  
replace peak = 2 if timeofuseperiod == "max peak" & season == "summer" /// 
  & (rateschedule == "ag-rb" | rateschedule == "ag-re") & hour >= 14 & hour <= 18 ///
  & (dow_num >= 1 & dow_num <= 5) 
    
replace partpeak = 1 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-rb" | rateschedule == "ag-re") & hour >= 8 & hour <= 21 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-rb" | rateschedule == "ag-re") & hour == 8 & minute == 0 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-rb" | rateschedule == "ag-re") & hour == 21 & minute == 30 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace offpeak = 1 if peak == 0 & partpeak ==0 & (rateschedule == "ag-rb" | rateschedule == "ag-re")


/* ag-v rates:
AG-V Short-Peak Time-of-Use Periods						
						
	Summer  (May-October)					
		Peak:*				
			Group I		12:00 noon to 4:00 pm	Monday through Friday (except holidays)
			Group II		1:00 pm to 5:00 pm	Monday through Friday (except holidays)
			Group III		2:00 pm to 6:00 pm	Monday through Friday (except holidays)
						
		Off-Peak:			All Other Hours	Monday through Friday
					All Day	Saturday, Sunday, Holidays
						
	Winter  (November-April)					
		Partial Peak:			8:30 am to 9:30 pm	Monday through Friday (except holidays)
						
		Off-Peak:			All Other Hours	Monday through Friday
					All Day	Saturday, Sunday, Holidays
*/


// 2 == "possible peak"
replace peak = 2 if timeofuseperiod == "max peak" /// 
  & (rateschedule == "ag-vb" | rateschedule == "ag-ve") & hour >= 12 & hour <= 16 ///
  & (dow_num >=1 & dow_num <= 5) & season == "summer"
  
replace peak = 2 if timeofuseperiod == "max peak" /// 
  & (rateschedule == "ag-vb" | rateschedule == "ag-ve") & hour >= 13 & hour <= 17 ///
  & (dow_num >=1 & dow_num <= 5) & season == "summer"
  

replace peak = 2 if timeofuseperiod == "max peak" /// 
  &(rateschedule == "ag-vb" | rateschedule == "ag-ve") & hour >= 14 & hour <= 18 ///
  & (dow_num >=1 & dow_num <= 5) & season == "summer"
  
replace partpeak = 1 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-vb" | rateschedule == "ag-ve") & hour >= 8 & hour <= 21 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-vb" | rateschedule == "ag-ve") & hour == 8 & minute == 0 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-vb" | rateschedule == "ag-ve") & hour == 21 & minute == 30 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace offpeak = 1 if peak == 0 & partpeak ==0 & (rateschedule == "ag-vb" | rateschedule == "ag-ve") 




/* 
AG-4 Time-of-Use Periods						
						
	Summer  (May-October)					
		For Rates A, B, D, and E				
						
			Peak:		12:00 noon to 6:00 pm	Monday through Friday (except holidays)
						
			Off-Peak:		All Other Hours	Monday through Friday
					All Day	Saturday, Sunday, Holidays
						
		For Rates C and F				
						
			Peak:		12:00 noon to 6:00 pm	Monday through Friday (except holidays)
						
			Partial-Peak:		8:30 am to 12:00 pm	Monday through Friday (except holidays)
					6:00 pm to 9:30 pm	Monday through Friday (except holidays)
						
			Off-Peak:		9:30 pm to 8:30 am	Monday through Friday 
					All Day	Saturday, Sunday, Holidays
						
	Winter  (November-April)					
		For Rates A, B, C, D, E, and F				
						
			Partial-Peak:		8:30 am to 9:30 pm	Monday through Friday (except holidays)
						
			Off-Peak:		All Other Hours	Monday through Friday 
					All Day	Saturday, Sunday, Holidays
						

*/

replace peak = 1 if timeofuseperiod == "max peak" & season == "summer" /// 
  & (rateschedule == "ag-4a" | rateschedule == "ag-4b" | /// 
   rateschedule == "ag-4d" | rateschedule == "ag-4e")  ///
  & hour >= 12 & hour <= 18 ///
  & (dow_num >= 1 & dow_num <= 5) 

  
replace peak = 1 if timeofuseperiod == "max peak" & season == "summer" /// 
  & (rateschedule == "ag-4c" | rateschedule == "ag-4f")  ///
  & hour >= 12 & hour <= 18 ///
  & (dow_num >= 1 & dow_num <= 5) 
  
  
replace partpeak = 1 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-4c" | rateschedule == "ag-4f") & hour >= 8 & hour <= 12 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "summer"
  
replace partpeak = 1 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-4c" | rateschedule == "ag-4f") & hour >= 18 & hour <= 21 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "summer"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-4c" | rateschedule == "ag-4f") & hour == 8 & minute == 0 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "summer"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-4c" | rateschedule == "ag-4f") & hour == 21 & minute == 30 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "summer"

replace partpeak = 1 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-4c" | rateschedule == "ag-4f") & hour >= 8 & hour <= 21 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
    & (rateschedule == "ag-4a" | rateschedule == "ag-4b" | /// 
   rateschedule == "ag-4d" | rateschedule == "ag-4e")  ///
 & hour == 8 & minute == 0 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
    & (rateschedule == "ag-4a" | rateschedule == "ag-4b" | /// 
   rateschedule == "ag-4d" | rateschedule == "ag-4e")  ///
 & hour == 21 & minute == 30 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace offpeak = 1 if peak == 0 & partpeak ==0 ///
   & (rateschedule == "ag-4a" | rateschedule == "ag-4b" | /// 
   rateschedule == "ag-4d" | rateschedule == "ag-4e")  ///
 
  

replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
    & (rateschedule == "ag-4c" | rateschedule == "ag-4f")  ///
 & hour == 8 & minute == 0 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
    & (rateschedule == "ag-4c" | rateschedule == "ag-4f")  ///
 & hour == 21 & minute == 30 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace offpeak = 1 if peak == 0 & partpeak ==0 ///
   & (rateschedule == "ag-4c" | rateschedule == "ag-4f")  ///

   
/* AG-5 Large Time-of-Use Periods						
						
	Summer  (May-October)					
		For Rates A, B, D, and E				
						
			Peak:		12:00 noon to 6:00 pm	Monday through Friday (except holidays)
						
			Off-Peak:		All Other Hours	Monday through Friday
					All Day	Saturday, Sunday, Holidays
						
		For Rates C and F				
						
			Peak:		12:00 noon to 6:00 pm	Monday through Friday (except holidays)
						
			Partial-Peak:		8:30 am to 12:00 pm	Monday through Friday (except holidays)
					6:00 pm to 9:30 pm	Monday through Friday (except holidays)
						
			Off-Peak:		9:30 pm to 8:30 am	Monday through Friday 
					All Day	Saturday, Sunday, Holidays
						
	Winter  (November-April)					
		For Rates A, B, C, D, E, and F				
						
			Partial-Peak:		8:30 am to 9:30 pm	Monday through Friday (except holidays)
						
			Off-Peak:		All other hours	Monday through Friday 
					All day	Saturday, Sunday, Holidays
						

*/

replace peak = 1 if timeofuseperiod == "max peak" & season == "summer" /// 
  & (rateschedule == "ag-5a" | rateschedule == "ag-5b" | rateschedule == "ag-5c" | ///
  rateschedule == "ag-5d" | rateschedule == "ag-5e" | rateschedule == "ag-5f") /// 
  & hour >= 12 & hour <= 18 ///
  & (dow_num >= 1 & dow_num <= 5) 

    
replace partpeak = 1 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-5c" | rateschedule == "ag-5f") & hour >= 8 & hour <= 12 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "summer"

replace partpeak = 1 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-5c" | rateschedule == "ag-5f") & hour >= 18 & hour <= 21 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "summer"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-5c" | rateschedule == "ag-5f") & hour == 8 & minute == 0 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "summer"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-5c" | rateschedule == "ag-5f") & hour == 21 & minute == 30 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "summer"
  

replace partpeak = 1 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-5c" | rateschedule == "ag-5f") & hour >= 8 & hour <= 21 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
    & (rateschedule == "ag-5a" | rateschedule == "ag-5b" | rateschedule == "ag-5c" | ///
  rateschedule == "ag-5d" | rateschedule == "ag-5e" | rateschedule == "ag-5f") /// 
 & hour == 8 & minute == 0 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
    & (rateschedule == "ag-5a" | rateschedule == "ag-5b" | rateschedule == "ag-5c" | ///
  rateschedule == "ag-5d" | rateschedule == "ag-5e" | rateschedule == "ag-5f") /// 
 & hour == 21 & minute == 30 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  

replace offpeak = 1 if peak == 0 & partpeak ==0 /// 
& (rateschedule == "ag-5a" | rateschedule == "ag-5b" | rateschedule == "ag-5c" | ///
  rateschedule == "ag-5d" | rateschedule == "ag-5e" | rateschedule == "ag-5f")  


// set up the energy charge for all hours of the day
egen peakenergy_prelim = mean(energycharge) if timeofuse == "max peak", by(rateschedule season)
egen offpeakenergy_prelim = mean(energycharge) if timeofuse == "off-peak", by(rateschedule season)
egen partialpeakenergy_prelim = mean(energycharge) if timeofuse == "part-peak", by(rateschedule season)


egen peakenergy = mean(peakenergy_prelim), by(rateschedule season)
egen offpeakenergy = mean(offpeakenergy_prelim), by(rateschedule season)
egen partialpeakenergy = mean(partialpeakenergy_prelim), by(rateschedule season)


replace energycharge = peakenergy if peak == 1 & tou == 1
replace energycharge = offpeakenergy if offpeak == 1 & tou == 1
replace energycharge = partialpeakenergy if partpeak == 1 & tou == 1

drop peakenergy offpeakenergy partialpeakenergy *_prelim


// NOT SURE IF DEMAND CHARGES ARE ADDITIVE?
gen maxdemandcharge = .
replace maxdemandcharge = demandcharge if timeofuse == "maximum"

egen maxdemand = mean(maxdemandcharge), by(rateschedule)

replace maxdemandcharge = maxdemand
drop maxdemand

drop if timeofuse == "maximum"

replace rateschedule = upper(rateschedule)


// organize the dataset
order rateschedule rate_start rate_end tou season  dow_num hour minute ///
  offpeak partpeak peak demandcharge maxdemandcharge energycharge customercharge metercharge
  
drop averagetotalrate  

rename demandcharge5kw demandcharge
rename energychargekwh energycharge
rename pdp1 pdpcharge
rename pdp2 pdpcredit


label variable rateschedule "rate name"
label variable rate_start_date "rate period start date"
label variable rate_end_date "rate period end date"
label variable tou "tou rate? (1/0)"
label variable season "summer vs winter? summer: may-oct, winter = nov-apr"
label variable dow_num "day of week (following stata dow numbering)"
lab define dowl 0 "sunday" 1 "monday" 2 "tuesday" 3 "wednesday" ///
  4 "thursday" 5 "friday" 6 "saturday" 
label values dow_num dowl

label variable hour "hour of day"
label variable minute "minute of day"
label variable offpeak "is off peak? (1/0)"
label variable partpeak "is partial peak? (1/0)"
label variable peak "is peak? 1: yes 2: choice 0: no"

lab define peakl 0 "no" 1 "yes" 2 "choice"
label values peak peakl

label variable demandcharge "demand charge ($/kW)"
label variable maxdemandcharge "base demand charge ($/kW)"
label variable energycharge "energy charge ($/kWh)"
label variable customercharge "customer charge ($/day)"
label variable metercharge "meter charge ($/day)"
label variable pdpcharge "peak day pricing charge ($/kwh during event hours)"
label variable pdpcredit "peak day pricing credit ($/kw)"

drop timeofuseperiod
duplicates drop

save "$dirpath_data_temp/cleaned/CLEANED_lgag_`dates'.dta", replace
}





  
  
    
  
  
  
 
foreach dates in "Current" {
// read dataset in
use "$dirpath_data_temp/lgag`dates'.dta", clear
rename *, lower

ds
foreach var in `r(varlist)' {
capture confirm string variable `var'
if !_rc {
  replace `var' = lower(`var')
}
}

// get rid of the footnotes
drop if timeofuseperiod == ""

// split the rate schedules into two
split rateschedule, p("4/")
drop rateschedule2 rateschedule3 rateschedule
rename rateschedule1 rateschedule

// fill in common variables across the rate schedule
foreach var in rateschedule ratedesign customercharge ///
  averagetotalrate3perkwh season {
 capture confirm numeric variable `var'
 if !_rc {
replace `var' = `var'[_n-1] if `var' == .
 }
 else {
replace `var' = `var'[_n-1] if `var' == ""
 }

}

foreach var in demandcharge energycharge pdp1 pdp2 {
 replace `var' = subinstr(`var', "-", "", .)
 destring `var', replace
}
replace pdp2 = -pdp2

egen pdp_mean = mean(pdp1charges), by(rateschedule)
replace pdp1 = pdp_mean
drop pdp_mean

foreach var in demandcharge energycharge pdp1 pdp2 {
replace `var' = 0 if `var' == .
}
// grab customer charge & meter charge
gen rownumber = _n

expand 2 if strpos(rateschedule, " and ")
bys rownumber: gen dupes = _n

split rateschedule, p(" and ")
replace rateschedule = ""
replace rateschedule = rateschedule1 if dupes == 1
replace rateschedule = rateschedule2 if dupes == 2
drop rateschedule1 rateschedule2


split customercharge, p(" plus ")
drop customercharge
rename customercharge1 customercharge
rename customercharge2 metercharge


split metercharge, p("   ")

replace metercharge = ""
replace metercharge = metercharge1 if dupes == 1
replace metercharge = metercharge2 if dupes == 2
drop metercharge1 metercharge2


replace metercharge = subinstr(metercharge, rateschedule, "", .)


foreach l in `c(alpha)' "/" "$" {
 replace customercharge = subinstr(customercharge, "`l'", "", .)
 replace metercharge = subinstr(metercharge, "`l'", "", .)
}
replace customercharge = trim(itrim(customercharge))
replace metercharge = trim(itrim(metercharge))

replace customercharge = "0" if customercharge == ""
replace metercharge = "0" if metercharge == ""


destring customercharge metercharge, replace

drop rownumber dupes

// get the start & end date for the rates
rename file dates
replace dates = "180301-999999"
local dates = dates
split dates, p("-")
replace dates1 = "20" + dates1
replace dates2 = "20" + dates2

gen rate_start_date = date(dates1, "YMD") 
gen rate_end_date = date(dates2, "YMD") 

format rate_start rate_end %td

drop dates dates1 dates2

// create a tou flag
gen tou = 0
replace tou = 1 if strpos(ratedesign, "tou")
replace tou = 1 if strpos(ratedesign, "time-of-use")
replace tou = 1 if strpos(ratedesign, "time of use")

drop ratedesign

// create an indicator for peak, off peak, partial peak for each hour
gen rownr = _n if tou == 1 & timeofuseperiod != "maximum" 
expand 24 if tou == 1 & timeofuseperiod != "maximum" 
bys rownr: gen hour = _n
replace hour = hour - 1
expand 2 if tou == 1 & timeofuseperiod != "maximum" 
bys rownr hour: gen minute = _n
replace minute = 0 if minute == 1
replace minute = 30 if minute == 2
expand 7 if tou == 1 & timeofuseperiod != "maximum"
bys rownr hour minute: gen dow_num = _n
replace dow_num = dow_num - 1
// sunday = 0, monday = 1, ... saturday = 6

drop rownr

// create a peak flag
gen offpeak = 0 if tou ==1 & timeofuseperiod != "maximum"
gen partpeak = 0 if tou == 1 & timeofuseperiod != "maximum"
gen peak = 0 if tou==1 & timeofuseperiod != "maximum"



/* ag-r rates: (group 1: summer - 12 noon to 6p, mtw)
 group 2: summer - 12 noon to 6 pm w th f
 off peak all other days/hours
 
 winter: partial peak 830a-930p m-f
 off peak all other days/hours
*/

// 2 == "possible peak"
replace peak = 2 if timeofuseperiod == "max peak" & season == "summer" /// 
  & (rateschedule == "ag-rb" | rateschedule == "ag-re")  & hour >= 12 & hour <= 18 ///
  & (dow_num >= 1 & dow_num <= 5) 
  
replace peak = 2 if timeofuseperiod == "max peak" & season == "summer" /// 
  & (rateschedule == "ag-rb" | rateschedule == "ag-re") & hour >= 14 & hour <= 18 ///
  & (dow_num >= 1 & dow_num <= 5) 
    
replace partpeak = 1 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-rb" | rateschedule == "ag-re") & hour >= 8 & hour <= 21 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-rb" | rateschedule == "ag-re") & hour == 8 & minute == 0 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-rb" | rateschedule == "ag-re") & hour == 21 & minute == 30 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace offpeak = 1 if peak == 0 & partpeak ==0 & (rateschedule == "ag-rb" | rateschedule == "ag-re")


/* ag-v rates:
AG-V Short-Peak Time-of-Use Periods						
						
	Summer  (May-October)					
		Peak:*				
			Group I		12:00 noon to 4:00 pm	Monday through Friday (except holidays)
			Group II		1:00 pm to 5:00 pm	Monday through Friday (except holidays)
			Group III		2:00 pm to 6:00 pm	Monday through Friday (except holidays)
						
		Off-Peak:			All Other Hours	Monday through Friday
					All Day	Saturday, Sunday, Holidays
						
	Winter  (November-April)					
		Partial Peak:			8:30 am to 9:30 pm	Monday through Friday (except holidays)
						
		Off-Peak:			All Other Hours	Monday through Friday
					All Day	Saturday, Sunday, Holidays
*/


// 2 == "possible peak"
replace peak = 2 if timeofuseperiod == "max peak" /// 
  & (rateschedule == "ag-vb" | rateschedule == "ag-ve") & hour >= 12 & hour <= 16 ///
  & (dow_num >=1 & dow_num <= 5) & season == "summer"
  
replace peak = 2 if timeofuseperiod == "max peak" /// 
  & (rateschedule == "ag-vb" | rateschedule == "ag-ve") & hour >= 13 & hour <= 17 ///
  & (dow_num >=1 & dow_num <= 5) & season == "summer"
  

replace peak = 2 if timeofuseperiod == "max peak" /// 
  &(rateschedule == "ag-vb" | rateschedule == "ag-ve") & hour >= 14 & hour <= 18 ///
  & (dow_num >=1 & dow_num <= 5) & season == "summer"
  
replace partpeak = 1 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-vb" | rateschedule == "ag-ve") & hour >= 8 & hour <= 21 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-vb" | rateschedule == "ag-ve") & hour == 8 & minute == 0 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-vb" | rateschedule == "ag-ve") & hour == 21 & minute == 30 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace offpeak = 1 if peak == 0 & partpeak ==0 & (rateschedule == "ag-vb" | rateschedule == "ag-ve") 




/* 
AG-4 Time-of-Use Periods						
						
	Summer  (May-October)					
		For Rates A, B, D, and E				
						
			Peak:		12:00 noon to 6:00 pm	Monday through Friday (except holidays)
						
			Off-Peak:		All Other Hours	Monday through Friday
					All Day	Saturday, Sunday, Holidays
						
		For Rates C and F				
						
			Peak:		12:00 noon to 6:00 pm	Monday through Friday (except holidays)
						
			Partial-Peak:		8:30 am to 12:00 pm	Monday through Friday (except holidays)
					6:00 pm to 9:30 pm	Monday through Friday (except holidays)
						
			Off-Peak:		9:30 pm to 8:30 am	Monday through Friday 
					All Day	Saturday, Sunday, Holidays
						
	Winter  (November-April)					
		For Rates A, B, C, D, E, and F				
						
			Partial-Peak:		8:30 am to 9:30 pm	Monday through Friday (except holidays)
						
			Off-Peak:		All Other Hours	Monday through Friday 
					All Day	Saturday, Sunday, Holidays
						

*/

replace peak = 1 if timeofuseperiod == "max peak" & season == "summer" /// 
  & (rateschedule == "ag-4a" | rateschedule == "ag-4b" | /// 
   rateschedule == "ag-4d" | rateschedule == "ag-4e")  ///
  & hour >= 12 & hour <= 18 ///
  & (dow_num >= 1 & dow_num <= 5) 

  
replace peak = 1 if timeofuseperiod == "max peak" & season == "summer" /// 
  & (rateschedule == "ag-4c" | rateschedule == "ag-4f")  ///
  & hour >= 12 & hour <= 18 ///
  & (dow_num >= 1 & dow_num <= 5) 
  
  
replace partpeak = 1 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-4c" | rateschedule == "ag-4f") & hour >= 8 & hour <= 12 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "summer"
  
replace partpeak = 1 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-4c" | rateschedule == "ag-4f") & hour >= 18 & hour <= 21 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "summer"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-4c" | rateschedule == "ag-4f") & hour == 8 & minute == 0 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "summer"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-4c" | rateschedule == "ag-4f") & hour == 21 & minute == 30 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "summer"

replace partpeak = 1 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-4c" | rateschedule == "ag-4f") & hour >= 8 & hour <= 21 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
    & (rateschedule == "ag-4a" | rateschedule == "ag-4b" | /// 
   rateschedule == "ag-4d" | rateschedule == "ag-4e")  ///
 & hour == 8 & minute == 0 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
    & (rateschedule == "ag-4a" | rateschedule == "ag-4b" | /// 
   rateschedule == "ag-4d" | rateschedule == "ag-4e")  ///
 & hour == 21 & minute == 30 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace offpeak = 1 if peak == 0 & partpeak ==0 ///
   & (rateschedule == "ag-4a" | rateschedule == "ag-4b" | /// 
   rateschedule == "ag-4d" | rateschedule == "ag-4e")  ///
 
  

replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
    & (rateschedule == "ag-4c" | rateschedule == "ag-4f")  ///
 & hour == 8 & minute == 0 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
    & (rateschedule == "ag-4c" | rateschedule == "ag-4f")  ///
 & hour == 21 & minute == 30 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace offpeak = 1 if peak == 0 & partpeak ==0 ///
   & (rateschedule == "ag-4c" | rateschedule == "ag-4f")  ///

   
/* AG-5 Large Time-of-Use Periods						
						
	Summer  (May-October)					
		For Rates A, B, D, and E				
						
			Peak:		12:00 noon to 6:00 pm	Monday through Friday (except holidays)
						
			Off-Peak:		All Other Hours	Monday through Friday
					All Day	Saturday, Sunday, Holidays
						
		For Rates C and F				
						
			Peak:		12:00 noon to 6:00 pm	Monday through Friday (except holidays)
						
			Partial-Peak:		8:30 am to 12:00 pm	Monday through Friday (except holidays)
					6:00 pm to 9:30 pm	Monday through Friday (except holidays)
						
			Off-Peak:		9:30 pm to 8:30 am	Monday through Friday 
					All Day	Saturday, Sunday, Holidays
						
	Winter  (November-April)					
		For Rates A, B, C, D, E, and F				
						
			Partial-Peak:		8:30 am to 9:30 pm	Monday through Friday (except holidays)
						
			Off-Peak:		All other hours	Monday through Friday 
					All day	Saturday, Sunday, Holidays
						

*/

replace peak = 1 if timeofuseperiod == "max peak" & season == "summer" /// 
  & (rateschedule == "ag-5a" | rateschedule == "ag-5b" | rateschedule == "ag-5c" | ///
  rateschedule == "ag-5d" | rateschedule == "ag-5e" | rateschedule == "ag-5f") /// 
  & hour >= 12 & hour <= 18 ///
  & (dow_num >= 1 & dow_num <= 5) 

    
replace partpeak = 1 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-5c" | rateschedule == "ag-5f") & hour >= 8 & hour <= 12 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "summer"

replace partpeak = 1 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-5c" | rateschedule == "ag-5f") & hour >= 18 & hour <= 21 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "summer"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-5c" | rateschedule == "ag-5f") & hour == 8 & minute == 0 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "summer"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-5c" | rateschedule == "ag-5f") & hour == 21 & minute == 30 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "summer"
  

replace partpeak = 1 if timeofuseperiod == "part-peak" /// 
  & (rateschedule == "ag-5c" | rateschedule == "ag-5f") & hour >= 8 & hour <= 21 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
    & (rateschedule == "ag-5a" | rateschedule == "ag-5b" | rateschedule == "ag-5c" | ///
  rateschedule == "ag-5d" | rateschedule == "ag-5e" | rateschedule == "ag-5f") /// 
 & hour == 8 & minute == 0 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  
replace partpeak = 0 if timeofuseperiod == "part-peak" /// 
    & (rateschedule == "ag-5a" | rateschedule == "ag-5b" | rateschedule == "ag-5c" | ///
  rateschedule == "ag-5d" | rateschedule == "ag-5e" | rateschedule == "ag-5f") /// 
 & hour == 21 & minute == 30 ///
  & (dow_num >= 1 & dow_num <= 5) & season == "winter"
  

replace offpeak = 1 if peak == 0 & partpeak ==0 /// 
& (rateschedule == "ag-5a" | rateschedule == "ag-5b" | rateschedule == "ag-5c" | ///
  rateschedule == "ag-5d" | rateschedule == "ag-5e" | rateschedule == "ag-5f")  


// set up the energy charge for all hours of the day
egen peakenergy_prelim = mean(energycharge) if timeofuse == "max peak", by(rateschedule season)
egen offpeakenergy_prelim = mean(energycharge) if timeofuse == "off-peak", by(rateschedule season)
egen partialpeakenergy_prelim = mean(energycharge) if timeofuse == "part-peak", by(rateschedule season)


egen peakenergy = mean(peakenergy_prelim), by(rateschedule season)
egen offpeakenergy = mean(offpeakenergy_prelim), by(rateschedule season)
egen partialpeakenergy = mean(partialpeakenergy_prelim), by(rateschedule season)


replace energycharge = peakenergy if peak == 1 & tou == 1
replace energycharge = offpeakenergy if offpeak == 1 & tou == 1
replace energycharge = partialpeakenergy if partpeak == 1 & tou == 1

drop peakenergy offpeakenergy partialpeakenergy *_prelim


// NOT SURE IF DEMAND CHARGES ARE ADDITIVE?
gen maxdemandcharge = .
replace maxdemandcharge = demandcharge if timeofuse == "maximum"

egen maxdemand = mean(maxdemandcharge), by(rateschedule)

replace maxdemandcharge = maxdemand
drop maxdemand

drop if timeofuse == "maximum"

replace rateschedule = upper(rateschedule)


// organize the dataset
order rateschedule rate_start rate_end tou season  dow_num hour minute ///
  offpeak partpeak peak demandcharge maxdemandcharge energycharge customercharge metercharge
  
drop averagetotalrate  

rename demandcharge5kw demandcharge
rename energychargekwh energycharge
rename pdp1 pdpcharge
rename pdp2 pdpcredit


label variable rateschedule "rate name"
label variable rate_start_date "rate period start date"
label variable rate_end_date "rate period end date"
label variable tou "tou rate? (1/0)"
label variable season "summer vs winter? summer: may-oct, winter = nov-apr"
label variable dow_num "day of week (following stata dow numbering)"
lab define dowl 0 "sunday" 1 "monday" 2 "tuesday" 3 "wednesday" ///
  4 "thursday" 5 "friday" 6 "saturday" 
label values dow_num dowl

label variable hour "hour of day"
label variable minute "minute of day"
label variable offpeak "is off peak? (1/0)"
label variable partpeak "is partial peak? (1/0)"
label variable peak "is peak? 1: yes 2: choice 0: no"

lab define peakl 0 "no" 1 "yes" 2 "choice"
label values peak peakl

label variable demandcharge "demand charge ($/kW)"
label variable maxdemandcharge "base demand charge ($/kW)"
label variable energycharge "energy charge ($/kWh)"
label variable customercharge "customer charge ($/day)"
label variable metercharge "meter charge ($/day)"
label variable pdpcharge "peak day pricing charge ($/kwh during event hours)"
label variable pdpcredit "peak day pricing credit ($/kw)"

drop timeofuseperiod
duplicates drop

save "$dirpath_data_temp/cleaned/CLEANED_lgag_`dates'.dta", replace
}
******** APPEND
clear
set obs 1

cd "$dirpath_data_temp/cleaned"
local files: dir . files "*.dta"
foreach f in `files' {
 append using "`f'"
}

  
