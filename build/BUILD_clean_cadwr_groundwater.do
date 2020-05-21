clear all
version 13
set more off

**************************************************************
**** Script to import and clean CA DWR groundwater data ******
**************************************************************

** 2017 data downloaded at http://wdl.water.ca.gov/waterdatalibrary/groundwater/index.cfm 
** (top link on the page: https://d3.water.ca.gov/owncloud/index.php/s/smQyUOe4wkxwkNr)

** 2020 data downloaded at:
** https://data.ca.gov/dataset/continuous-groundwater-level-measurements
** https://data.ca.gov/dataset/periodic-groundwater-level-measurements

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

*******************************************************************************
*******************************************************************************

** 1. Main groundwater level (GWL) dataset
if 1==0{

** Import GWL file (2017 pull)
insheet using "$dirpath_data/groundwater/Statewide_GWL_Data_20170905/gwl_file.csv", double comma clear
unique elevation_id
assert r(unique)==r(N)

** Append GWL file from 2020 pull
preserve
insheet using "$dirpath_data/groundwater/periodic_gwl_bulkdatadownload_20200513/measurements.csv", double comma clear
rename stn_id casgem_station_id
rename wlm_id elevation_id
rename msmt_date measurement_date
rename wlm_rpe rp_elevation
rename wlm_gse gs_elevation
rename rdng_ws ws_reading
rename rdng_rp rp_reading
rename wlm_qa_desc measurement_issue_id
rename wlm_desc measurement_method_id
rename wlm_acc_desc measurement_accuracy_id
rename wlm_org_id org_id
rename wlm_org_name org_name
rename msmt_cmt comments
rename * *2020
rename elevation_id2020 elevation_id
unique elevation_id
assert r(unique)==r(N)
tempfile raw2020
save `raw2020'
restore
merge 1:1 elevation_id using `raw2020' 

** Clean and label identifiers and dates
la var casgem_station_id "Station identifier"
assert casgem_station_id==casgem_station_id2020 if _merge==3
assert casgem_station_id!=. if _merge==1 | _merge==3
replace casgem_station_id = casgem_station_id2020 if _merge==2
assert casgem_station_id!=.
drop casgem_station_id2020
tostring casgem_station_id, replace
replace casgem_station_id = "0" + casgem_station_id if length(casgem_station_id)<5
assert length(casgem_station_id)==5
unique casgem_station_id // 40,363 unique stations

la var site_code "Unique station identifier (lat/lon)"
assert site_code==site_code2020 if _merge==3
assert site_code!="" if _merge==1 | _merge==3
replace site_code = site_code2020 if _merge==2
assert site_code!=""
drop site_code2020
assert length(site_code)==18
egen temp_group1 = group(casgem_station_id)
egen temp_group2 = group(casgem_station_id site_code)
assert temp_group1==temp_group2 // redundant as an identifier, BUT is also lat/lon, so i'll keep it for now
drop temp_group1 temp_group2 

la var elevation_id "Record identifier (unique for GWL observations)"
unique elevation_id
assert r(unique)==r(N) // this is unique, and very weirdly named
assert elevation_id!=.

gen date = date(word(measurement_date,1),"MDY")
gen date2020 = date(word(measurement_date2020,1),"YMD")
format %td date date2020
br if date!=date2020 & _merge==3
replace date = date2020 if date!=date2020 & _merge==3 & elevation_id==1875174
assert date==date2020 if _merge==3
assert date!=. if _merge==1 | _merge==3
replace date = date2020 if _merge==2
assert date!=.
drop date2020 measurement_date measurement_date2020
order date, after(elevation_id)
la var date "Measurement date"
gen year = year(date)
la var year "Measurement year"
order year, after(date)
tab year _merge
hist date if inrange(year,2000,2018) & _merge==2
egen temp_min = min(year), by(site_code)
egen temp_max = max(year), by(site_code)
tab temp_min
tab temp_max // 44% of all sites continue thru 2019
hist date if year>2016 // a bit of a dropoff after september 2019
drop temp*
egen temp_min = min(_merge==3), by(site_code)
egen temp_max = max(_merge==3), by(site_code)
tab temp* if _merge==2 //43% of new 2020 observations are from sites we previously had
tab temp* if _merge==2 & year<2017 // for pre-2017 new observations, 87% are from new (to us) sites
drop temp*

** Clean, label, and process surface and groundwater measurements
la var rp_elevation "Reference point elevation (feet above sea level)"
br casgem_station_id elevation_id date year rp_elevation* if rp_elevation!=rp_elevation2020 & _merge==3
gen temp = rp_elevation - rp_elevation2020
sum temp, detail // vast majority of discrepancies are rounding error
sum temp if temp!=0, detail
egen temp2 = sd(temp), by(casgem_station_id)
sum temp2, detail
sum temp2 if temp!=0, detail
egen temp_sd2017 = sd(rp_elevation), by(casgem_station_id)
egen temp_sd2020 = sd(rp_elevation2020), by(casgem_station_id)
sum temp_sd2017, detail
sum temp_sd2020, detail
sum temp if (temp_sd2017>2 | temp_sd2020>2) & temp!=0, detail 
sum temp if (temp_sd2017<=2 & temp_sd2020<=2) & temp!=0, detail
	// tl;dr -- there are discrepancies, so i'm carrying forward the 2020 number where the differ
assert rp_elevation!=. if _merge==1 | _merge==3
replace rp_elevation = rp_elevation2020 if _merge==2
assert rp_elevation!=.
gen discrep_2017_2020 = _merge==3 & abs(temp)>=1
count if discrep_2017_2020==1 // 23,344 with discrepancies to carry forward
drop temp*
sum rp_elevation, detail

la var gs_elevation "Ground surface elevation (ground feet above sea level)"
br casgem_station_id elevation_id date year gs_elevation* if gs_elevation!=gs_elevation2020 & _merge==3
gen temp = gs_elevation - gs_elevation2020
sum temp, detail // vast majority of discrepancies are rounding error
sum temp if temp!=0, detail
egen temp2 = sd(temp), by(casgem_station_id)
sum temp2, detail
sum temp2 if temp!=0, detail
count if gs_elevation==. & (_merge==1 | _merge==3) // 46 missings
count if gs_elevation2020==. & (_merge==2 | _merge==3) // 46 missings
replace gs_elevation = gs_elevation2020 if _merge==2
replace discrep_2017_2020 = 1 if _merge==3 & abs(temp)>=1 & temp!=. // 170 new discrepancies
count if discrep_2017_2020==1 // 23,514 with discrepancies to carry forward
sum gs_elevation, detail
correlate rp_elevation gs_elevation
gen temp3 = gs_elevation-rp_elevation
sum temp3, detail // very close for almost all observations
drop temp*

la var rp_reading "Reference point reading (water feet below surface)"
br casgem_station_id elevation_id date year rp_reading* if rp_reading!=rp_reading2020 & _merge==3
gen temp = rp_reading - rp_reading2020
sum temp, detail // vast majority of discrepancies are rounding error
sum temp if temp!=0, detail
count if rp_reading==. & (_merge==1 | _merge==3) // 150,780 missings
count if rp_reading2020==. & (_merge==2 | _merge==3) // 164,645 missings
replace rp_reading = rp_reading2020 if _merge==2
replace discrep_2017_2020 = 1 if _merge==3 & abs(temp)>=1 & temp!=. // 342 new discrepancies
count if discrep_2017_2020==1 // 23,856 with discrepancies to carry forward
sum rp_reading, detail
drop temp*

la var ws_reading "Water surface reading (correction to water feet below surface)"
br casgem_station_id elevation_id date year ws_reading* if ws_reading!=ws_reading2020 & _merge==3
gen temp = ws_reading - ws_reading2020
sum temp, detail // vast majority of discrepancies are rounding error
sum temp if temp!=0, detail
count if ws_reading==. & (_merge==1 | _merge==3) // 149,107 missings
count if ws_reading2020==. & (_merge==2 | _merge==3) // 162,739 missings
replace ws_reading = ws_reading2020 if _merge==2
replace discrep_2017_2020 = 1 if _merge==3 & abs(temp)>=1 & temp!=. // 13 new discrepancies
count if discrep_2017_2020==1 // 23,869 with discrepancies to carry forward
sum ws_reading, detail // mostly zeros
drop temp*

	// These variables are TERRIBLY labeled in the README document, but I think I've
	// finally decoded what they mean:
	
	// "rp_reading" is the length of the tape that they drop down the well, from the 
	// reference point elevation ("rp_elevation", which is a bit above the surface elvation)
	// to the water surface. bigger "rp_reading" <==> lower water table
	
	// "ws_reading" is 99% missing/zero, and i've deduced (by comparing individual readings
	// for wells where it's non-missing/non-zero) that this is a correction factor. 
	// 86% of non-missing/non-zero "ws_reading" values are for the steel-tape measurement
	// method, which represnts only 5% of overall depth readings since 2005
	
	// "ws_reading" DGP: someone drops a tape from the reference point into the well, 
	// and measures the length of tape dropped in. but, they might drop the tape too far!
	// "rp_reading" is the full length of the tape dropped , and "ws_reading" is what 
	// you need to subtract off that full tape length to get to the length of tape that 
	// would have exactly hit the water
	
	// make better labels to reflect what these variables actually mean
la var rp_reading "Reference point reading (water depth from reference point)"
la var ws_reading "Correction to subtract from reference point reading (feet)"

	// make water depth correction
sum rpe_wse2020, detail	
sum rpe_wse2020 if ws_reading2020!=. & ws_reading2020!=0, detail	
gen rp_ws_depth = rp_reading
replace rp_ws_depth = rp_ws_depth - ws_reading if ws_reading!=. & ws_reading!=0
la var rp_ws_depth "Water depth (feet) below reference point, corrected"
gen temp = rp_ws_depth - rpe_wse2020
sum temp if _merge==2, detail // bang on!
sum temp if _merge!=2, detail // mostly bang on!
sum temp if _merge!=2 & discrep_2017_2020==0, detail // same
sum temp if _merge!=2 & discrep_2017_2020==1, detail // same
replace discrep_2017_2020 = 1 if _merge==3 & abs(temp)>=1 & temp!=. // 0 new discrepancies
count if discrep_2017_2020==1 // 23,869 with discrepancies to carry forward
drop temp*
rename rpe_wse2020 rp_ws_depth2020

	// water depth below surface
gen gs_ws_depth = rp_ws_depth - (rp_elevation-gs_elevation)	
la var gs_ws_depth "Water depth (feet) below ground surface, corrected"
gen temp = gs_ws_depth - gse_wse2020
sum temp if _merge==2, detail // bang on!
sum temp if _merge!=2, detail // mostly bang on!
sum temp if _merge!=2 & discrep_2017_2020==0, detail // same
sum temp if _merge!=2 & discrep_2017_2020==1, detail // same
replace discrep_2017_2020 = 1 if _merge==3 & abs(temp)>=1.04 & temp!=. // 0 new discrepancies
count if discrep_2017_2020==1 // 23,869 with discrepancies to carry forward
drop temp*
rename gse_wse2020 gs_ws_depth2020

	// water surface elevation w/r/t sea level
gen ws_elevation = gs_elevation - gs_ws_depth
la var ws_elevation "Water surface elevation (feet) above sea level"	
gen temp = ws_elevation - wse2020
sum temp if _merge==2, detail // bang on!
sum temp if _merge!=2, detail // mostly bang on!
sum temp if _merge!=2 & discrep_2017_2020==0, detail // same
sum temp if _merge!=2 & discrep_2017_2020==1, detail // same
replace discrep_2017_2020 = 1 if _merge==3 & abs(temp)>=1.06 & temp!=. // 0 new discrepancies
count if discrep_2017_2020==1 // 23,869 with discrepancies to carry forward
drop temp*
rename wse2020 ws_elevation2020

	// reorder variables
order rp_elevation gs_elevation rp_reading ws_reading rp_ws_depth gs_ws_depth ///
	ws_elevation, after(year)
br	

	// assess crazy outliers
count if gs_ws_depth<0 & year>=2005 
local rN = r(N)
count if year>=2005
di `rN'/r(N) // 0.026% of observations have negative water depth
count if gs_ws_depth<0 & year>=2005
count if gs_ws_depth<0 & year>=2005 & _merge==2 // 80% of these are new obseravtions
count if gs_ws_depth<0 & year>=2005 & _merge==2 & inrange(year,2011,2014) // mostly b/tw 2011-2014
count if year>=2005 & _merge==2 & inrange(year,2011,2014) // 30% of new 2011-2014 observations have negative depth!
count if gs_ws_depth<0 & year>=2005 & _merge==3 & gs_ws_depth2020>=0 // 98 2020 pulls can fix a negative depth observation
br if gs_ws_depth<0	
br if gs_ws_depth<0	& year>=2005 & measurement_issue_id==. & measurement_issue_id2020==""
gen neg_depth = gs_ws_depth<0
egen temp = max(neg_depth) if year>=2005, by(site_code)
egen neg_depth_ever = mean(temp), by(site_code)
la var neg_depth "Readings report water surface ABOVE ground surface"
la var neg_depth_ever "Site where water is ever (post-2005) reported to have negative depth"
drop temp

** Clean and label remaining variables
la var measurement_issue_id "Measurement problem code"
tab measurement_issue_id if _merge!=2, missing // 17% populated, 26 unique issues!
tab measurement_issue_id2020 if _merge!=1, missing // 16% populated, 24 unique issues!

la var measurement_method_id "Measurement method code"
tab measurement_method_id if _merge!=2, missing // 90% populated, 7 unique methods
tab measurement_method_id2020 if _merge!=1, missing // 91% populated, 7 unique methods

la var measurement_accuracy_id "Measurement accuracy code"
tab measurement_accuracy_id if _merge!=2, missing // 90% populated, 5 unique accuracy codes
tab measurement_accuracy_id2020 if _merge!=1, missing // 91% populated, 5 unique methods

la var casgem_reading "Reading is a casgem submittal"
tab casgem_reading, missing

la var org_id "Monitoring agency code"
assert org_id==org_id2020 if _merge==3
replace org_id = org_id2020 if _merge==2
assert org_id!=.

la var org_name "Monitoring agency name"
br year org_name org_name2020 if _merge==3 & org_name!=org_name2020
count if _merge==3 & org_name!=org_name2020 // 2994 discrepancies, but all obviously just minor string variations
unique org_id2020 if _merge==3 & org_name!=org_name2020 //... from only 3 orgs
replace org_name2020 = "Indian Wells Valley Cooperative Groundwater Management Group" if ///
	org_name=="Indian Wells Valley Cooperative Groundwater Management Group" & org_id==5037 & ///
	org_name2020=="Indian Wells Valley Groundwater Authority (CASGEM)" & org_id2020==5037
replace org_name2020 = "Siskiyou County Public Health and Community Development" if ///
	org_name=="Siskiyou County Public Health and Community Development" & org_id==5109 & ///
	org_name2020=="Siskiyou County Natural Resource Department" & org_id2020==5109
replace org_name2020 = "Stanislaus & Tuolumne Rivers Groundwate Basin Association" if ///
	org_name=="Stanislaus & Tuolumne Rivers Groundwate Basin Association" & org_id==1335 & ///
	org_name2020=="Stanislaus & Tuolumne Rivers Groundwater Basin Association GSA" & org_id2020==1335
assert org_name==org_name2020 & org_id==org_id if _merge==3
replace org_name = org_name2020 if _merge==2
tab org_id, missing
tab org_name, missing
egen temp_group1 = group(org_id)
egen temp_group2 = group(org_id org_name)
egen temp_tag = tag(org_id org_name)
sort temp_group1
br org_id org_name temp* if temp_tag // need to fix 3 names
replace org_name = "Stanislaus & Tuolumne Rivers Groundwater Basin Association (GSA)" if org_id==1335
replace org_name = "Indian Wells Valley Cooperative Groundwater Management Group" if org_id==5037
replace org_name = "Siskiyou County Natural Resource Department" if org_id==5109
egen temp_group3 = group(org_id)
egen temp_group4 = group(org_id org_name)
assert temp_group3==temp_group4 // confirms that agency names are clean!
assert org_id!=. & org_name!=""
drop temp* org_id2020 org_name2020

la var comments "Measurement Remarks"
count if comments=="" // hooboy this field is a doozie
br comments*
count if comments==comments2020 & _merge==3
count if comments!=comments2020 & _merge==3
replace comments = comments2020 if _merge==2
drop comments2020

la var coop_agency_org_id "Cooperating Agency Code"
la var coop_org_name "Cooperating Agency Name"
count if coop_agency_org_id!=coop_agency_org_id2020 & _merge==3
count if coop_org_name!=coop_org_name2020 & _merge==3
	// clean enough for a field we probably won't ever use
replace coop_agency_org_id = coop_agency_org_id2020 if _merge==2
replace coop_org_name = coop_org_name2020 if _merge==2
tab coop_agency_org_id, missing
tab coop_org_name, missing
egen temp_group1 = group(org_id)
egen temp_group2 = group(org_id org_name)
assert temp_group1==temp_group2 // confirms that agency names are clean!
assert coop_agency_org_id!=. & coop_org_name!=""
rename coop_agency_org_id coop_org_id
drop temp* coop_agency_org_id2020 coop_org_name2020
rename _merge _merge2020

** Merge in measurement accuracy descriptions 
preserve
insheet using "$dirpath_data/groundwater/Statewide_GWL_Data_20170905/elevation_accuracy_type.csv", double comma clear
keep elevation_accuracy_type_id elevation_accuracy_cd
rename elevation_accuracy_type_id measurement_accuracy_id
rename elevation_accuracy_cd measurement_accuracy_desc
tempfile macc
save `macc'
restore
merge m:1 measurement_accuracy_id using `macc', keep(1 3)
assert (_merge==3 | measurement_accuracy_id==.) if _merge2020!=2
drop _merge
la var measurement_accuracy_desc "Measurement accuracy description"
order measurement_accuracy_desc, after(measurement_accuracy_id)
egen temp = tag(measurement_accuracy_id measurement_accuracy_desc measurement_accuracy_id2020), missing
sort measurement_accuracy_id measurement_accuracy_desc measurement_accuracy_id2020
br measurement_accuracy_id measurement_accuracy_desc measurement_accuracy_id2020 _merge2020 if temp
replace measurement_accuracy_id2020 = "Unknown" if measurement_accuracy_id2020=="Water level accuracy is unknown"
replace measurement_accuracy_id2020 = "1 Ft" if measurement_accuracy_id2020=="Water level accuracy to nearest foot"
replace measurement_accuracy_id2020 = "0.1 Ft" if measurement_accuracy_id2020=="Water level accuracy to nearest tenth of a foot"
replace measurement_accuracy_id2020 = "0.01 Ft" if measurement_accuracy_id2020=="Water level accuracy to nearest hundredth of a foot"
replace measurement_accuracy_id2020 = "0.001 Ft" if measurement_accuracy_id2020=="Water level accuracy to nearest thousandth of a foot"
count if measurement_accuracy_desc!=measurement_accuracy_id2020 & _merge==3 // only 98 discrepancies
replace discrep_2017_2020 = 1 if measurement_accuracy_desc!=measurement_accuracy_id2020 & _merge==3 // 41 new discrepancies
replace measurement_accuracy_desc = measurement_accuracy_id2020 if _merge==2
egen temp2 = mode(measurement_accuracy_id), by(measurement_accuracy_desc)
assert temp2!=. if measurement_accuracy_desc!=""
replace measurement_accuracy_id = temp2 if measurement_accuracy_id==. & measurement_accuracy_desc!=""
egen temp_group1 = group(measurement_accuracy_id), missing
egen temp_group2 = group(measurement_accuracy_id measurement_accuracy_desc), missing
assert temp_group1==temp_group2 // confirms that agency names are clean!
drop temp*

** Merge in measurement method descriptions 
preserve
insheet using "$dirpath_data/groundwater/Statewide_GWL_Data_20170905/elevation_measure_method_type.csv", double comma clear
keep elev_measure_method_type_id elev_measure_method_desc
rename elev_measure_method_type_id measurement_method_id
rename elev_measure_method_desc measurement_method_desc
tempfile mmeth
save `mmeth'
restore
merge m:1 measurement_method_id using `mmeth', keep(1 3)
assert (_merge==3 | measurement_method_id==.) if _merge2020!=2
drop _merge
la var measurement_method_desc "Measurement method description"
order measurement_method_desc, after(measurement_method_id)
egen temp = tag(measurement_method_id measurement_method_desc measurement_method_id2020), missing
sort measurement_method_id measurement_method_desc measurement_method_id2020
br measurement_method_id measurement_method_desc measurement_method_id2020 _merge2020 if temp
	// the strings match up, thankfully
count if measurement_method_desc!=measurement_method_id2020 & _merge==3 // only 89 discrepancies, which we don't really care about
count if measurement_method_desc=="" & measurement_method_id2020!="" & _merge==3 // 10 of these are missings to populate
replace measurement_method_desc = measurement_method_id2020 if _merge==2 | (_merge==3 & measurement_method_desc=="")
egen temp2 = mode(measurement_method_id), by(measurement_method_desc)
assert temp2!=. if measurement_method_desc!=""
replace measurement_method_id = temp2 if measurement_method_id==. & measurement_method_desc!=""
egen temp_group1 = group(measurement_method_id), missing
egen temp_group2 = group(measurement_method_id measurement_method_desc), missing
assert temp_group1==temp_group2 // confirms that agency names are clean!
drop temp* measurement_method_id2020

** Merge in measurement issue descriptions
preserve
insheet using "$dirpath_data/groundwater/Statewide_GWL_Data_20170905/measurement_issue_type.csv", double comma clear
keep measurement_issue_type_id measurement_issue_type_desc measurement_issue_type_class
rename measurement_issue_type_id measurement_issue_id
rename measurement_issue_type_desc measurement_issue_desc
rename measurement_issue_type_class measurement_issue_class
tempfile miss
save `miss'
restore
merge m:1 measurement_issue_id using `miss', keep(1 3)
assert (_merge==3 | measurement_issue_id==.) | _merge2020==2
drop _merge
egen temp = tag(measurement_issue_id measurement_issue_desc measurement_issue_id2020), missing
sort measurement_issue_id measurement_issue_desc measurement_issue_id2020
br measurement_issue_id measurement_issue_desc measurement_issue_id2020 measurement_issue_class _merge2020 if temp
	// the strings match up, thankfully
count if measurement_issue_desc!=measurement_issue_id2020 & _merge==3 // only 245 discrepancies, which we'll come back to
count if measurement_issue_desc=="" & measurement_issue_id2020!="" & _merge==3 // 179 of these are missings to populate
replace measurement_issue_desc = measurement_issue_id2020 if _merge==2 | (_merge==3 & measurement_issue_desc=="")
egen temp2 = mode(measurement_issue_id), by(measurement_issue_desc)
assert temp2!=. if measurement_issue_desc!=""
replace measurement_issue_id = temp2 if measurement_issue_id==. & measurement_issue_desc!=""
egen temp_group1 = group(measurement_issue_id), missing
egen temp_group2 = group(measurement_issue_id measurement_issue_desc), missing
assert temp_group1==temp_group2 // confirms that agency names are clean!
rename measurement_issue_id2020 measurement_issue_desc2020
tab measurement_issue_id measurement_issue_class if _merge==3, missing 
replace measurement_issue_class = "Q" if inrange(measurement_issue_id,1,14)
replace measurement_issue_class = "N" if inrange(measurement_issue_id,15,26)
	// two measurement issues are duplicated for Q vs N levels
tab measurement_issue_class if measurement_issue_desc=="Pumping" & year<2005 & _merge==2
tab measurement_issue_class if measurement_issue_desc=="Pumping" & measurement_issue_desc2020=="Pumping" & year<2005
	// 2/3 of "Pumping" are N
tab measurement_issue_class if measurement_issue_desc=="Casing leaking or wet" & year<2005 & _merge==3
tab measurement_issue_class if measurement_issue_desc=="Casing leaking or wet" & measurement_issue_desc2020=="Casing leaking or wet" & year<2005
	// 2/3 of "Casing leaking or wet" are Q
preserve
keep measurement_issue_id measurement_issue_desc measurement_issue_class
duplicates drop
drop if measurement_issue_id==.
count
rename measurement_issue_id measurement_issue_id2020
rename measurement_issue_desc measurement_issue_desc2020
rename measurement_issue_class measurement_issue_class2020
sort measurement_issue_id
list
*drop if measurement_issue_desc2020=="Pumping" & measurement_issue_class2020=="Q"
*drop if measurement_issue_desc2020=="Casing leaking or wet" & measurement_issue_class2020=="N"
tempfile miss2020
save `miss2020'
restore
joinby measurement_issue_desc2020 using `miss2020', unmatched(master)
duplicates t elevation_id, gen(dup)
tab measurement_issue_id2020 dup, missing
unique elevation_id
local uniq = r(unique)
	// keep the Qs iff reading is nonmissing
drop if dup>0 & measurement_issue_id2020==2 & rp_reading==.
drop if dup>0 & measurement_issue_id2020==16 & rp_reading!=.
drop if dup>0 & measurement_issue_id2020==4 & rp_reading==.
drop if dup>0 & measurement_issue_id2020==23 & rp_reading!=.
unique elevation_id
assert r(unique)==`uniq'
assert r(unique)==r(N)
assert measurement_issue_class2020=="Q" if inlist(measurement_issue_id2020,1,14)
assert measurement_issue_class2020=="N" if inlist(measurement_issue_id2020,15,26)
sort measurement_issue_id
	// fix misassigned for the two ambiguous ones
gen temp3 = measurement_issue_id==4 & measurement_issue_id2020==23 & rp_reading==. 
gen temp4 = measurement_issue_id==16 & measurement_issue_id2020==2 & rp_reading!=. 
replace measurement_issue_id = 23 if temp3==1
replace measurement_issue_class = "N" if temp3==1
replace measurement_issue_id = 2 if temp4==1
replace measurement_issue_class = "Q" if temp4==1
br elevation_id year measurement_issue_id* measurement_issue_class* _merge2020 discrep if measurement_issue_id!=measurement_issue_id2020 & _merge==3
	// amazingly a non-issue, hardly any meaningful discrepancies!
br elevation_id year measurement_issue_id* measurement_issue_class* _merge2020 discrep if measurement_issue_class!=measurement_issue_class2020 & _merge==3
	// exactly one discrepancy! amazing, and after all that i'm just dropping these 
assert measurement_issue_id!=. if measurement_issue_id2020!=.	
assert measurement_issue_desc!="" if measurement_issue_desc2020!=""
assert measurement_issue_class!="" if measurement_issue_class2020!=""	
drop temp* measurement_issue_id2020 measurement_issue_desc2020 measurement_issue_class2020	
assert inlist(measurement_issue_class,"Q","N","")
	// deal with missings and "N" flags
tab year _merge2020 if rp_reading==. & measurement_issue_class!="N"
tab year _merge2020 if rp_reading==. & measurement_issue_class!="N" & measurement_issue_desc=="" // only 81
tab measurement_issue_desc _merge2020 if rp_reading==. & measurement_issue_class!="N", missing
br rp_reading *issue* _merge2020 if rp_reading==. & measurement_issue_class!="N"
replace measurement_issue_class = "N" if measurement_issue_class=="" & rp_reading==.
replace measurement_issue_id = 22 if measurement_issue_desc=="Other" & _merge2020==2 & rp_reading==.
replace measurement_issue_class = "N" if measurement_issue_desc=="Other" & _merge2020==2 & rp_reading==.
replace measurement_issue_desc = "Special/Other" if measurement_issue_id==22 & _merge2020==2 & rp_reading==.
replace measurement_issue_id = 26 if measurement_issue_desc=="Flowing" & _merge2020==2 & rp_reading==.
replace measurement_issue_class = "N" if measurement_issue_desc=="Flowing" & _merge2020==2 & rp_reading==.
replace measurement_issue_desc = "Flowing artesian well" if measurement_issue_id==26 & _merge2020==2 & rp_reading==.
assert rp_reading!=. if measurement_issue_class!="N"
la var measurement_issue_id "Measurement issue code"
la var measurement_issue_desc "Measurement issue description"
la var measurement_issue_class "Q = questionable, N = no measurement"
order measurement_issue_desc measurement_issue_class, after(measurement_issue_id)
drop comments // lots of overlap with issue descriptions
drop _merge dup

** Extract lat/lon from site_code
gen lat = substr(site_code,1,6)
gen lon = substr(site_code,8,7)
destring lat lon, replace
replace lat = lat/10000
replace lon = -lon/10000
la var lat "Site latitude (extracted from site_code)"
la var lon "Site longitude (extracted from site_code)"

** Drop duplicates with multiple records, but otherwise identical
order elevation_id
sort casgem_station_id-lon
duplicates t casgem_station_id-lon, gen(dup)
tab dup
br if dup>10
duplicates drop casgem_station_id-lon, force
drop dup

** Average dups with multiple readings on the same date, but otherwise identical
duplicates t casgem_station_id-year measurement_issue_id-coop_org_name _merge2020 discrep_2017_2020 lon, gen(dup)
tab dup
tab dup discrep_2017_2020 // discrepancies not an issue with this dedupification
br if dup>0 & discrep_2017_2020==1
foreach v of varlist rp_elevation-ws_elevation {
	egen double temp = mean(`v'), by(casgem_station_id-year measurement_issue_id-coop_org_name _merge2020 discrep_2017_2020 lon)
	replace `v' = temp if dup>0
	drop temp
}
duplicates drop casgem_station_id-year measurement_issue_id-coop_org_name _merge2020 discrep_2017_2020 lon, force
drop dup

** Drop dups with multiple readings on the same date, where one reading is missing/questionable
duplicates t casgem_station_id site_code date discrep_2017_2020, gen(dup)
tab dup 
tab dup discrep_2017_2020 // only 138 dups have discrepancies
unique casgem_station_id site_code date discrep_2017_2020
local uniq = r(unique)
egen temp_minN = min(measurement_issue_class=="N"), by(casgem_station_id site_code date discrep_2017_2020)
egen temp_maxN = max(measurement_issue_class=="N"), by(casgem_station_id site_code date discrep_2017_2020)
drop if dup>0 & temp_minN<temp_maxN & measurement_issue_class=="N"
egen temp_minQ = min(measurement_issue_class=="Q"), by(casgem_station_id site_code date discrep_2017_2020)
egen temp_maxQ = max(measurement_issue_class=="Q"), by(casgem_station_id site_code date discrep_2017_2020)
drop if dup>0 & temp_minQ<temp_maxQ & measurement_issue_class=="Q"
unique casgem_station_id site_code date discrep_2017_2020
assert r(unique)==`uniq'
drop dup temp*

** Drop dups with multiple readings on the same date, where one reading's method is unknown
duplicates t casgem_station_id site_code date discrep_2017_2020, gen(dup)
tab dup
tab dup discrep_2017_2020 // 130 dups have discrepancies
unique casgem_station_id site_code date discrep_2017_2020
local uniq = r(unique)
egen temp_min = min(measurement_method_desc=="Unknown"), by(casgem_station_id site_code date discrep_2017_2020)
egen temp_max = max(measurement_method_desc=="Unknown"), by(casgem_station_id site_code date discrep_2017_2020)
drop if dup>0 & temp_min<temp_max & measurement_method_desc=="Unknown"
unique casgem_station_id site_code date discrep_2017_2020
assert r(unique)==`uniq'
drop dup temp*

** Drop dups with multiple readings on the same date, where one reading's accuracy is worse 
duplicates t casgem_station_id site_code date discrep_2017_2020, gen(dup)
tab dup
tab dup discrep_2017_2020 // 130 dups have discrepancies
tab dup measurement_accuracy_desc, missing
tab dup measurement_accuracy_desc if year>2005, missing
unique casgem_station_id site_code date discrep_2017_2020
local uniq = r(unique)
egen temp_min = min(measurement_accuracy_desc=="Unknown"), by(casgem_station_id site_code date discrep_2017_2020)
egen temp_max = max(measurement_accuracy_desc=="Unknown"), by(casgem_station_id site_code date discrep_2017_2020)
drop if dup>0 & temp_min<temp_max & measurement_accuracy_desc=="Unknown"
unique casgem_station_id site_code date discrep_2017_2020
assert r(unique)==`uniq'
drop dup temp*

** Drop dups with multiple readings on the same date, where one reading's accuracy is listed as more accurate
duplicates t casgem_station_id site_code date discrep_2017_2020, gen(dup)
tab dup
tab dup discrep_2017_2020 // 130 dups have discrepancies
unique casgem_station_id site_code date discrep_2017_2020
local uniq = r(unique)
egen double temp_min = min(real(word(measurement_accuracy_desc,1))), by(casgem_station_id site_code date discrep_2017_2020)
egen double temp_max = max(real(word(measurement_accuracy_desc,1))), by(casgem_station_id site_code date discrep_2017_2020)
drop if dup>0 & temp_min<temp_max & real(word(measurement_accuracy_desc,1))==temp_max
unique casgem_station_id site_code date discrep_2017_2020
assert r(unique)==`uniq'
drop dup temp*

** Drop dups where one dup has a 2017-to-2020 discrepancy
duplicates t casgem_station_id site_code date, gen(dup)
tab dup
tab dup discrep_2017_2020
duplicates t casgem_station_id site_code date measurement_issue_class, gen(dup2)
tab dup dup2
br if dup>0 & dup2!=dup
unique casgem_station_id site_code date
local uniq = r(unique)
gen temp = 1
replace temp = 2 if measurement_issue_class=="Q"
replace temp = 3 if measurement_issue_class=="N"
egen double temp_minC = min(temp), by(casgem_station_id site_code date)
egen double temp_maxC = max(temp), by(casgem_station_id site_code date)
egen temp_min = min(discrep_2017_2020), by(casgem_station_id site_code date)
egen temp_max = max(discrep_2017_2020), by(casgem_station_id site_code date)
drop if dup>0 & temp_minC<temp_maxC & temp>temp_minC
drop if dup>0 & temp_minC==temp_maxC & temp_min<temp_max & discrep_2017_2020==1
unique casgem_station_id site_code date
assert r(unique)==`uniq'
drop dup* temp*
egen temp_min = min(discrep_2017_2020), by(casgem_station_id site_code date)
egen temp_max = max(discrep_2017_2020), by(casgem_station_id site_code date)
assert temp_min==temp_max
drop temp*

** Drop dups where dates overlap, and one dup is only in the 2020 data pull
duplicates t casgem_station_id site_code date, gen(dup)
tab year dup
sum date if _merge2020==3
di %td r(max)
unique casgem_station_id site_code date
local uniq = r(unique)
egen double temp_min = min(_merge2020), by(casgem_station_id site_code date)
egen double temp_max = max(_merge2020), by(casgem_station_id site_code date)
tab _merge2020 if temp_min<temp_max
drop if dup>0 & temp_min<temp_max & temp_max==3 & _merge2020<3
unique casgem_station_id site_code date
assert r(unique)==`uniq'
drop dup* temp*

** Evaluate dups where dates overlap (lots of _merge<3s)
duplicates t casgem_station_id site_code date, gen(dup)
tab year dup
egen double temp_min = min(_merge2020), by(casgem_station_id site_code date)
egen double temp_max = max(_merge2020), by(casgem_station_id site_code date)
br if dup>0 & temp_min<temp_max
	// seems best ot just average these
drop dup temp*

** Average reading of remaining dups
duplicates t casgem_station_id site_code date measurement_issue_class discrep_2017_2020, gen(dup)
tab year dup
unique casgem_station_id site_code date measurement_issue_class discrep_2017_2020
local uniq = r(unique)
foreach v of varlist rp_elevation-ws_elevation {
	egen double temp = mean(`v'), by(casgem_station_id site_code date measurement_issue_class discrep_2017_2020)
	replace `v' = temp if dup>0
	drop temp
}
duplicates drop casgem_station_id date measurement_issue_class discrep_2017_2020, force
unique casgem_station_id site_code date measurement_issue_class discrep_2017_2020
assert r(unique)==`uniq'
drop dup

** Confirm uniqueness
unique casgem_station_id date
assert r(unique)==r(N)

** Clean up
tab year discrep_2017_2020
tab measurement_issue_class discrep_2017_2020 if year>2005, missing
la var discrep_2017_2020 "Flag for discrepancies between 2017 and 2020 data pulls"
drop rp_elevation2020 gs_elevation2020 ws_reading2020 rp_reading2020 ws_elevation2020 rp_ws_depth2020 gs_ws_depth2020 ///
	measurement_accuracy_id2020 _merge2020

** Save
sort casgem_station_id date
compress
save "$dirpath_data/groundwater/ca_dwr_gwl.dta", replace

}

*******************************************************************************
*******************************************************************************

** 2. Main groundwater station (GST) dataset
if 1==0{

** Import GST file (2017 pull)
insheet using "$dirpath_data/groundwater/Statewide_GWL_Data_20170905/gst_file.csv", double comma clear
unique casgem_station_id
assert r(unique)==r(N)

** Append GST file from 2020 pull
preserve
insheet using "$dirpath_data/groundwater/periodic_gwl_bulkdatadownload_20200513/stations.csv", double comma clear
rename stn_id casgem_station_id
rename site_code site_code2020
rename swn state_well_number2020
rename well_name local_well_designation2020
rename latitude latitude2020
rename longitude longitude2020
rename wlm_method loc_method2020
rename wlm_acc loc_accuracy2020
rename basin_code basin_cd2020
rename basin_name basin_desc2020
rename county_name county_name2020
rename well_depth total_depth_ft2020
rename well_use casgem_station_use_desc2020
rename well_type well_type2020
rename wcr_no completion_rpt_nbr2020
unique casgem_station_id
assert r(unique)==r(N)
tempfile raw2020
save `raw2020'
restore
merge 1:1 casgem_station_id using `raw2020' 
assert _merge!=1

** Clean and label
la var casgem_station_id "Station identifier"
tostring casgem_station_id, replace
replace casgem_station_id = "0" + casgem_station_id if length(casgem_station_id)<5
assert length(casgem_station_id)==5
assert casgem_station_id!=""
unique casgem_station_id // 39,317 unique stations

la var site_code "Unique station identifier (lat/lon)"
assert site_code==site_code2020 if _merge==3
replace site_code = site_code2020 if _merge==2
assert length(site_code)==18
egen temp_group1 = group(casgem_station_id)
egen temp_group2 = group(casgem_station_id site_code)
assert temp_group1==temp_group2 // redundant as an identifier, BUT is also lat/lon, so i'll keep it for now
drop temp_group1 temp_group2 
assert site_code!=""
drop site_code2020

la var state_well_number "State well number"
la var local_well_designation "Identifier used by local agency"
assert (length(state_well_number)==13) | ///
	(state_well_number=="" & local_well_designation!="") if _merge==3
replace local_well_designation = subinstr(local_well_designation,state_well_number,"",1) if _merge==3
assert real(substr(state_well_number,1,1))!=. if length(state_well_number)==13
replace local_well_designation2020 = subinstr(local_well_designation2020,state_well_number2020,"",1)
br if state_well_number=="" & length(local_well_designation)==13
replace state_well_number = local_well_designation if state_well_number=="" & _merge==3 & length(local_well_designation)==13 ///
	& strpos(local_well_designation," ")==0 & strpos(local_well_designation,"-")==0 & real(substr(local_well_designation,1,1))!=.
replace local_well_designation = subinstr(local_well_designation,state_well_number,"",1) if _merge==3
replace state_well_number2020 = local_well_designation2020 if state_well_number2020=="" & length(local_well_designation2020)==13 ///
	& strpos(local_well_designation2020," ")==0 & strpos(local_well_designation2020,"-")==0 & real(substr(local_well_designation2020,1,1))!=.
replace local_well_designation2020 = subinstr(local_well_designation2020,state_well_number2020,"",1)
br state_well_number state_well_number2020 local_well_designation if state_well_number!=state_well_number2020 & _merge==3
replace state_well_number = state_well_number2020 if state_well_number=="" & length(state_well_number2020)>=12 & ///
	real(substr(state_well_number2020,1,1))!=.
replace local_well_designation = local_well_designation2020 if local_well_designation==""
br if (length(state_well_number)!=13) & !(state_well_number=="" & local_well_designation!="")
replace local_well_designation = subinstr(local_well_designation,state_well_number,"",1)
count if state_well_number=="" & local_well_designation==""
br state_well_number* local_well_designation if local_well_designation!="" & state_well_number==""
replace state_well_number2020 = "" if state_well_number2020==state_well_number
replace local_well_designation2020 = "" if local_well_designation2020==local_well_designation

la var latitude "Station latitude"
la var longitude "Station longitude"
replace latitude = latitude2020 if _merge==2
replace longitude = longitude2020 if _merge==2
twoway scatter latitude longitude, msize(vtiny) aspect(1.6)
sum latitude, detail
sum longitude, detail
assert latitude!=. & longitude!=.
br site_code latitude* longitude* if abs(latitude-latitude2020)>0.001 // only a handful
br site_code latitude* longitude* if abs(longitude-longitude2020)>0.001 // only a handful
	// resolve discrepancies by using lat/lon embedded in site_code
gen lat = substr(site_code,1,6)
gen lon = substr(site_code,8,7)
destring lat lon, replace
replace lat = lat/10000
replace lon = -lon/10000
gen temp1 = _merge==3 & abs(latitude-lat)>abs(latitude2020-lat) & abs(latitude-latitude2020)>0.001
gen temp2 = _merge==3 & abs(longitude-lon)>abs(longitude2020-lon) & abs(longitude-longitude2020)>0.001
replace latitude = latitude2020 if temp1==1 & temp2==1
replace longitude = longitude2020 if temp1==1 & temp2==1
drop lat lon latitude2020 longitude2020 temp*

la var loc_method "Method by which well was located"
tab loc_method
tab loc_method2020
replace loc_method = loc_method2020 if loc_method==""
tab loc_method loc_method2020 if loc_method!=loc_method2020, missing
drop loc_method2020

la var loc_accuracy "Accuracy of well location"
tab loc_accuracy
tab loc_accuracy2020
replace loc_accuracy = subinstr(loc_accuracy,"ft.","ft",1)
replace loc_accuracy2020 = subinstr(loc_accuracy2020,"ft.","ft",1)
tab loc_accuracy loc_accuracy2020
replace loc_accuracy = loc_accuracy2020 if loc_accuracy=="Unknown"  
drop loc_accuracy2020

la var basin_cd "Groundwater basin code of well"
la var basin_desc "Groundwater basin name of well"
replace basin_cd = subinstr(subinstr(basin_cd,"-0","-",1),"-0","-",1) // remove leading zeros for merge
replace basin_cd2020 = subinstr(subinstr(basin_cd2020,"-0","-",1),"-0","-",1) // remove leading zeros for merge
count if basin_cd!=basin_cd2020 & _merge==3 & basin_cd!="" & basin_cd2020!=""
tab basin_cd basin_cd2020 if  basin_cd!=basin_cd2020 & _merge==3 & basin_cd!="" & basin_cd2020!=""
replace basin_cd = basin_cd2020 if basin_cd==""
count if basin_desc!=basin_desc2020 & _merge==3 & basin_desc!="" & basin_desc2020!=""
replace basin_desc = basin_desc2020 if basin_desc==""
br if basin_cd=="" | basin_desc==""
rename basin_desc basin_name
drop basin_cd2020 basin_desc2020

la var is_voluntary_reporting "Code (Y/N) indicating reporting status"
tab is_voluntary_reporting, missing

la var total_depth_ft "Total depth of well (if public)"
sum total_depth_ft, detail
sum total_depth_ft2020, detail
correlate total_depth_ft total_depth_ft2020 if _merge==3 & !abs(total_depth_ft-total_depth_ft2020)<1 // rho = 0.78
replace total_depth_ft = total_depth_ft2020 if _merge==2
replace total_depth_ft = total_depth_ft2020 if _merge==3 & total_depth_ft==.
count if total_depth_ft==. // 32207 missings out of 43723
drop total_depth_ft2020

rename casgem_station_use_desc well_use_desc
la var well_use_desc "Reported use of well"
tab well_use_desc, missing
tab casgem_station_use_desc2020, missing
tab well_use_desc casgem_station_use_desc2020 if _merge==3, missing // very few discrepancies
replace well_use_desc = casgem_station_use_desc2020 if _merge==2
replace well_use_desc = casgem_station_use_desc2020 if _merge==3 & well_use_desc==""
drop casgem_station_use_desc2020

la var completion_rpt_nbr "Well completion report number"
count if completion_rpt_nbr!="" & _merge==3
count if completion_rpt_nbr!=completion_rpt_nbr2020 & _merge==3 // only 100 discrepancies
replace completion_rpt_nbr = completion_rpt_nbr2020 if _merge==2
replace completion_rpt_nbr = completion_rpt_nbr2020 if _merge==3 & completion_rpt_nbr==""
drop completion_rpt_nbr2020

la var county_name "Station county"
count if county_name!=county_name2020 & _merge==3
tab county_name county_name2020 if county_name!=county_name2020 & _merge==3
replace county_name = county_name2020 if _merge==2
assert county_name!=""
order county_name, after (longitude)
drop county_name2020
rename _merge _merge2020

** Merge in basin region
local nobs = _N
	// read in basin code-to-region xwalk
preserve
insheet using "$dirpath_data/groundwater/Statewide_GWL_Data_20170905/qryGWBasins.csv", double comma clear
unique basin_cd
assert r(unique)==r(N)
tab basin_cd basin_region_id
tempfile basins
save `basins'
restore	
	// merge
merge m:1 basin_cd using `basins', gen(_merge)
tab basin_cd _merge if _merge==1
	// for non-merges, take the modal basin_region_id
gen temp_prefix = substr(basin_cd,1,4)
egen temp_min = min(basin_region_id), by(temp_prefix)
egen temp_max = max(basin_region_id), by(temp_prefix)
egen temp_mode = mode(basin_region_id), by(temp_prefix)
replace basin_region_id = temp_mode if temp_min==temp_max
	// manually assign remainig  missings
replace basin_region_id = 3 if basin_region_id==. & basin_cd=="3-2.01"
replace basin_region_id = 7 if basin_region_id==. & basin_cd=="5-22.17"
replace basin_region_id = 4 if basin_region_id==. & basin_cd=="8-4.01"
replace basin_region_id = 4 if basin_region_id==. & basin_cd=="8-4.02"
replace basin_region_id = 7 if basin_region_id==. & basin_cd=="5-22.18"
replace basin_region_id = 6 if basin_region_id==. & basin_cd=="5-22.19"
replace basin_region_id = 4 if basin_region_id==. & basin_cd=="9-7.01"
replace basin_region_id = 4 if basin_region_id==. & basin_cd=="9-7.02"
replace basin_region_id = 4 if basin_region_id==. & basin_cd=="9-33"
count if basin_region_id==.
assert basin_cd=="" & basin_name=="" if basin_region_id==.
drop if _merge==2
drop _merge basin_id basin_desc temp*
assert _N==`nobs'
	// read in basin region names
preserve
insheet using "$dirpath_data/groundwater/Statewide_GWL_Data_20170905/BASIN_REGION.csv", double comma clear
unique basin_region_id
assert r(unique)==r(N)
keep basin_region_id basin_region_desc
tempfile basinsR
save `basinsR'
restore	
	// merge
merge m:1 basin_region_id using `basinsR', gen(_merge)
assert _merge==3 | basin_region_id==.
drop _merge
	// label
la var basin_region_id "Groundwater basin region code"
la var basin_region_desc "Groundwater basin region description"
order basin_region*, after(basin_name)
	// plot
twoway ///
	(scatter latitude longitude if basin_region_id==0, mcolor(red)) ///
	(scatter latitude longitude if basin_region_id==0, mcolor(blue)) ///
	(scatter latitude longitude if basin_region_id==0, mcolor(green)) ///
	(scatter latitude longitude if basin_region_id==0, mcolor(orange)) ///
	(scatter latitude longitude if basin_region_id==0, mcolor(cyan)) ///
	(scatter latitude longitude if basin_region_id==0, mcolor(gold)) ///
	(scatter latitude longitude if basin_region_id==0, mcolor(maroon)) ///
	(scatter latitude longitude if basin_region_id==0, mcolor(black)) ///
	(scatter latitude longitude if basin_region_id==0, mcolor(gs8)) ///
	(scatter latitude longitude if basin_region_id==0, mcolor(mint)) ///
	(scatter latitude longitude if basin_region_id==1, msize(tiny) aspect(1.3) mcolor(red)) ///
	(scatter latitude longitude if basin_region_id==2, msize(tiny) mcolor(blue)) ///
	(scatter latitude longitude if basin_region_id==3, msize(tiny) mcolor(green)) ///
	(scatter latitude longitude if basin_region_id==4, msize(tiny) mcolor(orange)) ///
	(scatter latitude longitude if basin_region_id==5, msize(tiny) mcolor(cyan)) ///
	(scatter latitude longitude if basin_region_id==6, msize(tiny) mcolor(gold)) ///
	(scatter latitude longitude if basin_region_id==7, msize(tiny) mcolor(maroon)) ///
	(scatter latitude longitude if basin_region_id==8, msize(tiny) mcolor(black)) ///
	(scatter latitude longitude if basin_region_id==9, msize(tiny) mcolor(gs8)) ///
	(scatter latitude longitude if basin_region_id==10, msize(tiny) mcolor(mint)) ///
	, legend(pos(3) c(1) order(1 "North Coast" 2 "San Francisco Bay" 3 "Central Coast" ///
	4 "South Coast" 5 "Sacramento River" 6 "San Joaquin River" 7 "Tulare Lake" ///
	8 "North Lahontan" 9 "South Lahontan" 10 "Colorado River") size(small)) ///
	ylabel(, labsize(small) angle(0) nogrid) xlabel(, labsize(small)) ///
	xtitle("Longitude", size(small)) ytitle("Latitude", size(small)) ///
	title("Groundwater stations by region", size(medium) color(black)) ///
	graphregion(lcolor(white) fcolor(white) lstyle(none)) plotregion(fcolor(white) lcolor(white))
	
** True-up basin names with basin names from shapefile
split basin_cd , gen(tempA) parse("-")
split tempA2 , gen(tempB) parse(".")
replace tempB1 = "0" + tempB1 if length(tempB1)<3
replace tempB1 = "0" + tempB1 if length(tempB1)<3
replace basin_cd = tempA1 + "-" + tempB1
replace basin_cd = basin_cd + "." + tempB2 if tempB2!=""
drop temp*
rename basin_cd basin_sub_id
rename basin_name basin_sub_name
joinby basin_sub_id using "$dirpath_data/groundwater/ca_water_basins.dta", unmatched(master)
tab _merge // only 35 unmatched stations
tab basin_name if _merge==1 // only 4 unmatched, and 3 were already flagged as such
drop _merge basin_object_id basin_reg_off
la var basin_sub_id "Groundwater sub-basin identifier"
la var basin_sub_name "Groundwater sub-basin name"
order basin_sub_id basin_sub_name basin_sub_area_sqmi basin_id basin_name, after(loc_accuracy)
replace basin_sub_id = "" if basin_sub_id=="-00"
	
** Extract lat/lon from site_code
gen lat = substr(site_code,1,6)
gen lon = substr(site_code,8,7)
destring lat lon, replace
replace lat = lat/10000
replace lon = -lon/10000
gen temp_lat = abs(latitude-lat)
gen temp_lon = abs(longitude-lon)
sum temp_lat temp_lon, detail // the VAST majority are identical, save significant figures
drop temp* lat lon

** Well type
tab well_type
assert well_type!=""
la var well_type2020 "Well type"
rename well_type2020 well_type

** Clean up
br _merge state_well_number* local_well_designation* if state_well_number2020!="" | local_well_designation2020!=""
// not much information left in these 2020 variables
drop state_well_number2020 local_well_designation2020 _merge2020

** Create lat/lon group, to reduce the number of observations to rasterize
egen latlon_group = group(latitude longitude)
la var latlon_group	"Group by lat/lon, because multiple stations have identical lat/lon"
unique latlon_group
	
** Save
sort casgem_station_id
unique casgem_station_id
assert r(unique)==r(N)
compress
save "$dirpath_data/groundwater/ca_dwr_gst.dta", replace
}

*******************************************************************************
*******************************************************************************

** 3. Merge groundwater datasets (GWL and GST), and save working dataset of all wells
if 1==0{

** Execute merge
use "$dirpath_data/groundwater/ca_dwr_gwl.dta", clear
merge m:1 casgem_station_id site_code using "$dirpath_data/groundwater/ca_dwr_gst.dta"
assert _merge!=1 // all GWL wells merge into GST dataset
egen temp_tag = tag(casgem_station_id site_code)
tab _merge if temp_tag // 92% of wells in GST data match into GWL data
drop if _merge==2 

** Drop variables we won't use for the next steps
drop elevation_id casgem_reading org_id org_name coop_org_id coop_org_name state_well_number ///
	local_well_designation is_voluntary_reporting completion_rpt_nbr well_type 

** Confirm uniqueness
unique casgem_station_id date
assert r(unique)==r(N)
	
** Save
compress
save "$dirpath_data/groundwater/ca_dwr_gwl_merged.dta", replace

}

*******************************************************************************
*******************************************************************************

** 4. Construct monthly/quarterly panels of average groundwater depth by basin/sub-basin
if 1==1{

** Start with merged DWR dataset
use "$dirpath_data/groundwater/ca_dwr_gwl_merged.dta", clear

** Drop years prior to our sample
drop if year<2008

** Drop observations with nonmissing "No measurement" readings
drop if measurement_issue_class=="N"

** Create month and quarter varables
gen modate = ym(year(date), month(date))
format %tm modate
gen month = month(date)
gen quarter = 1 if inlist(month(date),1,2,3)
replace quarter = 2 if inlist(month(date),4,5,6)
replace quarter = 3 if inlist(month(date),7,8,9)
replace quarter = 4 if inlist(month(date),10,11,12)
gen qtr = yq(year(date),quarter)
format %tq qtr

** Flag questionable measurements
gen QUES = 0
replace QUES = 1 if measurement_issue_class!="" // issues flagged as either Questionable or No measurement
replace QUES = 1 if neg_depth==1 // negative depth
	// other potential refinements:
	//	- length/consistency of well's time series (for simple averages, not sure this is necessary)
	// 	- method of measurement (if some are particularly bad??)
	//  - location accuracy (but most seem pretty accurate)
	//	- discriminate based on type of measurement issue
	//  - flag 2017-2020 discrepancies as questionable? the correlation between the "1" and "2" averages (constructed below) 
	//    are all over 0.98 with vs. without discrepancies being flagged as questionable, so it's probably fine to leave them in

** Mean/sd by basin/month, including all DWR measurements
egen double gw_mth_bsn_mean1 = mean(gs_ws_depth), by(basin_id modate)
egen double gw_mth_bsn_sd1 = sd(gs_ws_depth), by(basin_id modate)
gen gw_mth_bsn_cnt1 = gs_ws_depth!=.

** Mean/sd by basin/month, excluding questionable measurements
egen double temp1 = mean(gs_ws_depth) if QUES==0, by(basin_id modate)
egen double temp2 = sd(gs_ws_depth) if QUES==0, by(basin_id modate)
egen double gw_mth_bsn_mean2 = mean(temp1), by(basin_id modate)
egen double gw_mth_bsn_sd2 = mean(temp2), by(basin_id modate)
drop temp*
gen gw_mth_bsn_cnt2 = gs_ws_depth!=. & QUES==0

** Mean/sd by basin/month, excluding questionable measurements and non-observational wells, without 2017-2020 discrepancies
egen double temp1 = mean(gs_ws_depth) if QUES==0 & well_use_desc=="Observation" & discrep_2017_2020==0, by(basin_id modate)
egen double temp2 = sd(gs_ws_depth) if QUES==0 & well_use_desc=="Observation" & discrep_2017_2020==0, by(basin_id modate)
egen double gw_mth_bsn_mean3 = mean(temp1), by(basin_id modate)
egen double gw_mth_bsn_sd3 = mean(temp2), by(basin_id modate)
drop temp*
gen gw_mth_bsn_cnt3 = gs_ws_depth!=. & QUES==0 & well_use_desc=="Observation" & discrep_2017_2020==0

** Mean/sd by sub-basin/month, including all DWR measurements
egen double gw_mth_sub_mean1 = mean(gs_ws_depth), by(basin_sub_id modate)
egen double gw_mth_sub_sd1 = sd(gs_ws_depth), by(basin_sub_id modate)
gen gw_mth_sub_cnt1 = gs_ws_depth!=.

** Mean/sd by sub-basin/month, excluding questionable measurements
egen double temp1 = mean(gs_ws_depth) if QUES==0, by(basin_sub_id modate)
egen double temp2 = sd(gs_ws_depth) if QUES==0, by(basin_sub_id modate)
egen double gw_mth_sub_mean2 = mean(temp1), by(basin_sub_id modate)
egen double gw_mth_sub_sd2 = mean(temp2), by(basin_sub_id modate)
drop temp*
gen gw_mth_sub_cnt2 = gs_ws_depth!=. & QUES==0

** Mean/sd by sub-basin/month, excluding questionable measurements and non-observational wells, without 2017-2020 discrepancies
egen double temp1 = mean(gs_ws_depth) if QUES==0 & well_use_desc=="Observation" & discrep_2017_2020==0, by(basin_sub_id modate)
egen double temp2 = sd(gs_ws_depth) if QUES==0 & well_use_desc=="Observation" & discrep_2017_2020==0, by(basin_sub_id modate)
egen double gw_mth_sub_mean3 = mean(temp1), by(basin_sub_id modate)
egen double gw_mth_sub_sd3 = mean(temp2), by(basin_sub_id modate)
drop temp*
gen gw_mth_sub_cnt3 = gs_ws_depth!=. & QUES==0 & well_use_desc=="Observation" & discrep_2017_2020==0

** Mean/sd by basin/quarter, including all DWR measurements
egen double gw_qtr_bsn_mean1 = mean(gs_ws_depth), by(basin_id qtr)
egen double gw_qtr_bsn_sd1 = sd(gs_ws_depth), by(basin_id qtr)
gen gw_qtr_bsn_cnt1 = gs_ws_depth!=.

** Mean/sd by basin/quarter, excluding questionable measurements
egen double temp1 = mean(gs_ws_depth) if QUES==0, by(basin_id qtr)
egen double temp2 = sd(gs_ws_depth) if QUES==0, by(basin_id qtr)
egen double gw_qtr_bsn_mean2 = mean(temp1), by(basin_id qtr)
egen double gw_qtr_bsn_sd2 = mean(temp2), by(basin_id qtr)
drop temp*
gen gw_qtr_bsn_cnt2 = gs_ws_depth!=. & QUES==0

** Mean/sd by basin/quarter, excluding questionable measurements and non-observational wells, without 2017-2020 discrepancies
egen double temp1 = mean(gs_ws_depth) if QUES==0 & well_use_desc=="Observation" & discrep_2017_2020==0, by(basin_id qtr)
egen double temp2 = sd(gs_ws_depth) if QUES==0 & well_use_desc=="Observation" & discrep_2017_2020==0, by(basin_id qtr)
egen double gw_qtr_bsn_mean3 = mean(temp1), by(basin_id qtr)
egen double gw_qtr_bsn_sd3 = mean(temp2), by(basin_id qtr)
drop temp*
gen gw_qtr_bsn_cnt3 = gs_ws_depth!=. & QUES==0 & well_use_desc=="Observation" & discrep_2017_2020==0

** Mean/sd by sub-basin/quarter, including all DWR measurements
egen double gw_qtr_sub_mean1 = mean(gs_ws_depth), by(basin_sub_id qtr)
egen double gw_qtr_sub_sd1 = sd(gs_ws_depth), by(basin_sub_id qtr)
gen gw_qtr_sub_cnt1 = gs_ws_depth!=.

** Mean/sd by sub-basin/quarter, excluding questionable measurements
egen double temp1 = mean(gs_ws_depth) if QUES==0, by(basin_sub_id qtr)
egen double temp2 = sd(gs_ws_depth) if QUES==0, by(basin_sub_id qtr)
egen double gw_qtr_sub_mean2 = mean(temp1), by(basin_sub_id qtr)
egen double gw_qtr_sub_sd2 = mean(temp2), by(basin_sub_id qtr)
drop temp*
gen gw_qtr_sub_cnt2 = gs_ws_depth!=. & QUES==0

** Mean/sd by sub-basin/quarter, excluding questionable measurements and non-observational wells, without 2017-2020 discrepancies
egen double temp1 = mean(gs_ws_depth) if QUES==0 & well_use_desc=="Observation" & discrep_2017_2020==0, by(basin_sub_id qtr)
egen double temp2 = sd(gs_ws_depth) if QUES==0 & well_use_desc=="Observation" & discrep_2017_2020==0, by(basin_sub_id qtr)
egen double gw_qtr_sub_mean3 = mean(temp1), by(basin_sub_id qtr)
egen double gw_qtr_sub_sd3 = mean(temp2), by(basin_sub_id qtr)
drop temp*
gen gw_qtr_sub_cnt3 = gs_ws_depth!=. & QUES==0 & well_use_desc=="Observation" & discrep_2017_2020==0

** Labels
la var gw_mth_bsn_mean1 "Depth (mean ft), basin/month, all measurements"	
la var gw_mth_bsn_sd1 "Depth (sd ft), basin/month, all measurements"	
la var gw_mth_bsn_mean2 "Depth (mean ft), basin/month, non-questionable measurements"	
la var gw_mth_bsn_sd2 "Depth (sd ft), basin/month, non-questionable measurements"	
la var gw_mth_bsn_mean3 "Depth (mean ft), basin/month, observational non-questionable measurements"	
la var gw_mth_bsn_sd3 "Depth (sd ft), basin/month, observational non-questionable measurements"	
la var gw_mth_sub_mean1 "Depth (mean ft), sub-basin/month, all measurements"	
la var gw_mth_sub_sd1 "Depth (sd ft), sub-basin/month, all measurements"	
la var gw_mth_sub_mean2 "Depth (mean ft), sub-basin/month, non-questionable measurements"	
la var gw_mth_sub_sd2 "Depth (sd ft), sub-basin/month, non-questionable measurements"	
la var gw_mth_sub_mean3 "Depth (mean ft), sub-basin/month, observational non-questionable measurements"	
la var gw_mth_sub_sd3 "Depth (sd ft), sub-basin/month, observational non-questionable measurements"	
la var gw_qtr_bsn_mean1 "Depth (mean ft), basin/quarter, all measurements"	
la var gw_qtr_bsn_sd1 "Depth (sd ft), basin/quarter, all measurements"	
la var gw_qtr_bsn_mean2 "Depth (mean ft), basin/quarter, non-questionable measurements"	
la var gw_qtr_bsn_sd2 "Depth (sd ft), basin/quarter, non-questionable measurements"	
la var gw_qtr_bsn_mean3 "Depth (mean ft), basin/quarter, observational non-questionable measurements"	
la var gw_qtr_bsn_sd3 "Depth (sd ft), basin/quarter, observational non-questionable measurements"	
la var gw_qtr_sub_mean1 "Depth (mean ft), sub-basin/quarter, all measurements"	
la var gw_qtr_sub_sd1 "Depth (sd ft), sub-basin/quarter, all measurements"	
la var gw_qtr_sub_mean2 "Depth (mean ft), sub-basin/quarter, non-questionable measurements"	
la var gw_qtr_sub_sd2 "Depth (sd ft), sub-basin/quarter, non-questionable measurements"	
la var gw_qtr_sub_mean3 "Depth (mean ft), sub-basin/quarter, observational non-questionable measurements"	
la var gw_qtr_sub_sd3 "Depth (sd ft), sub-basin/quarter, observational non-questionable measurements"	
la var modate "Year-Month"
la var month "Month"
la var qtr "Year-Quarter"
la var quarter "Quarter"

** Save basin/month panel
preserve
drop if basin_id=="" | modate==.
collapse (sum) gw_mth_bsn_cnt?, by(modate year month basin_id basin_name  ///
	gw_mth_bsn_mean? gw_mth_bsn_sd?) fast
unique basin_id modate
assert r(unique)==r(N)
la var gw_mth_bsn_cnt1 "Number of basin/month measurements (all)"	
la var gw_mth_bsn_cnt2 "Number of basin/month measurements (non-questionable)"	
la var gw_mth_bsn_cnt3 "Number of basin/month measurements (non-questionable, observation wells)"	
order basin_id basin_name year month modate *1 *2 *3
sort basin_id modate
compress
save "$dirpath_data/groundwater/avg_groundwater_depth_basin_month.dta", replace
restore

** Save sub-basin/month panel
preserve
drop if basin_sub_id=="" | basin_id=="" | modate==.
collapse (sum) gw_mth_sub_cnt?, by(modate year month basin_id basin_name  ///
	basin_sub_id basin_sub_name gw_mth_sub_mean? gw_mth_sub_sd?) fast
unique basin_sub_id modate
assert r(unique)==r(N)
la var gw_mth_sub_cnt1 "Number of sub-basin/month measurements (all)"	
la var gw_mth_sub_cnt2 "Number of sub-basin/month measurements (non-questionable)"	
la var gw_mth_sub_cnt3 "Number of sub-basin/month measurements (non-questionable, observation wells)"	
order basin_id basin_name basin_sub_id basin_sub_name year month modate *1 *2 *3
sort basin_id basin_sub_id modate
compress
save "$dirpath_data/groundwater/avg_groundwater_depth_subbasin_month.dta", replace
restore

** Save basin/quarter panel
preserve
drop if basin_id=="" | qtr==.
collapse (sum) gw_qtr_bsn_cnt?, by(qtr year quarter basin_id basin_name  ///
	gw_qtr_bsn_mean? gw_qtr_bsn_sd?) fast
unique basin_id qtr
assert r(unique)==r(N)
la var gw_qtr_bsn_cnt1 "Number of basin/quarter measurements (all)"	
la var gw_qtr_bsn_cnt2 "Number of basin/quarter measurements (non-questionable)"	
la var gw_qtr_bsn_cnt3 "Number of basin/quarter measurements (non-questionable, observation wells)"	
order basin_id basin_name year quarter qtr *1 *2 *3
sort basin_id qtr
compress
save "$dirpath_data/groundwater/avg_groundwater_depth_basin_quarter.dta", replace
restore

** Save sub-basin/quarter panel
preserve
drop if basin_sub_id=="" | basin_id=="" | qtr==.
collapse (sum) gw_qtr_sub_cnt?, by(qtr year quarter basin_id basin_name  ///
	basin_sub_id basin_sub_name gw_qtr_sub_mean? gw_qtr_sub_sd?) fast
unique basin_sub_id qtr
assert r(unique)==r(N)
la var gw_qtr_sub_cnt1 "Number of sub-basin/quarter measurements (all)"	
la var gw_qtr_sub_cnt2 "Number of sub-basin/quarter measurements (non-questionable)"	
la var gw_qtr_sub_cnt3 "Number of sub-basin/quarter measurements (non-questionable, observation wells)"	
order basin_id basin_name basin_sub_id basin_sub_name year quarter qtr *1 *2 *3
sort basin_id basin_sub_id qtr
compress
save "$dirpath_data/groundwater/avg_groundwater_depth_subbasin_quarter.dta", replace
restore

** Diagnostics: coverage by basin/quarter
use "$dirpath_data/pge_cleaned/sp_premise_gis.dta", clear
gen N_sps = 1
collapse (sum) N_sps, by(basin_id basin_name) fast
unique basin_id
assert r(unique)==r(N)
sum N_sps if N_sps>1000
local rsum = r(sum)
sum N_sps
di `rsum'/r(sum) // 93% of SPs are in a basin with at least 1000 other SPs
merge 1:m basin_id using "$dirpath_data/groundwater/avg_groundwater_depth_basin_quarter.dta"
tab _merge
sum N_sps if _merge==1
di r(sum) // 4429 SPs are in basins that don't merge into DWR data
drop if _merge==2
tabstat N_sps, by(_merge) s(sum)
keep if _merge==3
gen N_qtrs1 = gw_qtr_bsn_mean1!=.
gen N_qtrs2 = gw_qtr_bsn_mean2!=.
gen N_qtrs3 = gw_qtr_bsn_mean3!=.
collapse (sum) N_qtrs?, by(N_sps basin_id basin_name) fast
sum N_sps if N_qtrs1>30
local rsum = r(sum)
sum N_sps
di `rsum'/r(sum) // 90% of SPs are in basins with at least 30 quarters of readings

** Diagnostics: coverage by sub-basin/quarter
use "$dirpath_data/pge_cleaned/sp_premise_gis.dta", clear
gen N_sps = 1
collapse (sum) N_sps, by(basin_id basin_name basin_sub_id basin_sub_name) fast
unique basin_sub_id
assert r(unique)==r(N)
sum N_sps if N_sps>1000
local rsum = r(sum)
sum N_sps
di `rsum'/r(sum) // 85% of SPs are in a sub-basin with at least 1000 other SPs
merge 1:m basin_sub_id using "$dirpath_data/groundwater/avg_groundwater_depth_subbasin_quarter.dta"
tab _merge
sum N_sps if _merge==1
di r(sum) // 5063 SPs are in sub-basins that don't merge into DWR data
drop if _merge==2
tabstat N_sps, by(_merge) s(sum)
keep if _merge==3
gen N_qtrs1 = gw_qtr_sub_mean1!=.
gen N_qtrs2 = gw_qtr_sub_mean2!=.
gen N_qtrs3 = gw_qtr_sub_mean3!=.
collapse (sum) N_qtrs?, by(N_sps basin_id basin_name basin_sub_id basin_sub_name) fast
sum N_sps if N_qtrs1>30
local rsum = r(sum)
sum N_sps
di `rsum'/r(sum) // 73% of SPs are in basins with at least 30 quarters of readings

** Fill out basin/month panel
use "$dirpath_data/groundwater/avg_groundwater_depth_basin_month.dta", clear
egen temp = group(basin_id basin_name)
tsset temp modate
tsfill
foreach v of varlist basin_id basin_name {
	replace `v' = `v'[_n-1] if temp==temp[_n-1] & mi(`v')
	assert !mi(`v')
}
replace year = real(substr(string(modate,"%tm"),1,4)) if year==.
replace month = real(substr(string(modate,"%tm"),6,2)) if month==.
foreach v of varlist gw_mth_bsn_mean? {
	by temp: ipolate `v' modate, gen(temp1)
	replace `v' = temp1 if `v'==.
	drop temp1
}
drop *_sd? *_cnt? temp
drop if gw_mth_bsn_mean1==. & gw_mth_bsn_mean2==. & gw_mth_bsn_mean3==. 
unique basin_id modate
assert r(unique)==r(N)
compress
save "$dirpath_data/groundwater/avg_groundwater_depth_basin_month_full.dta", replace

** Fill out basin/quarter panel
use "$dirpath_data/groundwater/avg_groundwater_depth_basin_quarter.dta", clear
egen temp = group(basin_id basin_name)
tsset temp qtr
tsfill
foreach v of varlist basin_id basin_name {
	replace `v' = `v'[_n-1] if temp==temp[_n-1] & mi(`v')
	assert !mi(`v')
}
replace year = real(substr(string(qtr,"%tq"),1,4)) if year==.
replace quarter = real(substr(string(qtr,"%tq"),6,1)) if quarter==.
foreach v of varlist gw_qtr_bsn_mean? {
	by temp: ipolate `v' qtr, gen(temp1)
	replace `v' = temp1 if `v'==.
	drop temp1
}
drop *_sd? *_cnt? temp
drop if gw_qtr_bsn_mean1==. & gw_qtr_bsn_mean2==. & gw_qtr_bsn_mean3==. 
unique basin_id qtr
assert r(unique)==r(N)
compress
save "$dirpath_data/groundwater/avg_groundwater_depth_basin_quarter_full.dta", replace

** Fill out sub-basin/month panel
use "$dirpath_data/groundwater/avg_groundwater_depth_subbasin_month.dta", clear
egen temp = group(basin_id basin_name basin_sub_id basin_sub_name)
tsset temp modate
tsfill
foreach v of varlist basin_id basin_name basin_sub_id basin_sub_name {
	replace `v' = `v'[_n-1] if temp==temp[_n-1] & mi(`v')
	assert !mi(`v')
}
replace year = real(substr(string(modate,"%tm"),1,4)) if year==.
replace month = real(substr(string(modate,"%tm"),6,2)) if month==.
foreach v of varlist gw_mth_sub_mean? {
	by temp: ipolate `v' modate, gen(temp1)
	replace `v' = temp1 if `v'==.
	drop temp1
}
drop *_sd? *_cnt? temp
drop if gw_mth_sub_mean1==. & gw_mth_sub_mean2==. & gw_mth_sub_mean3==. 
unique basin_id basin_sub_id modate
assert r(unique)==r(N)
compress
save "$dirpath_data/groundwater/avg_groundwater_depth_subbasin_month_full.dta", replace

** Fill out sub-basin/quarter panel
use "$dirpath_data/groundwater/avg_groundwater_depth_subbasin_quarter.dta", clear
egen temp = group(basin_id basin_name basin_sub_id basin_sub_name)
tsset temp qtr
tsfill
foreach v of varlist basin_id basin_name basin_sub_id basin_sub_name {
	replace `v' = `v'[_n-1] if temp==temp[_n-1] & mi(`v')
	assert !mi(`v')
}
replace year = real(substr(string(qtr,"%tq"),1,4)) if year==.
replace quarter = real(substr(string(qtr,"%tq"),6,1)) if quarter==.
foreach v of varlist gw_qtr_sub_mean? {
	by temp: ipolate `v' qtr, gen(temp1)
	replace `v' = temp1 if `v'==.
	drop temp1
}
drop *_sd? *_cnt? temp
drop if gw_qtr_sub_mean1==. & gw_qtr_sub_mean2==. & gw_qtr_sub_mean3==. 
unique basin_id basin_sub_id qtr
assert r(unique)==r(N)
compress
save "$dirpath_data/groundwater/avg_groundwater_depth_subbasin_quarter_full.dta", replace

}

*******************************************************************************
*******************************************************************************

** 5. Rasterize panels of groundwater depth
if 1==1{

** Start with merged DWR dataset
use "$dirpath_data/groundwater/ca_dwr_gwl_merged.dta", clear

** Drop years prior to our sample
drop if year<2008

** Create month and quarter varables
gen modate = ym(year(date), month(date))
format %tm modate
gen month = month(date)
gen quarter = 1 if inlist(month(date),1,2,3)
replace quarter = 2 if inlist(month(date),4,5,6)
replace quarter = 3 if inlist(month(date),7,8,9)
replace quarter = 4 if inlist(month(date),10,11,12)
gen qtr = yq(year(date),quarter)
format %tq qtr

** Flag questionable measurements
gen QUES = 0
replace QUES = 1 if measurement_issue_class!="" // issues flagged as either Questionable or No measurement
replace QUES = 1 if neg_depth==1 // negative depth

** Collapse to the latlon-month, and export
preserve
drop if latlon_group==. | modate==.
egen gs_ws_depth_1 = mean(gs_ws_depth), by(modate latlon_group)
egen temp2 = mean(gs_ws_depth) if QUES==0, by(modate latlon_group)
egen gs_ws_depth_2 = mean(temp2), by(modate latlon_group)
egen temp3 = mean(gs_ws_depth) if QUES==0 & well_use_desc=="Observation", by(modate latlon_group)
egen gs_ws_depth_3 = mean(temp3), by(modate latlon_group)
keep latlon_group modate year month latitude longitude basin_id gs_ws_depth_?
order latlon_group modate year month latitude longitude basin_id gs_ws_depth_?
sort latlon_group modate year month latitude longitude basin_id gs_ws_depth_?
drop if gs_ws_depth_1==. & gs_ws_depth_2==. & gs_ws_depth_3==.
duplicates drop
unique latlon_group modate
assert r(unique)==r(N)

outsheet using "$dirpath_data/misc/ca_dwr_depth_latlon_month.txt", comma replace
restore

** Collapse to the station-quarter, and export
preserve
drop if latlon_group==. | qtr==.
egen gs_ws_depth_1 = mean(gs_ws_depth), by(qtr latlon_group)
egen temp2 = mean(gs_ws_depth) if QUES==0, by(qtr latlon_group)
egen gs_ws_depth_2 = mean(temp2), by(qtr latlon_group)
egen temp3 = mean(gs_ws_depth) if QUES==0 & well_use_desc=="Observation", by(qtr latlon_group)
egen gs_ws_depth_3 = mean(temp3), by(qtr latlon_group)
keep latlon_group qtr year quarter latitude longitude basin_id gs_ws_depth_?
order latlon_group qtr year quarter latitude longitude basin_id gs_ws_depth_?
sort latlon_group qtr year quarter latitude longitude basin_id gs_ws_depth_?
duplicates drop
drop if gs_ws_depth_1==. & gs_ws_depth_2==. & gs_ws_depth_3==.
unique latlon_group qtr
assert r(unique)==r(N)
outsheet using "$dirpath_data/misc/ca_dwr_depth_latlon_quarter.txt", comma replace
restore

** Run "BUILD_gis_gw_depth_raster.R" to rasterize monthly/quarterly cross-sections 
**   of groundwater depth!

** Run "BUILD_gis_gw_depth_extract.R" to extract groundwater depths from each 
**   monthly/quarterly raster, for SP lat/lons and APEP pump lat/lons!

}

*******************************************************************************
*******************************************************************************

** 6. Construct panels of groundwater depth for SPs (monthly)
if 1==1{

** 6a. Statewide rasters (ignoring basin boundaries)
{
** Read in output from GIS script to extract monthly depths from rasters
insheet using "$dirpath_data/misc/prems_gw_depths_from_rasters.csv", double comma clear

** Drop quarterly variables, and SP-specific variables
drop *_????q? prem_lat prem_long bad_geocode_flag missing_geocode_flag x y

** Destring numeric variables before reshaping, to reduce file size
foreach v of varlist depth_??_* {
	cap replace `v' = "" if `v'=="NA"
	destring `v', replace
}

** Reshape long, to convert into SP-month panel
reshape long depth_1s depth_1b depth_2s depth_2b depth_3s depth_3b ///
	distkm_1 distkm_2 distkm_3, i(sp_uuid pull) j(MODATE) string

** Reformat string variables
tostring sp_uuid pull, replace
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
assert length(sp_uuid)==10 & real(sp_uuid)!=.
gen modate = ym(real(substr(MODATE,2,4)),real(substr(MODATE,7,2)))
format %tm modate
assert string(modate,"%tm")==substr(MODATE,2,10)
drop MODATE
order sp_uuid pull modate
unique sp_uuid pull modate
assert r(unique)==r(N)
sort sp_uuid modate pull

** Make unique by SP-modate
duplicates t sp_uuid modate, gen(dup)
br if dup>0 // the vast majority have identical lat/lon across march/august pulls
unique sp_uuid modate
local uniq = r(unique)
drop if dup==1 & sp_uuid==sp_uuid[_n-1] & modate==modate[_n-1] & pull=="20180827" & ///
	pull[_n-1]=="20180322" & depth_1s==depth_1s[_n-1] & depth_1b==depth_1b[_n-1] & ///
	depth_2s==depth_2s[_n-1] & depth_2b==depth_2b[_n-1] & depth_3s==depth_3s[_n-1] & ///
	depth_3b==depth_3b[_n-1] & distkm_1==distkm_1[_n-1] & distkm_2==distkm_2[_n-1] & ///
	distkm_3==distkm_3[_n-1]
unique sp_uuid modate
assert r(unique)==`uniq'
duplicates t sp_uuid modate, gen(dup2)
br if dup2>0
unique sp_uuid if dup2>0 //23 SPs where lat/lon changes across march/august pulls
egen temp_max = max(pull=="20180322"), by(sp_uuid modate)
unique sp_uuid modate
local uniq = r(unique)
drop if temp_max==1 & pull!="20180322" // keep March pull coordinates, doesn't really matter b/c everything is very close
unique sp_uuid modate
assert r(unique)==`uniq'
assert r(unique)==r(N)
drop dup* temp*

** Merge in basin identifiers
merge m:1 sp_uuid using "$dirpath_data/pge_cleaned/sp_premise_gis.dta", keepusing(basin_id)
drop if _merge==2
assert _merge==3
drop _merge

** Merge in number of groundwater measurements in each basin/month
merge m:1 basin_id modate using "$dirpath_data/groundwater/avg_groundwater_depth_basin_month.dta", ///
	keep(1 3) keepusing(gw_mth_bsn_mean1 gw_mth_bsn_mean2 gw_mth_bsn_mean3 ///
	gw_mth_bsn_cnt1 gw_mth_bsn_cnt2 gw_mth_bsn_cnt3)
foreach v of varlist gw_mth_bsn_cnt? {
	replace `v' = 0 if _merge==1
	assert `v'!=.
}
sum gw_mth_bsn_cnt?, detail
	
** Convert kilometers to miles
foreach v of varlist distkm_? {
	replace `v' = `v'*0.621371
	local v2 = subinstr("`v'","km","_miles",1)
	rename `v' `v2'
}	
	
** Distance threshold from nearest raster point
sum dist_miles_1 if gw_mth_bsn_cnt1>1, detail
sum dist_miles_1 if gw_mth_bsn_cnt1>10, detail
sum dist_miles_1 if gw_mth_bsn_cnt1>50, detail
sum dist_miles_1 if gw_mth_bsn_cnt1>100, detail
sum dist_miles_1 if gw_mth_bsn_cnt1>500, detail
sum dist_miles_1 if gw_mth_bsn_cnt1==0, detail

** Label
la var sp_uuid "Service Point ID (anonymized, 10-digit)"
la var pull "Which PGE data pull is this SP from?"
la var modate "Year-Month"
la var depth_1s "Extracted gw depth (all measurements, simple, feet)"
la var depth_1b "Extracted gw depth (all measurements, bilinear, feet)"
la var depth_2s "Extracted gw depth (non-ques measurements, simple, feet)"
la var depth_2b "Extracted gw depth (non-ques measurements, bilinear, feet)"
la var depth_3s "Extracted gw depth (obs non-ques measurements, simple, feet)"
la var depth_3b "Extracted gw depth (obs non-ques measurements, bilinear, feet)"
la var dist_miles_1 "Miles to nearest gw measurement in raster (all)"
la var dist_miles_2 "Miles to nearest gw measurement in raster (non-ques)"
la var dist_miles_3 "Miles to nearest gw measurement in raster (obs non-ques)"
rename depth_?? gw_rast_depth_mth_??
rename dist_miles_? gw_rast_dist_mth_?
drop _merge

** Save
unique sp_uuid modate
assert r(unique)==r(N)
compress
save "$dirpath_data/groundwater/groundwater_depth_sp_month_rast.dta", replace
}

** 6b. San Joaquin Valley only rasters
{
** Read in output from GIS script to extract monthly depths from rasters
insheet using "$dirpath_data/misc/prems_gw_depths_from_rasters_SJ.csv", double comma clear

** Drop quarterly variables, and SP-specific variables
drop *_????q? prem_lat prem_long bad_geocode_flag missing_geocode_flag x y

** Destring numeric variables before reshaping, to reduce file size
foreach v of varlist depth_??_* {
	cap replace `v' = "" if `v'=="NA"
	destring `v', replace
}

** Reshape long, to convert into SP-month panel
reshape long depth_1s depth_1b depth_2s depth_2b depth_3s depth_3b ///
	distkm_sj_1 distkm_sj_2 distkm_sj_3, i(sp_uuid pull) j(MODATE) string

** Reformat string variables
tostring sp_uuid pull, replace
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
assert length(sp_uuid)==10 & real(sp_uuid)!=.
gen modate = ym(real(substr(MODATE,2,4)),real(substr(MODATE,7,2)))
format %tm modate
assert string(modate,"%tm")==substr(MODATE,2,10)
drop MODATE
order sp_uuid pull modate
unique sp_uuid pull modate
assert r(unique)==r(N)
sort sp_uuid modate pull

** Make unique by SP-modate
duplicates t sp_uuid modate, gen(dup)
br if dup>0 // the vast majority have identical lat/lon across march/august pulls
unique sp_uuid modate
local uniq = r(unique)
drop if dup==1 & sp_uuid==sp_uuid[_n-1] & modate==modate[_n-1] & pull=="20180827" & ///
	pull[_n-1]=="20180322" & depth_1s==depth_1s[_n-1] & depth_1b==depth_1b[_n-1] & ///
	depth_2s==depth_2s[_n-1] & depth_2b==depth_2b[_n-1] & depth_3s==depth_3s[_n-1] & ///
	depth_3b==depth_3b[_n-1] & distkm_sj_1==distkm_sj_1[_n-1] & distkm_sj_2==distkm_sj_2[_n-1] & ///
	distkm_sj_3==distkm_sj_3[_n-1]
unique sp_uuid modate
assert r(unique)==`uniq'
duplicates t sp_uuid modate, gen(dup2)
br if dup2>0
unique sp_uuid if dup2>0 //23 SPs where lat/lon changes across march/august pulls
egen temp_max = max(pull=="20180322"), by(sp_uuid modate)
unique sp_uuid modate
local uniq = r(unique)
drop if temp_max==1 & pull!="20180322" // keep March pull coordinates, doesn't really matter b/c everything is very close
unique sp_uuid modate
assert r(unique)==`uniq'
assert r(unique)==r(N)
drop dup* temp*

** Merge in basin identifiers
merge m:1 sp_uuid using "$dirpath_data/pge_cleaned/sp_premise_gis.dta", keepusing(basin_id)
drop if _merge==2
assert _merge==3
drop _merge

** Drop units not in the San Joaquin Valley basin
gen temp = 0
foreach v of varlist depth* {
	replace temp = 1 if `v'==.
}
egen temp_min = min(temp), by(sp_uuid)
tab basin_id temp_min
keep if basin_id=="5-022" // San Joaquin Valley
drop temp* basin_id

** Convert kilometers to miles
foreach v of varlist distkm_sj_? {
	replace `v' = `v'*0.621371
	local v2 = subinstr("`v'","km","_miles",1)
	rename `v' `v2'
}	

** Rename depth variables
foreach v of varlist depth_?? {
	local v2 = subinstr("`v'","depth_","depth_sj_",1)
	rename `v' `v2'
}
	
** Label
la var sp_uuid "Service Point ID (anonymized, 10-digit)"
la var pull "Which PGE data pull is this SP from?"
la var modate "Year-Month"
la var depth_sj_1s "Extracted SJ gw depth (all measurements, simple, feet)"
la var depth_sj_1b "Extracted SJ gw depth (all measurements, bilinear, feet)"
la var depth_sj_2s "Extracted SJ gw depth (non-ques measurements, simple, feet)"
la var depth_sj_2b "Extracted SJ gw depth (non-ques measurements, bilinear, feet)"
la var depth_sj_3s "Extracted SJ gw depth (obs non-ques measurements, simple, feet)"
la var depth_sj_3b "Extracted SJ gw depth (obs non-ques measurements, bilinear, feet)"
la var dist_miles_sj_1 "Miles to nearest SJ gw measurement in raster (all)"
la var dist_miles_sj_2 "Miles to nearest SJ gw measurement in raster (non-ques)"
la var dist_miles_sj_3 "Miles to nearest SJ gw measurement in raster (obs non-ques)"
rename depth_sj_?? gw_rast_depth_sj_mth_??
rename dist_miles_sj_? gw_rast_dist_sj_mth_?

** Save
unique sp_uuid modate
assert r(unique)==r(N)
compress
save "$dirpath_data/groundwater/groundwater_depth_sp_month_rast_SJ.dta", replace

}

** 6c. Sacramento Valley only rasters
{
** Read in output from GIS script to extract monthly depths from rasters
insheet using "$dirpath_data/misc/prems_gw_depths_from_rasters_SAC.csv", double comma clear

** Drop quarterly variables, and SP-specific variables
drop *_????q? prem_lat prem_long bad_geocode_flag missing_geocode_flag x y

** Destring numeric variables before reshaping, to reduce file size
foreach v of varlist depth_??_* {
	cap replace `v' = "" if `v'=="NA"
	destring `v', replace
}

** Reshape long, to convert into SP-month panel
reshape long depth_1s depth_1b depth_2s depth_2b depth_3s depth_3b ///
	distkm_sac_1 distkm_sac_2 distkm_sac_3, i(sp_uuid pull) j(MODATE) string

** Reformat string variables
tostring sp_uuid pull, replace
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
assert length(sp_uuid)==10 & real(sp_uuid)!=.
gen modate = ym(real(substr(MODATE,2,4)),real(substr(MODATE,7,2)))
format %tm modate
assert string(modate,"%tm")==substr(MODATE,2,10)
drop MODATE
order sp_uuid pull modate
unique sp_uuid pull modate
assert r(unique)==r(N)
sort sp_uuid modate pull

** Make unique by SP-modate
duplicates t sp_uuid modate, gen(dup)
br if dup>0 // the vast majority have identical lat/lon across march/august pulls
unique sp_uuid modate
local uniq = r(unique)
drop if dup==1 & sp_uuid==sp_uuid[_n-1] & modate==modate[_n-1] & pull=="20180827" & ///
	pull[_n-1]=="20180322" & depth_1s==depth_1s[_n-1] & depth_1b==depth_1b[_n-1] & ///
	depth_2s==depth_2s[_n-1] & depth_2b==depth_2b[_n-1] & depth_3s==depth_3s[_n-1] & ///
	depth_3b==depth_3b[_n-1] & distkm_sac_1==distkm_sac_1[_n-1] & distkm_sac_2==distkm_sac_2[_n-1] & ///
	distkm_sac_3==distkm_sac_3[_n-1]
unique sp_uuid modate
assert r(unique)==`uniq'
duplicates t sp_uuid modate, gen(dup2)
br if dup2>0
unique sp_uuid if dup2>0 //23 SPs where lat/lon changes across march/august pulls
egen temp_max = max(pull=="20180322"), by(sp_uuid modate)
unique sp_uuid modate
local uniq = r(unique)
drop if temp_max==1 & pull!="20180322" // keep March pull coordinates, doesn't really matter b/c everything is very close
unique sp_uuid modate
assert r(unique)==`uniq'
assert r(unique)==r(N)
drop dup* temp*

** Merge in basin identifiers
merge m:1 sp_uuid using "$dirpath_data/pge_cleaned/sp_premise_gis.dta", keepusing(basin_id)
drop if _merge==2
assert _merge==3
drop _merge

** Drop units not in the Sacramento Valley basin
gen temp = 0
foreach v of varlist depth* {
	replace temp = 1 if `v'==.
}
egen temp_min = min(temp), by(sp_uuid)
tab basin_id temp_min
keep if basin_id=="5-021" // Sacramento Valley
drop temp* basin_id

** Convert kilometers to miles
foreach v of varlist distkm_sac_? {
	replace `v' = `v'*0.621371
	local v2 = subinstr("`v'","km","_miles",1)
	rename `v' `v2'
}	

** Rename depth variables
foreach v of varlist depth_?? {
	local v2 = subinstr("`v'","depth_","depth_sac_",1)
	rename `v' `v2'
}
	
** Label
la var sp_uuid "Service Point ID (anonymized, 10-digit)"
la var pull "Which PGE data pull is this SP from?"
la var modate "Year-Month"
la var depth_sac_1s "Extracted SAC gw depth (all measurements, simple, feet)"
la var depth_sac_1b "Extracted SAC gw depth (all measurements, bilinear, feet)"
la var depth_sac_2s "Extracted SAC gw depth (non-ques measurements, simple, feet)"
la var depth_sac_2b "Extracted SAC gw depth (non-ques measurements, bilinear, feet)"
la var depth_sac_3s "Extracted SAC gw depth (obs non-ques measurements, simple, feet)"
la var depth_sac_3b "Extracted SAC gw depth (obs non-ques measurements, bilinear, feet)"
la var dist_miles_sac_1 "Miles to nearest gw measurement in SAC raster (all)"
la var dist_miles_sac_2 "Miles to nearest gw measurement in SAC raster (non-ques)"
la var dist_miles_sac_3 "Miles to nearest gw measurement in SAC raster (obs non-ques)"
rename depth_sac_?? gw_rast_depth_sac_mth_??
rename dist_miles_sac_? gw_rast_dist_sac_mth_?

** Save
unique sp_uuid modate
assert r(unique)==r(N)
compress
save "$dirpath_data/groundwater/groundwater_depth_sp_month_rast_SAC.dta", replace

}


}

*******************************************************************************
*******************************************************************************

** 7. Construct panels of groundwater depth for SPs (quarterly)
if 1==1{

** 7a. Statewide rasters (ignoring basin boundaries)
{
** Read in output from GIS script to extract quarterly depths from rasters
insheet using "$dirpath_data/misc/prems_gw_depths_from_rasters.csv", double comma clear

** Drop monthly variables, and SP-specific variables
drop *_????m? *_????m?? prem_lat prem_long bad_geocode_flag missing_geocode_flag x y

** Destring numeric variables before reshaping, to reduce file size
foreach v of varlist depth_??_* {
	cap replace `v' = "" if `v'=="NA"
	destring `v', replace
}

** Reshape long, to convert into SP-quarter panel
reshape long depth_1s depth_1b depth_2s depth_2b depth_3s depth_3b ///
	distkm_1 distkm_2 distkm_3, i(sp_uuid pull) j(QTR) string

** Reformat string variables
tostring sp_uuid pull, replace
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
assert length(sp_uuid)==10 & real(sp_uuid)!=.
gen qtr = yq(real(substr(QTR,2,4)),real(substr(QTR,7,2)))
format %tq qtr
assert string(qtr,"%tq")==substr(QTR,2,10)
drop QTR
order sp_uuid pull qtr
unique sp_uuid pull qtr
assert r(unique)==r(N)
sort sp_uuid qtr pull

** Make unique by SP-quarter
duplicates t sp_uuid qtr, gen(dup)
br if dup>0 // the vast majority have identical lat/lon across march/august pulls
unique sp_uuid qtr
local uniq = r(unique)
drop if dup==1 & sp_uuid==sp_uuid[_n-1] & qtr==qtr[_n-1] & pull=="20180827" & ///
	pull[_n-1]=="20180322" & depth_1s==depth_1s[_n-1] & depth_1b==depth_1b[_n-1] & ///
	depth_2s==depth_2s[_n-1] & depth_2b==depth_2b[_n-1] & depth_3s==depth_3s[_n-1] & ///
	depth_3b==depth_3b[_n-1] & distkm_1==distkm_1[_n-1] & distkm_2==distkm_2[_n-1] & ///
	distkm_3==distkm_3[_n-1]
unique sp_uuid qtr
assert r(unique)==`uniq'
duplicates t sp_uuid qtr, gen(dup2)
br if dup2>0
unique sp_uuid if dup2>0 //23 SPs where lat/lon changes across march/august pulls
egen temp_max = max(pull=="20180322"), by(sp_uuid qtr)
unique sp_uuid qtr
local uniq = r(unique)
drop if temp_max==1 & pull!="20180322" // keep March pull coordinates, doesn't really matter b/c everything is very close
unique sp_uuid qtr
assert r(unique)==`uniq'
assert r(unique)==r(N)
drop dup* temp*

** Merge in basin identifiers
merge m:1 sp_uuid using "$dirpath_data/pge_cleaned/sp_premise_gis.dta", keepusing(basin_id)
drop if _merge==2
assert _merge==3
drop _merge

** Merge in number of groundwater measurements in each basin/quarter
merge m:1 basin_id qtr using "$dirpath_data/groundwater/avg_groundwater_depth_basin_quarter.dta", ///
	keep(1 3) keepusing(gw_qtr_bsn_mean1 gw_qtr_bsn_mean2 gw_qtr_bsn_mean3 ///
	gw_qtr_bsn_cnt1 gw_qtr_bsn_cnt2 gw_qtr_bsn_cnt3)
foreach v of varlist gw_qtr_bsn_cnt? {
	replace `v' = 0 if _merge==1
	assert `v'!=.
}
sum gw_qtr_bsn_cnt?, detail
	
** Convert kilometers to miles
foreach v of varlist distkm_? {
	replace `v' = `v'*0.621371
	local v2 = subinstr("`v'","km","_miles",1)
	rename `v' `v2'
}	
	
** Distance threshold from nearest raster point
sum dist_miles_1 if gw_qtr_bsn_cnt1>1, detail
sum dist_miles_1 if gw_qtr_bsn_cnt1>10, detail
sum dist_miles_1 if gw_qtr_bsn_cnt1>50, detail
sum dist_miles_1 if gw_qtr_bsn_cnt1>100, detail
sum dist_miles_1 if gw_qtr_bsn_cnt1>500, detail
sum dist_miles_1 if gw_qtr_bsn_cnt1==0, detail

** Label
la var sp_uuid "Service Point ID (anonymized, 10-digit)"
la var pull "Which PGE data pull is this SP from?"
la var qtr "Year-Quarter"
la var depth_1s "Extracted gw depth (all measurements, simple, feet)"
la var depth_1b "Extracted gw depth (all measurements, bilinear, feet)"
la var depth_2s "Extracted gw depth (non-ques measurements, simple, feet)"
la var depth_2b "Extracted gw depth (non-ques measurements, bilinear, feet)"
la var depth_3s "Extracted gw depth (obs non-ques measurements, simple, feet)"
la var depth_3b "Extracted gw depth (obs non-ques measurements, bilinear, feet)"
la var dist_miles_1 "Miles to nearest gw measurement in raster (all)"
la var dist_miles_2 "Miles to nearest gw measurement in raster (non-ques)"
la var dist_miles_3 "Miles to nearest gw measurement in raster (obs non-ques)"
rename depth_?? gw_rast_depth_qtr_??
rename dist_miles_? gw_rast_dist_qtr_?
drop _merge

** Save
unique sp_uuid qtr
assert r(unique)==r(N)
compress
save "$dirpath_data/groundwater/groundwater_depth_sp_quarter_rast.dta", replace
}

** 7b. San Joaquin Valley only rasters
{
** Read in output from GIS script to extract quarterly depths from rasters
insheet using "$dirpath_data/misc/prems_gw_depths_from_rasters_SJ.csv", double comma clear

** Drop monthly variables, and SP-specific variables
drop *_????m? *_????m?? prem_lat prem_long bad_geocode_flag missing_geocode_flag x y

** Destring numeric variables before reshaping, to reduce file size
foreach v of varlist depth_??_* {
	cap replace `v' = "" if `v'=="NA"
	destring `v', replace
}

** Reshape long, to convert into SP-quarter panel
reshape long depth_1s depth_1b depth_2s depth_2b depth_3s depth_3b ///
	distkm_sj_1 distkm_sj_2 distkm_sj_3, i(sp_uuid pull) j(QTR) string

** Reformat string variables
tostring sp_uuid pull, replace
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
assert length(sp_uuid)==10 & real(sp_uuid)!=.
gen qtr = yq(real(substr(QTR,2,4)),real(substr(QTR,7,2)))
format %tq qtr
assert string(qtr,"%tq")==substr(QTR,2,10)
drop QTR
order sp_uuid pull qtr
unique sp_uuid pull qtr
assert r(unique)==r(N)
sort sp_uuid qtr pull

** Make unique by SP-quarter
duplicates t sp_uuid qtr, gen(dup)
br if dup>0 // the vast majority have identical lat/lon across march/august pulls
unique sp_uuid qtr
local uniq = r(unique)
drop if dup==1 & sp_uuid==sp_uuid[_n-1] & qtr==qtr[_n-1] & pull=="20180827" & ///
	pull[_n-1]=="20180322" & depth_1s==depth_1s[_n-1] & depth_1b==depth_1b[_n-1] & ///
	depth_2s==depth_2s[_n-1] & depth_2b==depth_2b[_n-1] & depth_3s==depth_3s[_n-1] & ///
	depth_3b==depth_3b[_n-1] & distkm_sj_1==distkm_sj_1[_n-1] & distkm_sj_2==distkm_sj_2[_n-1] & ///
	distkm_sj_3==distkm_sj_3[_n-1]
unique sp_uuid qtr
assert r(unique)==`uniq'
duplicates t sp_uuid qtr, gen(dup2)
br if dup2>0
unique sp_uuid if dup2>0 //23 SPs where lat/lon changes across march/august pulls
egen temp_max = max(pull=="20180322"), by(sp_uuid qtr)
unique sp_uuid qtr
local uniq = r(unique)
drop if temp_max==1 & pull!="20180322" // keep March pull coordinates, doesn't really matter b/c everything is very close
unique sp_uuid qtr
assert r(unique)==`uniq'
assert r(unique)==r(N)
drop dup* temp*

** Merge in basin identifiers
merge m:1 sp_uuid using "$dirpath_data/pge_cleaned/sp_premise_gis.dta", keepusing(basin_id)
drop if _merge==2
assert _merge==3
drop _merge

** Drop units not in the San Joaquin Valley basin
gen temp = 0
foreach v of varlist depth* {
	replace temp = 1 if `v'==.
}
egen temp_min = min(temp), by(sp_uuid)
tab basin_id temp_min
keep if basin_id=="5-022" // San Joaquin Valley
drop temp* basin_id

** Convert kilometers to miles
foreach v of varlist distkm_sj_? {
	replace `v' = `v'*0.621371
	local v2 = subinstr("`v'","km","_miles",1)
	rename `v' `v2'
}	

** Rename depth variables
foreach v of varlist depth_?? {
	local v2 = subinstr("`v'","depth_","depth_sj_",1)
	rename `v' `v2'
}
	
** Label
la var sp_uuid "Service Point ID (anonymized, 10-digit)"
la var pull "Which PGE data pull is this SP from?"
la var qtr "Year-Quarter"
la var depth_sj_1s "Extracted SJ gw depth (all measurements, simple, feet)"
la var depth_sj_1b "Extracted SJ gw depth (all measurements, bilinear, feet)"
la var depth_sj_2s "Extracted SJ gw depth (non-ques measurements, simple, feet)"
la var depth_sj_2b "Extracted SJ gw depth (non-ques measurements, bilinear, feet)"
la var depth_sj_3s "Extracted SJ gw depth (obs non-ques measurements, simple, feet)"
la var depth_sj_3b "Extracted SJ gw depth (obs non-ques measurements, bilinear, feet)"
la var dist_miles_sj_1 "Miles to nearest SJ gw measurement in raster (all)"
la var dist_miles_sj_2 "Miles to nearest SJ gw measurement in raster (non-ques)"
la var dist_miles_sj_3 "Miles to nearest SJ gw measurement in raster (obs non-ques)"
rename depth_sj_?? gw_rast_depth_sj_qtr_??
rename dist_miles_sj_? gw_rast_dist_sj_qtr_?

** Save
unique sp_uuid qtr
assert r(unique)==r(N)
compress
save "$dirpath_data/groundwater/groundwater_depth_sp_quarter_rast_SJ.dta", replace

}

** 7c. Sacramento Valley only rasters
{
** Read in output from GIS script to extract quarterly depths from rasters
insheet using "$dirpath_data/misc/prems_gw_depths_from_rasters_SAC.csv", double comma clear

** Drop monthly variables, and SP-specific variables
drop *_????m? *_????m?? prem_lat prem_long bad_geocode_flag missing_geocode_flag x y

** Destring numeric variables before reshaping, to reduce file size
foreach v of varlist depth_??_* {
	cap replace `v' = "" if `v'=="NA"
	destring `v', replace
}

** Reshape long, to convert into SP-quarter panel
reshape long depth_1s depth_1b depth_2s depth_2b depth_3s depth_3b ///
	distkm_sac_1 distkm_sac_2 distkm_sac_3, i(sp_uuid pull) j(QTR) string

** Reformat string variables
tostring sp_uuid pull, replace
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
assert length(sp_uuid)==10 & real(sp_uuid)!=.
gen qtr = yq(real(substr(QTR,2,4)),real(substr(QTR,7,2)))
format %tq qtr
assert string(qtr,"%tq")==substr(QTR,2,10)
drop QTR
order sp_uuid pull qtr
unique sp_uuid pull qtr
assert r(unique)==r(N)
sort sp_uuid qtr pull

** Make unique by SP-quarter
duplicates t sp_uuid qtr, gen(dup)
br if dup>0 // the vast majority have identical lat/lon across march/august pulls
unique sp_uuid qtr
local uniq = r(unique)
drop if dup==1 & sp_uuid==sp_uuid[_n-1] & qtr==qtr[_n-1] & pull=="20180827" & ///
	pull[_n-1]=="20180322" & depth_1s==depth_1s[_n-1] & depth_1b==depth_1b[_n-1] & ///
	depth_2s==depth_2s[_n-1] & depth_2b==depth_2b[_n-1] & depth_3s==depth_3s[_n-1] & ///
	depth_3b==depth_3b[_n-1] & distkm_sac_1==distkm_sac_1[_n-1] & distkm_sac_2==distkm_sac_2[_n-1] & ///
	distkm_sac_3==distkm_sac_3[_n-1]
unique sp_uuid qtr
assert r(unique)==`uniq'
duplicates t sp_uuid qtr, gen(dup2)
br if dup2>0
unique sp_uuid if dup2>0 //23 SPs where lat/lon changes across march/august pulls
egen temp_max = max(pull=="20180322"), by(sp_uuid qtr)
unique sp_uuid qtr
local uniq = r(unique)
drop if temp_max==1 & pull!="20180322" // keep March pull coordinates, doesn't really matter b/c everything is very close
unique sp_uuid qtr
assert r(unique)==`uniq'
assert r(unique)==r(N)
drop dup* temp*

** Merge in basin identifiers
merge m:1 sp_uuid using "$dirpath_data/pge_cleaned/sp_premise_gis.dta", keepusing(basin_id)
drop if _merge==2
assert _merge==3
drop _merge

** Drop units not in the Sacramento Valley basin
gen temp = 0
foreach v of varlist depth* {
	replace temp = 1 if `v'==.
}
egen temp_min = min(temp), by(sp_uuid)
tab basin_id temp_min
keep if basin_id=="5-021" // Sacramento Valley
drop temp* basin_id

** Convert kilometers to miles
foreach v of varlist distkm_sac_? {
	replace `v' = `v'*0.621371
	local v2 = subinstr("`v'","km","_miles",1)
	rename `v' `v2'
}	

** Rename depth variables
foreach v of varlist depth_?? {
	local v2 = subinstr("`v'","depth_","depth_sac_",1)
	rename `v' `v2'
}
	
** Label
la var sp_uuid "Service Point ID (anonymized, 10-digit)"
la var pull "Which PGE data pull is this SP from?"
la var qtr "Year-Quarter"
la var depth_sac_1s "Extracted SAC gw depth (all measurements, simple, feet)"
la var depth_sac_1b "Extracted SAC gw depth (all measurements, bilinear, feet)"
la var depth_sac_2s "Extracted SAC gw depth (non-ques measurements, simple, feet)"
la var depth_sac_2b "Extracted SAC gw depth (non-ques measurements, bilinear, feet)"
la var depth_sac_3s "Extracted SAC gw depth (obs non-ques measurements, simple, feet)"
la var depth_sac_3b "Extracted SAC gw depth (obs non-ques measurements, bilinear, feet)"
la var dist_miles_sac_1 "Miles to nearest SAC gw measurement in raster (all)"
la var dist_miles_sac_2 "Miles to nearest SAC gw measurement in raster (non-ques)"
la var dist_miles_sac_3 "Miles to nearest SAC gw measurement in raster (obs non-ques)"
rename depth_sac_?? gw_rast_depth_sac_qtr_??
rename dist_miles_sac_? gw_rast_dist_sac_qtr_?

** Save
unique sp_uuid qtr
assert r(unique)==r(N)
compress
save "$dirpath_data/groundwater/groundwater_depth_sp_quarter_rast_SAC.dta", replace

}

}

*******************************************************************************
*******************************************************************************

** 8. Construct panels of groundwater depth for APEP pumps (monthly)
if 1==1{

** 8a. Statewide ratsers (ignoring basin boundaries)
{
** Read in output from GIS script to extract monthly depths from rasters
insheet using "$dirpath_data/misc/pumps_gw_depths_from_rasters.csv", double comma clear

** Drop quarterly variables, and APEP-specific variables
drop *_????q? pump_lat pump_lon x y

** Destring numeric variables before reshaping, to reduce file size
foreach v of varlist depth_??_* {
	cap replace `v' = "" if `v'=="NA"
	destring `v', replace
}

** Reshape long, to convert into pump-month panel
reshape long depth_1s depth_1b depth_2s depth_2b depth_3s depth_3b ///
	distkm_1 distkm_2 distkm_3, i(latlon_group) j(MODATE) string

** Reformat string variables
destring latlon_group, replace
assert latlon_group!=.
unique latlon_group MODATE
assert r(unique)==r(N)
gen modate = ym(real(substr(MODATE,2,4)),real(substr(MODATE,7,2)))
format %tm modate
assert string(modate,"%tm")==substr(MODATE,2,10)
drop MODATE
order latlon_group modate
unique latlon_group modate
assert r(unique)==r(N)
sort latlon_group modate

** Merge in basin identifiers
preserve
use "$dirpath_data/pge_cleaned/apep_pump_gis.dta", clear
keep latlon_group basin_id
duplicates drop
tempfile apep_basins
save `apep_basins'
restore
merge m:1 latlon_group using `apep_basins', keepusing(basin_id)
drop if _merge==2
assert _merge==3
drop _merge

** Merge in number of groundwater measurements in each basin/month
merge m:1 basin_id modate using "$dirpath_data/groundwater/avg_groundwater_depth_basin_month.dta", ///
	keep(1 3) keepusing(gw_mth_bsn_mean1 gw_mth_bsn_mean2 gw_mth_bsn_mean3 ///
	gw_mth_bsn_cnt1 gw_mth_bsn_cnt2 gw_mth_bsn_cnt3)
foreach v of varlist gw_mth_bsn_cnt? {
	replace `v' = 0 if _merge==1
	assert `v'!=.
}
sum gw_mth_bsn_cnt?, detail
	
** Convert kilometers to miles
foreach v of varlist distkm_? {
	replace `v' = `v'*0.621371
	local v2 = subinstr("`v'","km","_miles",1)
	rename `v' `v2'
}	
	
** Distance threshold from nearest raster point
sum dist_miles_1 if gw_mth_bsn_cnt1>1, detail
sum dist_miles_1 if gw_mth_bsn_cnt1>10, detail
sum dist_miles_1 if gw_mth_bsn_cnt1>50, detail
sum dist_miles_1 if gw_mth_bsn_cnt1>100, detail
sum dist_miles_1 if gw_mth_bsn_cnt1>500, detail
sum dist_miles_1 if gw_mth_bsn_cnt1==0, detail

** Label
la var latlon_group "APEP lat/lon identifier"
la var modate "Year-Month"
la var depth_1s "Extracted gw depth (all measurements, simple, feet)"
la var depth_1b "Extracted gw depth (all measurements, bilinear, feet)"
la var depth_2s "Extracted gw depth (non-ques measurements, simple, feet)"
la var depth_2b "Extracted gw depth (non-ques measurements, bilinear, feet)"
la var depth_3s "Extracted gw depth (obs non-ques measurements, simple, feet)"
la var depth_3b "Extracted gw depth (obs non-ques measurements, bilinear, feet)"
la var dist_miles_1 "Miles to nearest gw measurement in raster (all)"
la var dist_miles_2 "Miles to nearest gw measurement in raster (non-ques)"
la var dist_miles_3 "Miles to nearest gw measurement in raster (obs non-ques)"
rename depth_?? gw_rast_depth_mth_??
rename dist_miles_? gw_rast_dist_mth_?
drop _merge

** Save
unique latlon_group modate
assert r(unique)==r(N)
compress
save "$dirpath_data/groundwater/groundwater_depth_apep_month_rast.dta", replace
}

** 8b. San Joaquin Valley only rasters
{
** Read in output from GIS script to extract monthly depths from rasters
insheet using "$dirpath_data/misc/pumps_gw_depths_from_rasters_SJ.csv", double comma clear

** Drop quarterly variables, and APEP-specific variables
drop *_????q? pump_lat pump_lon x y

** Destring numeric variables before reshaping, to reduce file size
foreach v of varlist depth_??_* {
	cap replace `v' = "" if `v'=="NA"
	destring `v', replace
}

** Reshape long, to convert into pump-month panel
reshape long depth_1s depth_1b depth_2s depth_2b depth_3s depth_3b ///
	distkm_sj_1 distkm_sj_2 distkm_sj_3, i(latlon_group) j(MODATE) string

** Reformat string variables
destring latlon_group, replace
assert latlon_group!=.
unique latlon_group MODATE
assert r(unique)==r(N)
gen modate = ym(real(substr(MODATE,2,4)),real(substr(MODATE,7,2)))
format %tm modate
assert string(modate,"%tm")==substr(MODATE,2,10)
drop MODATE
order latlon_group modate
unique latlon_group modate
assert r(unique)==r(N)
sort latlon_group modate

** Merge in basin identifiers
preserve
use "$dirpath_data/pge_cleaned/apep_pump_gis.dta", clear
keep latlon_group basin_id
duplicates drop
tempfile apep_basins
save `apep_basins'
restore
merge m:1 latlon_group using `apep_basins', keepusing(basin_id)
drop if _merge==2
assert _merge==3
drop _merge

** Drop units not in the San Joaquin Valley basin
gen temp = 0
foreach v of varlist depth* {
	replace temp = 1 if `v'==.
}
egen temp_min = min(temp), by(latlon_group)
tab basin_id temp_min
keep if basin_id=="5-022" // San Joaquin Valley
drop temp* basin_id
	
** Convert kilometers to miles
foreach v of varlist distkm_sj_? {
	replace `v' = `v'*0.621371
	local v2 = subinstr("`v'","km","_miles",1)
	rename `v' `v2'
}	

** Rename depth variables
foreach v of varlist depth_?? {
	local v2 = subinstr("`v'","depth_","depth_sj_",1)
	rename `v' `v2'
}

** Label
la var latlon_group "APEP lat/lon identifier"
la var modate "Year-Month"
la var depth_sj_1s "Extracted SJ gw depth (all measurements, simple, feet)"
la var depth_sj_1b "Extracted SJ gw depth (all measurements, bilinear, feet)"
la var depth_sj_2s "Extracted SJ gw depth (non-ques measurements, simple, feet)"
la var depth_sj_2b "Extracted SJ gw depth (non-ques measurements, bilinear, feet)"
la var depth_sj_3s "Extracted SJ gw depth (obs non-ques measurements, simple, feet)"
la var depth_sj_3b "Extracted SJ gw depth (obs non-ques measurements, bilinear, feet)"
la var dist_miles_sj_1 "Miles to nearest SJ gw measurement in raster (all)"
la var dist_miles_sj_2 "Miles to nearest SJ gw measurement in raster (non-ques)"
la var dist_miles_sj_3 "Miles to nearest SJ gw measurement in raster (obs non-ques)"
rename depth_sj_?? gw_rast_depth_sj_mth_??
rename dist_miles_sj_? gw_rast_dist_sj_mth_?

** Save
unique latlon_group modate
assert r(unique)==r(N)
compress
save "$dirpath_data/groundwater/groundwater_depth_apep_month_rast_SJ.dta", replace
}

** 8c. Sacramento Valley only rasters
{
** Read in output from GIS script to extract monthly depths from rasters
insheet using "$dirpath_data/misc/pumps_gw_depths_from_rasters_SAC.csv", double comma clear

** Drop quarterly variables, and APEP-specific variables
drop *_????q? pump_lat pump_lon x y

** Destring numeric variables before reshaping, to reduce file size
foreach v of varlist depth_??_* {
	cap replace `v' = "" if `v'=="NA"
	destring `v', replace
}

** Reshape long, to convert into pump-month panel
reshape long depth_1s depth_1b depth_2s depth_2b depth_3s depth_3b ///
	distkm_sac_1 distkm_sac_2 distkm_sac_3, i(latlon_group) j(MODATE) string

** Reformat string variables
destring latlon_group, replace
assert latlon_group!=.
unique latlon_group MODATE
assert r(unique)==r(N)
gen modate = ym(real(substr(MODATE,2,4)),real(substr(MODATE,7,2)))
format %tm modate
assert string(modate,"%tm")==substr(MODATE,2,10)
drop MODATE
order latlon_group modate
unique latlon_group modate
assert r(unique)==r(N)
sort latlon_group modate

** Merge in basin identifiers
preserve
use "$dirpath_data/pge_cleaned/apep_pump_gis.dta", clear
keep latlon_group basin_id
duplicates drop
tempfile apep_basins
save `apep_basins'
restore
merge m:1 latlon_group using `apep_basins', keepusing(basin_id)
drop if _merge==2
assert _merge==3
drop _merge

** Drop units not in the Sacramento Valley basin
gen temp = 0
foreach v of varlist depth* {
	replace temp = 1 if `v'==.
}
egen temp_min = min(temp), by(latlon_group)
tab basin_id temp_min
keep if basin_id=="5-021" // Sacramento Valley
drop temp* basin_id
	
** Convert kilometers to miles
foreach v of varlist distkm_sac_? {
	replace `v' = `v'*0.621371
	local v2 = subinstr("`v'","km","_miles",1)
	rename `v' `v2'
}	

** Rename depth variables
foreach v of varlist depth_?? {
	local v2 = subinstr("`v'","depth_","depth_sac_",1)
	rename `v' `v2'
}

** Label
la var latlon_group "APEP lat/lon identifier"
la var modate "Year-Month"
la var depth_sac_1s "Extracted SAC gw depth (all measurements, simple, feet)"
la var depth_sac_1b "Extracted SAC gw depth (all measurements, bilinear, feet)"
la var depth_sac_2s "Extracted SAC gw depth (non-ques measurements, simple, feet)"
la var depth_sac_2b "Extracted SAC gw depth (non-ques measurements, bilinear, feet)"
la var depth_sac_3s "Extracted SAC gw depth (obs non-ques measurements, simple, feet)"
la var depth_sac_3b "Extracted SAC gw depth (obs non-ques measurements, bilinear, feet)"
la var dist_miles_sac_1 "Miles to nearest SAC gw measurement in raster (all)"
la var dist_miles_sac_2 "Miles to nearest SAC gw measurement in raster (non-ques)"
la var dist_miles_sac_3 "Miles to nearest SAC gw measurement in raster (obs non-ques)"
rename depth_sac_?? gw_rast_depth_sac_mth_??
rename dist_miles_sac_? gw_rast_dist_sac_mth_?

** Save
unique latlon_group modate
assert r(unique)==r(N)
compress
save "$dirpath_data/groundwater/groundwater_depth_apep_month_rast_SAC.dta", replace
}

}

*******************************************************************************
*******************************************************************************

** 9. Construct panels of groundwater depth for APEP pumps (quarterly)
if 1==1{

** 9a. Statewide ratsers (ignoring basin boundaries)
{
** Read in output from GIS script to extract quarterly depths from rasters
insheet using "$dirpath_data/misc/pumps_gw_depths_from_rasters.csv", double comma clear

** Drop monthly variables, and APEP-specific variables
drop *_????m? *_????m?? pump_lat pump_lon x y

** Destring numeric variables before reshaping, to reduce file size
foreach v of varlist depth_??_* {
	cap replace `v' = "" if `v'=="NA"
	destring `v', replace
}

** Reshape long, to convert into pump-quarter panel
reshape long depth_1s depth_1b depth_2s depth_2b depth_3s depth_3b ///
	distkm_1 distkm_2 distkm_3, i(latlon_group) j(QTR) string

** Reformat string variables
destring latlon_group, replace
assert latlon_group!=.
unique latlon_group QTR
assert r(unique)==r(N)
gen qtr = yq(real(substr(QTR,2,4)),real(substr(QTR,7,2)))
format %tq qtr
assert string(qtr,"%tq")==substr(QTR,2,10)
drop QTR
order latlon_group qtr
unique latlon_group qtr
assert r(unique)==r(N)
sort latlon_group qtr

** Merge in basin identifiers
preserve
use "$dirpath_data/pge_cleaned/apep_pump_gis.dta", clear
keep latlon_group basin_id
duplicates drop
tempfile apep_basins
save `apep_basins'
restore
merge m:1 latlon_group using `apep_basins', keepusing(basin_id)
drop if _merge==2
assert _merge==3
drop _merge

** Merge in number of groundwater measurements in each basin/quarter
merge m:1 basin_id qtr using "$dirpath_data/groundwater/avg_groundwater_depth_basin_quarter.dta", ///
	keep(1 3) keepusing(gw_qtr_bsn_mean1 gw_qtr_bsn_mean2 gw_qtr_bsn_mean3 ///
	gw_qtr_bsn_cnt1 gw_qtr_bsn_cnt2 gw_qtr_bsn_cnt3)
foreach v of varlist gw_qtr_bsn_cnt? {
	replace `v' = 0 if _merge==1
	assert `v'!=.
}
sum gw_qtr_bsn_cnt?, detail
	
** Convert kilometers to miles
foreach v of varlist distkm_? {
	replace `v' = `v'*0.621371
	local v2 = subinstr("`v'","km","_miles",1)
	rename `v' `v2'
}	
	
** Distance threshold from nearest raster point
sum dist_miles_1 if gw_qtr_bsn_cnt1>1, detail
sum dist_miles_1 if gw_qtr_bsn_cnt1>10, detail
sum dist_miles_1 if gw_qtr_bsn_cnt1>50, detail
sum dist_miles_1 if gw_qtr_bsn_cnt1>100, detail
sum dist_miles_1 if gw_qtr_bsn_cnt1>500, detail
sum dist_miles_1 if gw_qtr_bsn_cnt1==0, detail

** Label
la var latlon_group "APEP lat/lon identifier"
la var qtr "Year-Quarter"
la var depth_1s "Extracted gw depth (all measurements, simple, feet)"
la var depth_1b "Extracted gw depth (all measurements, bilinear, feet)"
la var depth_2s "Extracted gw depth (non-ques measurements, simple, feet)"
la var depth_2b "Extracted gw depth (non-ques measurements, bilinear, feet)"
la var depth_3s "Extracted gw depth (obs non-ques measurements, simple, feet)"
la var depth_3b "Extracted gw depth (obs non-ques measurements, bilinear, feet)"
la var dist_miles_1 "Miles to nearest gw measurement in raster (all)"
la var dist_miles_2 "Miles to nearest gw measurement in raster (non-ques)"
la var dist_miles_3 "Miles to nearest gw measurement in raster (obs non-ques)"
rename depth_?? gw_rast_depth_qtr_??
rename dist_miles_? gw_rast_dist_qtr_?
drop _merge

** Save
unique latlon_group qtr
assert r(unique)==r(N)
compress
save "$dirpath_data/groundwater/groundwater_depth_apep_quarter_rast.dta", replace
}

** 9b. San Joaquin Valley only rasters
{
** Read in output from GIS script to extract monthly depths from rasters
insheet using "$dirpath_data/misc/pumps_gw_depths_from_rasters_SJ.csv", double comma clear

** Drop quarterly variables, and APEP-specific variables
drop *_????m? *_????m?? pump_lat pump_lon x y

** Destring numeric variables before reshaping, to reduce file size
foreach v of varlist depth_??_* {
	cap replace `v' = "" if `v'=="NA"
	destring `v', replace
}

** Reshape long, to convert into pump-month panel
reshape long depth_1s depth_1b depth_2s depth_2b depth_3s depth_3b ///
	distkm_sj_1 distkm_sj_2 distkm_sj_3, i(latlon_group) j(QTR) string

** Reformat string variables
destring latlon_group, replace
assert latlon_group!=.
unique latlon_group QTR
assert r(unique)==r(N)
gen qtr = yq(real(substr(QTR,2,4)),real(substr(QTR,7,2)))
format %tq qtr
assert string(qtr,"%tq")==substr(QTR,2,10)
drop QTR
order latlon_group qtr
unique latlon_group qtr
assert r(unique)==r(N)
sort latlon_group qtr

** Merge in basin identifiers
preserve
use "$dirpath_data/pge_cleaned/apep_pump_gis.dta", clear
keep latlon_group basin_id
duplicates drop
tempfile apep_basins
save `apep_basins'
restore
merge m:1 latlon_group using `apep_basins', keepusing(basin_id)
drop if _merge==2
assert _merge==3
drop _merge

** Drop units not in the San Joaquin Valley basin
gen temp = 0
foreach v of varlist depth* {
	replace temp = 1 if `v'==.
}
egen temp_min = min(temp), by(latlon_group)
tab basin_id temp_min
keep if basin_id=="5-022" // San Joaquin Valley
drop temp* basin_id
	
** Convert kilometers to miles
foreach v of varlist distkm_sj_? {
	replace `v' = `v'*0.621371
	local v2 = subinstr("`v'","km","_miles",1)
	rename `v' `v2'
}	

** Rename depth variables
foreach v of varlist depth_?? {
	local v2 = subinstr("`v'","depth_","depth_sj_",1)
	rename `v' `v2'
}

** Label
la var latlon_group "APEP lat/lon identifier"
la var qtr "Year-Quarter"
la var depth_sj_1s "Extracted SJ gw depth (all measurements, simple, feet)"
la var depth_sj_1b "Extracted SJ gw depth (all measurements, bilinear, feet)"
la var depth_sj_2s "Extracted SJ gw depth (non-ques measurements, simple, feet)"
la var depth_sj_2b "Extracted SJ gw depth (non-ques measurements, bilinear, feet)"
la var depth_sj_3s "Extracted SJ gw depth (obs non-ques measurements, simple, feet)"
la var depth_sj_3b "Extracted SJ gw depth (obs non-ques measurements, bilinear, feet)"
la var dist_miles_sj_1 "Miles to nearest SJ gw measurement in raster (all)"
la var dist_miles_sj_2 "Miles to nearest SJ gw measurement in raster (non-ques)"
la var dist_miles_sj_3 "Miles to nearest SJ gw measurement in raster (obs non-ques)"
rename depth_sj_?? gw_rast_depth_sj_qtr_??
rename dist_miles_sj_? gw_rast_dist_sj_qt_?

** Save
unique latlon_group qtr
assert r(unique)==r(N)
compress
save "$dirpath_data/groundwater/groundwater_depth_apep_quarter_rast_SJ.dta", replace
}

** 9c. Sacramento Valley only rasters
{
** Read in output from GIS script to extract monthly depths from rasters
insheet using "$dirpath_data/misc/pumps_gw_depths_from_rasters_SAC.csv", double comma clear

** Drop quarterly variables, and APEP-specific variables
drop *_????m? *_????m?? pump_lat pump_lon x y

** Destring numeric variables before reshaping, to reduce file size
foreach v of varlist depth_??_* {
	cap replace `v' = "" if `v'=="NA"
	destring `v', replace
}

** Reshape long, to convert into pump-month panel
reshape long depth_1s depth_1b depth_2s depth_2b depth_3s depth_3b ///
	distkm_sac_1 distkm_sac_2 distkm_sac_3, i(latlon_group) j(QTR) string

** Reformat string variables
destring latlon_group, replace
assert latlon_group!=.
unique latlon_group QTR
assert r(unique)==r(N)
gen qtr = yq(real(substr(QTR,2,4)),real(substr(QTR,7,2)))
format %tq qtr
assert string(qtr,"%tq")==substr(QTR,2,10)
drop QTR
order latlon_group qtr
unique latlon_group qtr
assert r(unique)==r(N)
sort latlon_group qtr

** Merge in basin identifiers
preserve
use "$dirpath_data/pge_cleaned/apep_pump_gis.dta", clear
keep latlon_group basin_id
duplicates drop
tempfile apep_basins
save `apep_basins'
restore
merge m:1 latlon_group using `apep_basins', keepusing(basin_id)
drop if _merge==2
assert _merge==3
drop _merge

** Drop units not in the Sacramento Valley basin
gen temp = 0
foreach v of varlist depth* {
	replace temp = 1 if `v'==.
}
egen temp_min = min(temp), by(latlon_group)
tab basin_id temp_min
keep if basin_id=="5-021" // Sacramento Valley
drop temp* basin_id
	
** Convert kilometers to miles
foreach v of varlist distkm_sac_? {
	replace `v' = `v'*0.621371
	local v2 = subinstr("`v'","km","_miles",1)
	rename `v' `v2'
}	

** Rename depth variables
foreach v of varlist depth_?? {
	local v2 = subinstr("`v'","depth_","depth_sac_",1)
	rename `v' `v2'
}

** Label
la var latlon_group "APEP lat/lon identifier"
la var qtr "Year-Quarter"
la var depth_sac_1s "Extracted SAC gw depth (all measurements, simple, feet)"
la var depth_sac_1b "Extracted SAC gw depth (all measurements, bilinear, feet)"
la var depth_sac_2s "Extracted SAC gw depth (non-ques measurements, simple, feet)"
la var depth_sac_2b "Extracted SAC gw depth (non-ques measurements, bilinear, feet)"
la var depth_sac_3s "Extracted SAC gw depth (obs non-ques measurements, simple, feet)"
la var depth_sac_3b "Extracted SAC gw depth (obs non-ques measurements, bilinear, feet)"
la var dist_miles_sac_1 "Miles to nearest SAC gw measurement in raster (all)"
la var dist_miles_sac_2 "Miles to nearest SAC gw measurement in raster (non-ques)"
la var dist_miles_sac_3 "Miles to nearest SAC gw measurement in raster (obs non-ques)"
rename depth_sac_?? gw_rast_depth_sac_qtr_??
rename dist_miles_sac_? gw_rast_dist_sac_qtr_?

** Save
unique latlon_group qtr
assert r(unique)==r(N)
compress
save "$dirpath_data/groundwater/groundwater_depth_apep_quarter_rast_SAC.dta", replace
}


}

*******************************************************************************
*******************************************************************************

** 10. Diagnostics 
{

** 10a. Monthly, SP, SJ
use "$dirpath_data/groundwater/groundwater_depth_sp_month_rast.dta", clear
keep if basin_id=="5-022"
merge 1:1 sp_uuid modate using "$dirpath_data/groundwater/groundwater_depth_sp_month_rast_SJ.dta"
assert _merge==3
drop _merge
merge m:1 sp_uuid using "$dirpath_data/pge_cleaned/sp_premise_gis.dta", keep(1 3) keepusing(prem_lat prem_lon)
correlate gw_rast_depth_mth_1s gw_rast_depth_sj_mth_1s
correlate gw_rast_depth_mth_1b gw_rast_depth_sj_mth_1b
correlate gw_rast_depth_mth_2s gw_rast_depth_sj_mth_2s
correlate gw_rast_depth_mth_2b gw_rast_depth_sj_mth_2b
correlate gw_rast_depth_mth_3s gw_rast_depth_sj_mth_3s
correlate gw_rast_depth_mth_3b gw_rast_depth_sj_mth_3b
twoway scatter gw_rast_depth_mth_2s gw_rast_depth_sj_mth_2s, msize(vtiny)
twoway scatter gw_rast_depth_mth_2s gw_rast_depth_sj_mth_2s if modate==ym(2015,3), msize(vtiny)
twoway scatter gw_rast_depth_mth_2s gw_rast_depth_sj_mth_2s if modate==ym(2015,7), msize(vtiny)
twoway scatter gw_rast_depth_mth_2s gw_rast_depth_sj_mth_2s if modate==ym(2013,8), msize(vtiny)
twoway scatter gw_rast_depth_mth_2s gw_rast_depth_sj_mth_2s if modate==ym(2011,8), msize(vtiny)
twoway scatter gw_rast_depth_mth_2s gw_rast_depth_sj_mth_2s if modate==ym(2016,9), msize(vtiny)
twoway scatter gw_rast_depth_mth_2s gw_rast_depth_sj_mth_2s if modate==ym(2013,1), msize(vtiny)
twoway scatter gw_rast_depth_mth_2s gw_rast_depth_sj_mth_2s if modate==ym(2010,5), msize(vtiny)

gen month = real(substr(string(modate,"%tm"),6,2))
twoway scatter gw_rast_depth_mth_2s gw_rast_depth_sj_mth_2s if month==8, msize(vtiny)

gen temp = gw_rast_depth_sj_mth_2s - gw_rast_depth_mth_2s
hist temp
sum temp, detail
egen temp_tag = tag(sp_uuid)

egen temp2 = mean(abs(temp)), by(sp_uuid)
sum temp2 if temp_tag, det
twoway ///
	(scatter prem_lat prem_lon if temp_tag & temp2<17, msize(vtiny)) ///
	(scatter prem_lat prem_lon if temp_tag & inrange(temp2,17,34), msize(vtiny)) ///
	(scatter prem_lat prem_lon if temp_tag & temp2>34, msize(vtiny)) ///
	
reg temp gw_rast_dist_sj_mth_2 gw_rast_dist_mth_2
gen temp3 = gw_rast_dist_sj_mth_2 - gw_rast_dist_mth_2

twoway scatter temp gw_rast_dist_sj_mth_2, msize(vtiny)

sum gw_rast_dist_sj_mth_2, detail
correlate gw_rast_depth_mth_2s gw_rast_depth_sj_mth_2s if inrange(gw_rast_dist_sj_mth_2,0,10)
correlate gw_rast_depth_mth_2s gw_rast_depth_sj_mth_2s if inrange(gw_rast_dist_sj_mth_2,10,20)
correlate gw_rast_depth_mth_2s gw_rast_depth_sj_mth_2s if inrange(gw_rast_dist_sj_mth_2,20,30)
correlate gw_rast_depth_mth_2s gw_rast_depth_sj_mth_2s if inrange(gw_rast_dist_sj_mth_2,30,40)
correlate gw_rast_depth_mth_2s gw_rast_depth_sj_mth_2s if inrange(gw_rast_dist_sj_mth_2,40,50)
correlate gw_rast_depth_mth_2s gw_rast_depth_sj_mth_2s if inrange(gw_rast_dist_sj_mth_2,50,60)
correlate gw_rast_depth_mth_2s gw_rast_depth_sj_mth_2s if inrange(gw_rast_dist_sj_mth_2,60,70)
correlate gw_rast_depth_mth_2s gw_rast_depth_sj_mth_2s if inrange(gw_rast_dist_sj_mth_2,70,80)
correlate gw_rast_depth_mth_2s gw_rast_depth_sj_mth_2s if inrange(gw_rast_dist_sj_mth_2,80,90)
correlate gw_rast_depth_mth_2s gw_rast_depth_sj_mth_2s if inrange(gw_rast_dist_sj_mth_2,90,.)




** 10b. Monthly, SP, SAC
use "$dirpath_data/groundwater/groundwater_depth_sp_month_rast.dta", clear
keep if basin_id=="5-021"
merge 1:1 sp_uuid modate using "$dirpath_data/groundwater/groundwater_depth_sp_month_rast_SAC.dta"
assert _merge==3
drop _merge
merge m:1 sp_uuid using "$dirpath_data/pge_cleaned/sp_premise_gis.dta", keep(1 3) keepusing(prem_lat prem_lon)
correlate gw_rast_depth_mth_1s gw_rast_depth_sac_mth_1s
correlate gw_rast_depth_mth_1b gw_rast_depth_sac_mth_1b
correlate gw_rast_depth_mth_2s gw_rast_depth_sac_mth_2s
correlate gw_rast_depth_mth_2b gw_rast_depth_sac_mth_2b
correlate gw_rast_depth_mth_3s gw_rast_depth_sac_mth_3s
correlate gw_rast_depth_mth_3b gw_rast_depth_sac_mth_3b
twoway scatter gw_rast_depth_mth_2s gw_rast_depth_sac_mth_2s, msize(vtiny)
twoway scatter gw_rast_depth_mth_2s gw_rast_depth_sac_mth_2s if modate==ym(2015,3), msize(vtiny)
twoway scatter gw_rast_depth_mth_2s gw_rast_depth_sac_mth_2s if modate==ym(2015,7), msize(vtiny)
twoway scatter gw_rast_depth_mth_2s gw_rast_depth_sac_mth_2s if modate==ym(2013,8), msize(vtiny)
twoway scatter gw_rast_depth_mth_2s gw_rast_depth_sac_mth_2s if modate==ym(2011,8), msize(vtiny)
twoway scatter gw_rast_depth_mth_2s gw_rast_depth_sac_mth_2s if modate==ym(2016,9), msize(vtiny)
twoway scatter gw_rast_depth_mth_2s gw_rast_depth_sac_mth_2s if modate==ym(2013,1), msize(vtiny)
twoway scatter gw_rast_depth_mth_2s gw_rast_depth_sac_mth_2s if modate==ym(2010,5), msize(vtiny)

gen month = real(substr(string(modate,"%tm"),6,2))
twoway scatter gw_rast_depth_mth_2s gw_rast_depth_sac_mth_2s if month==8, msize(vtiny)

gen temp = gw_rast_depth_sac_mth_2s - gw_rast_depth_mth_2s
hist temp
sum temp, detail
egen temp_tag = tag(sp_uuid)

egen temp2 = mean(abs(temp)), by(sp_uuid)
sum temp2 if temp_tag, det
twoway ///
	(scatter prem_lat prem_lon if temp_tag & temp2<-6, msize(vtiny)) ///
	(scatter prem_lat prem_lon if temp_tag & inrange(temp2,-6,1.42), msize(vtiny)) ///
	(scatter prem_lat prem_lon if temp_tag & temp2>1.42, msize(vtiny)) ///

reg temp gw_rast_dist_sac_mth_2 gw_rast_dist_mth_2
gen temp3 = gw_rast_dist_sac_mth_2 - gw_rast_dist_mth_2


** 10c. Quarterly, SP, SJ
use "$dirpath_data/groundwater/groundwater_depth_sp_quarter_rast.dta", clear
keep if basin_id=="5-022"
merge 1:1 sp_uuid qtr using "$dirpath_data/groundwater/groundwater_depth_sp_quarter_rast_SJ.dta"
assert _merge==3
drop _merge
merge m:1 sp_uuid using "$dirpath_data/pge_cleaned/sp_premise_gis.dta", keep(1 3) keepusing(prem_lat prem_lon)
correlate gw_rast_depth_qtr_1s gw_rast_depth_sj_qtr_1s
correlate gw_rast_depth_qtr_1b gw_rast_depth_sj_qtr_1b
correlate gw_rast_depth_qtr_2s gw_rast_depth_sj_qtr_2s
correlate gw_rast_depth_qtr_2b gw_rast_depth_sj_qtr_2b
correlate gw_rast_depth_qtr_3s gw_rast_depth_sj_qtr_3s
correlate gw_rast_depth_qtr_3b gw_rast_depth_sj_qtr_3b
twoway scatter gw_rast_depth_qtr_2s gw_rast_depth_sj_qtr_2s, msize(vtiny)
twoway scatter gw_rast_depth_qtr_2s gw_rast_depth_sj_qtr_2s if qtr==yq(2015,3), msize(vtiny)
twoway scatter gw_rast_depth_qtr_2s gw_rast_depth_sj_qtr_2s if qtr==yq(2015,1), msize(vtiny)
twoway scatter gw_rast_depth_qtr_2s gw_rast_depth_sj_qtr_2s if qtr==yq(2013,2), msize(vtiny)
twoway scatter gw_rast_depth_qtr_2s gw_rast_depth_sj_qtr_2s if qtr==yq(2011,4), msize(vtiny)
twoway scatter gw_rast_depth_qtr_2s gw_rast_depth_sj_qtr_2s if qtr==yq(2016,1), msize(vtiny)
twoway scatter gw_rast_depth_qtr_2s gw_rast_depth_sj_qtr_2s if qtr==yq(2013,2), msize(vtiny)
twoway scatter gw_rast_depth_qtr_2s gw_rast_depth_sj_qtr_2s if qtr==yq(2010,3), msize(vtiny)

gen quarter = real(substr(string(qtr,"%tq"),6,2))
correlate gw_rast_depth_qtr_2s gw_rast_depth_sj_qtr_2s if quarter==1
correlate gw_rast_depth_qtr_2s gw_rast_depth_sj_qtr_2s if quarter==2
correlate gw_rast_depth_qtr_2s gw_rast_depth_sj_qtr_2s if quarter==3
correlate gw_rast_depth_qtr_2s gw_rast_depth_sj_qtr_2s if quarter==4

twoway scatter gw_rast_depth_qtr_2s gw_rast_depth_sj_qtr_2s if quarter==3, msize(vtiny)

gen temp = gw_rast_depth_sj_qtr_2s - gw_rast_depth_qtr_2s
hist temp
sum temp, detail
egen temp_tag = tag(sp_uuid)

egen temp2 = mean(abs(temp)), by(sp_uuid)
sum temp2 if temp_tag, det
twoway ///
	(scatter prem_lat prem_lon if temp_tag & temp2<17, msize(vtiny)) ///
	(scatter prem_lat prem_lon if temp_tag & inrange(temp2,17,34), msize(vtiny)) ///
	(scatter prem_lat prem_lon if temp_tag & temp2>34, msize(vtiny)) ///
	
reg temp gw_rast_dist_sj_qtr_2 gw_rast_dist_qtr_2
gen temp3 = gw_rast_dist_sj_qtr_2 - gw_rast_dist_qtr_2

twoway scatter temp temp3, msize(vtiny)

sum gw_rast_dist_sj_qtr_2, detail
correlate gw_rast_depth_qtr_2s gw_rast_depth_sj_qtr_2s if inrange(gw_rast_dist_sj_qtr_2,0,10)
correlate gw_rast_depth_qtr_2s gw_rast_depth_sj_qtr_2s if inrange(gw_rast_dist_sj_qtr_2,10,20)
correlate gw_rast_depth_qtr_2s gw_rast_depth_sj_qtr_2s if inrange(gw_rast_dist_sj_qtr_2,20,30)
correlate gw_rast_depth_qtr_2s gw_rast_depth_sj_qtr_2s if inrange(gw_rast_dist_sj_qtr_2,30,40)
correlate gw_rast_depth_qtr_2s gw_rast_depth_sj_qtr_2s if inrange(gw_rast_dist_sj_qtr_2,40,50)
correlate gw_rast_depth_qtr_2s gw_rast_depth_sj_qtr_2s if inrange(gw_rast_dist_sj_qtr_2,50,60)
correlate gw_rast_depth_qtr_2s gw_rast_depth_sj_qtr_2s if inrange(gw_rast_dist_sj_qtr_2,60,.)

	}

*******************************************************************************
*******************************************************************************

