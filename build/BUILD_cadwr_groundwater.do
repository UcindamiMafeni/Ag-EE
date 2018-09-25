clear all
version 13
set more off

**************************************************************
**** Script to import and clean CA DWR groundwater data ******
**************************************************************

** Downloaded at http://wdl.water.ca.gov/waterdatalibrary/groundwater/index.cfm 
** (top link on the page: https://d3.water.ca.gov/owncloud/index.php/s/smQyUOe4wkxwkNr)

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

// PENDING
	
	* supplement with county-specific data pulls from CA DWR, if necessary???

*******************************************************************************
*******************************************************************************

** 1. Main groundwater level (GWL) dataset
if 1==0{
** Import GWL file
insheet using "$dirpath_data/groundwater/Statewide_GWL_Data_20170905/gwl_file.csv", double comma clear

** Clean and label identifiers and dates
la var casgem_station_id "Station identifier"
tostring casgem_station_id, replace
replace casgem_station_id = "0" + casgem_station_id if length(casgem_station_id)<5
assert length(casgem_station_id)==5
assert casgem_station_id!=""
unique casgem_station_id // 39,503 unique stations

la var site_code "Unique station identifier (lat/lon)"
assert length(site_code)==18
egen temp_group1 = group(casgem_station_id)
egen temp_group2 = group(casgem_station_id site_code)
assert temp_group1==temp_group2 // redundant as an identifier, BUT is also lat/lon, so i'll keep it for now
drop temp_group1 temp_group2 
assert site_code!=""

la var elevation_id "Record identifier (unique for GWL observations)"
unique elevation_id
assert r(unique)==r(N) // this is unique, and very weirdly named
assert elevation_id!=.

gen date = date(word(measurement_date,1),"MDY")
format %td date
order date, before(measurement_date)
assert date!=.
drop measurement_date
la var date "Measurement date"
gen year = year(date)
la var year "Measurement year"
order year, after(date)
*hist year
*hist year if inrange(year,2000,2018)
*hist date if inrange(year,2000,2018)
egen temp_min = min(year), by(site_code)
egen temp_max = max(year), by(site_code)
tab temp_min
tab temp_max // 43% of all sites continue into 2017
drop temp*

** Clean, label, and process surface and groundwater measurements
la var rp_elevation "Reference point elevation (feet above sea level)"
assert rp_elevation!=.
sum rp_elevation, detail

la var gs_elevation "Ground surface elevation (ground feet above sea level)"
count if gs_elevation==. // 46 missings out of 1.6M
sum gs_elevation, detail
correlate rp_elevation gs_elevation
gen temp = gs_elevation-rp_elevation
sum temp, detail // very close for almost all observations
drop temp

la var rp_reading "Reference point reading (water feet below surface)"
sum rp_reading, detail
coun if rp_reading==. // 150708 missings out of 1.6M

la var ws_reading "Water surface reading (correction to water feet below surface)"
sum ws_reading, detail // mostly zeros???
count if ws_reading==. // 149107 missings out of 1.6M

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
gen rp_ws_depth = rp_reading
replace rp_ws_depth = rp_ws_depth - ws_reading if ws_reading!=. & ws_reading!=0
la var rp_ws_depth "Water depth (feet) below reference point, corrected"

	// water depth below surface
gen gs_ws_depth = rp_ws_depth - (rp_elevation-gs_elevation)	
la var gs_ws_depth "Water depth (feet) below ground surface, corrected"

	// water surface elevation w/r/t sea level
gen ws_elevation = gs_elevation - gs_ws_depth
la var ws_elevation "Water surface elevation (feet) above sea level"	

	// reorder variables
order rp_elevation gs_elevation rp_reading ws_reading rp_ws_depth gs_ws_depth ///
	ws_elevation, after(year)

	// deal with crazy outliers
count if gs_ws_depth<0 & year>=2005 
local rN = r(N)
count if year>=2005
di `rN'/r(N) // 0.6% of observations have negative water depth
br if gs_ws_depth<0	
br if gs_ws_depth<0	& year>=2005 & measurement_issue_id==.
gen neg_depth = gs_ws_depth<0
egen temp = max(neg_depth) if year>=2005, by(site_code)
egen neg_depth_ever = mean(temp), by(site_code)
la var neg_depth "Readings report water surface ABOVE ground surface"
la var neg_depth_ever "Site where water is ever (post-2005) reported to have negative depth"
drop temp

** Clean and label remaining variables
la var measurement_issue_id "Measurement problem code"
tab measurement_issue_id, missing // 17% populated, 26 unique issues!

la var measurement_method_id "Measurement method code"
tab measurement_method_id, missing // 90% populated, 7 unique methods

la var measurement_accuracy_id "Measurement accuracy code"
tab measurement_accuracy_id, missing // 90% populated, 5 unique accuracy codes

la var casgem_reading "Reading is a casgem submittal"
tab casgem_reading, missing

la var org_id "Monitoring agency code"
la var org_name "Monitoring agency name"
tab org_id, missing
tab org_name, missing
egen temp_group1 = group(org_id)
egen temp_group2 = group(org_id org_name)
assert temp_group1==temp_group2 // confirms that agency names are clean!
drop temp_group1 temp_group2
assert org_id!=. & org_name!=""

la var comments "Measurement Remarks"
count if comments=="" // hooboy this field is a doozie

la var coop_agency_org_id "Cooperating Agency Code"
la var coop_org_name "Cooperating Agency Name"
tab coop_agency_org_id, missing
tab coop_org_name, missing
egen temp_group1 = group(org_id)
egen temp_group2 = group(org_id org_name)
assert temp_group1==temp_group2 // confirms that agency names are clean!
drop temp_group1 temp_group2
assert coop_agency_org_id!=. & coop_org_name!=""
rename coop_agency_org_id coop_org_id

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
assert _merge==3 | measurement_accuracy_id==.
drop _merge
la var measurement_accuracy_desc "Measurement accuracy description"
order measurement_accuracy_desc, after(measurement_accuracy_id)

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
assert _merge==3 | measurement_method_id==.
drop _merge
la var measurement_method_desc "Measurement method description"
order measurement_method_desc, after(measurement_method_id)

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
assert _merge==3 | measurement_issue_id==.
drop _merge
assert inlist(measurement_issue_class,"Q","N","")
assert rp_reading!=. if measurement_issue_class!="N"
la var measurement_issue_desc "Measurement issue description"
la var measurement_issue_class "Q = questionable, N = no measurement"
order measurement_issue_desc measurement_issue_class, after(measurement_issue_id)
drop comments // lots of overlap with issue descriptions

** Extract lat/lon from site_code
gen lat = substr(site_code,1,6)
gen lon = substr(site_code,8,7)
destring lat lon, replace
replace lat = lat/10000
replace lon = -lon/10000
la var lat "Site latitude (extracted from site_code)"
la var lon "Site longitude (extracted from site_code)"

** Save
sort site_code date
compress
save "$dirpath_data/groundwater/ca_dwr_gwl.dta", replace
}

*******************************************************************************
*******************************************************************************

** 2. Main groundwater station (GST) dataset
if 1==0{


** Import GST file
insheet using "$dirpath_data/groundwater/Statewide_GWL_Data_20170905/gst_file.csv", double comma clear

** Clean and label
la var casgem_station_id "Station identifier"
tostring casgem_station_id, replace
replace casgem_station_id = "0" + casgem_station_id if length(casgem_station_id)<5
assert length(casgem_station_id)==5
assert casgem_station_id!=""
unique casgem_station_id // 39,317 unique stations

la var site_code "Unique station identifier (lat/lon)"
assert length(site_code)==18
egen temp_group1 = group(casgem_station_id)
egen temp_group2 = group(casgem_station_id site_code)
assert temp_group1==temp_group2 // redundant as an identifier, BUT is also lat/lon, so i'll keep it for now
drop temp_group1 temp_group2 
assert site_code!=""

la var state_well_number "State well number"
la var local_well_designation "Identifier used by local agency"
assert (length(state_well_number)==13) | ///
	(state_well_number==""& local_well_designation!="")
replace local_well_designation = subinstr(local_well_designation,state_well_number,"",1)

la var latitude "Station latitude"
la var longitude "Station longitude"
twoway scatter latitude longitude, msize(vtiny) aspect(1.6)
sum latitude, detail
sum longitude, detail
assert latitude!=. & longitude!=.

la var loc_method "Method by which well was located"
tab loc_method

la var loc_accuracy "Accuracy of well location"
tab loc_accuracy

la var basin_cd "Groundwater basin code of well"
la var basin_desc "Groundwater basin name of well"
replace basin_cd = subinstr(subinstr(basin_cd,"-0","-",1),"-0","-",1) // remove leading zeros for merge
assert basin_cd!="" & basin_desc!=""
rename basin_desc basin_name

la var is_voluntary_reporting "Code (Y/N) indicating reporting status"
tab is_voluntary_reporting, missing

la var total_depth_ft "Total depth of well (if public)"
sum total_depth_ft, detail
count if total_depth_ft==. // 29117 missings out of 39317

rename casgem_station_use_desc well_use_desc
la var well_use_desc "Reported use of well"
tab well_use_desc, missing

la var completion_rpt_nbr "Completion report number (???)"

la var county_name "Station county"
order county_name, after (longitude)
assert county_name!=""

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
	// manually assign 4 missings
replace basin_region_id = 3 if basin_region_id==. & basin_cd=="3-2.01"
replace basin_region_id = 7 if basin_region_id==. & basin_cd=="5-22.17"
replace basin_region_id = 4 if basin_region_id==. & basin_cd=="8-4.01"
replace basin_region_id = 4 if basin_region_id==. & basin_cd=="8-4.02"
assert basin_region_id!=.
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
assert _merge==3
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
	
** Save
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
egen temp_tag = tag(casgem_station_id site_code)
tab _merge if temp_tag // 87% of wells inn GWL data match into GST data

	// diagnose non-merges
egen temp_min = min(year), by(casgem_station_id site_code)
egen temp_max = max(year), by(casgem_station_id site_code)
gen temp_diff = temp_max-temp_min
sum temp_diff if _merge==1, detail
sum temp_diff if _merge==3, detail // unmerged wells tend to have shorter tenures
drop temp_min temp_max temp_diff

	// attempt to resolve non=-merged using geodist
/*
preserve
keep if temp_tag & _merge<3
drop temp_tag
sort casgem_station_id site_code
egen temp_group1 = group(casgem_station_id site_code) if _merge==1
egen temp_group2 = group(casgem_station_id site_code) if _merge==2
unique temp_group1 temp_group2
assert r(unique)==r(N)
gen temp_nearest_id = .
gen temp_nearest_dist = .
levelsof temp_group1, local(levs)
foreach id in `levs' {
	qui sum lat if temp_group1==`id'
	local lat = r(mean)
	qui sum lon if temp_group1==`id'
	local lon = r(mean)
	di `id' "  " `lat' "  " `lon'
	qui geodist `lat' `lon' latitude longitude, gen(temp_dist) miles
	qui sum temp_dist
	local minD = r(min)
	qui sum temp_group2 if temp_dist==`minD'
	local minID = r(min)
	qui replace temp_nearest_dist = `minD' if temp_group1==`id'
	qui replace temp_nearest_id = `minID' if temp_group1==`id'
	drop temp_dist
}
sum temp_nearest_dist, detail // VERY few are right on top of each other! 5th pctile id 3.5 miles!
restore // this was a failure
*/

** Drop non-merged stations and flag non-merged GWL readings 
drop if _merge==2
gen flag_gwl_unmerged = _merge==1
replace latitude = lat if _merge==1 & latitude==.
replace longitude = lon if _merge==1 & longitude==. 
drop temp_tag _merge lat lon
la var flag_gwl_unmerged "GWL readings at stations missing from GST dataset"

** Drop variables we won't use
drop elevation_id casgem_reading org_id org_name coop_org_id coop_org_name state_well_number ///
	local_well_designation is_voluntary_reporting completion_rpt_nbr

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
replace QUES = 1 if measurement_issue_class=="" // issues flagged as either Questionable or No measurement
replace QUES = 1 if neg_depth==1 // negative depth
	// other potential refinements:
	//	- length/consistency of well's time series (for simple averages, not sure this is necessary)
	// 	- method of measurement (if some are particularly bad??)
	//  - location accuracy (but most seem pretty accurate)
	//	- discriminate based on type of measurement issue

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

** Mean/sd by basin/month, excluding questionable measurements and non-observational wells
egen double temp1 = mean(gs_ws_depth) if QUES==0 & well_use_desc=="Observation", by(basin_id modate)
egen double temp2 = sd(gs_ws_depth) if QUES==0 & well_use_desc=="Observation", by(basin_id modate)
egen double gw_mth_bsn_mean3 = mean(temp1), by(basin_id modate)
egen double gw_mth_bsn_sd3 = mean(temp2), by(basin_id modate)
drop temp*
gen gw_mth_bsn_cnt3 = gs_ws_depth!=. & QUES==0 & well_use_desc=="Observation"

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

** Mean/sd by sub-basin/month, excluding questionable measurements and non-observational wells
egen double temp1 = mean(gs_ws_depth) if QUES==0 & well_use_desc=="Observation", by(basin_sub_id modate)
egen double temp2 = sd(gs_ws_depth) if QUES==0 & well_use_desc=="Observation", by(basin_sub_id modate)
egen double gw_mth_sub_mean3 = mean(temp1), by(basin_sub_id modate)
egen double gw_mth_sub_sd3 = mean(temp2), by(basin_sub_id modate)
drop temp*
gen gw_mth_sub_cnt3 = gs_ws_depth!=. & QUES==0 & well_use_desc=="Observation"

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

** Mean/sd by basin/quarter, excluding questionable measurements and non-observational wells
egen double temp1 = mean(gs_ws_depth) if QUES==0 & well_use_desc=="Observation", by(basin_id qtr)
egen double temp2 = sd(gs_ws_depth) if QUES==0 & well_use_desc=="Observation", by(basin_id qtr)
egen double gw_qtr_bsn_mean3 = mean(temp1), by(basin_id qtr)
egen double gw_qtr_bsn_sd3 = mean(temp2), by(basin_id qtr)
drop temp*
gen gw_qtr_bsn_cnt3 = gs_ws_depth!=. & QUES==0 & well_use_desc=="Observation"

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

** Mean/sd by sub-basin/quarter, excluding questionable measurements and non-observational wells
egen double temp1 = mean(gs_ws_depth) if QUES==0 & well_use_desc=="Observation", by(basin_sub_id qtr)
egen double temp2 = sd(gs_ws_depth) if QUES==0 & well_use_desc=="Observation", by(basin_sub_id qtr)
egen double gw_qtr_sub_mean3 = mean(temp1), by(basin_sub_id qtr)
egen double gw_qtr_sub_sd3 = mean(temp2), by(basin_sub_id qtr)
drop temp*
gen gw_qtr_sub_cnt3 = gs_ws_depth!=. & QUES==0 & well_use_desc=="Observation"

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

}

*******************************************************************************
*******************************************************************************

** 5. Rasterize panels of groundwater depth


quarterly(?) rasters of groundwater depth


2 to 20 measurements per 100 square miles
monthly is ideally what we want

{observation wells only with no issues; any use wells with no issues}
{rasters}
