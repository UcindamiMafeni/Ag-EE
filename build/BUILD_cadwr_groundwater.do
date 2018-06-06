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

*******************************************************************************
*******************************************************************************

** 1. Main groundwater level (GWL) dataset
{
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
rename coop_agency_org_id coorp_org_id

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

** Save
sort site_code date
compress
save "$dirpath_data/groundwater/ca_dwr_gwl.dta", replace
}

*******************************************************************************
*******************************************************************************

** 2. Main groundwater station (GST) dataset
{
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
order basin_region*, after(basin_desc)

** Save
compress
save "$dirpath_data/groundwater/ca_dwr_gst.dta", replace
}

*******************************************************************************
*******************************************************************************

** 3. Add in 

// PENDING
	
	* supplement with county-specific data pulls from CA DWR, if necessary???
