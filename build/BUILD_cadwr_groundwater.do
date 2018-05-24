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

** Clean and label
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
hist year
hist year if inrange(year,2000,2018)
hist date if inrange(year,2000,2018)

la var rp_elevation "Reference point elevation (feet)"
assert rp_elevation!=.
sum rp_elevation, detail

la var gs_elevation "Ground surface elevation (feet)"
count if gs_elevation==. // 46 missings out of 1.6M
sum gs_elevation, detail
correlate rp_elevation gs_elevation
gen temp = gs_elevation-rp_elevation
sum temp, detail // very close for almost all observations
drop temp

la var ws_reading "Water surface reading (feet)"
sum ws_reading, detail // mostly zeros???
count if ws_reading==. // 149107 missings out of 1.6M

la var rp_reading "Reference point reading (feet)"
sum rp_reading, detail
coun if rp_reading==. // 150708 missings out of 1.6M

la var measurement_issue_id "Measurement problem code"
tab measurement_issue_id, missing // 17% populated, 26 unique issues!

la var measurement_method_id "Measurement method code"
tab measurement_method_id, missing // 90% populated, 7 unique methods

la var measurement_accuracy_id "Measurement accuracy code"
tab measurement_accuracy_id, missing // 90% populated, 5 unique accuracy codes

la var casgem_reading "Reading is a casgem submittal"
tab casgem_reading, missing

la var org_id "Monitoring enity agency code"
la var org_name "Monitoring anity agency name"
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

** Save
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
la var longitude "station longitude"
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
egen temp_group1 = group(basin_cd)
egen temp_group2 = group(basin_cd basin_desc)
assert temp_group1==temp_group2 // redundant as an identifier, BUT is also lat/lon, so i'll keep it for now
drop temp_group1 temp_group2 
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

** Save
compress
save "$dirpath_data/groundwater/ca_dwr_gst.dta", replace
}

*******************************************************************************
*******************************************************************************

// PENDING
	
	* figure out from Andrew if these are the right data! 
	* incorproate measurement accuracy codes, etc
	* supplement with county-specific data pulls from CA DWR, if necessary???s
