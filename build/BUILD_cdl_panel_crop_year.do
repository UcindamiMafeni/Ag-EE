clear all
version 13
set more off

*********************************************************************************************
**** Script to process Parcel/CLU concordance, and aggregate units up to fields and farms ***
*********************************************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"
global dirpath_code "T:/Home/Louis/backup/AgEE/AgEE_code/build"
*global R_exe_path "C:/PROGRA~1/MIE74D~1/ROPEN~1/bin/x64/R"
*global R_lib_path "C:/Program Files/Microsoft/R Open/R-3.4.4/library"

*******************************************************************************
*******************************************************************************

** 1. Import and clean CLU/CDL concordance 
{

** Run auxilary GIS script "BUILD_export_clu_cdl_conc.R" to export as csv

** Import parcel/CLU concordance
insheet using "$dirpath_data/misc/clu_cdl_conc.csv", double clear

** County
split clu_id, parse("-") gen(temp)
rename temp1 county
tab county
drop temp*

** Merge in area for each CLU
preserve
insheet using "$dirpath_data/misc/CLUs_cleaned.csv", double clear
keep clu_id cluacres
tempfile cluacres
save `cluacres'
restore
merge m:1 clu_id using `cluacres', keep(1 3) nogen
sort clu_id year

** Ditch count and total, which are in units of pixels
drop count total

** Confirm fractions sum to 1
egen temp = sum(fraction), by(clu_id year)
assert temp==1
drop temp

** Area by crop
gen landtype_acres = cluacres*fraction
preserve 
collapse (sum) landtype_acres, by(landtype)
gsort -landtype_acres
order landtype_acres
list
restore
preserve
collapse (sum) landtype_acres, by(landtype year)
gsort landtype year
order landtype year landtype_acres
list if inlist(landtype,"Almonds","Alfalfa")
restore

** Fix overlapping
replace overlapping = "1" if overlapping=="TRUE"
replace overlapping = "0" if overlapping=="FALSE"
destring overlapping, replace 
 
** Label variables
la var county "County name"
la var clu_id "Unique CLU ID (county lon lat acres)"
la var year "Year of Cropland Data Layer"
la var value "Numeric CDL code for land type"
la var landtype "CDL land type classification"
la var landtype_acres "Acres within CLU of given landtype"
la var cluacres "Area of CLU (acres)"
la var fraction "Fraction of CLU area of given landtype"
la var overlapping "Dummy = 1 if CLU overlaps another CLU"

** Save
order county clu_id year value landtype fraction landtype_acres cluacres
sort *
unique clu_id year value
assert r(unique)==r(N)
compress
save "$dirpath_data/cleaned_spatial/cdl_panel_crop_year.dta", replace

}

*******************************************************************************
*******************************************************************************

** 2. Classify and clean land types

*******************************************************************************
*******************************************************************************

** 3. Merge into parcels and units

*******************************************************************************
*******************************************************************************
