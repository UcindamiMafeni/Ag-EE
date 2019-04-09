clear all
version 13
set more off

*********************************************************************************************
**** Script to process Parcel/CLU concordance, and aggregate units up to fields and farms ***
*********************************************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"
global dirpath_code "S:/Louis/backup/AgEE/AgEE_code/build"
*global R_exe_path "C:/PROGRA~1/MIE74D~1/ROPEN~1/bin/x64/R"
*global R_lib_path "C:/Program Files/Microsoft/R Open/R-3.4.4/library"

*******************************************************************************
*******************************************************************************

** 1. Import and clean parcel/CLU concordance 
{

** Run auxilary GIS script "BUILD_export_clu_parcel_conc.R" to export as csv

** Import parcel/CLU concordance
insheet using "$dirpath_data/misc/clu_parcel_conc.csv", double clear

** Merge in area for each parcel
preserve
insheet using "$dirpath_data/misc/Parcels_cleaned.csv", double clear
keep parcelid parcelacres
tempfile parcelacres
save `parcelacres'
restore
merge m:1 parcelid using `parcelacres', keep(1 3) nogen

** Merge in area for each CLU
preserve
insheet using "$dirpath_data/misc/CLUs_cleaned.csv", double clear
keep clu_id cluacres
tempfile cluacres
save `cluacres'
restore
merge m:1 clu_id using `cluacres', keep(1 3) nogen

** Confirm acres add up
assert round(intacres,0.01)<=round(parcelacres,0.01)
assert cluacres-intacres > -0.5

** Percents of intersected area
rename intperc pct_int_clu
gen pct_int_parcel = intacres/parcelacres

** Fix largest
replace largest = "1" if largest=="TRUE"
replace largest = "0" if largest=="FALSE"
destring largest, replace 
rename largest largest_parcel 
egen temp = max(pct_int_parcel), by(parcelid)
gen largest_clu = pct_int_parcel==temp
egen temp1 = sum(largest_clu), by(parcelid)
egen temp2 = sum(largest_parcel), by(clu_id)
egen temp3 = max(intacres), by(parcelid)
replace largest_clu = 0 if intacres<temp3 & temp1>1
egen temp4 = max(largest_clu), by(parcelid)
assert temp2==1
assert temp4==1 | intacres==0
drop temp*
 
** Label variables
la var parcelid "Unique parcel ID (county acres lon lat)"
la var clu_id "Unique CLU ID (county lon lat acres)"
la var parcelacres "Area of parcel (acres)"
la var cluacres "Area of CLU (acres)"
la var intacres "Acres of intersection b/tw CLU and parcel polygons"
la var pct_int_clu "Pct of CLU acres in intersection"
la var pct_int_parcel "Pct of parcel acres in intersection"
la var county "County name"
la var largest_parcel "Dummy for modal Parcel merged to this CLU"
la var largest_clu "Dummy for modal CLU merged to this parcel"

** Save
order county parcelid clu_id parcelacres cluacres intacres pct_int_parcel pct_int_clu ///
	largest_clu largest_parcel
sort *
unique parcelid clu_id
assert r(unique)==r(N)
compress
save "$dirpath_data/cleaned_spatial/clu_parcel_conc.dta", replace

}

*******************************************************************************
*******************************************************************************

** 2. Merge in SPs, reconcile parcels vs CLUs, establish panel units (ignoring pumps)

*******************************************************************************
*******************************************************************************

** 3. Merge in pumps, reconcile parcels vs CLUs, establish panel units (ignoring SPs)

*******************************************************************************
*******************************************************************************

** 4. Reconcile two sets of panel units vis a vis SP-pump merge 

*******************************************************************************
*******************************************************************************
