clear all
version 13
set more off

*******************************************************************************
**** Script to process/clean Parcel/CLU concordance and CLU/CDL concordance ***
*******************************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"
global dirpath_code "T:/Home/Louis/backup/AgEE/AgEE_code/build"
*global R_exe_path "C:/PROGRA~1/MIE74D~1/ROPEN~1/bin/x64/R"
*global R_lib_path "C:/Program Files/Microsoft/R Open/R-3.4.4/library"

*******************************************************************************
*******************************************************************************

** 1. Run auxilary GIS script "BUILD_gis_export_clu_parcel_concs.R" to export 
**    all RDS files as csvs

*******************************************************************************
*******************************************************************************

** 2. Import and clean CLU/CDL concordance 
{

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
assert round(temp,0.000001)==1
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
 
** Flag ever-crop CLUs
gen noncrop = 0
replace noncrop = 1 if landtype=="Aquaculture"
replace noncrop = 1 if landtype=="Background"
replace noncrop = 1 if landtype=="Barren"
replace noncrop = 1 if landtype=="Deciduous Forest"
replace noncrop = 1 if landtype=="Developed/High Intensity"
replace noncrop = 1 if landtype=="Developed/Low Intensity"
replace noncrop = 1 if landtype=="Developed/Med Intensity"
replace noncrop = 1 if landtype=="Developed/Open Space"
replace noncrop = 1 if landtype=="Evergreen Forest"
replace noncrop = 1 if landtype=="Forest"
replace noncrop = 1 if landtype=="Grass/Pasture"
replace noncrop = 1 if landtype=="Herbaceous Wetlands"
replace noncrop = 1 if landtype=="Mixed Forest"
replace noncrop = 1 if landtype=="Open Water"
replace noncrop = 1 if landtype=="Perennial Ice/Snow"
replace noncrop = 1 if landtype=="Shrubland"
replace noncrop = 1 if landtype=="Wetlands"
replace noncrop = 1 if landtype=="Woody Wetlands"
egen ever_crop = max(noncrop==0), by(clu_id)
unique clu_id
local uniq = r(unique)
unique clu_id if ever_crop==1
di r(unique)/`uniq' // 96% of CLUs

** Strengthen this definition to remove the tiny slivers of CLUs of a given landtype
gen noncrop_nontrivial = noncrop
replace noncrop_nontrivial = . if fraction<0.01 & landtype_acres<1
egen ever_crop_nontrivial = max(noncrop_nontrivial==0), by(clu_id)
unique clu_id
local uniq = r(unique)
unique clu_id if ever_crop_nontrivial==1
di r(unique)/`uniq' // 95% of CLUs

** Create continuous versions
egen fraction_crop = sum((1-noncrop)*fraction), by(clu_id year)
egen temp_tag = tag(clu_id year)
hist fraction_crop if temp_tag
sum fraction_crop if temp_tag, detail
gen acres_crop = fraction_crop*cluacres
hist acres_crop if temp_tag
sum acres_crop if temp_tag, detail
unique clu_id
local uniq = r(unique)
unique clu_id if fraction_crop>0.2
di r(unique)/`uniq' // 89% of CLUs
unique clu_id if fraction_crop>0.2 | acres_crop>1
di r(unique)/`uniq' // 93% of CLUs

** Flag CLUs that never meet either 20% or 1 acre crop thresholds
egen ever_crop_pct = max((fraction_crop>0.2) | (acres_crop>1)), by(clu_id)
sum ever_crop_pct
tabstat cluacres, by(ever_crop_pct) s(sum)
tabstat landtype_acres, by(noncrop) s(sum) // 50% of acres are non-crop
tabstat landtype_acres if noncrop==1, by(ever_crop_pct) s(sum) 
tabstat landtype_acres if noncrop==0, by(ever_crop_pct) s(sum)
	// 7% of acres are non-crop, in non-ever-crop CLUs

** Label variables
la var county "County name"
la var clu_id "Unique CLU ID (county lon lat acres)"
la var year "Year of Cropland Data Layer"
la var value "Numeric CDL code for land type"
la var landtype "CDL land type classification"
la var landtype_acres "Acres within CLU of given landtype"
la var cluacres "Area of CLU (acres)"
la var fraction "Fraction of CLU area of given landtype"
la var overlapping "Dummy if CLU overlaps another CLU"
la var noncrop "Dummy for non-crop landtypes"
la var ever_crop "Dummy if CLU ever has nonzero crop acreage"
la var noncrop_nontrivial "Dummy for non-crop landtypes > 1% or 1 acre"
la var ever_crop_nontrivial "Dummy if CLU ever has nontrivial crop acreage"
la var fraction_crop "Fraction of CLU acreage with crops in year t"
la var acres_crop "Total CLU acres with crop in year t"
la var ever_crop_pct "Dummy if CLU ever has crop coverage >20% or >1 acre"  
drop temp*

** Save
order county clu_id year value landtype fraction landtype_acres cluacres ///
	acres_crop fraction_crop ever_crop ever_crop_nontrivial ever_crop_pct ///
	noncrop noncrop_nontrivial overlapping
sort *
unique clu_id year value
assert r(unique)==r(N)
compress
save "$dirpath_data/cleaned_spatial/cdl_panel_crop_year_full.dta", replace

** Export list of ever_crop_pct CLUs
use "$dirpath_data/cleaned_spatial/cdl_panel_crop_year_full.dta", clear
keep if ever_crop_pct==1
keep clu_id
duplicates drop
outsheet using "$dirpath_data/misc/ever_crop_clus.csv", comma replace

}

*******************************************************************************
*******************************************************************************

** 3. Import and clean parcel/CLU concordance 
{

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
gen temp1 = parcelacres-intacres
assert temp1>-0.01
gen temp2 = cluacres-intacres
assert temp2>-0.5
drop temp*

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
assert temp2>=1 // every CLUs has at least 1 largest parcel
unique clu_id
local uniq = r(unique)
unique clu_id if temp2>1
di r(unique)/`uniq' // 0.1% of CLUs have a tie for largest parcel
egen temp3 = max(intacres), by(parcelid)
replace largest_clu = 0 if intacres<temp3 & temp1>1
egen temp4 = max(largest_clu), by(parcelid)
assert temp4==1 | intacres==0
drop temp*
 
** Merge in indicator for ever-crop CLUs
preserve
insheet using "$dirpath_data/misc/ever_crop_clus.csv", double clear names
tempfile clus
save `clus'
restore
merge m:1 clu_id using `clus', keep(1 3) gen(merge_ever_crop_clus)
unique parcelid
unique parcelid if merge_ever_crop_clus==3
gen ever_crop_clu = merge_ever_crop_clus==3 
 
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
la var ever_crop_clu "Dummy for CLU ever has crop coverage >20% or >1 acre"

** Save master concordance dataset
order county parcelid clu_id parcelacres cluacres intacres pct_int_parcel pct_int_clu ///
	largest_clu largest_parcel
sort *
unique parcelid clu_id
assert r(unique)==r(N)
compress
save "$dirpath_data/cleaned_spatial/clu_parcel_conc.dta", replace

** Isolate parcels with more than 1% of TOTAL area or more than 1 TOTAL acre in an ever-crop CLU
use "$dirpath_data/cleaned_spatial/clu_parcel_conc.dta", clear
collapse (sum) sum_pct_int_parcel=pct_int_parcel sum_intacres=intacres ///
	(max) max_intacres=intacres max_largest_parcel=largest_parcel ///
	(count) n_clus=largest_clu, by(county parcelid parcelacres ever_crop_clu) fast
tab ever_crop_clu
unique parcelid if ever_crop_clu==1 // 360660 parcels matched to an ever-crop CLU
egen temp = max(ever_crop_clu), by(parcelid)
unique parcelid if temp==0 // 43980 parcels never matched to an ever-crop CLU (10%)
drop temp
keep if ever_crop_clu==1 // keep only parcels that match to an ever-crop CLU
unique parcelid
assert r(unique)==r(N)
hist sum_pct_int_parcel
sum sum_pct_int_parcel, detail
sum sum_intacres, detail
sum max_largest_parcel
preserve
keep if (sum_pct_int_parcel>0.1 & sum_pct_int_parcel!=.) /// keep if >10% of area overlaps with an ever-crop CLU
	  | (sum_intacres>1 & sum_intacres!=.) /// OR if >1 acres of area overlaps with an ever-crop CLU
	  | (max_largest_parcel==1) // OR if modal parcel in any single ever-crop CLU
hist sum_pct_int_parcel
sum sum_pct_int_parcel, detail
restore
tempfile parcels_slivers
save `parcels_slivers'

** Merge back into concordance dataset and save
use "$dirpath_data/cleaned_spatial/clu_parcel_conc.dta", clear
merge m:1 parcelid using `parcels_slivers', nogen  
la var sum_pct_int_parcel "Pct of parcel acres in ANY ever-crop CLU intersection"
la var sum_intacres "Sum of parcel acres in ANY ever-crop CLU intersection"
la var max_intacres "Largest ever-crop CLU intersection for parcel"
la var max_largest_parcel "Is parcel ever the model parcel in an ever-crop CLU?"
la var n_clus "Number of ever-crop CLUs that parcel is matched to"
sort *
cap drop merge_ever_crop_clus

** Flag parcels to drop because their TOTAL intersections are too small
gen drop_parcelid_total = !( ///
	(sum_pct_int_parcel>0.1 & sum_pct_int_parcel!=.) /// keep if >10% of area overlaps with an ever-crop CLU
	| (sum_intacres>1 & sum_intacres!=.) /// OR if >1 acres of area overlaps with an ever-crop CLU
	| (max_largest_parcel==1) /// OR if modal parcel in any single ever-crop CLU
	)
unique parcelid
local uniq = r(unique)
unique parcelid if drop_parcelid_total==0
di r(unique)/`uniq' // 76% of parcels will remain
unique clu_id if ever_crop_clu==1
local uniq = r(unique)
unique clu_id if ever_crop_clu==1 & drop_parcelid_total==0
assert r(unique)==`uniq' // confirm no ever-crop CLUs will get fully dropped

** What's a sliver? Want to isolate bad matches induced by measurement error!
sum intacres, detail
hist intacres if intacres<5, freq
hist intacres if intacres<1, freq
hist intacres if intacres<0.5, freq
hist intacres if intacres<0.5 & drop_parcelid_total==0, freq
hist intacres if intacres<1 & drop_parcelid_total==0 & (intacres>0.02 | parcelacres<1), freq
hist intacres if intacres<1 & drop_parcelid_total==0 & (intacres>0.04 | parcelacres<1), freq
hist intacres if intacres<1 & drop_parcelid_total==0 & (intacres>0.08 | parcelacres<1), freq
hist intacres if intacres<1 & drop_parcelid_total==0 & (intacres>0.12 | parcelacres<1), freq
hist intacres if intacres<1 & drop_parcelid_total==0 & (intacres>0.16 | parcelacres<1), freq
hist intacres if intacres<1 & drop_parcelid_total==0 & (intacres>0.18 | parcelacres<1), freq
hist intacres if intacres<1 & drop_parcelid_total==0 & (intacres>0.20 | parcelacres<1), freq
hist intacres if intacres<1 & drop_parcelid_total==0 & (intacres>0.18 | parcelacres<1) & cluacres>1, freq
hist intacres if intacres<1 & drop_parcelid_total==0 & (intacres>0.18 | parcelacres<1) & cluacres>1 & largest_parcel==0, freq
	// can't kill the weird peak around 0.14
sum intacres if intacres<1 & drop_parcelid_total==0 & (intacres>0.18 | parcelacres<1), detail	
tabstat	intacres if intacres<1 & drop_parcelid_total==0 & (intacres>0.18 | parcelacres<1), ///
	by(county) s(p50 count)
hist intacres if intacres<0.5 & county=="Kern", freq
hist intacres if intacres<0.5 & county=="Fresno", freq
hist intacres if intacres<0.5 & county=="Tulare", freq
hist intacres if intacres<0.5 & county=="San Joaquin", freq
	// shows up in some counties, not in others
gen temp_ratio = parcelacres/intacres
sum temp_ratio if inrange(intacres,0.14,0.17), detail // AHA! it's just a local mode in parcel size!
sum temp_ratio if inrange(intacres,0.24,0.27), detail // % of 1's falls by half!
	// NOT a problem, thankfully!
hist intacres if intacres<1 & drop_parcelid_total==0 & (intacres>0.05 | parcelacres<.15) & temp_ratio>1.5, freq
local sliver = 0.05 // define slivers as less than 50 feet x 50 feet, quite conservative

** Flag parcels-CLU pairs with only slivers of area
gen temp_int_sliver = intacres<`sliver' 
unique parcelid 
local uniq = r(unique)
unique parcelid if temp_int_sliver==0
di 1 - r(unique)/`uniq' // 8% of parcels never have more than a sliver in any sigle CLU
unique clu_id 
local uniq = r(unique)
unique clu_id if temp_int_sliver==0
di 1 - r(unique)/`uniq' // 0.05% of CLUs never have more than a sliver in any sigle parcel

** Flag parcels that never merge more than a sliver
egen temp_int_sliver_minp = min(temp_int_sliver), by(parcelid)
egen temp_tag_p = tag(parcelid)
sum parcelacres if temp_int_sliver_minp==1 & temp_tag_p, detail

** Confirm largest parcel flag works
egen temp_lp_flag = max(largest_parcel), by(clu_id)
assert temp_lp_flag==1

** Flag parcels to drop because their MAX intersections are too small
gen drop_slivers = temp_int_sliver // drop all slivers (i.e. <0.05 acres)
replace drop_slivers = 0 if parcelacres<3*`sliver' // but not if total parcel area < 3 slivers
replace drop_slivers = 1 if intacres<0.01 // but yes if area is less than 20 feet x 20 feet 
replace drop_slivers = 0 if largest_parcel==1 // and not if largest parcel matched to a CLU
unique parcelid
local uniq = r(unique)
unique parcelid if drop_parcelid_total==0 & drop_slivers==0
di r(unique)/`uniq' // 76% of parcels will remain
unique clu_id if ever_crop_clu==1
local uniq = r(unique)
unique clu_id if ever_crop_clu==1 & drop_parcelid_total==0 & drop_slivers==0
assert r(unique)==`uniq' // confirm no ever-crop CLUs will get fully dropped

** Clean up
tab drop*
la var drop_parcelid_total "Parcel's total ever-crop merged area is <10% and <1 acre"
la var drop_slivers "Intersection too tiny: <0.01 acres or (<0.05 acres & parcel <0.15 acres)"
drop temp*
sort *
compress
save "$dirpath_data/cleaned_spatial/clu_parcel_conc.dta", replace

** Export list of ever-CLU-matched parcels to csv
use "$dirpath_data/cleaned_spatial/clu_parcel_conc.dta", clear
unique parcelid
drop if drop_parcelid_total==1
unique parcelid
drop if drop_slivers==1
unique parcelid
keep parcelid
duplicates drop
outsheet using "$dirpath_data/misc/parcels_in_clus.csv", comma replace


}

*******************************************************************************
*******************************************************************************

