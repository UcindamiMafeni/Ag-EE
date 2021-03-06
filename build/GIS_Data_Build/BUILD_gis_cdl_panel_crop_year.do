clear all
version 13
set more off

******************************************************************
**** Script to build CLU[-group] by year panels of crop choice ***
******************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

*******************************************************************************
*******************************************************************************

** 1. Classify crops as annuals vs. perennials
{
	// Insheet crop categories, extracted from USDA PDF downloaded from
	// https://www.nrcs.usda.gov/wps/PA_NRCSConsumption/download?cid=stelprdb1262734&ext=pdf
insheet using "$dirpath_data/ag data/misc/USDA_annuals_perennials_list.csv", clear comma names	
duplicates drop
replace annualorperennial = "Annual" if annualorperennial=="Annuals"
replace annualorperennial = "Annual" if annualorperennial=="Considered Annual"
unique cropname
assert r(unique)==r(N)
tempfile usda_ap
save `usda_ap'
	
	// Isolate CDL land types
use "$dirpath_data/cleaned_spatial/cdl_panel_crop_year_full.dta", clear
keep landtype noncrop
duplicates drop

	// Prep CDL land types for merge with USDA crop list
gen temp = 1
replace temp = 2 if regexm(landtype,"Dbl Crop")
expand temp // expand double-crop land types, to merge each crop separately
sort landtype 
br
gen cropname = landtype
replace cropname = subinstr(cropname,"Dbl Crop ","",1)
replace cropname = substr(cropname,1,strpos(cropname,"/")-1) if temp==2 & landtype==landtype[_n+1]
replace cropname = substr(cropname,strpos(cropname,"/")+1,100) if temp==2 & landtype==landtype[_n-1]
replace cropname = "Almond" if cropname=="Almonds"
replace cropname = "Apple" if cropname=="Apples"
replace cropname = "Avocado" if cropname=="Avocados"
replace cropname = "Blueberry" if cropname=="Blueberries"
replace cropname = "Cherry" if cropname=="Cherries"
replace cropname = "Chickpeas" if cropname=="Chick Peas"
replace cropname = "Cranberry" if cropname=="Cranberries"
replace cropname = "Cucumber" if cropname=="Cucumbers"
replace cropname = "Eggplant" if cropname=="Eggplants"
replace cropname = "Olive" if cropname=="Olives"
replace cropname = "Onion" if cropname=="Onions"
replace cropname = "Peach" if cropname=="Peaches"
replace cropname = "Pear" if cropname=="Pears"
replace cropname = "Pecan" if cropname=="Pecans"
replace cropname = "Pistachio" if cropname=="Pistachios"
replace cropname = "Plum" if cropname=="Plums"
replace cropname = "Pomegranate" if cropname=="Pomegranates"
replace cropname = "Prune" if cropname=="Prunes"
replace cropname = "Sugar Beets" if cropname=="Sugarbeets"
replace cropname = "Sunflowers" if cropname=="Sunflower"
replace cropname = "Walnut" if cropname=="Walnuts"
replace cropname = "Melons" if inlist(cropname,"Cantaloupe","Cantaloupes", ///
	"Honeydew Melons","Watermelons")
replace cropname = "Wheat" if inlist(cropname,"Durum Wheat","Durum Wht", ///
	"Spring Wheat","WinWht","Winter Wheat")
replace cropname = "Popcorn" if cropname=="Pop or Orn Corn"
*replace cropname = "Clover" if cropname=="Clover/Wildflowers"
replace cropname = "Citrus" if cropname=="Oranges" // hilariously missing
replace cropname = "Triticale" if cropname=="Triticale/Corn"
drop temp

	// Merge CDL and USDA crop lists
merge m:1 cropname using `usda_ap'
assert _merge==2 if type=="Horticulture"
assert _merge==1 if noncrop==1 // confirm noncrops are actually noncrops
tab landtype if  _merge==1 & noncrop==0

	// Keep master only
drop if _merge==2
duplicates drop
drop type _merge
sort cropname

	// Fix missing/ambiguous Annual/Perennial 
br if !inlist(annualorperennial,"Annual","Perennial")
replace annualorperennial = "Annual" if cropname=="Other Small Grains" // obvious
replace annualorperennial = "Perennial" if cropname=="Other Tree Crops" // obvious
replace annualorperennial = "Perennial" if cropname=="Alfalfa" // grown as a short perennial
replace annualorperennial = "Annual" if cropname=="Celery" // grown as annual, actualy biennal
*replace annualorperennial = "Perennial" if cropname=="Clover" // conflicting information here
	// but perennial (3 years) in the eyes of the most recent Davis cost study (1991)
*replace annualorperennial = "Annual" if cropname=="Fallow/Idle Cropland" // vacuously annual
replace annualorperennial = "Annual" if cropname=="Herbs" // basil, parsley, cilantro are all annual	
replace annualorperennial = "Annual" if cropname=="Misc Vegs & Fruits" // default to annual
replace annualorperennial = "Annual" if cropname=="Other Crops" // default to annual	
replace annualorperennial = "Perennial" if cropname=="Other Hay/Non Alfalfa" // most hays are perennial
	// https://www.nrcs.usda.gov/internet/FSE_DOCUMENTS/nrcs144p2_016364.pdf
*replace annualorperennial = "Annual" if cropname=="Sod/Grass Seed" // ambiguous, but sod gets ripped up

	// Assign non-crops as either "cropable" or "not cropable"
replace annualorperennial = "Cropable" if noncrop==1 & cropname=="Clover/Wildflowers"
replace annualorperennial = "Cropable" if noncrop==1 & cropname=="Fallow/Idle Cropland"
replace annualorperennial = "Cropable" if noncrop==1 & cropname=="Grass/Pasture"
replace annualorperennial = "Cropable" if noncrop==1 & cropname=="Sod/Grass Seed"
	// all of landtypes would require cutting down forests, major land conversion, etc.
replace annualorperennial = "Not Cropable" if noncrop==1 & annualorperennial==""
assert inlist(annualorperennial,"Annual","Perennial","Cropable","Not Cropable")	

	// Make unique by land type
gen perennial = annualorperennial=="Perennial"
gen cropable = annualorperennial!="Not Cropable"
drop annualorperennial cropname
duplicates drop
unique landtype
assert r(unique)==r(N)

	// Sort, label, and save
sort landtype	
la var perennial "Dummy for perennial crops (default is annual where ambiguous)"
la var cropable "Dummy for land that either has crops or could potential switch into crops"
compress
save "$dirpath_data/cleaned_spatial/landtype_perennial.dta", replace

}

*******************************************************************************
*******************************************************************************

** 2. Classify crops by fruit, vegetable, grain, feed, etc.
{
	// Collapse CDL acreage by land type
use "$dirpath_data/cleaned_spatial/cdl_panel_crop_year_full.dta", clear
collapse (sum) landtype_acres, by(landtype noncrop) fast

	// Define some broad categories
gen landtype_cat = ""
replace landtype_cat = "feed" if inlist(landtype,"Alfalfa","Other Hay/Non Alfalfa", ///"Clover/Wildflowers", ///
	"Camelina","Vetch","Triticale","Triticale/Corn")
replace landtype_cat = "nuts" if inlist(landtype,"Almonds","Walnuts","Pecans","Pistachios")
replace landtype_cat = "fruit trees" if inlist(landtype,"Apples","Apricots","Avocados","Cherries","Citrus","Nectarines")
replace landtype_cat = "fruit trees" if inlist(landtype,"Peaches","Plums","Pomegranates","Pears","Prunes","Oranges", ///
	"Olives","Other Tree Crops")
replace landtype_cat = "grapes" if inlist(landtype,"Grapes")
replace landtype_cat = "other fruit" if inlist(landtype,"Blueberries","Cantaloupes","Cranberries", ///
	"Honeydew Melons", "Strawberries","Watermelons","Caneberries")
replace landtype_cat = "vegetables" if inlist(landtype,"Asparagus","Broccoli","Cabbage","Carrots","Cauliflower")
replace landtype_cat = "vegetables" if inlist(landtype,"Celery","Chick Peas","Cucumbers","Eggplants","Greens","Peas","Pumpkins")
replace landtype_cat = "vegetables" if inlist(landtype,"Potatoes","Onions","Garlic","Radishes","Lettuce","Squash")
replace landtype_cat = "vegetables" if inlist(landtype,"Sweet Potatoes","Potatoes","Tomatoes","Turnips","Peppers")
replace landtype_cat = "vegetables" if inlist(landtype,"Misc Vegs & Fruits","Dry Beans","Herbs","Mint","Lentils")
replace landtype_cat = "cereal" if inlist(landtype,"Barley","Corn","Dbl Crop Barley/Corn","Dbl Crop Barley/Sorghum")
replace landtype_cat = "cereal" if inlist(landtype,"Dbl Crop Durum Wht/Sorghum","Dbl Crop Oats/Corn","Rice","Rye")
replace landtype_cat = "cereal" if inlist(landtype,"Dbl Crop WinWht/Corn","Dbl Crop WinWht/Sorghum","Durum Wheat")
replace landtype_cat = "cereal" if inlist(landtype,"Millet","Oats","Sorghum","Spring Wheat","Winter Wheat", ///
	"Sweet Corn","Other Small Grains","Pop or Orn Corn")
replace landtype_cat = "fallow" if inlist(landtype,"Fallow/Idle Cropland")
replace landtype_cat = "other" if inlist(landtype,"Canola","Mustard","Safflower","Sunflower","Sugarbeets","Sugarcane")
replace landtype_cat = "other" if inlist(landtype,"Cotton","Other Crops","Christmas Trees","Soybeans") //,"Sod/Grass Seed")
replace landtype_cat = "vegetables" if regexm(landtype,"Dbl Crop Lettuce/") // assign based on first double crop
replace landtype_cat = "cereal" if regexm(landtype,"Dbl Crop WinWht/") // assign based on first double crop
replace landtype_cat = "cropable grass" if inlist(landtype,"Clover/Wildflowers","Grass/Pasture","Sod/Grass Seed")
replace landtype_cat = "not cropable" if noncrop==1 & landtype_cat==""
assert landtype_cat!=""

	// Acreage by category
preserve
collapse (sum) landtype_acres, by(landtype_cat)
gsort -landtype_acres
order landtype_cat
egen sum = sum(landtype_acres)
gen pct = landtype_acres/sum
drop sum
list
restore

	// Acreage by crop within category
sum landtype_acres
local sum1 = r(sum)
levelsof landtype_cat, local(levs)
foreach cat in `levs' {
	preserve
	keep if landtype_cat=="`cat'"
	gsort -landtype_acres
	order landtype_cat landtype
	egen sum2 = sum(landtype_acres)
	gen pct_cat = landtype_acres/sum2
	gen pct_all = landtype_acres/`sum1'
	drop sum2
	list
	restore
}

	// Make cotton its own category
replace landtype_cat = "cotton" if landtype=="Cotton" // it's quite big!

	// Combine two fruit categories, since "other fruit" is small
*replace landtype_cat = "fruit" if inlist(landtype_cat,"fruit trees","other fruit")	
	// Turning this off now that we might want to make a distintion between vines and trees

replace landtype_cat = proper(landtype_cat)

	// Compare within and across category percentages
preserve
sum landtype_acres
local sum1 = r(sum)
gen pct_all = landtype_acres/`sum1'
replace landtype = "<1% of total" if pct_all<0.01
collapse (sum) landtype_acres pct_all, by(landtype_cat landtype) fast
order landtype_cat landtype landtype_acres pct_all
gen temp = word(landtype,1)=="<1%"
gsort temp -landtype_acres
drop temp landtype_acres
list
restore
	
	// Store percent of total crop acreage
sum landtype_acres
gen pct_total_crop_acres = landtype_acres/r(sum)
	
	// Label and save categories
la var landtype_acres "Total acreage of landtype, summed over all years"
la var landtype_cat "Land type category (our own, not definitive)"
la var pct_total_crop_acres "Total landtype acres / total crop acres (all years, all CLUs)"
order landtype_acres
sort landtype_cat landtype_acres
compress
save "$dirpath_data/cleaned_spatial/landtype_categories.dta", replace
	

}

*******************************************************************************
*******************************************************************************

** 3. CLU-by-year panels (long)
{
	// Start with full panel
use "$dirpath_data/cleaned_spatial/cdl_panel_crop_year_full.dta", clear
keep county clu_id year landtype fraction landtype_acres cluacres acres_crop ///
	fraction_crop ever_crop_pct

	// Deal with slivers?
qui count
local N = r(N)
qui count if fraction<=0.01 & landtype_acres<0.5 // 16.0%
di r(N)/`N'
qui count if fraction<=0.01 & landtype_acres<0.3 // 12.5%
di r(N)/`N'
qui count if fraction<=0.01 & landtype_acres<0.2 // 0.0%
di r(N)/`N'	
qui count if fraction<=0.05 & landtype_acres<0.2 // 0.0%
di r(N)/`N'	
qui count if fraction<=0.10 & landtype_acres<0.2 // 0.1%
di r(N)/`N'	
qui count if fraction<=0.20 & landtype_acres<0.2 // 0.1%
di r(N)/`N'	
qui count if fraction<=0.20 & landtype_acres<0.4 // 22.8%
di r(N)/`N'	
qui count if fraction<=0.20 & landtype_acres<0.6 // 33.4%
di r(N)/`N'	
qui count if fraction<=0.20 & landtype_acres<0.8 // 41.0%
di r(N)/`N'	
qui count if fraction<=0.20 & landtype_acres<1.0 // 45.7%
di r(N)/`N'	
	// Keeping slivers for now, they're not that small and drop off precipitously
	// (this is a sign that the CLU polygons align with discreet boundaries in the 
	// Cropland Data Layer images, which is very good news)
		
	// Merge in perennial and cropable dummies
merge m:1 landtype using "$dirpath_data/cleaned_spatial/landtype_perennial.dta"
assert _merge==3
drop _merge	
	
	// Merge in land type categories
merge m:1 landtype using "$dirpath_data/cleaned_spatial/landtype_categories.dta"
assert _merge==3
drop _merge	

	// Save panel: CLU*year, long, by crop
sort county clu_id year landtype landtype_cat
order county clu_id year landtype landtype_cat 	
unique clu_id year landtype
assert r(unique)==r(N)
duplicates r clu_id year
compress
save "$dirpath_data/cleaned_spatial/CDL_panel_clu_crop_year_long.dta", replace

	// Collapse to categories and perennial
use "$dirpath_data/cleaned_spatial/CDL_panel_clu_crop_year_long.dta", clear
tab landtype_cat perennial
foreach v of varlist fraction landtype_acres {
	egen double temp = sum(`v'), by(county clu_id year landtype_cat noncrop perennial cropable)
	replace `v' = temp
	drop temp
}
drop landtype pct_total_crop_acres
duplicates drop	
la var fraction "Fraction of CLU area of given category/perennial"
la var landtype_acres "Acres within CLU of given category/perennial"
rename landtype_acres cat_peren_acres
	
	// Save panel: CLU*year, long, by category/perennal
sort county clu_id year landtype_cat perennial
order county clu_id year landtype_cat perennial
unique clu_id year landtype_cat perennial
assert r(unique)==r(N)
duplicates r clu_id year
compress
save "$dirpath_data/cleaned_spatial/CDL_panel_clu_cat_year_long.dta", replace

	// Define broader land categories
use "$dirpath_data/cleaned_spatial/CDL_panel_clu_cat_year_long.dta", clear
tab landtype_cat perennial
gen landtype_bigcat = ""
replace landtype_bigcat = "Not Cropable" if inlist(landtype_cat,"Not Cropable")
replace landtype_bigcat = "Noncrop" if inlist(landtype_cat,"Fallow","Cropable Grass")
replace landtype_bigcat = "Annual" if perennial==0 & landtype_bigcat==""
replace landtype_bigcat = "Fruit/Nut Perennial" if inlist(landtype_cat,"Fruit Trees","Grapes","Nuts","Other Fruit") & perennial==1
replace landtype_bigcat = "Other Perennial" if perennial==1 & landtype_bigcat==""
tab landtype_cat landtype_bigcat, missing
assert landtype_bigcat!=""
	
	// Collapse to broader land categories
foreach v of varlist fraction cat_peren_acres {
	egen double temp = sum(`v'), by(county clu_id year landtype_bigcat)
	replace `v' = temp
	drop temp
}
drop landtype_cat perennial
duplicates drop	
la var landtype_bigcat "Broad land type category (our own, not definitive)"
la var fraction "Fraction of CLU area of given broad category"
la var cat_peren_acres "Acres within CLU of given broad category"
rename cat_peren_acres bigcat_acres
	
	// Save panel: CLU*year, long, by broad category
sort county clu_id year landtype_bigcat
order county clu_id year landtype_bigcat
unique clu_id year landtype_bigcat
assert r(unique)==r(N)
duplicates r clu_id year
compress
save "$dirpath_data/cleaned_spatial/CDL_panel_clu_bigcat_year_long.dta", replace

}

*******************************************************************************
*******************************************************************************

** 4. CLU-by-year panel, using broad land categories (wide)
{
	// Load panel: CLU*year, long, by broad category
use "$dirpath_data/cleaned_spatial/CDL_panel_clu_bigcat_year_long.dta", clear

	// Shorten strings pre-reshape
tab landtype_bigcat
replace landtype = subinstr(landtype," ","",.)
replace landtype = subinstr(landtype,"/","",.)
compress
tab landtype

	// Drop redundant cropable and noncrop dummies
tab landtype cropable, missing
tab landtype noncrop, missing
drop cropable noncrop

	// Reshape wide: categories
rename fraction frac_
rename bigcat_acres acres_
rename acres_crop crop_acres
reshape wide frac_ acres_, i(county clu_id year cluacres crop_acres ///
	fraction_crop ever_crop_pct) j(landtype_bigcat) string
	
	// Switch missings to zeros
foreach v of varlist frac_* acres_* {
	replace `v' = 0 if `v'==.
	qui sum `v'
	if r(max)==0 {
		di "`v'"
		assert 2+2==5
	}
}	
	
	// Confirm acres and fractions add up
egen temp_f = rowtotal(frac_*)
egen temp_a = rowtotal(acres_*)
assert round(abs(temp_f-1),0.000001)==0
assert round(abs(temp_a-cluacres),0.1)==0
drop temp*
	
	// Fix labels
foreach v of varlist frac_* {
	local v2 = subinstr("`v'","_"," ",.)
	local vbigcat = word("`v2'",2)
	la var `v' "Fraction of CLU area: `vbigcat'"
}
foreach v of varlist acres_* {
	local v2 = subinstr("`v'","_"," ",.)
	local vbigcat = word("`v2'",2)
	la var `v' "Acres of CLU: `vbigcat'"
}
la var cluacres "Total CLU acres (full polygon)"
la var crop_acres "Total CLU acres with crop in year t"
gen temp = acres_Annual+acres_FruitNutPerennial+acres_OtherPerennial
assert abs(temp-crop_acres)<0.001
drop temp

	// Remove fallow acres (i.e. potential crop acres) from crop acres
/*
gen crop_acres_planted = acres_Annual+acres_FruitNutPerennial+acres_OtherPerennial
assert crop_acres_planted>-0.001
replace crop_acres_planted = 0 if crop_acres_planted<0
la var crop_acres_planted "Total CLU acres with crop planted in year t"
la var fraction_crop "Fraction of CLU acres with crops or fallow in year t"
gen fraction_crop_planted = frac_Annual+frac_FruitNutPerennial+frac_OtherPerennial
assert fraction_crop_planted>-0.0000001
replace fraction_crop_planted = 0 if fraction_crop_planted<0
assert fraction_crop_planted<1.000001
replace fraction_crop_planted = 1 if fraction_crop_planted>1
la var fraction_crop_planted "Fraction of CLU acres with crops planted in year t"
order crop_acres_planted, after(crop_acres)
order fraction_crop_planted, after(fraction_crop)
*/

	// Modal land category by CLU-year	
foreach c in Annual FruitNutPerennial Noncrop NotCropable OtherPerennial {
	egen double temp_`c' = rowtotal(frac_`c'*)
}
egen double temp_mode = rowmax(temp_*)
sum temp_mode, detail
foreach c in Annual FruitNutPerennial Noncrop NotCropable OtherPerennial {
	gen mode_`c' = temp_mode==temp_`c' & temp_mode>0
	la var mode_`c' "Dummy for `c' as modal land category in year"
}
egen temp_mode_check = rowmax(mode_*)
tab temp_mode_check
tab temp_mode_check if temp_mode>0
assert temp_mode_check==1 if temp_mode>0
drop temp*

	// Modal land category with >=50% by CLU-year
foreach c in Annual FruitNutPerennial Noncrop NotCropable OtherPerennial {
	gen mode50_`c' = mode_`c'*(frac_`c'>0.5)
	la var mode50_`c' "Dummy for `c' >50% in year"
}
	
	// Ever-category dummies
foreach c in Annual FruitNutPerennial Noncrop NotCropable OtherPerennial {
	egen double temp_sumF = rowtotal(frac_`c'*)
	egen double temp_sumA = rowtotal(acres_`c'*)
	egen double temp_maxF = max(temp_sumF), by(clu_id)
	egen double temp_maxA = max(temp_sumA), by(clu_id)
	gen ever_`c' = temp_maxF>0.20 | temp_maxA>1
	la var ever_`c' "Dummy for CLU ever having >20% or >1 acre = `c'"
	drop temp*
}	
sum ever*

	// Ever 50% dummies for land categories
foreach c in Annual FruitNutPerennial Noncrop NotCropable OtherPerennial {
	egen double temp_sumF = rowtotal(frac_`c'*)
	egen double temp_sumA = rowtotal(acres_`c'*)
	egen double temp_maxF = max(temp_sumF), by(clu_id)
	egen double temp_maxA = max(temp_sumA), by(clu_id)
	gen ever50_`c' = temp_maxF>0.50 
	la var ever50_`c' "Dummy for CLU ever having >50% = `c'"
	drop temp*
}
sum ever*
	
	// Mode-switcher indicator
foreach c in Annual FruitNutPerennial Noncrop NotCropable OtherPerennial {
	egen temp_`c' = max(mode_`c'), by(clu_id)
}
egen temp_sum = rowtotal(temp_*)
gen mode_switcher = temp_sum>1 
la var mode_switcher "Dummy for CLUs where yearly mode ever switches land categories"	
tab temp_sum
sum mode_switcher
drop temp*

	// Mode-50-switcher indicator
foreach c in Annual FruitNutPerennial Noncrop NotCropable OtherPerennial {
	egen temp_`c' = max(mode50_`c'), by(clu_id)
}
egen temp_sum = rowtotal(temp_*)
gen mode50_switcher = temp_sum>1 
la var mode50_switcher "Dummy for CLUs where >50% ever switches land categories"	
tab temp_sum
sum mode50_switcher
drop temp*
	
	// Save panel: CLU*year, wide, by category/perennal
sort county clu_id year
order frac_Annual frac_FruitNutPerennial frac_OtherPerennial frac_Noncrop frac_NotCropable ///
	acres_Annual acres_FruitNutPerennial acres_OtherPerennial acres_Noncrop acres_NotCropable, after(ever_crop_pct)
order mode_switcher, after(mode_OtherPerennial)
order mode50_switcher, after(mode50_OtherPerennial)
unique clu_id year
assert r(unique)==r(N)
compress
save "$dirpath_data/cleaned_spatial/CDL_panel_clu_bigcat_year_wide.dta", replace

}

*******************************************************************************
*******************************************************************************

** 5. CLU-by-year panel, using crop categories (wide)
{
	// Load panel: CLU*year, long, by category/perennial
use "$dirpath_data/cleaned_spatial/CDL_panel_clu_cat_year_long.dta", clear

	// Reshape wide: categories
tab landtype_cat
rename fraction frac_
rename cat_peren_acres acres_
rename acres_crop crop_acres
drop noncrop cropable // redundant
replace landtype_cat = subinstr(landtype_cat," ","",1)
reshape wide frac_ acres_, i(county clu_id year perennial cluacres crop_acres ///
	fraction_crop ever_crop_pct) j(landtype_cat) string

	// Reshape wide: perennial
tostring perennial, replace
replace perennial = "_A" if perennial=="0"	
replace perennial = "_P" if perennial=="1"
reshape wide frac_* acres_*, i(county clu_id year cluacres crop_acres ///
	fraction_crop ever_crop_pct) j(perennial) string
	
	// Drop always-zeros
foreach v of varlist frac_* acres_* {
	replace `v' = 0 if `v'==.
	qui sum `v'
	if r(max)==0 {
		di "`v'"
		drop `v'
	}
}	
rename *CropableGrass_A *CropableGrass
rename *Fallow_A *Fallow
rename *NotCropable_A *NotCropable
	
	// Confirm acres and fractions add up
egen temp_f = rowtotal(frac_*)
gen temp_cf = temp_f - frac_Fallow - frac_CropableGrass - frac_NotCropable
egen temp_a = rowtotal(acres_*)
gen temp_ca = temp_a - acres_Fallow - acres_CropableGrass - acres_NotCropable
assert round(abs(temp_f-1),0.000001)==0
assert round(abs(temp_cf-fraction_crop),0.000001)==0
assert round(abs(temp_a-cluacres),0.1)==0
assert round(abs(temp_ca-crop_acres),0.1)==0
drop temp*
	
	// Fix labels
foreach v of varlist frac_* {
	local v2 = subinstr("`v'","_"," ",.)
	local vcat = word("`v2'",2)
	local vap = word("`v2'",3)
	if "`vap'"=="A" {
		local vap2 = ", Annual"
	}
	else if "`vap'"=="P" {
		local vap2 = ", Perennial"
	}
	else {
		local vap2 = ""
	}
	la var `v' "Fraction of CLU area: `vcat'`vap2'"
}
foreach v of varlist acres_* {
	local v2 = subinstr("`v'","_"," ",.)
	local vcat = word("`v2'",2)
	local vap = word("`v2'",3)
	if "`vap'"=="A" {
		local vap2 = ", Annual"
	}
	else if "`vap'"=="P" {
		local vap2 = ", Perennial"
	}
	else {
		local vap2 = ""
	}
	la var `v' "Acres of CLU: `vcat'`vap2'"
}

	// Modal crop category by CLU-year (excluding noncrops)	
foreach c in Cereal Cotton Feed FruitTrees Grapes Nuts Other OtherFruit Vegetables {
	egen double temp_`c' = rowtotal(frac_`c'*)
}
egen double temp_mode = rowmax(temp_*)
sum temp_mode, detail
foreach c in Cereal Cotton Feed FruitTrees Grapes Nuts Other OtherFruit Vegetables {
	gen mode_`c' = temp_mode==temp_`c' & temp_mode>0
	la var mode_`c' "Dummy for `c' as modal crop category in year (excl noncrops)"
}
egen temp_mode_check = rowmax(mode_*)
tab temp_mode_check
tab temp_mode_check if temp_mode>0
assert temp_mode_check==1 if temp_mode>0
drop temp*
	
	// Modal crop category by CLU-year (including Fallow but no other noncrops)
foreach c in Cereal Cotton Feed FruitTrees Grapes Nuts Other OtherFruit Vegetables Fallow {
	egen double temp_`c' = rowtotal(frac_`c'*)
}
egen double temp_mode = rowmax(temp_*)
sum temp_mode, detail
gen mode_Fallow = temp_mode==temp_Fallow & temp_mode>0
la var mode_Fallow "Dummy for Fallow as modal category in year (excl other noncrops)"
drop temp*

	// Modal crop category by CLU-year (including all noncrops)
foreach c in Cereal Cotton Feed FruitTrees Grapes Nuts Other OtherFruit Vegetables {
	egen double temp_`c' = rowtotal(frac_`c'*)
}
gen tempNoncropAny = frac_Fallow + frac_CropableGrass + frac_NotCropable
gen tempNoncropCropable = frac_Fallow + frac_CropableGrass
egen double tempMode1 = rowmax(temp_* tempNoncropAny)
egen double tempMode2 = rowmax(temp_* tempNoncropCropable)
egen double tempMode3 = rowmax(temp_* frac_Fallow frac_CropableGrass frac_NotCropable)
sum tempMode?, detail
gen mode_Noncrop = inlist(tempMode1,tempNoncropAny) & tempMode1>0
la var mode_Noncrop "Dummy for Noncrop (pooled) as modal category in year"
gen mode_Cropable = inlist(tempMode2,tempNoncropCropable) & tempMode2>0
la var mode_Cropable "Dummy for cropable Noncrop (pooled) as modal category in year"
gen mode_NotCropable = inlist(tempMode3,frac_NotCropable) & tempMode3>0
la var mode_NotCropable "Dummy for not-cropable Noncrop  as modal category in year"
drop temp*
	
	// Modal crop is perennial (excludling all Noncrop)
foreach c in _A _P {
	egen double temp`c' = rowtotal(frac_*`c')
}
egen double temp_mode = rowmax(temp_*)
sum temp_mode, detail
gen mode_P = temp_mode==temp_P & temp_mode>0
la var mode_P "Dummy for perennial > annual acres in year (excl Fallow and Noncrop)"
sum mode_P
sum mode_P if temp_mode>0
drop temp*
	
	// Ever-category dummies
foreach c in Cereal Cotton Feed FruitTrees Grapes Nuts Other OtherFruit Vegetables {
	egen double temp_sumF = rowtotal(frac_`c'*)
	egen double temp_sumA = rowtotal(acres_`c'*)
	egen double temp_maxF = max(temp_sumF), by(clu_id)
	egen double temp_maxA = max(temp_sumA), by(clu_id)
	gen ever_`c' = temp_maxF>0.20 | temp_maxA>1
	la var ever_`c' "Dummy for CLU ever having >20% or >1 acre = `c'"
	drop temp*
}	
sum ever*
	
	// Clarlfy two labels
la var cluacres "Total CLU acres (full polygon)"
la var crop_acres "Total CLU acres with crop in year t"
	
/*	
	// Remove fallow acres (i.e. potential crop acres) from crop acres
gen crop_acres_planted = crop_acres - acres_Fallow_A
assert crop_acres_planted>-0.001
replace crop_acres_planted = 0 if crop_acres_planted<0
la var crop_acres_planted "Total CLU acres with crop planted in year t"
la var fraction_crop "Fraction of CLU acres with crops or fallow in year t"
gen fraction_crop_planted = fraction_crop - frac_Fallow_A
assert fraction_crop_planted>-0.0000001
replace fraction_crop_planted = 0 if fraction_crop_planted<0
assert fraction_crop_planted<1.000001
replace fraction_crop_planted = 1 if fraction_crop_planted>1
la var fraction_crop_planted "Fraction of CLU acres with crops planted in year t"
order crop_acres_planted, after(crop_acres)
order fraction_crop_planted, after(fraction_crop)
*/
	
	// Mode-switcher indicator
foreach c in Cereal Cotton Feed FruitTrees Grapes Nuts Other OtherFruit Vegetables {
	egen temp_`c' = max(mode_`c'), by(clu_id)
}
egen temp_sum = rowtotal(temp_*)
gen mode_switcher = temp_sum>1 
la var mode_switcher "Dummy for CLUs where yearly mode ever switches crop categories"	
tab temp_sum
sum mode_switcher
drop temp*
	
	// Save panel: CLU*year, wide, by category/perennal
sort county clu_id year
order frac_Cereal_* frac_Cotton_* frac_Feed_* frac_FruitTrees_* ///
	frac_Grapes_* frac_Nuts_* frac_OtherFruit_* frac_Vegetables_* frac_Other_* ///
	frac_Fallow frac_CropableGrass frac_NotCropable ///
	acres_Cereal_* acres_Cotton_* acres_Feed_* acres_FruitTrees_* ///
	acres_Grapes_* acres_Nuts_* acres_OtherFruit_* acres_Vegetables_* acres_Other_* ///
	acres_Fallow acres_CropableGrass acres_NotCropable, after(ever_crop_pct)
order mode_switcher, after(mode_P)
unique clu_id year
assert r(unique)==r(N)
compress
save "$dirpath_data/cleaned_spatial/CDL_panel_clu_cat_year_wide.dta", replace

}

*******************************************************************************
*******************************************************************************

** 6. CLU-by-year panel, using individual crops (wide)
{
	// Load panel: CLU*year, long, by crop
use "$dirpath_data/cleaned_spatial/CDL_panel_clu_crop_year_long.dta", clear

	// Aggregate small crops up to category/perennial
egen temp_tag = tag(landtype)
tab pct_total_crop_acres if temp_tag	
egen temp_denom_year = sum(landtype_acres), by(year)
egen temp_num_year = sum(landtype_acres), by(landtype year)
gen temp_share_year = temp_num_year/temp_denom_year
egen temp_share_max = max(temp_share_year), by(landtype)
tab landtype if pct_total_crop_acres>=0.01 
tab landtype if pct_total_crop_acres<0.01 & temp_share_max>=0.01
tab temp_share_year year if landtype=="Plums" // max was in 2008
tab temp_share_year year if landtype=="Safflower" // max was in 2008
	// Using the ever >1% criterion isn't obscuring any crops that peaked late
drop temp*
tostring perennial, replace
replace perennial = "A" if perennial=="0"
replace perennial = "P" if perennial=="1"
replace landtype = "Other " + landtype_cat + " " + perennial if pct_total_crop_acres<0.01 & noncrop==0

	// Shorten strings pre-reshape
tab landtype
replace landtype = subinstr(landtype,"Dbl Crop ","",1)
replace landtype = subinstr(landtype,"/Non Alfalfa","",1)
replace landtype = subinstr(landtype,"/Idle Cropland","",1)
replace landtype = subinstr(landtype,"Developed/","",1)
replace landtype = subinstr(landtype,"/Pasture","",1)
replace landtype = "Evergreen" if landtype=="Evergreen Forest"
replace landtype = subinstr(landtype,"Vegetables","Veg",1)
replace landtype = subinstr(landtype,"Other Other","Other",1)
replace landtype = subinstr(landtype," ","_",.) if word(landtype,1)=="Other" & landtype!="Other Hay"
replace landtype = subinstr(landtype," ","",.)
replace landtype = subinstr(landtype,"/","",.)
compress
tab landtype

	// Combined not-cropable landtypes
tab landtype if cropable==0
replace landtype = "NotCropable" if cropable==0

	// Collapse by new landtypes
foreach v of varlist fraction landtype_acres {
	egen double temp = sum(`v'), by(county clu_id year landtype)
	replace `v' = temp
	drop temp
}
drop pct_total_crop_acres
duplicates drop
unique clu_id year landtype
assert r(unique)==r(N)
drop landtype_cat perennial noncrop cropable

	// Reshape wide: landtype
rename fraction frac_
rename landtype_acres acres_
rename acres_crop crop_acres
reshape wide frac_ acres_, i(county clu_id year cluacres crop_acres fraction_crop ///
	ever_crop_pct) j(landtype) string
	
	// Switch missings to zeros
foreach v of varlist frac_* acres_* {
	replace `v' = 0 if `v'==.
	qui sum `v'
	if r(max)==0 {
		di "`v'"
		assert 2+2==5
	}
}	
	
	// Confirm acres and fractions add up
egen temp_f = rowtotal(frac_*)
egen temp_a = rowtotal(acres_*)	
assert round(abs(temp_f-1),0.000001)==0
assert round(abs(temp_a-cluacres),0.1)==0
drop temp*
	
	// Fix labels
foreach v of varlist frac_* {
	local v2 = subinstr("`v'","_"," ",.)
	local vcat = word("`v2'",2)
	if word("`v2'",2)=="Other" {
		local vcat = trim(subinstr(subinstr("`v2'",word("`v2'",1),"",1),word("`v2'",wordcount("`v2'")),"",1))
	}
	local vap = word("`v2'",wordcount("`v2'"))
	if "`vap'"=="A" {
		local vap2 = ", Annual"
	}
	else if "`vap'"=="P" {
		local vap2 = ", Perennial"
	}
	else {
		local vap2 = ""
	}
	la var `v' "Fraction of CLU area: `vcat'`vap2'"
}
foreach v of varlist acres_* {
	local v2 = subinstr("`v'","_"," ",.)
	local vcat = word("`v2'",2)
	if word("`v2'",2)=="Other" {
		local vcat = trim(subinstr(subinstr("`v2'",word("`v2'",1),"",1),word("`v2'",wordcount("`v2'")),"",1))
	}
	local vap = word("`v2'",wordcount("`v2'"))
	if "`vap'"=="A" {
		local vap2 = ", Annual"
	}
	else if "`vap'"=="P" {
		local vap2 = ", Perennial"
	}
	else {
		local vap2 = ""
	}
	la var `v' "Acres of CLU: `vcat'`vap2'"
}

	// Clarify variable label
la var cluacres "Total CLU acres (full polygon)"

/*
	// Remove fallow acres (i.e. potential crop acres) from crop acres
la var crop_acres "Total CLU acres with crop or fallow in year t"
gen crop_acres_planted = crop_acres - acres_Fallow
assert crop_acres_planted>-0.001
replace crop_acres_planted = 0 if crop_acres_planted<0
la var crop_acres_planted "Total CLU acres with crop planted in year t"
la var fraction_crop "Fraction of CLU acres with crops or fallow in year t"
gen fraction_crop_planted = fraction_crop - frac_Fallow
assert fraction_crop_planted>-0.0000001
replace fraction_crop_planted = 0 if fraction_crop_planted<0
assert fraction_crop_planted<1.000001
replace fraction_crop_planted = 1 if fraction_crop_planted>1
la var fraction_crop_planted "Fraction of CLU acres with crops planted in year t"
order crop_acres_planted, after(crop_acres)
order fraction_crop_planted, after(fraction_crop)
*/
	
	// Ever dummies for specific crops
foreach c in Alfalfa Almonds Corn Cotton Grapes OtherHay Pistachios Rice Tomatoes Walnuts Wh {	
	egen double temp_sumF = rowtotal(frac_*`c'*)
	egen double temp_sumA = rowtotal(acres_*`c'*)
	egen double temp_maxF = max(temp_sumF), by(clu_id)
	egen double temp_maxA = max(temp_sumA), by(clu_id)
	if "`c'"=="Wh" {
		local c = "Wheat"
	}
	gen ever_`c' = temp_maxF>0.20 | temp_maxA>1
	la var ever_`c' "Dummy for CLU ever having >20% or >1 acre = `c'"
	drop temp*
}
sum ever*

	// Ever 50% dummies for specific crops
foreach c in Alfalfa Almonds Corn Cotton Grapes OtherHay Pistachios Rice Tomatoes Walnuts Wh {	
	egen double temp_sumF = rowtotal(frac_*`c'*)
	replace temp_sumF = temp_sumF/fraction_crop
	replace temp_sumF = 0 if fraction_crop==0
	egen double temp_maxF = max(temp_sumF>0.50), by(clu_id)
	if "`c'"=="Wh" {
		local c = "Wheat"
	}
	gen ever50_`c' = temp_maxF>0.50 
	la var ever50_`c' "Dummy for CLU ever having >=50% crop/fallow acres = `c'"
	drop temp*
}
sum ever*

	// Ever 50% switcher dummies
sort clu_id year
br clu_id year ever50*	
egen temp = rowtotal(ever50*)	
egen temp_tag = tag(clu_id)
tab temp if temp_tag
rename temp ever50_count 
la var ever50_count "Number of distinct, specific crops with ever >=50% crop/fallow acres for CLU"
drop temp*

	// Save panel: CLU*year, wide, by crop (>1% only)
sort county clu_id year
order frac_*, after(ever_crop_pct)
order frac_Other_*, after(frac_WinterWheat)
order acres_Other_*, after(acres_WinterWheat)
order frac_Other_A frac_Other_P, after(frac_Other_Veg_P)
order acres_Other_A acres_Other_P, after(acres_Other_Veg_P)
order frac_Fallow frac_Grass frac_CloverWildflowers frac_SodGrassSeed frac_NotCropable, after(frac_Other_P)
order acres_Fallow acres_Grass acres_CloverWildflowers acres_SodGrassSeed acres_NotCropable, after(acres_Other_P)
unique clu_id year
assert r(unique)==r(N)
compress
save "$dirpath_data/cleaned_spatial/CDL_panel_clu_crop_year_wide.dta", replace

}

*******************************************************************************
*******************************************************************************

** 7. Group-by-year panel, using broad land categories (wide)
{
	// Merge in CLU group identifiers
use "$dirpath_data/cleaned_spatial/CDL_panel_clu_bigcat_year_wide.dta", clear
drop county // GIS-assigned county, which conflicts with raw counties in CLU-parcel concordance
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_conc_groups.dta", ///
	keep(1 3) keepusing(clu_group*)

	// Diagnostics
unique clu_id
local uniq = r(unique)
unique clu_id if _merge==1	
di r(unique)/`uniq' // 9% of CLUs don't have a group

unique clu_id if ever_crop_pct==1
local uniq = r(unique)
unique clu_id if ever_crop_pct==1 & _merge==1
di r(unique)/`uniq' // 5% of ever-crop CLUs don't have a group
tab _merge ever_crop_pct

	// Drop unmerged CLUs: they are CLUs that don't show up at all in parcel concordance
keep if _merge==3
drop _merge

	// Collapse and save, looping over CLU group identifiers
foreach s in 0 10 25 50 75 {
	
	preserve
	
	// Drop other clu_group identifiers
	foreach s2 in 0 10 25 50 75 {
		if `s2'!=`s' {
			drop clu_group`s2'
		}
	}
	
	// Sum acres variables
	foreach v of varlist *acres* {
		egen double temp = sum(`v'), by(clu_group`s' year)
		replace `v' = temp
		local vl: variable label `v'
		local vl2 = subinstr("`vl'","CLU","CLU group",.)
		la var `v' "`vl2'"
		drop temp
	}
	rename cluacres clu_group`s'_acres

	// Recalculate fraction variables
	foreach v of varlist frac_* {
		local vA = subinstr("`v'","frac_","acres_",1)
		gen double temp = `vA'/clu_group`s'_acres
		replace `v' = temp
		local vl: variable label `v'
		local vl2 = subinstr("`vl'","CLU","CLU group",.)
		la var `v' "`vl2'"
		drop temp
	}
	replace fraction_crop = crop_acres/clu_group`s'_acres
	*replace fraction_crop_planted = crop_acres_planted/clu_group`s'_acres
	la var fraction_crop "Fraction of CLU group acres with crops or fallow in year t"
	*la var fraction_crop_planted "Fraction of CLU group acres with crops planted in year t"

	// Take max of mode variables 
	foreach v of varlist mode_* {
		egen double temp = max(`v'), by(clu_group`s' year)
		replace `v' = temp
		local vl: variable label `v'
		local vl2 = subinstr("`vl'","in year","in any CLU in year",.)
		la var `v' "`vl2'"
		drop temp
	}
	la var mode_switcher "Dummy for any CLU where yearly mode ever switches crop categories"
	
	// Take max of mode-50 variables 
	foreach v of varlist mode50_* {
		egen double temp = max(`v'), by(clu_group`s' year)
		replace `v' = temp
		local vl: variable label `v'
		local vl2 = subinstr("`vl'","in year","in any CLU in year",.)
		la var `v' "`vl2'"
		drop temp
	}
	la var mode50_switcher "Dummy for any CLU where yearly >50% ever switches crop categories"

	// Take max of ever variables 
	foreach v of varlist ever_* {
		egen double temp = max(`v'), by(clu_group`s' year)
		replace `v' = temp
		local vl: variable label `v'
		local vl2 = subinstr("`vl'","CLU","any CLU",.)
		la var `v' "`vl2'"
		drop temp
	}
	
	// Take max of ever-50 variables 
	foreach v of varlist ever50_* {
		egen double temp = max(`v'), by(clu_group`s' year)
		replace `v' = temp
		local vl: variable label `v'
		local vl2 = subinstr("`vl'","CLU","any CLU",.)
		la var `v' "`vl2'"
		drop temp
	}
	
	// Collapse
	drop clu_id
	duplicates drop
	unique clu_group`s' year
	assert r(unique)==r(N)
	
	// Reorder, sort, and save
	order clu_group`s' year
	sort clu_group`s' year
	compress
	save "$dirpath_data/cleaned_spatial/CDL_panel_clugroup`s'_bigcat_year_wide.dta", replace
	
	restore
}	

}

*******************************************************************************
*******************************************************************************

** 8. Group-by-year panel, using crop categories (wide)
{
	// Merge in CLU group identifiers
use "$dirpath_data/cleaned_spatial/CDL_panel_clu_cat_year_wide.dta", clear
drop county // GIS-assigned county, which conflicts with raw counties in CLU-parcel concordance
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_conc_groups.dta", ///
	keep(1 3) keepusing(clu_group*)

	// Diagnostics
unique clu_id
local uniq = r(unique)
unique clu_id if _merge==1	
di r(unique)/`uniq' // 9% of CLUs don't have a group

unique clu_id if ever_crop_pct==1
local uniq = r(unique)
unique clu_id if ever_crop_pct==1 & _merge==1
di r(unique)/`uniq' // 5% of ever-crop CLUs don't have a group
tab _merge ever_crop_pct

	// Drop unmerged CLUs: they are CLUs that don't show up at all in parcel concordance
keep if _merge==3
drop _merge

	// Collapse and save, looping over CLU group identifiers
foreach s in 0 10 25 50 75 {
	
	preserve
	
	// Drop other clu_group identifiers
	foreach s2 in 0 10 25 50 75 {
		if `s2'!=`s' {
			drop clu_group`s2'
		}
	}
	
	// Sum acres variables
	foreach v of varlist *acres* {
		egen double temp = sum(`v'), by(clu_group`s' year)
		replace `v' = temp
		local vl: variable label `v'
		local vl2 = subinstr("`vl'","CLU","CLU group",.)
		la var `v' "`vl2'"
		drop temp
	}
	rename cluacres clu_group`s'_acres

	// Recalculate fraction variables
	foreach v of varlist frac_* {
		local vA = subinstr("`v'","frac_","acres_",1)
		gen double temp = `vA'/clu_group`s'_acres
		replace `v' = temp
		local vl: variable label `v'
		local vl2 = subinstr("`vl'","CLU","CLU group",.)
		la var `v' "`vl2'"
		drop temp
	}
	replace fraction_crop = crop_acres/clu_group`s'_acres
	*replace fraction_crop_planted = crop_acres_planted/clu_group`s'_acres
	la var fraction_crop "Fraction of CLU group acres with crops or fallow in year t"
	*la var fraction_crop_planted "Fraction of CLU group acres with crops planted in year t"

	// Take max of mode variables 
	foreach v of varlist mode_* {
		egen double temp = max(`v'), by(clu_group`s' year)
		replace `v' = temp
		local vl: variable label `v'
		local vl2 = subinstr("`vl'","in year","in any CLU in year",.)
		la var `v' "`vl2'"
		drop temp
	}
	la var mode_switcher "Dummy for any CLU where yearly mode ever switches crop categories"

	// Take max of ever variables 
	foreach v of varlist ever_* {
		egen double temp = max(`v'), by(clu_group`s' year)
		replace `v' = temp
		local vl: variable label `v'
		local vl2 = subinstr("`vl'","CLU","any CLU",.)
		la var `v' "`vl2'"
		drop temp
	}
	
	// Collapse
	drop clu_id
	duplicates drop
	unique clu_group`s' year
	assert r(unique)==r(N)
	
	// Reorder, sort, and save
	order clu_group`s' year
	sort clu_group`s' year
	compress
	save "$dirpath_data/cleaned_spatial/CDL_panel_clugroup`s'_cat_year_wide.dta", replace
	
	restore
}	

}

*******************************************************************************
*******************************************************************************

** 9. Group-by-year panel, using individual crops (wide)
{
	// Merge in CLU group identifiers
use "$dirpath_data/cleaned_spatial/CDL_panel_clu_crop_year_wide.dta", clear
drop county // GIS-assigned county, which conflicts with raw counties in CLU-parcel concordance
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_conc_groups.dta", ///
	keep(1 3) keepusing(clu_group*)

	// Diagnostics
unique clu_id
local uniq = r(unique)
unique clu_id if _merge==1	
di r(unique)/`uniq' // 9% of CLUs don't have a group

unique clu_id if ever_crop_pct==1
local uniq = r(unique)
unique clu_id if ever_crop_pct==1 & _merge==1
di r(unique)/`uniq' // 5% of ever-crop CLUs don't have a group
tab _merge ever_crop_pct

	// Drop unmerged CLUs: they are CLUs that don't show up at all in parcel concordance
keep if _merge==3
drop _merge

	// Collapse and save, looping over CLU group identifiers
foreach s in 0 10 25 50 75 {
	
	preserve
	
	// Drop other clu_group identifiers
	foreach s2 in 0 10 25 50 75 {
		if `s2'!=`s' {
			drop clu_group`s2'
		}
	}
	
	// Sum acres variables
	foreach v of varlist *acres* {
		egen double temp = sum(`v'), by(clu_group`s' year)
		replace `v' = temp
		local vl: variable label `v'
		local vl2 = subinstr("`vl'","CLU","CLU group",.)
		la var `v' "`vl2'"
		drop temp
	}
	rename cluacres clu_group`s'_acres

	// Recalculate fraction variables
	foreach v of varlist frac_* {
		local vA = subinstr("`v'","frac_","acres_",1)
		gen double temp = `vA'/clu_group`s'_acres
		replace `v' = temp
		local vl: variable label `v'
		local vl2 = subinstr("`vl'","CLU","CLU group",.)
		la var `v' "`vl2'"
		drop temp
	}
	replace fraction_crop = crop_acres/clu_group`s'_acres
	*replace fraction_crop_planted = crop_acres_planted/clu_group`s'_acres
	la var fraction_crop "Fraction of CLU group acres with crops or fallow in year t"
	*la var fraction_crop_planted "Fraction of CLU group acres with crops planted in year t"

	// Take max of ever variables 
	foreach v of varlist ever_* ever50_* {
		egen double temp = max(`v'), by(clu_group`s' year)
		replace `v' = temp
		local vl: variable label `v'
		local vl2 = subinstr("`vl'","CLU","any CLU",.)
		la var `v' "`vl2'"
		drop temp
	}
	
	// Collapse
	drop clu_id
	duplicates drop
	unique clu_group`s' year
	assert r(unique)==r(N)
	
	// Reorder, sort, and save
	order clu_group`s' year
	sort clu_group`s' year
	compress
	save "$dirpath_data/cleaned_spatial/CDL_panel_clugroup`s'_crop_year_wide.dta", replace
	
	restore
}	

}

*******************************************************************************
*******************************************************************************
