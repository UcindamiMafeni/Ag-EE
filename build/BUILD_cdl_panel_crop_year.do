clear all
version 13
set more off

******************************************************************
**** Script to build CLU[-group] by year panels of crop choice ***
******************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"
global dirpath_code "T:/Home/Louis/backup/AgEE/AgEE_code/build"
*global R_exe_path "C:/PROGRA~1/MIE74D~1/ROPEN~1/bin/x64/R"
*global R_lib_path "C:/Program Files/Microsoft/R Open/R-3.4.4/library"

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
use  "$dirpath_data/cleaned_spatial/cdl_panel_crop_year_full.dta", clear
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
replace cropname = "Blueberry" if cropname=="Blueberries"
replace cropname = "Cherry" if cropname=="Cherries"
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
replace cropname = "Clover" if cropname=="Clover/Wildflowers"
replace cropname = "Citrus" if cropname=="Oranges" // hilariously missing
drop temp

	// Merge CDL and USDA crop lists
merge m:1 cropname using `usda_ap'
assert _merge==2 if type=="Horticulture"
assert _merge==1 if noncrop==1 // confirm noncrops are actually noncrops
drop if noncrop==1
drop noncrop

	// Two obvious replacements
sort cropname _merge
list cropname if _merge==1	
replace annualorperennial = "Annual" if cropname=="Other Small Grains" 
replace annualorperennial = "Perennial" if cropname=="Other Tree Crops" 

	// Keep master only
drop if _merge==2
duplicates drop
drop type _merge
sort cropname

	// Fix missing/ambiguous Annual/Perennial 
br if !inlist(annualorperennial,"Annual","Perennial")
replace annualorperennial = "Perennial" if cropname=="Alfalfa" // grown as a short perennial
replace annualorperennial = "Annual" if cropname=="Celery" // grown as annual, actualy biennal
replace annualorperennial = "Perennial" if cropname=="Clover" // conflicting information here
	// but perennial (3 years) in the eyes of the most recent Davis cost study (1991)
replace annualorperennial = "Annual" if cropname=="Fallow/Idle Cropland" // vacuously annual
replace annualorperennial = "Annual" if cropname=="Herbs" // basil, parsley, cilantro are all annual	
replace annualorperennial = "Annual" if cropname=="Misc Vegs & Fruits" // default to annual
replace annualorperennial = "Annual" if cropname=="Other Crops" // default to annual	
replace annualorperennial = "Perennial" if cropname=="Other Hay/Non Alfalfa" // most hays are perennial
	// https://www.nrcs.usda.gov/internet/FSE_DOCUMENTS/nrcs144p2_016364.pdf
replace annualorperennial = "Annual" if cropname=="Sod/Grass Seed" // ambiguous, but sod gets ripped up
assert inlist(annualorperennial,"Annual","Perennial")	
gen perennial = annualorperennial=="Perennial"

	// Make unique by land type
drop annualorperennial cropname
duplicates drop
unique landtype
assert r(unique)==r(N)

	// Sort, label, and save
sort landtype	
la var perennial "Dummy for perennial crops (default is annual where ambiguous)"
compress
save "$dirpath_data/cleaned_spatial/landtype_perennial.dta", replace

}

*******************************************************************************
*******************************************************************************

** 2. Classify crops by fruit, vegetable, grain, feed, etc.
{
	// Collapse CDL acreage by land type
use  "$dirpath_data/cleaned_spatial/cdl_panel_crop_year_full.dta", clear
collapse (sum) landtype_acres, by(landtype noncrop) fast

	// Drop noncrop land types
drop if noncrop==1
drop noncrop

	// Define some broad categories
gen landtype_cat = ""
replace landtype_cat = "feed" if inlist(landtype,"Alfalfa","Other Hay/Non Alfalfa","Clover/Wildflowers", ///
	"Camelina","Vetch","Triticale")
replace landtype_cat = "nuts" if inlist(landtype,"Almonds","Walnuts","Pecans","Pistachios")
replace landtype_cat = "fruit trees" if inlist(landtype,"Apples","Apricots","Cherries","Citrus","Nectarines")
replace landtype_cat = "fruit trees" if inlist(landtype,"Peaches","Plums","Pomegranates","Pears","Prunes","Oranges", ///
	"Olives","Other Tree Crops")
replace landtype_cat = "grapes" if inlist(landtype,"Grapes")
replace landtype_cat = "other fruit" if inlist(landtype,"Blueberries","Cantaloupes","Cranberries", ///
	"Honeydew Melons", "Strawberries","Watermelons","Caneberries")
replace landtype_cat = "vegetables" if inlist(landtype,"Asparagus","Broccoli","Cabbage","Carrots","Cauliflower")
replace landtype_cat = "vegetables" if inlist(landtype,"Celery","Cucumbers","Eggplants","Greens","Peas","Pumpkins")
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
replace landtype_cat = "other" if inlist(landtype,"Cotton","Other Crops","Sod/Grass Seed","Christmas Trees","Soybeans")
replace landtype_cat = "vegetables" if regexm(landtype,"Dbl Crop Lettuce/") // assign based on first double crop
replace landtype_cat = "cereal" if regexm(landtype,"Dbl Crop WinWht/") // assign based on first double crop
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
replace landtype_cat = "fruit" if inlist(landtype_cat,"fruit trees","other fruit")	
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
	
	// Label and save categories
la var landtype_acres "Total acreage of landtype, summed over all years"
la var landtype_cat "Land type category (our own, not definitive)"
order lantdtype_acres
sort landtype_cat landtype_acres
compress
save "$dirpath_data/cleaned_spatial/landtype_categories.dta", replace
	

}

*******************************************************************************
*******************************************************************************

	// Start with full (mostly raw) CLU-crop-year panel
use  "$dirpath_data/cleaned_spatial/cdl_panel_crop_year_full.dta", clear

	// Classify crop types: annual vs perennial
	// soure: https://www.google.com/url?sa=t&rct=j&q=&esrc=s&source=web&cd=11&ved=2ahUKEwiItqO0747iAhVAHzQIHbFjA1QQFjAKegQIABAC&url=https%3A%2F%2Fwww.nrcs.usda.gov%2Fwps%2FPA_NRCSConsumption%2Fdownload%3Fcid%3Dstelprdb1262734%26ext%3Dpdf&usg=AOvVaw3Iyv4darXgyq27B9Db0v99
tab landtype if noncrop==0




                    Alfalfa |    932,140       10.45       10.45
                    Almonds |    926,268       10.39       20.84
                     Apples |      9,677        0.11       20.95
                   Apricots |      4,590        0.05       21.00
                  Asparagus |      5,085        0.06       21.06
                     Barley |    100,105        1.12       22.18
                Blueberries |      3,908        0.04       22.23
                   Broccoli |      6,755        0.08       22.30
                    Cabbage |      2,271        0.03       22.33
                   Camelina |        209        0.00       22.33
                Caneberries |        473        0.01       22.34
                     Canola |        970        0.01       22.35
                Cantaloupes |     29,929        0.34       22.68
                    Carrots |     26,783        0.30       22.98
                Cauliflower |      1,125        0.01       22.99
                     Celery |        578        0.01       23.00
                   Cherries |     91,180        1.02       24.02
            Christmas Trees |        271        0.00       24.03
                     Citrus |     67,376        0.76       24.78
         Clover/Wildflowers |     68,524        0.77       25.55
                       Corn |    244,471        2.74       28.29
                     Cotton |    151,592        1.70       29.99
                Cranberries |         24        0.00       29.99
                  Cucumbers |     10,634        0.12       30.11
       Dbl Crop Barley/Corn |      2,803        0.03       30.14
    Dbl Crop Barley/Sorghum |      1,303        0.01       30.16
 Dbl Crop Durum Wht/Sorghum |        109        0.00       30.16
    Dbl Crop Lettuce/Barley |         59        0.00       30.16
Dbl Crop Lettuce/Cantaloupe |      1,007        0.01       30.17
    Dbl Crop Lettuce/Cotton |        346        0.00       30.18
 Dbl Crop Lettuce/Durum Wht |      1,959        0.02       30.20
         Dbl Crop Oats/Corn |    111,014        1.25       31.44
       Dbl Crop WinWht/Corn |    157,452        1.77       33.21
     Dbl Crop WinWht/Cotton |      1,596        0.02       33.23
    Dbl Crop WinWht/Sorghum |     37,187        0.42       33.64
   Dbl Crop WinWht/Soybeans |          5        0.00       33.64
                  Dry Beans |     41,937        0.47       34.11
                Durum Wheat |     54,283        0.61       34.72
                  Eggplants |        117        0.00       34.72
       Fallow/Idle Cropland |  1,083,389       12.15       46.87
                     Forest |        551        0.01       46.88
                     Garlic |     14,462        0.16       47.04
                     Grapes |    756,278        8.48       55.53
                     Greens |     11,242        0.13       55.65
                      Herbs |     10,049        0.11       55.76
            Honeydew Melons |     10,061        0.11       55.88
                    Lentils |         54        0.00       55.88
                    Lettuce |     26,538        0.30       56.17
                     Millet |        170        0.00       56.18
                       Mint |      3,058        0.03       56.21
         Misc Vegs & Fruits |     24,509        0.27       56.49
                    Mustard |          3        0.00       56.49
                 Nectarines |     20,281        0.23       56.71
                       Oats |    299,423        3.36       60.07
                     Olives |    139,508        1.56       61.64
                     Onions |     46,776        0.52       62.16
                    Oranges |    260,978        2.93       65.09
                Other Crops |     20,758        0.23       65.32
      Other Hay/Non Alfalfa |    380,006        4.26       69.58
         Other Small Grains |      1,527        0.02       69.60
           Other Tree Crops |     45,481        0.51       70.11
                    Peaches |     57,426        0.64       70.75
                      Pears |     11,991        0.13       70.89
                       Peas |     15,680        0.18       71.06
                     Pecans |      5,526        0.06       71.13
                    Peppers |      9,180        0.10       71.23
                 Pistachios |    330,484        3.71       74.94
                      Plums |    144,321        1.62       76.55
               Pomegranates |     69,017        0.77       77.33
            Pop or Orn Corn |        443        0.00       77.33
                   Potatoes |     17,085        0.19       77.52
                     Prunes |     25,584        0.29       77.81
                   Pumpkins |      3,954        0.04       77.86
                   Radishes |        303        0.00       77.86
                       Rice |    182,520        2.05       79.91
                        Rye |     27,349        0.31       80.21
                  Safflower |     89,943        1.01       81.22
             Sod/Grass Seed |     18,255        0.20       81.43
                    Sorghum |     24,081        0.27       81.70
                   Soybeans |         24        0.00       81.70
               Spring Wheat |     17,678        0.20       81.90
                     Squash |      5,439        0.06       81.96
               Strawberries |     25,273        0.28       82.24
                 Sugarbeets |     13,901        0.16       82.40
                  Sugarcane |        379        0.00       82.40
                  Sunflower |     40,671        0.46       82.86
                 Sweet Corn |     12,766        0.14       83.00
             Sweet Potatoes |      8,270        0.09       83.09
                   Tomatoes |    216,423        2.43       85.52
                  Triticale |     65,069        0.73       86.25
                    Turnips |        175        0.00       86.25
                      Vetch |      5,993        0.07       86.32
                    Walnuts |    530,629        5.95       92.27
                Watermelons |     21,527        0.24       92.51
               Winter Wheat |    530,593        5.95       98.46
             Woody Wetlands |    137,154        1.54      100.00




*******************************************************************************
*******************************************************************************

** 3. Merge into parcels and units

*******************************************************************************
*******************************************************************************
