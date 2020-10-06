clear all
version 13
set more off

**********************************************************************
**** Script to build CLU-year panel for Dynamic Discrete Choice *****
**********************************************************************
//output is dataset with 4 rows for each clu-year
// each row has a choice with choice-specific revenue and groundwater cost


global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

use "$dirpath_data/cleaned_spatial/CDL_panel_clu_crop_year_long.dta"

//////////////////////////
// MERGE IN STUFF/////////
//////////////////////////

{
//Merge in counties
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_county_conc.dta"
drop if _merge!=3
drop _merge

//Merge in water district
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_wdist_conc_wide.dta"
drop if _merge!=3
drop _merge

//Merge in Estimated Water Usage
preserve
use "$dirpath_data/merged_pge/clu_annual_water_panel.dta", clear
decode clu_id, gen(clu_id_temp)
drop clu_id
rename clu_id_temp clu_id

// drop bad observations
keep if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & flag_weird_pump==0
drop if mode_Annual + mode_FruitNutPerennial + mode_OtherPerennial + mode_Noncrop > 1

//drop variables to make this shit easier to look at for now
keep clu_id year af_rast_dd_mth_2SP ann_bill_kwh kwhaf_rast_dd_mth_2SP mean_p_kwh

replace clu_id = strtrim(clu_id)

tempfile water_panel
save `water_panel'
restore

replace clu_id=strtrim(clu_id)

merge m:1 clu_id year using "`water_panel'"
drop if _merge!=3
drop _merge
}

/////////////////////////////
// Put into Big Categories///
/////////////////////////////

{
gen landtype_bigcat = ""
replace landtype_bigcat = "Not Cropable" if inlist(landtype_cat,"Not Cropable")
replace landtype_bigcat = "Noncrop" if inlist(landtype_cat,"Fallow","Cropable Grass")
replace landtype_bigcat = "Annual" if perennial==0 & landtype_bigcat==""
replace landtype_bigcat = "Fruit/Nut Perennial" if inlist(landtype_cat,"Fruit Trees","Grapes","Nuts","Other Fruit") & perennial==1
replace landtype_bigcat = "Other Perennial" if perennial==1 & landtype_bigcat==""
tab landtype_cat landtype_bigcat, missing
assert landtype_bigcat!=""
}



//////////////////////////////
// Get Crop Prices////////////
//////////////////////////////
{
preserve 

use "$dirpath_data/misc/Crop_Data/County_Year_Yield/2007_County_Crop_details.dta", clear

gen year=2007

foreach j of numlist 2008/2018{
	append using "$dirpath_data/misc/Crop_Data/County_Year_Yield/`j'_County_Crop_details.dta"
	replace year=`j' if missing(year)
}

destring Harvested_Acreage Yield_Per_Acre Production Price_Per_Unit Total_Value, replace force


// Regularize Crop Names
{
drop if missing(Harvested_Acreage)|missing(Yield_Per_Acre)|missing(Production)|missing(Price_Per_Unit)
drop if missing(crop)
drop marked_for_deletion

replace crop = subinstr(crop, ", ALL", "", 1)
replace crop = subinstr(crop, ", UNSPECIFIED", "", 1)
replace crop = "Beans" if regex(crop, "BEANS")
replace crop = "Melons" if regex(crop, "MELON")
replace crop = "Citrus" if regex(crop, "ORANGES")
replace crop = "Broccoli" if regex(crop, "BROCCOLI")
replace crop = subinstr(crop, ", FOOD SERVICE", "", 1)
replace crop = subinstr(crop, ", FRESH MARKET", "", 1)
replace crop = subinstr(crop, ", SWEET", "", 1)
replace crop = subinstr(crop, ", SEED", "", 1)
replace crop = subinstr(crop, ", PLANTING", "", 1)
replace crop = subinstr(crop, ", PROCESSING", "", 1)
replace crop = "Cotton" if regex(crop, "COTTON")
replace crop = "Grapes" if regex(crop, "GRAPES")
replace crop = "Alfalfa" if regex(crop, "HAY")
replace crop = "Alfalfa" if regex(crop, "ALFALFA")
replace crop = "Lettuce" if regex(crop, "LETTUCE")
replace crop = "Pasture" if regex(crop, "PASTURE")
replace crop = "Peach" if regex(crop, "PEACHES")
replace crop = "Pear" if regex(crop, "PEARS")
replace crop = "Peppers" if regex(crop, "PEPPERS")
replace crop = "Plum" if regex(crop, "PLUMS")
replace crop = "Tomatoes" if regex(crop, "TOMATOES")
replace crop = "Walnut" if regex(crop, "WALNUTS")
replace crop = "Strawberries" if regex(crop, "STRAWBERRIES")
replace crop = "Blueberry" if regex(crop, "BLUEBERRIES")
replace crop = "Cranberry" if regex(crop, "CRANBERRIES")
replace crop = "Cabbage" if regex(crop, "CABBAGE")
replace crop = "Corn" if regex(crop, "CORN")
replace crop = "Grain" if regex(crop, "GRAIN")
replace crop = "Vegetables" if regex(crop, "VEGETABLES")
replace crop = "Herbs" if regex(crop, "HERBS")
replace crop = "Peas" if regex(crop, "PEAS")
replace crop = "Barley" if regex(crop, "BARLEY")
replace crop = "Rice" if regex(crop, "RICE")
replace crop = "Sorghum" if regex(crop, "SORGHUM")
replace crop = "Berry" if regex(crop, "BERRIES,")
replace crop = "Onion" if regex(crop, "ONIONS")
replace crop = "Beets" if regex(crop, "BEETS")
replace crop = "Flowers" if regex(crop, "FLOWERS")
replace crop = "Field Crop" if regex(crop, "FIELD CROP")
replace crop = "Nursery" if regex(crop, "NURSERY")
replace crop = "Potatoes" if regex(crop, "POTATOES")
replace crop = "Citrus" if regex(crop, "TANGERINES")
replace crop = "Citrus" if regex(crop, "GRAPEFRUIT")
replace crop = "Citrus" if regex(crop, "LIME")
replace crop = "Citrus" if regex(crop, "TANGELO")
replace crop = "Citrus" if regex(crop, "KUMQUAT")
replace crop = "Persimmon" if regex(crop, "PERSIMMONS")
replace crop = "Greens" if regex(crop, "GREENS")
replace crop = "Almond" if regex(crop, "ALMOND")
replace crop = "Avocado" if regex(crop, "AVOCADO")
replace crop = "Apple" if regex(crop, "APPLE")
replace crop = "Parsnip" if regex(crop, "PARSNIP")
replace crop = "Pistachio" if regex(crop, "PISTACHIO")
replace crop = "Pomegranate" if regex(crop, "POMEGRANATE")
replace crop = "Brussel Sprouts" if regex(crop, "BRUSSELS SPROUTS")
replace crop = "Cherry" if regex(crop, "CHERRIES")
replace crop = "Cucumber" if regex(crop, "CUCUMBER")
replace crop = "Herbs" if regex(crop, "CILANTRO")
replace crop = "Fig" if regex(crop, "FIG")
replace crop = "Pecan" if regex(crop, "PECAN")
replace crop = "Quinces" if regex(crop, "Quinces")
replace crop = "Rutabaga" if regex(crop, "RUTABAGA")
replace crop = "Sunflowers" if regex(crop, "SUNFLOWER")
replace crop = "Taro" if regex(crop, "TARO")
replace crop = "Tomatillos" if regex(crop, "TOMATILLO")
replace crop = "Olive" if regex(crop, "OLIVE")
replace crop = "Macadamia" if regex(crop, "MACADAMIA")
replace crop = "Guava" if regex(crop, "GUAVA")

replace crop = trim(crop)

replace crop = strproper(crop)
}

//Put crops into landtype_cat bins
{
gen landtype_cat = ""
replace landtype_cat = "feed" if inlist(crop,"Alfalfa","Other Hay/Non Alfalfa", ///"Clover/Wildflowers", ///
	"Camelina","Vetch","Triticale","Triticale/Corn", "Straw")
replace landtype_cat = "nuts" if inlist(crop,"Almond","Walnut","Pecan","Pistachio", "Peanuts")
replace landtype_cat = "fruit trees" if inlist(crop,"Apple","Apricots","Avocado","Cherries","Citrus","Dates", "Nectarines")
replace landtype_cat = "fruit trees" if inlist(crop,"Peach","Plum","Pomegranate","Pear","Prunes","Oranges", ///
	"Olive","Other Tree Crops")
replace landtype_cat = "fruit trees" if inlist(crop, "Fig", "Fruits & Nuts", "Macadamia", "Lemons", "Kiwifruit", "Chestnuts")
replace landtype_cat = "grapes" if inlist(crop,"Grapes")
replace landtype_cat = "other fruit" if inlist(crop,"Blueberry","Cantaloupes","Cranberries", ///
	"Honeydew Melons", "Strawberries","Watermelons","Caneberries", "Berry", "Cherry")
	replace landtype_cat = "other fruit" if inlist(crop, "Melons", "Guava", "Guar")
replace landtype_cat = "vegetables" if inlist(crop,"Asparagus", "Anise (Fennel)", "Artichokes", "Broccoli","Cabbage","Carrots","Cauliflower")
replace landtype_cat = "vegetables" if inlist(crop,"Celery","Chick Peas","Cucumber","Eggplant","Greens","Peas","Pumpkins")
replace landtype_cat = "vegetables" if inlist(crop,"Potatoes","Onion","Garlic","Radishes","Lettuce","Squash", "Yucca")
replace landtype_cat = "vegetables" if inlist(crop,"Sweet Potatoes","Potatoes","Tomatoes","Turnips","Peppers", "Tomatillos")
replace landtype_cat = "vegetables" if inlist(crop,"Brussel Sprouts", "Chives", "Endive", "Escarole", "Field Crop", "Okra", "Spinach", "Swiss Chard")
replace landtype_cat = "vegetables" if inlist(crop, "Parsnip", "Rutabaga", "Rappini", "Taro", "Vegetables", "Watercress", "Beets", "Kale")
replace landtype_cat = "vegetables" if inlist(crop, "Leeks", "Radicchio", "Kohlrabi")
replace landtype_cat = "vegetables" if inlist(crop,"Mushrooms", "Misc Vegs & Fruits","Dry Beans","Herbs","Mint","Lentils", "Beans", "Parsley")
replace landtype_cat = "cereal" if inlist(crop,"Grain","Barley","Corn","Dbl Crop Barley/Corn","Dbl Crop Barley/Sorghum")
replace landtype_cat = "cereal" if inlist(crop,"Dbl Crop Durum Wht/Sorghum","Dbl Crop Oats/Corn","Rice","Rye", "Ryegrass, Perennial")
replace landtype_cat = "cereal" if inlist(crop,"Dbl Crop WinWht/Corn","Dbl Crop WinWht/Sorghum","Durum Wheat", "Wheat", "Triticale")
replace landtype_cat = "cereal" if inlist(crop,"Millet","Oats","Sorghum","Spring Wheat","Winter Wheat", ///
	"Sweet Corn","Other Small Grains","Pop or Orn Corn")
replace landtype_cat = "fallow" if inlist(crop,"Fallow/Idle Cropland")
replace landtype_cat = "other" if inlist(crop,"Canola","Mustard","Safflower","Sunflowers","Sugarbeets","Sugarcane")
replace landtype_cat = "other" if inlist(crop,"Other Crops","Christmas Trees","Soybeans") //,"Sod/Grass Seed")
replace landtype_cat = "Cotton" if inlist(crop, "Cotton")
replace landtype_cat = "vegetables" if regexm(crop,"Dbl Crop Lettuce/") // assign based on first double crop
replace landtype_cat = "cereal" if regexm(crop,"Dbl Crop WinWht/") // assign based on first double crop
replace landtype_cat = "cropable grass" if inlist(crop,"Clover/Wildflowers","Grass/Pasture","Sod/Grass Seed", "Pasture")
drop if landtype_cat==""
}

replace landtype_cat=strproper(landtype_cat)
replace County = strtrim(County)
collapse (sum) Harvested_Acreage Total_Value, by(landtype_cat County year)
gen landcat_price = Total_Value/Harvested_Acreage
rename County county_name
tempfile Prices
save `Prices'

collapse (sum) Harvested_Acreage Total_Value, by(landtype_cat year)
gen landcat_price_nosp = Total_Value/Harvested_Acreage
tempfile Prices_nosp
save `Prices_nosp'
restore

merge m:1 county_name year landtype_cat using "`Prices'"
drop if _merge==2
drop _merge

merge m:1 year landtype_cat using "`Prices_nosp'"
drop if _merge==2
drop _merge


// Get County-year bigcat prices - collapse and sum by bigcat
preserve
gen CLUsliver_revenue = landtype_acres*landcat_price
collapse (sum) CLUsliver_revenue landtype_acres, by(county_name year landtype_bigcat)
gen bigcat_price_per_acre = CLUsliver_revenue/landtype_acres
tempfile bigcat_prices
save `bigcat_prices'
restore


//Generate prices with no county variation
preserve
gen CLUsliver_revenue_nosp = landtype_acres*landcat_price_nosp
collapse (sum) CLUsliver_revenue landtype_acres, by(year landtype_bigcat)
gen bigcat_price_per_acre_nosp = CLUsliver_revenue_nosp/landtype_acres
tempfile bigcat_prices_nosp
save `bigcat_prices_nosp'
restore
}

///////////////////////////////
// Collapse to CLU-bigcat//////
//////////////////////////////
{
collapse (sum) fraction landtype_acres, by(clu_id year cluacres landtype_bigcat county_name user_id_list af_rast_dd_mth_2SP ann_bill_kwh kwhaf_rast_dd_mth_2SP mean_p_kwh)
egen modal_acres = max(landtype_acres), by(clu_id year)
gen modecrop=1 if abs(landtype_acres-modal_acres)<0.001

gen mode_crop=1 if modecrop==1&landtype_bigcat=="Noncrop"
replace mode_crop=2 if modecrop==1&landtype_bigcat=="Annual"
replace mode_crop=3 if modecrop==1&landtype_bigcat=="Fruit/Nut Perennial"
replace mode_crop=4 if modecrop==1&landtype_bigcat=="Other Perennial"

gen frac_FruitNuttmp = fraction if landtype_bigcat=="Fruit/Nut Perennial"
egen frac_FruitNut = max(frac_FruitNuttmp), by(clu_id year)
gen frac_Annualtmp = fraction if landtype_bigcat=="Annual"
egen frac_Annual = max(frac_Annualtmp), by(clu_id year)
gen frac_OtherPerennialtmp = fraction if landtype_bigcat=="Other Perennial"
egen frac_OtherPerennial = max(frac_OtherPerennialtmp), by(clu_id year)
gen frac_Noncroptmp = fraction if landtype_bigcat=="Noncrop"
egen frac_Noncrop = max(frac_Noncroptmp), by(clu_id year)

drop frac_FruitNuttmp frac_Annualtmp frac_OtherPerennialtmp frac_Noncroptmp


replace frac_FruitNut=0 if missing(frac_FruitNut)
replace frac_Annual=0 if missing(frac_Annual)
replace frac_OtherPerennial=0 if missing(frac_OtherPerennial)
replace frac_Noncrop=0 if missing(frac_Noncrop)
}
////////////////////////////////////
// Put datset into format for clogit///
////////////////////////////////////
// eg 4 entries for each clu-year
{
gen modecrop1=mode_crop==1
gen modecrop2=mode_crop==2
gen modecrop3=mode_crop==3
gen modecrop4=mode_crop==4

drop if modecrop!=1
drop modecrop

// drop slivers
drop if landtype_acres<1
drop if fraction<.2
drop if modecrop1+modecrop2+modecrop3+modecrop4<1

egen clu_year_id = group(clu_id year)
sort clu_year_id
by clu_year_id: gen find_multiple_modes = _n
drop if find_multiple_modes>1  // should arbitrarily drop one of the "more than one modes" idk if there's a better way to do this
drop find_multiple_modes

reshape long modecrop, i(clu_year_id) j(choice)

//put in crop prices in correct rows
replace landtype_bigcat="Noncrop" if choice==1
replace landtype_bigcat="Annual" if choice==2
replace landtype_bigcat="Fruit/Nut Perennial" if choice==3
replace landtype_bigcat="Other Perennial" if choice==4

merge m:1 county_name year landtype_bigcat using "`bigcat_prices'"
drop if _merge==2
drop _merge
drop fraction landtype_acres CLUsliver_revenue
replace bigcat_price_per_acre=0 if choice==1
gen counterfactual_revenue=bigcat_price_per_acre*cluacres

merge m:1 year landtype_bigcat using "`bigcat_prices_nosp'"
drop if _merge==2
drop _merge
}

///////////////////////////////////////
// Calculate AF/acre //////////////////
///////////////////////////////////////

{

//only use "fallow" observations with 90%+ to calculate fallow water usage
gen modecrop_temp=modecrop
replace modecrop_temp=0 if choice==1&modecrop==1&frac_Noncrop<.9


egen sum_af_wdist_crop = sum(af_rast_dd_mth_2SP) if modecrop_temp==1&missing(user_id_list)==0, by(user_id_list year choice)
egen acres_wdist_crop = sum(modal_acres) if modecrop_temp==1&missing(user_id_list)==0, by(user_id_list year choice)
gen mean_af_per_acre_wdist = sum_af_wdist_crop/acres_wdist_crop 

egen counterfactual_mean_af_per_acre = max(mean_af_per_acre_wdist), by(user_id_list year choice)
drop sum_af_wdist_crop acres_wdist_crop mean_af_per_acre_wdist

gen counterfactual_groundwater_cost = counterfactual_mean_af_per_acre*kwhaf_rast_dd_mth_2SP*mean_p_kwh

tab choice, sum(counterfactual_mean_af_per_acre)
drop modecrop_temp

// Use regression method instead of means
gen acres_FruitNut=cluacres*frac_FruitNut
gen acres_Annual = cluacres*frac_Annual
gen acres_OtherPerennial = cluacres*frac_OtherPerennial
gen acres_Noncrop = cluacres*frac_Noncrop
tempfile data
save `data'

statsby, by(user_id_list year): reg af_rast_dd_mth_2SP acres_FruitNut acres_Annual acres_OtherPerennial acres_Noncrop if modecrop==1, nocons
tempfile coeffs
save `coeffs'

twoway kdensity _b_acres_FruitNut|| kdensity _b_acres_Annual ||kdensity _b_acres_OtherPerennial||kdensity _b_acres_Noncrop
merge 1:m user_id_list year using `data'
drop _merge

gen counterfactual_water_cost = kwhaf_rast_dd_mth_2SP*mean_p_kwh*_b_acres_FruitNut if choice==3
replace counterfactual_water_cost = kwhaf_rast_dd_mth_2SP*mean_p_kwh*_b_acres_OtherPerennial if choice==4
replace counterfactual_water_cost = kwhaf_rast_dd_mth_2SP*mean_p_kwh*_b_acres_Annual if choice==2
replace counterfactual_water_cost = kwhaf_rast_dd_mth_2SP*mean_p_kwh*_b_acres_Noncrop if choice==1

}

////////////////////////////////////////////////
// figure out what's wrong with af measures////
///////////////////////////////////////////////
{
preserve
tab choice if choice==mode_crop, sum(af_rast_dd_mth_2SP)
gen af_per_acre=af_rast_dd_mth_2SP/cluacres
tab choice if choice==mode_crop, sum(af_per_acre)
reg af_per_acre frac_Noncrop if mode_crop==1&choice==1

twoway scatter frac_FruitNut frac_Noncrop if mode_crop==1&choice==1&frac_Noncrop>0.9, msize(0.2)||scatter frac_Annual frac_Noncrop if mode_crop==1&choice==1&frac_Noncrop>0.9, msize(0.2)||scatter frac_OtherPerennial frac_Noncrop if frac_Noncrop>0.9&mode_crop==1&choice==1, msize(0.2)
reg frac_FruitNut frac_Noncrop if mode_crop==1&choice==1&year==2010
reg frac_Annual frac_Noncrop if mode_crop==1&choice==1&year==2010
reg frac_OtherPerennial frac_Noncrop if mode_crop==1&choice==1&year==2010

keep if choice==1&mode_crop==1
xtile quant_noncrop=frac_Noncrop, n(4)

tab quant_noncrop if year==2010, sum(frac_FruitNut)
tab quant_noncrop if year==2010, sum(frac_Annual)
tab quant_noncrop if year==2010, sum(frac_OtherPerennial)

xtile quant_af=af_per_acre, n(5)
tab quant_af if year==2016, sum(frac_FruitNut)
tab quant_af if year==2016, sum(frac_Noncrop)
tab quant_af if year==2016, sum(frac_Annual)
tab quant_af if year==2016, sum(frac_OtherPerennial)

scatter af_per_acre frac_Noncrop if choice==1&mode_crop==1, msize(0.3)
graph bar frac_Noncrop frac_FruitNut frac_Annual frac_OtherPerennial, over(quant_af)
reg af_per_acre frac_Noncrop
restore
}

/////////////////////////////////////////////////////
// Generate Investment, Disinvestment Dummies////////
/////////////////////////////////////////////////////

{
preserve
keep clu_id year choice mode_crop
keep if choice==mode_crop
drop choice
encode clu_id, gen(clu_idtemp)
xtset clu_idtemp year
gen lagchoice=l.mode_crop
drop clu_idtemp
tempfile lagchoice
save `lagchoice'
restore

merge m:1 clu_id year using "`lagchoice'"

//Generate the dummies to estimate investment/disinvestment costs (annuals are unidentified)
gen destroy_perennial=(choice==1|choice==2|choice==4)&(lagchoice==3)
gen new_hay= (choice==4)|(lagchoice!=4)
gen new_perennial=(choice==3)&(lagchoice!=3)
}


clogit modecrop bigcat_price_per_acre counterfactual_water_cost destroy_perennial new_hay new_perennial i.choice#i.year, group(clu_id)
clogit modecrop bigcat_price_per_acre counterfactual_water_cost destroy_perennial new_hay new_perennial i.choice, group(clu_id)

clogit modecrop bigcat_price_per_acre counterfactual_groundwater_cost destroy_perennial new_hay new_perennial i.choice#i.year, group(clu_id)
clogit modecrop bigcat_price_per_acre counterfactual_groundwater_cost destroy_perennial new_hay new_perennial i.choice, group(clu_id)


label var counterfactual_groundwater_cost "expected groundwater usage per acre in af-calculated using means by water district"
label var counterfactual_water_cost "expected groundwater usage per acre in af - calculated using wd-level regs"
label var bigcat_price_per_acre "average price per acre at the county-level"
label var bigcat_price_per_acre_nosp "average price per acre with no spatial variation"

//What if I enforce that groundwater_cost=0 for fallow land?
preserve
replace counterfactual_groundwater_cost=0 if choice==1
clogit modecrop bigcat_price_per_acre counterfactual_groundwater_cost destroy_perennial new_hay new_perennial i.choice#i.year, group(clu_id)
restore

preserve
replace counterfactual_water_cost=0 if choice==1
clogit modecrop bigcat_price_per_acre counterfactual_water_cost destroy_perennial new_hay new_perennial i.choice#i.year, group(clu_id)
restore

