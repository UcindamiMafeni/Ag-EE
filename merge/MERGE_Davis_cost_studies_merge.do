
** File to merge Davis cost studies with CDL land types 
clear

global path = "T:/Projects/Pump Data/data"


*********************************************************************
*********************************************************************

** 1. Merge Davis Cost names to CDL drop names
{
	// prep Davis Cost studies for merge
use "$path/Davis cost studies/Davis_cost_studies_processed_all.dta", clear
unique Number
assert r(unique)==r(N)
duplicates t Name_of_crop year location, gen(dup)
br if dup>0
	// for alfalfa dup, keep larger operation (they're very similar)
drop if Number==0
	// for cling peaches dup, keep early variety (they're pretty similar)
drop if Number==67
	
unique Name_of_crop year location
assert r(unique)==r(N)
keep Number Name_of_crop year location

	// prep for merge
gen landtype = Name_of_crop
replace landtype = "Alfalfa" if landtype=="Alfalfa Hay"	
replace landtype = "Oats" if landtype=="Oat Hay"
*replace landtype = "Other Hay/Non Alfalfa" if landtype=="Oat Hay"
replace landtype = "Sorghum" if landtype=="Grain Sorghum"	
replace landtype = "Sorghum" if landtype=="Sorghum Silage"	
replace landtype = "Lettuce" if landtype=="Iceberg Lettuce"	
replace landtype = "Lettuce" if landtype=="Romaine Hearts"
replace landtype = "Onions" if landtype=="Onions-Dehydrated"
replace landtype = "Apples" if landtype=="Processing Apples"
replace landtype = "Citrus" if landtype=="Lemons"
replace landtype = "Peppers" if regexm(landtype,"Bell Pepper")
replace landtype = "Strawberries" if landtype=="Strawberry"
replace landtype = "Peaches" if landtype=="Cling Peach"	
replace landtype = "Plums" if landtype=="Plums-Fresh Market"	
replace landtype = "Prunes" if landtype=="Prunes (Dried Plums)"	
replace landtype = "Potatoes" if landtype=="Potato (Chippers for Processing)"	
replace landtype = "Potatoes" if landtype=="Potato (Fresh Market)"	
replace landtype = "Walnuts" if landtype=="Walnut"	
replace landtype = "Pistachios" if landtype=="Pistachio"	
replace landtype = "Olives" if landtype=="Olives for Olive Oil"	
replace landtype = "Pears" if landtype=="Pear"	
replace landtype = "Cherries" if landtype=="Sweet Cherry"	
replace landtype = "Tomatoes" if landtype=="Processing Tomatoes"	
replace landtype = "Greens" if landtype=="Spinach"	
replace landtype = "Dry Beans" if substr(landtype,1,8)=="Dry Bean"
replace landtype = "Grapes" if substr(landtype,1,9)=="Winegrape"
replace landtype = "Grapes" if substr(landtype,1,5)=="Grape"
replace landtype = "Grapes" if substr(landtype,1,10)=="DOV-Raisin"
expand 2 if inlist(landtype,"Corn (Silage)","Field Corn","Silage Corn-Conservation Tillage"), gen(temp)
replace landtype = "Sweet Corn" if inlist(landtype,"Corn (Silage)","Field Corn") & temp==0
replace landtype = "Corn" if inlist(landtype,"Corn (Silage)","Field Corn") & temp==1
drop temp
expand 2 if inlist(landtype,"Wheat"), gen(temp)
replace landtype = "Winter Wheat" if inlist(landtype,"Wheat") & temp==0
replace landtype = "Spring Wheat" if inlist(landtype,"Wheat") & temp==1
expand 2 if inlist(landtype,"Spring Wheat"), gen(temp2)
replace landtype = "Durum Wheat" if inlist(landtype,"Spring Wheat") & temp==1 & temp2==1
drop temp*

// Notes: 
// Cotton: most is not pima, and they're grown in the same places, so I'm matching everything to the non Pima variety (a.k.a. upland or acala)
// For organice, I'm using the conventional version (if we have it)


	// merge in land categories
merge m:1 landtype using "$path/cleaned_spatial/landtype_categories.dta" 

br
br if regexm(landtype,"asture")

egen temp_tag = tag(landtype)

sum pct if temp_tag
di r(sum)

sum pct if temp_tag & _merge==3
di r(sum)

sum pct if temp_tag & (_merge==3 | regexm(landtype,"Fallow"))
di r(sum)

gsort -pct
br landtype pct _merge if _merge==2 //& pct>0.01
	// top 4 unmatched categories
di .0335586 + .0213511 + .0103545 + .010121 
gsort landtype_cat -pct
br

	// save essential elements for crosswalk
drop if _merge==1
keep Number Name_of_crop year location landtype pct
compress
rename year davis_year
save "$path/Davis cost studies/davis_crop_studies_xwalk.dta", replace

}

*********************************************************************
*********************************************************************

** 2. Differentiate by basin
{
	// county and basin IDs for merge
use "$path/pge_cleaned/sp_premise_gis.dta", clear
unique sp_uuid
assert r(unique)==r(N)
gen sp_count = _n
collapse (count) sp_count, by(county_fips county basin_id basin_sub_id basin_name basin_sub_name) fast
sum sp_count
local rsum = r(sum)
sum sp_count if !mi(county_fips) & !mi(basin_id)
di r(sum)/`rsum' // 98.0% nonmissing
drop if mi(county_fips) & mi(basin_id)
sum sp_count
local rsum = r(sum)
sum sp_count if !mi(county_fips) & !mi(basin_id)
di r(sum)/`rsum' // 99.4% nonmissing after dropping bad lat/lons
collapse (sum) sp_count, by(county_fips county) fast
tempfile counties
save `counties'

	// CDL what was grown in each county for merge
use "$path/cleaned_spatial/cdl_panel_crop_year_full.dta", clear
collapse (sum) cluacres, by(landtype county) fast
egen temp = sum(cluacres), by(landtype)
gen cluacres_pct_of_crop = cluacres/temp
egen temp2 = sum(cluacres), by(county)
gen cluacres_pct_of_county = cluacres/temp2
egen temp3 = sum(cluacres)
gen cluacres_pct_total = cluacres/temp3
drop temp*
gsort landtype -cluacres 
preserve
import dbase using "$path/spatial/Counties/CA_Counties_TIGER2016.dbf", clear
keep COUNTYFP NAME
rename NAME county
tempfile county_fips
save `county_fips'
restore
merge m:1 county using `county_fips', nogen
tempfile crops
save `crops'

	// merge both into crosswalk
use "$path/Davis cost studies/davis_crop_studies_xwalk.dta", clear
joinby landtype using `crops', unmatched(both)
tabstat cluacres_pct_total if _merge==2, by(landtype) s(sum) // 54% of area is not cropland
sum cluacres_pct_total if _merge==2
replace cluacres_pct_total = cluacres_pct_total/(1-r(sum))
assert _merge!=1
drop if _merge==2
drop _merge
merge m:1 county using `counties'
drop if _merge==2
assert COUNTYFP==county_fips if county_fips!=""
replace county_fips = COUNTYFP if county_fips==""
assert county_fips!="" if county!=""
drop COUNTYFP


	// clean Davis locations
replace location = upper(location)
replace location = subinstr(location,"-"," ",.)
replace location = subinstr(location," –"," ",.)
replace location = subinstr(location," AND "," & ",.)
replace location = trim(itrim(location))
tab location

	// code up matches --> https://coststudies.ucdavis.edu/en/current/map/
gen davis_location_match = 0
gen county_counter = 0

	// San Joaquin Valley
gen tempN = inlist(county,"San Joaquin","Stanislaus","Merced","Madera")
gen tempS = inlist(county,"Fresno","Kings","Tulare","Kern")
replace davis_location_match = 1 if location=="SAN JOAQUIN VALLEY" & (tempN | tempS)
replace davis_location_match = 1 if location=="SACRAMENTO & SAN JOAQUIN VALLEYS" & (tempN | tempS)
replace davis_location_match = 1 if location=="SAN JOAQUIN VALLEY NORTH" & tempN
replace davis_location_match = 1 if location=="SACRAMENTO VALLEY & NORTHERN SAN JOAQUIN VALLEY" & tempN
replace davis_location_match = 1 if location=="SACRAMENTO & NORTHERN SAN JOAQUIN VALLEYS" & tempN
replace davis_location_match = 1 if location=="SAN JOAQUIN VALLEY SOUTH" & tempS
replace davis_location_match = 1 if location=="SOUTHERN SAN JOAQUIN VALLEY" & tempS
replace county_counter = county_counter + tempN + tempS
drop temp*

	// Sacramento Valley
gen temp = inlist(county,"Tehama","Butte","Glenn","Colusa")
replace temp = 1 if inlist(county,"Yuba","Yolo","Sutter","Solano","Sacramento")
replace davis_location_match = 1 if location=="SACRAMENTO VALLEY" & temp
replace davis_location_match = 1 if location=="SACRAMENTOVALLEY" & temp
replace davis_location_match = 1 if location=="SACRAMENTO & SAN JOAQUIN VALLEYS" & temp
replace davis_location_match = 1 if location=="SACRAMENTO VALLEY & NORTHERN SAN JOAQUIN VALLEY" & temp
replace davis_location_match = 1 if location=="SACRAMENTO & NORTHERN SAN JOAQUIN VALLEYS" & temp
replace davis_location_match = 1 if location=="SACRAMENTO VALLEY & NORTHERN DELTA" & temp
replace county_counter = county_counter + temp
drop temp*

	// North Coast
gen temp = inlist(county,"Del Norte","Humboldt","Mendocino","Lake","Sonoma","Napa","Marin")
replace davis_location_match = 1 if location=="NORTH COAST (LAKE COUNTY)" & county=="Lake"
replace davis_location_match = 1 if location=="NORTH COAST MENDOCINO COUNTY" & county=="Mendocino"
replace davis_location_match = 1 if location=="NORTH COAST, NAPA" & county=="Napa"
replace davis_location_match = 1 if location=="RUSSIAN RIVER VALLEY SONOMA COUNTY" & county=="Sonoma"
replace davis_location_match = 1 if location=="NORTH COAST" & temp
replace county_counter = county_counter + temp
drop temp*

	// Central Coast
gen temp = inlist(county,"Santa Clara","Santa Cruz","San Benito","Monterey","San Luis Obispo")
replace davis_location_match = 1 if location=="FREEDOM REGION PAJARO VALLEY SANTA CRUZ COUNTY" & county=="Santa Cruz"
replace davis_location_match = 1 if location=="CENTRAL COAST (SAN LUIS OBISPO COUNTY)" & county=="San Luis Obispo"
replace davis_location_match = 1 if location=="CENTRAL COAST" & temp
replace county_counter = county_counter + temp
drop temp*

	// South Coast
gen temp = inlist(county,"Santa Barbara","Ventura","Los Angeles","San Diego","Orange")
replace davis_location_match = 1 if location=="VENTURA COUNTY" & county=="Ventura"
replace county_counter = county_counter + temp
drop temp*

	// Intermountain
gen temp = inlist(county,"Siskiyou","Modoc","Lassen","Shasta","Trinity")
	// these are so sparse I'm just assigning to anywhere in the region
replace temp = 1 if inlist(county,"Plumas","Sierra","Placer","Nevada","El Dorado","Alpine","Amador","Calaveras")
replace davis_location_match = 1 if location=="SISKIYOU COUNTY BUTTE VALLEY" & temp
replace davis_location_match = 1 if location=="SISKIYOU COUNTY SCOTT VALLEY" & temp
replace davis_location_match = 1 if location=="SIERRA NEVADA FOOTHILLS" & temp
replace davis_location_match = 1 if location=="INTERMOUNTAIN REGION KLAMATH BASIN" & temp
replace county_counter = county_counter + temp
drop temp*

	// Bay Area
gen temp = inlist(county,"San Francisco","Contra Costa","Alameda","San Mateo")
replace davis_location_match = 1 if location=="SACRAMENTO VALLEY & NORTHERN DELTA" & county=="Contra Costa"
replace county_counter = county_counter + temp
drop temp*

	// Southeast Interior
gen temp = inlist(county,"Tuolumne","Mono","Mariposa","Inyo","San Bernardino","Riverside","Imperial")	
	// none :(
replace county_counter = county_counter + temp
drop temp*

tab county county_counter
assert county_counter==1 if county!=""
drop county_counter

egen temp = max(davis_location_match), by(location)
assert temp==1 if location!=""
drop temp

	// Drop unmatched if there's a match
unique landtype county_fips county 
local uniq = r(unique)
egen temp = max(davis_location_match), by(landtype county_fips county )	
drop if davis_location_match==0 & temp==1
drop temp
unique landtype county_fips county 
assert r(unique)==`uniq'

	// Assess dups
duplicates t landtype county_fips county , gen(dup)
tab dup
tab landtype if dup==0 & Number!=.
tab landtype if dup>0 & Number!=.
egen temp_tag = tag(landtype county_fips county )
tabstat cluacres if Number!=. & temp_tag, by(dup) s(sum)
tabstat cluacres if Number==. & temp_tag, by(dup) s(sum)

	// Adjudicate dups with more than one match (non-grape edition)
egen temp = sum(davis_location_match), by(landtype county_fips county )	
tab temp dup
tab landtype temp if temp>1
sort landtype Number
br if temp>1
	// Alfalfa: split ties by favoring flood over drip (less common: https://www.sacbee.com/news/politics-government/article2591279.html)
unique landtype county_fips county 
local uniq = r(unique)
drop if temp==2 & landtype=="Alfalfa" & Number==3
unique landtype county_fips county 
assert r(unique)==`uniq'
	// Alfalfa: take Scott Valley over Butte Valley (both in Siskiyou, not a high-stakes choice)
unique landtype county_fips county 
local uniq = r(unique)
drop if temp==2 & landtype=="Alfalfa" & Number==6
unique landtype county_fips county 
assert r(unique)==`uniq'
	// Broccoli: keep SLO-specific for SLO
unique landtype county_fips county 
local uniq = r(unique)
drop if temp==2 & landtype=="Broccoli" & Number==30
unique landtype county_fips county 
assert r(unique)==`uniq'
	// Corn: split on field vs silage based on https://apps1.cdfa.ca.gov/FertilizerResearch/docs/Corn_Production_CA.pdf
	// Silage corn is much bigger than field corn in South San Joaquin
unique landtype county_fips county 
local uniq = r(unique)
drop if temp==2 & landtype=="Corn" & Number==35
unique landtype county_fips county 
assert r(unique)==`uniq'
	// Dry beans: prefer single-cropped https://anrcatalog.ucanr.edu/pdf/8402.pdf
unique landtype county_fips county 
local uniq = r(unique)
drop if temp==2 & landtype=="Dry Beans" & Number==18
unique landtype county_fips county 
assert r(unique)==`uniq'
	// Lettuce: prefer iceberg since it's more commonly grown and more profitable
unique landtype county_fips county 
local uniq = r(unique)
drop if temp==2 & landtype=="Lettuce" & Number==57
unique landtype county_fips county 
assert r(unique)==`uniq'
	// Olives: prefer olive oil, which has overtaken table/canned olives in the last 10 years: https://apps1.cdfa.ca.gov/FertilizerResearch/docs/Olive_Production_CA.pdf
unique landtype county_fips county 
local uniq = r(unique)
drop if temp==2 & landtype=="Olives" & Number==60
unique landtype county_fips county 
assert r(unique)==`uniq'
	// Peppers: prefer fresh market
unique landtype county_fips county 
local uniq = r(unique)
drop if temp==2 & landtype=="Peppers" & Number==72
unique landtype county_fips county 
assert r(unique)==`uniq'
	// Potatoes: prefer fresh market
unique landtype county_fips county 
local uniq = r(unique)
drop if temp==2 & landtype=="Potatoes" & Number==75
unique landtype county_fips county 
assert r(unique)==`uniq'
	// Sorghum: waving my hands and picking grain sorghum
unique landtype county_fips county 
local uniq = r(unique)
drop if temp==2 & landtype=="Sorghum" & Number==81
unique landtype county_fips county 
assert r(unique)==`uniq'
	// Sweet corn: field corn is as close as we'll get
unique landtype county_fips county 
local uniq = r(unique)
drop if temp==2 & landtype=="Sweet Corn" & Number==38
unique landtype county_fips county 
assert r(unique)==`uniq'
		
	// Adjudicate dups with more than one match (grape edition)
br if temp>1 & landtype=="Grapes"
tab county if landtype=="Grapes" & temp>1
tab Name_of_crop if landtype=="Grapes" & temp>1	
/* https://www.nass.usda.gov/Statistics_by_State/California/Publications/Specialty_and_Other_Releases/Grapes/Acreage/2018/201804gabtb00.pdf

Raisin acres in 2016: (gonna go with OHTS since it's worth more money to the farmer)
Fresno 114062
Kern 8320
Kings 401
Madera 25231
San Joaquin 3
Stanislaus 168
Tulare 7786

Autum Kings acres in 2016:
Fresno 562
Kern 2698
Kings 62
Madera 274
San Joaquin 0
Stanislaus 1
Tulare 2495

Scarlet Royals acres in 2016:
Fresno 954
Kern 2729
Kings 142
Madera 136
San Joaquin 0
Stanislaus 2
Tulare 2867

Flame Seedless acres in 2016:
Fresno 3009
Kern 5717
Kings 52
Madera 407
San Joaquin 6
Stanislaus 6
Tulare 2762

Sheegene 21 acres in 2016:
Fresno 84
Kern 272
Kings 0
Madera 28
San Joaquin 0
Stanislaus 0
Tulare 171

Wine grapes acres in 2016:
Fresno 40108
Kern 17570
Kings 1418
Madera 33352
San Joaquin 73285
Stanislaus 7115
Tulare 5769
*/
	// Fresno is modally raisins
tab Name_of_crop Number if county=="Fresno" & landtype=="Grapes"
unique landtype county_fips county 
local uniq = r(unique)
drop if temp>=2 & landtype=="Grapes" & Number!=44 & county=="Fresno"
unique landtype county_fips county 
assert r(unique)==`uniq'
	
	// Kern is modally winegrapes, but wine grapes isn't an option, so the next mode is raisins
tab Name_of_crop Number if county=="Kern" & landtype=="Grapes"
unique landtype county_fips county 
local uniq = r(unique)
drop if temp>=2 & landtype=="Grapes" & Number!=44 & county=="Kern"
unique landtype county_fips county 
assert r(unique)==`uniq'

	// Kings is modally winegrapes, but wine grapes isn't an option, so the next mode is raisins
tab Name_of_crop Number if county=="Kings" & landtype=="Grapes"
unique landtype county_fips county 
local uniq = r(unique)
drop if temp>=2 & landtype=="Grapes" & Number!=44 & county=="Kings"
unique landtype county_fips county 
assert r(unique)==`uniq'

	// Madera is modally winegrapes
tab Name_of_crop Number if county=="Madera" & landtype=="Grapes"
unique landtype county_fips county 
local uniq = r(unique)
drop if temp>=2 & landtype=="Grapes" & Number!=51 & county=="Madera"
unique landtype county_fips county 
assert r(unique)==`uniq'

	// Merced is almost entirely winegrapes
tab Name_of_crop Number if county=="Merced" & landtype=="Grapes"
unique landtype county_fips county 
local uniq = r(unique)
drop if temp>=2 & landtype=="Grapes" & Number!=51 & county=="Merced"
unique landtype county_fips county 
assert r(unique)==`uniq'

	// San Joaquin is almsot entirely winegrapes
tab Name_of_crop Number if county=="San Joaquin" & landtype=="Grapes"
unique landtype county_fips county 
local uniq = r(unique)
drop if temp>=2 & landtype=="Grapes" & Number!=51 & county=="San Joaquin"
unique landtype county_fips county 
assert r(unique)==`uniq'

	// Stanislaus is almsot entirely winegrapes
tab Name_of_crop Number if county=="Stanislaus" & landtype=="Grapes"
unique landtype county_fips county 
local uniq = r(unique)
drop if temp>=2 & landtype=="Grapes" & Number!=51 & county=="Stanislaus"
unique landtype county_fips county 
assert r(unique)==`uniq'

	// Tulare is modally raisins
tab Name_of_crop Number if county=="Tulare" & landtype=="Grapes"
unique landtype county_fips county 
local uniq = r(unique)
drop if temp>=2 & landtype=="Grapes" & Number!=44 & county=="Tulare"
unique landtype county_fips county 
assert r(unique)==`uniq'

	// Confirm duplicate matches are eliminated
egen temp2 = sum(davis_location_match), by(landtype county_fips county )		
assert inlist(temp2,0,1)
drop dup* temp*

	// Code up regions for assigning adjacently
gen davis_region = ""
replace davis_region = "San Joaquin North" if inlist(county,"San Joaquin","Stanislaus","Merced","Madera")
replace davis_region = "San Joaquin South" if inlist(county,"Fresno","Kings","Tulare","Kern")
replace davis_region = "Sacramento Valley" if inlist(county,"Tehama","Butte","Glenn","Colusa")
replace davis_region = "Sacramento Valley" if inlist(county,"Yuba","Yolo","Sutter","Solano","Sacramento")
replace davis_region = "North Coast" if inlist(county,"Del Norte","Humboldt","Mendocino","Lake","Sonoma","Napa","Marin")
replace davis_region = "Central Coast" if inlist(county,"Santa Clara","Santa Cruz","San Benito","Monterey","San Luis Obispo")
replace davis_region = "South Coast" if inlist(county,"Santa Barbara","Ventura","Los Angeles","San Diego","Orange")
replace davis_region = "Intermountain" if inlist(county,"Siskiyou","Modoc","Lassen","Shasta","Trinity")
replace davis_region = "Intermountain" if inlist(county,"Plumas","Sierra","Placer","Nevada","El Dorado","Alpine","Amador","Calaveras")
replace davis_region = "Bay Area" if inlist(county,"San Francisco","Contra Costa","Alameda","San Mateo")
replace davis_region = "Southeast Interior" if inlist(county,"Tuolumne","Mono","Mariposa","Inyo","San Bernardino","Riverside","Imperial")	
assert davis_region!="" if county!=""

	// Assess dups that don't have a location match
duplicates t landtype county_fips county , gen(dup)
tab dup
tab landtype if dup>0 & Number!=.

	// Split ties based on nearest region
br Number Name_of_crop location landtype county davis_region davis_location_match dup if dup>0 & Number!=.	
	// Alfalfa
tab location davis_region if landtype=="Alfalfa" & dup>0 & Number!=.
unique landtype county_fips county 
local uniq = r(unique)
drop if dup>0 & landtype=="Alfalfa" & Number!=2 & inlist(davis_region,"Bay Area","North Coast")
drop if dup>0 & landtype=="Alfalfa" & Number!=1 & inlist(davis_region,"Central Coast","Southeast Interior","South Coast")
unique landtype county_fips county 
assert r(unique)==`uniq'
	// Almonds
tab location davis_region if landtype=="Almonds" & dup>0 & Number!=.
tab Number location if landtype=="Almonds" & dup>0 & Number!=.
unique landtype county_fips county 
local uniq = r(unique)
drop if dup>0 & landtype=="Almonds" & Number!=8 & inlist(davis_region,"Intermountain","North Coast")
drop if dup>0 & landtype=="Almonds" & Number!=9 & inlist(davis_region,"Bay Area")
drop if dup>0 & landtype=="Almonds" & Number!=10 & inlist(davis_region,"Central Coast","Southeast Interior","South Coast")
unique landtype county_fips county 
assert r(unique)==`uniq'
	// Broccoli
tab location davis_region if landtype=="Broccoli" & dup>0 & Number!=.
tab Number location if landtype=="Broccoli" & dup>0 & Number!=.
unique landtype county_fips county 
local uniq = r(unique)
drop if dup>0 & landtype=="Broccoli" & Number!=30 
unique landtype county_fips county 
assert r(unique)==`uniq'
	// Corn
tab location davis_region if landtype=="Corn" & dup>0 & Number!=.
tab Number location if landtype=="Corn" & dup>0 & Number!=.
unique landtype county_fips county 
local uniq = r(unique)
drop if dup>0 & landtype=="Corn" & Number!=37 & inlist(davis_region,"Intermountain","North Coast","Bay Area")
drop if dup>0 & landtype=="Corn" & Number!=38 & inlist(davis_region,"Central Coast","Southeast Interior","South Coast")
unique landtype county_fips county 
assert r(unique)==`uniq'
	// Dry beans
tab location davis_region if landtype=="Dry Beans" & dup>0 & Number!=.
tab Number location if landtype=="Dry Beans" & dup>0 & Number!=.
unique landtype county_fips county 
local uniq = r(unique)
drop if dup>0 & landtype=="Dry Beans" & Number!=19 
unique landtype county_fips county 
assert r(unique)==`uniq'
	// Lettuce
tab location davis_region if landtype=="Lettuce" & dup>0 & Number!=.
tab Number location if landtype=="Lettuce" & dup>0 & Number!=.
unique landtype county_fips county 
local uniq = r(unique)
drop if dup>0 & landtype=="Lettuce" & Number!=56 
unique landtype county_fips county 
assert r(unique)==`uniq'
	// Olives
tab location davis_region if landtype=="Olives" & dup>0 & Number!=.
tab Number location if landtype=="Olives" & dup>0 & Number!=.
unique landtype county_fips county 
local uniq = r(unique)
drop if dup>0 & landtype=="Olives" & Number!=59 
unique landtype county_fips county 
assert r(unique)==`uniq'
	// Peppers
tab location davis_region if landtype=="Peppers" & dup>0 & Number!=.
tab Number location if landtype=="Peppers" & dup>0 & Number!=.
unique landtype county_fips county 
local uniq = r(unique)
drop if dup>0 & landtype=="Peppers" & Number!=71 
unique landtype county_fips county 
assert r(unique)==`uniq'
	// Potatoes
tab location davis_region if landtype=="Potatoes" & dup>0 & Number!=.
tab Number location if landtype=="Potatoes" & dup>0 & Number!=.
unique landtype county_fips county 
local uniq = r(unique)
drop if dup>0 & landtype=="Potatoes" & Number!=76 
unique landtype county_fips county 
assert r(unique)==`uniq'
	// Sorghum
tab location davis_region if landtype=="Sorghum" & dup>0 & Number!=.
tab Number location if landtype=="Sorghum" & dup>0 & Number!=.
unique landtype county_fips county 
local uniq = r(unique)
drop if dup>0 & landtype=="Sorghum" & Number!=80
unique landtype county_fips county 
assert r(unique)==`uniq'
	// Sweet Corn
tab location davis_region if landtype=="Sweet Corn" & dup>0 & Number!=.
tab Number location if landtype=="Sweet Corn" & dup>0 & Number!=.
unique landtype county_fips county 
local uniq = r(unique)
drop if dup>0 & landtype=="Sweet Corn" & Number!=37 & inlist(davis_region,"Intermountain","North Coast","Bay Area")
drop if dup>0 & landtype=="Sweet Corn" & Number!=35 & inlist(davis_region,"Central Coast","Southeast Interior","South Coast")
unique landtype county_fips county 
assert r(unique)==`uniq'
	// Durum Wheat
tab location davis_region if landtype=="Durum Wheat" & dup>0 & Number!=.
tab Number location if landtype=="Durum Wheat" & dup>0 & Number!=.
unique landtype county_fips county 
local uniq = r(unique)
drop if dup>0 & landtype=="Durum Wheat" & Number!=92 & inlist(davis_region,"Intermountain","North Coast","Bay Area")
drop if dup>0 & landtype=="Durum Wheat" & Number!=93 & inlist(davis_region,"San Joaquin North","Central Coast","Southeast Interior","South Coast")
unique landtype county_fips county 
assert r(unique)==`uniq'
	// Pears
tab location davis_region if landtype=="Pears" & dup>0 & Number!=.
tab Number location if landtype=="Pears" & dup>0 & Number!=.
unique landtype county_fips county 
local uniq = r(unique)
drop if dup>0 & landtype=="Pears" & Number!=68 & inlist(davis_region,"Intermountain")
drop if dup>0 & landtype=="Pears" & Number!=70 & inlist(davis_region,"San Joaquin North","San Joaquin South","Central Coast","Southeast Interior","Bay Area")
unique landtype county_fips county 
assert r(unique)==`uniq'	
	// Spring Wheat
tab location davis_region if landtype=="Spring Wheat" & dup>0 & Number!=.
tab Number location if landtype=="Spring Wheat" & dup>0 & Number!=.
unique landtype county_fips county 
local uniq = r(unique)
drop if dup>0 & landtype=="Spring Wheat" & Number!=92 & inlist(davis_region,"Intermountain","North Coast","Bay Area")
drop if dup>0 & landtype=="Spring Wheat" & Number!=93 & inlist(davis_region,"San Joaquin North","Central Coast","Southeast Interior","South Coast")
unique landtype county_fips county 
assert r(unique)==`uniq'
	// Tomatoes
tab location davis_region if landtype=="Tomatoes" & dup>0 & Number!=.
tab Number location if landtype=="Tomatoes" & dup>0 & Number!=.
unique landtype county_fips county 
local uniq = r(unique)
drop if dup>0 & landtype=="Tomatoes" & Number!=87 & inlist(davis_region,"Intermountain","North Coast","Bay Area")
drop if dup>0 & landtype=="Tomatoes" & Number!=86 & inlist(davis_region,"San Joaquin North","Central Coast","South Coast","Southeast Interior")
unique landtype county_fips county 
assert r(unique)==`uniq'
	// Walnuts
tab location davis_region if landtype=="Walnuts" & dup>0 & Number!=.
tab Number location if landtype=="Walnuts" & dup>0 & Number!=.
unique landtype county_fips county 
local uniq = r(unique)
drop if dup>0 & landtype=="Walnuts" & Number!=91 & inlist(davis_region,"Intermountain","North Coast")
drop if dup>0 & landtype=="Walnuts" & Number!=88 & inlist(davis_region,"Bay Area")
drop if dup>0 & landtype=="Walnuts" & Number!=89 & inlist(davis_region,"San Joaquin South","Central Coast","Southeast Interior","South Coast")
unique landtype county_fips county 
assert r(unique)==`uniq'
	// Winter Wheat
tab location davis_region if landtype=="Winter Wheat" & dup>0 & Number!=.
tab Number location if landtype=="Winter Wheat" & dup>0 & Number!=.
unique landtype county_fips county 
local uniq = r(unique)
drop if dup>0 & landtype=="Winter Wheat" & Number!=92 & inlist(davis_region,"Intermountain","Bay Area","North Coast")
drop if dup>0 & landtype=="Winter Wheat" & Number!=93 & inlist(davis_region,"San Joaquin North","Central Coast","Southeast Interior","South Coast")
unique landtype county_fips county 
assert r(unique)==`uniq'


	// Adjudicate dups with more than one match (grape edition)
br if dup>0 & landtype=="Grapes"
tab county if landtype=="Grapes" & dup>0
tab Name_of_crop if landtype=="Grapes" & dup>1	
/* https://www.nass.usda.gov/Statistics_by_State/California/Publications/Specialty_and_Other_Releases/Grapes/Acreage/2018/201804gabtb00.pdf

Raisin acres in 2016: (gonna go with OHTS since it's worth more money to the farmer)
Alameda 0
Contra Costa 0
Del Norte 0
Humboldt 0
Imperial 0
Inyo 0
Lake 1
Marin 0
Mariposa 0
Mono 0
Monterey 1
Riverside 801
San Benito 0
San Bernardino 152
San Diego 2
San Luis Obispo 207
San Mateo 0
Santa Barbara 0
Santa Clara 0
Santa Cruz 0
Tuolumne 0
Ventura 0

Table grapes acres in 2016: (all types)
Alameda 10
Contra Costa 2
Del Norte 0
Humboldt 1
Imperial 0
Inyo 0
Lake 4
Marin 0
Mariposa 0
Mono 0
Monterey 0
Riverside 133
San Benito 0
San Bernardino 0
San Diego 2
San Luis Obispo 0
San Mateo 0
Santa Barbara 0
Santa Clara 0
Santa Cruz 0
Tuolumne 0
Ventura 0

Wine grapes acres in 2016:
Alameda 2921
Contra Costa 1781
Del Norte 0
Humboldt 111
Imperial 0
Inyo 0
Lake 9420
Marin 166
Mariposa 61
Mono 0
Monterey 45416
Riverside 1154
San Benito 2711
San Bernardino 479
San Diego 577
San Luis Obispo 33951
San Mateo 103
Santa Barbara 15826
Santa Clara 1561
Santa Cruz 490
Tuolumne 24
Ventura 40

// ALL COUNTIES CAN BE WINEGRAPES, this is easy

*/
	// Remove all non-wine-grapes
tab Name_of_crop if landtype=="Grapes" & dup>1	
unique landtype county_fips county 
local uniq = r(unique)
drop if dup>0 & landtype=="Grapes" & substr(Name_of_crop,1,9)!="Winegrape"
unique landtype county_fips county 
assert r(unique)==`uniq'
	// Assign geographies
tab location davis_region if landtype=="Grapes" & dup>0 & Number!=.
tab Number location if landtype=="Grapes" & dup>0 & Number!=.
tab location Name_of_crop if landtype=="Grapes" & dup>0 & Number!=.
tab location if landtype=="Grapes" & dup>0 & Number!=.
unique landtype county_fips county 
local uniq = r(unique)
drop if dup>0 & landtype=="Grapes" & Number!=50 & inlist(davis_region,"North Coast")
drop if dup>0 & landtype=="Grapes" & Number!=51 & inlist(davis_region,"Bay Area","Central Coast","South Coast")
drop if dup>0 & landtype=="Grapes" & Number!=52 & inlist(davis_region,"Southeast Interior")
unique landtype county_fips county 
assert r(unique)==`uniq'

	// Confirm duplicate matches are eliminated
duplicates t landtype county_fips county, gen(dup2)
assert dup2==0
drop dup* 
unique landtype county
assert r(unique)==r(N)
drop if _merge==2
assert landtype!=""

	// Assess how well we've done
tabstat cluacres_pct_total, s(sum)
gen davis_match = Number!=.
replace davis_match = 1 if regexm(landtype,"Fallow")
replace davis_location_match = 1 if regexm(landtype,"Fallow")
assert davis_match if davis_location_match 
tabstat cluacres_pct_total, by(davis_match) s(sum)
tabstat cluacres_pct_total, by(davis_location_match) s(sum)
tabstat cluacres_pct_total if davis_match==0, by(landtype) s(sum)
tabstat cluacres_pct_total if sp_count>100 & sp_count!=., by(davis_match) s(sum)
tabstat cluacres_pct_total if sp_count>100 & sp_count!=., by(davis_location_match) s(sum)

tabstat cluacres_pct_of_crop if landtype=="Almonds", by(davis_location_match) s(sum)
	// 93% have a location match
tabstat cluacres_pct_of_crop if landtype=="Alfalfa", by(davis_location_match) s(sum)
	// 82% have a location match
tabstat cluacres_pct_of_crop if landtype=="Walnuts", by(davis_location_match) s(sum)
	// 66% have a location match
tabstat cluacres_pct_of_crop if landtype=="Pistachios", by(davis_location_match) s(sum)
	// 70% have a location match
tabstat cluacres_pct_of_crop if landtype=="Corn", by(davis_location_match) s(sum)
	// 89% have a location match
tabstat cluacres_pct_of_crop if landtype=="Grapes", by(davis_location_match) s(sum)
	// 78% have a location match
	
	
	// Clean up
keep Number Name_of_crop location landtype county county_fips davis_region davis_match davis_location_match
la var Number "Davis cost studies PDF identifier"
la var Name_of_crop "Davis cost studies name of crop"
la var location "Davis cost studies location"
la var landtype "CDL land type"
la var davis_location_match "Dummy for crop + location match"
la var davis_match "Dummy for crop match (not necessarily location)"
la var davis_region "County regions, per Davis classification"

	// Save
unique landtype county_fips
assert r(unique)==r(N)
sort landtype county_fips
compress
save "$path/Davis cost studies/Davis_to_CDL_county_xwalk.dta", replace

}


*********************************************************************
*********************************************************************
