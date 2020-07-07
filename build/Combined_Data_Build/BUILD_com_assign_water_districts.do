clear all
version 13
set more off

*****************************************************************************
**** Script to create spatial concordance between CLUs and other polygons ***
*****************************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"
global dirpath_code "T:/Home/Louis/backup/AgEE/AgEE_code/build"
*global R_exe_path "C:/PROGRA~1/MIE74D~1/ROPEN~1/bin/x64/R"
*global R_lib_path "C:/Program Files/Microsoft/R Open/R-3.4.4/library"

*******************************************************************************
*******************************************************************************

** 1. Run auxilary GIS script "BUILD_com_clu_water_districts.R" to spatially join 
**    CLUs to Nick Hagerty water district polygons and export to csv

*******************************************************************************
*******************************************************************************

** 2. Create data set of water district attributes
{
	// read in all water districts
insheet using "$dirpath_data/misc/water_districts_all.csv", clear comma names
unique user_id
assert r(unique)==r(N)
tempfile allwds
save `allwds'

	// read in only water districts that we observe receiving positive allocations
insheet using "$dirpath_data/misc/water_districts_pos.csv", clear comma names
unique user_id
assert r(unique)==r(N)
keep user_id
tempfile poswds
save `poswds'

	// read in water districts that we are using (the Hagerty-endorsed subset)
insheet using "$dirpath_data/misc/water_districts_hag.csv", clear comma names
unique user_id
assert r(unique)==r(N)
keep user_id
tempfile hagwds
save `hagwds'


	// merge them together
use `allwds', clear
merge 1:1 user_id using `poswds'	
gen pos = _merge==3
drop _merge
merge 1:1 user_id using `hagwds'	
gen hag = _merge==3
drop _merge
gsort -hag -pos user_id

	// assess subset of polygons linked to positive allocations
replace pwsid = "" if pwsid=="NA"
replace agencyuniq = . if agencyuniq==0
tab source
tab source pos
tab source hag
tabstat totarea, by(source) s(min p10 p25 p50 p75 p90 max)
tabstat totarea if pos, by(source) s(min p10 p25 p50 p75 p90 max)
tabstat totarea if hag, by(source) s(min p10 p25 p50 p75 p90 max)
tab source pos if regexm(upper(username),"WATER DISTRICT") | regexm(upper(username),"W.D.")
tab source pos if regexm(upper(username),"IRRIGATION") | regexm(upper(username),"I.D.")
tab source hag if regexm(upper(username),"WATER DISTRICT") | regexm(upper(username),"W.D.")
tab source hag if regexm(upper(username),"IRRIGATION") | regexm(upper(username),"I.D.")
	
	// clean up and drop extraneous variables
drop user_x user_y totarea shape_leng shape_area	

	// label
la var pwsid "Unique water district polygon ID for source=='cehtp' "
la var agencyuniq "Unique water district polygon ID for source=='agencies'"
la var user_id "Unique water district polygon ID (Hagerty), across all sources"
la var username "Name of water district polygon"
la var source "Source of polygon, per Nick Hagerty"
la var totacres "Size (acres) of water district polygon"
la var pos "Dummy for water distict polygons ever linked to positive allocations"
la var hag "Dummy for finalized subset of Hagerty-endorsed polygons (what we're using)"
rename pos alloc_pos
rename hag hag_subset
assert alloc_pos==1 if hag_subset==1

	// save
order user_id alloc_pos hag_subset username	
sort user_id
unique user_id
assert r(unique)==r(N)
compress
save "$dirpath_data/surface_water/hagerty_surface_water_shapefiles_info.dta", replace

}

*******************************************************************************
*******************************************************************************

** 3. Process CLU-to-water district polygon-to-polygon join
{
	// Process CLU assignments to ALL water districts
insheet using "$dirpath_data/misc/CLU_water_districts_all.csv", clear comma names
foreach vy of varlist cluacresy countyy {
	local vx = substr("`vy'",1,length("`vy'")-1) + "x"
	replace `vx' = "" if `vx'=="NA" & `vy'!="NA"
	replace `vy' = "" if `vy'=="NA" & `vx'!="NA"
	replace `vx' = `vy' if `vy'!="" & `vx'==""
	assert inlist(`vy',"",`vx')
	drop `vy'
}
rename cluacresx cluacres
rename countyx county
foreach v of varlist * {
	replace `v' = "" if `v'=="NA"
	destring `v', replace
}
assert clu_id!="" 

gen overlap = intacres/cluacres
sum overlap, detail // most intersections are complete, and the units match well
replace overlap = 0 if overlap==.
gen match = overlap>=0.5
preserve
collapse (sum) match, by(clu_id)
tab match
restore

gen wdist_hag = 0
compress 
tempfile clu_wdist_all
save `clu_wdist_all'

	// Process CLU assignments to Hagerty water districts (the subset of polygons we want)
insheet using "$dirpath_data/misc/CLU_water_districts_hag.csv", clear comma names
foreach vy of varlist cluacresy countyy {
	local vx = substr("`vy'",1,length("`vy'")-1) + "x"
	replace `vx' = "" if `vx'=="NA" & `vy'!="NA"
	replace `vy' = "" if `vy'=="NA" & `vx'!="NA"
	replace `vx' = `vy' if `vy'!="" & `vx'==""
	assert inlist(`vy',"",`vx')
	drop `vy'
}
rename cluacresx cluacres
rename countyx county
foreach v of varlist * {
	replace `v' = "" if `v'=="NA"
	destring `v', replace
}
assert clu_id!="" 

gen overlap = intacres/cluacres
sum overlap, detail // most intersections are complete, and the units match well
replace overlap = 0 if overlap==.
gen match = overlap>=0.5
preserve
collapse (sum) match, by(clu_id)
tab match
restore

gen wdist_hag = 1
compress 
tempfile clu_wdist_hag
save `clu_wdist_hag'

	// Append both datasets
clear
append using `clu_wdist_all' `clu_wdist_hag'	
drop county pwsid agencyuniq user_x user_y totarea shape_leng shape_area totacres
br
egen matches_count = sum(match), by(clu_id wdist_hag)
egen temp_tag_clu_hag = tag(clu_id wdist_hag)
tab matches_count wdist_hag if temp_tag_clu_hag
	// restricting to the set of Hagerty water distict polygons only removes a TON of dups, which means the GIS code worked
gsort clu_id -wdist_hag

	// Confirm that all Hagerty matches are also in the full match dataset, and drop
egen temp1 = min(wdist_hag), by(clu_id user_id)
egen temp2 = max(wdist_hag), by(clu_id user_id)
assert temp1==0 if temp2==1 & user_id!=.
unique clu_id
local uniq = r(unique)
unique clu_id user_id if user_id!=.
local uniq2 = r(unique)
drop if wdist_hag==0 & temp2==1 & user_id!=.
unique clu_id
assert r(unique)==`uniq'
unique clu_id user_id if user_id!=.
assert r(unique)==`uniq2'
drop temp*

	// Find Hagerty polygons that are always matched to the same Hagerty polygons

	// Flag user_ids that match to each CLU
egen temp_hag1_temp = min(user_id) if wdist_hag==1 , by(clu_id)
egen temp_hag1 = mean(temp_hag1_temp), by(clu_id)
egen temp_hag2_temp = min(user_id) if wdist_hag==1 & !inlist(user_id,temp_hag1), by(clu_id)
egen temp_hag2 = mean(temp_hag2_temp), by(clu_id)
egen temp_hag3_temp = min(user_id) if wdist_hag==1 & !inlist(user_id,temp_hag1,temp_hag2), by(clu_id)
egen temp_hag3 = mean(temp_hag3_temp), by(clu_id)
egen temp_hag4_temp = min(user_id) if wdist_hag==1 & !inlist(user_id,temp_hag1,temp_hag2,temp_hag3), by(clu_id)
egen temp_hag4 = mean(temp_hag4_temp), by(clu_id)
egen temp_hag5_temp = min(user_id) if wdist_hag==1 & !inlist(user_id,temp_hag1,temp_hag2,temp_hag3,temp_hag4), by(clu_id)
egen temp_hag5 = mean(temp_hag5_temp), by(clu_id)
assert temp_hag5==.
drop temp_hag?_temp temp_hag5

	// For non-Hagerty polygons, find indicies of Hagerty polygons they already overlap with (in linked CLUs)
preserve
keep if wdist_hag==0
keep user_id match temp_hag?
duplicates drop
	// expand out indices of all Hagerty polygons that ever matched to user_id
egen id1 = min(temp_hag1), by(user_id)
egen id2_temp = min(temp_hag1) if !inlist(temp_hag1,id1), by(user_id)
egen id2 = mean(id2_temp), by(user_id)
egen id3_temp = min(temp_hag1) if !inlist(temp_hag1,id1,id2), by(user_id)
egen id3 = mean(id3_temp), by(user_id)
egen id4_temp = min(temp_hag1) if !inlist(temp_hag1,id1,id2,id3), by(user_id)
egen id4 = mean(id4_temp), by(user_id)
egen id5_temp = min(temp_hag1) if !inlist(temp_hag1,id1,id2,id3,id4), by(user_id)
egen id5 = mean(id5_temp), by(user_id)
egen id6_temp = min(temp_hag1) if !inlist(temp_hag1,id1,id2,id3,id4,id5), by(user_id)
egen id6 = mean(id6_temp), by(user_id)
egen id7_temp = min(temp_hag1) if !inlist(temp_hag1,id1,id2,id3,id4,id5,id6), by(user_id)
egen id7 = mean(id7_temp), by(user_id)
egen id8_temp = min(temp_hag1) if !inlist(temp_hag1,id1,id2,id3,id4,id5,id6,id7), by(user_id)
egen id8 = mean(id8_temp), by(user_id)
egen id9_temp = min(temp_hag1) if !inlist(temp_hag1,id1,id2,id3,id4,id5,id6,id7,id8), by(user_id)
egen id9 = mean(id9_temp), by(user_id)
egen id10_temp = min(temp_hag1) if !inlist(temp_hag1,id1,id2,id3,id4,id5,id6,id7,id8,id9), by(user_id)
egen id10 = mean(id10_temp), by(user_id)
egen id11_temp = min(temp_hag1) if !inlist(temp_hag1,id1,id2,id3,id4,id5,id6,id7,id8,id9,id10), by(user_id)
egen id11 = mean(id11_temp), by(user_id)
egen id12_temp = min(temp_hag1) if !inlist(temp_hag1,id1,id2,id3,id4,id5,id6,id7,id8,id9,id10,id11), by(user_id)
egen id12 = mean(id12_temp), by(user_id)
egen id13_temp = min(temp_hag1) if !inlist(temp_hag1,id1,id2,id3,id4,id5,id6,id7,id8,id9,id10,id11,id12), by(user_id)
egen id13 = mean(id13_temp), by(user_id)
egen id14_temp = min(temp_hag1) if !inlist(temp_hag1,id1,id2,id3,id4,id5,id6,id7,id8,id9,id10,id11,id12,id13), by(user_id)
egen id14 = mean(id14_temp), by(user_id)
egen id15_temp = min(temp_hag1) if !inlist(temp_hag1,id1,id2,id3,id4,id5,id6,id7,id8,id9,id10,id11,id12,id13,id14), by(user_id)
egen id15 = mean(id15_temp), by(user_id)
egen id16_temp = min(temp_hag1) if !inlist(temp_hag1,id1,id2,id3,id4,id5,id6,id7,id8,id9,id10,id11,id12,id13,id14,id15), by(user_id)
egen id16 = mean(id16_temp), by(user_id)
assert id16==.
drop id*_temp id16
egen id16_temp = min(temp_hag2) if !inlist(temp_hag2,id1,id2,id3,id4,id5,id6,id7,id8,id9,id10,id11,id12,id13,id14,id15), by(user_id)
egen id16 = mean(id16_temp), by(user_id)
egen id17_temp = min(temp_hag2) if !inlist(temp_hag2,id1,id2,id3,id4,id5,id6,id7,id8,id9,id10,id11,id12,id13,id14,id15,id16), by(user_id)
egen id17 = mean(id17_temp), by(user_id)
egen id18_temp = min(temp_hag2) if !inlist(temp_hag2,id1,id2,id3,id4,id5,id6,id7,id8,id9,id10,id11,id12,id13,id14,id15,id16,id17), by(user_id)
egen id18 = mean(id18_temp), by(user_id)
egen id19_temp = min(temp_hag2) if !inlist(temp_hag2,id1,id2,id3,id4,id5,id6,id7,id8,id9,id10,id11,id12,id13,id14,id15,id16,id17,id18), by(user_id)
egen id19 = mean(id19_temp), by(user_id)
egen id20_temp = min(temp_hag2) if !inlist(temp_hag2,id1,id2,id3,id4,id5,id6,id7,id8,id9,id10,id11,id12,id13,id14,id15,id16,id17,id18,id19), by(user_id)
egen id20 = mean(id20_temp), by(user_id)
assert id20==.
drop id*_temp id20
egen id20_temp = min(temp_hag3) if !inlist(temp_hag3,id1,id2,id3,id4,id5,id6,id7,id8,id9,id10,id11,id12,id13,id14,id15,id16,id17,id18,id19), by(user_id)
egen id20 = mean(id20_temp), by(user_id)
egen id21_temp = min(temp_hag3) if !inlist(temp_hag3,id1,id2,id3,id4,id5,id6,id7,id8,id9,id10,id11,id12,id13,id14,id15,id16,id17,id18,id19,id20), by(user_id)
egen id21 = mean(id21_temp), by(user_id)
assert id21==.
drop id*_temp id21
egen id21_temp = min(temp_hag4) if !inlist(temp_hag4,id1,id2,id3,id4,id5,id6,id7,id8,id9,id10,id11,id12,id13,id14,id15,id16,id17,id18,id19,id20), by(user_id)
egen id21 = mean(id21_temp), by(user_id)
assert id21==.
drop id*_temp id21
	// create dummies for whether ID is present (any match)
forvalues i = 1/20 {
	gen hag`i' = 1 - (temp_hag1!=id`i' & temp_hag2!=id`i' & temp_hag3!=id`i' & temp_hag4!=id`i')
	replace hag`i' = . if id`i'==.
	replace hag`i' = hag`i' * id`i'
}
	// flag IDs that are always present (any match)
forvalues i = 1/20 {
	gen temp1 = 1 - (temp_hag1!=id`i' & temp_hag2!=id`i' & temp_hag3!=id`i' & temp_hag4!=id`i')
	replace temp1 = . if id`i'==.
	egen hag`i'_any = min(temp1), by(user_id)
	egen temp2 = min(temp1) if match==1, by(user_id)
	egen hag`i'_match = mean(temp2), by(user_id)
	drop temp1 temp2
	replace hag`i'_any = hag`i'_any*id`i'
	replace hag`i'_match = hag`i'_match*id`i'
	replace hag`i'_any = . if hag`i'_any==0
	replace hag`i'_match = . if hag`i'_match==0
}
	// consolidate flags: any CLU merge
egen HAG_id1_any = rowmin(hag*_any)
foreach v of varlist hag*_any {
	replace `v' = . if `v'==HAG_id1_any
}
egen HAG_id2_any = rowmin(hag*_any)
foreach v of varlist hag*_any {
	replace `v' = . if `v'==HAG_id2_any
}
egen HAG_id3_any = rowmin(hag*_any)
foreach v of varlist hag*_any {
	replace `v' = . if `v'==HAG_id3_any
}
egen HAG_id4_any = rowmin(hag*_any)
foreach v of varlist hag*_any {
	replace `v' = . if `v'==HAG_id4_any
}
assert HAG_id4_any==.
drop HAG_id4_any
	// consolidate flags: matched CLU merge
egen HAG_id1_match = rowmin(hag*_match)
foreach v of varlist hag*_match {
	replace `v' = . if `v'==HAG_id1_match
}
egen HAG_id2_match = rowmin(hag*_match)
foreach v of varlist hag*_match {
	replace `v' = . if `v'==HAG_id2_match
}
egen HAG_id3_match = rowmin(hag*_match)
foreach v of varlist hag*_match {
	replace `v' = . if `v'==HAG_id3_match
}
egen HAG_id4_match = rowmin(hag*_match)
foreach v of varlist hag*_match {
	replace `v' = . if `v'==HAG_id4_match
}
assert HAG_id4_match==.
drop HAG_id4_match
	// drop intermediate variables
drop id* hag* temp* match
duplicates drop
	// remove redundant match indices
foreach v of varlist HAG_id?_match {
	replace `v' = . if inlist(`v',HAG_id1_any,HAG_id2_any,HAG_id3_any)
}	
	// reshape away the distinction between "any" and "match" (can come back and undo this if we want?)
rename HAG_* *
rename *_any *
rename *_match *1
reshape long id, i(user_id) j(number)
drop number
drop if id==.
duplicates drop
rename id user_id_hag_overlap
duplicates t user_id, gen(dup)
gen j = dup+1
replace j = j[_n-1] + 1 if user_id[_n-1]==user_id
replace j = j - dup
drop dup
reshape wide user_id_hag_overlap, i(user_id) j(j)
la var user_id_hag_overlap1 "Polygon user_id #1 in Hagerty subset that  always overlaps"
la var user_id_hag_overlap2 "Polygon user_id #2 in Hagerty subset that always overlaps"
la var user_id_hag_overlap3 "Polygon user_id #3 in Hagerty subset that always overlaps"
sort user_id
unique user_id 
assert r(unique)==r(N)
assert user_id_hag_overlap1!=.
compress
tempfile hag_overlaps
save `hag_overlaps'
restore

	// Merge overlap ids into water district info data
preserve
use "$dirpath_data/surface_water/hagerty_surface_water_shapefiles_info.dta", clear
merge 1:1 user_id using `hag_overlaps', nogen
compress
save "$dirpath_data/surface_water/hagerty_surface_water_shapefiles_info.dta", replace
restore

	// Merge overlap ids into CLU-matched data
merge m:1 user_id using `hag_overlaps', nogen	
drop temp*

	// Pause to assess
unique user_id // 3573 polygons overlap with a CLU
unique user_id if match==1 // 2593 polygons overlap with more than 50% of a CLU's area
unique user_id if match==1 & wdist_hag==1 // 174 Hagerty polygons overlap with more than 50% of a CLU's area
unique user_id if match==1 & wdist_hag==0 // 2419 non-Hagerty overlap with more than 50% of a CLU's area
unique user_id if match==1 & wdist_hag==0 & user_id_hag_overlap1!=.
	// of those 2149, 1072 are ALWAYS co-merged with the same Hagerty polygon(s) (rendering them redundant)
unique user_id if match==1 & wdist_hag==0 & user_id_hag_overlap1==. // of those 2149, 1347 are not redundant
unique clu_id if match==1 & wdist_hag==1 // 178916 CLUs match to a Hagerty polygon
unique clu_id if match==1 // an additional 35815 CLUs match only to polygons without an allocation
	
	// Flag CLUs with matches that are only for non-Hagerty polygons
egen temp1 = max(match==1) if wdist_hag==0, by(clu_id)
egen temp2 = mean(temp1), by(clu_id)
egen temp3 = max(match==1) if wdist_hag==1, by(clu_id)
egen temp4 = mean(temp3), by(clu_id)
gen flag_wdist_nonhag_alloc_only = temp2==1 & temp4==0
unique clu_id if flag_wdist_nonhag_alloc_only==1
drop temp*
		
	// Drop redundant polygons
egen temp1 = min(user_id_hag_overlap1==.), by(clu_id)
egen temp2 = max(user_id_hag_overlap2==.), by(clu_id)
unique clu_id
local uniq = r(unique)
drop if temp1==0 & temp2==1 & user_id_hag_overlap1!=. & wdist_hag==0
unique clu_id
assert r(unique)==`uniq'	
drop temp*	
assert user_id_hag_overlap1==.
drop user_id_hag_overlap?

	// Drop intersections that are identical in area
egen double temp1 = max(intacres) if wdist_hag==1, by(clu_id)
egen double temp2 = mean(temp1), by(clu_id)	
unique clu_id
local uniq = r(unique)
drop if intacres==temp2 & wdist_hag==0 // this is a LOT, which suggests that my redundancy check is quite conservative
unique clu_id
assert r(unique)==`uniq'	
unique user_id if wdist_hag==0	
drop temp*
	
	// Drop non-Hagerty polygon matches
unique clu_id 
local uniq = r(unique)
unique clu_id if wdist_hag==1
drop if wdist_hag==0
unique clu_id
assert r(unique)==`uniq'
tab matches_count
drop wdist_hag  

	// Remove non matches
unique clu_id if matches_count==0 & overlap>0.10 // 1537 CLUs have between 10% and 50% of acreage in a polygon, which we're throwing out
gen temp = tot_int_area/cluacres
sum temp if matches_count==0, detail
sort temp
br if matches_count==0
unique clu_id if matches_count==0 & temp>=0.5 & temp!=. // only 17 CLUs have combined matches >50% without a 50% majority
drop cluacres intacres tot_int_area overlap temp
foreach v of varlist username source user_id {
	cap replace `v' = "" if match==0
	cap replace `v' = . if match==0
}
duplicates drop
unique clu_id
local uniq = r(unique)
egen temp = max(user_id!=.), by(clu_id)
drop if user_id==. & temp==1
unique clu_id
assert r(unique)==`uniq'
drop temp

	// clean up and label
br	
duplicates t clu_id, gen(dup)
tab dup
assert dup+1==matches_count if match!=0
drop match dup
la var clu_id "CLU unique identifier"
la var user_id "Unique water district polygon ID (Hagerty), across all sources"
la var username "Name of water district polygon"
la var source "Source of polygon, per Nick Hagerty"
la var matches_count "# polygons (in Hagerty subset) that CLU overlaps with at least 50% of its area"
la var flag_wdist_nonhag_alloc_only "Flag for CLUs that only match to non-Hagerty water district polygons"
rename matches_count wdist_matches_count

	// save long version
order clu_id user_id
sort clu_id user_id
unique clu_id user_id
assert r(unique)==r(N)
compress
save "$dirpath_data/cleaned_spatial/clu_wdist_conc_long.dta", replace

	// reshape wide
use "$dirpath_data/cleaned_spatial/clu_wdist_conc_long.dta", clear
sort clu_id user_id	
gen temp = _n
egen temp_min = min(temp), by(clu_id)

gen user_id2 = .
gen username2 = ""
gen source2 = ""
replace user_id2 = user_id[_n+1] if wdist_matches_count>1 & clu_id==clu[_n+1] & temp==temp_min
replace username2 = username[_n+1] if wdist_matches_count>1 & clu_id==clu[_n+1] & temp==temp_min
replace source2 = source[_n+1] if wdist_matches_count>1 & clu_id==clu[_n+1] & temp==temp_min

gen user_id3 = .
gen username3 = ""
gen source3 = ""
replace user_id3 = user_id[_n+2] if wdist_matches_count>2 & clu_id==clu[_n+2] & temp==temp_min
replace username3 = username[_n+2] if wdist_matches_count>2 & clu_id==clu[_n+2] & temp==temp_min
replace source3 = source[_n+2] if wdist_matches_count>2 & clu_id==clu[_n+2] & temp==temp_min

assert wdist_matches_count<=3
keep if temp==temp_min
drop temp temp_min
unique clu_id 
assert r(unique)==r(N)

rename user_id user_id1
rename username username1
rename source source1

gen user_id_list = string(user_id1) + " " + string(user_id2) + " " + string(user_id3)
replace user_id_list = trim(itrim(subinstr(user_id_list,".","",.)))
unique user_id_list

la var clu_id "CLU unique identifier"
forvalues i = 1/3 {
	la var user_id`i' "Unique water district polygon ID ` i' (Hagerty), across all sources"
	la var username`i' "Name of water district polygon `i'"
	la var source`i' "Source of polygon `i', per Nick Hagerty"
}
la var user_id_list "LIst of unique water district polygon IDs matched to CLU"

sort clu_id
order clu_id user_id_list wdist_matches_count flag
unique clu_id 
assert r(unique)==r(N)
compress
save "$dirpath_data/cleaned_spatial/clu_wdist_conc_wide.dta", replace

}

*******************************************************************************
*******************************************************************************

** 4. Assign water districts to points, based on their assigned CLUs
if 1==1{

** 4a. PGE SPs
{
use "$dirpath_data/pge_cleaned/pge_sp_premise_gis.dta", clear
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_wdist_conc_wide.dta", keep(1 3) nogen
foreach v of varlist user_id_list wdist_matches_count flag_wdist_nonhag_alloc_only user_id? username? source? {
	rename `v' `v'A
}
rename clu_id clu_idA
rename clu_id_ec clu_id 
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_wdist_conc_wide.dta", keep(1 3) nogen
foreach v of varlist user_id_list wdist_matches_count flag_wdist_nonhag_alloc_only user_id? username? source? {
	rename `v' `v'EC
}
rename clu_id clu_id_ec
rename clu_idA clu_id
gen user_id_list_match = user_id_listA==user_id_listEC
assert clu_id!=clu_id_ec if user_id_list_match==0
tab user_id_list_match
br sp_uuid user_id_list* *A *EC if user_id_list_match==0
count if user_id_list_match==0 // only 425
count if user_id_list_match==0 & user_id_listA!="" & user_id_listEC!="" & ///
	(strpos(user_id_listA,user_id_listEC) | strpos(user_id_listEC,user_id_listA)) // 5 of 425
compress
save "$dirpath_data/pge_cleaned/pge_sp_premise_gis.dta", replace
}

** 4b. APEP pumps
{
use "$dirpath_data/pge_cleaned/apep_pump_gis.dta", clear
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_wdist_conc_wide.dta", keep(1 3) nogen
foreach v of varlist user_id_list wdist_matches_count flag_wdist_nonhag_alloc_only user_id? username? source? {
	rename `v' `v'A
}
rename clu_id clu_idA
rename clu_id_ec clu_id 
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_wdist_conc_wide.dta", keep(1 3) nogen
foreach v of varlist user_id_list wdist_matches_count flag_wdist_nonhag_alloc_only user_id? username? source? {
	rename `v' `v'EC
}
rename clu_id clu_id_ec
rename clu_idA clu_id
gen user_id_list_match = user_id_listA==user_id_listEC
assert clu_id!=clu_id_ec if user_id_list_match==0
tab user_id_list_match
sort latlon_group
br latlon_group user_id_list* *A *EC if user_id_list_match==0
count if user_id_list_match==0 // only 152
count if user_id_list_match==0 & user_id_listA!="" & user_id_listEC!="" & ///
	(strpos(user_id_listA,user_id_listEC) | strpos(user_id_listEC,user_id_listA)) // 1 of 152
compress
save "$dirpath_data/pge_cleaned/apep_pump_gis.dta", replace
}

** 4c. SCE SPs
{
use "$dirpath_data/sce_cleaned/sce_sp_premise_gis.dta", clear
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_wdist_conc_wide.dta", keep(1 3) nogen
foreach v of varlist user_id_list wdist_matches_count flag_wdist_nonhag_alloc_only user_id? username? source? {
	rename `v' `v'A
}
rename clu_id clu_idA
rename clu_id_ec clu_id 
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_wdist_conc_wide.dta", keep(1 3) nogen
foreach v of varlist user_id_list wdist_matches_count flag_wdist_nonhag_alloc_only user_id? username? source? {
	rename `v' `v'EC
}
rename clu_id clu_id_ec
rename clu_idA clu_id
gen user_id_list_match = user_id_listA==user_id_listEC
assert clu_id!=clu_id_ec if user_id_list_match==0
tab user_id_list_match
br sp_uuid user_id_list* *A *EC if user_id_list_match==0
count if user_id_list_match==0 // only 381
count if user_id_list_match==0 & user_id_listA!="" & user_id_listEC!="" & ///
	(strpos(user_id_listA,user_id_listEC) | strpos(user_id_listEC,user_id_listA)) // 3 of 381
compress
save "$dirpath_data/sce_cleaned/sce_sp_premise_gis.dta", replace
}

}

*******************************************************************************
*******************************************************************************

** 5. Run auxiliary GIS scrpt "BUILD_com_points_in_water_districts.R", 
**    which plunks all 3 sets of lat/lons into water district polygons

*******************************************************************************
*******************************************************************************

** 6. Assign water districts to points directly, and harmonize
if 1==1{

** 6a. PGE SPs
{
	// import GIS output
import delimited "$dirpath_data/misc/pge_prem_coord_polygon_wdist_hag.txt", delimiter("%") clear 
tostring sp_uuid, replace
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
assert length(sp_uuid)==10 & real(sp_uuid)!=.
rename wdist wdist_nameP
rename in_wdist in_wdistP
rename wdist_id wdist_idP
rename nearestwdist_dist_m meters_to_nearest_wdistP
replace wdist_nameP = "" if wdist_nameP=="NA"
replace wdist_idP = "" if wdist_idP=="NA"
destring wdist_idP, replace
replace wdist_idP = nearestwdist_id if wdist_idP==. & nearestwdist_id!=0
assert wdist_idP!=.
drop nearestwdist_id prem_lat prem_long longitude latitude bad_geocode_flag pull
tempfile wdists
save `wdists'
	
	// merge GIS output into master
use "$dirpath_data/pge_cleaned/pge_sp_premise_gis.dta", clear
merge 1:1 sp_uuid using `wdists'
assert _merge!=2
assert missing_geocode_flag==1 if _merge==1
drop _merge
tab in_wdistP

	// assess cases where CLU-assignment did not find (settle on) a water district
gen temp_wdist_matchA = inlist(wdist_idP,user_id1A,user_id2A,user_id3A)
gen temp_wdist_matchEC = inlist(wdist_idP,user_id1EC,user_id2EC,user_id3EC)
tab temp_wdist_match* if wdist_idP!=. & meters_to_nearest_wdistP==0 
tab temp_wdist_match* if wdist_idP!=. & meters_to_nearest_wdistP==0 & in_clu==1 & in_clu_ec==1
br sp_uuid in_clu in_clu_ec user_id_list* wdist_idP meters_to_nearest_wdistP temp* ///
	if wdist_idP!=. & meters_to_nearest_wdistP==0 & temp_wdist_matchA+temp_wdist_matchEC<2
drop temp*
	
	// Step through heirarchy of water district assignments

	// Step 1: all water districts and CLUs match, and are inside (not adjacent to) polygons
gen wdist_user_id_list = ""
gen wdist_confidence_rank = .
gen wdist_confidence_desc = ""
gen temp = wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	inlist(wdist_idP,user_id1A,user_id2A,user_id3A) & in_wdistP==1 & ///
	in_clu==1 & in_clu_ec==1 & clu_id==clu_id_ec
replace temp = 1 if	wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	user_id_listA=="" & in_wdistP==0 & in_clu==1 & in_clu_ec==1 & clu_id==clu_id_ec
replace wdist_user_id_list = user_id_listA if temp==1
replace wdist_confidence_rank = 1 if temp==1
replace wdist_confidence_desc = "CLU and water district assignments all agree, and are exact" if temp==1
drop temp
tab wdist_confidence_rank, missing // 44% of SPs are done
	
	// Step 2: all water districts match and are inside; CLUs match but are adjacent, <23m away
gen temp = wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	inlist(wdist_idP,user_id1A,user_id2A,user_id3A) & in_wdistP==1 & ///
	clu_id==clu_id_ec & clu_ec_nearest_dist_m<23 // 50% pctile
replace temp = 1 if wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	user_id_listA=="" & in_wdistP==0 & ///
	clu_id==clu_id_ec & clu_ec_nearest_dist_m<23 // 50% pctile
replace wdist_user_id_list = user_id_listA if temp==1
replace wdist_confidence_rank = 2 if temp==1
replace wdist_confidence_desc = "Water district assignments agree; CLU assignemnts agree and are w/in 23m" if temp==1
drop temp
tab wdist_confidence_rank, missing // 68% of SPs are done
	
	// Step 3: all water districts match and are inside; CLUs match but are adjacent, <1km away
gen temp = wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	inlist(wdist_idP,user_id1A,user_id2A,user_id3A) & in_wdistP==1 & ///
	clu_id==clu_id_ec & clu_ec_nearest_dist_m<=100 
replace temp = 1 if wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	user_id_listA=="" & in_wdistP==0 & ///
	clu_id==clu_id_ec & clu_ec_nearest_dist_m<=1000 
replace wdist_user_id_list = user_id_listA if temp==1
replace wdist_confidence_rank = 3 if temp==1
replace wdist_confidence_desc = "Water district assignments agree; CLU assignemnts agree and are w/in 1km" if temp==1
drop temp
tab wdist_confidence_rank, missing // 85% of SPs are done
	
	// Step 4: all water districts match and are inside; CLUs disagree, EC CLU is <1km away
gen temp = wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	inlist(wdist_idP,user_id1A,user_id2A,user_id3A) & in_wdistP==1 & ///
	clu_ec_nearest_dist_m<=1000 
replace temp = 1 if wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	user_id_listA=="" & in_wdistP==0 & ///
	clu_ec_nearest_dist_m<=1000 
replace wdist_user_id_list = user_id_listA if temp==1
replace wdist_confidence_rank = 4 if temp==1
replace wdist_confidence_desc = "Water district assignments agree; ever-crop CLU is w/in 1km" if temp==1
drop temp
tab wdist_confidence_rank, missing // 92% of SPs are done
	
	// Step 5: CLU-assigned water districts match; CLUs match, and are inside (not adjacent to) polygons
gen temp = wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	in_clu==1 & in_clu_ec==1 & clu_id==clu_id_ec	
replace wdist_user_id_list = user_id_listA if temp==1
replace wdist_confidence_rank = 5 if temp==1
replace wdist_confidence_desc = "CLU water district assignments agree; CLU assignments agree and are exact" if temp==1
drop temp
tab wdist_confidence_rank, missing // 92% of SPs are done
	
	// Step 6: CLU-assigned water districts match; CLUs match but are adjacent, <23m away
gen temp = wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	clu_id==clu_id_ec & clu_ec_nearest_dist_m<23 // 50% pctile
replace wdist_user_id_list = user_id_listA if temp==1
replace wdist_confidence_rank = 6 if temp==1
replace wdist_confidence_desc = "CLU water district assignments agree; CLU assignemnts agree and are w/in 23m" if temp==1
drop temp
tab wdist_confidence_rank, missing // 93% of SPs are done

	// Step 7: CLU-assigned water districts match; CLUs match but are adjacent, <1km away
gen temp = wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	clu_id==clu_id_ec & clu_ec_nearest_dist_m<1000  
replace wdist_user_id_list = user_id_listA if temp==1
replace wdist_confidence_rank = 7 if temp==1
replace wdist_confidence_desc = "CLU water district assignments agree; CLU assignemnts agree and are w/in 1km" if temp==1
drop temp
tab wdist_confidence_rank, missing // 95% of SPs are done

	// Step 8: CLU-assigned water districts match; CLUs disagree, EC CLU is <1km away
gen temp = wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	clu_ec_nearest_dist_m<=1000 
replace wdist_user_id_list = user_id_listA if temp==1
replace wdist_confidence_rank = 8 if temp==1
replace wdist_confidence_desc = "CLU water district assignments agree; ever-crop CLU is w/in 1km" if temp==1
drop temp
tab wdist_confidence_rank, missing // 95% of SPs are done

	// Step 9: point-wise water district assigned within polygon
gen temp = wdist_confidence_rank==. & wdist_idP!=. & in_wdistP==1 & meters_to_nearest_wdistP==0
count if temp==1
count if temp==1 & user_id_listEC!="" // only 13 have ever-crop CLUs with assigned water districts
assert in_clu_ec==0 if temp==1
replace wdist_user_id_list = string(wdist_idP) if temp==1
replace wdist_confidence_rank = 9 if wdist_confidence_rank==.
replace wdist_confidence_desc = "Rely only on sharp pointwise assignment to water district polygons; CLUs missing or not agreeing" ///
	if wdist_confidence_rank==9
drop temp	

	// Drop unnecessary/redundant water district variables
drop user_id_listA user_id_listEC wdist_matches_count* flag_wdist_nonhag_alloc_only* ///
	user_id1* user_id2* user_id3* username1* username2* username3* source1* source2* source3* ///
	user_id_list_match wdist_nameP in_wdistP wdist_idP nearestwdist meters_to_nearest_wdistP

	// Split out matched water districts
split wdist_user_id_list, gen(user_id)	
destring user_id?, replace
forvalues i = 1/3 {
	rename user_id`i' user_id
	merge m:1 user_id using "$dirpath_data/surface_water/hagerty_surface_water_shapefiles_info.dta", ///
		nogen keep(1 3) keepusing(username source)
	rename user_id user_id`i'
	rename username username`i'
	rename source source`i'
}
count if user_id2!=. // 2,643 points are in 2 polygons
count if user_id3!=. // 4 points are in 3 polygons

	// Label
la var wdist_user_id_list "List of Hagerty water district polygons assigned to SP"
la var wdist_confidence_rank "Rank of how confident we are in water district assignments"
la var wdist_confidence_desc "Description of water district assignment confidences"
la var user_id1 "Assigned water district 1"
la var user_id2 "Assigned water district 2"
la var user_id3 "Assigned water district 3"

	// Save
sort sp_uuid
unique sp_uuid
assert r(unique)==r(N)
compress
save "$dirpath_data/pge_cleaned/pge_sp_premise_gis.dta", replace
}

** 6b. APEP pumps
{
	// import GIS output
import delimited "$dirpath_data/misc/apep_pump_coord_polygon_wdist_hag.txt", delimiter("%") clear 
destring latlon_group, replace
assert latlon_group!=.
unique latlon_group
assert r(unique)==r(N)
rename wdist wdist_nameP
rename in_wdist in_wdistP
rename wdist_id wdist_idP
rename nearestwdist_dist_m meters_to_nearest_wdistP
replace wdist_nameP = "" if wdist_nameP=="NA"
replace wdist_idP = "" if wdist_idP=="NA"
destring wdist_idP, replace
replace wdist_idP = nearestwdist_id if wdist_idP==. & nearestwdist_id!=0
assert wdist_idP!=.
drop nearestwdist_id pump_lat pump_long longitude latitude
tempfile wdists
save `wdists'

	// merge GIS output into master
use "$dirpath_data/pge_cleaned/apep_pump_gis.dta", clear
merge m:1 latlon_group using `wdists'
assert _merge==3
drop _merge
tab in_wdistP

	// assess cases where CLU-assignment did not find (settle on) a water district
gen temp_wdist_matchA = inlist(wdist_idP,user_id1A,user_id2A,user_id3A)
gen temp_wdist_matchEC = inlist(wdist_idP,user_id1EC,user_id2EC,user_id3EC)
tab temp_wdist_match* if wdist_idP!=. & meters_to_nearest_wdistP==0 
tab temp_wdist_match* if wdist_idP!=. & meters_to_nearest_wdistP==0 & in_clu==1 & in_clu_ec==1
br latlon_group in_clu in_clu_ec user_id_list* wdist_idP meters_to_nearest_wdistP temp* ///
	if wdist_idP!=. & meters_to_nearest_wdistP==0 & temp_wdist_matchA+temp_wdist_matchEC<2
drop temp*
	
	// Step through heirarchy of water district assignments

	// Step 1: all water districts and CLUs match, and are inside (not adjacent to) polygons
gen wdist_user_id_list = ""
gen wdist_confidence_rank = .
gen wdist_confidence_desc = ""
gen temp = wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	inlist(wdist_idP,user_id1A,user_id2A,user_id3A) & in_wdistP==1 & ///
	in_clu==1 & in_clu_ec==1 & clu_id==clu_id_ec
replace temp = 1 if	wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	user_id_listA=="" & in_wdistP==0 & in_clu==1 & in_clu_ec==1 & clu_id==clu_id_ec
replace wdist_user_id_list = user_id_listA if temp==1
replace wdist_confidence_rank = 1 if temp==1
replace wdist_confidence_desc = "CLU and water district assignments all agree, and are exact" if temp==1
drop temp
tab wdist_confidence_rank, missing // 50% of SPs are done
	
	// Step 2: all water districts match and are inside; CLUs match but are adjacent, <23m away
gen temp = wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	inlist(wdist_idP,user_id1A,user_id2A,user_id3A) & in_wdistP==1 & ///
	clu_id==clu_id_ec & clu_ec_nearest_dist_m<23 // 50% pctile
replace temp = 1 if wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	user_id_listA=="" & in_wdistP==0 & ///
	clu_id==clu_id_ec & clu_ec_nearest_dist_m<23 // 50% pctile
replace wdist_user_id_list = user_id_listA if temp==1
replace wdist_confidence_rank = 2 if temp==1
replace wdist_confidence_desc = "Water district assignments agree; CLU assignemnts agree and are w/in 23m" if temp==1
drop temp
tab wdist_confidence_rank, missing // 69% of SPs are done
	
	// Step 3: all water districts match and are inside; CLUs match but are adjacent, <1km away
gen temp = wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	inlist(wdist_idP,user_id1A,user_id2A,user_id3A) & in_wdistP==1 & ///
	clu_id==clu_id_ec & clu_ec_nearest_dist_m<=100 
replace temp = 1 if wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	user_id_listA=="" & in_wdistP==0 & ///
	clu_id==clu_id_ec & clu_ec_nearest_dist_m<=1000 
replace wdist_user_id_list = user_id_listA if temp==1
replace wdist_confidence_rank = 3 if temp==1
replace wdist_confidence_desc = "Water district assignments agree; CLU assignemnts agree and are w/in 1km" if temp==1
drop temp
tab wdist_confidence_rank, missing // 78% of SPs are done
	
	// Step 4: all water districts match and are inside; CLUs disagree, EC CLU is <1km away
gen temp = wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	inlist(wdist_idP,user_id1A,user_id2A,user_id3A) & in_wdistP==1 & ///
	clu_ec_nearest_dist_m<=1000 
replace temp = 1 if wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	user_id_listA=="" & in_wdistP==0 & ///
	clu_ec_nearest_dist_m<=1000 
replace wdist_user_id_list = user_id_listA if temp==1
replace wdist_confidence_rank = 4 if temp==1
replace wdist_confidence_desc = "Water district assignments agree; ever-crop CLU is w/in 1km" if temp==1
drop temp
tab wdist_confidence_rank, missing // 88% of SPs are done
	
	// Step 5: CLU-assigned water districts match; CLUs match, and are inside (not adjacent to) polygons
gen temp = wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	in_clu==1 & in_clu_ec==1 & clu_id==clu_id_ec	
replace wdist_user_id_list = user_id_listA if temp==1
replace wdist_confidence_rank = 5 if temp==1
replace wdist_confidence_desc = "CLU water district assignments agree; CLU assignments agree and are exact" if temp==1
drop temp
tab wdist_confidence_rank, missing // 88% of SPs are done
	
	// Step 6: CLU-assigned water districts match; CLUs match but are adjacent, <23m away
gen temp = wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	clu_id==clu_id_ec & clu_ec_nearest_dist_m<23 // 50% pctile
replace wdist_user_id_list = user_id_listA if temp==1
replace wdist_confidence_rank = 6 if temp==1
replace wdist_confidence_desc = "CLU water district assignments agree; CLU assignemnts agree and are w/in 23m" if temp==1
drop temp
tab wdist_confidence_rank, missing // 88% of SPs are done

	// Step 7: CLU-assigned water districts match; CLUs match but are adjacent, <1km away
gen temp = wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	clu_id==clu_id_ec & clu_ec_nearest_dist_m<1000  
replace wdist_user_id_list = user_id_listA if temp==1
replace wdist_confidence_rank = 7 if temp==1
replace wdist_confidence_desc = "CLU water district assignments agree; CLU assignemnts agree and are w/in 1km" if temp==1
drop temp
tab wdist_confidence_rank, missing // 91% of SPs are done

	// Step 8: CLU-assigned water districts match; CLUs disagree, EC CLU is <1km away
gen temp = wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	clu_ec_nearest_dist_m<=1000 
replace wdist_user_id_list = user_id_listA if temp==1
replace wdist_confidence_rank = 8 if temp==1
replace wdist_confidence_desc = "CLU water district assignments agree; ever-crop CLU is w/in 1km" if temp==1
drop temp
tab wdist_confidence_rank, missing // 91% of SPs are done

	// Step 9: point-wise water district assigned within polygon
gen temp = wdist_confidence_rank==. & wdist_idP!=. & in_wdistP==1 & meters_to_nearest_wdistP==0
count if temp==1
count if temp==1 & user_id_listEC!="" // only 46 have ever-crop CLUs with assigned water districts
assert in_clu_ec==0 if temp==1
replace wdist_user_id_list = string(wdist_idP) if temp==1
replace wdist_confidence_rank = 9 if wdist_confidence_rank==.
replace wdist_confidence_desc = "Rely only on sharp pointwise assignment to water district polygons; CLUs missing or not agreeing" ///
	if wdist_confidence_rank==9
drop temp	

	// Drop unnecessary/redundant water district variables
drop user_id_listA user_id_listEC wdist_matches_count* flag_wdist_nonhag_alloc_only* ///
	user_id1* user_id2* user_id3* username1* username2* username3* source1* source2* source3* ///
	user_id_list_match wdist_nameP in_wdistP wdist_idP nearestwdist meters_to_nearest_wdistP

	// Split out matched water districts
split wdist_user_id_list, gen(user_id)	
destring user_id?, replace
forvalues i = 1/2 {
	rename user_id`i' user_id
	merge m:1 user_id using "$dirpath_data/surface_water/hagerty_surface_water_shapefiles_info.dta", ///
		nogen keep(1 3) keepusing(username source)
	rename user_id user_id`i'
	rename username username`i'
	rename source source`i'
}
count if user_id2!=. // 1,237 points are in 2 polygons
//count if user_id3!=. // 0 points are in 3 polygons

	// Label
la var wdist_user_id_list "List of water district polygons assigned to pumps"
la var wdist_confidence_rank "Rank of how confident we are in water district assignments"
la var wdist_confidence_desc "Description of water district assignment confidences"
la var user_id1 "Assigned water district 1"
la var user_id2 "Assigned water district 2"
//la var user_id3 "Assigned water district 3"

	// save
sort latlon_group
unique latlon_group apeptestid
assert r(unique)==r(N)
compress
save "$dirpath_data/pge_cleaned/apep_pump_gis.dta", replace
}

** 6c. SCE SPs
{
	// import GIS output
import delimited "$dirpath_data/misc/sce_prem_coord_polygon_wdist_hag.txt", delimiter("%") clear 
tostring sp_uuid, replace
assert real(sp_uuid)!=.
rename wdist wdist_nameP
rename in_wdist in_wdistP
rename wdist_id wdist_idP
rename nearestwdist_dist_m meters_to_nearest_wdistP
replace wdist_nameP = "" if wdist_nameP=="NA"
replace wdist_idP = "" if wdist_idP=="NA"
destring wdist_idP, replace
replace wdist_idP = nearestwdist_id if wdist_idP==. & nearestwdist_id!=0
assert wdist_idP!=.
drop nearestwdist_id prem_lat prem_long longitude latitude bad_geocode_flag pull
tempfile wdists
save `wdists'
	
	// merge GIS output into master
use "$dirpath_data/sce_cleaned/sce_sp_premise_gis.dta", clear
merge 1:1 sp_uuid using `wdists'
assert _merge!=2
assert missing_geocode_flag==1 if _merge==1
drop _merge
tab in_wdistP

	// assess cases where CLU-assignment did not find (settle on) a water district
gen temp_wdist_matchA = inlist(wdist_idP,user_id1A,user_id2A,user_id3A)
gen temp_wdist_matchEC = inlist(wdist_idP,user_id1EC,user_id2EC,user_id3EC)
tab temp_wdist_match* if wdist_idP!=. & meters_to_nearest_wdistP==0 
tab temp_wdist_match* if wdist_idP!=. & meters_to_nearest_wdistP==0 & in_clu==1 & in_clu_ec==1
br sp_uuid in_clu in_clu_ec user_id_list* wdist_idP meters_to_nearest_wdistP temp* ///
	if wdist_idP!=. & meters_to_nearest_wdistP==0 & temp_wdist_matchA+temp_wdist_matchEC<2
drop temp*
	
	// Step through heirarchy of water district assignments

	// Step 1: all water districts and CLUs match, and are inside (not adjacent to) polygons
gen wdist_user_id_list = ""
gen wdist_confidence_rank = .
gen wdist_confidence_desc = ""
gen temp = wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	inlist(wdist_idP,user_id1A,user_id2A,user_id3A) & in_wdistP==1 & ///
	in_clu==1 & in_clu_ec==1 & clu_id==clu_id_ec
replace temp = 1 if	wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	user_id_listA=="" & in_wdistP==0 & in_clu==1 & in_clu_ec==1 & clu_id==clu_id_ec
replace wdist_user_id_list = user_id_listA if temp==1
replace wdist_confidence_rank = 1 if temp==1
replace wdist_confidence_desc = "CLU and water district assignments all agree, and are exact" if temp==1
drop temp
tab wdist_confidence_rank, missing // 21% of SPs are done
	
	// Step 2: all water districts match and are inside; CLUs match but are adjacent, <23m away
gen temp = wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	inlist(wdist_idP,user_id1A,user_id2A,user_id3A) & in_wdistP==1 & ///
	clu_id==clu_id_ec & clu_ec_nearest_dist_m<23 // 50% pctile
replace temp = 1 if wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	user_id_listA=="" & in_wdistP==0 & ///
	clu_id==clu_id_ec & clu_ec_nearest_dist_m<23 // 50% pctile
replace wdist_user_id_list = user_id_listA if temp==1
replace wdist_confidence_rank = 2 if temp==1
replace wdist_confidence_desc = "Water district assignments agree; CLU assignemnts agree and are w/in 23m" if temp==1
drop temp
tab wdist_confidence_rank, missing // 35% of SPs are done
	
	// Step 3: all water districts match and are inside; CLUs match but are adjacent, <1km away
gen temp = wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	inlist(wdist_idP,user_id1A,user_id2A,user_id3A) & in_wdistP==1 & ///
	clu_id==clu_id_ec & clu_ec_nearest_dist_m<=100 
replace temp = 1 if wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	user_id_listA=="" & in_wdistP==0 & ///
	clu_id==clu_id_ec & clu_ec_nearest_dist_m<=1000 
replace wdist_user_id_list = user_id_listA if temp==1
replace wdist_confidence_rank = 3 if temp==1
replace wdist_confidence_desc = "Water district assignments agree; CLU assignemnts agree and are w/in 1km" if temp==1
drop temp
tab wdist_confidence_rank, missing // 43% of SPs are done
	
	// Step 4: all water districts match and are inside; CLUs disagree, EC CLU is <1km away
gen temp = wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	inlist(wdist_idP,user_id1A,user_id2A,user_id3A) & in_wdistP==1 & ///
	clu_ec_nearest_dist_m<=1000 
replace temp = 1 if wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	user_id_listA=="" & in_wdistP==0 & ///
	clu_ec_nearest_dist_m<=1000 
replace wdist_user_id_list = user_id_listA if temp==1
replace wdist_confidence_rank = 4 if temp==1
replace wdist_confidence_desc = "Water district assignments agree; ever-crop CLU is w/in 1km" if temp==1
drop temp
tab wdist_confidence_rank, missing // 52% of SPs are done
	
	// Step 5: CLU-assigned water districts match; CLUs match, and are inside (not adjacent to) polygons
gen temp = wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	in_clu==1 & in_clu_ec==1 & clu_id==clu_id_ec	
replace wdist_user_id_list = user_id_listA if temp==1
replace wdist_confidence_rank = 5 if temp==1
replace wdist_confidence_desc = "CLU water district assignments agree; CLU assignments agree and are exact" if temp==1
drop temp
tab wdist_confidence_rank, missing // 52% of SPs are done
	
	// Step 6: CLU-assigned water districts match; CLUs match but are adjacent, <23m away
gen temp = wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	clu_id==clu_id_ec & clu_ec_nearest_dist_m<23 // 50% pctile
replace wdist_user_id_list = user_id_listA if temp==1
replace wdist_confidence_rank = 6 if temp==1
replace wdist_confidence_desc = "CLU water district assignments agree; CLU assignemnts agree and are w/in 23m" if temp==1
drop temp
tab wdist_confidence_rank, missing // 52% of SPs are done

	// Step 7: CLU-assigned water districts match; CLUs match but are adjacent, <1km away
gen temp = wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	clu_id==clu_id_ec & clu_ec_nearest_dist_m<1000  
replace wdist_user_id_list = user_id_listA if temp==1
replace wdist_confidence_rank = 7 if temp==1
replace wdist_confidence_desc = "CLU water district assignments agree; CLU assignemnts agree and are w/in 1km" if temp==1
drop temp
tab wdist_confidence_rank, missing // 53% of SPs are done

	// Step 8: CLU-assigned water districts match; CLUs disagree, EC CLU is <1km away
gen temp = wdist_confidence_rank==. & user_id_listA==user_id_listEC & ///
	clu_ec_nearest_dist_m<=1000 
replace wdist_user_id_list = user_id_listA if temp==1
replace wdist_confidence_rank = 8 if temp==1
replace wdist_confidence_desc = "CLU water district assignments agree; ever-crop CLU is w/in 1km" if temp==1
drop temp
tab wdist_confidence_rank, missing // 53% of SPs are done

	// Step 9: point-wise water district assigned within polygon
gen temp = wdist_confidence_rank==. & wdist_idP!=. & in_wdistP==1 & meters_to_nearest_wdistP==0
count if temp==1
count if temp==1 & user_id_listEC!="" // only 46 have ever-crop CLUs with assigned water districts
assert in_clu_ec==0 if temp==1
replace wdist_user_id_list = string(wdist_idP) if temp==1
replace wdist_confidence_rank = 9 if wdist_confidence_rank==.
replace wdist_confidence_desc = "Rely only on sharp pointwise assignment to water district polygons; CLUs missing or not agreeing" ///
	if wdist_confidence_rank==9
drop temp	

	// Breakdown is not as good as PGE SPs, since almost half of SCE SPs aren't assigned to CLUs!
count if in_clu_ec==1
count if in_clu_ec==1 | clu_ec_nearest_dist_m<1000
count if clu_id_ec==""

	// Drop unnecessary/redundant water district variables
drop user_id_listA user_id_listEC wdist_matches_count* flag_wdist_nonhag_alloc_only* ///
	user_id1* user_id2* user_id3* username1* username2* username3* source1* source2* source3* ///
	user_id_list_match wdist_nameP in_wdistP wdist_idP nearestwdist meters_to_nearest_wdistP

	// Split out matched water districts
split wdist_user_id_list, gen(user_id)	
destring user_id?, replace
forvalues i = 1/3 {
	rename user_id`i' user_id
	merge m:1 user_id using "$dirpath_data/surface_water/hagerty_surface_water_shapefiles_info.dta", ///
		nogen keep(1 3) keepusing(username source)
	rename user_id user_id`i'
	rename username username`i'
	rename source source`i'
}
count if user_id2!=. // 1,870 points are in 2 polygons
count if user_id3!=. // 22 points are in 3 polygons

	// Label
la var wdist_user_id_list "List of water district polygons assigned to SP"
la var wdist_confidence_rank "Rank of how confident we are in water district assignments"
la var wdist_confidence_desc "Description of water district assignment confidences"
la var user_id1 "Assigned water district 1"
la var user_id2 "Assigned water district 2"
la var user_id3 "Assigned water district 3"

	// save
sort sp_uuid
unique sp_uuid
assert r(unique)==r(N)
compress
save "$dirpath_data/sce_cleaned/sce_sp_premise_gis.dta", replace
}

}

*******************************************************************************
*******************************************************************************



