clear all
version 13
set more off

***************************************************************
**** Script to aggregate CLUs units up to "farms" (parcels) ***
***************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"
global dirpath_code "T:/Home/Louis/backup/AgEE/AgEE_code/build"
*global R_exe_path "C:/PROGRA~1/MIE74D~1/ROPEN~1/bin/x64/R"
*global R_lib_path "C:/Program Files/Microsoft/R Open/R-3.4.4/library"

*******************************************************************************
*******************************************************************************

** 1. Aggregate parcel & CLU polygons up to "super-polygons"
{
	// Start with full concordance
use "$dirpath_data/cleaned_spatial/clu_parcel_conc.dta", clear
assert parcelid!="" & clu_id!=""
unique parcelid
unique clu_id
unique clu_id if ever_crop_clu==1

	// Bring in county as assigned by GIS (not necessarily the county named in CLU_ID)
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_county_conc.dta", ///
	keep(1 3) keepusing(county_name)
assert _merge==3
drop _merge
unique clu_id
local uniq = r(unique)
unique clu_id if county==county_name
di r(unique)/`uniq' 
	// for 99.8% of CLUs, this distinction doesn't matter
	// this code will proceed by using the raw county assignments, which form the
	// basis of the parcel-CLU concordance to begin with
drop county_name
	
	// Drop barely-matched parcels and slivers
drop if drop_parcelid_total==1
drop if drop_slivers==1
unique parcelid
unique clu_id
unique clu_id if ever_crop_clu==1

	// Created numeric IDs, indexed separately for each county (to facilitate joins below)
egen group_p = group(parcelid)	
egen group_c = group(clu_id)
egen temp_p = min(group_p), by(county)
egen temp_c = min(group_c), by(county)
replace group_p = group_p - temp_p + 1
replace group_c = group_c - temp_c + 1

	// Confirm uniqueness
unique group_p county
local uniq = r(unique)
unique parcelid
assert r(unique)==`uniq'
unique group_c county
local uniq = r(unique)
unique clu_id
assert r(unique)==`uniq'

	// Flag duplicates
duplicates t group_p county, gen(dup_p)
duplicates t group_c county, gen(dup_c)

	// Group all like-merged CLUs, looping over counties (removing small intersections)
foreach s in 0 10 25 50 75 { // loop over small intersection cutoffs

	qui gen temp_clu_ratios`s' = .
	qui levelsof county, local(levs)
	foreach cty in `levs' { // loop over counties
		
		// Keep only observations in county
		preserve
		qui keep if county=="`cty'"
		
		// Flag largest intersections for each CLU, and drop small intersections
		qui egen double temp = max(intacres), by(group_c)
		qui unique group_c
		local uniq = r(unique)
		drop if intacres<`s'/100 & intacres<temp & largest_clu==0 & largest_parcel==0 
			// dropping small intersections, but never the major polygon of a merge
		qui unique group_c
		assert r(unique)==`uniq'
			// we don't want two sets of intermerged polygons to be grouped together 
			// because of one tenuous intersection that's very small
		
		// Keep essential variables only
		qui keep group_p group_c
		qui unique group_p group_c
		assert r(unique)==r(N)
		
		// Outerjoin dataset to itself on parcel ID
		tempfile `cty'_temp1
		qui save ``cty'_temp1'
		rename group_c group_cM
		qui joinby group_p using ``cty'_temp1', unmatched(both)
		assert _merge==3
		drop _merge group_p
		qui duplicates drop
		
		// Drop doubled matches (i.e A joins to B, then B also joins to A)
		sort group_cM group_c
		qui drop if group_c>group_cM
		
		// Define group variable for each CLU, as its minimum matched CLU ID
		qui egen c_group = min(group_c), by(group_cM)
		qui unique group_cM
		local uniq1 = r(unique)
		qui unique c_group
		local uniq2 = r(unique)
		di "`cty' `s': `uniq1' unique CLUs"
		di "`cty' `s': `uniq2' non-disjoint groups"
		
		// Drop matched CLU ID (no longer needed)
		drop group_c
		qui duplicates drop
		rename group_cM group_c
		qui unique group_c
		assert r(unique)==r(N)
		sort group_c
		assert group_c==_n 
		
		// Combine non-disjoint groups so they become disjoint
		qui levelsof c_group, local(levs)
		foreach g in `levs' {
			if c_group[`g']!=`g' { 
				qui replace c_group = c_group[`g'] if c_group==`g'
			}
		}

		// Confirm groups are now fully disjoint
		qui levelsof c_group, local(levs)
		foreach g in `levs' {
			assert c_group[`g']==`g'
		}
		// (If A joins to B, and B joins to C, but C doesn't joing to A, then the above
		// step assigned C to B's group, when B doesn't have it's own group. This loop
		// concisely fixes all of these non-disjoint groupings by switching C's group
		// from B to A, in all such cases.)
		
		qui unique c_group
		local uniq3 = r(unique)
		di "`cty' `s': `uniq3' disjoint groups"
		local ratio = string(`uniq1'/`uniq3',"%9.2f")
		di "`cty' `s': `ratio' ratio of polygons to disjoint groups" _n
		
		// Merge back into main dataset
		gen county = "`cty'"
		rename c_group c_group`s'
		tempfile `cty'_temp2
		qui save ``cty'_temp2'
		restore
		qui merge m:1 county group_c using ``cty'_temp2', update
		qui replace temp_clu_ratios`s' = `uniq1'/`uniq3' if county=="`cty'"
		assert _merge>=3 if county=="`cty'"
		drop _merge
	}	
	unique county group_c	
	unique county c_group`s'
	la var c_group`s' "CLU group identifiers, ignoring intersections <0.`s' acres"
	assert c_group`s'!=.

}	

	// Group all like-merged parcels, looping over counties (removing small intersections)
foreach s in 0 10 25 50 75 { // loop over small intersection cutoffs

	qui gen temp_parcel_ratios`s' = .
	qui levelsof county, local(levs)
	foreach cty in `levs' { // loop over counties
		
		// Keep only observations in county
		preserve
		qui keep if county=="`cty'"
		
		// Flag largest intersections for each parcel, and drop small intersections
		qui egen double temp = max(intacres), by(group_p)
		qui unique group_p
		local uniq = r(unique)
		drop if intacres<`s'/100 & intacres<temp & largest_clu==0 & largest_parcel==0 
			// dropping small intersections, but never the major polygon of a merge
		qui unique group_p
		assert r(unique)==`uniq'
			// we don't want two sets of intermerged polygons to be grouped together 
			// because of one tenuous intersection that's very small

		// Keep essential variables only
		qui keep group_p group_c
		qui unique group_p group_c
		assert r(unique)==r(N)
		
		// Outerjoin dataset to itself on CLU ID
		tempfile `cty'_temp1
		qui save ``cty'_temp1'
		rename group_p group_pM
		qui joinby group_c using ``cty'_temp1', unmatched(both)
		assert _merge==3
		drop _merge group_c
		qui duplicates drop
		
		// Drop doubled matches (i.e A joins to B, then B also joins to A)
		sort group_pM group_p
		qui drop if group_p>group_pM
		
		// Define group variable for each parcel, as its minimum matched parcel ID
		qui egen p_group = min(group_p), by(group_pM)
		qui unique group_pM
		local uniq1 = r(unique)
		qui unique p_group
		local uniq2 = r(unique)
		di "`cty' `s': `uniq1' parcels"
		di "`cty' `s': `uniq2' non-disjoint groups"
		
		// Drop matched parcel ID (no longer needed)
		drop group_p
		qui duplicates drop
		rename group_pM group_p
		qui unique group_p
		assert r(unique)==r(N)
		sort group_p
		assert group_p==_n 
		
		// Combine non-disjoint groups so they become disjoint
		qui levelsof p_group, local(levs)
		foreach g in `levs' {
			if p_group[`g']!=`g' { 
				qui replace p_group = p_group[`g'] if p_group==`g'
			}
		}
		
		// Confirm groups are now fully disjoint
		qui levelsof p_group, local(levs)
		foreach g in `levs' {
			assert p_group[`g']==`g'
		}

		// (If A joins to B, and B joins to C, but C doesn't joing to A, then the above
		// step assigned C to B's group, when B doesn't have it's own group. This loop
		// concisely fixes all of these non-disjoint groupings by switching C's group
		// from B to A, in all such cases.)
		
		qui unique p_group
		local uniq3 = r(unique)
		di "`cty' `s': `uniq3' disjoint groups"
		local ratio = string(`uniq1'/`uniq3',"%9.2f")
		di "`cty' `s': `ratio' ratio of polygons to disjoint groups" _n
		
		// Merge back into main dataset
		gen county = "`cty'"
		rename p_group p_group`s'
		tempfile `cty'_temp2
		qui save ``cty'_temp2'
		restore
		qui merge m:1 county group_p using ``cty'_temp2', update
		qui replace temp_parcel_ratios`s' = `uniq1'/`uniq3' if county=="`cty'"
		assert _merge>=3 if county=="`cty'"
		drop _merge
	}	
	unique county group_p	
	unique county p_group`s'
	la var p_group`s' "Parcel group identifiers, ignoring intersections <0.`s' acres"
	assert p_group`s'!=.

}	

preserve
gen uniq_clu = .
levelsof county, local(levs) 
foreach cty in `levs' {
	unique group_c if county=="`cty'"
	replace uniq_clu = r(unique) if county=="`cty'"
}
keep county temp_*_ratios* uniq_clu
duplicates drop
gsort -uniq_clu
list
restore

	// Reindex to non-county-specific IDs
foreach v of varlist c_group* p_group* {
	assert `v'!=.
	unique county `v'
	local uniq = r(unique)
	tostring `v', replace
	replace `v' = county + " " + `v'
	unique `v'
	assert r(unique)==`uniq'
}
	
	// Rename group IDs
rename c_group* clu_group*
rename p_group* parcel_group*

	// Drop unnecessary variables
drop temp* group_p group_c dup_p dup_c	
compress

	// Save CLU groups
preserve
keep county clu_id clu_group*	
duplicates drop
unique clu_id
assert r(unique)==r(N)	
gen temp = _n
foreach v of varlist clu_group* {
	assert `v'!=""
	local vnew = subinstr("`v'","clu_group","clu_count",1)
	egen `vnew' = count(temp), by(`v')
	la var `vnew' "Number of unique CLUs grouped in `v'"
}
foreach v of varlist clu_group* {
	local vnew = subinstr("`v'","clu_group","temp_tag",1)
	egen `vnew' = tag(`v')
}	
foreach v of varlist clu_count* {
	local vnew = subinstr("`v'","clu_count","temp_count",1)
	gen `vnew' = min(`v',10)
}	
twoway ///
	(hist temp_count0  if temp_tag0 , w(1) color(black) fcolor(none) lw(thick)) ///
	(hist temp_count10 if temp_tag10, w(1) color(blue) fcolor(none) lw(thick)) ///
	(hist temp_count25 if temp_tag25, w(1) color(green) fcolor(none) lw(thick)) ///
	(hist temp_count50 if temp_tag50, w(1) color(red) fcolor(none) lw(thick)) ///
	(hist temp_count75 if temp_tag75, w(1) color(yellow) fcolor(none) lw(thick)), ///
	legend(order(1 "0" 2 "10" 3 "25" 4 "50" 5 "75") c(5))
drop temp*
sort clu_id
order county clu_id
compress
save "$dirpath_data/cleaned_spatial/clu_conc_groups.dta", replace
restore

	// Save parcel groups
preserve
keep county parcelid parcel_group*	
duplicates drop
unique parcelid
assert r(unique)==r(N)
gen temp = _n
foreach v of varlist parcel_group* {
	assert `v'!=""
	local vnew = subinstr("`v'","parcel_group","parcel_count",1)
	egen `vnew' = count(temp), by(`v')
	la var `vnew' "Number of unique parcels grouped in `v'"
}	
foreach v of varlist parcel_group* {
	local vnew = subinstr("`v'","parcel_group","temp_tag",1)
	egen `vnew' = tag(`v')
}	
foreach v of varlist parcel_count* {
	local vnew = subinstr("`v'","parcel_count","temp_count",1)
	gen `vnew' = min(`v',10)
}	
twoway ///
	(hist temp_count0  if temp_tag0 , w(1) color(black) fcolor(none) lw(thick)) ///
	(hist temp_count10 if temp_tag10, w(1) color(blue) fcolor(none) lw(thick)) ///
	(hist temp_count25 if temp_tag25, w(1) color(green) fcolor(none) lw(thick)) ///
	(hist temp_count50 if temp_tag50, w(1) color(red) fcolor(none) lw(thick)) ///
	(hist temp_count75 if temp_tag75, w(1) color(yellow) fcolor(none) lw(thick)), ///
	legend(order(1 "0" 2 "10" 3 "25" 4 "50" 5 "75") c(5))
drop temp*
sort parcelid
order county parcelid	
compress
save "$dirpath_data/cleaned_spatial/parcel_conc_groups.dta", replace
restore

	// Save group-wise concordances
foreach s in 0 10 25 50 75 {
	preserve
	keep county clu_group`s' parcel_group`s'
	duplicates drop
	sort *
	compress
	save "$dirpath_data/cleaned_spatial/groups_conc`s'.dta", replace
	restore
}
	
	// Save full conconrdance with both sets of groups
keep county clu_id parcelid
unique clu_id parcelid
assert r(unique)==r(N)
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_conc_groups.dta", nogen
merge m:1 parcelid using "$dirpath_data/cleaned_spatial/parcel_conc_groups.dta", nogen
merge m:1 clu_id using "$dirpath_data/cleaned_spatial/clu_county_conc.dta", ///
	keep(1 3) keepusing(county_name) nogen
sort county clu_id parcelid
order county clu_id parcelid
compress
save "$dirpath_data/cleaned_spatial/clu_parcel_conc_groups_full.dta", replace

}

*******************************************************************************
*******************************************************************************



