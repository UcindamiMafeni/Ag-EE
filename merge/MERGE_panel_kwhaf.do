clear all
version 13
set more off

**********************************************************************
**** Script to contruct SP-month panel of kWh/AF conversion rates ****
**********************************************************************

global dirpath "S:/Matt/ag_pump"
global dirpath_data "$dirpath/data"

	***** COME BACK AND FIX THIS STUFF LATER:
	
	*1. Deal with the SPs with multiple APEP projets (right now, I'm ignoring project #2)
	
*******************************************************************************
*******************************************************************************

** 1. Construct SP-by-month panel of kwh/af conversion rates!
{

** Start with SP-APEP merge
use "$dirpath_data/merged/sp_apep_proj_merged.dta", clear

** Keep essential variables only
keep sp_uuid customertype farmtype waterenduse apeptestid test_date_stata apeptestid_uniq apep_proj_count ///
	date_proj_finish* est_savings_kwh_yr*

** Merge in SP-level GIS data
merge m:1 sp_uuid using "$dirpath_data/pge_cleaned/sp_premise_gis.dta", keep(1 3) nogen ///
	keepusing(prem_lat prem_long bad_geocode_flag missing_geocode_flag wdist_id county_fips ///
	basin_id basin_sub_id)
foreach v of varlist bad_geocode_flag missing_geocode_flag wdist_id county_fips basin_id basin_sub_id {
	rename `v' `v'SP
}

** Merge in pump test variables
merge 1:1 apeptestid customertype farmtyp waterenduse test_date_stata using ///
	"$dirpath_data/pge_cleaned/pump_test_data.dta", keep(1 3) nogen	

** Merge in pump-level GIS data
merge m:1 apeptestid crop using "$dirpath_data/pge_cleaned/apep_pump_gis.dta", keep(1 3) nogen ///
	keepusing(pumplatnew pumplongnew latlon_group bad_geocode_flag wdist_id county_fips basin_id basin_sub_id)

** Create numeric groups	
egen basin_id_group = group(basin_id)
egen basin_sub_id_group = group(basin_sub_id)
egen basin_idSP_group = group(basin_idSP)
egen basin_sub_idSP_group = group(basin_sub_idSP)

** Create test modate
gen test_modate = ym(year(test_date_stata),month(test_date_stata))
format %tm test_modate
sort sp_uuid test_modate test_date_stata

** Create SP-pump identifiers
egen sp_pump_id = group(sp_uuid pumpmke mtrmake mtrsn hp_nameplate rpm_nameplate), missing

** Create more fine-grained SP-pump ID for SPs with two otherwise indistiguishable tests in same month
duplicates t sp_uuid sp_pump_id test_modate, gen(dup)
br dup sp_uuid sp_pump_id watersource mtrmake mtrsn *nameplate test_modate test_date_stata flow_gpm kwhaf af24hrs ope hp ///
	pumpmke gearheadmake drvtype pumptype latlon_group if dup>0
	// Not an obvious way to split these. Almost all appear to be widely varying tests of a single pump
	// at a single point in time! Will treat each as its own time series for the purposes of estimating
	// kwh/af, then collapse to SP/month-specific averages 
gen temp_row = _n if dup>0
egen sp_pump_id2 = group(sp_pump_id temp_row) if dup>0
egen sp_pump_id3 = group(sp_pump_id sp_pump_id2), missing	
unique sp_pump_id // to be averaged weighting by flow_gpm
unique sp_pump_id if dup==0	
unique sp_pump_id if dup>0
unique sp_pump_id2 if dup>0
unique sp_pump_id3 if dup>0 // to be unweightedly-averaged
drop temp*	
	
** Establish SP-specific cases, to use when collapsing full panel to the SP/month level
gen case = .
egen sp_uuid_tag = tag(sp_uuid)

	// CASE 1: singleton pump/month, no projects (could still have multiple tests for 1 pump on 1 date)
egen temp1 = min(sp_pump_id), by(sp_uuid)
egen temp2 = max(sp_pump_id), by(sp_uuid)
egen temp3 = group(sp_pump_id test_modate), missing
egen temp4 = min(temp3), by(sp_uuid)
egen temp5 = max(temp3), by(sp_uuid)	
replace case = 1 if temp1==temp2 & temp4==temp5	& apep_proj_count==0
tab case apep_proj_count if sp_uuid_tag, missing
drop temp*

	// CASE 2: single pump, multiple months, no projects (could still have multiple tests for 1 pump on 1 date)
egen temp1 = min(sp_pump_id), by(sp_uuid)
egen temp2 = max(sp_pump_id), by(sp_uuid)
egen temp3 = group(sp_pump_id test_modate), missing
egen temp4 = min(temp3), by(sp_uuid)
egen temp5 = max(temp3), by(sp_uuid)	
replace case = 2 if temp1==temp2 & temp4<temp5 & apep_proj_count==0 & case==.
tab case apep_proj_count if sp_uuid_tag, missing
drop temp*

	// CASE 3: multiple pumps, multiple months, non-conflicting date ranges, no projects 
egen temp1 = min(sp_pump_id), by(sp_uuid)
egen temp2 = max(sp_pump_id), by(sp_uuid)
egen temp3 = group(sp_pump_id test_modate), missing
egen temp4 = min(temp3), by(sp_uuid)
egen temp5 = max(temp3), by(sp_uuid)	
egen temp6 = min(test_date_stata), by(sp_pump_id)
egen temp7 = max(test_date_stata), by(sp_pump_id)
sort sp_uuid test_date_stata sp_pump_id
gen temp8 = (sp_uuid==sp_uuid[_n-1] & sp_pump_id!=sp_pump_id[_n-1] & temp6<temp7[_n-1]) | ///
	(sp_uuid==sp_uuid[_n+1] & sp_pump_id!=sp_pump_id[_n+1] & temp7>temp6[_n+1])
egen temp9 = max(temp8), by(sp_uuid)
replace case = 3 if temp1<temp2 & temp4<temp5 & temp9==0 & apep_proj_count==0 & case==.
tab case apep_proj_count if sp_uuid_tag, missing
drop temp*

	// CASE 4: multiple pumps, multiple months, conflicting date ranges, no projects 
egen temp1 = min(sp_pump_id), by(sp_uuid)
egen temp2 = max(sp_pump_id), by(sp_uuid)
egen temp3 = group(sp_pump_id test_modate), missing
egen temp4 = min(temp3), by(sp_uuid)
egen temp5 = max(temp3), by(sp_uuid)	
egen temp6 = min(test_date_stata), by(sp_pump_id)
egen temp7 = max(test_date_stata), by(sp_pump_id)
sort sp_uuid test_date_stata sp_pump_id
gen temp8 = (sp_uuid==sp_uuid[_n-1] & sp_pump_id!=sp_pump_id[_n-1] & temp6<temp7[_n-1]) | ///
	(sp_uuid==sp_uuid[_n+1] & sp_pump_id!=sp_pump_id[_n+1] & temp7>temp6[_n+1])
egen temp9 = max(temp8), by(sp_uuid)
replace case = 4 if temp1<temp2 & temp4<temp5 & temp9==1 & apep_proj_count==0 & case==.
tab case apep_proj_count if sp_uuid_tag, missing
drop temp*

	// Define pre/post dummies for SPs with APEP projects
gen apep_post = test_modate>=ym(year(date_proj_finish),month(date_proj_finish))
replace apep_post = . if date_proj_finish==. | apep_proj_count==0
assert inlist(apep_post,0,1) if apep_proj_count!=0 
	// for 3 of the 5 SPs with 2 projects, functionally equivalent to use the first project finish date
egen sp_uuid_post_tag = tag(sp_uuid apep_post) if apep_post!=.
	
	// CASE 1: project, singleton pump/month w/in pre/post periods (could still have multiple tests for 1 pump on 1 date)
egen temp1 = min(sp_pump_id), by(sp_uuid apep_post)
egen temp2 = max(sp_pump_id), by(sp_uuid apep_post)
egen temp3 = group(sp_pump_id test_modate apep_post), missing
egen temp4 = min(temp3), by(sp_uuid apep_post)
egen temp5 = max(temp3), by(sp_uuid apep_post)	
replace case = 1 if temp1==temp2 & temp4==temp5	& apep_proj_count>0 & case==.
tab case apep_proj_count if sp_uuid_tag, missing
tab case apep_post if sp_uuid_post_tag, missing
drop temp*

	// CASE 2: project, single pump + multiple months w/in pre/post periods (could still have multiple tests for 1 pump on 1 date)
egen temp1 = min(sp_pump_id), by(sp_uuid apep_post)
egen temp2 = max(sp_pump_id), by(sp_uuid apep_post)
egen temp3 = group(sp_pump_id test_modate apep_post), missing
egen temp4 = min(temp3), by(sp_uuid apep_post)
egen temp5 = max(temp3), by(sp_uuid apep_post)	
replace case = 2 if temp1==temp2 & temp4<temp5	& apep_proj_count>0 & case==.
tab case apep_proj_count if sp_uuid_tag, missing
tab case apep_post if sp_uuid_post_tag, missing
drop temp*

	// CASE 3: project, multiple pumps + multiple months, non-conflicting date ranges (w/in pre/post periods)
egen temp1 = min(sp_pump_id), by(sp_uuid apep_post)
egen temp2 = max(sp_pump_id), by(sp_uuid apep_post)
egen temp3 = group(sp_pump_id test_modate apep_post), missing
egen temp4 = min(temp3), by(sp_uuid apep_post)
egen temp5 = max(temp3), by(sp_uuid apep_post)	
egen temp6 = min(test_date_stata), by(sp_pump_id apep_post)
egen temp7 = max(test_date_stata), by(sp_pump_id apep_post)
sort sp_uuid test_date_stata sp_pump_id
gen temp8 = (sp_uuid==sp_uuid[_n-1] & sp_pump_id!=sp_pump_id[_n-1] & apep_post==apep_post[_n-1] & temp6<temp7[_n-1]) | ///
	        (sp_uuid==sp_uuid[_n+1] & sp_pump_id!=sp_pump_id[_n+1] & apep_post==apep_post[_n+1] & temp7>temp6[_n+1])
egen temp9 = max(temp8), by(sp_uuid apep_post)
replace case = 3 if temp1<temp2 & temp4<temp5 & temp9==0 & apep_proj_count>0 & case==.
tab case apep_proj_count if sp_uuid_tag, missing
tab case apep_post if sp_uuid_post_tag, missing
drop temp*

	// CASE 4: project, multiple pumps + multiple months, conflicting date ranges (w/in pre/post periods)
egen temp1 = min(sp_pump_id), by(sp_uuid apep_post)
egen temp2 = max(sp_pump_id), by(sp_uuid apep_post)
egen temp3 = group(sp_pump_id test_modate apep_post), missing
egen temp4 = min(temp3), by(sp_uuid apep_post)
egen temp5 = max(temp3), by(sp_uuid apep_post)	
egen temp6 = min(test_date_stata), by(sp_pump_id apep_post)
egen temp7 = max(test_date_stata), by(sp_pump_id apep_post)
sort sp_uuid test_date_stata sp_pump_id
gen temp8 = (sp_uuid==sp_uuid[_n-1] & sp_pump_id!=sp_pump_id[_n-1] & apep_post==apep_post[_n-1] & temp6<temp7[_n-1]) | ///
	        (sp_uuid==sp_uuid[_n+1] & sp_pump_id!=sp_pump_id[_n+1] & apep_post==apep_post[_n+1] & temp7>temp6[_n+1])
egen temp9 = max(temp8), by(sp_uuid apep_post)
replace case = 4 if temp1<temp2 & temp4<temp5 & temp9==1 & apep_proj_count>0 & case==.
tab case apep_proj_count if sp_uuid_tag, missing
tab case apep_post if sp_uuid_post_tag, missing
drop temp*

	// Confirm every pump test has an assigned case!
assert case!=.	

** Flag relevant date ranges for Case 2 SP-pumps
sort sp_pump_id test_date_stata
br sp_uuid sp_pump_id sp_pump_id3 test_date_stata test_modate case if case==2 & apep_proj_count==0
gen test_modate_before = .
replace test_modate_before = test_modate[_n-1] if sp_pump_id==sp_pump_id[_n-1] & test_modate>test_modate[_n-1] ///
	& apep_proj_count==0 & case==2
replace test_modate_before = test_modate[_n-1] if sp_pump_id==sp_pump_id[_n-1] & test_modate>test_modate[_n-1] ///
	& apep_proj_count>0 & case==2 & apep_post==apep_post[_n-1]
gen test_modate_after = .
replace test_modate_after = test_modate[_n+1] if sp_pump_id==sp_pump_id[_n+1] & test_modate<test_modate[_n+1] ///
	& apep_proj_count==0 & case==2
replace test_modate_after = test_modate[_n+1] if sp_pump_id==sp_pump_id[_n+1] & test_modate<test_modate[_n+1] ///
	& apep_proj_count>0 & case==2 & apep_post==apep_post[_n+1]
format %tm test_modate_before test_modate_after
egen temp1 = mode(test_modate_before), by(sp_pump_id test_modate)
egen temp2 = mode(test_modate_after), by(sp_pump_id test_modate)
replace test_modate_before = temp1 if test_modate_before==. & temp1!=. & case==2
replace test_modate_after = temp2 if test_modate_after==. & temp2!=. & case==2
drop temp*

** Flag relevant date ranges for Case 3 SPs
sort sp_uuid test_date_stata
br sp_uuid sp_pump_id sp_pump_id3 test_date_stata test_modate* case if case==3 & apep_proj_count==0
replace test_modate_before = test_modate[_n-1] if sp_uuid==sp_uuid[_n-1] & test_modate>test_modate[_n-1] ///
	& apep_proj_count==0 & case==3
replace test_modate_before = test_modate[_n-1] if sp_uuid==sp_uuid[_n-1] & test_modate>test_modate[_n-1] ///
	& apep_proj_count>0 & case==3 & apep_post==apep_post[_n-1]
replace test_modate_after = test_modate[_n+1] if sp_uuid==sp_uuid[_n+1] & test_modate<test_modate[_n+1] ///
	& apep_proj_count==0 & case==3
replace test_modate_after = test_modate[_n+1] if sp_uuid==sp_uuid[_n+1] & test_modate<test_modate[_n+1] ///
	& apep_proj_count>0 & case==3 & apep_post==apep_post[_n+1]
egen temp1 = mode(test_modate_before), by(sp_pump_id test_modate)
egen temp2 = mode(test_modate_after), by(sp_pump_id test_modate)
replace test_modate_before = temp1 if test_modate_before==. & temp1!=. & case==3
replace test_modate_after = temp2 if test_modate_after==. & temp2!=. & case==3
drop temp*

** Flag relevant date ranges for Case 4 SPs
sort sp_uuid sp_pump_id test_date_stata
br sp_uuid sp_pump_id sp_pump_id3 test_date_stata test_modate* case if case==4 & apep_proj_count==0
replace test_modate_before = test_modate[_n-1] if sp_pump_id==sp_pump_id[_n-1] & test_modate>test_modate[_n-1] ///
	& apep_proj_count==0 & case==4
replace test_modate_before = test_modate[_n-1] if sp_pump_id==sp_pump_id[_n-1] & test_modate>test_modate[_n-1] ///
	& apep_proj_count>0 & case==4 & apep_post==apep_post[_n-1]
replace test_modate_after = test_modate[_n+1] if sp_pump_id==sp_pump_id[_n+1] & test_modate<test_modate[_n+1] ///
	& apep_proj_count==0 & case==4
replace test_modate_after = test_modate[_n+1] if sp_pump_id==sp_pump_id[_n+1] & test_modate<test_modate[_n+1] ///
	& apep_proj_count>0 & case==4 & apep_post==apep_post[_n+1]	
egen temp1 = mode(test_modate_before), by(sp_pump_id test_modate)
egen temp2 = mode(test_modate_after), by(sp_pump_id test_modate)
replace test_modate_before = temp1 if test_modate_before==. & temp1!=. & case==4
replace test_modate_after = temp2 if test_modate_after==. & temp2!=. & case==4
drop temp*

	// One extra step: stop carrying forward/backward an sp_pump_id where SP has another sp_pump_id outside its date range
sort sp_uuid test_date_stata sp_pump_id
egen temp_min = min(test_modate), by(sp_pump_id apep_post)	
egen temp_max = max(test_modate), by(sp_pump_id apep_post)	

egen temp_min1 = min(test_modate), by(sp_uuid apep_post)
egen temp_max1 = max(test_modate), by(sp_uuid apep_post)
forvalues i = 2/7 {
	local i1 = `i'-1
	egen temp1 = min(test_modate) if test_modate>temp_min`i1', by(sp_uuid apep_post)
	egen temp_min`i' = mode(temp1), by(sp_uuid apep_post)
	egen temp2 = max(test_modate) if test_modate<temp_max`i1' & temp_max`i1'!=., by(sp_uuid apep_post)
	egen temp_max`i' = mode(temp2), by(sp_uuid apep_post)
	drop temp1 temp2
}
assert temp_min7==. & temp_max7==. if case==4
drop temp_min7 temp_max7
gen temp_min_rowmax = .
gen temp_max_rowmin = . 
forvalues i = 1/6 {
	replace temp_min_rowmax = temp_min`i' if temp_min`i'<temp_min
	replace temp_max_rowmin = temp_max`i' if temp_max`i'>temp_max & temp_max`i'!=.
} 
replace test_modate_before = temp_min_rowmax if case==4 & test_modate_before==.
replace test_modate_after = temp_max_rowmin if case==4 & test_modate_after==.
	// Assumption we're making: outside of the date range for which we observe a specific SP's pump, 
	// another *other* pumps we observe for that same SP completely supersede the pump in question!s
drop temp*

** Expand to full test-by-month panel, and construct month variable
unique apeptestid_uniq
assert r(unique)==r(N)
expand 117, gen(temp_new)
sort apeptestid_uniq temp_new
gen modate = ym(2008,1) if temp_new==0
replace modate = modate[_n-1]+1 if apeptestid_uniq==apeptestid_uniq[_n-1]
format %tm modate
unique apeptestid_uniq modate
assert r(unique)==r(N)
drop temp_new

** Drop months outside of each pump test's relevant range
unique sp_uuid modate
local uniq = r(unique)
drop if modate<test_modate_before & test_modate_before!=.
drop if modate>test_modate_after
unique sp_uuid modate
assert r(unique)==`uniq'

** Assign interpolation weights for carrying kwh/af forward/backward
gen interp_wgt = .
gen months_until_test = test_modate - modate if test_modate>=modate
gen months_since_test = modate - test_modate if test_modate<=modate

	// Full weight in the month of the test
replace interp_wgt = 1 if modate==test_modate
	
	// Full weight in the months before the SP's first test (no-project SPs)
replace interp_wgt = 1 if modate<test_modate & test_modate_before==. & apep_proj_count==0

	// Full weight in the months after the SP's last test (no-project SPs)
replace interp_wgt = 1 if modate>test_modate & test_modate_after==. & apep_proj_count==0

	// Triangular kernel going backwards to previous test (no-project SPs)
replace interp_wgt = 1 - months_until_test/(test_modate - test_modate_before) if ///
	months_until_test!=. & months_until_test>0 & test_modate_before!=. & apep_proj_count==0 

	// Triangular kernel going forwards to next test (no-project SPs)
replace interp_wgt = 1 - months_since_test/(test_modate_after - test_modate) if ///
	months_since_test!=. & months_since_test>0 & test_modate_after!=. & apep_proj_count==0 

	// Confirm weights are all populated for no-project SPs
assert interp_wgt!=. if apep_proj_count==0

*sort sp_uuid sp_pump_id test_modate modate 
*br sp_uuid sp_pump_id test_modate modate test_modate_before test_modate_after months_until months_since interp_wgt	///
*	if test_modate_before!=. & test_modate_after!=.

	// Indicator for being post APEP project
gen post_apep_proj_finish = modate>=ym(year(date_proj_finish),month(date_proj_finish))	
	
	// Full weight in the months before the SP's first test (project SPs, within pre/post periods)
replace interp_wgt = 1 if modate<test_modate & test_modate_before==. & apep_proj_count>0 & post_apep_proj_finish==apep_post
	
	// Full weight in the months after the SP's last test (project SPs, within pre/post periods)
replace interp_wgt = 1 if modate>test_modate & test_modate_after==. & apep_proj_count>0 & post_apep_proj_finish==apep_post

	// Triangular kernel going backwards to previous test (project SPs, within pre/post periods)
replace interp_wgt = 1 - months_until_test/(test_modate - test_modate_before) if ///
	months_until_test!=. & months_until_test>0 & test_modate_before!=. & apep_proj_count>0 & post_apep_proj_finish==apep_post

	// Triangular kernel going forwards to next test (project SPs, within pre/post periods)
replace interp_wgt = 1 - months_since_test/(test_modate_after - test_modate) if ///
	months_since_test!=. & months_since_test>0 & test_modate_after!=. & apep_proj_count>0 & post_apep_proj_finish==apep_post 

	// Confirm weights are all populated for project SPs within pre/post periods
assert interp_wgt!=. if apep_proj_count>0 & apep_post==post_apep_proj_finish

	// Confirm weights are all missing for project SPs outside pre/post periods 
assert interp_wgt==. if apep_proj_count>0 & apep_post!=post_apep_proj_finish
	
** Drop pre/post period observations for redundant tests in the opposite period
gen temp1 = apep_post==post_apep_proj_finish
egen temp2 = max(temp1), by(sp_uuid post_apep_proj_finish)
unique sp_uuid modate
local uniq = r(unique)
assert temp1==temp2 if apep_proj_count==0
drop if temp1<temp2
unique sp_uuid modate
assert r(unique)==`uniq'
drop temp* 
		
** Indicators for being forced to extrapolate pump specs across pre/post project periods	
gen extrap_post_to_pre = apep_proj_count>0 & apep_post==1 & post_apep_proj_finish==0
gen extrap_pre_to_post = apep_proj_count>0 & apep_post==0 & post_apep_proj_finish==1
egen temp_tag = tag(sp_uuid post_apep_proj_finish)
sum extrap_post_to_pre if apep_proj_count>0 & temp_tag & post_apep_proj_finish==0 // 22% of SPs have no pre-period test
sum extrap_pre_to_post if apep_proj_count>0 & temp_tag & post_apep_proj_finish==1 // 46% of SPs have no post-period test
drop temp*

** Assign (full) interpolation weights in cases where we're extrapolating across pre/post project periods
assert test_modate_before==. if extrap_post_to_pre==1	
assert test_modate_after==. if extrap_pre_to_post==1	
assert interp_wgt==. if extrap_post_to_pre==1 | extrap_pre_to_post==1
replace interp_wgt = 1 if extrap_post_to_pre==1 | extrap_pre_to_post==1
assert interp_wgt!=. & inrange(interp_wgt,0,1)
	// At least for now, we're treating project 1 as the only project for the 5 SPs with 2 project finish dates! 

** For cases where we're extrapolating forwards (pre-to-post), assign post-period specs based on ex ante assumed upgrade
egen temp_tag = tag(sp_uuid) if extrap_pre_to_post==1
br pwl pwl_after totlift tdh tdh_after flow_gpm flow_gpm_after ope ope_after hp hp_after kw_input kw_input_after ///
	af24hrs af24hrs_after afperyr afperyr_after kwhaf kwhaf_after if temp_tag & extrap_pre_to_post==1
	// 1 weird thing: pwl_after is often much bigger than pwl. This should be pump-invariant, based on my 
	// understanding of what pwl is, unless flow is changing.
	// Because we need to predict drawdown below, I'm gonna assume pwl doesn't change with the upgrade
gen temp1 = tdh_after - tdh if temp_tag & extrap_pre_to_post==1
gen temp2 = flow_gpm_after - flow_gpm if temp_tag & extrap_pre_to_post==1
gen temp3 = hp_after - hp if temp_tag & extrap_pre_to_post==1
sum temp?, detail
replace totlift_gap = tdh_after - pwl - dchlvl_ft - gaugecor_ft - gaugeheight_ft - otherlosses_ft if extrap_pre_to_post==1
replace ope = ope_after if extrap_pre_to_post==1 & ope_after!=.
replace hp = hp_after if extrap_pre_to_post==1	
drop temp?	
	
** For cases where we're extrapolating backwards (post-to-pre), assume NOTHING else changed except OPE
egen temp_tag2 = tag(sp_uuid) if extrap_post_to_pre==1
br est_savings_kwh_yr afperyr kwhaf af24hrs kw_input hp totlift flow_gpm ope if temp_tag2 & extrap_post_to_pre==1
gen temp1 = est_savings_kwh_yr / afperyr // this gives us (old - new) kwhaf
gen temp2 = kwhaf + temp1 // this gets us old kwhaf
gen temp3 = temp2 * af24hrs / 24 // this gives us old kw
gen temp4 = temp3 / 0.7457 // this gives us old hp
gen temp5 = totlift * flow_gpm / 39.6 / temp4 // this gives us old OPE
sum ope temp5 if temp_tag2, detail // and these OPEs look mostly reasonable!
	// NOTE: I'm using 39.6 rather than the more precise 39.568345 because 39.6 is the number APEP used
replace ope = temp5 if extrap_post_to_pre==1
drop temp*
	// This is still super hand-wavy, and we'll probably end up dropping these extrapolated values anyways

** Create quarter variable
gen quarter	= .
replace quarter = 1 if inlist(real(substr(string(modate,"%tm"),6,2)),1,2,3)
replace quarter = 2 if inlist(real(substr(string(modate,"%tm"),6,2)),4,5,6)
replace quarter = 3 if inlist(real(substr(string(modate,"%tm"),6,2)),7,8,9)
replace quarter = 4 if inlist(real(substr(string(modate,"%tm"),6,2)),10,11,12)
assert quarter!=.
gen qtr = yq(real(substr(string(modate,"%tm"),1,4)),quarter)
format %tq qtr
assert qtr!=.
drop quarter	
	
** Merge in monthly and quarterly groundwater depths at the SP-level
merge m:1 sp_uuid modate using "$dirpath_data/groundwater/groundwater_depth_sp_month_rast.dta", ///
	keep(1 3) nogen keepusing(gw_*)
merge m:1 sp_uuid qtr using "$dirpath_data/groundwater/groundwater_depth_sp_quarter_rast.dta", ///
	keep(1 3) nogen keepusing(gw_*)
rename gw_* gw_*SP

** Merge in monthly and quarterly groundwater depths at the pump-level
merge m:1 latlon_group modate using "$dirpath_data/groundwater/groundwater_depth_apep_month_rast.dta", ///
	keep(1 3) nogen keepusing(gw_*)
merge m:1 latlon_group qtr using "$dirpath_data/groundwater/groundwater_depth_apep_quarter_rast.dta", ///
	keep(1 3) nogen keepusing(gw_*)
	
** Drop raster values using bilinear inverse distance weighting 
drop gw_rast_depth_???_?bSP gw_rast_depth_???_?b
	// virtually identical to simple IDW; this elimiates a whole bunch of functionally equivalent depths
	
** Standardize names to facilitate looping below
rename gw_mth_bsn_mean?SP gw_mean_depth_mth_?SP
rename gw_mth_bsn_mean? gw_mean_depth_mth_?
rename gw_qtr_bsn_mean?SP gw_mean_depth_qtr_?SP
rename gw_qtr_bsn_mean? gw_mean_depth_qtr_?



***	
** Predict drawdown where missing, and in expanded months, using some physics!
***

gen drwdwn_predict_step = .
gen drwdwn_predict_step_desc = ""
	
** Step 0: Flag observations to estimate drawdown model, and create prediction-specific regressors
sum drwdwn if modate==test_modate, detail // 99.5th pctile is 151
replace flag_bad_drwdwn = 1 if drwdwn==. | drwdwn<0 | drwdwn>151
gen temp_drwdwn_sample = flag_bad_drwdwn==0 & swl!=0 & modate==test_modate
tab temp_drwdwn_sample
gen ln_drwdwn = ln(1+drwdwn) // 5% zeros
gen ln_flow_gpm = ln(flow_gpm) // physics tells us s(Q,C) = Q*C
gen temp_depvar = ln_drwdwn - ln_flow_gpm // to force coeff on ln(Q) to be 1
gen rwl_2 = rwl^2
gen rwl_3 = rwl^3
gen rwl_4 = rwl^4
gen rwl_5 = rwl^5
gen rwl_6 = rwl^6
gen year = real(substr(string(modate,"%tm"),1,4)) 
egen sp_group = group(sp_uuid)


** Step 1 - Predict drawdown: pump location FEs, linear in groundwater depth (where enough repeat observations) 

	// Model A: pump location FEs and slopes in depth 
reghdfe temp_depvar if temp_drwdwn_sample==1, absorb(latlon_group##c.(rwl), savefe) residuals	
gen double TEMPa_cons = _b[_cons]
egen double TEMPa_fe1 = mean(__hdfe1__), by(latlon_group)
egen double TEMPa_slope1 = mean(__hdfe1__Slope1), by(latlon_group)
gen double TEMPa_drwdwn_hat = flow_gpm * exp(TEMPa_cons + TEMPa_fe1 + TEMPa_slope1*rwl) - 1
egen TEMP_count = count(apeptestid_uniq) if temp_drwdwn_sample==1 & TEMPa_drwdwn_hat!=., by(latlon_group)

	// Model B: pump location FEs only
reghdfe temp_depvar if temp_drwdwn_sample==1, absorb(latlon_group, savefe) residuals	
gen double TEMPb_cons = _b[_cons]
egen double TEMPb_fe1 = mean(__hdfe1__), by(latlon_group)
gen double TEMPb_drwdwn_hat = flow_gpm * exp(TEMPb_cons + TEMPb_fe1) - 1

	// Compare Models A vs B
twoway ///
	(scatter TEMPb_drwdwn_hat drwdwn if temp_drwdwn_sample==1 & drwdwn<100 & TEMP_count==3, color(blue) msize(vsmall)) ///
	(scatter TEMPa_drwdwn_hat drwdwn if temp_drwdwn_sample==1 & drwdwn<100 & TEMP_count==3, color(red) msize(vsmall))
	// The first model is overfit with only 2 observations per pump location, because it's loading 
	// basically all changes in drawdown onto chagnes in water level. 
	// With 3+ observations per pump, water level probably  tells us something (crude) about how
	// transmissivity (etc) changes at a specific location when the water level rises/falls
egen TEMP_count2 = mean(TEMP_count), by(latlon_group)	
assert mod(TEMP_count2,1)==0 if TEMP_count2!=. // constant within group
count if TEMP_count2==2

	// Flag step in drawdown populating stepdown
gen TEMP_nonmissing = TEMPb_drwdwn_hat!=. | TEMPa_fe1!=.
replace drwdwn_predict_step = 1 if TEMP_nonmissing==1
replace drwdwn_predict_step_desc = "pump location FEs (& slopes in depth if >2 obs)" if drwdwn_predict_step==1
	
	// Population drawdown for each groundwater depth measurement
foreach v of varlist gw_????_depth_???_* {
	local v2 = subinstr("`v'","_depth_","_DRWDWNhat_",1)
	
	// Populate drwdwn_hat using first model (linear in depth)
	gen double `v2' = flow_gpm * exp(TEMPa_cons + TEMPa_fe1 + TEMPa_slope1*`v') - 1
	
	// Populate drwdwn_hat using second model if only 2 obs (to avoid overfitting)
	replace `v2' = TEMPb_drwdwn_hat if TEMP_count2==2
	
	// De-populate drwdwn_hat if predicted value is above 151 or below -5 (allowing for a small buffer close to zero)
	replace `v2' = . if `v2'>151 | `v2'<-5

	// Ensure drwdwn_hat is non-negative
	replace `v2' = max(`v2',0) if `v2'!=.
}
	
	// Clean up before proceeding
drop TEMP* _*


** Step 2 - Predict drawdown: SP FEs, linear in groundwater depth (where enough repeat observations) 

	// Model A: SP FEs and slopes in depth 
reghdfe temp_depvar if temp_drwdwn_sample==1, absorb(sp_group##c.(rwl), savefe) residuals	
gen double TEMPa_cons = _b[_cons]
egen double TEMPa_fe1 = mean(__hdfe1__), by(sp_group)
egen double TEMPa_slope1 = mean(__hdfe1__Slope1), by(sp_group)
gen double TEMPa_drwdwn_hat = flow_gpm * exp(TEMPa_cons + TEMPa_fe1 + TEMPa_slope1*rwl) - 1
egen TEMP_count = count(apeptestid_uniq) if temp_drwdwn_sample==1 & TEMPa_drwdwn_hat!=., by(sp_group)

	// Model B: SP FEs only
reghdfe temp_depvar if temp_drwdwn_sample==1, absorb(sp_group, savefe) residuals	
gen double TEMPb_cons = _b[_cons]
egen double TEMPb_fe1 = mean(__hdfe1__), by(sp_group)
gen double TEMPb_drwdwn_hat = flow_gpm * exp(TEMPb_cons + TEMPb_fe1) - 1

	// Compare Models A vs B
twoway ///
	(scatter TEMPb_drwdwn_hat drwdwn if temp_drwdwn_sample==1 & drwdwn<100 & TEMP_count==3, color(blue) msize(vsmall)) ///
	(scatter TEMPa_drwdwn_hat drwdwn if temp_drwdwn_sample==1 & drwdwn<100 & TEMP_count==3, color(red) msize(vsmall))
	// The first model is overfit with only 2 observations per SP, because it's loading 
	// basically all changes in drawdown onto chagnes in water level. 
	// With 3+ observations per pump, water level probably  tells us something (crude) about how
	// transmissivity (etc) changes at a specific location when the water level rises/falls
egen TEMP_count2 = mean(TEMP_count), by(sp_group)	
assert mod(TEMP_count2,1)==0 if TEMP_count2!=. // constant within group
count if TEMP_count2==2

	// Create flag always-missings, since this will pick a whole bunch of SP-months with missing GW readings
gen TEMP_allmissing = 1
	
	// Population drawdown for each groundwater depth measurement
foreach v of varlist gw_????_depth_???_* {
	local v2 = subinstr("`v'","_depth_","_DRWDWNhat_",1)
	
	// Calcalate drwdwn_hat using first model (linear in depth)
	gen TEMP = flow_gpm * exp(TEMPa_cons + TEMPa_fe1 + TEMPa_slope1*`v') - 1 
	
	// Recalculate drwdwn_hat using second model if only 2 obs (to avoid overfitting)
	replace TEMP = TEMPb_drwdwn_hat if TEMP_count2==2
		
	// Abort if predicted value is above 151 or below -5 (allowing for a small buffer close to zero)
	replace TEMP = . if TEMP>151 | TEMP<-5

	// Ensure drwdwn_hat is non-negative
	replace TEMP = max(TEMP,0) if TEMP!=.
	
	// Flag always-missings, since this will pick a whole bunch of SP-months with missing GW readings
	replace TEMP_allmissing = 0 if `v2'!=.
	
	// Populate drwdwn_hat where still missing
	replace `v2' = TEMP if `v2'==.
	
	// Drop temp variable
	drop TEMP
}
	
	// Flag step in drawdown populating stepdown
gen TEMP_nonmissing = TEMPb_drwdwn_hat!=. | TEMPa_fe1!=.
replace drwdwn_predict_step = 2 if TEMP_nonmissing==1 & (drwdwn_predict_step==. | TEMP_allmissing==1)
replace drwdwn_predict_step_desc = "SP FEs (& slopes in depth if >2 obs)" if drwdwn_predict_step==2

	// Clean up before proceeding
drop TEMP* _*


** Step 3 - Predict drawdown: wdist-by-year FEs, quadratic in groundwater depth  

	// Water district by year FEs and 2nd order polynomial in depth 
reghdfe temp_depvar if temp_drwdwn_sample==1, absorb(wdist_idSP#year##c.(rwl rwl_2), savefe) residuals	
gen double TEMP_cons = _b[_cons]
egen double TEMP_fe1 = mean(__hdfe1__), by(wdist_idSP year)
egen double TEMP_slope1 = mean(__hdfe1__Slope1), by(wdist_idSP year)
egen double TEMP_slope2 = mean(__hdfe1__Slope2), by(wdist_idSP year)
gen double TEMP_drwdwn_hat = flow_gpm * exp(TEMP_cons + TEMP_fe1 + TEMP_slope1*rwl + TEMP_slope2*rwl_2) - 1

	// Flag over-fit cells (fewer than 4 observations)
egen TEMP_count = count(apeptestid_uniq) if temp_drwdwn_sample==1 & TEMP_drwdwn_hat!=., by(wdist_idSP year)
tab TEMP_count
egen TEMP_count2 = mean(TEMP_count), by(wdist_idSP year)	
assert mod(TEMP_count2,1)==0 if TEMP_count2!=. // constant within group

	// Scatter plot
twoway ///
	(scatter TEMP_drwdwn_hat drwdwn if temp_drwdwn_sample==1 & drwdwn<100 & TEMP_drwdwn_hat<300 & drwdwn_predict_step!=., color(blue) msize(vsmall)) ///
	(scatter TEMP_drwdwn_hat drwdwn if temp_drwdwn_sample==1 & drwdwn<100 & TEMP_drwdwn_hat<300 & drwdwn_predict_step==., color(red) msize(vsmall))
	// shrug emoji; both look like buckshot, but R^2 of model is 0.67, so...?

	// Create flag always-missings, since this will pick a whole bunch of SP-months with missing GW readings
gen TEMP_allmissing = 1
	
	// Population drawdown for each groundwater depth measurement
foreach v of varlist gw_????_depth_???_* {
	local v2 = subinstr("`v'","_depth_","_DRWDWNhat_",1)
	
	// Calcalate drwdwn_hat 
	gen TEMP = flow_gpm * exp(TEMP_cons + TEMP_fe1 + TEMP_slope1*`v' + TEMP_slope2*`v'^2) - 1

	// Eliminate overfit prediction (cells with <4 observations)
	replace TEMP = . if TEMP_count2<4
	
	// Abort if predicted value is above 151 or below -5 (allowing for a small buffer close to zero)
	replace TEMP = . if TEMP>151 | TEMP<-5

	// Ensure drwdwn_hat is non-negative
	replace TEMP = max(TEMP,0) if TEMP!=.
	
	// Flag always-missings, since this will pick a whole bunch of SP-months with missing GW readings
	replace TEMP_allmissing = 0 if `v2'!=.
	
	// Populate drwdwn_hat where still missing
	replace `v2' = TEMP if `v2'==.
	
	// Drop temp variable
	drop TEMP
}
	
	// Flag step in drawdown populating stepdown
gen TEMP_nonmissing = TEMP_drwdwn_hat!=. | TEMP_fe1!=.
replace drwdwn_predict_step = 3 if TEMP_nonmissing==1 & (drwdwn_predict_step==. | TEMP_allmissing==1) & TEMP_count2>=4
replace drwdwn_predict_step_desc = "Water district by year FEs & quadratic in depth" if drwdwn_predict_step==3

	// Clean up before proceeding
drop TEMP* _*


** Step 4 - Predict drawdown: wdist FEs, quadratic in groundwater depth  

	// Water district FEs and 2nd order polynomial in depth 
reghdfe temp_depvar if temp_drwdwn_sample==1, absorb(wdist_idSP##c.(rwl rwl_2), savefe) residuals	
gen double TEMP_cons = _b[_cons]
egen double TEMP_fe1 = mean(__hdfe1__), by(wdist_idSP)
egen double TEMP_slope1 = mean(__hdfe1__Slope1), by(wdist_idSP)
egen double TEMP_slope2 = mean(__hdfe1__Slope2), by(wdist_idSP)
gen double TEMP_drwdwn_hat = flow_gpm * exp(TEMP_cons + TEMP_fe1 + TEMP_slope1*rwl + TEMP_slope2*rwl_2) - 1

	// Flag over-fit cells (fewer than 4 observations)
egen TEMP_count = count(apeptestid_uniq) if temp_drwdwn_sample==1 & TEMP_drwdwn_hat!=., by(wdist_idSP)
tab TEMP_count
egen TEMP_count2 = mean(TEMP_count), by(wdist_idSP)	
assert mod(TEMP_count2,1)==0 if TEMP_count2!=. // constant within group

	// Scatter plot
twoway ///
	(scatter TEMP_drwdwn_hat drwdwn if temp_drwdwn_sample==1 & drwdwn<100 & TEMP_drwdwn_hat<300 & drwdwn_predict_step!=., color(blue) msize(vsmall)) ///
	(scatter TEMP_drwdwn_hat drwdwn if temp_drwdwn_sample==1 & drwdwn<100 & TEMP_drwdwn_hat<300 & drwdwn_predict_step==., color(red) msize(vsmall))
	// shrug emoji; both look like buckshot, but R^2 of model is 0.59, so...?

	// Create flag always-missings, since this will pick a whole bunch of SP-months with missing GW readings
gen TEMP_allmissing = 1
	
	// Population drawdown for each groundwater depth measurement
foreach v of varlist gw_????_depth_???_* {
	local v2 = subinstr("`v'","_depth_","_DRWDWNhat_",1)
	
	// Calcalate drwdwn_hat 
	gen TEMP = flow_gpm * exp(TEMP_cons + TEMP_fe1 + TEMP_slope1*`v' + TEMP_slope2*`v'^2) - 1

	// Eliminate overfit prediction (cells with <4 observations)
	replace TEMP = . if TEMP_count2<4
	
	// Abort if predicted value is above 151 or below -5 (allowing for a small buffer close to zero)
	replace TEMP = . if TEMP>151 | TEMP<-5

	// Ensure drwdwn_hat is non-negative
	replace TEMP = max(TEMP,0) if TEMP!=.
	
	// Flag always-missings, since this will pick a whole bunch of SP-months with missing GW readings
	replace TEMP_allmissing = 0 if `v2'!=.
	
	// Populate drwdwn_hat where still missing
	replace `v2' = TEMP if `v2'==.
	
	// Drop temp variable
	drop TEMP
}
	
	// Flag step in drawdown populating stepdown
gen TEMP_nonmissing = TEMP_drwdwn_hat!=. | TEMP_fe1!=.
replace drwdwn_predict_step = 4 if TEMP_nonmissing==1 & (drwdwn_predict_step==. | TEMP_allmissing==1) & TEMP_count2>=4
replace drwdwn_predict_step_desc = "Water district FEs & quadratic in depth" if drwdwn_predict_step==4

	// Clean up before proceeding
drop TEMP* _*


** Step 5 - Predict drawdown: subbasin-by-year FEs, quadratic in groundwater depth  

	// Sub-basin by year FEs and 2nd order polynomial in depth 
reghdfe temp_depvar if temp_drwdwn_sample==1, absorb(basin_sub_idSP_group#year##c.(rwl rwl_2), savefe) residuals	
gen double TEMP_cons = _b[_cons]
egen double TEMP_fe1 = mean(__hdfe1__), by(basin_sub_idSP_group year)
egen double TEMP_slope1 = mean(__hdfe1__Slope1), by(basin_sub_idSP_group year)
egen double TEMP_slope2 = mean(__hdfe1__Slope2), by(basin_sub_idSP_group year)
gen double TEMP_drwdwn_hat = flow_gpm * exp(TEMP_cons + TEMP_fe1 + TEMP_slope1*rwl + TEMP_slope2*rwl_2) - 1

	// Flag over-fit cells (fewer than 4 observations)
egen TEMP_count = count(apeptestid_uniq) if temp_drwdwn_sample==1 & TEMP_drwdwn_hat!=., by(basin_sub_idSP_group year)
tab TEMP_count
egen TEMP_count2 = mean(TEMP_count), by(basin_sub_idSP_group year)	
assert mod(TEMP_count2,1)==0 if TEMP_count2!=. // constant within group

	// Scatter plot
twoway ///
	(scatter TEMP_drwdwn_hat drwdwn if temp_drwdwn_sample==1 & drwdwn<100 & TEMP_drwdwn_hat<300 & drwdwn_predict_step!=., color(blue) msize(vsmall)) ///
	(scatter TEMP_drwdwn_hat drwdwn if temp_drwdwn_sample==1 & drwdwn<100 & TEMP_drwdwn_hat<300 & drwdwn_predict_step==., color(red) msize(vsmall))
	// shrug emoji; both look like buckshot, but R^2 of model is 0.62, so...?

	// Create flag always-missings, since this will pick a whole bunch of SP-months with missing GW readings
gen TEMP_allmissing = 1
	
	// Population drawdown for each groundwater depth measurement
foreach v of varlist gw_????_depth_???_* {
	local v2 = subinstr("`v'","_depth_","_DRWDWNhat_",1)
	
	// Calcalate drwdwn_hat 
	gen TEMP = flow_gpm * exp(TEMP_cons + TEMP_fe1 + TEMP_slope1*`v' + TEMP_slope2*`v'^2) - 1

	// Eliminate overfit prediction (cells with <4 observations)
	replace TEMP = . if TEMP_count2<4
	
	// Abort if predicted value is above 151 or below -5 (allowing for a small buffer close to zero)
	replace TEMP = . if TEMP>151 | TEMP<-5

	// Ensure drwdwn_hat is non-negative
	replace TEMP = max(TEMP,0) if TEMP!=.
	
	// Flag always-missings, since this will pick a whole bunch of SP-months with missing GW readings
	replace TEMP_allmissing = 0 if `v2'!=.
	
	// Populate drwdwn_hat where still missing
	replace `v2' = TEMP if `v2'==.
	
	// Drop temp variable
	drop TEMP
}
	
	// Flag step in drawdown populating stepdown
gen TEMP_nonmissing = TEMP_drwdwn_hat!=. | TEMP_fe1!=.
replace drwdwn_predict_step = 5 if TEMP_nonmissing==1 & (drwdwn_predict_step==. | TEMP_allmissing==1) & TEMP_count2>=4
replace drwdwn_predict_step_desc = "Sub-basin by year FEs & quadratic in depth" if drwdwn_predict_step==5
	
	// Clean up before proceeding
drop TEMP* _*


** Step 6 - Predict drawdown: subbasin FEs, quadratic in groundwater depth  

	// Sub-basin FEs and 2nd order polynomial in depth 
reghdfe temp_depvar if temp_drwdwn_sample==1, absorb(basin_sub_idSP_group##c.(rwl rwl_2), savefe) residuals	
gen double TEMP_cons = _b[_cons]
egen double TEMP_fe1 = mean(__hdfe1__), by(basin_sub_idSP_group)
egen double TEMP_slope1 = mean(__hdfe1__Slope1), by(basin_sub_idSP_group)
egen double TEMP_slope2 = mean(__hdfe1__Slope2), by(basin_sub_idSP_group)
gen double TEMP_drwdwn_hat = flow_gpm * exp(TEMP_cons + TEMP_fe1 + TEMP_slope1*rwl + TEMP_slope2*rwl_2) - 1

	// Flag over-fit cells (fewer than 4 observations)
egen TEMP_count = count(apeptestid_uniq) if temp_drwdwn_sample==1 & TEMP_drwdwn_hat!=., by(basin_sub_idSP_group)
tab TEMP_count
egen TEMP_count2 = mean(TEMP_count), by(basin_sub_idSP_group)	
assert mod(TEMP_count2,1)==0 if TEMP_count2!=. // constant within group

	// Scatter plot
twoway ///
	(scatter TEMP_drwdwn_hat drwdwn if temp_drwdwn_sample==1 & drwdwn<100 & TEMP_drwdwn_hat<300 & drwdwn_predict_step!=., color(blue) msize(vsmall)) ///
	(scatter TEMP_drwdwn_hat drwdwn if temp_drwdwn_sample==1 & drwdwn<100 & TEMP_drwdwn_hat<300 & drwdwn_predict_step==., color(red) msize(vsmall))
	// shrug emoji; both look like buckshot, but R^2 of model is 0.59, so...?

	// Create flag always-missings, since this will pick a whole bunch of SP-months with missing GW readings
gen TEMP_allmissing = 1
	
	// Population drawdown for each groundwater depth measurement
foreach v of varlist gw_????_depth_???_* {
	local v2 = subinstr("`v'","_depth_","_DRWDWNhat_",1)
	
	// Calcalate drwdwn_hat
	gen TEMP = flow_gpm * exp(TEMP_cons + TEMP_fe1 + TEMP_slope1*`v' + TEMP_slope2*`v'^2) - 1

	// Eliminate overfit prediction (cells with <4 observations)
	replace TEMP = . if TEMP_count2<4
	
	// Abort if predicted value is above 151 or below -5 (allowing for a small buffer close to zero)
	replace TEMP = . if TEMP>151 | TEMP<-5

	// Ensure drwdwn_hat is non-negative
	replace TEMP = max(TEMP,0) if TEMP!=.
	
	// Flag always-missings, since this will pick a whole bunch of SP-months with missing GW readings
	replace TEMP_allmissing = 0 if `v2'!=.
	
	// Populate drwdwn_hat where still missing
	replace `v2' = TEMP if `v2'==.
	
	// Drop temp variable
	drop TEMP
}
	
	// Flag step in drawdown populating stepdown
gen TEMP_nonmissing = TEMP_drwdwn_hat!=. | TEMP_fe1!=.
replace drwdwn_predict_step = 6 if TEMP_nonmissing==1 & (drwdwn_predict_step==. | TEMP_allmissing==1) & TEMP_count2>=4
replace drwdwn_predict_step_desc = "Sub-basin FEs & quadratic in depth" if drwdwn_predict_step==6
	
	// Clean up before proceeding
drop TEMP* _*


** Step 7 - Predict drawdown: subbasin-by-year FEs, quadratic in groundwater depth (sub-basin as assigned at the pump location)

	// Sub-basin by year FEs and 2nd order polynomial in depth 
reghdfe temp_depvar if temp_drwdwn_sample==1, absorb(basin_sub_id_group#year##c.(rwl rwl_2), savefe) residuals	
gen double TEMP_cons = _b[_cons]
egen double TEMP_fe1 = mean(__hdfe1__), by(basin_sub_id_group year)
egen double TEMP_slope1 = mean(__hdfe1__Slope1), by(basin_sub_id_group year)
egen double TEMP_slope2 = mean(__hdfe1__Slope2), by(basin_sub_id_group year)
gen double TEMP_drwdwn_hat = flow_gpm * exp(TEMP_cons + TEMP_fe1 + TEMP_slope1*rwl + TEMP_slope2*rwl_2) - 1

	// Flag over-fit cells (fewer than 4 observations)
egen TEMP_count = count(apeptestid_uniq) if temp_drwdwn_sample==1 & TEMP_drwdwn_hat!=., by(basin_sub_id_group year)
tab TEMP_count
egen TEMP_count2 = mean(TEMP_count), by(basin_sub_id_group year)	
assert mod(TEMP_count2,1)==0 if TEMP_count2!=. // constant within group

	// Scatter plot
twoway ///
	(scatter TEMP_drwdwn_hat drwdwn if temp_drwdwn_sample==1 & drwdwn<100 & TEMP_drwdwn_hat<300 & drwdwn_predict_step!=., color(blue) msize(vsmall)) ///
	(scatter TEMP_drwdwn_hat drwdwn if temp_drwdwn_sample==1 & drwdwn<100 & TEMP_drwdwn_hat<300 & drwdwn_predict_step==., color(red) msize(vsmall))
	// shrug emoji; both look like buckshot, but R^2 of model is 0.62, so...?

	// Create flag always-missings, since this will pick a whole bunch of SP-months with missing GW readings
gen TEMP_allmissing = 1

	// Population drawdown for each groundwater depth measurement
foreach v of varlist gw_????_depth_???_* {
	local v2 = subinstr("`v'","_depth_","_DRWDWNhat_",1)
	
	// Calcalate drwdwn_hat 
	gen TEMP = flow_gpm * exp(TEMP_cons + TEMP_fe1 + TEMP_slope1*`v' + TEMP_slope2*`v'^2) - 1

	// Eliminate overfit prediction (cells with <4 observations)
	replace TEMP = . if TEMP_count2<4
	
	// Abort if predicted value is above 151 or below -5 (allowing for a small buffer close to zero)
	replace TEMP = . if TEMP>151 | TEMP<-5

	// Ensure drwdwn_hat is non-negative
	replace TEMP = max(TEMP,0) if TEMP!=.
	
	// Flag always-missings, since this will pick a whole bunch of SP-months with missing GW readings
	replace TEMP_allmissing = 0 if `v2'!=.
	
	// Populate drwdwn_hat where still missing
	replace `v2' = TEMP if `v2'==.
	
	// Drop temp variable
	drop TEMP
}
	
	// Flag step in drawdown populating stepdown
gen TEMP_nonmissing = TEMP_drwdwn_hat!=. | TEMP_fe1!=.
replace drwdwn_predict_step = 7 if TEMP_nonmissing==1 & (drwdwn_predict_step==. | TEMP_allmissing==1) & TEMP_count2>=4
replace drwdwn_predict_step_desc = "Sub-basin (at pump) by year FEs & quadratic in depth" if drwdwn_predict_step==7
	
	// Clean up before proceeding
drop TEMP* _*


** Step 8 - Predict drawdown: subbasin FEs, quadratic in groundwater depth (sub-basin as assigned at the pump location)

	// Sub-basin FEs and 2nd order polynomial in depth 
reghdfe temp_depvar if temp_drwdwn_sample==1, absorb(basin_sub_id_group##c.(rwl rwl_2), savefe) residuals	
gen double TEMP_cons = _b[_cons]
egen double TEMP_fe1 = mean(__hdfe1__), by(basin_sub_id_group)
egen double TEMP_slope1 = mean(__hdfe1__Slope1), by(basin_sub_id_group)
egen double TEMP_slope2 = mean(__hdfe1__Slope2), by(basin_sub_id_group)
gen double TEMP_drwdwn_hat = flow_gpm * exp(TEMP_cons + TEMP_fe1 + TEMP_slope1*rwl + TEMP_slope2*rwl_2) - 1

	// Flag over-fit cells (fewer than 4 observations)
egen TEMP_count = count(apeptestid_uniq) if temp_drwdwn_sample==1 & TEMP_drwdwn_hat!=., by(basin_sub_id_group)
tab TEMP_count
egen TEMP_count2 = mean(TEMP_count), by(basin_sub_id_group)	
assert mod(TEMP_count2,1)==0 if TEMP_count2!=. // constant within group

	// Scatter plot
twoway ///
	(scatter TEMP_drwdwn_hat drwdwn if temp_drwdwn_sample==1 & drwdwn<100 & TEMP_drwdwn_hat<300 & drwdwn_predict_step!=., color(blue) msize(vsmall)) ///
	(scatter TEMP_drwdwn_hat drwdwn if temp_drwdwn_sample==1 & drwdwn<100 & TEMP_drwdwn_hat<300 & drwdwn_predict_step==., color(red) msize(vsmall))
	// shrug emoji; both look like buckshot, but R^2 of model is 0.62, so...?

	// Create flag always-missings, since this will pick a whole bunch of SP-months with missing GW readings
gen TEMP_allmissing = 1

	// Population drawdown for each groundwater depth measurement
foreach v of varlist gw_????_depth_???_* {
	local v2 = subinstr("`v'","_depth_","_DRWDWNhat_",1)
	
	// Calcalate drwdwn_hat 
	gen TEMP = flow_gpm * exp(TEMP_cons + TEMP_fe1 + TEMP_slope1*`v' + TEMP_slope2*`v'^2) - 1

	// Eliminate overfit prediction (cells with <4 observations)
	replace TEMP = . if TEMP_count2<4
	
	// Abort if predicted value is above 151 or below -5 (allowing for a small buffer close to zero)
	replace TEMP = . if TEMP>151 | TEMP<-5

	// Ensure drwdwn_hat is non-negative
	replace TEMP = max(TEMP,0) if TEMP!=.
	
	// Flag always-missings, since this will pick a whole bunch of SP-months with missing GW readings
	replace TEMP_allmissing = 0 if `v2'!=.
	
	// Populate drwdwn_hat where still missing
	replace `v2' = TEMP if `v2'==.
	
	// Drop temp variable
	drop TEMP
}
	
	// Flag step in drawdown populating stepdown
gen TEMP_nonmissing = TEMP_drwdwn_hat!=. | TEMP_fe1!=.
replace drwdwn_predict_step = 8 if TEMP_nonmissing==1 & (drwdwn_predict_step==. | TEMP_allmissing==1) & TEMP_count2>=4
replace drwdwn_predict_step_desc = "Sub-basin (at pump) FEs & quadratic in depth" if drwdwn_predict_step==8
	
	// Clean up before proceeding
drop TEMP* _*


** Some diagnostics on remaining missings
br sp_uuid customertype farmtype waterenduse bad_geocode_flagSP missing_geocode_flagSP basin_sub_idSP pumplatnew pumplongnew ///
	pge_badge_nbr latlon_group bad_geocode_flag test_modate case temp_drwdwn_sample if drwdwn_predict_step==.
count if drwdwn_predict_step==.
count if drwdwn_predict_step==. & bad_geocode_flag==0 & bad_geocode_flagSP==0
count if drwdwn_predict_step==. & bad_geocode_flag==0 & bad_geocode_flagSP==0 & missing_geocode_flagSP==0
count if drwdwn_predict_step==. & bad_geocode_flag==0 & bad_geocode_flagSP==0 & missing_geocode_flagSP==0 & swl!=.	
br sp_uuid customertype farmtype waterenduse bad_geocode_flagSP missing_geocode_flagSP basin_sub_idSP pumplatnew pumplongnew ///
	pge_badge_nbr latlon_group bad_geocode_flag test_modate case temp_drwdwn_sample if drwdwn_predict_step==. & bad_geocode_flag==0 & ///
	bad_geocode_flagSP==0 & missing_geocode_flagSP==0 & swl!=.
	// most of remaining missings have missing SWL, but some don't


** Step 9 - Predict drawdown: simple within-pump-location average drawdown

	// Pump location means only 
egen double TEMP_fe0 = mean(temp_depvar) if temp_drwdwn_sample==1, by(latlon_group)
egen double TEMP_fe1 = mean(TEMP_fe0), by(latlon_group)
gen double TEMP_drwdwn_hat = flow_gpm * exp(TEMP_fe1) - 1	

	// Create flag always-missings, since this will pick a whole bunch of SP-months with missing GW readings
gen TEMP_allmissing = 1

	// Population drawdown for each groundwater depth measurement
foreach v of varlist gw_????_depth_???_* {
	local v2 = subinstr("`v'","_depth_","_DRWDWNhat_",1)
	
	// Calcalate drwdwn_hat 
	gen TEMP = flow_gpm * exp(TEMP_fe1) - 1
	
	// Abort if predicted value is above 151 or below -5 (allowing for a small buffer close to zero)
	replace TEMP = . if TEMP>151 | TEMP<-5

	// Ensure drwdwn_hat is non-negative
	replace TEMP = max(TEMP,0) if TEMP!=.
	
	// Flag always-missings, since this will pick a whole bunch of SP-months with missing GW readings
	replace TEMP_allmissing = 0 if `v2'!=.
	
	// Populate drwdwn_hat where still missing
	replace `v2' = TEMP if `v2'==.
	
	// Drop temp variable
	drop TEMP
}

	// Flag step in drawdown populating stepdown
gen TEMP_nonmissing = TEMP_drwdwn_hat!=. | TEMP_fe1!=.
replace drwdwn_predict_step = 9 if TEMP_nonmissing==1 & (drwdwn_predict_step==. | TEMP_allmissing==1)
replace drwdwn_predict_step_desc = "Simple within-pump-location average" if drwdwn_predict_step==9
	
	// Clean up before proceeding
drop TEMP* 

	
** Step 10 - Predict drawdown: simple within-SP average drawdown

	// SP means only 
egen double TEMP_fe0 = mean(temp_depvar) if temp_drwdwn_sample==1, by(sp_group)
egen double TEMP_fe1 = mean(TEMP_fe0), by(sp_group)
gen double TEMP_drwdwn_hat = flow_gpm * exp(TEMP_fe1) - 1	

	// Create flag always-missings, since this will pick a whole bunch of SP-months with missing GW readings
gen TEMP_allmissing = 1

	// Population drawdown for each groundwater depth measurement
foreach v of varlist gw_????_depth_???_* {
	local v2 = subinstr("`v'","_depth_","_DRWDWNhat_",1)
	
	// Calcalate drwdwn_hat 
	gen TEMP = flow_gpm * exp(TEMP_fe1) - 1
	
	// Abort if predicted value is above 151 or below -5 (allowing for a small buffer close to zero)
	replace TEMP = . if TEMP>151 | TEMP<-5

	// Ensure drwdwn_hat is non-negative
	replace TEMP = max(TEMP,0) if TEMP!=.
	
	// Flag always-missings, since this will pick a whole bunch of SP-months with missing GW readings
	replace TEMP_allmissing = 0 if `v2'!=.
	
	// Populate drwdwn_hat where still missing
	replace `v2' = TEMP if `v2'==.
	
	// Drop temp variable
	drop TEMP
}

	// Flag step in drawdown populating stepdown
gen TEMP_nonmissing = TEMP_drwdwn_hat!=. | TEMP_fe1!=.
replace drwdwn_predict_step = 10 if TEMP_nonmissing==1 & (drwdwn_predict_step==. | TEMP_allmissing==1)
replace drwdwn_predict_step_desc = "Simple within-SP average" if drwdwn_predict_step==10
	
	// Clean up before proceeding
drop TEMP* 


** Some diagnostics on remaining missings
br sp_uuid customertype farmtype waterenduse bad_geocode_flagSP missing_geocode_flagSP basin_sub_idSP pumplatnew pumplongnew ///
	pge_badge_nbr latlon_group bad_geocode_flag test_modate case temp_drwdwn_sample if drwdwn_predict_step==.
unique sp_uuid
unique sp_uuid if drwdwn_predict_step==.
egen TEMP = min(drwdwn_predict_step==.), by(sp_uuid)
unique sp_uuid if TEMP // all but 33 SPs have SOME drawdown assigned, and all missings are always missing
drop TEMP

** Step 11 - Predict drawdown: simple within water district by year averages (including outside drawdown sample)

	// Water district by year means only 
egen double TEMP_fe0 = mean(temp_depvar) if temp_drwdwn_sample==1, by(wdist_idSP year)
egen double TEMP_fe1 = mean(TEMP_fe0), by(wdist_idSP year)
gen double TEMP_drwdwn_hat = flow_gpm * exp(TEMP_fe1) - 1	

	// Create flag always-missings, since this will pick a whole bunch of SP-months with missing GW readings
gen TEMP_allmissing = 1

	// Population drawdown for each groundwater depth measurement
foreach v of varlist gw_????_depth_???_* {
	local v2 = subinstr("`v'","_depth_","_DRWDWNhat_",1)
	
	// Calcalate drwdwn_hat 
	gen TEMP = flow_gpm * exp(TEMP_fe1) - 1
	
	// Abort if predicted value is above 151 or below -5 (allowing for a small buffer close to zero)
	replace TEMP = . if TEMP>151 | TEMP<-5

	// Ensure drwdwn_hat is non-negative
	replace TEMP = max(TEMP,0) if TEMP!=.
	
	// Flag always-missings, since this will pick a whole bunch of SP-months with missing GW readings
	replace TEMP_allmissing = 0 if `v2'!=.
	
	// Populate drwdwn_hat where still missing
	replace `v2' = TEMP if `v2'==.
	
	// Drop temp variable
	drop TEMP
}

	// Flag step in drawdown populating stepdown
gen TEMP_nonmissing = TEMP_drwdwn_hat!=. | TEMP_fe1!=.
replace drwdwn_predict_step = 11 if TEMP_nonmissing==1 & (drwdwn_predict_step==. | TEMP_allmissing==1)
replace drwdwn_predict_step_desc = "Simple within-wdist/year average" if drwdwn_predict_step==11
	
	// Clean up before proceeding
drop TEMP* 


** Step 12 - Predict drawdown: simple within water district averages (including outside drawdown sample)

	// Water district means only 
egen double TEMP_fe0 = mean(temp_depvar) if temp_drwdwn_sample==1, by(wdist_idSP)
egen double TEMP_fe1 = mean(TEMP_fe0), by(wdist_idSP)
gen double TEMP_drwdwn_hat = flow_gpm * exp(TEMP_fe1) - 1	

	// Create flag always-missings, since this will pick a whole bunch of SP-months with missing GW readings
gen TEMP_allmissing = 1

	// Population drawdown for each groundwater depth measurement
foreach v of varlist gw_????_depth_???_* {
	local v2 = subinstr("`v'","_depth_","_DRWDWNhat_",1)
	
	// Calcalate drwdwn_hat 
	gen TEMP = flow_gpm * exp(TEMP_fe1) - 1
	
	// Abort if predicted value is above 151 or below -5 (allowing for a small buffer close to zero)
	replace TEMP = . if TEMP>151 | TEMP<-5

	// Ensure drwdwn_hat is non-negative
	replace TEMP = max(TEMP,0) if TEMP!=.
	
	// Flag always-missings, since this will pick a whole bunch of SP-months with missing GW readings
	replace TEMP_allmissing = 0 if `v2'!=.
	
	// Populate drwdwn_hat where still missing
	replace `v2' = TEMP if `v2'==.
	
	// Drop temp variable
	drop TEMP
}

	// Flag step in drawdown populating stepdown
gen TEMP_nonmissing = TEMP_drwdwn_hat!=. | TEMP_fe1!=.
replace drwdwn_predict_step = 12 if TEMP_nonmissing==1 & (drwdwn_predict_step==. | TEMP_allmissing==1)
replace drwdwn_predict_step_desc = "Simple within-wdist average" if drwdwn_predict_step==12
	
	// Clean up before proceeding
drop TEMP* 


** Step 13 - Predict drawdown: simple within sub-basin district by year averages (including outside drawdown sample)

	// Sub-basin by year means only 
egen double TEMP_fe0 = mean(temp_depvar) if temp_drwdwn_sample==1, by(basin_sub_idSP_group year)
egen double TEMP_fe1 = mean(TEMP_fe0), by(basin_sub_idSP_group year)
gen double TEMP_drwdwn_hat = flow_gpm * exp(TEMP_fe1) - 1	

	// Create flag always-missings, since this will pick a whole bunch of SP-months with missing GW readings
gen TEMP_allmissing = 1

	// Population drawdown for each groundwater depth measurement
foreach v of varlist gw_????_depth_???_* {
	local v2 = subinstr("`v'","_depth_","_DRWDWNhat_",1)
	
	// Calcalate drwdwn_hat 
	gen TEMP = flow_gpm * exp(TEMP_fe1) - 1
	
	// Abort if predicted value is above 151 or below -5 (allowing for a small buffer close to zero)
	replace TEMP = . if TEMP>151 | TEMP<-5

	// Ensure drwdwn_hat is non-negative
	replace TEMP = max(TEMP,0) if TEMP!=.
	
	// Flag always-missings, since this will pick a whole bunch of SP-months with missing GW readings
	replace TEMP_allmissing = 0 if `v2'!=.
	
	// Populate drwdwn_hat where still missing
	replace `v2' = TEMP if `v2'==.
	
	// Drop temp variable
	drop TEMP
}

	// Flag step in drawdown populating stepdown
gen TEMP_nonmissing = TEMP_drwdwn_hat!=. | TEMP_fe1!=.
replace drwdwn_predict_step = 13 if TEMP_nonmissing==1 & (drwdwn_predict_step==. | TEMP_allmissing==1)
replace drwdwn_predict_step_desc = "Simple within-subbasin/year average" if drwdwn_predict_step==13
	
	// Clean up before proceeding
drop TEMP* 


** Step 14 - Predict drawdown: simple within water subbasin averages (including outside drawdown sample)

	// Sub-basin means only 
egen double TEMP_fe0 = mean(temp_depvar) if temp_drwdwn_sample==1, by(basin_sub_idSP_group)
egen double TEMP_fe1 = mean(TEMP_fe0), by(basin_sub_idSP_group)
gen double TEMP_drwdwn_hat = flow_gpm * exp(TEMP_fe1) - 1	

	// Create flag always-missings, since this will pick a whole bunch of SP-months with missing GW readings
gen TEMP_allmissing = 1

	// Population drawdown for each groundwater depth measurement
foreach v of varlist gw_????_depth_???_* {
	local v2 = subinstr("`v'","_depth_","_DRWDWNhat_",1)
	
	// Calcalate drwdwn_hat 
	gen TEMP = flow_gpm * exp(TEMP_fe1) - 1
	
	// Abort if predicted value is above 151 or below -5 (allowing for a small buffer close to zero)
	replace TEMP = . if TEMP>151 | TEMP<-5

	// Ensure drwdwn_hat is non-negative
	replace TEMP = max(TEMP,0) if TEMP!=.
	
	// Flag always-missings, since this will pick a whole bunch of SP-months with missing GW readings
	replace TEMP_allmissing = 0 if `v2'!=.
	
	// Populate drwdwn_hat where still missing
	replace `v2' = TEMP if `v2'==.
	
	// Drop temp variable
	drop TEMP
}

	// Flag step in drawdown populating stepdown
gen TEMP_nonmissing = TEMP_drwdwn_hat!=. | TEMP_fe1!=.
replace drwdwn_predict_step = 14 if TEMP_nonmissing==1 & (drwdwn_predict_step==. | TEMP_allmissing==1)
replace drwdwn_predict_step_desc = "Simple within-subbasin average" if drwdwn_predict_step==14
	
	// Clean up before proceeding
drop TEMP* 


** Step 14b - Predict drawdown: simple within water subbasin averages (including outside drawdown sample; subbasin as assign at pump location)

	// Sub-basin means only 
egen double TEMP_fe0 = mean(temp_depvar) if temp_drwdwn_sample==1, by(basin_sub_id_group)
egen double TEMP_fe1 = mean(TEMP_fe0), by(basin_sub_id_group)
gen double TEMP_drwdwn_hat = flow_gpm * exp(TEMP_fe1) - 1	

	// Create flag always-missings, since this will pick a whole bunch of SP-months with missing GW readings
gen TEMP_allmissing = 1

	// Population drawdown for each groundwater depth measurement
foreach v of varlist gw_????_depth_???_* {
	local v2 = subinstr("`v'","_depth_","_DRWDWNhat_",1)
	
	// Calcalate drwdwn_hat 
	gen TEMP = flow_gpm * exp(TEMP_fe1) - 1
	
	// Abort if predicted value is above 151 or below -5 (allowing for a small buffer close to zero)
	replace TEMP = . if TEMP>151 | TEMP<-5

	// Ensure drwdwn_hat is non-negative
	replace TEMP = max(TEMP,0) if TEMP!=.
	
	// Flag always-missings, since this will pick a whole bunch of SP-months with missing GW readings
	replace TEMP_allmissing = 0 if `v2'!=.
	
	// Populate drwdwn_hat where still missing
	replace `v2' = TEMP if `v2'==.
	
	// Drop temp variable
	drop TEMP
}

	// Flag step in drawdown populating stepdown
gen TEMP_nonmissing = TEMP_drwdwn_hat!=. | TEMP_fe1!=.
replace drwdwn_predict_step = 14 if TEMP_nonmissing==1 & (drwdwn_predict_step==. | TEMP_allmissing==1)
replace drwdwn_predict_step_desc = "Simple within-subbasin average" if drwdwn_predict_step==14
	
	// Clean up before proceeding
drop TEMP*


** Step 15 - Predict drawdown: simple within year averages (for a few remaining SP, extrapolating backwards from 2011)

	// Year-specific means only 
gen TEMP_year = max(year,2011)
egen double TEMP_fe0 = mean(temp_depvar) if temp_drwdwn_sample==1, by(TEMP_year)
egen double TEMP_fe1 = mean(TEMP_fe0), by(TEMP_year)
gen double TEMP_drwdwn_hat = flow_gpm * exp(TEMP_fe1) - 1	

	// Create flag always-missings, since this will pick a whole bunch of SP-months with missing GW readings
gen TEMP_allmissing = 1

	// Population drawdown for each groundwater depth measurement
foreach v of varlist gw_????_depth_???_* {
	local v2 = subinstr("`v'","_depth_","_DRWDWNhat_",1)
	
	// Calcalate drwdwn_hat 
	gen TEMP = flow_gpm * exp(TEMP_fe1) - 1
	
	// Ensure drwdwn_hat is non-negative
	replace TEMP = max(TEMP,0) if TEMP!=.
	
	// Ensure drwdwn_hat is less than 151 (99th pctile)
	replace TEMP = min(TEMP,151) if TEMP!=.
	
	// If still missing, replace with median drawdoen of 20
	replace TEMP = 20 if TEMP==.

	// Flag always-missings, since this will pick a whole bunch of SP-months with missing GW readings
	replace TEMP_allmissing = 0 if `v2'!=.
	
	// Populate drwdwn_hat where still missing
	replace `v2' = TEMP if `v2'==.
	
	// Drop temp variable
	drop TEMP
}

	// Flag step in drawdown populating stepdown
gen TEMP_nonmissing = TEMP_drwdwn_hat!=. | TEMP_fe1!=.
replace drwdwn_predict_step = 15 if TEMP_nonmissing==1 & (drwdwn_predict_step==. | TEMP_allmissing==1)
replace drwdwn_predict_step_desc = "Simple within-year average" if drwdwn_predict_step==15
	
	// Clean up before proceeding
drop TEMP*


** Confirm dradown hat is populated everywhere groundwater level is populated
foreach v of varlist gw_????_depth_???_* {
	local v2 = subinstr("`v'","_depth_","_DRWDWNhat_",1)
	assert `v2'!=. if `v'!=.
}
assert drwdwn_predict_step!=. & drwdwn_predict_step_desc!=""

** Gutchecks
sum drwdwn if flag_bad_drwdwn==0 & modate==test_modate, detail
sum gw_rast_DRWDWNhat_qtr_1sSP if flag_bad_drwdwn==0 & modate==test_modate, detail
sum gw_rast_DRWDWNhat_qtr_1sSP if flag_bad_drwdwn==1 & drwdwn!=. & modate==test_modate, detail
sum gw_rast_DRWDWNhat_qtr_1sSP if flag_bad_drwdwn==1 & drwdwn==. & modate==test_modate, detail
twoway scatter gw_rast_DRWDWNhat_qtr_1sSP drwdwn if flag_bad_drwdwn==0 & modate==test_modate & drwdwn<151, msize(tiny)

** For pump tests with good drawdown, keep predicted dradown within 1 standard deviation of measured drawdown 
sum drwdwn if flag_bad_drwdwn==0 & modate==test_modate, detail
local sd = r(sd)
gen temp_lo = drwdwn - `sd'
gen temp_hi = drwdwn + `sd'
foreach v of varlist *_DRWDWNhat_* {
	replace `v' = temp_lo if `v'<temp_lo & flag_bad_drwdwn==0 // about 13% of predictions at most
	replace `v' = temp_hi if `v'>temp_hi & flag_bad_drwdwn==0 // about 6% of predictions at most
}
drop temp*
twoway scatter gw_rast_DRWDWNhat_qtr_1sSP drwdwn if flag_bad_drwdwn==0 & modate==test_modate & drwdwn<151, msize(tiny)
	// much better! this ensures that dradown is mostly stable within a pump over time 

** Reverse-populate drawdown from predictions, for pump tests with bad or missing drwdwn
replace drwdwn = gw_rast_DRWDWNhat_qtr_1sSP if flag_bad_drwdwn==1 & modate==test_modate
egen double temp1 = mean(drwdwn) if flag_bad_drwdwn==1 & modate==test_modate, by(apeptestid_uniq)
egen double temp2 = mean(temp1) if flag_bad_drwdwn==1, by(apeptestid_uniq)
replace drwdwn = temp2 if flag_bad_drwdwn==1 & modate!=test_modate
egen temp3 = min(drwdwn), by(apeptestid_uniq)
egen temp4 = max(drwdwn), by(apeptestid_uniq)
assert temp3==temp4 & temp4<. if flag_bad_drwdwn==1
drop temp*

** For pump tests with bad drawdown, impose the same 1-sd constraint
sum drwdwn if flag_bad_drwdwn==0 & modate==test_modate, detail
local sd = r(sd)
gen temp_lo = drwdwn - `sd'
gen temp_hi = drwdwn + `sd'
foreach v of varlist *_DRWDWNhat_* {
	replace `v' = temp_lo if `v'<temp_lo & flag_bad_drwdwn==1 & temp_lo!=.
	replace `v' = temp_hi if `v'>temp_hi & flag_bad_drwdwn==1 & temp_hi!=.
}
drop temp*

** Confirm drawdown is always populated
assert inrange(drwdwn,0,151)

** Construct estimates of pump water level, and confirm internal consistency
preserve
keep if modate==test_modate & flag_bad_drwdwn==0
gen test = pwl - (swl + drwdwn)
sum test, detail
gen test2 = pwl - (rwl + drwdwn)
sum test2, detail
restore
foreach v of varlist gw_????_depth_???_* {
	local v2 = subinstr("`v'","_depth_","_DRWDWNhat_",1)
	local v3 = subinstr("`v2'","gw_","PWL_",1)
	local v4 = subinstr("`v3'","_DRWDWNhat_","_DRWDWN_",1)
	gen `v3' = max(`v',0) + `v2' // one version using drwdwn predictions
	gen `v4' = max(`v',0) + drwdwn // one version that's constant within pump over time
}	

** Construct estimates of total lift, and confirm internal consistency
preserve
keep if modate==test_modate & flag_bad_drwdwn==0
gen test = totlift - (pwl+dchlvl_ft+gaugecor_ft+gaugeheight_ft+otherlosses_ft+totlift_gap)
sum test, detail
restore
replace totlift_gap = 0 if totlift_gap==. // a few random missings?
foreach v of varlist PWL_* {
	local v2 = subinstr("`v'","PWL_","TDH_",1)
	gen `v2' = `v' + dchlvl_ft + gaugecor_ft + gaugeheight_ft + otherlosses_ft + totlift_gap
	replace `v2' =  max(`v2',0) // shouldn't be negative! pumps pull water up!
}	

** Construct estimates of horesepower, and confirm internal consistency
preserve
keep if modate==test_modate & flag_bad_drwdwn==0
gen test = hp - (totlift * flow_gpm / ope / 39.6)
sum test, detail
restore
foreach v of varlist TDH_* {
	local v2 = subinstr("`v'","TDH_","HP_",1)
	gen `v2' = `v' * flow_gpm / ope / 39.6
}	

** Convert estiamted horsepower to estimated kw, and confirm internal consistency
preserve
keep if modate==test_modate & flag_bad_drwdwn==0
gen test = kw_input - (hp * 0.7457)
sum test, detail
restore
foreach v of varlist HP_* {
	local v2 = subinstr("`v'","HP_","KW_",1)
	gen `v2' = `v' * 0.7457
}	

** Construct estimated kwh per af, and confirm internal consistency
preserve
keep if modate==test_modate & flag_bad_drwdwn==0
gen test = kwhaf - (kw_input * 24 / af24hrs)
gen test2 = test/kwhaf
sum test*, detail
restore
foreach v of varlist KW_* {
	local v2 = subinstr("`v'","KW_","KWHAF_",1)
	gen `v2' = `v' * 24 / af24hrs
	assert `v2'!=. & `v2'>=0
}
sum kwhaf KWHAF_rast_DRWDWNhat_qtr_1sSP if modate==test_modate, detail
correlate kwhaf KWHAF_rast_DRWDWN_qtr_1sSP if modate==test_modate
	// rho = 0.49, pretty decent?

	
** Prepare to collapse by keeping only relevant variables
keep sp_uuid customertype farmtype waterenduse apeptestid test_date_stata apeptestid_uniq ///
	date_proj_finish apep_proj_count watersource drvtype pumptype flow_gpm kwhaf ///
	latlon_group test_modate sp_pump_id sp_pump_id3 case apep_post test_modate_before ///
	test_modate_after modate interp_wgt months_until_test months_since_test ///
	post_apep_proj_finish extrap_post_to_pre extrap_pre_to_post qtr ///
	gw_rast_dist_mth_1SP gw_rast_dist_mth_2SP gw_rast_dist_mth_3SP  ///
	gw_mth_bsn_cnt1SP gw_mth_bsn_cnt2SP gw_mth_bsn_cnt3SP ///
	gw_rast_dist_qtr_1SP gw_rast_dist_qtr_2SP gw_rast_dist_qtr_3SP ///
	gw_qtr_bsn_cnt1SP gw_qtr_bsn_cnt2SP gw_qtr_bsn_cnt3SP ///
	gw_rast_dist_mth_1 gw_rast_dist_mth_2 gw_rast_dist_mth_3 ///
	gw_mth_bsn_cnt1 gw_mth_bsn_cnt2 gw_mth_bsn_cnt3 ///
	gw_rast_dist_qtr_1 gw_rast_dist_qtr_2 gw_rast_dist_qtr_3 ///
	gw_qtr_bsn_cnt1 gw_qtr_bsn_cnt2 gw_qtr_bsn_cnt3 ///
	drwdwn_predict_step drwdwn_predict_step_desc flag_bad_drwdwn KWHAF_*	


** Collapse to SP-pump-month level (unweighted averages)
	// For the few cases where we have parallel time series within a(n observable) pump

	// Unweighted means of numeric variables
foreach v of varlist flow_gpm kwhaf KWHAF_* {
	egen double temp1 = mean(`v') if `v'!=., by(sp_uuid sp_pump_id modate test_modate)
	egen double temp2 = mean(temp1), by(sp_uuid sp_pump_id modate test_modate)
	replace `v' = temp2 if temp2!=.
	drop temp*
}

	// Take minimum of distances to nearest groundwater measurement
foreach v of varlist gw_rast_dist_* {
	egen double temp = min(`v'), by(sp_uuid sp_pump_id modate test_modate)
	replace `v' = temp if temp!=.
	drop temp
}	

	// Take maximum of counts of groundwater measurements
foreach v of varlist gw_*_cnt* {
	egen double temp = max(`v'), by(sp_uuid sp_pump_id modate test_modate)
	replace `v' = temp if temp!=.
	drop temp
}	

	// Take maximum of drawdown prediction step flags 
egen temp1 = max(drwdwn_predict_step), by(sp_uuid sp_pump_id modate test_modate)
gen temp2 = drwdwn_predict_step_desc if drwdwn_predict_step==temp1
egen temp3 = mode(temp2), by(sp_uuid sp_pump_id modate test_modate)
assert temp3!="" & temp1!=.
replace drwdwn_predict_step = temp1
replace drwdwn_predict_step_desc = temp3
drop temp*

	// Take maximum of post indicators and extrapolation flags
foreach v of varlist case apep_post post_apep_proj_finish extrap_post_to_pre extrap_pre_to_post flag_bad_drwdwn {	
	if regexm("`v'","extrap") {
		assert `v'!=.
	}
	egen temp = max(`v'), by(sp_uuid sp_pump_id modate test_modate)
	replace `v' = temp
	drop temp
}
	
	// Take minimum of APEP pump test IDs (to have for linking back to APEP dataset)
foreach v of varlist apeptestid apeptestid_uniq latlon_group {
	egen temp = min(`v'), by(sp_uuid sp_pump_id modate test_modate)
	replace `v' = temp
	drop temp
}	
drop test_date_stata

	// Take mode of APEP pump test categorical characteristics
foreach v of varlist customertype farmtype waterenduse watersource drvtype pumptype {
	egen temp = mode(`v'), by(sp_uuid sp_pump_id modate test_modate) minmode
	replace `v' = temp
	drop temp
}
	
	// Take average of interpolation weights
egen double temp = mean(interp_wgt), by(sp_uuid sp_pump_id modate test_modate)
assert inrange(temp,0,1)
replace interp_wgt = temp
drop temp
	
	// Drop indentifier for parallel "pump" time series, and collapse!
drop sp_pump_id3 
duplicates drop

	// Confirm uniqueness 
unique sp_uuid sp_pump_id modate test_modate
assert r(unique)==r(N)


** Rescale interpolation weights
egen double temp = sum(interp_wgt), by(sp_uuid modate)
replace interp_wgt = interp_wgt/temp
egen double temp2 = sum(interp_wgt), by(sp_uuid modate)
assert round(temp2,0.00001)==1
drop temp*
		
** Flag weird pumps
tab drvtype watersource, missing
tab pumptype 
gen flag_weird_pump = 0
replace flag_weird_pump = 1 if drvtype!="Electric Motor" & drvtype!=""
replace flag_weird_pump = 1 if watersource!="Well" & watersource!=""
drop drvtype watersource pumptype

** Flag weird customers 
tab customertype waterenduse, missing
tab farmtype
gen flag_weird_cust = 0
replace flag_weird_cust = 1 if customertype=="Irrigation Districts"
replace flag_weird_cust = 1 if inlist(waterenduse,"district","food processing","industrial","municipal")
drop customertype waterenduse farmtype


** Collapse to SP-month level (averaging using interpolation weights)

	// Drop if weight = 0, as these observations will contribute nothing
drop if interp_wgt==0	
drop flow_gpm // not using this variable after all

	// Weight-average numeric variables
foreach v of varlist kwhaf KWHAF_* {
	assert `v'!=.
	egen double temp = sum(interp_wgt * `v'), by(sp_uuid modate)
	assert temp!=.
	replace `v' = temp
	drop temp
}

	// Take minimum of distances to nearest groundwater measurement
foreach v of varlist gw_rast_dist_* {
	egen double temp = min(`v'), by(sp_uuid modate)
	replace `v' = temp if temp!=.
	drop temp
}	

	// Take maximum of counts of groundwater measurements
foreach v of varlist gw_*_cnt* {
	egen double temp = max(`v'), by(sp_uuid modate)
	replace `v' = temp if temp!=.
	drop temp
}	

	// Take maximum of drawdown prediction step flags 
egen temp1 = max(drwdwn_predict_step), by(sp_uuid modate)
gen temp2 = drwdwn_predict_step_desc if drwdwn_predict_step==temp1
egen temp3 = mode(temp2), by(sp_uuid modate)
assert temp3!="" & temp1!=.
replace drwdwn_predict_step = temp1
replace drwdwn_predict_step_desc = temp3
drop temp*

	// Take maximum of post indicators and extrapolation flags
foreach v of varlist case apep_post post_apep_proj_finish extrap_post_to_pre extrap_pre_to_post flag_bad_drwdwn {	
	if regexm("`v'","extrap") {
		assert `v'!=.
	}
	egen temp = max(`v'), by(sp_uuid modate)
	replace `v' = temp
	drop temp
}
	
	// Take minimum of APEP pump test IDs (to have for linking back to APEP dataset)
foreach v of varlist apeptestid apeptestid_uniq latlon_group {
	egen temp = min(`v'), by(sp_uuid modate)
	replace `v' = temp
	drop temp
}	

	// Take maximum of APEP weird flags
foreach v of varlist flag_weird_pump flag_weird_cust {
	assert `v'!=.
	egen temp = max(`v'), by(sp_uuid modate)
	replace `v' = temp
	drop temp
}

	// Take minimum of months until/since a pump test
foreach v of varlist months_until_test months_since_test {
	egen temp = min(`v'), by(sp_uuid modate)
	replace `v' = temp
	drop temp	
}

	// Make sure test_modate_before and _after are actuall before/after the month in question
duplicates t sp_uuid modate, gen(dup)
sort sp_uuid modate test_modate
br sp_uuid modate test_modate test_modate_before test_modate_after interp_wgt months* if dup>0
egen temp1 = max(test_modate_before), by(sp_uuid modate)
egen temp2 = min(test_modate_after), by(sp_uuid modate)
replace test_modate_before = temp1 
replace test_modate_after = temp2
drop temp* dup

	// Replace test_modate with nearest_test_modate
assert months_until_test!=. | months_since_test!=.	
gen temp1 = modate - months_since_test
gen temp2 = modate + months_until_test
gen nearest_test_modate = temp2
replace nearest_test_modate = temp1 if months_since_test<=months_until_test
assert nearest_test_modate!=.
format %tm nearest_test_modate
drop temp* test_modate

	// Drop pump indentifier and collapse!
drop sp_pump_id interp_wgt qtr
duplicates drop

	// Confirm uniqueness 
unique sp_uuid modate
assert r(unique)==r(N)


** Rename and label variables
rename case apep_interp_case
la var apep_interp_case "1 single test; 2 tri kernel; 3 tri kernel, >1 pump; 4 parallel, >1 pump"

la var post_apep_proj_finish "Dummy for after SP's 1st APEP project is completed (always 0 if no project)"
assert post_apep_proj_finish==0 if apep_proj_count==0
drop apep_post // redundant now
la var extrap_post_to_pre "Pump specs from post-to-pre project extrapolation (assume only OPE changed)"
la var extrap_pre_to_post "Pump specs from pre-to-post project extrapolation (assume _after variables)"

la var flag_bad_drwdwn "Flag for drawdown <0 ft, >151 ft, or missing"
la var drwdwn_predict_step "Ordinal (best-to-worst) in quality of drawdown predictions"
la var drwdwn_predict_step_desc "Description of how each setep predicts drawdown"

la var modate "Year-Month"
la var nearest_test_modate "Month of nearest (in time) pump test linked to SP-month"
la var months_until_test "Months until next pump test linked to SP-month"
la var months_since_test "Months since last pump test linked to SP-month"
la var test_modate_before "Month of preceding pump test, used to interpolate SP-month"
la var test_modate_after "Month of following pump test, used to interpolate SP-month"
gen months_to_nearest_test = min(months_until_test, months_since_test)
la var months_to_nearest_test "Months to nearest test (before/after) month, for SP"
assert months_to_nearest_test!=. & months_to_nearest_test==abs(nearest_test_modate-modate)

la var flag_weird_pump "Dummy for pumps that are either non-electric or non-well"
la var flag_weird_cust "Dummy for irrigation districts or non-ag end-uses"

rename *_DRWDWN* *_dd*
rename *_1s *_1
rename *_2s *_2
rename *_3s *_3
rename *_1sSP *_1SP
rename *_2sSP *_2SP
rename *_3sSP *_3SP
rename KWHAF_* kwhaf_*

rename kwhaf kwhaf_apep_measured
la var kwhaf_apep_measured "KWH/AF as measured by APEP test(s); often interpolated from measured values"

la var kwhaf_rast_dd_mth_1 "Predicted KWH/AF (pump latlon, mthly rasters, fixed drawdown, all gw)"
la var kwhaf_rast_dd_mth_2 "Predicted KWH/AF (pump latlon, mthly rasters, fixed drawdown, non-ques gw)"
la var kwhaf_rast_dd_mth_3 "Predicted KWH/AF (pump latlon, mthly rasters, fixed drawdown, obs non-ques gw)"
la var kwhaf_rast_ddhat_mth_1 "Predicted KWH/AF (pump latlon, mthly rasters, predicted drawdown, all gw)"
la var kwhaf_rast_ddhat_mth_2 "Predicted KWH/AF (pump latlon, mthly rasters, predicted drawdown, non-ques gw)"
la var kwhaf_rast_ddhat_mth_3 "Predicted KWH/AF (pump latlon, mthly rasters, predicted drawdown, obs non-ques gw)"
la var kwhaf_rast_dd_qtr_1 "Predicted KWH/AF (pump latlon, qtrly rasters, fixed drawdown, all gw)"
la var kwhaf_rast_dd_qtr_2 "Predicted KWH/AF (pump latlon, qtrly rasters, fixed drawdown, non-ques gw)"
la var kwhaf_rast_dd_qtr_3 "Predicted KWH/AF (pump latlon, qtrly rasters, fixed drawdown, obs non-ques gw)"
la var kwhaf_rast_ddhat_qtr_1 "Predicted KWH/AF (pump latlon, qtrly rasters, predicted drawdown, all gw)"
la var kwhaf_rast_ddhat_qtr_2 "Predicted KWH/AF (pump latlon, qtrly rasters, predicted drawdown, non-ques gw)"
la var kwhaf_rast_ddhat_qtr_3 "Predicted KWH/AF (pump latlon, qtrly rasters, predicted drawdown, obs non-ques gw)"

la var kwhaf_mean_dd_mth_1 "Predicted KWH/AF (pump latlon, mthly means, fixed drawdown, all gw)"
la var kwhaf_mean_dd_mth_2 "Predicted KWH/AF (pump latlon, mthly means, fixed drawdown, non-ques gw)"
la var kwhaf_mean_dd_mth_3 "Predicted KWH/AF (pump latlon, mthly means, fixed drawdown, obs non-ques gw)"
la var kwhaf_mean_ddhat_mth_1 "Predicted KWH/AF (pump latlon, mthly means, predicted drawdown, all gw)"
la var kwhaf_mean_ddhat_mth_2 "Predicted KWH/AF (pump latlon, mthly means, predicted drawdown, non-ques gw)"
la var kwhaf_mean_ddhat_mth_3 "Predicted KWH/AF (pump latlon, mthly means, predicted drawdown, obs non-ques gw)"
la var kwhaf_mean_dd_qtr_1 "Predicted KWH/AF (pump latlon, qtrly means, fixed drawdown, all gw)"
la var kwhaf_mean_dd_qtr_2 "Predicted KWH/AF (pump latlon, qtrly means, fixed drawdown, non-ques gw)"
la var kwhaf_mean_dd_qtr_3 "Predicted KWH/AF (pump latlon, qtrly means, fixed drawdown, obs non-ques gw)"
la var kwhaf_mean_ddhat_qtr_1 "Predicted KWH/AF (pump latlon, qtrly means, predicted drawdown, all gw)"
la var kwhaf_mean_ddhat_qtr_2 "Predicted KWH/AF (pump latlon, qtrly means, predicted drawdown, non-ques gw)"
la var kwhaf_mean_ddhat_qtr_3 "Predicted KWH/AF (pump latlon, qtrly means, predicted drawdown, obs non-ques gw)"

la var kwhaf_rast_dd_mth_1SP "Predicted KWH/AF (SP latlon, mthly rasters, fixed drawdown, all gw)"
la var kwhaf_rast_dd_mth_2SP "Predicted KWH/AF (SP latlon, mthly rasters, fixed drawdown, non-ques gw)"
la var kwhaf_rast_dd_mth_3SP "Predicted KWH/AF (SP latlon, mthly rasters, fixed drawdown, obs non-ques gw)"
la var kwhaf_rast_ddhat_mth_1SP "Predicted KWH/AF (SP latlon, mthly rasters, predicted drawdown, all gw)"
la var kwhaf_rast_ddhat_mth_2SP "Predicted KWH/AF (SP latlon, mthly rasters, predicted drawdown, non-ques gw)"
la var kwhaf_rast_ddhat_mth_3SP "Predicted KWH/AF (SP latlon, mthly rasters, predicted drawdown, obs non-ques gw)"
la var kwhaf_rast_dd_qtr_1SP "Predicted KWH/AF (SP latlon, qtrly rasters, fixed drawdown, all gw)"
la var kwhaf_rast_dd_qtr_2SP "Predicted KWH/AF (SP latlon, qtrly rasters, fixed drawdown, non-ques gw)"
la var kwhaf_rast_dd_qtr_3SP "Predicted KWH/AF (SP latlon, qtrly rasters, fixed drawdown, obs non-ques gw)"
la var kwhaf_rast_ddhat_qtr_1SP "Predicted KWH/AF (SP latlon, qtrly rasters, predicted drawdown, all gw)"
la var kwhaf_rast_ddhat_qtr_2SP "Predicted KWH/AF (SP latlon, qtrly rasters, predicted drawdown, non-ques gw)"
la var kwhaf_rast_ddhat_qtr_3SP "Predicted KWH/AF (SP latlon, qtrly rasters, predicted drawdown, obs non-ques gw)"

la var kwhaf_mean_dd_mth_1SP "Predicted KWH/AF (SP latlon, mthly means, fixed drawdown, all gw)"
la var kwhaf_mean_dd_mth_2SP "Predicted KWH/AF (SP latlon, mthly means, fixed drawdown, non-ques gw)"
la var kwhaf_mean_dd_mth_3SP "Predicted KWH/AF (SP latlon, mthly means, fixed drawdown, obs non-ques gw)"
la var kwhaf_mean_ddhat_mth_1SP "Predicted KWH/AF (SP latlon, mthly means, predicted drawdown, all gw)"
la var kwhaf_mean_ddhat_mth_2SP "Predicted KWH/AF (SP latlon, mthly means, predicted drawdown, non-ques gw)"
la var kwhaf_mean_ddhat_mth_3SP "Predicted KWH/AF (SP latlon, mthly means, predicted drawdown, obs non-ques gw)"
la var kwhaf_mean_dd_qtr_1SP "Predicted KWH/AF (SP latlon, qtrly means, fixed drawdown, all gw)"
la var kwhaf_mean_dd_qtr_2SP "Predicted KWH/AF (SP latlon, qtrly means, fixed drawdown, non-ques gw)"
la var kwhaf_mean_dd_qtr_3SP "Predicted KWH/AF (SP latlon, qtrly means, fixed drawdown, obs non-ques gw)"
la var kwhaf_mean_ddhat_qtr_1SP "Predicted KWH/AF (SP latlon, qtrly means, predicted drawdown, all gw)"
la var kwhaf_mean_ddhat_qtr_2SP "Predicted KWH/AF (SP latlon, qtrly means, predicted drawdown, non-ques gw)"
la var kwhaf_mean_ddhat_qtr_3SP "Predicted KWH/AF (SP latlon, qtrly means, predicted drawdown, obs non-ques gw)"


** Sort, order and save
sort sp_uuid modate
order sp_uuid modate months_until_test months_since_test months_to_nearest_test nearest_test_modate ///
	test_modate_* apep_proj_count date_proj_finish post_apep_proj_finish extrap_* apep_interp_case ///
	apeptestid* latlon_group flag_bad_drwdwn drwdwn* flag_weird_pump flag_weird_cust ///
	kwhaf_apep_measured kwhaf*
unique sp_uuid modate
assert r(unique)==r(N)	
compress
save "$dirpath_data/merged/sp_month_kwhaf_panel.dta", replace

	
}

*******************************************************************************
*******************************************************************************

