*** BREAK MATCHES INTO VARIOUS TIERS ***
/*

Input:
Phrase-wise matches from the temp folder.
	$path_master/Temp

Output:
Matches that have been split into various tiers, depending on how good they seem.
	$path_out/Tier_1.dta

*/

do 0_Set_Path.do

global path_in= "$path_master/Temp"
global path_out= "$path_master/Cut_1"

//files whihch start with Match_
cd "$path_in"
local files: dir . files "Match_*"

//lower limits for each tier
local ll_1= 0.908
local ll_2= 0.8006

forvalues i=1(1)3 {
	clear
	set obs 1
	gen source=""
	save "$path_out/Tier_`i'.dta", replace
}
//set trace on
foreach f of local files {
	//extract key that was used to match, 
	//who's your daddy
	local new_pitaji= subinstr("`f'",".dta","",.)
	
	//save in Tier_1, these are almost pristine matches
	use "`f'", clear
	keep if similscore>=`ll_1'
	gen source= "`new_pitaji'"
	append using "$path_out/Tier_1.dta"
	save "$path_out/Tier_1.dta", replace
	
	
	//save in Tier_2, these are the ones to review
	use "`f'", clear
	keep if similscore>=`ll_2' & similscore<`ll_1'
	append using "$path_out/Tier_2.dta"
	replace source= "`new_pitaji'"
	save "$path_out/Tier_2.dta", replace
	
	//save in Tier_3, mostly crap
	use "`f'", clear
	keep if similscore<`ll_2'
	append using "$path_out/Tier_3.dta"
	replace source= "`new_pitaji'"
	save "$path_out/Tier_3.dta", replace
}

forvalues i=1(1)3 {
	use "$path_out/Tier_`i'.dta", clear
	drop if missing(name_DBF)
	
	//these are actually the same matches, so where they were identified is irrelevant
	duplicates drop id_DBF id_PO, force
	duplicates tag id_DBF, gen(dupes)
	duplicates tag id_PO, gen(dupes_PO)
	//easier eyeballing for the matches
	gsort -dupes -dupes_PO -similscore
	gen PO_duplicates=.
	gen DBF_duplicates=.
	gen bad_match=.
	save "$path_out/Tier_`i'.dta", replace
}
