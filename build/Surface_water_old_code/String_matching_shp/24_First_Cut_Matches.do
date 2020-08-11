do 0_Set_Path.do

global path_in="C:/Users/Chimmay Lohani/Dropbox/EW_Downloads/Code/Deliverables/Rest"
global path_out="$path_in"
global path_temp= "$path_in/Temp"

local stubs Colorado CVP
local keys `" "water district" "irrigation district" "water company" "city" "county" "community services district" "water agency" "water company" "water storage district" "'

// matchit idmaster txtmaster using filename.dta , idusing(varname) txtusing(varname) [options]


//before continuing with the matching, I'm creating a list of unique shapefile names in the temporary folder
use "$path_master/Water_districts.dta", clear
keep name_shp index_shp
duplicates drop
save "$path_temp/Unique_shapefile_names.dta", replace

foreach stub of local stubs {
	use "$path_master/Water_districts.dta", clear
	matchit index_shp name_shp using "$path_in/`stub'/Contracts_cleaned.dta", idusing(index) txtusing(name_dup) override
	gsort -similscore
	save "$path_out/`stub'/Naive_match.dta", replace
	
	clear
	set obs 1
	gen delete=.
	save "$path_out/`stub'/Collected_phrase_matches.dta", replace
	
	foreach key of local keys {
	//for using data
	use "$path_master/Water_districts.dta", clear
	keep if strpos(name_shp,"`key'")>0
	replace name_shp= subinstr(name_shp,"`key'","",.)
	replace name_shp= strtrim(name_shp)
	replace name_shp= stritrim(name_shp)
	keep name_shp index_shp
	duplicates drop
	save "$path_temp/Water_districts_`key'.dta", replace
	
	//for master data
	use "$path_in/`stub'/Contracts_cleaned.dta", clear
	keep if strpos(name_dup,"`key'")>0
	replace name_dup= subinstr(name_dup,"`key'","",.)
	replace name_dup= strtrim(name_dup)
	replace name_dup= stritrim(name_dup)
	keep name_dup index
	save "$path_temp/`stub'/Contracts_`stub'_`key'.dta", replace
	
	matchit index name_dup using "$path_temp/Water_districts_`key'.dta", idusing(index_shp) txtusing(name_shp) override
	gsort -similscore
	save "$path_temp/`stub'/Match_`stub'_`key'.dta", replace
	
	// add original names for these to be recognisable 
	keep index index_shp similscore
	merge m:1 index using "$path_in/`stub'/Contracts_cleaned.dta"
	assert _merge!=1
	keep if _merge==3
	drop _merge
	keep index index_shp similscore name_dup
	
	merge m:1 index_shp using "$path_temp/Unique_shapefile_names.dta"
	assert _merge!=1
	keep if _merge==3
	drop _merge
	
	append using "$path_out/`stub'/Collected_phrase_matches.dta"
	save "$path_out/`stub'/Collected_phrase_matches.dta", replace
	}
	
	use "$path_out/`stub'/Collected_phrase_matches.dta", clear
	drop if _n==_N
	save "$path_out/`stub'/Collected_phrase_matches.dta", replace
}
