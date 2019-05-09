do 0_Set_Path.do

global path_in= "$path_master/Names"
global path_out= "$path_master/Cut_1"
global path_temp= "$path_master/Temp"


// matchit idmaster txtmaster using filename.dta , idusing(varname) txtusing(varname) [options]

//first cut PO has index id_PO and name 'name'
// DBF file has no index and name 'name'
local keys farm acreage water district irrigation field homestead lawn meadow nursery orchard pasture plantation ranch acres

//make index for dbf file
use "$path_in/Entity_Names_DBF.dta", clear
cap gen id_DBF=_n
save "$path_in/Entity_Names_DBF.dta", replace
//set trace on
foreach key of local keys {
	//for using data
	use "$path_in/First_Cut_PO.dta", clear
	replace name= ustrlower(name)
	keep if strpos(name,"`key'")>0
	replace name= subinstr(name,"`key'","",.)
	replace name= strtrim(name)
	replace name= stritrim(name)
	keep name id_PO
	rename name name_PO
	save "$path_temp/First_Cut_PO_`key'.dta", replace
	
	//for master data
	use "$path_in/Entity_Names_DBF.dta", clear
	replace name= ustrlower(name)
	keep if strpos(name,"`key'")>0
	replace name= subinstr(name,"`key'","",.)
	replace name= strtrim(name)
	replace name= stritrim(name)
	keep name id_DBF
	rename name name_DBF
	save "$path_temp/DBF_`key'.dta", replace
	
	matchit id_DBF name_DBF using "$path_temp/First_Cut_PO_`key'.dta", idusing(id_PO) txtusing(name_PO) override
	gsort -similscore
	save "$path_temp/Match_`key'.dta", replace
	
}


//now for "water district" and "irrigation district"

local keys2 `" "water district" "irrigation district" "'

foreach key of local keys2 {
	
	use "$path_in/First_Cut_PO.dta", clear
	replace name= ustrlower(name)
	keep if strpos(name,"`key'")>0
	replace name= subinstr(name,"`key'","",.)
	replace name= strtrim(name)
	replace name= stritrim(name)
	keep name id_PO
	rename name name_PO
	save "$path_temp/First_Cut_PO_`key'.dta", replace
	
	//for master data
	use "$path_in/Entity_Names_DBF.dta", clear
	replace name= ustrlower(name)
	keep if strpos(name,"`key'")>0
	replace name= subinstr(name,"`key'","",.)
	replace name= strtrim(name)
	replace name= stritrim(name)
	keep name id_DBF
	rename name name_DBF
	save "$path_temp/DBF_`key'.dta", replace
	
	matchit id_DBF name_DBF using "$path_temp/First_Cut_PO_`key'.dta", idusing(id_PO) txtusing(name_PO) override
	gsort -similscore
	save "$path_temp/Match_`key'.dta", replace

}
