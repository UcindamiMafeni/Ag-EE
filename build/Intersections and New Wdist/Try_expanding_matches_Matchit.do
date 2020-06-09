global path_in= "C:\Users\clohani\OneDrive\Desktop\Check_WRIMS_Matches"
global path_new= "C:\Users\clohani\Dropbox\New_Water_Districts"

// Now we will try to get matches for contracts using secondary matched ones as a pivot, getting more string matches.
// We will string clean a little bit, and then proceed with string matches

/*
1. Take names of contracts, at this point all, make a name that standardises WD, ID type names'
2. Append a list of water districts from shapefile, and names of matched contracts and standardise the names
3. use matchit to match between these two, and see what we get
*/

** Standardise all Haggerty WRIMS contracts 

//import delimited using  "$path_in/haggerty_wr70_cleaned.csv", clear varnames(1)
import delimited using "$path_in/Old_unmatched_post_2ndary_matches_contracts.csv", clear varnames(1)
keep name_po
rename name_po primary_owner
keep primary_owner
duplicates drop primary_owner, force
rename primary_owner name_PO 


** This segment of code is for reformatting the names 
local phrases_DISTRICT `" "DIST. " "DIST " "'
gen names_PO_cln= strupper(name_PO)
replace names_PO_cln= strltrim(names_PO_cln)
replace names_PO_cln= strrtrim(names_PO_cln)
replace names_PO_cln= " " + names_PO_cln + " "

foreach ph of local phrases_DISTRICT {
	replace names_PO_cln= subinstr(names_PO_cln,"`ph'","DISTRICT",.)
}


local phrases_CALIFORNIA `" "CALIF. " "CALIF " "'
local phrases_DEPARTMENT `" "DEPT. " "DEPT " "'
local phrases_WD `" "WATER DISTRICT" "WATER DIST"  "'
local phrases_ID `" "IRRIGATION DISTRICT" "IRRIGATION DIST" " I D " "'
local phrases_WA `" "WATER ASSOCIATION" "WATER AGENCY" " W A " "'
local phrases_WCD `" "WATER CONSERVATION DISTRICT" " W C D " "'
local phrases_WC `" "WATER COMMITTEE" "WATER COMPANY" "WATER COOPERATIVE" "WATER CO-OPERATIVE" "WATER CO." "WATER CO" " W C " "'
local phrases_FC `" "FARMING COMPANY" "FARMING COOPERATIVE" "FARMING CO-OPERATIVE" "FARMING CO." "FARMING CO" " F C "  "'
local phrases_WR `" "WATER RESOURCES" "WATER RESOURCE" " W R " "'
local phrases_MWC `" "MUTUAL WATER COMPANY" "MUTUAL WATER CO" " M W C " "'
local phrases_PUD `" "PUBLIC UTILITIES DISTRICT" "PUBLIC UTILITY DISTRICT" "PUBLIC UTILITY DIST." "PUBLIC UTILITY DIST" " P U D" "'
local phrases_IC `" "IRRIGATION COMPANY" "IRRIGATION CO." "IRRIGATION CO" " I C " "'
local phrases_RD `" "RECLAMATION DISTRICT" "RECLAMATION DIST." "RECLAMATION DIST" " R D " "'


//truncate spaces, then add in front and back and then replace, to get the funky ones like IC 
local phraselist CALIFORNIA DEPARTMENT WD ID WA WCD WC FC WR MWC PUD IC RD

foreach phrase of local phraselist {
	dis "`phrase'" 
	foreach ph of local phrases_`phrase' {
		replace names_PO_cln= subinstr(names_PO_cln,"`ph'","`phrase'",.)
	}
}
duplicates drop
gen id_PO= _n
save "$path_in/temp/WRIMS_unmatched_contracts_modified_names.dta", replace

// Appending list of matched contracts

import dbase "$path_in/Water_Districts.dbf", clear
rename AGENCYNAME name
gen other_WRIMS= 0

append using "$path_in/Unique_final_matches.dta"
keep name name_DBF other_WRIMS
replace name= name_DBF if missing(name)
replace other_WRIMS= 1 if missing(other_WRIMS)
keep name other_WRIMS
duplicates drop


// Code for cleaning recycled, will rename things 

rename name name_PO

local phrases_DISTRICT `" "DIST. " "DIST " "'
gen names_PO_cln= strupper(name_PO)
replace names_PO_cln= strltrim(names_PO_cln)
replace names_PO_cln= strrtrim(names_PO_cln)
replace names_PO_cln= " " + names_PO_cln + " "

foreach ph of local phrases_DISTRICT {
	replace names_PO_cln= subinstr(names_PO_cln,"`ph'","DISTRICT",.)
}


local phrases_CALIFORNIA `" "CALIF. " "CALIF " "'
local phrases_DEPARTMENT `" "DEPT. " "DEPT " "'
local phrases_WD `" "WATER DISTRICT" "WATER DIST"  "'
local phrases_ID `" "IRRIGATION DISTRICT" "IRRIGATION DIST" " I D " "'
local phrases_WA `" "WATER ASSOCIATION" "WATER AGENCY" " W A " "'
local phrases_WCD `" "WATER CONSERVATION DISTRICT" " W C D " "'
local phrases_WC `" "WATER COMMITTEE" "WATER COMPANY" "WATER COOPERATIVE" "WATER CO-OPERATIVE" "WATER CO." "WATER CO" " W C " "'
local phrases_FC `" "FARMING COMPANY" "FARMING COOPERATIVE" "FARMING CO-OPERATIVE" "FARMING CO." "FARMING CO" " F C "  "'
local phrases_WR `" "WATER RESOURCES" "WATER RESOURCE" " W R " "'
local phrases_MWC `" "MUTUAL WATER COMPANY" "MUTUAL WATER CO" " M W C " "'
local phrases_PUD `" "PUBLIC UTILITIES DISTRICT" "PUBLIC UTILITY DISTRICT" "PUBLIC UTILITY DIST." "PUBLIC UTILITY DIST" " P U D" "'
local phrases_IC `" "IRRIGATION COMPANY" "IRRIGATION CO." "IRRIGATION CO" " I C " "'
local phrases_RD `" "RECLAMATION DISTRICT" "RECLAMATION DIST." "RECLAMATION DIST" " R D " "'


//truncate spaces, then add in front and back and then replace, to get the funky ones like IC 
local phraselist CALIFORNIA DEPARTMENT WD ID WA WCD WC FC WR MWC PUD IC RD

foreach phrase of local phraselist {
	dis "`phrase'" 
	foreach ph of local phrases_`phrase' {
		replace names_PO_cln= subinstr(names_PO_cln,"`ph'","`phrase'",.)
	}
}

rename (names_PO_cln name_PO) (names_cln name_PO)
duplicates drop

gen id_list= _n

save "$path_in/temp/Appended_matches_Wdis_mod_names.dta", replace

//matchit idmaster txtmaster using filename.dta , idusing(varname) txtusing(varname)

use "$path_in/temp/WRIMS_unmatched_contracts_modified_names.dta", clear
matchit id_PO names_PO_cln using "$path_in/temp/Appended_matches_Wdis_mod_names.dta", idusing(id_list) txtusing(names_cln)
