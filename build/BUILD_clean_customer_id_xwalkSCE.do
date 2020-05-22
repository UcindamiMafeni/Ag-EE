clear all
version 13
set more off

*********************************************************
**** Script to clean raw SCE customer ID crosswalk ******
*********************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"
global dirpath_raw "T:/Raw Data/PumpData"

** Extract raw xwalk
import excel "$dirpath_raw/SCE05042020/SA to CA Table SCE29202610522.xlsx", clear sheet("ADHQ1376_1_updated") firstrow
dropmiss, force
compress
save "$dirpath_data/sce_raw/customer_id_xwalk_20200504.dta", replace

** Clean xwalk
use "$dirpath_data/sce_raw/customer_id_xwalk_20200504.dta", clear
duplicates drop

rename SERV_ACCT_NUM sa_uuid
assert sa_uuid!=.
la var sa_uuid "SCE service account ID"

rename CSS_INSTL_SERV_NUM sp_uuid
count if sp_uuid==. // missing for 2,112 out of 50,932
la var sp_uuid "SCE service point ID (=CSS_INSTL_SERV_NUM)"

rename CUST_ACCT_NUM prsn_uuid 
count if prsn_uuid==. // missing for 1,469 out of 50,932 
assert sp_uuid==. if prsn_uuid==.
la var prsn_uuid "SCE person (customer) ID"

tostring *, replace
foreach v of varlist * {
	replace `v' = "" if `v'=="."
	gen temp = length(`v')
	tab temp
	drop temp
}

unique sa_uuid
duplicates t sa_uuid, gen(dup)
br if dup>0 // multiple person IDs per SP
duplicates t sa_uuid sp_uuid, gen(dup2)
assert dup2==dup
tab dup

duplicates r prsn_uuid
duplicates r prsn_uuid if dup>0
drop dup*

sort *
compress
save "$dirpath_data/sce_cleaned/custer_id_xwalk_20200504.dta", replace



