clear all
version 13
set more off

***********************************************************************
**** Script to clean raw SCE data -- pump test project data file ******
***********************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"

** Load raw SCE pump test project data
use "$dirpath_data/sce_raw/pump_overhaul_data_20190916.dta", clear
duplicates drop
dropmiss, obs force
dropmiss, force
gen pull = 2019
append using "$dirpath_data/sce_raw/pump_overhaul_data_20200722.dta"
duplicates drop
dropmiss, obs force
dropmiss, force 
replace pull = 2020 if pull==.

** Remove dups that are identical across pulls
duplicates t ApplicationReceivedDate-Year, gen(dup)
tab dup // all but 2 are identical!
unique ApplicationReceivedDate-Year
local uniq = r(unique)
drop if dup==1 & pull==2020
unique ApplicationReceivedDate-Year
assert r(unique)==`uniq'
tab pull
sort Year * 
br
drop pull dup


foreach var of varlist * {
 label variable `var' ""
}
gen uniq_proj_id = _n
la var uniq_proj_id "Unique SCE project ID"
order uniq_proj_id

** SCE SA ID
rename ServAcctCSSNum serv_acct_css_num
assert inlist(substr(serv_acct_css_num,1,2),"SA","CU")
gen sa_uuid = ""
replace sa_uuid = substr(serv_acct_css_num,3,100) if substr(serv_acct_css_num,1,2)=="SA"
replace sa_uuid = substr(serv_acct_css_num,3,100) if substr(serv_acct_css_num,1,2)=="CU"
gen flag_cu_prefix = substr(serv_acct_css_num,1,2)=="CU"
tab flag_cu_prefix
drop serv_acct_css_num
la var sa_uuid "SCE service account ID"
order sa_uuid, after(uniq_proj_id)
unique sa_uuid // 993 (out of 1337 total)

** Dates
rename ApplicationReceivedDate application_received_date
rename IRReceivedDateInvoiceRecvd invoice_received_date
gen date_application = date(application_received_date,"DMY")
gen date_invoice = date(invoice_received_date,"MDY")
format %td date*
order date*, after(sa_uuid)
gen temp = date_invoice - date_application
hist temp
gen flag_date_problem = temp<0 | temp==.
br if flag_date_problem
la var flag_date_problem "Date inconsistency (pre after post, or missing)"
la var date_application "Date application was received (pre project)"
la var date_invoice "Date invoice was received (post project)"
drop application_received_date invoice_received_date temp

** Product 
rename ProductDescription product_description
tab product_description
gen product_type = word(product_description,1)
drop product_description
la var product_type "Ag or indsturial?"
order product_type, after(date_invoice)

rename ProductStatus product_status
tab product_status
rename product_status eligibility
la var eligibility "Eligible for subsidies?"
order eligibility, after(product_type)

rename ProductSubStatus product_substatus
tab product_substatus
tab eligibility product_substatus
gen declined = product_substatus=="Declined"
la var declined "Declined?"
order declined, after(eligibility)
drop product_substatus

rename InstalledQuantity installed_quantity
tab installed_quantity declined
la var installed_quantity "Installed quantity (... of pumps? of measures?):"

** Subsidy amount
rename NetIncentiveAmount net_incentive
destring net_incentive, replace
tab declined if net_incentive==0
tab declined if net_incentive!=0
tab eligibility if net_incentive==0
tab eligibility if net_incentive!=0
la var net_incentive "Net incentive received on project"
order net_incentive, after(declined)
assert net_incentive!=.
hist net_incentive
hist net_incentive if net_incentive>0

gen flag_incentive_problem = 0
replace flag_incentive_problem = 1 if net_incentive>0 & declined==1
replace flag_incentive_problem = 1 if net_incentive>0 & eligibility=="Ineligible"
la var flag_incentive_problem "Inconsistency in incentive amount, eligiblity, declined"

** Savings
rename kWSavings kw_savings 
rename kWhSavings kwh_savings
destring kw_savings kwh_savings, replace
hist kw_savings
hist kwh_savings
assert kwh_savings>=0 & kwh_savings!=.
assert kw_savings!=.
la var kwh_savings "Estimated kWh savings (... ex ante? per year?)"
la var kw_savings "Change in motor power (sometimes negative)"
order  net_incentive kwh_savings kw_savings installed_quantity, after(declined)

** Variables we likely don't need
rename ContractDescription contract_description
rename CustomerName customer_name
rename CustomerAddress customer_address
rename CustomerCity customer_city
rename CustomerZip customer_zip
rename SAAddress sa_address
rename SACity sa_city
rename ContactZip contact_zip
rename Year sce_year
la var contract_description "Contract description"
la var customer_name "Customer name"
la var sa_address "Service account address"
la var sa_city "Service account city"
la var contact_zip "Contact ZIP code"
la var customer_address "Customer address"
la var customer_city "Customer city
la var customer_zip "Customer ZIP code"
la var sce_year "Year SCE categorized project under"

** Merge into pump test data (direct)
preserve
use "$dirpath_data/sce_cleaned/sce_pump_test_data.dta", clear
collapse (min) mindate = test_date_stata (max) maxdate = test_date_stata (count) ntests = uniq_id ///
	(mean) booster_pump, by(sa_uuid)
tempfile accts
save `accts'
restore
merge m:1 sa_uuid using `accts'	
br if _merge==1 & sa_uuid!=""
tab flag_cu_prefix _merge 
gen flag_sa_not_in_pumptests = _merge==1 
tab ntests _merge
tab booster_pump _merge
drop if _merge==2 
drop mindate maxdate ntests booster_pump _merge

** Attempt to diagnose non-merges using xwalk
preserve
keep if flag_cu_prefix==1 | flag_sa_not_in_pumptests==1
keep sa_uuid flag_cu_prefix flag_sa_not_in_pumptests
duplicates drop
merge 1:m sa_uuid using "$dirpath_data/sce_cleaned/customer_id_xwalk_20200504_updated.dta", keep(1 3)
tab flag_cu_prefix _merge // they're not SA IDs
tab flag_sa_not_in_pumptests _merge 
restore

preserve
keep if flag_cu_prefix==1 | flag_sa_not_in_pumptests==1
keep sa_uuid flag_cu_prefix flag_sa_not_in_pumptests
duplicates drop
rename sa_uuid sp_uuid
merge 1:m sp_uuid using "$dirpath_data/sce_cleaned/customer_id_xwalk_20200504_updated.dta", keep(1 3)
tab flag_cu_prefix _merge // they're not SP IDs
tab flag_sa_not_in_pumptests _merge 
restore

preserve
keep if flag_cu_prefix==1 | flag_sa_not_in_pumptests==1
keep sa_uuid flag_cu_prefix flag_sa_not_in_pumptests
duplicates drop
rename sa_uuid prsn_uuid
merge 1:m prsn_uuid using "$dirpath_data/sce_cleaned/customer_id_xwalk_20200504_updated.dta", keep(1 3)
tab flag_cu_prefix _merge // they're not person IDs
tab flag_sa_not_in_pumptests _merge 
restore

// NO IDEA
// 2014 CSS number is systematically different in the raw data, something to ask about
la var flag_cu_prefix "Flag for observations reported in 2014, with prefix CU on sa_uuid"
la var flag_sa_not_in_pumptests "Flag for SAs that don't merge into pump test dataset"

** Save
compress
save "$dirpath_data/sce_cleaned/sce_pump_test_project_data.dta", replace

