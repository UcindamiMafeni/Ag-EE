clear


** Script to import compiled Davis Cost Studies numbers into a single dataset
** These numbers were painstakingly extracted from non-archived PDFs found here: https://coststudies.ucdavis.edu/en/current/
** This .do file imports, cleans, and processes the excel spreadsheet where RAs (Xinyi Wang, Anna Schmidt) manually copied
** cost estimates, PDF by PDF.


global path = "B:/Dropbox/Dropbox/Documents/Research/Energy Water Project/Data"


* Import compiled dataset from excel
import excel "${path}/Davis cost studies/summary_Jan.xlsx", sheet("summary_Anna1") firstrow allstring clear
dropmiss, force
dropmiss, force obs
describe
destring _all, replace
mdesc
drop if Number==.


* Label and re-order the main variables (static)
rename Quanitity5 Quantity5
gen temp = 0
foreach i in 1 2 3 4 5 {
	la var Quantity`i' "Quantity produced per acre (output `i')"
	la var Unit`i' "Units of quantity (output `i')"
	la var Price`i' "Price in $/unit (output `i')"
	replace temp = temp + Quantity`i'*Price`i' if Quantity`i'!=.
}

assert TOTAL_GROSS_RETURNS!=.
la var TOTAL_GROSS_RETURNS "Revenue in $/acre (P*Q summed across products; mature if perennial)"
gen temp2 = TOTAL_GROSS_RETURNS-temp
br Number-TOTAL_GROSS_RETURNS temp* if abs(temp2)>1
twoway scatter TOTAL_GROSS_RETURNS temp, msize(tiny)
rename temp TEMP_revenue
drop temp*

rename Organic_RegistrationCertificati Organic_Registration
rename *Forest* *Frost*
foreach v of varlist Fertilizer-InterestonCapital {
	local vlab = subinstr("`v'","_"," ",.)
	la var `v' "Cost in $/acre, category: `vlab'"
	replace `v' = 0 if `v'==.
	rename `v' vc_`v'
}

assert TOTAL_OPERATING_COSTS!=.
la var TOTAL_OPERATING_COSTS "Total operating cost in $/acre, summed across categories"
egen temp = rowtotal(vc_*)
gen temp2 = TOTAL_OPERATING_COSTS - temp
twoway scatter TOTAL_OPERATING_COSTS temp, msize(tiny)
br if abs(temp2)>1
rename temp TEMP_vc
drop temp2

assert TOTAL_CASH_OVERHEAD_COSTS!=.
la var TOTAL_CASH_OVERHEAD_COSTS "Total cash overhead costs, in $/acre"

assert TOTAL_CASH_COSTS!=.
assert abs(TOTAL_CASH_COSTS-(TOTAL_CASH_OVERHEAD_COSTS+TOTAL_OPERATING_COSTS))<=1
la var TOTAL_CASH_COSTS "Operating costs + cash overhead costs, in $/acre"

assert TOTAL_NONCASH_OVERHEAD_COSTS!=.
la var TOTAL_NONCASH_OVERHEAD_COSTS "Total non-cash overhead cots, in $/acre"

assert TOTAL_COSTS!=.
assert abs(TOTAL_COSTS-(TOTAL_NONCASH_OVERHEAD_COSTS+TOTAL_CASH_COSTS))<=1
la var TOTAL_COSTS "Operating + (non)-cash overhead costs, in $/acre"


* Merge in spreadsheet of water P and Q
preserve 
import excel "${path}/Davis cost studies/irrigation_q_p.xlsx", firstrow clear
dropmiss, force
dropmiss, obs force
assert inlist(units_water,"AcIn","acin","ac in","ac/in")
drop units_water
gen temp = q_water*p_water
twoway scatter temp vc_Irrigation
correlate temp vc_Irrigation
	/* NOTE: this correlation is not perfect because of rounding, and because (for
	a few crops), non-marginal costs are rolled in to the Irrigation subcategory. 
	These are things like capital upkeep, additives, testing, and other fixed costs.
	Another note: the category "Frost Protection" involves water, but that water cost is
	not included in irrigation or in these quantities. */
hist q_water
sum q_water, detail
twoway scatter vc_Irrigation q_water
drop vc_Irrigation temp
tempfile irrig
save `irrig'
restore
merge 1:1 Number using `irrig'
assert _merge==3
drop _merge
la var q_water "Assumed annual irrigation quantity, in acre-inches/acre"
la var p_water "Assumed water price, in $/acre-inch"




* Revenues and costs for early years, before maturity
rename *1st *yr1
rename *2nd *yr2
rename *3rd *yr3
rename *4th *yr4
rename *5th *yr5
rename *6th *yr6
rename *7th *yr7
rename *8th *yr8
replace Total_Cost_Acre_yr1 = 6040 if Total_Cost_Acre_yr1==6060 & Number==34 // typo 
replace Total_Cost_Acre_yr3 = 2585 if Total_Cost_Acre_yr3==2586 & Number==70 // typo 
replace Total_Net_Profit_Acre_yr7 = -252 if Total_Net_Profit_Acre_yr7==252 & Number==88 // typo 
replace Total_Accum_Net_Cost_Acre_yr6 = 6448 if Total_Accum_Net_Cost_Acre_yr6==448 & Number==8 // typo 
replace Total_Accum_Net_Cost_Acre_yr2 = 9539 if Total_Accum_Net_Cost_Acre_yr2==-9539 & Number==29 // typo 
replace Total_Accum_Net_Cost_Acre_yr3 = -3055 if Total_Accum_Net_Cost_Acre_yr3==3055 & Number==29 // typo 
replace Total_Accum_Net_Cost_Acre_yr1 = 23245 if Total_Accum_Net_Cost_Acre_yr1==23345 & Number==28 // typo 
replace Total_Accum_Net_Cost_Acre_yr7 = 22075 if Total_Accum_Net_Cost_Acre_yr7==21571 & Number==88 // typo 


	// Identity 1: Total_Cost_Acre - Income_Acre_from_Production = Total_Net_Cost_Acre
forvalues y = 1/8 {
	gen temp`y' = (Total_Cost_Acre_yr`y' - Income_Acre_from_Production_yr`y') - Total_Net_Cost_Acre_yr`y'
	tab temp`y'
}
drop temp*
	// this adds up to zero in every case but one (rounding), so fill in missings accordingly
forvalues y = 1/8 {	
	di `y'
	replace Income_Acre_from_Production_yr`y' = 0 if Total_Cost_Acre_yr`y'!=. & Total_Cost_Acre_yr`y'==Total_Net_Cost_Acre_yr`y' & Income_Acre_from_Production_yr`y'==.
	replace Income_Acre_from_Production_yr`y' = 0 if Total_Cost_Acre_yr`y'!=. & Total_Net_Cost_Acre_yr`y'==. & Income_Acre_from_Production_yr`y'==.
	replace Total_Net_Cost_Acre_yr`y' = Total_Cost_Acre_yr`y' - Income_Acre_from_Production_yr`y' if Total_Net_Cost_Acre_yr`y'==. & Total_Cost_Acre_yr`y'!=. & Income_Acre_from_Production_yr`y'!=.
	assert inlist(mi(Total_Cost_Acre_yr`y') + mi(Income_Acre_from_Production_yr`y') + mi(Total_Net_Cost_Acre_yr`y'),0,3)

}

	// Identity 2: Total_Cost_Acre - Total_NonCash_overhaead = Total_Cash_Cost_Acre
forvalues y = 1/8 {
	di `y'
	assert inlist(mi(Total_Cost_Acre_yr`y') + mi(Total_NonCash_overhead_yr`y'),0,2)
	gen Total_Cash_Cost_Acre_yr`y' = Total_Cost_Acre_yr`y' - Total_NonCash_overhead_yr`y'
	order Total_Cash_Cost_Acre_yr`y', before(Total_NonCash_overhead_yr`y')
}

	// Identity 3: Total_Net_Cost_Acre = - Total_Net_Profit_Acre
forvalues y = 1/8 {
	di `y'
	gen temp`y' = Total_Net_Cost_Acre_yr`y' + Total_Net_Profit_Acre_yr`y'
	tab temp`y'
}	
drop temp*
forvalues y = 1/8 {
	di `y'
	replace Total_Net_Profit_Acre_yr`y' = -Total_Net_Cost_Acre_yr`y' if Total_Net_Profit_Acre_yr`y'==.
	assert inlist(mi(Total_Net_Profit_Acre_yr`y') + mi(Total_Net_Cost_Acre_yr`y'),0,2)
}

	// Identity 4: Total_Accum_Net_Cost_Acre = running sum of Total_Net_Cost_Acre
gen temp = 0
forvalues y = 1/8 {
	di `y'
	replace temp = temp + Total_Net_Cost_Acre_yr`y' 
	gen temp`y' = temp - Total_Accum_Net_Cost_Acre_yr`y'
	tab temp`y'
}	
drop temp*

	// Labels
forvalues y = 1/8 {
	la var Total_Cash_Cost_Acre_yr`y' "Operating + cash overhead costs in $/acre (year `y')"
	la var Total_NonCash_overhead_yr`y' "Noncash overhead costs in $/acre (year `y')"
	la var Total_Cost_Acre_yr`y' "Operating + (non)-cash overhead costs in $/acre (year `y')"
	la var Income_Acre_from_Production_yr`y' "Income from production in $/acre (year `y')"
	la var Total_Net_Cost_Acre_yr`y' "Net cost (i.e. negative net profit) in $/acre (year `y')"
	la var Total_Net_Profit_Acre_yr`y' "Net profit in $/acre (year `y')"
	la var Total_Accum_Net_Cost_Acre_yr`y' "Cumulatve net cost in $/acre (thru year `y')"
}
		
	
* Label other variables
la var Annual_Perennial "Annual, perennial, or locally determined?"
la var LifeSpan "Assumed lifespan of crop"
tab LifeSpan Annual_Perennial
la var Acres "Assumed acres planted (only for alfalfa)"
la var Estab "Estimates include establishment costs"
replace Estab=0 if Estab==.
la var Number "PDF identifier"
la var File_name_of_PDF "PDF file name"
rename File_name_of_PDF file_name
la var Name_of_crop "Crop name"
la var Year_of_study "Year of study"
rename Year_of_study year
replace year = "2013" if year=="2012-2013"
replace year = "2016" if year=="2106"
destring year, replace
tab year
assert year!=.
rename Location_of_study location
la var location "Location of study"
order LifeSpan Estab Acres, after(TOTAL_COSTS)


* Some minor corrections I stumbled upon

	// remove subsidies for onions
replace Quantity2 = . if Number==61
replace Unit2 = "" if Number==61
replace Price2 = . if Number==61
replace TOTAL_GROSS_RETURNS = Quantity1*Price1 if Number==61

	// fix spinach, which is weirdly not populated in the PDF table
replace Quantity1 = 6500 if Quantity1==. & Number==82
replace Unit1 = "Lb." if Unit1=="" & Number==82
replace Price1 = 1 if Price1==. & Number==82
replace TOTAL_GROSS_RETURNS = Quantity1*Price1 if Number==82

	// fix quantity rounding for sweet cherries
replace Quantity2 = 157.5 if Number==34 & Quantity2==158

	// fix quantity rounding for dry beans
replace Quantity1 = 22.5 if Number==18 & Quantity1==23

	// adjust total renevue for organic pears (added wrong)
replace TOTAL_GROSS_RETURNS = TEMP_revenue if Number==69 & TOTAL_GROSS_RETURNS==7740

	// adjust total costs for organic pears (doesn't add up)
replace TOTAL_OPERATING_COSTS = TEMP_vc if Number==69 & TOTAL_OPERATING_COSTS==6033

	// adjust total costs for potatoes-chippers (doesn't add up)
replace TOTAL_OPERATING_COSTS = TEMP_vc if Number==75 & TOTAL_OPERATING_COSTS==2528

	// adjust total costs for potatoes-chippers (doesn't add up)
replace TOTAL_OPERATING_COSTS = TEMP_vc if Number==76 & TOTAL_OPERATING_COSTS==2529

	// adjust total costs for alfalfa (doesn't add up)
replace TOTAL_OPERATING_COSTS = TEMP_vc if Number==2 & TOTAL_OPERATING_COSTS==1015

	// adjust total costs for almonds (doesn't add up)
replace TOTAL_OPERATING_COSTS = TEMP_vc if Number==9 & TOTAL_OPERATING_COSTS==2251

	// adjust totals
replace TOTAL_CASH_COSTS = TOTAL_OPERATING_COSTS + TOTAL_CASH_OVERHEAD_COSTS if inlist(Number,69,75,76,2,9)	
replace TOTAL_COSTS = TOTAL_CASH_COSTS + TOTAL_NONCASH_OVERHEAD_COSTS if inlist(Number,69,75,76,2,9)	
drop TEMP*


* Profits
gen PROFITS = TOTAL_GROSS_RETURNS - TOTAL_COSTS
sum PROFITS, detail
order PROFITS q_water p_water, after(TOTAL_COSTS)
la var PROFITS "Total gross returns - Total costs ($/acre)"

* Marginal value product of water
gen MVP_water = TOTAL_GROSS_RETURNS / q_water
sort MVP_water
br Name_of_crop TOTAL_GROSS_RETURNS q_water MVP PROFIT
sort Number
br
order MVP_water, after(p_water)
la var MVP_water "Marg value product of water in $/inch (Rev / q_water)"


* Clean up before saving

/*
foreach v of varlist * {
	local v2 = lower("`v'")
	rename `v' `v2'
}
rename total_* tot_*
rename *_accum_* *_cum_*
rename *_from_production_* *_from_prod_*
*/

sort Number
unique Number
assert r(unique)==r(N)
compress
save "${path}/Davis cost studies/Davis_cost_studies_processed_all.dta", replace
