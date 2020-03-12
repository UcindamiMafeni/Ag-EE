


use "T:\Projects\Pump Data\data\Davis cost studies\Davis_cost_studies_processed_all.dta", clear

gen profits_less_water = PROFITS + vc_Irrigation
gen oper_profits_less_water = TOTAL_GROSS_RETURNS - TOTAL_OPERATING_COSTS + vc_Irrigation

twoway scatter profits_less_water q_water
list Name_of_crop profits_less_water if abs(profits_less_water)>5000

twoway scatter oper_profits_less_water q_water if abs(oper_profits_less_water)<5000

gen water_share_costs = q_water*p_water / TOTAL_OPERATING_COSTS
gen p_water_af = p_water*12

sum water_share_costs, detail
sum p_water_af, detail

list Number Name_of_crop water_share_costs if inlist(Number,0,8,39,62,73,88,92)
sort water_share_costs
br Number Name_of_crop water_share_costs

forvalues p = 10(10)300 {
	gen oper_prof_water`p' = TOTAL_GROSS_RETURNS - TOTAL_OPERATING_COSTS + q_water*p_water - q_water*`p'/12
	gen prof_water`p' = PROFITS + q_water*p_water - q_water*`p'/12
}

reshape long prof_water oper_prof_water, i(Number Name_of_crop) j(water_price)

separate prof_water, by(Number) gen(PROF)
separate oper_prof_water, by(Number) gen(OP_PROF)

twoway line PROF1-PROF93 water_price, legend(off)
twoway line OP_PROF* water_price, legend(off)

drop PROF1-PROF93 OP_PROF1-OP_PROF93


twoway ///
	(line prof_water water_price if Number==0) ///
	(line prof_water water_price if Number==10) ///
	(line prof_water water_price if Number==15) ///
	(line prof_water water_price if Number==62) ///
	(line prof_water water_price if Number==73) ///
	(line prof_water water_price if Number==55) ///
	(line prof_water water_price if Number==93) ///
	, legend(order(1 "Alfalfa" 2 "Almonds" 3 "Garbanzo" 4 "Oranges" 5 "Pistachios" 6 "Lemon" 7 "Wheat") c(4))
	

twoway ///
	(line oper_prof_water water_price if Number==0) ///
	(line oper_prof_water water_price if Number==10) ///
	(line oper_prof_water water_price if Number==15) ///
	(line oper_prof_water water_price if Number==62) ///
	(line oper_prof_water water_price if Number==73) ///
	(line oper_prof_water water_price if Number==55) ///
	(line oper_prof_water water_price if Number==93) ///
	, legend(order(1 "Alfalfa" 2 "Almonds" 3 "Garbanzo" 4 "Oranges" 5 "Pistachios" 6 "Lemon" 7 "Wheat") c(4))


br Name_of_crop TOTAL_GROSS_RETURNS TOTAL_OPERATING_COSTS q_water p_water oper_prof* if inlist(Name_of_crop,"Almonds","Alfalfa","Cotton","Wheat","Pistachio","Walnuts")
sort oper_prof_water10

br Name_of_crop TOTAL_GROSS_RETURNS TOTAL_OPERATING_COSTS PROFITS q_water p_water prof* if inlist(Name_of_crop,"Almonds","Alfalfa","Cotton","Wheat","Pistachio","Walnuts")
sort prof_water10

gen test = q_water*p_water - vc_Irrigation
sum test, detail

gen temp = q_water*p_water
correlate vc_Irrigation temp
sort test
br Name_of_crop year vc_Irrigation q_water p_water temp test
