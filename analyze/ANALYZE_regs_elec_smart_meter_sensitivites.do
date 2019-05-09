use "S:\Matt\ag_pump\data\merged\sp_month_elec_panel.dta" , clear
keep if pull=="20180719" 
 
	// Isolate dumb-to-start switchers
egen ever_dumb = max(inlist(rt_sched_cd,"AG-1A","AG-1B")), by(sp_uuid)
egen ever_smart= max(inlist(rt_sched_cd,"AG-1A","AG-1B")==0), by(sp_uuid)
egen ever_ice  = max(inlist(rt_sched_cd,"AG-ICE")), by(sp_uuid)

*unique sp_uuid
*drop if ever_ice==1
*unique sp_uuid
*drop if ever_dumb==0
*unique sp_uuid
*drop if ever_smart==0

	// Find last month of dumb
egen temp1 = max(modate) if inlist(rt_sched_cd,"AG-1A","AG-1B"), by(sp_uuid)	
egen last_dumb = mean(temp1), by(sp_uuid)

	// Find first month of smart
egen temp2 = min(modate) if inlist(rt_sched_cd,"AG-1A","AG-1B")==0, by(sp_uuid)	
egen first_smart = mean(temp2), by(sp_uuid)

unique sp_uuid if first_smart<last_dumb
unique sp_uuid if first_smart-1==last_dumb


gen dumb_smart_switchers = ever_dumb==1 & ever_smart==1
gen dumb_smart_switchers_bad = ever_dumb==1 & ever_smart==1 & first_smart<last_dumb

unique sp_uuid
unique sp_uuid if dumb_smart_switchers==1
unique sp_uuid if dumb_smart_switchers_bad==1


// Define default global: sample
global if_sample = "if flag_nem==0 & flag_geocode_badmiss==0 & flag_irregular_bill==0 & flag_weird_cust==0 & merge_sp_water_panel==3"
	
// Define default global: dependent variable
global DEPVAR = "ihs_kwh"
	
// Define default global: RHS	
global RHS = "(log_p_mean = log_mean_p_kwh_ag_default)"
	
// Define default global: FEs
global FEs = "sp_group#month sp_group#rt_large_ag modate"
	
// Define default global: cluster variables
global VCE = "sp_group modate"	


// Run 2SLS regression
ivreghdfe $DEPVAR $RHS $if_sample & dumb_smart_switchers_bad==0, absorb(${FEs}) cluster(${VCE})

ivreghdfe $DEPVAR $RHS $if_sample & dumb_smart_switchers==0, absorb(${FEs}) cluster(${VCE})

ivreghdfe $DEPVAR $RHS $if_sample & dumb_smart_switchers==1, absorb(${FEs}) cluster(${VCE})

ivreghdfe $DEPVAR $RHS $if_sample & dumb_smart_switchers==0, absorb(${FEs}#county_group) cluster(${VCE})

ivreghdfe $DEPVAR $RHS $if_sample & dumb_smart_switchers==1, absorb(${FEs}#county_group) cluster(${VCE})

ivreghdfe $DEPVAR $RHS $if_sample & dumb_smart_switchers==0, absorb(${FEs}#wdist_group) cluster(${VCE})

ivreghdfe $DEPVAR $RHS $if_sample & dumb_smart_switchers==1, absorb(${FEs}#wdist_group) cluster(${VCE})

ivreghdfe $DEPVAR $RHS $if_sample & dumb_smart_switchers==1, absorb(${FEs}#rt_large_ag) cluster(${VCE})

egen mean_usage1 = mean(mnth_bill_kwh) ${if_sample}, by(sp_uuid)
egen mean_usage2 = mean(mean_usage1), by(sp_uuid)
egen tag = tag(sp_uuid)
sum mean_usage2 if tag, detail
gen mean_usage_high = mean_usage2>r(p50)

ivreghdfe $DEPVAR $RHS $if_sample & dumb_smart_switchers==1, absorb(${FEs}#mean_usage_high) cluster(${VCE})
ivreghdfe $DEPVAR $RHS $if_sample & dumb_smart_switchers==1, absorb(${FEs}#mean_usage_high#rt_large_ag) cluster(${VCE})
ivreghdfe $DEPVAR $RHS $if_sample , absorb(${FEs}#mean_usage_high#rt_large_ag) cluster(${VCE})
ivreghdfe $DEPVAR $RHS $if_sample , absorb(${FEs}#mean_usage_high#rt_large_ag#dumb_smart_switchers) cluster(${VCE})
ivreghdfe $DEPVAR $RHS $if_sample , absorb(${FEs}#dumb_smart_switchers) cluster(${VCE})
ivreghdfe $DEPVAR $RHS $if_sample & dumb_smart_switchers_bad==0, absorb(${FEs}#dumb_smart_switchers) cluster(${VCE})
ivreghdfe log1_100kwh $RHS $if_sample & dumb_smart_switchers_bad==0, absorb(${FEs}#dumb_smart_switchers) cluster(${VCE})

gen ihs_kwh0 =  ln(mnth_bill_kwh + sqrt((mnth_bill_kwh)^2+1))

ivreghdfe ihs_kwh0 $RHS $if_sample , absorb(${FEs}) cluster(${VCE})
ivreghdfe ihs_kwh0 $RHS $if_sample , absorb(${FEs}#dumb_smart_switchers) cluster(${VCE})
ivreghdfe ihs_kwh0 $RHS $if_sample & dumb_smart_switchers_bad==0, absorb(${FEs}#dumb_smart_switchers) cluster(${VCE})

gen mnth_bill_kwh_dumb = mnth_bill_kwh * inlist(rt_sched_cd,"AG-1A","AG-1B")
gen mnth_bill_kwh_ice = mnth_bill_kwh * inlist(rt_sched_cd,"AG-ICE")
gen mnth_bill_kwh_smart = mnth_bill_kwh * inlist(rt_sched_cd,"AG-1A","AG-1B","AG-ICE")==0

keep $if_sample
collapse (sum) mnth_bill_kwh*, by(modate county_group county_fips) fast

egen county_group2 = group(county_fips)
sort county_group2 modate

gen d_ds_ratio = mnth_bill_kwh_dumb / (mnth_bill_kwh_dumb + mnth_bill_kwh_smart)

twoway ///
	(line d_ds_ratio modate if county_group2==1) ///
	(line d_ds_ratio modate if county_group2==2) ///
	(line d_ds_ratio modate if county_group2==3) ///
	(line d_ds_ratio modate if county_group2==4) ///
	(line d_ds_ratio modate if county_group2==5) ///
	(line d_ds_ratio modate if county_group2==6) ///
	(line d_ds_ratio modate if county_group2==7) ///
	(line d_ds_ratio modate if county_group2==8) ///
	(line d_ds_ratio modate if county_group2==9) ///
	(line d_ds_ratio modate if county_group2==10) ///
	(line d_ds_ratio modate if county_group2==11) ///
	(line d_ds_ratio modate if county_group2==12) ///
	(line d_ds_ratio modate if county_group2==13) ///
	(line d_ds_ratio modate if county_group2==14) ///
	(line d_ds_ratio modate if county_group2==15) ///
	(line d_ds_ratio modate if county_group2==16) ///
	(line d_ds_ratio modate if county_group2==17) ///
	(line d_ds_ratio modate if county_group2==18) ///
	(line d_ds_ratio modate if county_group2==19) ///
	(line d_ds_ratio modate if county_group2==20) ///
	(line d_ds_ratio modate if county_group2==21) ///
	(line d_ds_ratio modate if county_group2==22) ///
	(line d_ds_ratio modate if county_group2==23) ///
	(line d_ds_ratio modate if county_group2==24) ///
	(line d_ds_ratio modate if county_group2==25) ///
	(line d_ds_ratio modate if county_group2==26) ///
	(line d_ds_ratio modate if county_group2==27) ///
	(line d_ds_ratio modate if county_group2==28) ///
	(line d_ds_ratio modate if county_group2==29) ///
	(line d_ds_ratio modate if county_group2==30) ///
	(line d_ds_ratio modate if county_group2==31) ///
	(line d_ds_ratio modate if county_group2==32) ///
	(line d_ds_ratio modate if county_group2==33) 

