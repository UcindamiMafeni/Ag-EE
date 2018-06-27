

*** LEFTOVER CODE FOR DE-DUPIFYING METERS AFTER MERGING INTO BILLING DATA

	// Resolve duplicates using xwalk (based on bill start/end dates)
unique sa_uuid sp_uuid if merge_billing_customer==3
local uniq = r(unique)
gen temp_to_keep = _merge==3 & ///
	(inrange(bill_start_dt,mtr_install_date,mtr_remove_date) | ///
	inrange(bill_end_dt,mtr_install_date,mtr_remove_date))
egen temp_to_keep_max = max(temp_to_keep) if merge_billing_customer==3 & _merge==3, ///
	by(sa_uuid sp_uuid bill_start_dt)
drop if temp_to_keep==0 & temp_to_keep_max==1
unique sa_uuid sp_uuid bill_start_dt if merge_billing_customer==3
assert `uniq'==r(unique)

	// Diagnose remaining duplicates using xwalk (based on bill start/end dates)
duplicates t sa_uuid sp_uuid bill_start_dt, gen(dup)
tab dup if _merge==3 & merge_billing_customer==3
sort sa_uuid sp_uuid bill_start_dt 
br if dup>0 & _merge==3 & merge_billing_customer==3
	// Remaining dups are meters with install/repalcement dates that straddle a bill
	// Keep these dupes for now
	
	// Diagnose dups that did not merge into billing data 
tab dup	if merge_billing_customer==2 & _merge==3
br if dup>0 & merge_billing_customer==2 & _merge==3	
	// Remainign dups have SA/SP start/stop dates that partially overlap meter dates
	// Keep these dupes for now
drop dup temp*	
	
	// Append meter IDs that got dropped, to maximize chances of merging into APEP data
preserve
keep pge_badge_nbr
duplicates drop
merge 1:m pge_badge_nbr using "$dirpath_data/pge_cleaned/xwalk_sp_meter_date.dta", ///
	keep(2)
duplicates drop
tempfile meters
save `meters'
restore	
append using `meters'
