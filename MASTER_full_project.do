clear all
version 13
set more off

******************************************************
**** MASTER DO FILE TO RUN ALL OTHER DOFILES *********
******************************************************

global dirpath "T:/Projects/Pump Data"
*global dirpath_code "S:/Fiona/backup/Ag-EE"
global dirpath_code "T:Home/Louis/backup/AgEE/AgEE_code"

** additional directory paths
global dirpath_data "$dirpath/data"

*********************************************************************************
*********************************************************************************

*** All project code is linearized below [direct dependencies are in brackets] 

************ BUILD (B) ************ BUILD (B) ************ BUILD (B) ************ 
************ BUILD (B) ************ BUILD (B) ************ BUILD (B) ************  

*** B1: IMPORT DATA TO STATA
do "$dirpath_code/build/BUILD_import_data.do"

*** B2: CREATE LIST OF NAICS CODES
do "$dirpath_code/build/BUILD_naics_descr.do"

*** B3: CLEAN CUSTOMER DATA [B1 B2]
do "$dirpath_code/build/BUILD_clean_customer_data.do"
	// calls auxilary GIS scripts "BUILD_gis_climate_zone_20180322.R" 
	//                        and "BUILD_gis_climate_zone_20180719.R"
	//                        and "BUILD_gis_climate_zone_20180827.R"

*** B4: CLEAN BILLING DATA [B1]
do "$dirpath_code/build/BUILD_clean_bill_data.do"

*** B5: CLEAN ENERGY EFFICIENCY DATA [B1]
do "$dirpath_code/build/BUILD_clean_energy_efficiency_data.do"

*** B6: CLEAN INTERVAL DATA [B1]
do "$dirpath_code/build/BUILD_clean_interval_data.do"

*** B7: CLEAN METER BADGE NUMBER DATA [B1]
do "$dirpath_code/build/BUILD_clean_badge_number_data.do"

*** B8: CROSS-VALIDATE BILLING VS. INTERVAL DATA [B4 B6]
do "$dirpath_code/build/BUILD_compare_billing_interval.do" 

*** B9: CROSS-VALIDATE BILLING VS. CUSTOMER DATA [B3 B8]
do "$dirpath_code/build/BUILD_compare_billing_customer.do"

*** B10: CROSS-VALIDATE CUSTOMER VS. BILLING/INTERVAL DATA [B3 B6 B9]
do "$dirpath_code/build/BUILD_compare_customer_usage.do"

*** B11: CROSS-VALIDATE CUSTOMER VS. EE DATA [B5 B10]
do "$dirpath_code/build/BUILD_compare_customer_ee.do"

*** B12: CROSS-VALIDATE CUSTOMER VS. METER HISTORY DATA, CREATE XWALK [B7 B11]
do "$dirpath_code/build/BUILD_compare_customer_meter.do"

*** B13: MONTHIFY BILLING DATA [B9]
do "$dirpath_code/build/BUILD_monthify_billing_data.do"

*** B14: CLEAN PUMP TEST DATA [B1]
do "$dirpath_code/build/BUILD_clean_pump_test_data.do"

*** B15: CLEAN PUMP TEST PROJECT DATA [B1 B14]
do "$dirpath_code/build/BUILD_clean_pump_test_project_data.do"

*** B16: CLEAN PGE RATE DATA
do "$dirpath_code/build/BUILD_clean_rate_data.do"

*** B17: BUILD EVENT DAY DATASET
do "$dirpath_code/build/BUILD_event_days.do"

*** B18: CONVERT PARCEL SHAPEFILES TO USABLE FORMAT
// "BUILD_parcel_conversion.R"

*** B19: CLEAN PARCEL SHAPEFILES [B18]
// "BUILD_parcel_clean.R"
	
*** B20: CLEAN COMMON LAND UNIT SHAPEFILES
// "BUILD_clu_clean.R"

*** B21: CREATE CONCORDANCE THAT OVERLAYS CLU & PARCEL SHAPEFILES [B19 B20]
// "BUILD_gis_clu_parcel_conc.R"
	
*** B22: COOKIE-CUTTER CROPLAND DATA LAYER ANNUAL FOR EACH CLU [B20]
// "BUILD_gis_clu_cdl_conc.R"
	// calls auxiliary scripts "BUILD_gis_clu_cdl_conc.py"
	//                     and "constants.R"

*** B23: ASSIGN SPS AND APEP PUMPS TO VARIOUS POLYGONS [B11 B14 B19 B20 B21]
do "$dirpath_code/build/BUILD_assign_gis_polygons.do"
	// calls auxilary GIS scripts "BUILD_gis_water_districts.R" 
	//                        and "BUILD_gis_counties.R"
	//                        and "BUILD_gis_water_basins.R"
	//                        and "BUILD_gis_parcel_assign.R"
	//                        and "BUILD_gis_clu_assign.R"

*** B24: PROCESS CLU-PARCEL CONCORDANCE, AND AGGREGATE UNITS UP TO FIELDS AND FARMS [B21 B23]
do "$dirpath_code/build/BUILD_aggr_units_fields_farms.do"
	// calls auxiliary scripts "BUILD_export_clu_parcel_conc.R"

*** B25: PROCESS CLU-CDL CONCORDANCE, AND CONSTRUCT ANNUAL PANEL OF CROPS [B22 B23 B24]
do "$dirpath_code/build/BUILD_cdl_panel_crop_year.do"
	// calls auxiliary scripts "BUILD_export_clu_cdl_conc.R"

*** B26: ASSIGN DAILY MIN/MAX TEMPERATURE TO EACH SP AND APEP PUMP [B23]
do "$dirpath_code/build/BUILD_daily_temperatures.do"
	// calls auxilary GIS scripts "BUILD_prism_daily_temperature.R" 

*** B27: CLEAN CA DWR GROUNDWATER DATA [B23]
do "$dirpath_code/build/BUILD_clean_cadwr_groundwater.do"
	// calls auxilary GIS scripts "BUILD_gis_gw_depth_raster.R" 
	//                        and "BUILD_gis_gw_depth_extract.R"


	

************ MERGE (M) ************ MERGE (M) ************ MERGE (M) ************ 
************ MERGE (M) ************ MERGE (M) ************ MERGE (M) ************  

*** M1: MERGE BILLING/INTERVAL DATA WITH RATE DATA [B8 B9 B16 B17]
do "$dirpath_code/merge/MERGE_bills_interval_rates.do"

*** M2: ASSIGN AVERAGE PRICES TO BILLS WITHOUT AMI DATA (EXTRAPOLATION) [B9 M1]
do "$dirpath_code/merge/MERGE_bills_noninterval_rates.do"

*** M3: ASSIGN AVERAGE PRICES TO ALL (MONTHIFIED) BILLS (INTERNALLY CONSISTENT) [B9 B16 B17]
do "$dirpath_code/merge/MERGE_bills_rate_nomerge.do"

*** M4: CONSTRUCT INSTRUMENTS FOR ELECTRICITY PRICE [B16 B17 M1 M3]
do "$dirpath_code/merge/MERGE_instruments.do"

*** M5: CONSTRUCT PANEL DATASETS FOR ELECTRICITY REGRESSIONS [B12 B13 B23 B26 B27 M1 M3 M4]
do "$dirpath_code/merge/MERGE_analysis_elec_regs.do"

*** M6: MERGE CUSTOMER DETAILS & APEP DATASETS TO CONSTRUCT MASTER XSECTION(S) [B9 B11 B12 B14 B15 B23 B27]
do "$dirpath_code/merge/MERGE_customer_apep_units.do"

*** M7: CONSTRUCT PANEL OF KWH/AF CONVERSION RATES [B14 B23 B27 M6]
do "$dirpath_code/merge/MERGE_panel_kwhaf.do"

*** M8: CONSTRUCT PANEL DATASETS FOR WATER REGRESSIONS [B23 B27 M5 M7]
do "$dirpath_code/merge/MERGE_analysis_water_regs.do"





*************** ANALYZE ****************
