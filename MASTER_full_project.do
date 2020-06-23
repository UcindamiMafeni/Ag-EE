clear all
version 13
set more off

******************************************************
**** MASTER DO FILE TO RUN ALL OTHER DOFILES *********
******************************************************

*global dirpath "T:/Projects/Pump Data"
*global dirpath_code "S:/Fiona/backup/Ag-EE"
*global dirpath_code "T:/Home/Louis/backup/AgEE/AgEE_code"

** additional directory paths
*global dirpath_data "$dirpath/data"

*********************************************************************************
*********************************************************************************

*** All project code is linearized below [direct dependencies are in brackets] 

************ BUILD (B) ************ BUILD (B) ************ BUILD (B) ************ 
************ BUILD (B) ************ BUILD (B) ************ BUILD (B) ************  

***
*** PGE.1-PGE.17 build the full set of PGE-exclusive datasets
***
{
*** B.PGE.1: IMPORT RAW PGE DATA TO STATA
do "$dirpath_code/build/PGE_Data_Build/BUILD_pge_import_data.do"

*** B.PGE.2: CREATE LIST OF NAICS CODES
do "$dirpath_code/build/PGE_Data_Build/BUILD_pge_naics_descr.do"

*** B.PGE.3: CLEAN PGE CUSTOMER DATA [B.PGE.1 B.PGE.2]
do "$dirpath_code/build/PGE_Data_Build/BUILD_pge_clean_customer_data.do"
	// calls auxilary GIS scripts "BUILD_pge_gis_climate_zone_20180322.R" 
	//                        and "BUILD_pge_gis_climate_zone_20180719.R"
	//                        and "BUILD_pge_gis_climate_zone_20180827.R"

*** B.PGE.4: CLEAN PGE BILLING DATA [B.PGE.1]
do "$dirpath_code/build/PGE_Data_Build/BUILD_pge_clean_bill_data.do"

*** B.PGE.5: CLEAN PGE ENERGY EFFICIENCY DATA [PGE.1]
do "$dirpath_code/build/PGE_Data_Build/BUILD_pge_clean_energy_efficiency_data.do"

*** B.PGE.6: CLEAN PGE INTERVAL DATA [B.PGE.1]
do "$dirpath_code/build/PGE_Data_Build/BUILD_pge_clean_interval_data.do"

*** B.PGE.7: CLEAN PGE METER BADGE NUMBER DATA [B.PGE.1]
do "$dirpath_code/build/PGE_Data_Build/BUILD_pge_clean_badge_number_data.do"

*** B.PGE.8: CROSS-VALIDATE PGE BILLING VS. INTERVAL DATA [B.PGE.4 B.PGE.6]
do "$dirpath_code/build/PGE_Data_Build/BUILD_pge_compare_billing_interval.do" 

*** B.PGE.9: CROSS-VALIDATE PGE BILLING VS. CUSTOMER DATA [B.PGE.3 B.PGE.8]
do "$dirpath_code/build/PGE_Data_Build/BUILD_pge_compare_billing_customer.do"

*** B.PGE.10: CROSS-VALIDATE PGE CUSTOMER VS. BILLING/INTERVAL DATA [B.PGE.3 B.PGE.6 B.PGE.9]
do "$dirpath_code/build/PGE_Data_Build/BUILD_pge_compare_customer_usage.do"

*** B.PGE.11: CROSS-VALIDATE PGE CUSTOMER VS. EE DATA [B.PGE.5 B.PGE.10]
do "$dirpath_code/build/PGE_Data_Build/BUILD_pge_compare_customer_ee.do"

*** B.PGE.12: CROSS-VALIDATE PGE CUSTOMER VS. METER HISTORY DATA, CREATE XWALK [B.PGE.7 B.PGE.11]
do "$dirpath_code/build/PGE_Data_Build/BUILD_pge_compare_customer_meter.do"

*** B.PGE.13: MONTHIFY PGE BILLING DATA [B.PGE.9]
do "$dirpath_code/build/PGE_Data_Build/BUILD_pge_monthify_billing_data.do"

*** B.PGE.14: CLEAN PGE PUMP TEST DATA [B.PGE.1]
do "$dirpath_code/build/PGE_Data_Build/BUILD_pge_clean_pump_test_data.do"

*** B.PGE.15: CLEAN PGE PUMP TEST PROJECT DATA [B.PGE.1 B.PGE.14]
do "$dirpath_code/build/PGE_Data_Build/BUILD_pge_clean_pump_test_project_data.do"

*** B.PGE.16: CLEAN PGE RATE DATA
do "$dirpath_code/build/PGE_Data_Build/BUILD_pge_clean_rate_data.do"

*** B.PGE.17: BUILD PGE EVENT DAY DATASET
do "$dirpath_code/build/PGE_Data_Build/BUILD_pge_event_days.do"
}
***
***


***
*** B.SCE.1-B.SCE.15 build the full set of SCE-exclusive datasets
***
{
*** B.SCE.1: IMPORT RAW SCE DATA TO STATA
do "$dirpath_code/build/SCE_Data_Build/BUILD_sce_import_data.do"

*** B.SCE.2: CLEAN SCE CUSTOMER ID CROSSWALK [B.SCE.1]
do "$dirpath_code/build/SCE_Data_Build/BUILD_sce_clean_customer_id_xwalk.do"

*** B.SCE.3: CLEAN SCE CUSTOMER DATA [B.PGE.2 B.SCE.1 B.SCE.2]
do "$dirpath_code/build/SCE_Data_Build/BUILD_sce_clean_customer_data.do"
	// calls auxilary GIS script "BUILD_sce_gis_climate_zone_20190916.R" 
	
*** B.SCE.4: CLEAN SCE BILLING DATA [B.SCE.1]
do "$dirpath_code/build/SCE_Data_Build/BUILD_sce_clean_bill_data.do"

*** B.SCE.5: CLEAN SCE ENERGY EFFICIENCY DATA [B.SCE.1]
do "$dirpath_code/build/SCE_Data_Build/BUILD_sce_clean_energy_efficiency_data.do"

*** B.SCE.6: CLEAN SCE INTERVAL DATA [B.SCE.1]
do "$dirpath_code/build/SCE_Data_Build/BUILD_sce_clean_interval_data.do"

*** B.SCE.7: CROSS-VALIDATE SCE BILLING VS. INTERVAL DATA [B.SCE.4 B.SCE.6]
do "$dirpath_code/build/SCE_Data_Build/BUILD_sce_compare_billing_interval.do" 

*** B.SCE.8: CROSS-VALIDATE SCE BILLING VS. CUSTOMER DATA [B.SCE.3 B.SCE.7]
do "$dirpath_code/build/SCE_Data_Build/BUILD_sce_compare_billing_customer.do"

*** B.SCE.9: CROSS-VALIDATE SCE CUSTOMER VS. BILLING/INTERVAL DATA [B.SCE.3 B.SCE.6 B.SCE.8]
do "$dirpath_code/build/SCE_Data_Build/BUILD_sce_compare_customer_usage.do"

*** B.SCE.10: CROSS-VALIDATE SCE CUSTOMER VS. EE DATA [B.SCE.5 B.SCE.9]
do "$dirpath_code/build/SCE_Data_Build/BUILD_sce_compare_customer_ee.do"

*** B.SCE.11: MONTHIFY SCE BILLING DATA [B.SCE.8]
do "$dirpath_code/build/SCE_Data_Build/BUILD_sce_monthify_billing_data.do"

*** B.SCE.12: CLEAN SCE PUMP TEST DATA [B.SCE.1]
do "$dirpath_code/build/SCE_Data_Build/BUILD_sce_clean_pump_test_data.do"

*** B.SCE.13: CLEAN SCE PUMP TEST PROJECT DATA [B.SCE.1 B.SCE.3 B.SCE.12]
do "$dirpath_code/build/PGE_Data_Build/BUILD_sce_clean_pump_test_project_data.do"

*** B.SCE.14: CLEAN PGE RATE DATA
do "$dirpath_code/build/PGE_Data_Build/BUILD_sce_clean_rate_data.do"
	// this still needs some work

}
***
***


***
*** B.GIS.1-B.GIS.?? build the full set of geographic datasets that rely on publicly available spatial data
***
{
*** B.GIS.1: CONVERT PARCEL SHAPEFILES TO USABLE FORMAT
// "GIS_Data_Build/BUILD_gis_parcel_conversion.R"

*** B.GIS.2: CLEAN PARCEL SHAPEFILES [B.GIS.1]
// "GIS_Data_Build/BUILD_gis_parcel_clean.R"
	
*** B.GIS.3: CLEAN COMMON LAND UNIT SHAPEFILES
// "GIS_Data_Build/BUILD_gis_clu_clean.R"

*** B.GIS.4: CREATE CONCORDANCE THAT OVERLAYS CLU & PARCEL SHAPEFILES [B.GIS.2 B.GIS.3]
// "GIS_Data_Build/BUILD_gis_clu_parcel_conc.R"
	
*** B.GIS.5: COOKIE-CUTTER CROPLAND DATA LAYER ANNUAL FOR EACH CLU [B.GIS.3]
// "GIS_Data_Build/BUILD_gis_clu_cdl_conc.R"
	// calls auxiliary scripts "BUILD_gis_clu_cdl_conc.py"
	//                     and "constants.R"

*** B.GIS.6: PROCESS TWO CONCORDANCES IN STATA [B.GIS.4 B.GIS.5]
do "$dirpath_code/build/GIS_Data_Build/BUILD_gis_process_concordances.do"
	// calls auxiliary script "BUILD_gis_export_clu_parcel_concs.R"
	
*** B.GIS.?: PROCESS CLU-PARCEL CONCORDANCE, AND AGGREGATE UNITS UP TO FIELDS AND FARMS [B23 B24]
do "$dirpath_code/build/BUILD_aggr_units_fields_farms.do"

*** B.GIS.?: CONSTRUCT ANNUAL PANEL OF CROPS AT THE FIELD AND FARM LEVELS [B23 B25]
do "$dirpath_code/build/BUILD_cdl_panel_crop_year.do"
		
*** B.GIS.?: CREATE CONCORDANCES BETWEEN CLUs: WATER BASINS, COUNTIES [B.GIS.4 B.GIS.5]
CHIN: polygon-to-polygon merge of CLU-to-county, CLU-to-water-basin	
}
***
***
	

***
*** B.SFW.1-B.SFW.?? build all surface water datasets
***
	
CHIN's linear code goes here
The last step of this code will be the polygon-to-polygon merge of CLUs to water districts
***
***
	
	
	
	
RERORGANIZE STARTING HERE	
	
	
	
*** B24: ASSIGN SPS AND APEP PUMPS TO VARIOUS POLYGONS [B11 B14 B19 B20 B23]
do "$dirpath_code/build/BUILD_assign_gis_polygons.do"
	// calls auxilary GIS scripts "BUILD_gis_water_districts.R" 
	//                        and "BUILD_gis_counties.R"
	//                        and "BUILD_gis_water_basins.R"
	//                        and "BUILD_gis_parcel_assign.R"
	//                        and "BUILD_gis_clu_assign.R"


*** B27: ASSIGN DAILY MIN/MAX TEMPERATURE TO EACH SP AND APEP PUMP [B24]
do "$dirpath_code/build/BUILD_daily_temperatures.do"
	// calls auxilary GIS scripts "BUILD_prism_daily_temperature.R" 

*** B28: CLEAN CA DWR GROUNDWATER DATA [B24]
do "$dirpath_code/build/BUILD_clean_cadwr_groundwater.do"
	// calls auxilary GIS scripts "BUILD_gis_gw_depth_raster.R" 
	//                        and "BUILD_gis_gw_depth_extract.R"

	

*** B??: CLEAN AND COMBINE SURFACE WATER DATA
do "$dirpath_code/build/???"

*** B??: CLEAN CA WELL COMPLETION REPORT DATA 
do "$dirpath_code/build/BUILD_clean_well_completion.do"
	
	
	

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

*** M5: CONSTRUCT PANEL DATASETS FOR ELECTRICITY REGRESSIONS [B12 B13 B25 B27 B28 M1 M3 M4]
do "$dirpath_code/merge/MERGE_analysis_elec_regs.do"

*** M6: MERGE CUSTOMER DETAILS & APEP DATASETS TO CONSTRUCT MASTER XSECTION(S) [B9 B11 B12 B14 B15 B25 B28]
do "$dirpath_code/merge/MERGE_customer_apep_units.do"

*** M7: CONSTRUCT PANEL OF KWH/AF CONVERSION RATES [B14 B25 B28 M6]
do "$dirpath_code/merge/MERGE_panel_kwhaf.do"

*** M8: CONSTRUCT PANEL DATASETS FOR WATER REGRESSIONS [B25 B28 M5 M7]
do "$dirpath_code/merge/MERGE_analysis_water_regs.do"




*************** ANALYZE ****************
