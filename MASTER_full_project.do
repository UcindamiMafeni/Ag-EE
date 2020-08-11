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
*** B.PGE.1-B.PGE.17 build the full set of PGE-exclusive datasets
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

*** B.SCE.13: CLEAN SCE PUMP TEST PROJECT DATA [B.SCE.1 B.SCE.10 B.SCE.12]
do "$dirpath_code/build/SCE_Data_Build/BUILD_sce_clean_pump_test_project_data.do"

*** B.SCE.14: CLEAN SCE RATE DATA
do "$dirpath_code/build/SCE_Data_Build/BUILD_sce_clean_rate_data.do"
	// this still needs some work

*** B.SCE.15: SCE RATE DIAGNOSTICS [B.SCE.8 B.SCE.10 B.SCE.12]
do "$dirpath_code/build/SCE_Data_Build/BUILD_sce_tariff_diagnostics.do"

}
***
***
***


***
*** B.GIS.1-B.GIS.10 build the full set of geographic datasets that rely on publicly available spatial data
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
	// calls auxiliary script "BUILD_gis_clu_cdl_conc.py"

*** B.GIS.6: PROCESS TWO CONCORDANCES IN STATA [B.GIS.4 B.GIS.5]
do "$dirpath_code/build/GIS_Data_Build/BUILD_gis_process_concordances.do"
	// calls auxiliary script "BUILD_gis_export_clu_parcel_concs.R"
	
*** B.GIS.7: PROCESS CLU-PARCEL CONCORDANCE, AND AGGREGATE CLUs UP TO PARCELS [B.GIS.6]
do "$dirpath_code/build/GIS_Data_Build/BUILD_gis_aggr_clus_parcels.do"

*** B.GIS.8: CONSTRUCT ANNUAL PANEL OF CROPS AT THE FIELD AND FARM LEVELS [B.GIS.6 B.GIS.7]
do "$dirpath_code/build/GIS_Data_Build/BUILD_gis_cdl_panel_crop_year.do"
		
*** B.GIS.9: CREATE CONCORDANCES BETWEEN CLUs and {COUNTIES; WATER BASINS; WATER DISTRICTS} [B.GIS.3]
do 	"$dirpath_code/build/GIS_Data_Build/BUILD_gis_process_clu_joins.do"
	// calls auxiliary scripts "BUILD_gis_clu_counties.R"
	//                     and "BUILD_gis_clu_basins.R"
	//                     and "BUILD_gis_clu_water_districts.R"

*** B.GIS.10: CLEAN CA WELL COMPLETION REPORT DATA, AND ASSIGN WELS TO CLUS [B.GIS.3]
do "$dirpath_code/build/GIS_Data_Build/BUILD_gis_clean_well_completion.do"
	// calls auxiliary script "BUILD_gis_clu_wells.R"

}
***
***
***
	

***
*** B.SFW.1-B.SFW.?? build all surface water datasets
***

* CREATE SUBSET OF SHAPEFILES
* run script "$dirpath_code/build/GIS_Data_Build/BUILD_Subset_Shapef_Wallocations.R"
	
* ASSIGN ALLOCATIONS TO CLUS
* run script "$dirpath_code/build/GIS_Data_Build/BUILD_Hagerty_CLU_assignment_revised.R"

* CHECK THE AREA INTERSECTIONS AND GET SAMPLE EXHIBITS
* run script "$dirpath_code/build/GIS_Data_Build/BUILD_Check_CLU_UserFile_Intersections.R"

***
***
***
	
***
*** B.COM.1-B.COM.6 combine PGE and SCE data with other (mostly geographic) datasets
***
{	
*** B.COM.1: ASSIGN LAT/LONS OF PGE SPS, APEP PUMPS, AND SCE SPS TO CLU POLYGONS [B.PGE.11 B.PGE.14 B.SCE.10 B.GIS.3 B.GIS.6]
do "$dirpath_code/build/Combined_Data_Build/BUILD_com_points_to_clu_polygons.do"
	// calls auxilary GIS script "BUILD_com_clu_assign.R"

*** B.COM.2: USE CLU ASSIGNMENTS TO LINK PGE SPS, APEP PUMPS, AND SCE SPS TO OTHER POLYGONS [B.GIS.6 B.GIS.9 B.COM.1]
do "$dirpath_code/build/Combined_Data_Build/BUILD_com_assign_other_polygons.do"
	// calls auxilary GIS scripts "BUILD_com_points_in_counties.R"	
	//                        and "BUILD_com_points_in_water_basins.R"
	
*** B.COM.3: CLU/PARCEL DIAGNOSTICS AND GROUP ASSIGNMENTS [B.GIS.7 B.COM.2]
do "$dirpath_code/build/Combined_Data_Build/BUILD_com_assign_clu_parcel_groups.do"

*** B.COM.4: ASSIGN UNITS TO SURFACE WATER POLYGONS, BY CLU AND POINT-WISE [B.SFW.?? B.COM.1 B.COM.3]
do "$dirpath_code/build/Combined_Data_Build/BUILD_com_assign_water_districts.do"
	// calls auxilary GIS scripts "BUILD_com_clu_water_districts.R"	
	//                        and "BUILD_com_points_in_water_districts.R"
	
*** B.COM.5: ASSIGN DAILY MIN/MAX TEMPERATURE AND PRECITIPATION TO EACH PGE SP, APEP PUMP, SCE SP, CLU CENTROID [B.COM.1]
do "$dirpath_code/build/Combined_Data_Build/BUILD_com_daily_temperatures.do"
	// calls auxilary GIS scripts "BUILD_com_prism_daily_temperature.R" 

*** B.COM.6: CLEAN CA DWR GROUNDWATER DATA; CREATE MONTHLY PANELS (PGE SP, APEP PUMP, SCE SP, CLU CENTROID) [B.COM.1 B.COM.5]
do "$dirpath_code/build/Combined_Data_Build/BUILD_clean_cadwr_groundwater.do"
	// calls auxilary GIS scripts "BUILD_com_gw_depth_raster.R" 
	//                        and "BUILD_com_gw_depth_extract.R"
}

***
***
***
	

************ MERGE (M) ************ MERGE (M) ************ MERGE (M) ************ 
************ MERGE (M) ************ MERGE (M) ************ MERGE (M) ************  

***
*** M.PGE.1-M.PGE.8 build the full set of PGE-exclusive datasets
***

*** M.PGE.1: MERGE BILLING/INTERVAL DATA WITH RATE DATA [B.PGE.6 B.PGE.9 B.PGE.16 B.PGE.17]
do "$dirpath_code/merge/merge_PGE/MERGE_pge_bills_interval_rates.do"

*** M.PGE.2: ASSIGN AVERAGE PRICES TO BILLS WITHOUT AMI DATA (EXTRAPOLATION) [B.PGE.9 M.PGE.1]
do "$dirpath_code/merge/merge_PGE/MERGE_pge_bills_noninterval_rates.do"

*** M.PGE.3: ASSIGN AVERAGE PRICES TO ALL (MONTHIFIED) BILLS (INTERNALLY CONSISTENT) [B.PGE.9 B.PGE.16 B.PGE.17]
do "$dirpath_code/merge/merge_PGE/MERGE_pge_bills_rates_nomerge.do"

*** M.PGE.4: CONSTRUCT INSTRUMENTS FOR ELECTRICITY PRICE [B.PGE.16 B.PGE.17 M.PGE.1 M.PGE.3]
do "$dirpath_code/merge/merge_PGE/MERGE_pge_instruments.do"

*** M.PGE.5: CONSTRUCT MONTHLY/HOURLY PANEL DATASETS FOR ELECTRICITY REGRESSIONS [B.PGE.9 B.PGE.12 B.PGE.13 B.GIS.8 B.COM.4 B.COM.5 B.COM.6 M.PGE.1 M.PGE.3 M.PGE.4]
do "$dirpath_code/merge/merge_PGE/MERGE_pge_analysis_elec_regs.do"

*** M.PGE.6: MERGE CUSTOMER DETAILS & APEP DATASETS TO CONSTRUCT MASTER XSECTION(S) [B.PGE.9 B.PGE.11 B.PGE.12 B.PGE.14 B.PGE.15 B.COM.4 B.COM.6]
do "$dirpath_code/merge/merge_PGE/MERGE_pge_customer_apep_units.do"

*** M.PGE.7: CONSTRUCT MONTHLY PANEL OF KWH/AF CONVERSION RATES [B.PGE.14 B.COM.4 B.COM.6 M.PGE.6]
do "$dirpath_code/merge/merge_PGE/MERGE_pge_panel_kwhaf.do"

*** M.PGE.8: CONSTRUCT MONTHLY PANEL DATASETS FOR WATER REGRESSIONS [B.COM.4 B.COM.6 M.PGE.5 M.PGE.7]
do "$dirpath_code/merge/merge_PGE/MERGE_pge_analysis_water_regs.do"

*** M.PGE.9: COLLAPSE MONTHLY PANEL DATASETS TO ANNUAL LEVEL [M.PGE.8]
do "$dirpath_code/merge/merge_PGE/MERGE_pge_collapsed_analysis_regs.do"



*************** ANALYZE ****************
