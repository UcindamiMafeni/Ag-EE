clear all
version 13
set more off

**********************************************************************
**** Script to assign SPs and APEP pump daily min/max temperatures ***
**********************************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"
global dirpath_code "T:/Home/Louis/backup/AgEE/AgEE_code/build"
*global R_exe_path "C:/PROGRA~1/MIE74D~1/ROPEN~1/bin/x64/R"
*global R_lib_path "C:/Program Files/Microsoft/R Open/R-3.4.4/library"

*******************************************************************************
*******************************************************************************

** 1. Run auxilary GIS script "BUILD_prism_daily_temperature.R", which downloads
**    PRISM daily rasters for precip, max temp, and min temp, and then plunks
**    PGE SP points, APEP pump points, and SCE SP points into them

*******************************************************************************
*******************************************************************************

** 2. Construct daily temperatures for PGE SP coordinates
if 1==0{

** Loop over sample months
forvalues y = 2008/2019 {
	forvalues m = 1/12 {
			
		** Import results from GIS script
		local ym = "`y'" + substr("0" + "`m'",-2,2)
		insheet using "$dirpath_data/prism/temp/pge_prem_coord_daily_temperatures_`ym'.csv", double comma clear
		drop v1
		duplicates drop
		
		** Destring precip and temperatures
		cap replace ppt = "" if ppt=="NA"
		cap replace tmax = "" if tmax=="NA"
		cap replace tmin = "" if tmin=="NA"
		destring ppt tmax tmin, replace 
		rename ppt precip_mm
		rename tmax degreesC_max 
		rename tmin degreesC_min 
				
		**Reformat date
		gen temp = date(substr(string(date,"%12.0g"),1,4) + "/" + ///
						substr(string(date,"%12.0g"),5,2) + "/" + ///
						substr(string(date,"%12.0g"),7,2),"YMD")
		format %td temp
		assert temp!=.
		drop date
		rename temp date

		** Confirm uniqueness
		unique sp_uuid date 
		assert r(unique)==r(N)

		** Clean SP variable
		tostring sp_uuid, replace
		replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
		replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
		replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
		replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
		replace sp_uuid = "0" + sp_uuid if length(sp_uuid)<10
		assert length(sp_uuid)==10 & real(sp_uuid)!=.
		unique sp_uuid date
		assert r(unique)==r(N)

		** Label
		la var sp_uuid "Unique service point identifier"	
		la var date "Date"	
		la var precip_mm "Daily precipitation (mm) at premise"
		la var degreesC_max "Daily max temperature (C) at premise"
		la var degreesC_min "Daily min temperature (C) at premise"

		** Sort and save
		sort sp_uuid date
		order sp_uuid date
		compress
		save "$dirpath_data/prism/temp_daily/pge_sp_temperature_daily_`ym'.dta", replace	
	}
}
		
}		
		
*******************************************************************************
*******************************************************************************
		
** 3. Construct daily temperatures for APEP pump coordinates
if 1==0{

** Loop over sample months
forvalues y = 2008/2019 {
	forvalues m = 1/12 {
			
		** Import results from GIS script
		local ym = "`y'" + substr("0" + "`m'",-2,2)
		insheet using "$dirpath_data/prism/temp/apep_pump_coord_daily_temperatures_`ym'.csv", double comma clear
		drop v1
		duplicates drop
		
		** Destring precip and temperatures
		cap replace ppt = "" if ppt=="NA"
		cap replace tmax = "" if tmax=="NA"
		cap replace tmin = "" if tmin=="NA"
		destring ppt tmax tmin, replace 
		rename ppt precip_mm
		rename tmax degreesC_max 
		rename tmin degreesC_min 
				
		**Reformat date
		gen temp = date(substr(string(date,"%12.0g"),1,4) + "/" + ///
						substr(string(date,"%12.0g"),5,2) + "/" + ///
						substr(string(date,"%12.0g"),7,2),"YMD")
		format %td temp
		assert temp!=.
		drop date
		rename temp date

		** Confirm uniqueness
		destring latlon_group, replace
		assert latlon_group!=.
		unique latlon_group date
		assert r(unique)==r(N)

		** Label
		la var latlon_group "APEP pump location identifier"	
		la var date "Date"	
		la var precip_mm "Daily precipitation (mm) at premise"
		la var degreesC_max "Daily max temperature (C) at premise"
		la var degreesC_min "Daily min temperature (C) at premise"

		** Sort and save
		sort latlon_group date
		order latlon_group date
		compress
		save "$dirpath_data/prism/temp_daily/apep_pump_temperature_daily_`ym'.dta", replace	
	}
}
		
}		
		
*******************************************************************************
*******************************************************************************
	
** 4. Construct daily temperatures for SCE SP coordinates
if 1==0{

** Loop over sample months
forvalues y = 2008/2019 {
	forvalues m = 1/12 {
			
		** Import results from GIS script
		local ym = "`y'" + substr("0" + "`m'",-2,2)
		insheet using "$dirpath_data/prism/temp/sce_prem_coord_daily_temperatures_`ym'.csv", double comma clear
		drop v1
		duplicates drop
		
		** Destring precip and temperatures
		cap replace ppt = "" if ppt=="NA"
		cap replace tmax = "" if tmax=="NA"
		cap replace tmin = "" if tmin=="NA"
		destring ppt tmax tmin, replace 
		rename ppt precip_mm
		rename tmax degreesC_max 
		rename tmin degreesC_min 
				
		**Reformat date
		gen temp = date(substr(string(date,"%12.0g"),1,4) + "/" + ///
						substr(string(date,"%12.0g"),5,2) + "/" + ///
						substr(string(date,"%12.0g"),7,2),"YMD")
		format %td temp
		assert temp!=.
		drop date
		rename temp date

		** Confirm uniqueness
		tostring sp_uuid, replace
		assert real(sp_uuid)!=.
		unique sp_uuid date
		assert r(unique)==r(N)

		** Label
		la var sp_uuid "Unique service point identifier"	
		la var date "Date"	
		la var precip_mm "Daily precipitation (mm) at premise"
		la var degreesC_max "Daily max temperature (C) at premise"
		la var degreesC_min "Daily min temperature (C) at premise"

		** Sort and save
		sort sp_uuid date
		order sp_uuid date
		compress
		save "$dirpath_data/prism/temp_daily/sce_sp_temperature_daily_`ym'.dta", replace	
	}
}
		
}		
		
*******************************************************************************
*******************************************************************************
	
** 5. Append to yearly dtas for daily temperatures		
if 1==0{

// Loop over 3 types of units
foreach id in pge_sp apep_pump sce_sp {
	
	// Loop over sample years
	forvalues y = 2008/2019 {
		
		if inlist("`id'","pge_sp","sce_sp") {
			local uniq = "sp_uuid"
		}
		else if inlist("`id'","apep_pump") {
			local uniq = "latlon_group"
		}
	
		clear
		forvalues m = 1/12 {
			local ym = "`y'" + substr("0" + "`m'",-2,2)
			append using "$dirpath_data/prism/temp_daily/`id'_temperature_daily_`ym'.dta"
		}
		assert year(date)==`y'
		unique date
		unique `uniq' date
		assert r(unique)==r(N)
		sort `uniq' date
		compress
		save "$dirpath_data/prism/cleaned_daily/`id'_temperature_daily_`y'.dta", replace
	}
}	

}

*******************************************************************************
*******************************************************************************

** 6. Collapse daily dta to the monthly level
if 1==0{

// Loop over 3 types of units
foreach id in pge_sp apep_pump sce_sp {
	
	if inlist("`id'","pge_sp","sce_sp") {
		local uniq = "sp_uuid"
		local lab = "premise"
	}
	else if inlist("`id'","apep_pump") {
		local uniq = "latlon_group"
		local lab = "pump"
	}

	// Collapse daily data to month, by year
	forvalues y = 2008/2019 {
		use "$dirpath_data/prism/cleaned_daily/`id'_temperature_daily_`y'.dta", clear
		gen modate = ym(year(date),month(date))
		format %tm modate
		gen degreesC_mean = (degreesC_max + degreesC_min) / 2
		collapse (sum) precip_mm (mean) degreesC_*, by(`uniq' modate) fast
		tempfile `id'`y'
		save ``id'`y''
	}
	
	// Append all yearly files, at monthly level
	clear
	forvalues y = 2008/2019 {
		append using ``id'`y''
	}
	
	// Label
	cap la var sp_uuid "Unique service point identifier"	
	cap la var latlon_group "APEP pump location identifier"	
	la var modate "Year-Month"	
	la var precip_mm "Total monthly precipitation (mm) at `lab'"
	la var degreesC_max "Avg daily max temperature (C) at `lab'"
	la var degreesC_min "Avg daily min temperature (C) at `lab'"
	la var degreesC_mean "Avg daily 'mean' temperature (C) at `lab'"

	// Save	
	sort `uniq' modate
	order `uniq' modate
	compress
	save "$dirpath_data/prism/`id'_temperature_monthly.dta", replace	

}	

}

*******************************************************************************
*******************************************************************************

** 7. Some quick diagnostics, to make sure the temperature build worked, and fix nonmissing precip
{

	// Confirm sensible values
use "$dirpath_data/prism/pge_sp_temperature_monthly.dta", clear
foreach v of varlist precip_mm degreesC* {
	sum `v', detail
	sum `v' if substr(string(modate,"%tm"),-2,2)=="m1", detail
	sum `v' if substr(string(modate,"%tm"),-2,2)=="m8", detail
	unique sp_uuid if `v'==.
}

use "$dirpath_data/prism/apep_pump_temperature_monthly.dta", clear
foreach v of varlist precip_mm degreesC* {
	sum `v', detail
	sum `v' if substr(string(modate,"%tm"),-2,2)=="m1", detail
	sum `v' if substr(string(modate,"%tm"),-2,2)=="m8", detail
	unique latlon_group if `v'==.
}

use "$dirpath_data/prism/sce_sp_temperature_monthly.dta", clear
foreach v of varlist precip_mm degreesC* {
	sum `v', detail
	sum `v' if substr(string(modate,"%tm"),-2,2)=="m1", detail
	sum `v' if substr(string(modate,"%tm"),-2,2)=="m8", detail
	unique sp_uuid if `v'==.
}

	// Convert precipitation from zero to missing if temperature is missing
foreach id in pge_sp apep_pump sce_sp {
	use "$dirpath_data/prism/`id'_temperature_monthly.dta", clear
	replace precip_mm = . if degreesC_max==.
	compress
	save "$dirpath_data/prism/`id'_temperature_monthly.dta", replace
}

}

*******************************************************************************
*******************************************************************************

** 8. Remove memory hogging files we no longer need
if 1==0{

	// Remove csv output from R raster script
cd "$dirpath_data/prism/temp"
local files : dir . files "*.csv"
foreach f in `files' {
	erase `f'
}	

	// Remove temporary daily dta
cd "$dirpath_data/prism/temp_daily"
local files : dir . files "*.dta"
foreach f in `files' {
	erase `f'
}	

	// Remove unzipped PRISM rasters

}

*******************************************************************************
*******************************************************************************

		
