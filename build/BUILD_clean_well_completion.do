clear all
version 13
set more off

********************************************************
**** Script to clean California Well Completion Data ***
********************************************************

global dirpath "T:/Projects/Pump Data"
global dirpath_data "$dirpath/data"
global dirpath_code "T:/Home/Louis/backup/AgEE/AgEE_code/build"
*global R_exe_path "C:/PROGRA~1/MIE74D~1/ROPEN~1/bin/x64/R"
*global R_lib_path "C:/Program Files/Microsoft/R Open/R-3.4.4/library"

*******************************************************************************
*******************************************************************************

** 1. Clean raw WCR data
{

** Read in raw data from 2019 pull
insheet using "$dirpath_data/spatial/well completion reports/download_2019/OSWCR.csv", names double clear
unique wcrnumber
assert r(unique)==r(N)

** Merge with raw data from 2020 pull
preserve
insheet using "$dirpath_data/spatial/well completion reports/download_2020/OSWCR.csv", names double clear
rename * *2020
rename wcrnumber2020 wcrnumber
unique wcrnumber
assert r(unique)==r(N)
tempfile pull2020
save `pull2020'
restore
merge 1:1 wcrnumber using `pull2020'

** Unique identifier
unique wcrnumber
assert r(unique)==r(N)
la var wcrnumber "Unique WCR identifier"

** Legacy identifier
br legacylognumber* if legacylognumber!=legacylognumber2020 & _merge==3
replace legacylognumber = legacylognumber2020 if _merge==3 & ///
	!inlist(legacylognumber2020,"None","00000000")
replace legacylognumber = legacylognumber2020 if _merge==2
drop legacylognumber2020
unique legacylognumber
unique legacylognumber if legacylognumber!=""
sort legacylognumber
replace legacylognumber = "" if legacylognumber=="None"
replace legacylognumber = "" if legacylognumber=="NN"
replace legacylognumber = "" if legacylognumber=="n/a"
replace legacylognumber = "" if legacylognumber=="00000000"
replace legacylognumber = "" if legacylognumber=="00000 NN"
replace legacylognumber = "" if legacylognumber=="000000"
replace legacylognumber = "" if legacylognumber=="*******"
replace legacylognumber = "" if regexm(legacylognumber,"xxxx")
la var legacylognumber "Legacy DWR log number (a non-unique identifier)"

** Region office
br regionoffice* if regionoffice!=regionoffice2020 & _merge==3 // only 4
tab regionoffice*, missing
replace regionoffice = regionoffice2020 if regionoffice2020!=""
drop regionoffice2020
replace regionoffice = subinstr(regionoffice," Region Office","",1)
tab regionoffice, missing
rename regionoffice dwr_office
replace dwr_office = trim(itrim(dwr_office))
la var dwr_office "DWR region office"

** County
br countyname* if countyname!=countyname2020 & _merge==3 // only 14 
replace countyname = countyname2020 if countyname2020!=""
drop countyname2020
rename countyname county
tab county
replace county = "" if county=="None" | county=="Unknown"
replace county = "San Francisco" if substr(county,1,13)=="San Francisco"
replace county = trim(itrim(county))
la var county "County name"
compress

** Local permitting agency
br localpermitagency* if localpermitagency!=localpermitagency2020 & _merge==3
sort localpermitagency2020 
replace localpermitagency = localpermitagency2020 if !inlist(localpermitagency2020,"None","")
drop localpermitagency2020
tab localpermitagency
rename localpermitagency local_permit_authority
la var local_permit_authority "Local regulatory authority over well construction, alteration, destruction"

** Permit date
replace permitdate = "" if permitdate=="None"
replace permitdate2020 = "" if permitdate2020=="None"
sort permitdate*
br permitdate* _merge 
br permitdate* if permitdate!=permitdate2020 & _merge==3 // only 51, and most are "None"s
replace permitdate = permitdate2020 if permitdate2020!=""
drop permitdate2020
tab _merge
tab _merge if permitdate!="" // mostly missing, but most _merge==2's are nonmissing
gen temp = date(permitdate,"MDY")
assert temp!=. if permitdate!=""
order temp, after(permitdate)
drop permitdate
rename temp date_permit
format %td date_permit
la var date_permit "Well permit date (mostly missing)"

** Permit number
sort permitnumber* 
br permitnumber* _merge
br permitnumber* if permitnumber!=permitnumber2020 & _merge==3
replace permitnumber = permitnumber2020 if !inlist(permitnumber2020,"","0","N/A")
drop permitnumber2020
sort permitnumber
replace permitnumber = "" if permitnumber=="-"
replace permitnumber = "" if permitnumber=="0"
replace permitnumber = "" if permitnumber=="00"
replace permitnumber = "" if permitnumber=="000"
replace permitnumber = "" if permitnumber=="0000"
replace permitnumber = "" if permitnumber=="0000-000000"
replace permitnumber = "" if permitnumber=="00000"
replace permitnumber = "" if permitnumber=="NA"
replace permitnumber = "" if permitnumber=="N/A"
replace permitnumber = "" if permitnumber=="NA (BORING ONLY)"
replace permitnumber = "" if permitnumber=="NA(BORING ONLY)"
replace permitnumber = "" if permitnumber=="NO #"
replace permitnumber = "" if permitnumber=="NO NUMBER"
replace permitnumber = "" if permitnumber=="NO PERMIT"
replace permitnumber = "" if permitnumber=="NO PERMIT (DRUM)"
replace permitnumber = "" if permitnumber=="NO PERMIT ISSUED"
replace permitnumber = "" if permitnumber=="NO PERMIT NEEDED (AS PER)"
replace permitnumber = "" if permitnumber=="NO PERMIT NEEDED CALAVERAS CO"
replace permitnumber = "" if permitnumber=="NO PERMIT PER CARL"
replace permitnumber = "" if permitnumber=="NO PERMIT REG"
replace permitnumber = "" if permitnumber=="NO PERMIT REGD"
replace permitnumber = "" if permitnumber=="NO PERMIT REQUIRED"
replace permitnumber = "" if permitnumber=="NO REQUIRED"
replace permitnumber = "" if permitnumber=="NONE"
replace permitnumber = "" if permitnumber=="NONE 5/24/78"
replace permitnumber = "" if permitnumber=="NONE ASIGNED"
replace permitnumber = "" if permitnumber=="NONE ASS16NEO"
replace permitnumber = "" if permitnumber=="NONE GIVEN"
replace permitnumber = "" if permitnumber=="NONE ISSUED"
replace permitnumber = "" if permitnumber=="NONE NEEDED"
replace permitnumber = "" if permitnumber=="NONE REQ"
replace permitnumber = "" if permitnumber=="NONE REQUIRED"
replace permitnumber = "" if permitnumber=="NONE-9/17/77"
replace permitnumber = "" if permitnumber=="NOT KNOWN"
replace permitnumber = "" if permitnumber=="NOT NEEDED"
replace permitnumber = "" if permitnumber=="NOT REQUIRED"
replace permitnumber = "" if permitnumber=="No permit"
replace permitnumber = "" if permitnumber=="None"
replace permitnumber = "" if permitnumber=="None Required"
replace permitnumber = "" if permitnumber=="Not Require DWR Property"
replace permitnumber = "" if permitnumber=="Not required"
replace permitnumber = "" if permitnumber=="Number not issued"
sort permitnumber
replace permitnumber = trim(itrim(permitnumber))
la var permitnumber "Well permit number (mostly missing)"

** Owner-assigned well number
br ownerassigned* _merge
br ownerassigned* if ownerassignedwellnumber!=ownerassignedwellnumber2020 & _merge==3 // only 94
replace ownerassignedwellnumber = ownerassignedwellnumber2020 if ownerassignedwellnumber2020!=""
drop ownerassignedwellnumber2020
sort ownerassigned
replace ownerassigned = trim(itrim(ownerassigned))
replace ownerassigned = "" if upper(ownerassigned)=="N/A"
replace ownerassigned = "" if upper(ownerassigned)=="NA"
replace ownerassigned = "" if upper(ownerassigned)=="NONE"
*replace ownerassigned = "" if upper(word(ownerassigned,1))=="SAME"
la var ownerassigned "Owner assigned well number (mostly missing)"

** Well location
sort welllocation*
br welllocation* _merge
br welllocation* if welllocation!=welllocation2020 & _merge==3
replace welllocation = welllocation2020 if welllocation2020!=""
drop welllocation2020
replace welllocation = upper(trim(itrim(welllocation)))
replace welllocation = "" if welllocation=="NO"
replace welllocation = "" if welllocation=="NO ADDRESS"
replace welllocation = "" if welllocation=="NO ADDRESS - PASTURE"
replace welllocation = "" if welllocation=="NO ADDRESS - SEE MAP"
replace welllocation = "" if welllocation=="NO ADDRESS AS WELL"
replace welllocation = "" if welllocation=="NO ADDRESS ASSIGNED"
replace welllocation = "" if welllocation=="NO ADDRESS AVAILABLE"
replace welllocation = "" if welllocation=="NO ADDRESS ON FILE"
replace welllocation = "" if welllocation=="NO ADDRESS SEE LOCATION SKETCH"
replace welllocation = "" if welllocation=="NO ADDRESS SEE MAPS FOR LOCATION"
replace welllocation = "" if welllocation=="NO ADDRESS YET"
replace welllocation = "" if welllocation=="NO DESGINATED ADDRESS"
replace welllocation = "" if welllocation=="NO DESIGNED ADDRESS"
replace welllocation = "" if welllocation=="NO LOCATED"
replace welllocation = "" if welllocation=="NO MAILING ADDRESS"
replace welllocation = "" if welllocation=="NO PHYSICAL ADDRESS"
replace welllocation = "" if welllocation=="NO PHYSICAL ADDRESS/ALONG ADJACENT ROAD"
replace welllocation = "" if welllocation=="NO PHYSICAL ADDRESS/DIRT ROAD THROUGH SO"
replace welllocation = "" if welllocation=="NO PRESENT ADDRESS"
replace welllocation = "" if welllocation=="NO SITE ADDRESS"
replace welllocation = "" if welllocation=="NO SITUS"
replace welllocation = "" if welllocation=="NO SITUS ADDRESS"
replace welllocation = "" if welllocation=="NONE"
replace welllocation = "" if welllocation=="NONE ASSIGNED"
replace welllocation = "" if welllocation=="NONE AVAILABLE"
replace welllocation = "" if welllocation=="NONE SEE ATTACHED DWG"
replace welllocation = "" if welllocation=="NONE YET"
replace welllocation = "" if welllocation=="NONE, AG FIELDS"
replace welllocation = "" if welllocation=="NONE-DESTRUCTION PERMIT"
*replace welllocation = "" if welllocation=="SAM AS ABOVE"
*replace welllocation = "" if welllocation=="SAME"
*replace welllocation = "" if welllocation=="SAME ABOVE"
*replace welllocation = "" if welllocation=="SAME ADDRESS"
*replace welllocation = "" if welllocation=="SAME ADDRESS AS ABOVE"
*replace welllocation = "" if welllocation=="SAME ADDRESS AS ABOVE SEE MAP ON REVERSE"
*replace welllocation = "" if welllocation=="SAME AS"
*replace welllocation = "" if welllocation=="SAME AS ABOBE"
*replace welllocation = "" if welllocation=="SAME AS ABOVE ADDRESS"
*replace welllocation = "" if welllocation=="SAME AS ADDRES"
*replace welllocation = "" if welllocation=="SAME AS ADDRESS"
*replace welllocation = "" if welllocation=="SAME AS ADDRESS ABOVE"
replace welllocation = "" if welllocation=="SEE ATTACH SKETCH"
replace welllocation = "" if welllocation=="SEE ATTACHED"
replace welllocation = "" if welllocation=="SEE ATTACHED MAP"
replace welllocation = "" if welllocation=="SEE ATTACHED PLOT"
replace welllocation = "" if welllocation=="SEE ATTACHED SHEET"
replace welllocation = "" if welllocation=="SEE ATTACHED SKETCH"
replace welllocation = "" if welllocation=="SEE ATTACHED WELL"
replace welllocation = "" if welllocation=="SEE ATTACHMENT"
replace welllocation = "" if welllocation=="SEE ATTACHMENT B"
replace welllocation = "" if welllocation=="SEE ATTACHMENTS"
replace welllocation = "" if welllocation=="SEE BACK"
replace welllocation = "" if welllocation=="SEE BACK PAGE"
replace welllocation = "" if welllocation=="SEE BACK SHEET"
replace welllocation = "" if welllocation=="SEE BELOW"
replace welllocation = "" if welllocation=="SEE CONSTRUCTION LOG"
replace welllocation = "" if welllocation=="SEE DESCRIPTION"
replace welllocation = "" if welllocation=="SEE DESCRIPTION UNDER WELL LOG"
replace welllocation = "" if welllocation=="SEE DIAGRAM"
replace welllocation = "" if welllocation=="SEE DIAGRAM REVERSE SIDE"
replace welllocation = "" if welllocation=="SEE DISCRIPTION"
replace welllocation = "" if welllocation=="SEE ENCLOSED MAP"
replace welllocation = "" if welllocation=="SEE LEGAL"
replace welllocation = "" if welllocation=="SEE LOCAL PERMIT"
replace welllocation = "" if welllocation=="SEE LOCATION MPA ATTACHED"
replace welllocation = "" if welllocation=="SEE MAP"
replace welllocation = "" if welllocation=="SEE MAP ATTACHED"
replace welllocation = "" if welllocation=="SEE MAP ATTACHED EAST"
replace welllocation = "" if welllocation=="SEE MAP BELOW"
replace welllocation = "" if welllocation=="SEE MAP INCLOSED"
replace welllocation = "" if welllocation=="SEE MAP ON BACK"
replace welllocation = "" if welllocation=="SEE MAP ON FIRST REPORT"
replace welllocation = "" if welllocation=="SEE MAP ON REVERSE"
replace welllocation = "" if welllocation=="SEE MAP ON REVERSE SIDE"
replace welllocation = "" if welllocation=="SEE MAP ON REVERSE SIDE HERE OF"
replace welllocation = "" if welllocation=="SEE MAP OVER"
replace welllocation = "" if welllocation=="SEE MAP REVERSE"
replace welllocation = "" if welllocation=="SEE MAP REVERSE SIDE"
replace welllocation = "" if welllocation=="SEE MAPS IN BACK"
replace welllocation = "" if welllocation=="SEE MAY ATTACHED"
replace welllocation = "" if welllocation=="SEE OTHER SIDE"
replace welllocation = "" if welllocation=="SEE OWNER"
replace welllocation = "" if welllocation=="SEE PAGE 1"
replace welllocation = "" if welllocation=="SEE PERMIT"
replace welllocation = "" if welllocation=="SEE RESERVE SIDE"
replace welllocation = "" if welllocation=="SEE REVERSE"
replace welllocation = "" if welllocation=="SEE REVERSE SIDE"
replace welllocation = "" if welllocation=="SEE REVERSE SIDE FOR MAP"
replace welllocation = "" if welllocation=="SEE SFETCH BACK"
replace welllocation = "" if welllocation=="SEE SKETCH"
replace welllocation = "" if welllocation=="SEE SKETCH BELOW"
replace welllocation = "" if welllocation=="SEE SKETCH ON BACK"
replace welllocation = "" if welllocation=="SEE SKETCH/NO ADDRESS"
replace welllocation = "" if welllocation=="SEE WELL CONSTRUCTION"
br welllocation if strpos(welllocation,"_")
replace welllocation = subinstr(welllocation,"_","",.)
la var welllocation "Well location (often a street adress, often missing)"

** City
br city* _merge
br city city2020 if city!=city2020 & _merge==3 // only 79
replace city = city2020 if city2020!=""
drop city2020
replace city = upper(trim(itrim(city)))
sort city
br city welllocation
replace city = "" if city=="N/A"
replace city = "" if city=="NA"
replace city = "" if city=="NONE"
replace city = substr(city,2,10000) if inlist(substr(city,1,1),"'",",",".","]")
tab city if regexm(city,"SAME")
count if city==""
la var city "City/town/community where well is located (or nearest too)"

** Planned/former use
sort planneduseformeruse*
br planneduseformeruse* _merge
br planneduseformeruse* if planneduseformeruse!=planneduseformeruse2020 & _merge==3 // only 114
replace planneduseformeruse = planneduseformeruse2020 if planneduseformeruse2020!=""
drop planneduseformeruse2020
unique planneduseformeruse
replace planneduseformeruse = upper(trim(itrim(planneduseformeruse)))
unique planneduseformeruse
egen temp_tag = tag(planneduseformeruse)
rename planneduseformeruse use_category_raw
list use_category_raw if temp_tag==1 & regexm(use_category_raw,"AGRI") 
list use_category_raw if temp_tag==1 & !regexm(use_category_raw,"AGRI") ///
	& regexm(use_category_raw,"IRRI")

gen use_category_ag = regexm(use_category_raw,"AGRI")
list use_category_raw if temp_tag==1 & regexm(use_category_raw,"AG") & !regexm(use_category_raw,"AGRI")
replace use_category_ag = 1 if use_category_raw=="OTHER AG WELL"
replace use_category_ag = 1 if use_category_raw=="OTHER DOMESTIC AND AG"
replace use_category_ag = 1 if use_category_raw=="OTHER DOMESTIC/AG"
replace use_category_ag = 1 if use_category_raw=="OTHER DOMESTIC / AG"
replace use_category_ag = 1 if use_category_raw=="OTHER AG"
replace use_category_ag = 1 if use_category_raw=="OTHER IRRIGATION/AG"
replace use_category_ag = 1 if use_category_raw=="OTHER DOMESTIC & AG"
replace use_category_ag = 1 if use_category_raw=="OTHER AG. WELL"
replace use_category_ag = 1 if use_category_raw=="OTHER AG."

gen use_category_irr = regexm(use_category_raw,"IRRI")

list use_category_raw if temp_tag==1 & use_category_ag==0 & use_category_irr==0

la var use_category_raw "Planned or former well use"
la var use_category_ag "Planned or former well use = agriculture"
la var use_category_irr "Planned or former well use = irrigation"
order use_category_ag use_category_irr, after(use_category_raw)
drop temp*

** Driller name and license number
br drillername* drillerlice* _merge
br drillername* drillerlice* if drillerlicensenumber!=drillerlicensenumber2020 & _merge==3 // only 23
replace drillerlicensenumber = drillerlicensenumber2020 if drillerlicensenumber2020!=""
drop drillerlicensenumber2020
replace drillername = upper(trim(itrim(drillername)))
replace drillername2020 = upper(trim(itrim(drillername2020)))
sort drillername*
br drillername* drillerlice if drillername!=drillername2020 & _merge==3 // 732 conflict, most are the same
replace drillername = drillername2020 if drillername2020!=""
drop drillername2020
replace drillername = "" if drillername=="NONE"
replace drillerlicense = "" if upper(drillerlicense)=="NONE"
egen temp = mode(drillername), by(drillerlicensenumber)
unique drillerlice
unique drillerlice if drillername!=temp
br drillerlic drillername temp if drillername!=temp
	// not worth cleaning this
drop temp
rename drillername driller_name
rename drillerlicense driller_lic_nbr
la var driller_name "Driller name"
la var driller_lic_nbr "Driller license number (C-57)"
compress

** Record type
br recordtype if recordtype!=recordtype2020 & _merge==3 // 0 conflicts
replace recordtype = recordtype2020 if recordtype!=""
drop recordtype2020
levelsof recordtype
replace recordtype = subinstr(recordtype,"WellCompletion/","",1)
replace recordtype = subinstr(recordtype,"/NA","",.)
replace recordtype = subinstr(recordtype,"/Production or Monitoring","",.)
replace recordtype = subinstr(recordtype," and ","/",.)
replace recordtype = subinstr(recordtype," or ","/",.)
tab recordtype _merge, missing
rename recordtype record_type
la var record_type "Record type (new, modification/repair, destruction, drill/destroy)"

** Lat and lon
br decimal*
br decimal* if (decimallatitude!=decimallatitude2020 | decimallongitude!=decimallongitude2020) & _merge==3
	// 308 conflicts
br decimal* if max(abs(decimallatitude-decimallatitude2020),abs(decimallongitude-decimallongitude2020))>0.001 ///
	& decimallatitude+decimallatitude2020+decimallongitude+decimallongitude2020!=. & _merge==3
	// 253 conflicts
br decimal* if max(abs(decimallatitude-decimallatitude2020),abs(decimallongitude-decimallongitude2020))>0.01 ///
	& decimallatitude+decimallatitude2020+decimallongitude+decimallongitude2020!=. & _merge==3
	// 50 conflicts
replace decimallatitude = decimallatitude2020 if decimallatitude2020!=.
replace decimallongitude = decimallongitude2020 if decimallongitude2020!=.	
drop decimal*2020
count if decimallat>100 & decimallat!=.
replace decimallat = . if decimallat>100
count if decimallong>0 & decimallong!=.
tab decimallong if decimallong>0 & decimallong!=.
replace decimallong = -decimallong if decimallong>0
twoway (scatter decimallat decimallon, msize(vtiny))
rename decimallat well_latitude
rename decimallon well_longitude
la var well_latitude "Well latitude"
la var well_longitude "Well longitude"

** Method of determining coordinates
tab methodofdeterminationll methodofdeterminationll2020, missing
br methodof* if methodofdeterminationll!=methodofdeterminationll2020 & _merge==3 // 2019 pull is better
replace methodofdeterminationll = methodofdeterminationll2020 if _merge==2 | methodofdeterminationll==""
drop methodofdeterminationll2020
rename methodof latlon_method 
la var latlon_method "Method of determining horizontal coordinates"

** Lat/lon accuracy
tab llaccuracy*
br llaccuracy* if llaccuracy!=llaccuracy2020 & _merge==3 // 200 conflicts
replace llaccuracy = llaccuracy2020 if llaccuracy2020!=""
drop llaccuracy2020
replace llaccuracy = ">50 Ft" if llaccuracy==">50 FT"
tab llaccuracy
rename llaccuracy latlon_accuracy
la var latlon_accuracy "Accuracy of lat/lon"

** Horizontal datum
tab horizontaldatum*
br horizontaldatum* if horizontaldatum!=horizontaldatum2020 & _merge==3 // 230 conflicts
replace horizontaldatum = horizontaldatum2020 if horizontaldatum2020!=""
drop horizontaldatum2020
rename horizontaldatum latlon_projection
la var latlon_projection "Map projection (if any) used to determine lat/lon"

** Ground surface elevation
br groundsurfaceelevation* if groundsurfaceelevation!=groundsurfaceelevation2020 & _merge==3 // 2 conflicts
replace groundsurfaceelevation = groundsurfaceelevation2020 if groundsurfaceelevation!=""
drop groundsurfaceelevation2020
replace groundsurfaceelevation =  "" if groundsurfaceelevation=="None"
destring groundsurfaceelevation, replace
rename groundsurfaceelevation ground_surface_elev
la var ground_surface_elev "Ground-to-surface elevation (almost entirely missing)"

** Elevation accuracy
tab elevationaccuracy*
br elevationaccuracy* if elevationaccuracy!=elevationaccuracy2020 & _merge==3 // 2 conflicts
replace elevationaccuracy = elevationaccuracy2020 if elevationaccuracy2020!=""
drop elevationaccuracy2020
rename elevationaccuracy elev_accuracy
la var elev_accuracy "Accuracy of elevation measure"

** Elevation determination method
tab elevationdeterminationm*
replace elevationdeterminationmethod = elevationdeterminationmethod2020 if elevationdeterminationmethod2020!=""
drop elevationdeterminationmethod2020
rename elevationdeterminationm elev_method 
la var elev_method "Method of determining elevation measure (that's almost always missing)"

** Vertical datum
tab verticaldatum*
replace verticaldatum = verticaldatum2020 if verticaldatum2020!=""
drop verticaldatum2020
rename verticaldatum elev_projection
la var elev_projection "Datum/projection for elevation measurement"

** Township
br township* if township!=township2020 & _merge==3 // 65 conflicts
replace township = township2020 if township2020!=""
drop township2020
tab township
replace township = "" if inlist(township,"?","N0","NO","Non","S")
la var township "Public Land Survey System township of well location"

** Range
br range* if range!=range2020 & _merge==3 // 65 conflicts
replace range = range2020 if range2020!=""
drop range2020
tab range
replace range = "" if inlist(range,"?","Non","W","xxE")
la var range "Public Land Survey System range of well location"

** Section
br section* if section!=section2020 & _merge==3 // 65 conflicts
replace section = section2020 if section2020!=""
drop section2020
tab section
replace section = "" if inlist(section,"NN","NO NUMBER","NO NUMBERS","NONE","None","nn","?")
la var section "Public Land Survey System section of well location"

** Baseline meridian
br baselinemeridian* if baselinemeridian!=baselinemeridian2020 & _merge==3 // 65 conflicts
replace baselinemeridian = baselinemeridian2020 if baselinemeridian2020!=""
drop baselinemeridian2020
tab baselinemeridian _merge
replace baselinemeridian = "Humboldt" if baselinemeridian=="H"
replace baselinemeridian = "Mount Diablo" if baselinemeridian=="M"
replace baselinemeridian = "San Bernardino" if baselinemeridian=="S"
tab baselinemeridian
la var baselinemeridian "Public Land Survey System meridian baseline of well location"

** APN
sort apn*  
br apn apn2020 if apn!=apn2020 & _merge==3 // 240 conflicts
replace apn = apn2020 if !inlist(apn2020,"","None")
drop apn2020
replace apn = trim(itrim(apn))
replace apn = subinstr(apn," ","",1)
replace apn = "" if apn=="NA"
replace apn = "" if apn=="None"
replace apn = "" if apn=="Not Applicable"
replace apn = "" if apn=="UNKNOWN"
replace apn = "" if apn=="Unknown"
replace apn = "" if apn=="XXX"
replace apn = "" if apn=="XXXXXXXXX"
replace apn = "" if apn=="n/a"
replace apn = "" if apn=="na"
replace apn = "" if apn=="na"
replace apn = "" if apn=="none"
la var apn "Asessor's parcel number of well location"

** Date work ended
replace dateworkended = trim(subinstr(subinstr(dateworkended,"00:00:00.000000000","",1),"00:00:00","",1))
replace dateworkended2020 = trim(subinstr(subinstr(dateworkended2020,"00:00:00.000000000","",1),"00:00:00","",1))
br dateworkended* if dateworkended!=dateworkended2020 & _merge==3 // 44 conflicts
br dateworkended* if length(dateworkended)>10 | length(dateworkended2020)>10
replace dateworkended = word(dateworkended,1) if length(dateworkended)>10
replace dateworkended2020 = word(dateworkended2020,1) if length(dateworkended2020)>10
replace dateworkended = dateworkended2020 if dateworkended2020!=""
gen date_work_ended = date(dateworkended,"YMD")
assert date_work_ended!=. if dateworkended!=""
format %td date_work_ended
gen temp_year = year(date_work_ended)
tab temp_year
replace date_work_ended = . if temp_year>2020 | temp_year<1800
replace date_work_ended = . if date_work_ended>=mdy(5,1,2020)
tab date_work_ended if temp_year==2020
order date_work_ended, after(dateworkended)
la var date_work_ended "Date when work on well ended"
drop temp* dateworkended*

** Work flow status
sort workflowstatus*
br workflowstatus*
drop workflowstatus* // no relevant information

** Date received
sort receiveddate* 
replace receiveddate = trim(subinstr(subinstr(receiveddate,"00:00:00.000000000","",1),"00:00:00","",1))
replace receiveddate2020 = trim(subinstr(subinstr(receiveddate2020,"00:00:00.000000000","",1),"00:00:00","",1))
br receiveddate* if receiveddate!=receiveddate2020 & _merge==3 // 244 conflicts
br receiveddate* if length(receiveddate)>10 | length(receiveddate2020)>10
replace receiveddate = word(receiveddate,1) if length(receiveddate)>10
replace receiveddate2020 = word(receiveddate2020,1) if length(receiveddate2020)>10
replace receiveddate = receiveddate2020 if receiveddate2020!=""
gen date_received = date(receiveddate,"YMD")
assert date_received!=. if receiveddate!=""
format %td date_received
gen temp_year = year(date_received)
tab temp_year
replace date_received = . if temp_year>2020 | temp_year<1800
tab date_received if temp_year==2020
order date_received, after(receiveddate)
la var date_received "Date when WCR report was received (mostly missing)"
drop temp* receiveddate*

** Total drill depth
sort totaldrilldepth
br totaldrilldepth* if totaldrilldepth!=totaldrilldepth2020 & _merge==3
replace totaldrilldepth = totaldrilldepth2020 if !inlist(totaldrilldepth2020,"","None")
drop totaldrilldepth2020
replace totaldrilldepth = "" if totaldrilldepth=="None"
destring totaldrilldepth, replace
rename totaldrilldepth drill_depth_total
la var drill_depth_total "Total drill depth (ft)"

** Total completed depth
sort totalcompleteddepth
br totalcompleteddepth* if totalcompleteddepth!=totalcompleteddepth2020 & _merge==3
replace totalcompleteddepth = totalcompleteddepth2020 if !inlist(totalcompleteddepth2020,.)
drop totalcompleteddepth2020
br drill_depth_total totalcompleteddepth
rename totalcompleteddepth depth_total_completed
la var depth_total_completed "Depth to which well was completed (ft)"

** Top/bottom of perforated interval
sort topofperforatedinterval* bottomofperforatedinterval*
br topofperforatedinterval* if topofperforatedinterval!=topofperforatedinterval2020 & _merge==3  // 54 conflicts
replace topofperforatedinterval = topofperforatedinterval2020 if !inlist(topofperforatedinterval2020,"","None")
br bottomofperforatedinterval* if bottomofperforatedinterval!=bottomofperforatedinterval2020 & _merge==3 // 57 conflicts
replace bottomofperforatedinterval = bottomofperforatedinterval2020 if !inlist(bottomofperforatedinterval2020,"","None") 
drop topofperforatedinterval2020 bottomofperforatedinterval2020
br topofperforatedinterval bottomofperforatedinterval 
replace topofperforatedinterval = "" if topofperforatedinterval=="None"
replace bottomofperforatedinterval = "" if bottomofperforatedinterval=="None"
destring topofperforatedinterval bottomofperforatedinterval, replace
rename topofperforatedinterval perfinterval_top
rename bottomofperforatedinterval perfinterval_bottom
la var perfinterval_top "Top of perforated interval depth (ft)"
la var perfinterval_bottom "Bottom of perforared interval depth (ft)"

** Casing diameter
sort casingdiameter
br casingdiameter* if casingdiameter!=casingdiameter2020 & _merge==3 // 221 conflicts
replace casingdiameter = casingdiameter2020 if !inlist(casingdiameter2020,"","None") 
drop casingdiameter2020
replace casingdiameter = "" if casingdiameter=="None"
replace casingdiameter = "" if casingdiameter=="-99999"
destring casingdiameter, replace
sum casingdiameter, detail
la var casingdiameter "Diameter of well casing (inches?)"

** Drilling method
tab drillingmethod 
br drillingmethod* if drillingmethod!=drillingmethod2020 & _merge==3 // 16 conflicts
replace drillingmethod = drillingmethod2020 if !inlist(drillingmethod2020,"","None") 
drop drillingmethod2020
replace drillingmethod = upper(trim(itrim(drillingmethod)))
replace drillingmethod = "" if inlist(drillingmethod,"NONE","NA","XXX")
tab drillingmethod
la var drillingmethod "Description of drilling method"

** Fluid
tab fluid 
br fluid* if fluid!=fluid2020 & _merge==3 // 10 conflicts
replace fluid = fluid2020 if !inlist(fluid2020,"","None") 
drop fluid2020
replace fluid = upper(trim(itrim(fluid)))
tab fluid
rename fluid drillingfluid
la var drillingfluid "Drilling fluid"

** Static water level
sort staticwaterlevel
br staticwaterlevel* if staticwaterlevel!=staticwaterlevel2020 & _merge==3 // 10 conflicts
replace staticwaterlevel = staticwaterlevel2020 if !inlist(staticwaterlevel2020,"","None") 
drop staticwaterlevel2020
destring staticwaterlevel, replace force
rename staticwaterlevel swl_pump
la var swl_pump "Standing water level depth (ft) after well was built"

** Drawdown
sort totaldrawdown
br totaldrawdown* if totaldrawdown!=totaldrawdown2020 & _merge==3 // 7 conflicts
replace totaldrawdown = totaldrawdown2020 if !inlist(totaldrawdown2020,"","None") 
drop totaldrawdown2020
destring totaldrawdown, replace force
rename totaldrawdown drawdown_pump
la var drawdown_pump "Drawdown (ft) after initial pump test"

** Test type
tab testtype*
replace testtype = testtype2020 if testtype2020!="" & testtype2020!="None"
drop testtype2020
rename testtype test_type
la var test_type "Type of initial pump test"

** Pump test length
sort pumptestlength
br pumptestlength* if pumptestlength!=pumptestlength2020 & _merge==3 // 24 conflicts
replace pumptestlength = pumptestlength2020 if !inlist(pumptestlength2020,"","None") 
drop pumptestlength2020
destring pumptestlength, replace force
rename pumptestlength test_length
la var test_length "Initial pump test length (hours)"

** Well yield
sort wellyield 
br wellyield* if (wellyield!=wellyield2020 | wellyieldunitofmeasure!=wellyieldunitofmeasure2020) & _merge==3 // 24 conflicts
gen temp = !inlist(wellyield2020,"","None") & !inlist(wellyieldunitofmeasure2020,"","None")
replace wellyield = wellyield2020 if temp
replace wellyieldunitofmeasure = wellyieldunitofmeasure2020 if temp
drop wellyield2020 wellyieldunitofmeasure2020 temp
sort wellyield 
br wellyield*
tab wellyieldunit
replace wellyield = "" if wellyieldunit=="Miners Inches"
destring wellyield, force replace
assert wellyieldunit=="GPM" if wellyield!=.
drop wellyieldunit
rename wellyield well_gpm
la var well_gpm "Well yield (gallons per minute)"

** Other observations
sort otherobservations*
br otherobservations* if otherobservations!=otherobservations2020 & _merge==3 // 116 conflicts
replace otherobservations = otherobservations2020 if !inlist(otherobservations2020,"","None") 
drop otherobservations2020
replace otherobs = upper(itrim(trim(otherobs)))
br otherobs if length(otherobs)>100
replace otherobs = substr(otherobs,1,100)
replace otherobs = "" if otherobs=="NONE"
rename otherobs other_obs
la var other_obs "Other observations on WCR, sometimes address info, mostly missing"

** Save
drop _merge
sort wcrnumber
unique wcrnumber
assert r(unique)==r(N)
compress
save "$dirpath_data/cleaned_spatial/wcr_data_full.dta", replace

}

*******************************************************************************
*******************************************************************************
