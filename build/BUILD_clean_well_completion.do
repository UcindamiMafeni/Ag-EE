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

** Read in data
insheet using "$dirpath_data/spatial/well completion reports/OSWCR.csv", names double clear

** Identifiers
unique wcrnumber
assert r(unique)==r(N)
la var wcrnumber "Unique WCR identifier"
unique legacylognumber
unique legacylognumber if legacylognumber!=""
sort legacylognumber
replace legacylognumber = "" if legacylognumber=="None"
replace legacylognumber = "" if legacylognumber=="NN"
replace legacylognumber = "" if legacylognumber=="n/a"
replace legacylognumber = "" if regexm(legacylognumber,"xxxx")
la var legacylognumber "Legacy DWR log number (a non-unique identifier)"

** Region office
tab regionoffice, missing
replace regionoffice = subinstr(regionoffice," Region Office","",1)
tab regionoffice, missing
rename regionoffice dwr_office
replace dwr_office = trim(itrim(dwr_office))
la var dwr_office "DWR region office"

** County
rename countyname county
tab county
replace county = "" if county=="None" | county=="Unknown"
replace county = trim(itrim(county))
la var county "County name"

** Local permitting agency
tab localpermitagency
rename localpermitagency local_permit_authority
la var local_permit_authority "Local regulatory authority over well construction, alteration, destruction"

** Permit date
count if permitdate=="None" // almost all missing
replace permitdate = "" if permitdate=="None"
gen temp = date(permitdate,"MDY")
assert temp!=. if permitdate!=""
order temp, after(permitdate)
drop permitdate
rename temp date_permit
format %td date_permit
la var date_permit "Well permit date (mostly missing)"

** Permit number
sort permitnumber 
br permitnumber
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
replace permitnumber = trim(itrim(permitnumber))
la var permitnumber "Well permit number (mostly missing)"

** Owner-assigned well number
sort ownerassigned
br ownerassigned
replace ownerassigned = trim(itrim(ownerassigned))
replace ownerassigned = "" if upper(ownerassigned)=="N/A"
replace ownerassigned = "" if upper(ownerassigned)=="NA"
replace ownerassigned = "" if upper(ownerassigned)=="NONE"
*replace ownerassigned = "" if upper(word(ownerassigned,1))=="SAME"
la var ownerassigned "Owner assigned well number (mostly missing)"

** Well location
sort welllocation
br welllocation
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
la var welllocation "Well location (often a street adress, often missing)"

** City
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
replace use_category_ag = 1 if use_category_raw=="OTHER DOMESTIC / AG"
replace use_category_ag = 1 if use_category_raw=="OTHER AG"
replace use_category_ag = 1 if use_category_raw=="OTHER IRRIGATION/AG"
replace use_category_ag = 1 if use_category_raw=="OTHER DOMESTIC & AG"

gen use_category_irr = regexm(use_category_raw,"IRRI")

list use_category_raw if temp_tag==1 & use_category_ag==0 & use_category_irr==0

la var use_category_raw "Planned or former well use"
la var use_category_ag "Planned or former well use = agriculture"
la var use_category_irr "Planned or former well use = irrigation"
order use_category_ag use_category_irr, after(use_category_raw)
drop temp*

** Driller name and license number
br drillername drillerlice
replace drillername = upper(trim(itrim(drillername)))
sort drillername 
br drillername drillerlice
replace drillername = "" if drillername=="NONE"
replace drillerlicense = "" if upper(drillerlicense)=="NONE"
unique drillername
unique drillername drillerlice
rename drillername driller_name
rename drillerlicense driller_lic_nbr
la var driller_name "Driller name"
la var driller_lic_nbr "Driller license number (C-57)"

** Record type
levelsof recordtype
replace recordtype = subinstr(recordtype,"WellCompletion/","",1)
replace recordtype = subinstr(recordtype,"/NA","",.)
replace recordtype = subinstr(recordtype,"/Production or Monitoring","",.)
replace recordtype = subinstr(recordtype," and ","/",.)
replace recordtype = subinstr(recordtype," or ","/",.)
rename recordtype record_type
la var record_type "Record type (new, modification/repair, destruction, drill/destroy)"

** Lat and lon
br decimal*
replace decimallat = . if decimallat>100
twoway (scatter decimallat decimallon, msize(vtiny))
rename decimallat well_latitude
rename decimallon well_longitude
la var well_latitude "Well latitude"
la var well_longitude "Well longitude"

** Method of determining coordinates
tab methodof
rename methodof latlon_method 
la var latlon_method "Method of determining horizontal coordinates"

** Lat/lon accuracy
tab llaccuracy
replace llaccuracy = ">50 Ft" if llaccuracy==">50 FT"
rename llaccuracy latlon_accuracy
la var latlon_accuracy "Accuracy of lat/lon"

** Horizontal datum
tab horizontaldatum
rename horizontaldatum latlon_projection
la var latlon_projection "Map projection (if any) used to determine lat/lon"

** Ground surface elevation
tab groundsurfaceelevation
replace groundsurfaceelevation =  "" if groundsurfaceelevation=="None"
destring groundsurfaceelevation, replace
rename groundsurfaceelevation ground_surface_elev
la var ground_surface_elev "Ground-to-surface elevation (almost entirely missing)"

** Elevation accuracy
tab elevationaccuracy
rename elevationaccuracy elev_accuracy
la var elev_accuracy "Accuracy of elevation measure"

** Elevation determination method
tab elevationdeterminationm
rename elevationdeterminationm elev_method 
la var elev_method "Method of determining elevation measure (that's almost always missing)"

** Vertical datum
tab verticaldatum
rename verticaldatum elev_projection
la var elev_projection "Datum/projection for elevation measurement"

** Township
tab township
la var township "Public Land Survey System township of well location"

** Range
tab range
la var range "Public Land Survey System range of well location"

** Section
tab section
replace section = "" if inlist(section,"NN","NO NUMBER","NO NUMBERS","NONE","None","nn","?")
la var section "Public Land Survey System section of well location"

** Baseline meridian
tab baselinemeridian
la var baselinemeridian "Public Land Survey System meridian baseline of well location"

** APN
sort apn
br apn
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
br dateworkended 
replace dateworkended = subinstr(dateworkended,"00:00:00.000000000","",1)
replace dateworkended = trim(dateworkended)
gen temp = length(dateworkended)
br dateworkended if temp>10
replace dateworkended = word(dateworkended,1) if temp>10
gen date_work_ended = date(dateworkended,"YMD")
format %td date_work_ended
gen temp_year = year(date_work_ended)
tab temp_year
replace date_work_ended = . if temp_year>2019 | temp_year<1800
order date_work_ended, after(dateworkended)
la var date_work_ended "Date when work on well ended"
drop temp* dateworkended 

** Work flow status
sort workflowstatus
br workflowstatus
drop workflowstatus // no relevant information

** Date received
sort receiveddate 
br receiveddate
replace receiveddate = subinstr(receiveddate,"00:00:00.000000000","",1)
replace receiveddate = trim(receiveddate)
gen temp = length(receiveddate)
br receiveddate if temp>10
replace receiveddate = word(receiveddate,1) if temp>10
gen date_received = date(receiveddate,"YMD")
format %td date_received
gen temp_year = year(date_received)
tab temp_year
replace date_received = . if temp_year>2019 | temp_year<1800
order date_received, after(receiveddate)
la var date_received "Date when WCR report was received (mostly missing)"
drop temp* receiveddate 

** Total drill depth
sort totaldrilldepth
br totaldrilldepth 
replace totaldrilldepth = "" if totaldrilldepth=="None"
destring totaldrilldepth, replace
rename totaldrilldepth drill_depth_total
la var drill_depth_total "Total drill depth (ft)"

** Total completed depth
sort totalcompleteddepth
br drill_depth_total totalcompleteddepth
rename totalcompleteddepth depth_total_completed
la var depth_total_completed "Depth to which well was completed (ft)"

** Top/bottom of perforated interval
sort topofperforatedinterval bottomofperforatedinterval
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
br casingdiameter 
replace casingdiameter = "" if casingdiameter=="None"
replace casingdiameter = "" if casingdiameter=="-99999"
destring casingdiameter, replace
sum casingdiameter, detail
la var casingdiameter "Diameter of well casing (inches?)"

** Drilling method
tab drillingmethod 
replace drillingmethod = upper(trim(itrim(drillingmethod)))
replace drillingmethod = "" if inlist(drillingmethod,"NONE","NA","XXX")
la var drillingmethod "Description of drilling method"

** Fluid
tab fluid 
replace fluid = upper(trim(itrim(fluid)))
tab fluid
rename fluid drillingfluid
la var drillingfluid "Drilling fluid"

** Static water level
br staticwaterlevel 
destring staticwaterlevel, replace force
rename staticwaterlevel swl_pump
la var swl_pump "Standing water level depth (ft) after well was built"

** Drawdown
br totaldrawdown
destring totaldrawdown, replace force
rename totaldrawdown drawdown_pump
la var drawdown_pump "Drawdown (ft) after initial pump test"

** Test type
tab testtype
rename testtype test_type
la var test_type "Type of initial pump test"

** Pump test length
sort pumptestlength
br pumptestlength
destring pumptestlength, replace force
rename pumptestlength test_length
la var test_length "Initial pump test length (hours)"

** Well yield
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
sort otherobservations
br otherobs
replace otherobs = upper(itrim(trim(otherobs)))
br otherobs if length(otherobs)>100
replace otherobs = substr(otherobs,1,100)
replace otherobs = "" if otherobs=="NONE"
rename otherobs other_obs
la var other_obs "Other observations on WCR, sometimes address info, mostly missing"


** Save
sort wcrnumber
unique wcrnumber
assert r(unique)==r(N)
compress
save "$dirpath_data/cleaned_spatial/wcr_data_full.dta", replace

}

*******************************************************************************
*******************************************************************************
