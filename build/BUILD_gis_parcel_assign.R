#######################################################
#  Script to assign SP premises and APEP pumps to     #
#  Parcels (i.e. static taxed properties "farms")     #
####################################################### 
rm(list = ls())

#install.packages("ggmap")
#install.packages("ggplot2")
#install.packages("gstat")
#install.packages("sp")
#install.packages("maptools")
#install.packages("rgdal")
#install.packages("rgeos")
#install.packages("raster")
#install.packages("SDMTools")

#libP <- "C:/Program Files/Microsoft/R Open/R-3.4.4/library"

#options(repos="https://cran.cnr.berkeley.edu")
#install.packages(c("dplyr", "sf"))

library(ggmap) #, lib.loc=libP)
library(ggplot2) #, lib.loc=libP)
library(gstat) #, lib.loc=libP)
library(sp) #, lib.loc=libP)
library(sf) #, lib.loc=libP)
library(maptools) #, lib.loc=libP)
library(rgdal) #, lib.loc=libP)
library(rgeos) #, lib.loc=libP)
library(raster) #, lib.loc=libP)
library(SDMTools) #, lib.loc=libP)
library(tidyverse)
library(viridis)
library(rvest)
library(tmap)
library(parallel)
library(cluster)
library(dplyr)
library(data.table)

path <- "T:/Projects/Pump Data/data"

##########################################
### 1. Prep all relevant in shapefiles ###
##########################################

#Load Parcels SF data frame
setwd(paste0(path,"/cleaned_spatial/Parcels"))
Parcels_sf <- readRDS("parcels.RDS")
crs <- st_crs(Parcels_sf)

#Export list of Parcels 
setwd(paste0(path,"/misc"))
Parcels_data <- Parcels_sf
st_geometry(Parcels_data) <- NULL
filename <- "Parcels_cleaned.csv"
write.csv(Parcels_data, file=filename , row.names=FALSE, quote=FALSE)
rm(Parcels_data)

#Drop columns to save memory
Parcels_sf <- Parcels_sf[,names(Parcels_sf) %in% c("ParcelID","County","ParcelAcres")]

#Load CA state outline
setwd(paste0(path,"/spatial"))
CAoutline <- readOGR(dsn = "State", layer = "CA_State_TIGER2016")
CAoutline_sf <- st_as_sf(CAoutline)
CAoutline_sf <- st_transform(CAoutline_sf, crs)
rm(CAoutline)

#Load parcel-CLU concordance list
Parcels_conc_list <- as.data.frame(read.csv(paste0(path,"/misc/parcels_in_clus.csv")))
Parcels_conc_list <- as.data.frame(Parcels_conc_list$parcelid)
names(Parcels_conc_list) <- "ParcelID"
stopifnot(nrow(Parcels_conc_list)==nrow(unique(Parcels_conc_list))) #confirm uniqueness

#Right-join list of merged parcels to full SF object
Parcels_conc_sf <- right_join(Parcels_sf,Parcels_conc_list, by="ParcelID")

#Create county-specific SF data frames, and prep for nearest-polygon function
counties <- levels(Parcels_conc_sf$County) # create index of counties
for (i in 1:58) {
  
  new_name <- paste0("Parcels_dt_",gsub(" ","",counties[i]))
  
  # subset full SF object for county i
  new_sf <- Parcels_sf[Parcels_sf$County==counties[i],]
  
  # drop unnecessary variables, keeping only ID
  new_sf <- new_sf[,names(new_sf) %in% c("ParcelID")] 
  
  # reproject into planar coordinates
  #new_sf <- st_transform(new_sf, crs) 
  
  # remove polygons missing lat/lon
  new_sf <- new_sf[word(new_sf$ParcelID,4)!="NA",]
  
  # convert from polygon to line, which is equivalent + faster for calculating minimum distance to polygon
  new_sf <- st_cast(new_sf, 'MULTILINESTRING')
  
  # convert to data.table, which is way faster apparently
  new_dt <- as.data.table(new_sf)
  
  # export and clean up
  assign(new_name, new_dt)
  rm(new_name, new_sf, new_dt)
}



##############################################################
### 2. Assign SP lat/lons to Parcels (full set of parcels) ###
##############################################################

#Read PGE coordinates
setwd(paste0(path,"/misc"))
prems <- read.delim2("pge_prem_coord_3pulls.txt",header=TRUE,sep=",",stringsAsFactors=FALSE)
prems$longitude <- as.numeric(prems$prem_lon)
prems$latitude <- as.numeric(prems$prem_lat)

#Merge in assigned counties
prems_counties <- read.delim2("pge_prem_coord_polygon_counties.txt",header=TRUE,sep="%",stringsAsFactors=FALSE)
prems_counties <- prems_counties[,names(prems_counties) %in% c("sp_uuid","county")]
stopifnot(prems$sp_uuid==prems_counties$sp_uuid) # confirm merge by row id is good
prems$county <- prems_counties$county
rm(prems_counties)

#Convert to SF object
coordinates(prems) <- ~ longitude + latitude
prems_sf  <- st_as_sf(prems)
st_crs(prems_sf) <- st_crs(Parcels_sf)

#Assign each lat/lon to the polygon it's contained in
prems_sf$in_parcel_row <- sapply(st_intersects(prems_sf,Parcels_sf), function(z) if (length(z)==0) NA_integer_ else z[1])

#Package results
prems_sf$in_parcel <- is.na(prems_sf$in_parcel_row)==0
prems_sf$ParcelID <- Parcels_sf$ParcelID[prems_sf$in_parcel_row]
prems_sf$ParcelCounty <- Parcels_sf$County[prems_sf$in_parcel_row]
prems_sf$ParcelAcres <- Parcels_sf$ParcelAcres[prems_sf$in_parcel_row]

#Save temporary results
saveRDS(prems_sf, paste0(path,"/misc/temp1_prems_sf_in_parcels.RDS"))
prems_sf <- readRDS(paste0(path,"/misc/temp1_prems_sf_in_parcels.RDS"))

#Create vector of only those observations where parcel is missing
n <- nrow(prems_sf)
missings <- c(1:n)[(is.na(prems_sf$in_parcel_row) & is.na(prems_sf$county)==0)]

#Convert into data.table, which is faster for the nearest distance function
prems_dt <- as.data.table(prems_sf[,names(prems_sf) %in% 
                                     c("sp_uuid","county","geometry","in_parcel_row","in_parcel")])

#Function to calculate distance to a parcel polygon, for lat/lons not contained in a polygon
nearestFXN <- function(i) {
  
  # create single-row data table of prem lat/lon
  temp_prems_dt <- prems_dt[i,]
  
  # extract county for missing i
  temp_county <- gsub(" ", "", temp_prems_dt$county) 
  
  # create county-specific polygons object (actually a line data.table)
  temp_Parcels_dt <- get(paste0("Parcels_dt_",temp_county)) 
  
  # find ID nearest polygon (actually line) in county
  temp_row <- st_nearest_feature(temp_prems_dt$geometry, temp_Parcels_dt$geometry) 
  
  # assign index of nearest polygon (actually line)
  out_id <- as.character(temp_Parcels_dt[temp_row,]$ParcelID)

  # convert single-row prems_dt into SF object, and add CRS units
  temp_prems_sf <- st_as_sf(temp_prems_dt)
  temp_prems_sf <- st_set_crs(temp_prems_sf, crs) 
  
  # convert nearest polygon into single-row SF object, and add CRS units
  temp_Parcels_sf <- st_as_sf(temp_Parcels_dt[temp_row,])
  temp_Parcels_sf <- st_set_crs(temp_Parcels_sf, crs) 
  
  # find distance to nearest parcel (in meters)
  out_dist <- st_distance(temp_prems_sf, temp_Parcels_sf, by_element=TRUE)
  
  # convert distance to kilometers
  out_dist <- as.numeric(out_dist/1000)
  
  # package output
  out <- c(i,out_id,out_dist)
  names(out) <- c("prems_row","nearest_ID","nearest_dist_km")
  return(out)
  gc()
}

#Calculate distance to a parcel polygon, for lat/lons not contained in a polygon
cl <- makeCluster(4) #(cores - 1)
clusterEvalQ(cl, library(sf))
clusterEvalQ(cl, library(data.table))
clusterSetRNGStream(cl, 12345)
clusterExport(cl=cl, varlist=c('nearestFXN','prems_dt','crs','Parcels_dt_Alameda','Parcels_dt_Alpine',
                               'Parcels_dt_Amador','Parcels_dt_Butte','Parcels_dt_Calaveras','Parcels_dt_Colusa',
                               'Parcels_dt_ContraCosta','Parcels_dt_ElDorado','Parcels_dt_Fresno',
                               'Parcels_dt_Glenn','Parcels_dt_Humboldt','Parcels_dt_Kern','Parcels_dt_Kings',
                               'Parcels_dt_Lake','Parcels_dt_Lassen','Parcels_dt_LosAngeles','Parcels_dt_Madera',
                               'Parcels_dt_Marin','Parcels_dt_Mariposa','Parcels_dt_Mendocino','Parcels_dt_Merced',
                               'Parcels_dt_Monterey','Parcels_dt_Napa','Parcels_dt_Nevada','Parcels_dt_Placer',
                               'Parcels_dt_Plumas','Parcels_dt_Sacramento','Parcels_dt_SanBenito',
                               'Parcels_dt_SanFrancisco','Parcels_dt_SanJoaquin','Parcels_dt_SanLuisObispo',
                               'Parcels_dt_SanMateo','Parcels_dt_SantaBarbara','Parcels_dt_SantaClara',
                               'Parcels_dt_SantaCruz','Parcels_dt_Shasta','Parcels_dt_Sierra',
                               'Parcels_dt_Solano','Parcels_dt_Sonoma','Parcels_dt_Stanislaus','Parcels_dt_Sutter',
                               'Parcels_dt_Tehama','Parcels_dt_Trinity','Parcels_dt_Tulare','Parcels_dt_Tuolumne',
                               'Parcels_dt_Ventura','Parcels_dt_Yolo','Parcels_dt_Yuba'))
nearestFXN_out <- as.data.frame(t(parSapply(cl=cl, missings, function(x) nearestFXN(x))))
stopCluster(cl)

#Save temporary results
saveRDS(nearestFXN_out, paste0(path,"/misc/temp2_prems_sf_in_parcels.RDS"))
nearestFXN_out <- readRDS(paste0(path,"/misc/temp2_prems_sf_in_parcels.RDS"))

#Fix data types
nearestFXN_out$prems_row <- as.integer(as.character(nearestFXN_out$prems_row))
nearestFXN_out$nearest_dist_km <- as.numeric(as.character(nearestFXN_out$nearest_dist_km))

#Merge in areas
nearestFXN_out <- left_join(nearestFXN_out, Parcels_sf, by=c("nearest_ID"="ParcelID"))
nearestFXN_out <- nearestFXN_out[,names(nearestFXN_out) %in% c("prems_row","nearest_ID","nearest_dist_km","ParcelAcres")]
names(nearestFXN_out)[2] <- "nearest_ParcelID"
names(nearestFXN_out)[4] <- "nearest_ParcelAcres"

#Expand nearest outputs into full size
n_vect <- as.data.frame(c(1:n))
names(n_vect) <- "prems_row"
nearestFXN_out_expanded <- left_join(n_vect, nearestFXN_out, by="prems_row")

#Tranfer nearest outcomes to main dataset
prems_out <- st_drop_geometry(prems_sf)
prems_out <- cbind(prems_out,nearestFXN_out_expanded)

#Diagnostics
summary(as.numeric(prems_out$in_parcel))
summary(as.numeric(prems_out$in_parcel[prems_out$bad_geocode_flag==0]))
summary(as.numeric(prems_out$in_parcel[prems_out$bad_geocode_flag==0 & prems_out$pull=="20180719"]))
summary(prems_out[prems_out$in_parcel==0,]$nearest_dist_km)
summary(prems_out[prems_out$in_parcel==0 & prems_out$bad_geocode_flag==0 & prems_out$pull=="20180719",]$nearest_dist_km)

#Drop extraneous variables
prems_out <- prems_out[,names(prems_out) %in% c("sp_uuid","prem_lat","prem_long","bad_geocode_flag","pull",
                                                "in_parcel","ParcelID","ParcelCounty","ParcelAcres",
                                                "nearest_ParcelID", "nearest_dist_km", "nearest_ParcelAcres")]

#Export results to csv
filename <- paste0(path,"/misc/pge_prem_coord_polygon_parcels.csv")
write.csv(prems_out, file=filename , row.names=FALSE, quote=FALSE)




##############################################################
### 3. Assign SP lat/lons to Parcels (CLU-matched parcels) ###
##############################################################

#Reset list of SPs
prems2_sf <- st_as_sf(prems)
st_crs(prems2_sf) <- st_crs(Parcels_conc_sf)

#Assign each lat/lon to the polygon it's contained in
prems2_sf$in_parcel_row <- sapply(st_intersects(prems2_sf,Parcels_conc_sf), function(z) if (length(z)==0) NA_integer_ else z[1])

#Package results
prems2_sf$in_parcel <- is.na(prems2_sf$in_parcel_row)==0
prems2_sf$ParcelID <- Parcels_conc_sf$ParcelID[prems2_sf$in_parcel_row]
prems2_sf$ParcelCounty <- Parcels_conc_sf$County[prems2_sf$in_parcel_row]
prems2_sf$ParcelAcres <- Parcels_conc_sf$ParcelAcres[prems2_sf$in_parcel_row]

#Save temporary results
saveRDS(prems2_sf, paste0(path,"/misc/temp3_prems_sf_in_parcels.RDS"))
prems2_sf <- readRDS(paste0(path,"/misc/temp3_prems_sf_in_parcels.RDS"))

#Create vector of only those observations where parcel is missing
n <- nrow(prems2_sf)
missings <- c(1:n)[(is.na(prems2_sf$in_parcel_row) & is.na(prems2_sf$county)==0)]

#Convert into data.table, which is faster for the nearest distance function
prems2_dt <- as.data.table(prems2_sf[,names(prems2_sf) %in% 
                                     c("sp_uuid","county","geometry","in_parcel_row","in_parcel")])

#Convert polygons to lines (equivalent + faster for calculating minimum distance), and data.table
#Parcels_conc_dt <- st_cast(Parcels_conc_sf, 'MULTILINESTRING') #this breaks for some reason
#Parcels_conc_dt <- as.data.table(Parcels_conc_dt)
Parcels_conc_dt <- as.data.table(Parcels_conc_sf)

#Function to calculate distance to a parcel polygon, for lat/lons not contained in a polygon
nearestFXN <- function(i) {
  
  # create single-row data table of prem lat/lon
  temp_prems2_dt <- prems2_dt[i,]
  
  # extract county for missing i
  #temp_county <- gsub(" ", "", temp_prems2_dt$county) 
  
  # create county-specific polygons object (actually a line data.table)
  #temp_Parcels_conc_dt <- get(paste0("Parcels_conc_dt_",temp_county)) 
  
  # find ID nearest polygon (actually line) in county
  temp_row <- st_nearest_feature(temp_prems2_dt$geometry, Parcels_conc_dt$geometry) 
  
  # assign index of nearest polygon (actually line)
  out_id <- as.character(Parcels_conc_dt[temp_row,]$ParcelID)
  
  # convert single-row prems_dt into SF object, and add CRS units
  temp_prems2_sf <- st_as_sf(temp_prems2_dt)
  temp_prems2_sf <- st_set_crs(temp_prems2_sf, crs) 
  
  # convert nearest polygon into single-row SF object, and add CRS units
  temp_Parcels_conc_sf <- st_as_sf(Parcels_conc_dt[temp_row,])
  temp_Parcels_conc_sf <- st_set_crs(temp_Parcels_conc_sf, crs) 
  
  # find distance to nearest parcel (in meters)
  out_dist <- st_distance(temp_prems2_sf, temp_Parcels_conc_sf, by_element=TRUE)
  
  # convert distance to kilometers
  out_dist <- as.numeric(out_dist/1000)
  
  # package output
  out <- c(i,out_id,out_dist)
  names(out) <- c("prems2_row","nearest_ID","nearest_dist_km")
  return(out)
  gc()
}

#Calculate distance to a parcel polygon, for lat/lons not contained in a polygon
cl <- makeCluster(24) #(cores - 1)
clusterEvalQ(cl, library(sf))
clusterEvalQ(cl, library(data.table))
clusterSetRNGStream(cl, 12345)
clusterExport(cl=cl, varlist=c('nearestFXN','prems2_dt','crs','Parcels_conc_dt'))
nearestFXN_out2 <- as.data.frame(t(parSapply(cl=cl, missings, function(x) nearestFXN(x))))
stopCluster(cl)

#Save temporary results
saveRDS(nearestFXN_out2, paste0(path,"/misc/temp4_prems_sf_in_parcels.RDS"))
nearestFXN_out2 <- readRDS(paste0(path,"/misc/temp4_prems_sf_in_parcels.RDS"))

#Fix data types
nearestFXN_out2$prems2_row <- as.integer(as.character(nearestFXN_out2$prems2_row))
nearestFXN_out2$nearest_dist_km <- as.numeric(as.character(nearestFXN_out2$nearest_dist_km))

#Merge in areas
nearestFXN_out2 <- left_join(nearestFXN_out2, Parcels_conc_sf, by=c("nearest_ID"="ParcelID"))
nearestFXN_out2 <- nearestFXN_out2[,names(nearestFXN_out2) %in% c("prems2_row","nearest_ID","nearest_dist_km","ParcelAcres","County")]
names(nearestFXN_out2)[2] <- "nearest_ParcelID"
names(nearestFXN_out2)[4] <- "nearest_ParcelCounty"
names(nearestFXN_out2)[5] <- "nearest_ParcelAcres"

#Expand nearest outputs into full size
n_vect <- as.data.frame(c(1:n))
names(n_vect) <- "prems2_row"
nearestFXN_out2_expanded <- left_join(n_vect, nearestFXN_out2, by="prems2_row")

#Tranfer nearest outcomes to main dataset
prems_out2 <- st_drop_geometry(prems2_sf)
prems_out2 <- cbind(prems_out2,nearestFXN_out2_expanded)

#Diagnostics
summary(as.numeric(prems_out2$in_parcel))
summary(as.numeric(prems_out2$in_parcel[prems_out2$bad_geocode_flag==0]))
summary(as.numeric(prems_out2$in_parcel[prems_out2$bad_geocode_flag==0 & prems_out2$pull=="20180719"]))
summary(prems_out2[prems_out2$in_parcel==0,]$nearest_dist_km)
summary(prems_out2[prems_out2$in_parcel==0 & prems_out2$bad_geocode_flag==0 & prems_out2$pull=="20180719",]$nearest_dist_km)

#Drop extraneous variables
prems_out2 <- prems_out2[,names(prems_out2) %in% c("sp_uuid","prem_lat","prem_long","bad_geocode_flag","pull",
                                                   "in_parcel","ParcelID","ParcelCounty","ParcelAcres",
                                                   "nearest_ParcelID", "nearest_dist_km", "nearest_ParcelCounty","nearest_ParcelAcres")]

#Export results to csv
filename <- paste0(path,"/misc/pge_prem_coord_polygon_parcels_conc.csv")
write.csv(prems_out2, file=filename , row.names=FALSE, quote=FALSE)


################################################################
### 4. Assign APEP lat/lons to Parcels (full set of parcels) ###
################################################################

#Read pump coordinates
setwd(paste0(path,"/misc"))
pumps <- read.delim2("apep_pump_coord.txt",header=TRUE,sep=",",stringsAsFactors=FALSE)
pumps$longitude <- as.numeric(pumps$pump_lon)
pumps$latitude <- as.numeric(pumps$pump_lat)

#Merge in assigned counties
pumps_counties <- read.delim2("apep_pump_coord_polygon_counties.txt",header=TRUE,sep="%",stringsAsFactors=FALSE)
pumps_counties <- pumps_counties[,names(pumps_counties) %in% c("latlon_group","county")]
stopifnot(pumps$latlon_group==pumps_counties$latlon_group) # confirm merge by row id is good
pumps$county <- pumps_counties$county
rm(pumps_counties)

#Convert to SF object
coordinates(pumps) <- ~ longitude + latitude
pumps_sf  <- st_as_sf(pumps)
st_crs(pumps_sf) <- st_crs(Parcels_sf)

#Assign each lat/lon to the polygon it's contained in
pumps_sf$in_parcel_row <- sapply(st_intersects(pumps_sf,Parcels_sf), function(z) if (length(z)==0) NA_integer_ else z[1])

#Package results
pumps_sf$in_parcel <- is.na(pumps_sf$in_parcel_row)==0
pumps_sf$ParcelID <- Parcels_sf$ParcelID[pumps_sf$in_parcel_row]
pumps_sf$ParcelCounty <- Parcels_sf$County[pumps_sf$in_parcel_row]
pumps_sf$ParcelAcres <- Parcels_sf$ParcelAcres[pumps_sf$in_parcel_row]

#Save temporary results
saveRDS(pumps_sf, paste0(path,"/misc/temp1_pumps_sf_in_parcels.RDS"))
pumps_sf <- readRDS(paste0(path,"/misc/temp1_pumps_sf_in_parcels.RDS"))

#Create vector of only those observations where parcel is missing
n <- nrow(pumps_sf)
missings <- c(1:n)[(is.na(pumps_sf$in_parcel_row) & is.na(pumps_sf$county)==0)]

#Convert into data.table, which is faster for the nearest distance function
pumps_dt <- as.data.table(pumps_sf[,names(pumps_sf) %in% 
                                     c("latlon_group","county","geometry","in_parcel_row","in_parcel")])

#Function to calculate distance to a parcel polygon, for lat/lons not contained in a polygon
nearestFXN <- function(i) {
  
  # create single-row data table of pump lat/lon
  temp_pumps_dt <- pumps_dt[i,]
  
  # extract county for missing i
  temp_county <- gsub(" ", "", temp_pumps_dt$county) 
  
  # create county-specific polygons object (actually a line data.table)
  temp_Parcels_dt <- get(paste0("Parcels_dt_",temp_county)) 
  
  # find ID nearest polygon (actually line) in county
  temp_row <- st_nearest_feature(temp_pumps_dt$geometry, temp_Parcels_dt$geometry) 
  
  # assign index of nearest polygon (actually line)
  out_id <- as.character(temp_Parcels_dt[temp_row,]$ParcelID)
  
  # convert single-row pumps_dt into SF object, and add CRS units
  temp_pumps_sf <- st_as_sf(temp_pumps_dt)
  temp_pumps_sf <- st_set_crs(temp_pumps_sf, crs) 
  
  # convert nearest polygon into single-row SF object, and add CRS units
  temp_Parcels_sf <- st_as_sf(temp_Parcels_dt[temp_row,])
  temp_Parcels_sf <- st_set_crs(temp_Parcels_sf, crs) 
  
  # find distance to nearest parcel (in meters)
  out_dist <- st_distance(temp_pumps_sf, temp_Parcels_sf, by_element=TRUE)
  
  # convert distance to kilometers
  out_dist <- as.numeric(out_dist/1000)
  
  # package output
  out <- c(i,out_id,out_dist)
  names(out) <- c("pumps_row","nearest_ID","nearest_dist_km")
  return(out)
  gc()
}

#Calculate distance to a parcel polygon, for lat/lons not contained in a polygon
cl <- makeCluster(4) #(cores - 1)
clusterEvalQ(cl, library(sf))
clusterEvalQ(cl, library(data.table))
clusterSetRNGStream(cl, 12345)
clusterExport(cl=cl, varlist=c('nearestFXN','pumps_dt','crs','Parcels_dt_Alameda','Parcels_dt_Alpine',
                               'Parcels_dt_Amador','Parcels_dt_Butte','Parcels_dt_Calaveras','Parcels_dt_Colusa',
                               'Parcels_dt_ContraCosta','Parcels_dt_ElDorado','Parcels_dt_Fresno',
                               'Parcels_dt_Glenn','Parcels_dt_Humboldt','Parcels_dt_Kern','Parcels_dt_Kings',
                               'Parcels_dt_Lake','Parcels_dt_Lassen','Parcels_dt_LosAngeles','Parcels_dt_Madera',
                               'Parcels_dt_Marin','Parcels_dt_Mariposa','Parcels_dt_Mendocino','Parcels_dt_Merced',
                               'Parcels_dt_Monterey','Parcels_dt_Napa','Parcels_dt_Nevada','Parcels_dt_Placer',
                               'Parcels_dt_Plumas','Parcels_dt_Sacramento','Parcels_dt_SanBenito',
                               'Parcels_dt_SanFrancisco','Parcels_dt_SanJoaquin','Parcels_dt_SanLuisObispo',
                               'Parcels_dt_SanMateo','Parcels_dt_SantaBarbara','Parcels_dt_SantaClara',
                               'Parcels_dt_SantaCruz','Parcels_dt_Shasta','Parcels_dt_Sierra',
                               'Parcels_dt_Solano','Parcels_dt_Sonoma','Parcels_dt_Stanislaus','Parcels_dt_Sutter',
                               'Parcels_dt_Tehama','Parcels_dt_Trinity','Parcels_dt_Tulare','Parcels_dt_Tuolumne',
                               'Parcels_dt_Ventura','Parcels_dt_Yolo','Parcels_dt_Yuba'))
nearestFXN_out_pumps <- as.data.frame(t(parSapply(cl=cl, missings, function(x) nearestFXN(x))))
stopCluster(cl)

#Save temporary results
saveRDS(nearestFXN_out_pumps, paste0(path,"/misc/temp2_pumps_sf_in_parcels.RDS"))
nearestFXN_out_pumps <- readRDS(paste0(path,"/misc/temp2_pumps_sf_in_parcels.RDS"))

#Fix data types
nearestFXN_out_pumps$pumps_row <- as.integer(as.character(nearestFXN_out_pumps$pumps_row))
nearestFXN_out_pumps$nearest_dist_km <- as.numeric(as.character(nearestFXN_out_pumps$nearest_dist_km))

#Merge in areas
nearestFXN_out_pumps <- left_join(nearestFXN_out_pumps, Parcels_sf, by=c("nearest_ID"="ParcelID"))
nearestFXN_out_pumps <- nearestFXN_out_pumps[,names(nearestFXN_out_pumps) %in% c("pumps_row","nearest_ID","nearest_dist_km","ParcelAcres")]
names(nearestFXN_out_pumps)[2] <- "nearest_ParcelID"
names(nearestFXN_out_pumps)[4] <- "nearest_ParcelAcres"

#Expand nearest outputs into full size
n_vect <- as.data.frame(c(1:n))
names(n_vect) <- "pumps_row"
nearestFXN_out_pumps_expanded <- left_join(n_vect, nearestFXN_out_pumps, by="pumps_row")

#Tranfer nearest outcomes to main dataset
pumps_out <- st_drop_geometry(pumps_sf)
pumps_out <- cbind(pumps_out,nearestFXN_out_pumps_expanded)

#Diagnostics
summary(as.numeric(pumps_out$in_parcel))
summary(pumps_out[pumps_out$in_parcel==0,]$nearest_dist_km)

#Drop extraneous variables
pumps_out <- pumps_out[,names(pumps_out) %in% c("latlon_group","pump_lat","pump_long",
                                                "in_parcel","ParcelID","ParcelCounty","ParcelAcres",
                                                "nearest_ParcelID", "nearest_dist_km", "nearest_ParcelAcres")]

#Export results to csv
filename <- paste0(path,"/misc/apep_pump_coord_polygon_parcels.csv")
write.csv(pumps_out, file=filename , row.names=FALSE, quote=FALSE)



################################################################
### 5. Assign APEP lat/lons to Parcels (CLU-matched parcels) ###
################################################################

#Reset list of pumps
pumps2_sf <- st_as_sf(pumps)
st_crs(pumps2_sf) <- st_crs(Parcels_conc_sf)

#Assign each lat/lon to the polygon it's contained in
pumps2_sf$in_parcel_row <- sapply(st_intersects(pumps2_sf,Parcels_conc_sf), function(z) if (length(z)==0) NA_integer_ else z[1])

#Package results
pumps2_sf$in_parcel <- is.na(pumps2_sf$in_parcel_row)==0
pumps2_sf$ParcelID <- Parcels_conc_sf$ParcelID[pumps2_sf$in_parcel_row]
pumps2_sf$ParcelCounty <- Parcels_conc_sf$County[pumps2_sf$in_parcel_row]
pumps2_sf$ParcelAcres <- Parcels_conc_sf$ParcelAcres[pumps2_sf$in_parcel_row]

#Save temporary results
saveRDS(pumps2_sf, paste0(path,"/misc/temp3_pumps_sf_in_parcels.RDS"))
pumps2_sf <- readRDS(paste0(path,"/misc/temp3_pumps_sf_in_parcels.RDS"))

#Create vector of only those observations where parcel is missing
n <- nrow(pumps2_sf)
missings <- c(1:n)[(is.na(pumps2_sf$in_parcel_row) & is.na(pumps2_sf$county)==0)]

#Convert into data.table, which is faster for the nearest distance function
pumps2_dt <- as.data.table(pumps2_sf[,names(pumps2_sf) %in% 
                                       c("sp_uuid","county","geometry","in_parcel_row","in_parcel")])

#Convert polygons to lines (equivalent + faster for calculating minimum distance), and data.table
#Parcels_conc_dt <- st_cast(Parcels_conc_sf, 'MULTILINESTRING') #this breaks for some reason?
#Parcels_conc_dt <- as.data.table(Parcels_conc_dt)
Parcels_conc_dt <- as.data.table(Parcels_conc_sf)

#Function to calculate distance to a parcel polygon, for lat/lons not contained in a polygon
nearestFXN <- function(i) {
  
  # create single-row data table of prem lat/lon
  temp_pumps2_dt <- pumps2_dt[i,]
  
  # extract county for missing i
  #temp_county <- gsub(" ", "", temp_pumps2_dt$county) 
  
  # create county-specific polygons object (actually a line data.table)
  #temp_Parcels_conc_dt <- get(paste0("Parcels_conc_dt_",temp_county)) 
  
  # find ID nearest polygon (actually line) in county
  temp_row <- st_nearest_feature(temp_pumps2_dt$geometry, Parcels_conc_dt$geometry) 
  
  # assign index of nearest polygon (actually line)
  out_id <- as.character(Parcels_conc_dt[temp_row,]$ParcelID)
  
  # convert single-row prems_dt into SF object, and add CRS units
  temp_pumps2_sf <- st_as_sf(temp_pumps2_dt)
  temp_pumps2_sf <- st_set_crs(temp_pumps2_sf, crs) 
  
  # convert nearest polygon into single-row SF object, and add CRS units
  temp_Parcels_conc_sf <- st_as_sf(Parcels_conc_dt[temp_row,])
  temp_Parcels_conc_sf <- st_set_crs(temp_Parcels_conc_sf, crs) 
  
  # find distance to nearest parcel (in meters)
  out_dist <- st_distance(temp_pumps2_sf, temp_Parcels_conc_sf, by_element=TRUE)
  
  # convert distance to kilometers
  out_dist <- as.numeric(out_dist/1000)
  
  # package output
  out <- c(i,out_id,out_dist)
  names(out) <- c("pumps2_row","nearest_ID","nearest_dist_km")
  return(out)
  gc()
}

#Calculate distance to a parcel polygon, for lat/lons not contained in a polygon
cl <- makeCluster(24) #(cores - 1)
clusterEvalQ(cl, library(sf))
clusterEvalQ(cl, library(data.table))
clusterSetRNGStream(cl, 12345)
clusterExport(cl=cl, varlist=c('nearestFXN','pumps2_dt','crs','Parcels_conc_dt'))
nearestFXN_out_pumps2 <- as.data.frame(t(parSapply(cl=cl, missings, function(x) nearestFXN(x))))
stopCluster(cl)

#Save temporary results
saveRDS(nearestFXN_out_pumps2, paste0(path,"/misc/temp4_pumps_sf_in_parcels.RDS"))
nearestFXN_out_pumps2 <- readRDS(paste0(path,"/misc/temp4_pumps_sf_in_parcels.RDS"))

#Fix data types
nearestFXN_out_pumps2$pumps2_row <- as.integer(as.character(nearestFXN_out_pumps2$pumps2_row))
nearestFXN_out_pumps2$nearest_dist_km <- as.numeric(as.character(nearestFXN_out_pumps2$nearest_dist_km))

#Merge in areas
nearestFXN_out_pumps2 <- left_join(nearestFXN_out_pumps2, Parcels_sf, by=c("nearest_ID"="ParcelID"))
nearestFXN_out_pumps2 <- nearestFXN_out_pumps2[,names(nearestFXN_out_pumps2) %in% c("pumps2_row","nearest_ID","nearest_dist_km","County","ParcelAcres")]
names(nearestFXN_out_pumps2)[2] <- "nearest_ParcelID"
names(nearestFXN_out_pumps2)[4] <- "nearest_ParcelCounty"
names(nearestFXN_out_pumps2)[5] <- "nearest_ParcelAcres"

#Expand nearest outputs into full size
n_vect <- as.data.frame(c(1:n))
names(n_vect) <- "pumps2_row"
nearestFXN_out_pumps2_expanded <- left_join(n_vect, nearestFXN_out_pumps2, by="pumps2_row")

#Tranfer nearest outcomes to main dataset
pumps_out2 <- st_drop_geometry(pumps2_sf)
pumps_out2 <- cbind(pumps_out2,nearestFXN_out_pumps2_expanded)

#Diagnostics
summary(as.numeric(pumps_out2$in_parcel))
summary(pumps_out2[pumps_out2$in_parcel==0,]$nearest_dist_km)

#Drop extraneous variables
pumps_out2 <- pumps_out2[,names(pumps_out2) %in% c("latlon_group","pump_lat","pump_long",
                                                   "in_parcel","ParcelID","ParcelCounty","ParcelAcres",
                                                   "nearest_ParcelID","nearest_dist_km","nearest_ParcelCounty","nearest_ParcelAcres")]

#Export results to csv
filename <- paste0(path,"/misc/apep_pump_coord_polygon_parcels_conc.csv")
write.csv(pumps_out2, file=filename , row.names=FALSE, quote=FALSE)
