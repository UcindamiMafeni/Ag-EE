#######################################################
#  Script to assign SP premises and APEP pumps to     #
#  Common Land Units (i.e. static USDA "fields")      #
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

#Load CLU SF data frame
setwd(paste0(path,"/cleaned_spatial/CLU"))
CLUs_sf <- readRDS("clu.RDS")
crs <- st_crs(CLUs_sf)

#Export list of CLUs 
setwd(paste0(path,"/misc"))
CLUs_data <- CLUs_sf
st_geometry(CLUs_data) <- NULL
filename <- "CLUs_cleaned.csv"
write.csv(CLUs_data, file=filename , row.names=FALSE, quote=FALSE)
rm(CLUs_data)

#Load CA state outline
setwd(paste0(path,"/spatial"))
CAoutline <- readOGR(dsn = "State", layer = "CA_State_TIGER2016")
CAoutline_sf <- st_as_sf(CAoutline)
CAoutline_sf <- st_transform(CAoutline_sf, crs)
rm(CAoutline)

#Load list of ever-crop CLUs
CLUs_evercrop_list <- as.data.frame(read.csv(paste0(path,"/misc/ever_crop_clus.csv")))
names(CLUs_evercrop_list) <- "CLU_ID"
stopifnot(nrow(CLUs_evercrop_list)==nrow(unique(CLUs_evercrop_list))) #confirm uniqueness

#Right-join list of ever-crop CLUs to full SF object
CLUs_ec_sf <- right_join(CLUs_sf,CLUs_evercrop_list, by="CLU_ID")


################################################
### 2. Assign SP lat/lons to CLUs (all CLUs) ###
################################################

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
st_crs(prems_sf) <- st_crs(CLUs_sf)

#Assign each lat/lon to the polygon it's contained in
prems_sf$in_clu_row <- sapply(st_intersects(prems_sf,CLUs_sf), function(z) if (length(z)==0) NA_integer_ else z[1])

#Package results
prems_sf$in_clu <- is.na(prems_sf$in_clu_row)==0
prems_sf$CLU_ID <- CLUs_sf$CLU_ID[prems_sf$in_clu_row]
prems_sf$CLUCounty <- CLUs_sf$County[prems_sf$in_clu_row]
prems_sf$CLUAcres <- CLUs_sf$CLUAcres[prems_sf$in_clu_row]

#Save temporary results
saveRDS(prems_sf, paste0(path,"/misc/temp1_prems_sf_in_clus.RDS"))
prems_sf <- readRDS(paste0(path,"/misc/temp1_prems_sf_in_clus.RDS"))

#Create vector of only those observations where CLU is missing
n <- nrow(prems_sf)
missings <- c(1:n)[(is.na(prems_sf$in_clu_row) & is.na(prems_sf$county)==0)]

#Convert into data.table, which is faster for the nearest distance function
prems_dt <- as.data.table(prems_sf[,names(prems_sf) %in% 
                                     c("sp_uuid","county","geometry","in_clu_row","in_clu")])

#Convert polygons to lines (equivalent + faster for calculating minimum distance), and data.table
CLUs_dt <- st_cast(CLUs_sf, 'MULTILINESTRING')
CLUs_dt <- as.data.table(CLUs_dt)

#Function to calculate distance to a CLU polygon, for lat/lons not contained in a polygon
nearestFXN <- function(i) {
  
  # create single-row data table of prem lat/lon
  temp_prems_dt <- prems_dt[i,]
  
  # extract county for missing i
  #temp_county <- gsub(" ", "", temp_prems2_dt$county) 
  
  # create county-specific polygons object (actually a line data.table)
  #temp_CLUs_dt <- get(paste0("CLUs_dt_",temp_county)) 
  
  # find ID nearest polygon (actually line) in county
  temp_row <- st_nearest_feature(temp_prems_dt$geometry, CLUs_dt$geometry) 
  
  # assign index of nearest polygon (actually line)
  out_id <- as.character(CLUs_dt[temp_row,]$CLU_ID)
  
  # convert single-row prems_dt into SF object, and add CRS units
  temp_prems_sf <- st_as_sf(temp_prems_dt)
  temp_prems_sf <- st_set_crs(temp_prems_sf, crs) 
  
  # convert nearest polygon into single-row SF object, and add CRS units
  temp_CLUs_sf <- st_as_sf(CLUs_dt[temp_row,])
  temp_CLUs_sf <- st_set_crs(temp_CLUs_sf, crs) 
  
  # find distance to nearest CLU (in meters)
  out_dist <- st_distance(temp_prems_sf, temp_CLUs_sf, by_element=TRUE)
  
  # convert distance to kilometers
  out_dist <- as.numeric(out_dist/1000)
  
  # package output
  out <- c(i,out_id,out_dist)
  names(out) <- c("prems_row","nearest_ID","nearest_dist_km")
  return(out)
  gc()
}

#Calculate distance to a CLU polygon, for lat/lons not contained in a polygon
cl <- makeCluster(20) #(cores - 1)
clusterEvalQ(cl, library(sf))
clusterEvalQ(cl, library(data.table))
clusterSetRNGStream(cl, 12345)
clusterExport(cl=cl, varlist=c('nearestFXN','prems_dt','crs','CLUs_dt'))
nearestFXN_out <- as.data.frame(t(parSapply(cl=cl, missings, function(x) nearestFXN(x))))
stopCluster(cl)

#Save temporary results
saveRDS(nearestFXN_out, paste0(path,"/misc/temp2_prems_sf_in_clus.RDS"))
nearestFXN_out <- readRDS(paste0(path,"/misc/temp2_prems_sf_in_clus.RDS"))

#Fix data types
nearestFXN_out$prems_row <- as.integer(as.character(nearestFXN_out$prems_row))
nearestFXN_out$nearest_dist_km <- as.numeric(as.character(nearestFXN_out$nearest_dist_km))

#Merge in areas
nearestFXN_out <- left_join(nearestFXN_out, CLUs_sf, by=c("nearest_ID"="CLU_ID"))
nearestFXN_out <- nearestFXN_out[,names(nearestFXN_out) %in% c("prems_row","nearest_ID","nearest_dist_km","County","CLUAcres")]
names(nearestFXN_out)[2] <- "nearest_CLU_ID"
names(nearestFXN_out)[5] <- "nearest_CLUCounty"
names(nearestFXN_out)[4] <- "nearest_CLUAcres"

#Expand nearest outputs into full size
n_vect <- as.data.frame(c(1:n))
names(n_vect) <- "prems_row"
nearestFXN_out_expanded <- left_join(n_vect, nearestFXN_out, by="prems_row")

#Tranfer nearest outcomes to main dataset
prems_out <- st_drop_geometry(prems_sf)
prems_out <- cbind(prems_out,nearestFXN_out_expanded)

#Diagnostics
summary(as.numeric(prems_out$in_clu))
summary(as.numeric(prems_out$in_clu[prems_out$bad_geocode_flag==0]))
summary(as.numeric(prems_out$in_clu[prems_out$bad_geocode_flag==0 & prems_out$pull=="20180719"]))
summary(prems_out[prems_out$in_clu==0,]$nearest_dist_km)
summary(prems_out[prems_out$in_clu==0 & prems_out$bad_geocode_flag==0 & prems_out$pull=="20180719",]$nearest_dist_km)

#Drop extraneous variables
prems_out <- prems_out[,names(prems_out) %in% c("sp_uuid","prem_lat","prem_long","bad_geocode_flag","pull",
                                                "in_clu","CLU_ID","CLUCounty","CLUAcres",
                                                "nearest_CLU_ID","nearest_dist_km","nearest_CLUCounty","nearest_CLUAcres")]

#Export results to csv
filename <- paste0(path,"/misc/pge_prem_coord_polygon_clu.csv")
write.csv(prems_out, file=filename , row.names=FALSE, quote=FALSE)


######################################################
### 3. Assign SP lat/lons to CLUs (ever-crop CLUs) ###
######################################################

#Convert to SF object
prems2_sf  <- st_as_sf(prems)
st_crs(prems2_sf) <- st_crs(CLUs_ec_sf)

#Assign each lat/lon to the polygon it's contained in
prems2_sf$in_clu_row <- sapply(st_intersects(prems2_sf,CLUs_ec_sf), function(z) if (length(z)==0) NA_integer_ else z[1])

#Package results
prems2_sf$in_clu <- is.na(prems2_sf$in_clu_row)==0
prems2_sf$CLU_ID <- CLUs_ec_sf$CLU_ID[prems2_sf$in_clu_row]
prems2_sf$CLUCounty <- CLUs_ec_sf$County[prems2_sf$in_clu_row]
prems2_sf$CLUAcres <- CLUs_ec_sf$CLUAcres[prems2_sf$in_clu_row]

#Save temporary results
saveRDS(prems2_sf, paste0(path,"/misc/temp3_prems_sf_in_clus.RDS"))
prems2_sf <- readRDS(paste0(path,"/misc/temp3_prems_sf_in_clus.RDS"))

#Create vector of only those observations where CLU is missing
n <- nrow(prems2_sf)
missings <- c(1:n)[(is.na(prems2_sf$in_clu_row) & is.na(prems2_sf$county)==0)]

#Convert into data.table, which is faster for the nearest distance function
prems2_dt <- as.data.table(prems2_sf[,names(prems2_sf) %in% 
                                     c("sp_uuid","county","geometry","in_clu_row","in_clu")])

#Convert polygons to lines (equivalent + faster for calculating minimum distance), and data.table
CLUs_ec_dt <- st_cast(CLUs_ec_sf, 'MULTILINESTRING')
CLUs_ec_dt <- as.data.table(CLUs_ec_dt)

#Function to calculate distance to a CLU polygon, for lat/lons not contained in a polygon
nearestFXN <- function(i) {
  
  # create single-row data table of prem lat/lon
  temp_prems2_dt <- prems2_dt[i,]
  
  # extract county for missing i
  #temp_county <- gsub(" ", "", temp_prems2_dt$county) 
  
  # create county-specific polygons object (actually a line data.table)
  #temp_CLUs_ec_dt <- get(paste0("CLUs_ec_dt_",temp_county)) 
  
  # find ID nearest polygon (actually line) in county
  temp_row <- st_nearest_feature(temp_prems2_dt$geometry, CLUs_ec_dt$geometry) 
  
  # assign index of nearest polygon (actually line)
  out_id <- as.character(CLUs_ec_dt[temp_row,]$CLU_ID)
  
  # convert single-row prems_dt into SF object, and add CRS units
  temp_prems2_sf <- st_as_sf(temp_prems2_dt)
  temp_prems2_sf <- st_set_crs(temp_prems2_sf, crs) 
  
  # convert nearest polygon into single-row SF object, and add CRS units
  temp_CLUs_ec_sf <- st_as_sf(CLUs_ec_dt[temp_row,])
  temp_CLUs_ec_sf <- st_set_crs(temp_CLUs_ec_sf, crs) 
  
  # find distance to nearest CLU (in meters)
  out_dist <- st_distance(temp_prems2_sf, temp_CLUs_ec_sf, by_element=TRUE)
  
  # convert distance to kilometers
  out_dist <- as.numeric(out_dist/1000)
  
  # package output
  out <- c(i,out_id,out_dist)
  names(out) <- c("prems2_row","nearest_ID","nearest_dist_km")
  return(out)
  gc()
}

#Calculate distance to a CLU polygon, for lat/lons not contained in a polygon
cl <- makeCluster(20) #(cores - 1)
clusterEvalQ(cl, library(sf))
clusterEvalQ(cl, library(data.table))
clusterSetRNGStream(cl, 12345)
clusterExport(cl=cl, varlist=c('nearestFXN','prems2_dt','crs','CLUs_ec_dt'))
nearestFXN_out2 <- as.data.frame(t(parSapply(cl=cl, missings, function(x) nearestFXN(x))))
stopCluster(cl)

#Save temporary results
saveRDS(nearestFXN_out2, paste0(path,"/misc/temp4_prems_sf_in_clus.RDS"))
nearestFXN_out2 <- readRDS(paste0(path,"/misc/temp4_prems_sf_in_clus.RDS"))

#Fix data types
nearestFXN_out2$prems2_row <- as.integer(as.character(nearestFXN_out2$prems2_row))
nearestFXN_out2$nearest_dist_km <- as.numeric(as.character(nearestFXN_out2$nearest_dist_km))

#Merge in areas
nearestFXN_out2 <- left_join(nearestFXN_out2, CLUs_ec_sf, by=c("nearest_ID"="CLU_ID"))
nearestFXN_out2 <- nearestFXN_out2[,names(nearestFXN_out2) %in% c("prems2_row","nearest_ID","nearest_dist_km","County","CLUAcres")]
names(nearestFXN_out2)[2] <- "nearest_CLU_ID"
names(nearestFXN_out2)[5] <- "nearest_CLUCounty"
names(nearestFXN_out2)[4] <- "nearest_CLUAcres"

#Expand nearest outputs into full size
n_vect <- as.data.frame(c(1:n))
names(n_vect) <- "prems2_row"
nearestFXN_out2_expanded <- left_join(n_vect, nearestFXN_out2, by="prems2_row")

#Tranfer nearest outcomes to main dataset
prems_out2 <- st_drop_geometry(prems2_sf)
prems_out2 <- cbind(prems_out2,nearestFXN_out2_expanded)

#Diagnostics
summary(as.numeric(prems_out2$in_clu))
summary(as.numeric(prems_out2$in_clu[prems_out2$bad_geocode_flag==0]))
summary(as.numeric(prems_out2$in_clu[prems_out2$bad_geocode_flag==0 & prems_out2$pull=="20180719"]))
summary(prems_out2[prems_out2$in_clu==0,]$nearest_dist_km)
summary(prems_out2[prems_out2$in_clu==0 & prems_out2$bad_geocode_flag==0 & prems_out2$pull=="20180719",]$nearest_dist_km)

#Drop extraneous variables
prems_out2 <- prems_out2[,names(prems_out2) %in% c("sp_uuid","prem_lat","prem_long","bad_geocode_flag","pull",
                                                   "in_clu","CLU_ID","CLUCounty","CLUAcres",
                                                   "nearest_CLU_ID","nearest_dist_km","nearest_CLUCounty","nearest_CLUAcres")]

#Export results to csv
filename <- paste0(path,"/misc/pge_prem_coord_polygon_clu_ever_crop.csv")
write.csv(prems_out2, file=filename , row.names=FALSE, quote=FALSE)


##################################################
### 4. Assign APEP lat/lons to CLUs (all CLUs) ###
##################################################

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
st_crs(pumps_sf) <- st_crs(CLUs_sf)

#Assign each lat/lon to the polygon it's contained in
pumps_sf$in_clu_row <- sapply(st_intersects(pumps_sf,CLUs_sf), function(z) if (length(z)==0) NA_integer_ else z[1])

#Package results
pumps_sf$in_clu <- is.na(pumps_sf$in_clu_row)==0
pumps_sf$CLU_ID <- CLUs_sf$CLU_ID[pumps_sf$in_clu_row]
pumps_sf$CLUCounty <- CLUs_sf$County[pumps_sf$in_clu_row]
pumps_sf$CLUAcres <- CLUs_sf$CLUAcres[pumps_sf$in_clu_row]

#Save temporary results
saveRDS(pumps_sf, paste0(path,"/misc/temp1_pumps_sf_in_clus.RDS"))
pumps_sf <- readRDS(paste0(path,"/misc/temp1_pumps_sf_in_clus.RDS"))

#Create vector of only those observations where CLU is missing
n <- nrow(pumps_sf)
missings <- c(1:n)[(is.na(pumps_sf$in_clu_row) & is.na(pumps_sf$county)==0)]

#Convert into data.table, which is faster for the nearest distance function
pumps_dt <- as.data.table(pumps_sf[,names(pumps_sf) %in% 
                                     c("latlon_group","county","geometry","in_clu_row","in_clu")])

#Convert polygons to lines (equivalent + faster for calculating minimum distance), and data.table
CLUs_dt <- st_cast(CLUs_sf, 'MULTILINESTRING')
CLUs_dt <- as.data.table(CLUs_dt)

#Function to calculate distance to a CLU polygon, for lat/lons not contained in a polygon
nearestFXN <- function(i) {
  
  # create single-row data table of prem lat/lon
  temp_pumps_dt <- pumps_dt[i,]
  
  # extract county for missing i
  #temp_county <- gsub(" ", "", temp_pumps2_dt$county) 
  
  # create county-specific polygons object (actually a line data.table)
  #temp_CLUs_dt <- get(paste0("CLUs_dt_",temp_county)) 
  
  # find ID nearest polygon (actually line) in county
  temp_row <- st_nearest_feature(temp_pumps_dt$geometry, CLUs_dt$geometry) 
  
  # assign index of nearest polygon (actually line)
  out_id <- as.character(CLUs_dt[temp_row,]$CLU_ID)
  
  # convert single-row pumps_dt into SF object, and add CRS units
  temp_pumps_sf <- st_as_sf(temp_pumps_dt)
  temp_pumps_sf <- st_set_crs(temp_pumps_sf, crs) 
  
  # convert nearest polygon into single-row SF object, and add CRS units
  temp_CLUs_sf <- st_as_sf(CLUs_dt[temp_row,])
  temp_CLUs_sf <- st_set_crs(temp_CLUs_sf, crs) 
  
  # find distance to nearest CLU (in meters)
  out_dist <- st_distance(temp_pumps_sf, temp_CLUs_sf, by_element=TRUE)
  
  # convert distance to kilometers
  out_dist <- as.numeric(out_dist/1000)
  
  # package output
  out <- c(i,out_id,out_dist)
  names(out) <- c("pumps_row","nearest_ID","nearest_dist_km")
  return(out)
  gc()
}

#Calculate distance to a CLU polygon, for lat/lons not contained in a polygon
cl <- makeCluster(8) #(cores - 1)
clusterEvalQ(cl, library(sf))
clusterEvalQ(cl, library(data.table))
clusterSetRNGStream(cl, 12345)
clusterExport(cl=cl, varlist=c('nearestFXN','pumps_dt','crs','CLUs_dt'))
nearestFXN_out_pumps <- as.data.frame(t(parSapply(cl=cl, missings, function(x) nearestFXN(x))))
stopCluster(cl)

#Save temporary results
saveRDS(nearestFXN_out_pumps, paste0(path,"/misc/temp2_pumps_sf_in_clus.RDS"))
nearestFXN_out_pumps <- readRDS(paste0(path,"/misc/temp2_pumps_sf_in_clus.RDS"))

#Fix data types
nearestFXN_out_pumps$pumps_row <- as.integer(as.character(nearestFXN_out_pumps$pumps_row))
nearestFXN_out_pumps$nearest_dist_km <- as.numeric(as.character(nearestFXN_out_pumps$nearest_dist_km))

#Merge in areas
nearestFXN_out_pumps <- left_join(nearestFXN_out_pumps, CLUs_sf, by=c("nearest_ID"="CLU_ID"))
nearestFXN_out_pumps <- nearestFXN_out_pumps[,names(nearestFXN_out_pumps) %in% c("pumps_row","nearest_ID","nearest_dist_km","County","CLUAcres")]
names(nearestFXN_out_pumps)[2] <- "nearest_CLU_ID"
names(nearestFXN_out_pumps)[5] <- "nearest_CLUCounty"
names(nearestFXN_out_pumps)[4] <- "nearest_CLUAcres"

#Expand nearest outputs into full size
n_vect <- as.data.frame(c(1:n))
names(n_vect) <- "pumps_row"
nearestFXN_out_pumps_expanded <- left_join(n_vect, nearestFXN_out_pumps, by="pumps_row")

#Tranfer nearest outcomes to main dataset
pumps_out <- st_drop_geometry(pumps_sf)
pumps_out <- cbind(pumps_out,nearestFXN_out_pumps_expanded)

#Diagnostics
summary(as.numeric(pumps_out$in_clu))
summary(pumps_out[pumps_out$in_clu==0,]$nearest_dist_km)

#Drop extraneous variables
pumps_out <- pumps_out[,names(pumps_out) %in% c("latlon_group","pump_lat","pump_long",
                                                "in_clu","CLU_ID","CLUCounty","CLUAcres",
                                                "nearest_CLU_ID","nearest_dist_km","nearest_CLUCounty","nearest_CLUAcres")]

#Export results to csv
filename <- paste0(path,"/misc/apep_pump_coord_polygon_clu.csv")
write.csv(pumps_out, file=filename , row.names=FALSE, quote=FALSE)


########################################################
### 5. Assign APEP lat/lons to CLUs (ever-crop CLUs) ###
########################################################

#Convert to SF object
pumps2_sf  <- st_as_sf(pumps)
st_crs(pumps2_sf) <- st_crs(CLUs_ec_sf)

#Assign each lat/lon to the polygon it's contained in
pumps2_sf$in_clu_row <- sapply(st_intersects(pumps2_sf,CLUs_ec_sf), function(z) if (length(z)==0) NA_integer_ else z[1])

#Package results
pumps2_sf$in_clu <- is.na(pumps2_sf$in_clu_row)==0
pumps2_sf$CLU_ID <- CLUs_ec_sf$CLU_ID[pumps2_sf$in_clu_row]
pumps2_sf$CLUCounty <- CLUs_ec_sf$County[pumps2_sf$in_clu_row]
pumps2_sf$CLUAcres <- CLUs_ec_sf$CLUAcres[pumps2_sf$in_clu_row]

#Save temporary results
saveRDS(pumps2_sf, paste0(path,"/misc/temp3_pumps_sf_in_clus.RDS"))
pumps2_sf <- readRDS(paste0(path,"/misc/temp3_pumps_sf_in_clus.RDS"))

#Create vector of only those observations where CLU is missing
n <- nrow(pumps2_sf)
missings <- c(1:n)[(is.na(pumps2_sf$in_clu_row) & is.na(pumps2_sf$county)==0)]

#Convert into data.table, which is faster for the nearest distance function
pumps2_dt <- as.data.table(pumps2_sf[,names(pumps2_sf) %in% 
                                     c("latlon_group","county","geometry","in_clu_row","in_clu")])

#Convert polygons to lines (equivalent + faster for calculating minimum distance), and data.table
CLUs_ec_dt <- st_cast(CLUs_ec_sf, 'MULTILINESTRING')
CLUs_ec_dt <- as.data.table(CLUs_ec_dt)

#Function to calculate distance to a CLU polygon, for lat/lons not contained in a polygon
nearestFXN <- function(i) {
  
  # create single-row data table of prem lat/lon
  temp_pumps2_dt <- pumps2_dt[i,]
  
  # extract county for missing i
  #temp_county <- gsub(" ", "", temp_pumps2_dt$county) 
  
  # create county-specific polygons object (actually a line data.table)
  #temp_CLUs_ec_dt <- get(paste0("CLUs_ec_dt_",temp_county)) 
  
  # find ID nearest polygon (actually line) in county
  temp_row <- st_nearest_feature(temp_pumps2_dt$geometry, CLUs_ec_dt$geometry) 
  
  # assign index of nearest polygon (actually line)
  out_id <- as.character(CLUs_ec_dt[temp_row,]$CLU_ID)
  
  # convert single-row pumps_dt into SF object, and add CRS units
  temp_pumps2_sf <- st_as_sf(temp_pumps2_dt)
  temp_pumps2_sf <- st_set_crs(temp_pumps2_sf, crs) 
  
  # convert nearest polygon into single-row SF object, and add CRS units
  temp_CLUs_ec_sf <- st_as_sf(CLUs_ec_dt[temp_row,])
  temp_CLUs_ec_sf <- st_set_crs(temp_CLUs_ec_sf, crs) 
  
  # find distance to nearest CLU (in meters)
  out_dist <- st_distance(temp_pumps2_sf, temp_CLUs_ec_sf, by_element=TRUE)
  
  # convert distance to kilometers
  out_dist <- as.numeric(out_dist/1000)
  
  # package output
  out <- c(i,out_id,out_dist)
  names(out) <- c("pumps2_row","nearest_ID","nearest_dist_km")
  return(out)
  gc()
}

#Calculate distance to a CLU polygon, for lat/lons not contained in a polygon
cl <- makeCluster(12) #(cores - 1)
clusterEvalQ(cl, library(sf))
clusterEvalQ(cl, library(data.table))
clusterSetRNGStream(cl, 12345)
clusterExport(cl=cl, varlist=c('nearestFXN','pumps2_dt','crs','CLUs_ec_dt'))
nearestFXN_out_pumps2 <- as.data.frame(t(parSapply(cl=cl, missings, function(x) nearestFXN(x))))
stopCluster(cl)

#Save temporary results
saveRDS(nearestFXN_out_pumps2, paste0(path,"/misc/temp4_pumps_sf_in_clus.RDS"))
nearestFXN_out_pumps2 <- readRDS(paste0(path,"/misc/temp4_pumps_sf_in_clus.RDS"))

#Fix data types
nearestFXN_out_pumps2$pumps2_row <- as.integer(as.character(nearestFXN_out_pumps2$pumps2_row))
nearestFXN_out_pumps2$nearest_dist_km <- as.numeric(as.character(nearestFXN_out_pumps2$nearest_dist_km))

#Merge in areas
nearestFXN_out_pumps2 <- left_join(nearestFXN_out_pumps2, CLUs_ec_sf, by=c("nearest_ID"="CLU_ID"))
nearestFXN_out_pumps2 <- nearestFXN_out_pumps2[,names(nearestFXN_out_pumps2) %in% c("pumps2_row","nearest_ID","nearest_dist_km","County","CLUAcres")]
names(nearestFXN_out_pumps2)[2] <- "nearest_CLU_ID"
names(nearestFXN_out_pumps2)[5] <- "nearest_CLUCounty"
names(nearestFXN_out_pumps2)[4] <- "nearest_CLUAcres"

#Expand nearest outputs into full size
n_vect <- as.data.frame(c(1:n))
names(n_vect) <- "pumps2_row"
nearestFXN_out_pumps2_expanded <- left_join(n_vect, nearestFXN_out_pumps2, by="pumps2_row")

#Tranfer nearest outcomes to main dataset
pumps_out2 <- st_drop_geometry(pumps2_sf)
pumps_out2 <- cbind(pumps_out2,nearestFXN_out_pumps2_expanded)

#Diagnostics
summary(as.numeric(pumps_out2$in_clu))
summary(pumps_out2[pumps_out2$in_clu==0,]$nearest_dist_km)

#Drop extraneous variables
pumps_out2 <- pumps_out2[,names(pumps_out2) %in% c("latlon_group","pump_lat","pump_long",
                                                   "in_clu","CLU_ID","CLUCounty","CLUAcres",
                                                   "nearest_CLU_ID","nearest_dist_km","nearest_CLUCounty","nearest_CLUAcres")]

#Export results to csv
filename <- paste0(path,"/misc/apep_pump_coord_polygon_clu_ever_crop.csv")
write.csv(pumps_out2, file=filename , row.names=FALSE, quote=FALSE)

