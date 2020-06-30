#############################################################################
#  Script to assign PGE SP premises, PGE APEP pumps, and SCE SP premises to #
#  Common Land Units (i.e. static USDA "fields", our master spatial unit)   #
#############################################################################
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


####################################################
### 2. Assign PGE SP lat/lons to CLUs (all CLUs) ###
####################################################

#Read PGE coordinates
setwd(paste0(path,"/misc"))
prems <- read.delim2("pge_prem_coord_3pulls.txt",header=TRUE,sep=",",stringsAsFactors=FALSE)
prems$longitude <- as.numeric(prems$prem_lon)
prems$latitude <- as.numeric(prems$prem_lat)

#Convert to SF object
coordinates(prems) <- ~ longitude + latitude
prems_sf  <- st_as_sf(prems)
st_crs(prems_sf) <- st_crs(CLUs_sf)

#Assign each lat/lon to the polygon it's contained in
prems_sf$in_clu_row <- sapply(st_intersects(prems_sf,CLUs_sf), function(z) if (length(z)==0) NA_integer_ else z[1])

#Package results
prems_sf$in_clu <- is.na(prems_sf$in_clu_row)==0
prems_sf$CLU_ID <- CLUs_sf$CLU_ID[prems_sf$in_clu_row]
prems_sf$CLUAcres <- CLUs_sf$CLUAcres[prems_sf$in_clu_row]

#Create vector of only those observations where CLU is missing
n <- nrow(prems_sf)
missings <- c(1:n)[(is.na(prems_sf$in_clu_row))]
matches <- c(1:n)[(is.na(prems_sf$in_clu_row)==0)]

#Function to calculate distance to CLU polygon border, for lat/lons contained in a polygon
edgeFXN <- function(i) {
  
  # create single-row data table of prem lat/lon
  temp_prems_sf <- prems_sf[i,]
  
  # create single-row data table of matched CLU polygon
  temp_CLUs_sf <- CLUs_sf[temp_prems_sf$in_clu_row,]
  temp_CLUs_sf <- st_cast(temp_CLUs_sf, 'MULTILINESTRING')

  # find distance to edge of assigned CLU (in meters)
  out_dist <- st_distance(temp_prems_sf, temp_CLUs_sf) 
  
  # package output
  out <- c(i,out_dist)
  names(out) <- c("prems_row","edge_dist_m")
  return(out)
  gc()
}

#Convert into data.table, which is faster for the nearest feature function
prems_dt <- as.data.table(prems_sf[,names(prems_sf) %in% c("sp_uuid","geometry","in_clu_row","in_clu")])

#Convert polygons to lines (equivalent + faster for calculating minimum distance), and data.table
CLUs_dt <- st_cast(CLUs_sf, 'MULTILINESTRING')
CLUs_dt <- as.data.table(CLUs_dt)

#Function to calculate distance to neighboring CLU polygon, for lat/lons contained in a polygon
neighborFXN <- function(i) {
  
  # create single-row data table of prem lat/lon
  temp_prems_dt <- prems_dt[i,]
  
  # store row ID of assigned polygon
  temp_row <- temp_prems_dt$in_clu_row
  
  # remove nearest polygon from CLUs_dt
  CLUs_dt2 <- CLUs_dt[-c(temp_row), ] 
  
  # find ID of nearest neighboring polygon (actually line)
  temp_row2 <- st_nearest_feature(temp_prems_dt$geometry, CLUs_dt2$geometry) 
  
  # assign index of nearest neighboring polygon (actually line)
  out_id2 <- as.character(CLUs_dt2[temp_row2,]$CLU_ID)
  
  # convert single-row prems_dt into SF object, and add CRS units
  temp_prems_sf <- st_as_sf(temp_prems_dt)
  temp_prems_sf <- st_set_crs(temp_prems_sf, crs) 
  
  # convert nearest neighboring polygon into single-row SF object, and add CRS units
  temp_CLUs_sf2 <- st_as_sf(CLUs_dt2[temp_row2,])
  temp_CLUs_sf2 <- st_set_crs(temp_CLUs_sf2, crs) 
  
  # find distance to nearest neighboring CLU (in meters)
  out_dist2 <- st_distance(temp_prems_sf, temp_CLUs_sf2, by_element=TRUE)
  
  # package output
  out <- c(i,out_id2,out_dist2)
  names(out) <- c("prems_row","neighbor_ID","neighbor_dist_m")
  return(out)
  gc()
}

#Function to calculate distance to a CLU polygon, for lat/lons *not* contained in a polygon
nearestFXN <- function(i) {

  # create single-row data table of prem lat/lon
  temp_prems_dt <- prems_dt[i,]
  
  # find ID of nearest polygon (actually line)
  temp_row <- st_nearest_feature(temp_prems_dt$geometry, CLUs_dt$geometry) 
  
  # assign index of nearest polygon (actually line)
  out_id <- as.character(CLUs_dt[temp_row,]$CLU_ID)
  
  # remove nearest polygon from CLUs_dt
  CLUs_dt2 <- CLUs_dt[-c(temp_row), ] 
  
  # find ID of 2nd-nearest polygon (actually line)
  temp_row2 <- st_nearest_feature(temp_prems_dt$geometry, CLUs_dt2$geometry) 
  
  # assign index of nearest polygon (actually line)
  out_id2 <- as.character(CLUs_dt2[temp_row2,]$CLU_ID)
  
  # convert single-row prems_dt into SF object, and add CRS units
  temp_prems_sf <- st_as_sf(temp_prems_dt)
  temp_prems_sf <- st_set_crs(temp_prems_sf, crs) 
  
  # convert nearest polygon into single-row SF object, and add CRS units
  temp_CLUs_sf <- st_as_sf(CLUs_dt[temp_row,])
  temp_CLUs_sf <- st_set_crs(temp_CLUs_sf, crs) 
  
  # convert 2nd-nearest polygon into single-row SF object, and add CRS units
  temp_CLUs_sf2 <- st_as_sf(CLUs_dt2[temp_row2,])
  temp_CLUs_sf2 <- st_set_crs(temp_CLUs_sf2, crs) 

  # find distance to nearest CLU (in meters)
  out_dist <- st_distance(temp_prems_sf, temp_CLUs_sf, by_element=TRUE)

  # find distance to 2nd-nearest CLU (in meters)
  out_dist2 <- st_distance(temp_prems_sf, temp_CLUs_sf2, by_element=TRUE)

  # package output
  out <- c(i,out_id,out_dist,out_id2,out_dist2)
  names(out) <- c("prems_row","nearest_ID","nearest_dist_m","nearest2_ID","nearest2_dist_m")
  return(out)
  gc()
}

#Execute 3 CPU-intensive GIS functions, each in parallel
cl <- makeCluster(24) #(cores - 1)
clusterEvalQ(cl, library(sf))
clusterEvalQ(cl, library(data.table))
clusterSetRNGStream(cl, 12345)
clusterExport(cl=cl, varlist=c('edgeFXN','prems_sf','CLUs_sf','neighborFXN','nearestFXN','prems_dt','crs','CLUs_dt'))
edgeFXN_out <- as.data.frame(t(parSapply(cl=cl, matches, function(x) edgeFXN(x))))
neighborFXN_out <- as.data.frame(t(parSapply(cl=cl, matches, function(x) neighborFXN(x))))
nearestFXN_out <- as.data.frame(t(parSapply(cl=cl, missings, function(x) nearestFXN(x))))
stopCluster(cl)

#Fix data types
neighborFXN_out$prems_row <- as.integer(as.character(neighborFXN_out$prems_row))
neighborFXN_out$neighbor_dist_m <- as.numeric(as.character(neighborFXN_out$neighbor_dist_m))
nearestFXN_out$prems_row <- as.integer(as.character(nearestFXN_out$prems_row))
nearestFXN_out$nearest_dist_m <- as.numeric(as.character(nearestFXN_out$nearest_dist_m))
nearestFXN_out$nearest2_dist_m <- as.numeric(as.character(nearestFXN_out$nearest2_dist_m))

#Merge in areas
neighborFXN_out <- left_join(neighborFXN_out, CLUs_sf, by=c("neighbor_ID"="CLU_ID"))
neighborFXN_out <- neighborFXN_out[,names(neighborFXN_out) %in% c("prems_row","neighbor_ID","neighbor_dist_m","CLUAcres")]
names(neighborFXN_out)[2] <- "neighbor_CLU_ID"
names(neighborFXN_out)[4] <- "neighbor_CLUAcres"
nearestFXN_out <- left_join(nearestFXN_out, CLUs_sf, by=c("nearest_ID"="CLU_ID"))
nearestFXN_out <- nearestFXN_out[,names(nearestFXN_out) %in% c("prems_row","nearest_ID","nearest_dist_m","CLUAcres","nearest2_ID","nearest2_dist_m")]
names(nearestFXN_out)[2] <- "nearest_CLU_ID"
names(nearestFXN_out)[6] <- "nearest_CLUAcres"
nearestFXN_out <- left_join(nearestFXN_out, CLUs_sf, by=c("nearest2_ID"="CLU_ID"))
nearestFXN_out <- nearestFXN_out[,names(nearestFXN_out) %in% c("prems_row","nearest_CLU_ID","nearest_dist_m","nearest_CLUAcres","nearest2_ID","nearest2_dist_m","CLUAcres")]
names(nearestFXN_out)[4] <- "nearest2_CLU_ID"
names(nearestFXN_out)[7] <- "nearest2_CLUAcres"

#Expand parellelized outputs into full size
n_vect <- as.data.frame(c(1:n))
names(n_vect) <- "prems_row"
edgeFXN_out_expanded <- left_join(n_vect, edgeFXN_out, by="prems_row")
neighborFXN_out_expanded <- left_join(n_vect, neighborFXN_out, by="prems_row")
nearestFXN_out_expanded <- left_join(n_vect, nearestFXN_out, by="prems_row")

#Tranfer nearest outcomes to main dataset
prems_out <- st_drop_geometry(prems_sf)
prems_out <- cbind(prems_out,edgeFXN_out_expanded,neighborFXN_out_expanded,nearestFXN_out_expanded)

#Diagnostics
summary(as.numeric(prems_out$in_clu))
summary(as.numeric(prems_out$in_clu[prems_out$bad_geocode_flag==0]))
summary(as.numeric(prems_out$in_clu[prems_out$bad_geocode_flag==0 & prems_out$pull=="20180719"]))
summary(prems_out[prems_out$in_clu==0,]$nearest_dist_m)
summary(prems_out[prems_out$in_clu==0 & prems_out$bad_geocode_flag==0 & prems_out$pull=="20180719",]$nearest_dist_m)

#Drop extraneous variables
prems_out <- prems_out[,names(prems_out) %in% c("sp_uuid","prem_lat","prem_long","bad_geocode_flag","pull",
                                                "in_clu","CLU_ID","CLUAcres","edge_dist_m",
                                                "neighbor_CLU_ID","neighbor_dist_m","neighbor_CLUAcres",
                                                "nearest_CLU_ID","nearest_dist_m","nearest_CLUAcres",
                                                "nearest2_CLU_ID","nearest2_dist_m","nearest2_CLUAcres")]

#Export results to csv
filename <- paste0(path,"/misc/pge_prem_coord_polygon_clu.csv")
write.csv(prems_out, file=filename , row.names=FALSE, quote=FALSE)


##########################################################
### 3. Assign PGE SP lat/lons to CLUs (ever-crop CLUs) ###
##########################################################

#Convert to SF object
prems2_sf  <- st_as_sf(prems)
st_crs(prems2_sf) <- st_crs(CLUs_ec_sf)

#Assign each lat/lon to the polygon it's contained in
prems2_sf$in_clu_row <- sapply(st_intersects(prems2_sf,CLUs_ec_sf), function(z) if (length(z)==0) NA_integer_ else z[1])

#Package results
prems2_sf$in_clu <- is.na(prems2_sf$in_clu_row)==0
prems2_sf$CLU_ID <- CLUs_ec_sf$CLU_ID[prems2_sf$in_clu_row]
prems2_sf$CLUAcres <- CLUs_ec_sf$CLUAcres[prems2_sf$in_clu_row]

#Create vector of only those observations where CLU is missing
n <- nrow(prems2_sf)
missings <- c(1:n)[(is.na(prems2_sf$in_clu_row))]
matches <- c(1:n)[(is.na(prems2_sf$in_clu_row)==0)]

#Function to calculate distance to CLU polygon border, for lat/lons contained in a polygon
edgeFXN <- function(i) {
  
  # create single-row data table of prem lat/lon
  temp_prems2_sf <- prems2_sf[i,]
  
  # create single-row data table of matched CLU polygon
  temp_CLUs_ec_sf <- CLUs_ec_sf[temp_prems2_sf$in_clu_row,]
  temp_CLUs_ec_sf <- st_cast(temp_CLUs_ec_sf, 'MULTILINESTRING')
  
  # find distance to edge of assigned CLU (in meters)
  out_dist <- st_distance(temp_prems2_sf, temp_CLUs_ec_sf) 
  
  # package output
  out <- c(i,out_dist)
  names(out) <- c("prems2_row","edge_dist_m")
  return(out)
  gc()
}

#Convert into data.table, which is faster for the nearest distance function
prems2_dt <- as.data.table(prems2_sf[,names(prems2_sf) %in% c("sp_uuid","geometry","in_clu_row","in_clu")])

#Convert polygons to lines (equivalent + faster for calculating minimum distance), and data.table
CLUs_ec_dt <- st_cast(CLUs_ec_sf, 'MULTILINESTRING')
CLUs_ec_dt <- as.data.table(CLUs_ec_dt)

#Function to calculate distance to neighboring CLU polygon, for lat/lons contained in a polygon
neighborFXN <- function(i) {
  
  # create single-row data table of prem lat/lon
  temp_prems2_dt <- prems2_dt[i,]
  
  # store row ID of assigned polygon
  temp_row <- temp_prems2_dt$in_clu_row
  
  # remove nearest polygon from CLUs_dt
  CLUs_ec_dt2 <- CLUs_ec_dt[-c(temp_row), ] 
  
  # find ID of nearest neighboring polygon (actually line)
  temp_row2 <- st_nearest_feature(temp_prems2_dt$geometry, CLUs_ec_dt2$geometry) 
  
  # assign index of nearest neighboring polygon (actually line)
  out_id2 <- as.character(CLUs_ec_dt2[temp_row2,]$CLU_ID)
  
  # convert single-row prems_dt into SF object, and add CRS units
  temp_prems2_sf <- st_as_sf(temp_prems2_dt)
  temp_prems2_sf <- st_set_crs(temp_prems2_sf, crs) 
  
  # convert nearest neighboring polygon into single-row SF object, and add CRS units
  temp_CLUs_ec_sf2 <- st_as_sf(CLUs_ec_dt2[temp_row2,])
  temp_CLUs_ec_sf2 <- st_set_crs(temp_CLUs_ec_sf2, crs) 
  
  # find distance to nearest neighboring CLU (in meters)
  out_dist2 <- st_distance(temp_prems2_sf, temp_CLUs_ec_sf2, by_element=TRUE)
  
  # package output
  out <- c(i,out_id2,out_dist2)
  names(out) <- c("prems2_row","neighbor_ID","neighbor_dist_m")
  return(out)
  gc()
}

#Function to calculate distance to a CLU polygon, for lat/lons *not* contained in a polygon
nearestFXN <- function(i) {
  
  # create single-row data table of prem lat/lon
  temp_prems2_dt <- prems2_dt[i,]
  
  # find ID of nearest polygon (actually line)
  temp_row <- st_nearest_feature(temp_prems2_dt$geometry, CLUs_ec_dt$geometry) 
  
  # assign index of nearest polygon (actually line)
  out_id <- as.character(CLUs_ec_dt[temp_row,]$CLU_ID)
  
  # remove nearest polygon from CLUs_dt
  CLUs_ec_dt2 <- CLUs_ec_dt[-c(temp_row), ] 
  
  # find ID of 2nd-nearest polygon (actually line)
  temp_row2 <- st_nearest_feature(temp_prems2_dt$geometry, CLUs_ec_dt2$geometry) 
  
  # assign index of nearest polygon (actually line)
  out_id2 <- as.character(CLUs_ec_dt2[temp_row2,]$CLU_ID)
  
  # convert single-row prems_dt into SF object, and add CRS units
  temp_prems2_sf <- st_as_sf(temp_prems2_dt)
  temp_prems2_sf <- st_set_crs(temp_prems2_sf, crs) 
  
  # convert nearest polygon into single-row SF object, and add CRS units
  temp_CLUs_ec_sf <- st_as_sf(CLUs_ec_dt[temp_row,])
  temp_CLUs_ec_sf <- st_set_crs(temp_CLUs_ec_sf, crs) 
  
  # convert 2nd-nearest polygon into single-row SF object, and add CRS units
  temp_CLUs_ec_sf2 <- st_as_sf(CLUs_ec_dt2[temp_row2,])
  temp_CLUs_ec_sf2 <- st_set_crs(temp_CLUs_ec_sf2, crs) 
  
  # find distance to nearest CLU (in meters)
  out_dist <- st_distance(temp_prems2_sf, temp_CLUs_ec_sf, by_element=TRUE)
  
  # find distance to 2nd-nearest CLU (in meters)
  out_dist2 <- st_distance(temp_prems2_sf, temp_CLUs_ec_sf2, by_element=TRUE)
  
  # package output
  out <- c(i,out_id,out_dist,out_id2,out_dist2)
  names(out) <- c("prems2_row","nearest_ID","nearest_dist_m","nearest2_ID","nearest2_dist_m")
  return(out)
  gc()
}

#Execute 3 CPU-intensive GIS functions, each in parallel
cl <- makeCluster(24) #(cores - 1)
clusterEvalQ(cl, library(sf))
clusterEvalQ(cl, library(data.table))
clusterSetRNGStream(cl, 12345)
clusterExport(cl=cl, varlist=c('edgeFXN','prems2_sf','CLUs_ec_sf','neighborFXN','nearestFXN','prems2_dt','crs','CLUs_ec_dt'))
edgeFXN_ec_out <- as.data.frame(t(parSapply(cl=cl, matches, function(x) edgeFXN(x))))
neighborFXN_ec_out <- as.data.frame(t(parSapply(cl=cl, matches, function(x) neighborFXN(x))))
nearestFXN_ec_out <- as.data.frame(t(parSapply(cl=cl, missings, function(x) nearestFXN(x))))
stopCluster(cl)

#Fix data types
neighborFXN_ec_out$prems2_row <- as.integer(as.character(neighborFXN_ec_out$prems2_row))
neighborFXN_ec_out$neighbor_dist_m <- as.numeric(as.character(neighborFXN_ec_out$neighbor_dist_m))
nearestFXN_ec_out$prems2_row <- as.integer(as.character(nearestFXN_ec_out$prems2_row))
nearestFXN_ec_out$nearest_dist_m <- as.numeric(as.character(nearestFXN_ec_out$nearest_dist_m))
nearestFXN_ec_out$nearest2_dist_m <- as.numeric(as.character(nearestFXN_ec_out$nearest2_dist_m))

#Merge in areas
neighborFXN_ec_out <- left_join(neighborFXN_ec_out, CLUs_ec_sf, by=c("neighbor_ID"="CLU_ID"))
neighborFXN_ec_out <- neighborFXN_ec_out[,names(neighborFXN_ec_out) %in% c("prems2_row","neighbor_ID","neighbor_dist_m","CLUAcres")]
names(neighborFXN_ec_out)[2] <- "neighbor_CLU_ID"
names(neighborFXN_ec_out)[4] <- "neighbor_CLUAcres"
nearestFXN_ec_out <- left_join(nearestFXN_ec_out, CLUs_ec_sf, by=c("nearest_ID"="CLU_ID"))
nearestFXN_ec_out <- nearestFXN_ec_out[,names(nearestFXN_ec_out) %in% c("prems2_row","nearest_ID","nearest_dist_m","CLUAcres","nearest2_ID","nearest2_dist_m")]
names(nearestFXN_ec_out)[2] <- "nearest_CLU_ID"
names(nearestFXN_ec_out)[6] <- "nearest_CLUAcres"
nearestFXN_ec_out <- left_join(nearestFXN_ec_out, CLUs_ec_sf, by=c("nearest2_ID"="CLU_ID"))
nearestFXN_ec_out <- nearestFXN_ec_out[,names(nearestFXN_ec_out) %in% c("prems2_row","nearest_CLU_ID","nearest_dist_m","nearest_CLUAcres","nearest2_ID","nearest2_dist_m","CLUAcres")]
names(nearestFXN_ec_out)[4] <- "nearest2_CLU_ID"
names(nearestFXN_ec_out)[7] <- "nearest2_CLUAcres"

#Expand nearest outputs into full size
n_vect <- as.data.frame(c(1:n))
names(n_vect) <- "prems2_row"
edgeFXN_ec_out_expanded <- left_join(n_vect, edgeFXN_ec_out, by="prems2_row")
neighborFXN_ec_out_expanded <- left_join(n_vect, neighborFXN_ec_out, by="prems2_row")
nearestFXN_ec_out_expanded <- left_join(n_vect, nearestFXN_ec_out, by="prems2_row")

#Tranfer nearest outcomes to main dataset
prems_ec_out <- st_drop_geometry(prems2_sf)
prems_ec_out <- cbind(prems_ec_out,edgeFXN_ec_out_expanded,neighborFXN_ec_out_expanded,nearestFXN_ec_out_expanded)

#Diagnostics
summary(as.numeric(prems_ec_out$in_clu))
summary(as.numeric(prems_ec_out$in_clu[prems_ec_out$bad_geocode_flag==0]))
summary(as.numeric(prems_ec_out$in_clu[prems_ec_out$bad_geocode_flag==0 & prems_ec_out$pull=="20180719"]))
summary(prems_ec_out[prems_ec_out$in_clu==0,]$nearest_dist_m)
summary(prems_ec_out[prems_ec_out$in_clu==0 & prems_ec_out$bad_geocode_flag==0 & prems_ec_out$pull=="20180719",]$nearest_dist_m)

#Drop extraneous variables
prems_ec_out <- prems_ec_out[,names(prems_ec_out) %in% c("sp_uuid","prem_lat","prem_long","bad_geocode_flag","pull",
                                                         "in_clu","CLU_ID","CLUAcres","edge_dist_m",
                                                         "neighbor_CLU_ID","neighbor_dist_m","neighbor_CLUAcres",
                                                         "nearest_CLU_ID","nearest_dist_m","nearest_CLUAcres",
                                                         "nearest2_CLU_ID","nearest2_dist_m","nearest2_CLUAcres")]

#Export results to csv
filename <- paste0(path,"/misc/pge_prem_coord_polygon_clu_ever_crop.csv")
write.csv(prems_ec_out, file=filename , row.names=FALSE, quote=FALSE)


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
pumps_sf$CLUAcres <- CLUs_sf$CLUAcres[pumps_sf$in_clu_row]

#Create vector of only those observations where CLU is missing
n <- nrow(pumps_sf)
missings <- c(1:n)[(is.na(pumps_sf$in_clu_row))]
matches <- c(1:n)[(is.na(pumps_sf$in_clu_row)==0)]

#Function to calculate distance to CLU polygon border, for lat/lons contained in a polygon
edgeFXN <- function(i) {
  
  # create single-row data table of pump lat/lon
  temp_pumps_sf <- pumps_sf[i,]
  
  # create single-row data table of matched CLU polygon
  temp_CLUs_sf <- CLUs_sf[temp_pumps_sf$in_clu_row,]
  temp_CLUs_sf <- st_cast(temp_CLUs_sf, 'MULTILINESTRING')
  
  # find distance to edge of assigned CLU (in meters)
  out_dist <- st_distance(temp_pumps_sf, temp_CLUs_sf) 
  
  # package output
  out <- c(i,out_dist)
  names(out) <- c("pumps_row","edge_dist_m")
  return(out)
  gc()
}

#Convert into data.table, which is faster for the nearest distance function
pumps_dt <- as.data.table(pumps_sf[,names(pumps_sf) %in% c("latlon_group","geometry","in_clu_row","in_clu")])

#Convert polygons to lines (equivalent + faster for calculating minimum distance), and data.table
CLUs_dt <- st_cast(CLUs_sf, 'MULTILINESTRING')
CLUs_dt <- as.data.table(CLUs_dt)

#Function to calculate distance to neighboring CLU polygon, for lat/lons contained in a polygon
neighborFXN <- function(i) {
  
  # create single-row data table of pump lat/lon
  temp_pumps_dt <- pumps_dt[i,]
  
  # store row ID of assigned polygon
  temp_row <- temp_pumps_dt$in_clu_row
  
  # remove nearest polygon from CLUs_dt
  CLUs_dt2 <- CLUs_dt[-c(temp_row), ] 
  
  # find ID of nearest neighboring polygon (actually line)
  temp_row2 <- st_nearest_feature(temp_pumps_dt$geometry, CLUs_dt2$geometry) 
  
  # assign index of nearest neighboring polygon (actually line)
  out_id2 <- as.character(CLUs_dt2[temp_row2,]$CLU_ID)
  
  # convert single-row pumps_dt into SF object, and add CRS units
  temp_pumps_sf <- st_as_sf(temp_pumps_dt)
  temp_pumps_sf <- st_set_crs(temp_pumps_sf, crs) 
  
  # convert nearest neighboring polygon into single-row SF object, and add CRS units
  temp_CLUs_sf2 <- st_as_sf(CLUs_dt2[temp_row2,])
  temp_CLUs_sf2 <- st_set_crs(temp_CLUs_sf2, crs) 
  
  # find distance to nearest neighboring CLU (in meters)
  out_dist2 <- st_distance(temp_pumps_sf, temp_CLUs_sf2, by_element=TRUE)
  
  # package output
  out <- c(i,out_id2,out_dist2)
  names(out) <- c("pumps_row","neighbor_ID","neighbor_dist_m")
  return(out)
  gc()
}

#Function to calculate distance to a CLU polygon, for lat/lons *not* contained in a polygon
nearestFXN <- function(i) {
  
  # create single-row data table of pump lat/lon
  temp_pumps_dt <- pumps_dt[i,]
  
  # find ID of nearest polygon (actually line)
  temp_row <- st_nearest_feature(temp_pumps_dt$geometry, CLUs_dt$geometry) 
  
  # assign index of nearest polygon (actually line)
  out_id <- as.character(CLUs_dt[temp_row,]$CLU_ID)
  
  # remove nearest polygon from CLUs_dt
  CLUs_dt2 <- CLUs_dt[-c(temp_row), ] 
  
  # find ID of 2nd-nearest polygon (actually line)
  temp_row2 <- st_nearest_feature(temp_pumps_dt$geometry, CLUs_dt2$geometry) 
  
  # assign index of nearest polygon (actually line)
  out_id2 <- as.character(CLUs_dt2[temp_row2,]$CLU_ID)
  
  # convert single-row pumps_dt into SF object, and add CRS units
  temp_pumps_sf <- st_as_sf(temp_pumps_dt)
  temp_pumps_sf <- st_set_crs(temp_pumps_sf, crs) 
  
  # convert nearest polygon into single-row SF object, and add CRS units
  temp_CLUs_sf <- st_as_sf(CLUs_dt[temp_row,])
  temp_CLUs_sf <- st_set_crs(temp_CLUs_sf, crs) 
  
  # convert 2nd-nearest polygon into single-row SF object, and add CRS units
  temp_CLUs_sf2 <- st_as_sf(CLUs_dt2[temp_row2,])
  temp_CLUs_sf2 <- st_set_crs(temp_CLUs_sf2, crs) 
  
  # find distance to nearest CLU (in meters)
  out_dist <- st_distance(temp_pumps_sf, temp_CLUs_sf, by_element=TRUE)
  
  # find distance to 2nd-nearest CLU (in meters)
  out_dist2 <- st_distance(temp_pumps_sf, temp_CLUs_sf2, by_element=TRUE)
  
  # package output
  out <- c(i,out_id,out_dist,out_id2,out_dist2)
  names(out) <- c("pumps_row","nearest_ID","nearest_dist_m","nearest2_ID","nearest2_dist_m")
  return(out)
  gc()
}

#Execute 3 CPU-intensive GIS functions, each in parallel
cl <- makeCluster(18) #(cores - 1)
clusterEvalQ(cl, library(sf))
clusterEvalQ(cl, library(data.table))
clusterSetRNGStream(cl, 12345)
clusterExport(cl=cl, varlist=c('edgeFXN','pumps_sf','CLUs_sf','neighborFXN','nearestFXN','pumps_dt','crs','CLUs_dt'))
edgeFXN_out <- as.data.frame(t(parSapply(cl=cl, matches, function(x) edgeFXN(x))))
neighborFXN_out <- as.data.frame(t(parSapply(cl=cl, matches, function(x) neighborFXN(x))))
nearestFXN_out <- as.data.frame(t(parSapply(cl=cl, missings, function(x) nearestFXN(x))))
stopCluster(cl)

#Fix data types
neighborFXN_out$pumps_row <- as.integer(as.character(neighborFXN_out$pumps_row))
neighborFXN_out$neighbor_dist_m <- as.numeric(as.character(neighborFXN_out$neighbor_dist_m))
nearestFXN_out$pumps_row <- as.integer(as.character(nearestFXN_out$pumps_row))
nearestFXN_out$nearest_dist_m <- as.numeric(as.character(nearestFXN_out$nearest_dist_m))
nearestFXN_out$nearest2_dist_m <- as.numeric(as.character(nearestFXN_out$nearest2_dist_m))

#Merge in areas
neighborFXN_out <- left_join(neighborFXN_out, CLUs_sf, by=c("neighbor_ID"="CLU_ID"))
neighborFXN_out <- neighborFXN_out[,names(neighborFXN_out) %in% c("pumps_row","neighbor_ID","neighbor_dist_m","CLUAcres")]
names(neighborFXN_out)[2] <- "neighbor_CLU_ID"
names(neighborFXN_out)[4] <- "neighbor_CLUAcres"
nearestFXN_out <- left_join(nearestFXN_out, CLUs_sf, by=c("nearest_ID"="CLU_ID"))
nearestFXN_out <- nearestFXN_out[,names(nearestFXN_out) %in% c("pumps_row","nearest_ID","nearest_dist_m","CLUAcres","nearest2_ID","nearest2_dist_m")]
names(nearestFXN_out)[2] <- "nearest_CLU_ID"
names(nearestFXN_out)[6] <- "nearest_CLUAcres"
nearestFXN_out <- left_join(nearestFXN_out, CLUs_sf, by=c("nearest2_ID"="CLU_ID"))
nearestFXN_out <- nearestFXN_out[,names(nearestFXN_out) %in% c("pumps_row","nearest_CLU_ID","nearest_dist_m","nearest_CLUAcres","nearest2_ID","nearest2_dist_m","CLUAcres")]
names(nearestFXN_out)[4] <- "nearest2_CLU_ID"
names(nearestFXN_out)[7] <- "nearest2_CLUAcres"

#Expand nearest outputs into full size
n_vect <- as.data.frame(c(1:n))
names(n_vect) <- "pumps_row"
edgeFXN_out_expanded <- left_join(n_vect, edgeFXN_out, by="pumps_row")
neighborFXN_out_expanded <- left_join(n_vect, neighborFXN_out, by="pumps_row")
nearestFXN_out_expanded <- left_join(n_vect, nearestFXN_out, by="pumps_row")

#Tranfer nearest outcomes to main dataset
pumps_out <- st_drop_geometry(pumps_sf)
pumps_out <- cbind(pumps_out,edgeFXN_out_expanded,neighborFXN_out_expanded,nearestFXN_out_expanded)

#Diagnostics
summary(as.numeric(pumps_out$in_clu))
summary(pumps_out[pumps_out$in_clu==0,]$nearest_dist_m)

#Drop extraneous variables
pumps_out <- pumps_out[,names(pumps_out) %in% c("latlon_group","pump_lat","pump_long",
                                                "in_clu","CLU_ID","CLUAcres","edge_dist_m",
                                                "neighbor_CLU_ID","neighbor_dist_m","neighbor_CLUAcres",
                                                "nearest_CLU_ID","nearest_dist_m","nearest_CLUAcres",
                                                "nearest2_CLU_ID","nearest2_dist_m","nearest2_CLUAcres")]

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
pumps2_sf$CLUAcres <- CLUs_ec_sf$CLUAcres[pumps2_sf$in_clu_row]

#Create vector of only those observations where CLU is missing
n <- nrow(pumps2_sf)
missings <- c(1:n)[(is.na(pumps2_sf$in_clu_row))]
matches <- c(1:n)[(is.na(pumps2_sf$in_clu_row)==0)]

#Function to calculate distance to CLU polygon border, for lat/lons contained in a polygon
edgeFXN <- function(i) {
  
  # create single-row data table of pump lat/lon
  temp_pumps2_sf <- pumps2_sf[i,]
  
  # create single-row data table of matched CLU polygon
  temp_CLUs_ec_sf <- CLUs_ec_sf[temp_pumps2_sf$in_clu_row,]
  temp_CLUs_ec_sf <- st_cast(temp_CLUs_ec_sf, 'MULTILINESTRING')
  
  # find distance to edge of assigned CLU (in meters)
  out_dist <- st_distance(temp_pumps2_sf, temp_CLUs_ec_sf) 
  
  # package output
  out <- c(i,out_dist)
  names(out) <- c("pumps2_row","edge_dist_m")
  return(out)
  gc()
}

#Convert into data.table, which is faster for the nearest distance function
pumps2_dt <- as.data.table(pumps2_sf[,names(pumps2_sf) %in% c("latlon_group","geometry","in_clu_row","in_clu")])

#Convert polygons to lines (equivalent + faster for calculating minimum distance), and data.table
CLUs_ec_dt <- st_cast(CLUs_ec_sf, 'MULTILINESTRING')
CLUs_ec_dt <- as.data.table(CLUs_ec_dt)

#Function to calculate distance to neighboring CLU polygon, for lat/lons contained in a polygon
neighborFXN <- function(i) {
  
  # create single-row data table of pump lat/lon
  temp_pumps2_dt <- pumps2_dt[i,]
  
  # store row ID of assigned polygon
  temp_row <- temp_pumps2_dt$in_clu_row
  
  # remove nearest polygon from CLUs_dt
  CLUs_ec_dt2 <- CLUs_ec_dt[-c(temp_row), ] 
  
  # find ID of nearest neighboring polygon (actually line)
  temp_row2 <- st_nearest_feature(temp_pumps2_dt$geometry, CLUs_ec_dt2$geometry) 
  
  # assign index of nearest neighboring polygon (actually line)
  out_id2 <- as.character(CLUs_ec_dt2[temp_row2,]$CLU_ID)
  
  # convert single-row pumps_dt into SF object, and add CRS units
  temp_pumps2_sf <- st_as_sf(temp_pumps2_dt)
  temp_pumps2_sf <- st_set_crs(temp_pumps2_sf, crs) 
  
  # convert nearest neighboring polygon into single-row SF object, and add CRS units
  temp_CLUs_ec_sf2 <- st_as_sf(CLUs_ec_dt2[temp_row2,])
  temp_CLUs_ec_sf2 <- st_set_crs(temp_CLUs_ec_sf2, crs) 
  
  # find distance to nearest neighboring CLU (in meters)
  out_dist2 <- st_distance(temp_pumps2_sf, temp_CLUs_ec_sf2, by_element=TRUE)
  
  # package output
  out <- c(i,out_id2,out_dist2)
  names(out) <- c("pumps2_row","neighbor_ID","neighbor_dist_m")
  return(out)
  gc()
}

#Function to calculate distance to a CLU polygon, for lat/lons *not* contained in a polygon
nearestFXN <- function(i) {
  
  # create single-row data table of pump lat/lon
  temp_pumps2_dt <- pumps2_dt[i,]
  
  # find ID of nearest polygon (actually line)
  temp_row <- st_nearest_feature(temp_pumps2_dt$geometry, CLUs_ec_dt$geometry) 
  
  # assign index of nearest polygon (actually line)
  out_id <- as.character(CLUs_ec_dt[temp_row,]$CLU_ID)
  
  # remove nearest polygon from CLUs_dt
  CLUs_ec_dt2 <- CLUs_ec_dt[-c(temp_row), ] 
  
  # find ID of 2nd-nearest polygon (actually line)
  temp_row2 <- st_nearest_feature(temp_pumps2_dt$geometry, CLUs_ec_dt2$geometry) 
  
  # assign index of nearest polygon (actually line)
  out_id2 <- as.character(CLUs_ec_dt2[temp_row2,]$CLU_ID)
  
  # convert single-row pumps_dt into SF object, and add CRS units
  temp_pumps2_sf <- st_as_sf(temp_pumps2_dt)
  temp_pumps2_sf <- st_set_crs(temp_pumps2_sf, crs) 
  
  # convert nearest polygon into single-row SF object, and add CRS units
  temp_CLUs_ec_sf <- st_as_sf(CLUs_ec_dt[temp_row,])
  temp_CLUs_ec_sf <- st_set_crs(temp_CLUs_ec_sf, crs) 
  
  # convert 2nd-nearest polygon into single-row SF object, and add CRS units
  temp_CLUs_ec_sf2 <- st_as_sf(CLUs_ec_dt2[temp_row2,])
  temp_CLUs_ec_sf2 <- st_set_crs(temp_CLUs_ec_sf2, crs) 
  
  # find distance to nearest CLU (in meters)
  out_dist <- st_distance(temp_pumps2_sf, temp_CLUs_ec_sf, by_element=TRUE)
  
  # find distance to 2nd-nearest CLU (in meters)
  out_dist2 <- st_distance(temp_pumps2_sf, temp_CLUs_ec_sf2, by_element=TRUE)
  
  # package output
  out <- c(i,out_id,out_dist,out_id2,out_dist2)
  names(out) <- c("pumps2_row","nearest_ID","nearest_dist_m","nearest2_ID","nearest2_dist_m")
  return(out)
  gc()
}

#Execute 3 CPU-intensive GIS functions, each in parallel
cl <- makeCluster(18) #(cores - 1)
clusterEvalQ(cl, library(sf))
clusterEvalQ(cl, library(data.table))
clusterSetRNGStream(cl, 12345)
clusterExport(cl=cl, varlist=c('edgeFXN','pumps2_sf','CLUs_ec_sf','neighborFXN','nearestFXN','pumps2_dt','crs','CLUs_ec_dt'))
edgeFXN_ec_out <- as.data.frame(t(parSapply(cl=cl, matches, function(x) edgeFXN(x))))
neighborFXN_ec_out <- as.data.frame(t(parSapply(cl=cl, matches, function(x) neighborFXN(x))))
nearestFXN_ec_out <- as.data.frame(t(parSapply(cl=cl, missings, function(x) nearestFXN(x))))
stopCluster(cl)

#Fix data types
neighborFXN_ec_out$pumps2_row <- as.integer(as.character(neighborFXN_ec_out$pumps2_row))
neighborFXN_ec_out$neighbor_dist_m <- as.numeric(as.character(neighborFXN_ec_out$neighbor_dist_m))
nearestFXN_ec_out$pumps2_row <- as.integer(as.character(nearestFXN_ec_out$pumps2_row))
nearestFXN_ec_out$nearest_dist_m <- as.numeric(as.character(nearestFXN_ec_out$nearest_dist_m))
nearestFXN_ec_out$nearest2_dist_m <- as.numeric(as.character(nearestFXN_ec_out$nearest2_dist_m))

#Merge in areas
neighborFXN_ec_out <- left_join(neighborFXN_ec_out, CLUs_ec_sf, by=c("neighbor_ID"="CLU_ID"))
neighborFXN_ec_out <- neighborFXN_ec_out[,names(neighborFXN_ec_out) %in% c("pumps2_row","neighbor_ID","neighbor_dist_m","CLUAcres")]
names(neighborFXN_ec_out)[2] <- "neighbor_CLU_ID"
names(neighborFXN_ec_out)[4] <- "neighbor_CLUAcres"
nearestFXN_ec_out <- left_join(nearestFXN_ec_out, CLUs_ec_sf, by=c("nearest_ID"="CLU_ID"))
nearestFXN_ec_out <- nearestFXN_ec_out[,names(nearestFXN_ec_out) %in% c("pumps2_row","nearest_ID","nearest_dist_m","CLUAcres","nearest2_ID","nearest2_dist_m")]
names(nearestFXN_ec_out)[2] <- "nearest_CLU_ID"
names(nearestFXN_ec_out)[6] <- "nearest_CLUAcres"
nearestFXN_ec_out <- left_join(nearestFXN_ec_out, CLUs_ec_sf, by=c("nearest2_ID"="CLU_ID"))
nearestFXN_ec_out <- nearestFXN_ec_out[,names(nearestFXN_ec_out) %in% c("pumps2_row","nearest_CLU_ID","nearest_dist_m","nearest_CLUAcres","nearest2_ID","nearest2_dist_m","CLUAcres")]
names(nearestFXN_ec_out)[4] <- "nearest2_CLU_ID"
names(nearestFXN_ec_out)[7] <- "nearest2_CLUAcres"

#Expand nearest outputs into full size
n_vect <- as.data.frame(c(1:n))
names(n_vect) <- "pumps2_row"
edgeFXN_ec_out_expanded <- left_join(n_vect, edgeFXN_ec_out, by="pumps2_row")
neighborFXN_ec_out_expanded <- left_join(n_vect, neighborFXN_ec_out, by="pumps2_row")
nearestFXN_ec_out_expanded <- left_join(n_vect, nearestFXN_ec_out, by="pumps2_row")

#Tranfer nearest outcomes to main dataset
pumps_ec_out <- st_drop_geometry(pumps2_sf)
pumps_ec_out <- cbind(pumps_ec_out,edgeFXN_ec_out_expanded,neighborFXN_ec_out_expanded,nearestFXN_ec_out_expanded)

#Diagnostics
summary(as.numeric(pumps_ec_out$in_clu))
summary(pumps_ec_out[pumps_ec_out$in_clu==0,]$nearest_dist_km)

#Drop extraneous variables
pumps_ec_out <- pumps_ec_out[,names(pumps_ec_out) %in% c("latlon_group","pump_lat","pump_long",
                                                         "in_clu","CLU_ID","CLUAcres","edge_dist_m",
                                                         "neighbor_CLU_ID","neighbor_dist_m","neighbor_CLUAcres",
                                                         "nearest_CLU_ID","nearest_dist_m","nearest_CLUAcres",
                                                         "nearest2_CLU_ID","nearest2_dist_m","nearest2_CLUAcres")]

#Export results to csv
filename <- paste0(path,"/misc/apep_pump_coord_polygon_clu_ever_crop.csv")
write.csv(pumps_ec_out, file=filename , row.names=FALSE, quote=FALSE)



####################################################
### 6. Assign SCE SP lat/lons to CLUs (all CLUs) ###
####################################################

#Read SCE coordinates
setwd(paste0(path,"/misc"))
socal <- read.delim2("sce_prem_coord_1pull.txt",header=TRUE,sep=",",stringsAsFactors=FALSE)
socal$longitude <- as.numeric(socal$prem_lon)
socal$latitude <- as.numeric(socal$prem_lat)

#Convert to SF object
coordinates(socal) <- ~ longitude + latitude
socal_sf  <- st_as_sf(socal)
st_crs(socal_sf) <- st_crs(CLUs_sf)

#Assign each lat/lon to the polygon it's contained in
socal_sf$in_clu_row <- sapply(st_intersects(socal_sf,CLUs_sf), function(z) if (length(z)==0) NA_integer_ else z[1])

#Package results
socal_sf$in_clu <- is.na(socal_sf$in_clu_row)==0
socal_sf$CLU_ID <- CLUs_sf$CLU_ID[socal_sf$in_clu_row]
socal_sf$CLUAcres <- CLUs_sf$CLUAcres[socal_sf$in_clu_row]

#Create vector of only those observations where CLU is missing
n <- nrow(socal_sf)
missings <- c(1:n)[(is.na(socal_sf$in_clu_row))]
matches <- c(1:n)[(is.na(socal_sf$in_clu_row)==0)]

#Function to calculate distance to CLU polygon border, for lat/lons contained in a polygon
edgeFXN <- function(i) {
  
  # create single-row data table of prem lat/lon
  temp_socal_sf <- socal_sf[i,]
  
  # create single-row data table of matched CLU polygon
  temp_CLUs_sf <- CLUs_sf[temp_socal_sf$in_clu_row,]
  temp_CLUs_sf <- st_cast(temp_CLUs_sf, 'MULTILINESTRING')
  
  # find distance to edge of assigned CLU (in meters)
  out_dist <- st_distance(temp_socal_sf, temp_CLUs_sf) 
  
  # package output
  out <- c(i,out_dist)
  names(out) <- c("socal_row","edge_dist_m")
  return(out)
  gc()
}

#Convert into data.table, which is faster for the nearest feature function
socal_dt <- as.data.table(socal_sf[,names(socal_sf) %in% c("sp_uuid","geometry","in_clu_row","in_clu")])

#Convert polygons to lines (equivalent + faster for calculating minimum distance), and data.table
CLUs_dt <- st_cast(CLUs_sf, 'MULTILINESTRING')
CLUs_dt <- as.data.table(CLUs_dt)

#Function to calculate distance to neighboring CLU polygon, for lat/lons contained in a polygon
neighborFXN <- function(i) {
  
  # create single-row data table of prem lat/lon
  temp_socal_dt <- socal_dt[i,]
  
  # store row ID of assigned polygon
  temp_row <- temp_socal_dt$in_clu_row
  
  # remove nearest polygon from CLUs_dt
  CLUs_dt2 <- CLUs_dt[-c(temp_row), ] 
  
  # find ID of nearest neighboring polygon (actually line)
  temp_row2 <- st_nearest_feature(temp_socal_dt$geometry, CLUs_dt2$geometry) 
  
  # assign index of nearest neighboring polygon (actually line)
  out_id2 <- as.character(CLUs_dt2[temp_row2,]$CLU_ID)
  
  # convert single-row socal_dt into SF object, and add CRS units
  temp_socal_sf <- st_as_sf(temp_socal_dt)
  temp_socal_sf <- st_set_crs(temp_socal_sf, crs) 
  
  # convert nearest neighboring polygon into single-row SF object, and add CRS units
  temp_CLUs_sf2 <- st_as_sf(CLUs_dt2[temp_row2,])
  temp_CLUs_sf2 <- st_set_crs(temp_CLUs_sf2, crs) 
  
  # find distance to nearest neighboring CLU (in meters)
  out_dist2 <- st_distance(temp_socal_sf, temp_CLUs_sf2, by_element=TRUE)
  
  # package output
  out <- c(i,out_id2,out_dist2)
  names(out) <- c("socal_row","neighbor_ID","neighbor_dist_m")
  return(out)
  gc()
}

#Function to calculate distance to a CLU polygon, for lat/lons *not* contained in a polygon
nearestFXN <- function(i) {
  
  # create single-row data table of prem lat/lon
  temp_socal_dt <- socal_dt[i,]
  
  # find ID of nearest polygon (actually line)
  temp_row <- st_nearest_feature(temp_socal_dt$geometry, CLUs_dt$geometry) 
  
  # assign index of nearest polygon (actually line)
  out_id <- as.character(CLUs_dt[temp_row,]$CLU_ID)
  
  # remove nearest polygon from CLUs_dt
  CLUs_dt2 <- CLUs_dt[-c(temp_row), ] 
  
  # find ID of 2nd-nearest polygon (actually line)
  temp_row2 <- st_nearest_feature(temp_socal_dt$geometry, CLUs_dt2$geometry) 
  
  # assign index of nearest polygon (actually line)
  out_id2 <- as.character(CLUs_dt2[temp_row2,]$CLU_ID)
  
  # convert single-row socal_dt into SF object, and add CRS units
  temp_socal_sf <- st_as_sf(temp_socal_dt)
  temp_socal_sf <- st_set_crs(temp_socal_sf, crs) 
  
  # convert nearest polygon into single-row SF object, and add CRS units
  temp_CLUs_sf <- st_as_sf(CLUs_dt[temp_row,])
  temp_CLUs_sf <- st_set_crs(temp_CLUs_sf, crs) 
  
  # convert 2nd-nearest polygon into single-row SF object, and add CRS units
  temp_CLUs_sf2 <- st_as_sf(CLUs_dt2[temp_row2,])
  temp_CLUs_sf2 <- st_set_crs(temp_CLUs_sf2, crs) 
  
  # find distance to nearest CLU (in meters)
  out_dist <- st_distance(temp_socal_sf, temp_CLUs_sf, by_element=TRUE)
  
  # find distance to 2nd-nearest CLU (in meters)
  out_dist2 <- st_distance(temp_socal_sf, temp_CLUs_sf2, by_element=TRUE)
  
  # package output
  out <- c(i,out_id,out_dist,out_id2,out_dist2)
  names(out) <- c("socal_row","nearest_ID","nearest_dist_m","nearest2_ID","nearest2_dist_m")
  return(out)
  gc()
}

#Execute 3 CPU-intensive GIS functions, each in parallel
cl <- makeCluster(24) #(cores - 1)
clusterEvalQ(cl, library(sf))
clusterEvalQ(cl, library(data.table))
clusterSetRNGStream(cl, 12345)
clusterExport(cl=cl, varlist=c('edgeFXN','socal_sf','CLUs_sf','neighborFXN','nearestFXN','socal_dt','crs','CLUs_dt'))
edgeFXN_out <- as.data.frame(t(parSapply(cl=cl, matches, function(x) edgeFXN(x))))
neighborFXN_out <- as.data.frame(t(parSapply(cl=cl, matches, function(x) neighborFXN(x))))
nearestFXN_out <- as.data.frame(t(parSapply(cl=cl, missings, function(x) nearestFXN(x))))
stopCluster(cl)

#Fix data types
neighborFXN_out$socal_row <- as.integer(as.character(neighborFXN_out$socal_row))
neighborFXN_out$neighbor_dist_m <- as.numeric(as.character(neighborFXN_out$neighbor_dist_m))
nearestFXN_out$socal_row <- as.integer(as.character(nearestFXN_out$socal_row))
nearestFXN_out$nearest_dist_m <- as.numeric(as.character(nearestFXN_out$nearest_dist_m))
nearestFXN_out$nearest2_dist_m <- as.numeric(as.character(nearestFXN_out$nearest2_dist_m))

#Merge in areas
neighborFXN_out <- left_join(neighborFXN_out, CLUs_sf, by=c("neighbor_ID"="CLU_ID"))
neighborFXN_out <- neighborFXN_out[,names(neighborFXN_out) %in% c("socal_row","neighbor_ID","neighbor_dist_m","CLUAcres")]
names(neighborFXN_out)[2] <- "neighbor_CLU_ID"
names(neighborFXN_out)[4] <- "neighbor_CLUAcres"
nearestFXN_out <- left_join(nearestFXN_out, CLUs_sf, by=c("nearest_ID"="CLU_ID"))
nearestFXN_out <- nearestFXN_out[,names(nearestFXN_out) %in% c("socal_row","nearest_ID","nearest_dist_m","CLUAcres","nearest2_ID","nearest2_dist_m")]
names(nearestFXN_out)[2] <- "nearest_CLU_ID"
names(nearestFXN_out)[6] <- "nearest_CLUAcres"
nearestFXN_out <- left_join(nearestFXN_out, CLUs_sf, by=c("nearest2_ID"="CLU_ID"))
nearestFXN_out <- nearestFXN_out[,names(nearestFXN_out) %in% c("socal_row","nearest_CLU_ID","nearest_dist_m","nearest_CLUAcres","nearest2_ID","nearest2_dist_m","CLUAcres")]
names(nearestFXN_out)[4] <- "nearest2_CLU_ID"
names(nearestFXN_out)[7] <- "nearest2_CLUAcres"

#Expand parellelized outputs into full size
n_vect <- as.data.frame(c(1:n))
names(n_vect) <- "socal_row"
edgeFXN_out_expanded <- left_join(n_vect, edgeFXN_out, by="socal_row")
neighborFXN_out_expanded <- left_join(n_vect, neighborFXN_out, by="socal_row")
nearestFXN_out_expanded <- left_join(n_vect, nearestFXN_out, by="socal_row")

#Tranfer nearest outcomes to main dataset
socal_out <- st_drop_geometry(socal_sf)
socal_out <- cbind(socal_out,edgeFXN_out_expanded,neighborFXN_out_expanded,nearestFXN_out_expanded)

#Diagnostics
summary(as.numeric(socal_out$in_clu))
summary(as.numeric(socal_out$in_clu[socal_out$bad_geocode_flag==0]))
summary(socal_out[socal_out$in_clu==0,]$nearest_dist_m)

#Drop extraneous variables
socal_out <- socal_out[,names(socal_out) %in% c("sp_uuid","prem_lat","prem_long","bad_geocode_flag","pull",
                                                "in_clu","CLU_ID","CLUAcres","edge_dist_m",
                                                "neighbor_CLU_ID","neighbor_dist_m","neighbor_CLUAcres",
                                                "nearest_CLU_ID","nearest_dist_m","nearest_CLUAcres",
                                                "nearest2_CLU_ID","nearest2_dist_m","nearest2_CLUAcres")]

#Export results to csv
filename <- paste0(path,"/misc/sce_prem_coord_polygon_clu.csv")
write.csv(socal_out, file=filename , row.names=FALSE, quote=FALSE)


##########################################################
### 7. Assign SCE SP lat/lons to CLUs (ever-crop CLUs) ###
##########################################################

#Convert to SF object
socal2_sf  <- st_as_sf(socal)
st_crs(socal2_sf) <- st_crs(CLUs_ec_sf)

#Assign each lat/lon to the polygon it's contained in
socal2_sf$in_clu_row <- sapply(st_intersects(socal2_sf,CLUs_ec_sf), function(z) if (length(z)==0) NA_integer_ else z[1])

#Package results
socal2_sf$in_clu <- is.na(socal2_sf$in_clu_row)==0
socal2_sf$CLU_ID <- CLUs_ec_sf$CLU_ID[socal2_sf$in_clu_row]
socal2_sf$CLUAcres <- CLUs_ec_sf$CLUAcres[socal2_sf$in_clu_row]

#Create vector of only those observations where CLU is missing
n <- nrow(socal2_sf)
missings <- c(1:n)[(is.na(socal2_sf$in_clu_row))]
matches <- c(1:n)[(is.na(socal2_sf$in_clu_row)==0)]

#Function to calculate distance to CLU polygon border, for lat/lons contained in a polygon
edgeFXN <- function(i) {
  
  # create single-row data table of prem lat/lon
  temp_socal2_sf <- socal2_sf[i,]
  
  # create single-row data table of matched CLU polygon
  temp_CLUs_ec_sf <- CLUs_ec_sf[temp_socal2_sf$in_clu_row,]
  temp_CLUs_ec_sf <- st_cast(temp_CLUs_ec_sf, 'MULTILINESTRING')
  
  # find distance to edge of assigned CLU (in meters)
  out_dist <- st_distance(temp_socal2_sf, temp_CLUs_ec_sf) 
  
  # package output
  out <- c(i,out_dist)
  names(out) <- c("socal2_row","edge_dist_m")
  return(out)
  gc()
}

#Convert into data.table, which is faster for the nearest distance function
socal2_dt <- as.data.table(socal2_sf[,names(socal2_sf) %in% c("sp_uuid","geometry","in_clu_row","in_clu")])

#Convert polygons to lines (equivalent + faster for calculating minimum distance), and data.table
CLUs_ec_dt <- st_cast(CLUs_ec_sf, 'MULTILINESTRING')
CLUs_ec_dt <- as.data.table(CLUs_ec_dt)

#Function to calculate distance to neighboring CLU polygon, for lat/lons contained in a polygon
neighborFXN <- function(i) {
  
  # create single-row data table of prem lat/lon
  temp_socal2_dt <- socal2_dt[i,]
  
  # store row ID of assigned polygon
  temp_row <- temp_socal2_dt$in_clu_row
  
  # remove nearest polygon from CLUs_dt
  CLUs_ec_dt2 <- CLUs_ec_dt[-c(temp_row), ] 
  
  # find ID of nearest neighboring polygon (actually line)
  temp_row2 <- st_nearest_feature(temp_socal2_dt$geometry, CLUs_ec_dt2$geometry) 
  
  # assign index of nearest neighboring polygon (actually line)
  out_id2 <- as.character(CLUs_ec_dt2[temp_row2,]$CLU_ID)
  
  # convert single-row socal_dt into SF object, and add CRS units
  temp_socal2_sf <- st_as_sf(temp_socal2_dt)
  temp_socal2_sf <- st_set_crs(temp_socal2_sf, crs) 
  
  # convert nearest neighboring polygon into single-row SF object, and add CRS units
  temp_CLUs_ec_sf2 <- st_as_sf(CLUs_ec_dt2[temp_row2,])
  temp_CLUs_ec_sf2 <- st_set_crs(temp_CLUs_ec_sf2, crs) 
  
  # find distance to nearest neighboring CLU (in meters)
  out_dist2 <- st_distance(temp_socal2_sf, temp_CLUs_ec_sf2, by_element=TRUE)
  
  # package output
  out <- c(i,out_id2,out_dist2)
  names(out) <- c("socal2_row","neighbor_ID","neighbor_dist_m")
  return(out)
  gc()
}

#Function to calculate distance to a CLU polygon, for lat/lons *not* contained in a polygon
nearestFXN <- function(i) {
  
  # create single-row data table of prem lat/lon
  temp_socal2_dt <- socal2_dt[i,]
  
  # find ID of nearest polygon (actually line)
  temp_row <- st_nearest_feature(temp_socal2_dt$geometry, CLUs_ec_dt$geometry) 
  
  # assign index of nearest polygon (actually line)
  out_id <- as.character(CLUs_ec_dt[temp_row,]$CLU_ID)
  
  # remove nearest polygon from CLUs_dt
  CLUs_ec_dt2 <- CLUs_ec_dt[-c(temp_row), ] 
  
  # find ID of 2nd-nearest polygon (actually line)
  temp_row2 <- st_nearest_feature(temp_socal2_dt$geometry, CLUs_ec_dt2$geometry) 
  
  # assign index of nearest polygon (actually line)
  out_id2 <- as.character(CLUs_ec_dt2[temp_row2,]$CLU_ID)
  
  # convert single-row socal_dt into SF object, and add CRS units
  temp_socal2_sf <- st_as_sf(temp_socal2_dt)
  temp_socal2_sf <- st_set_crs(temp_socal2_sf, crs) 
  
  # convert nearest polygon into single-row SF object, and add CRS units
  temp_CLUs_ec_sf <- st_as_sf(CLUs_ec_dt[temp_row,])
  temp_CLUs_ec_sf <- st_set_crs(temp_CLUs_ec_sf, crs) 
  
  # convert 2nd-nearest polygon into single-row SF object, and add CRS units
  temp_CLUs_ec_sf2 <- st_as_sf(CLUs_ec_dt2[temp_row2,])
  temp_CLUs_ec_sf2 <- st_set_crs(temp_CLUs_ec_sf2, crs) 
  
  # find distance to nearest CLU (in meters)
  out_dist <- st_distance(temp_socal2_sf, temp_CLUs_ec_sf, by_element=TRUE)
  
  # find distance to 2nd-nearest CLU (in meters)
  out_dist2 <- st_distance(temp_socal2_sf, temp_CLUs_ec_sf2, by_element=TRUE)
  
  # package output
  out <- c(i,out_id,out_dist,out_id2,out_dist2)
  names(out) <- c("socal2_row","nearest_ID","nearest_dist_m","nearest2_ID","nearest2_dist_m")
  return(out)
  gc()
}

#Execute 3 CPU-intensive GIS functions, each in parallel
cl <- makeCluster(24) #(cores - 1)
clusterEvalQ(cl, library(sf))
clusterEvalQ(cl, library(data.table))
clusterSetRNGStream(cl, 12345)
clusterExport(cl=cl, varlist=c('edgeFXN','socal2_sf','CLUs_ec_sf','neighborFXN','nearestFXN','socal2_dt','crs','CLUs_ec_dt'))
edgeFXN_ec_out <- as.data.frame(t(parSapply(cl=cl, matches, function(x) edgeFXN(x))))
neighborFXN_ec_out <- as.data.frame(t(parSapply(cl=cl, matches, function(x) neighborFXN(x))))
nearestFXN_ec_out <- as.data.frame(t(parSapply(cl=cl, missings, function(x) nearestFXN(x))))
stopCluster(cl)

#Fix data types
neighborFXN_ec_out$socal2_row <- as.integer(as.character(neighborFXN_ec_out$socal2_row))
neighborFXN_ec_out$neighbor_dist_m <- as.numeric(as.character(neighborFXN_ec_out$neighbor_dist_m))
nearestFXN_ec_out$socal2_row <- as.integer(as.character(nearestFXN_ec_out$socal2_row))
nearestFXN_ec_out$nearest_dist_m <- as.numeric(as.character(nearestFXN_ec_out$nearest_dist_m))
nearestFXN_ec_out$nearest2_dist_m <- as.numeric(as.character(nearestFXN_ec_out$nearest2_dist_m))

#Merge in areas
neighborFXN_ec_out <- left_join(neighborFXN_ec_out, CLUs_ec_sf, by=c("neighbor_ID"="CLU_ID"))
neighborFXN_ec_out <- neighborFXN_ec_out[,names(neighborFXN_ec_out) %in% c("socal2_row","neighbor_ID","neighbor_dist_m","CLUAcres")]
names(neighborFXN_ec_out)[2] <- "neighbor_CLU_ID"
names(neighborFXN_ec_out)[4] <- "neighbor_CLUAcres"
nearestFXN_ec_out <- left_join(nearestFXN_ec_out, CLUs_ec_sf, by=c("nearest_ID"="CLU_ID"))
nearestFXN_ec_out <- nearestFXN_ec_out[,names(nearestFXN_ec_out) %in% c("socal2_row","nearest_ID","nearest_dist_m","CLUAcres","nearest2_ID","nearest2_dist_m")]
names(nearestFXN_ec_out)[2] <- "nearest_CLU_ID"
names(nearestFXN_ec_out)[6] <- "nearest_CLUAcres"
nearestFXN_ec_out <- left_join(nearestFXN_ec_out, CLUs_ec_sf, by=c("nearest2_ID"="CLU_ID"))
nearestFXN_ec_out <- nearestFXN_ec_out[,names(nearestFXN_ec_out) %in% c("socal2_row","nearest_CLU_ID","nearest_dist_m","nearest_CLUAcres","nearest2_ID","nearest2_dist_m","CLUAcres")]
names(nearestFXN_ec_out)[4] <- "nearest2_CLU_ID"
names(nearestFXN_ec_out)[7] <- "nearest2_CLUAcres"

#Expand nearest outputs into full size
n_vect <- as.data.frame(c(1:n))
names(n_vect) <- "socal2_row"
edgeFXN_ec_out_expanded <- left_join(n_vect, edgeFXN_ec_out, by="socal2_row")
neighborFXN_ec_out_expanded <- left_join(n_vect, neighborFXN_ec_out, by="socal2_row")
nearestFXN_ec_out_expanded <- left_join(n_vect, nearestFXN_ec_out, by="socal2_row")

#Tranfer nearest outcomes to main dataset
socal_ec_out <- st_drop_geometry(socal2_sf)
socal_ec_out <- cbind(socal_ec_out,edgeFXN_ec_out_expanded,neighborFXN_ec_out_expanded,nearestFXN_ec_out_expanded)

#Diagnostics
summary(as.numeric(socal_ec_out$in_clu))
summary(as.numeric(socal_ec_out$in_clu[socal_ec_out$bad_geocode_flag==0]))
summary(socal_ec_out[socal_ec_out$in_clu==0,]$nearest_dist_m)

#Drop extraneous variables
socal_ec_out <- socal_ec_out[,names(socal_ec_out) %in% c("sp_uuid","prem_lat","prem_long","bad_geocode_flag","pull",
                                                         "in_clu","CLU_ID","CLUAcres","edge_dist_m",
                                                         "neighbor_CLU_ID","neighbor_dist_m","neighbor_CLUAcres",
                                                         "nearest_CLU_ID","nearest_dist_m","nearest_CLUAcres",
                                                         "nearest2_CLU_ID","nearest2_dist_m","nearest2_CLUAcres")]

#Export results to csv
filename <- paste0(path,"/misc/sce_prem_coord_polygon_clu_ever_crop.csv")
write.csv(socal_ec_out, file=filename , row.names=FALSE, quote=FALSE)

