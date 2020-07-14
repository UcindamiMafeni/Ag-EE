#############################################################################
#  Script to assign wells from the CA DWR Well Completion Reports to        #
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


######################################################
### 2. Assign WCR well lat/lons to CLUs (all CLUs) ###
######################################################

#Read well coordinates
setwd(paste0(path,"/misc"))
wells <- read.delim2("wcr_coordinates.txt",header=TRUE,sep=",",stringsAsFactors=FALSE)
head(wells)
wells$longitude <- as.numeric(wells$well_longitude)
wells$latitude <- as.numeric(wells$well_latitude)

#Convert to SF object
coordinates(wells) <- ~ longitude + latitude
wells_sf  <- st_as_sf(wells)
st_crs(wells_sf) <- st_crs(CLUs_sf)

#Assign each lat/lon to the polygon it's contained in
wells_sf$in_clu_row <- sapply(st_intersects(wells_sf,CLUs_sf), function(z) if (length(z)==0) NA_integer_ else z[1])

#Package results
wells_sf$in_clu <- is.na(wells_sf$in_clu_row)==0
wells_sf$CLU_ID <- CLUs_sf$CLU_ID[wells_sf$in_clu_row]
wells_sf$CLUAcres <- CLUs_sf$CLUAcres[wells_sf$in_clu_row]

#Create vector of only those observations where CLU is missing
n <- nrow(wells_sf)
missings <- c(1:n)[(is.na(wells_sf$in_clu_row))]
matches <- c(1:n)[(is.na(wells_sf$in_clu_row)==0)]

#Function to calculate distance to CLU polygon border, for lat/lons contained in a polygon
edgeFXN <- function(i) {
  
  # create single-row data table of prem lat/lon
  temp_wells_sf <- wells_sf[i,]
  
  # create single-row data table of matched CLU polygon
  temp_CLUs_sf <- CLUs_sf[temp_wells_sf$in_clu_row,]
  temp_CLUs_sf <- st_cast(temp_CLUs_sf, 'MULTILINESTRING')

  # find distance to edge of assigned CLU (in meters)
  out_dist <- st_distance(temp_wells_sf, temp_CLUs_sf) 
  
  # package output
  out <- c(i,out_dist)
  names(out) <- c("wells_row","edge_dist_m")
  return(out)
  gc()
}

#Convert into data.table, which is faster for the nearest feature function
wells_dt <- as.data.table(wells_sf[,names(wells_sf) %in% c("wcrnumber","geometry","in_clu_row","in_clu")])

#Convert polygons to lines (equivalent + faster for calculating minimum distance), and data.table
CLUs_dt <- st_cast(CLUs_sf, 'MULTILINESTRING')
CLUs_dt <- as.data.table(CLUs_dt)

#Function to calculate distance to neighboring CLU polygon, for lat/lons contained in a polygon
neighborFXN <- function(i) {
  
  # create single-row data table of prem lat/lon
  temp_wells_dt <- wells_dt[i,]
  
  # store row ID of assigned polygon
  temp_row <- temp_wells_dt$in_clu_row
  
  # remove nearest polygon from CLUs_dt
  CLUs_dt2 <- CLUs_dt[-c(temp_row), ] 
  
  # find ID of nearest neighboring polygon (actually line)
  temp_row2 <- st_nearest_feature(temp_wells_dt$geometry, CLUs_dt2$geometry) 
  
  # assign index of nearest neighboring polygon (actually line)
  out_id2 <- as.character(CLUs_dt2[temp_row2,]$CLU_ID)
  
  # convert single-row wells_dt into SF object, and add CRS units
  temp_wells_sf <- st_as_sf(temp_wells_dt)
  temp_wells_sf <- st_set_crs(temp_wells_sf, crs) 
  
  # convert nearest neighboring polygon into single-row SF object, and add CRS units
  temp_CLUs_sf2 <- st_as_sf(CLUs_dt2[temp_row2,])
  temp_CLUs_sf2 <- st_set_crs(temp_CLUs_sf2, crs) 
  
  # find distance to nearest neighboring CLU (in meters)
  out_dist2 <- st_distance(temp_wells_sf, temp_CLUs_sf2, by_element=TRUE)
  
  # package output
  out <- c(i,out_id2,out_dist2)
  names(out) <- c("wells_row","neighbor_ID","neighbor_dist_m")
  return(out)
  gc()
}

#Function to calculate distance to a CLU polygon, for lat/lons *not* contained in a polygon
nearestFXN <- function(i) {

  # create single-row data table of prem lat/lon
  temp_wells_dt <- wells_dt[i,]
  
  # find ID of nearest polygon (actually line)
  temp_row <- st_nearest_feature(temp_wells_dt$geometry, CLUs_dt$geometry) 
  
  # assign index of nearest polygon (actually line)
  out_id <- as.character(CLUs_dt[temp_row,]$CLU_ID)
  
  # remove nearest polygon from CLUs_dt
  CLUs_dt2 <- CLUs_dt[-c(temp_row), ] 
  
  # find ID of 2nd-nearest polygon (actually line)
  temp_row2 <- st_nearest_feature(temp_wells_dt$geometry, CLUs_dt2$geometry) 
  
  # assign index of nearest polygon (actually line)
  out_id2 <- as.character(CLUs_dt2[temp_row2,]$CLU_ID)
  
  # convert single-row wells_dt into SF object, and add CRS units
  temp_wells_sf <- st_as_sf(temp_wells_dt)
  temp_wells_sf <- st_set_crs(temp_wells_sf, crs) 
  
  # convert nearest polygon into single-row SF object, and add CRS units
  temp_CLUs_sf <- st_as_sf(CLUs_dt[temp_row,])
  temp_CLUs_sf <- st_set_crs(temp_CLUs_sf, crs) 
  
  # convert 2nd-nearest polygon into single-row SF object, and add CRS units
  temp_CLUs_sf2 <- st_as_sf(CLUs_dt2[temp_row2,])
  temp_CLUs_sf2 <- st_set_crs(temp_CLUs_sf2, crs) 

  # find distance to nearest CLU (in meters)
  out_dist <- st_distance(temp_wells_sf, temp_CLUs_sf, by_element=TRUE)

  # find distance to 2nd-nearest CLU (in meters)
  out_dist2 <- st_distance(temp_wells_sf, temp_CLUs_sf2, by_element=TRUE)

  # package output
  out <- c(i,out_id,out_dist,out_id2,out_dist2)
  names(out) <- c("wells_row","nearest_ID","nearest_dist_m","nearest2_ID","nearest2_dist_m")
  return(out)
  gc()
}

#Execute 3 CPU-intensive GIS functions, each in parallel
cl <- makeCluster(18) #(cores - 1)
clusterEvalQ(cl, library(sf))
clusterEvalQ(cl, library(data.table))
clusterSetRNGStream(cl, 12345)
clusterExport(cl=cl, varlist=c('edgeFXN','wells_sf','CLUs_sf','neighborFXN','nearestFXN','wells_dt','crs','CLUs_dt'))
edgeFXN_out <- as.data.frame(t(parSapply(cl=cl, matches, function(x) edgeFXN(x))))
neighborFXN_out <- as.data.frame(t(parSapply(cl=cl, matches, function(x) neighborFXN(x))))
nearestFXN_out <- as.data.frame(t(parSapply(cl=cl, missings, function(x) nearestFXN(x))))
stopCluster(cl)

#Fix data types
neighborFXN_out$wells_row <- as.integer(as.character(neighborFXN_out$wells_row))
neighborFXN_out$neighbor_dist_m <- as.numeric(as.character(neighborFXN_out$neighbor_dist_m))
nearestFXN_out$wells_row <- as.integer(as.character(nearestFXN_out$wells_row))
nearestFXN_out$nearest_dist_m <- as.numeric(as.character(nearestFXN_out$nearest_dist_m))
nearestFXN_out$nearest2_dist_m <- as.numeric(as.character(nearestFXN_out$nearest2_dist_m))

#Merge in areas
neighborFXN_out <- left_join(neighborFXN_out, CLUs_sf, by=c("neighbor_ID"="CLU_ID"))
neighborFXN_out <- neighborFXN_out[,names(neighborFXN_out) %in% c("wells_row","neighbor_ID","neighbor_dist_m","CLUAcres")]
names(neighborFXN_out)[2] <- "neighbor_CLU_ID"
names(neighborFXN_out)[4] <- "neighbor_CLUAcres"
nearestFXN_out <- left_join(nearestFXN_out, CLUs_sf, by=c("nearest_ID"="CLU_ID"))
nearestFXN_out <- nearestFXN_out[,names(nearestFXN_out) %in% c("wells_row","nearest_ID","nearest_dist_m","CLUAcres","nearest2_ID","nearest2_dist_m")]
names(nearestFXN_out)[2] <- "nearest_CLU_ID"
names(nearestFXN_out)[6] <- "nearest_CLUAcres"
nearestFXN_out <- left_join(nearestFXN_out, CLUs_sf, by=c("nearest2_ID"="CLU_ID"))
nearestFXN_out <- nearestFXN_out[,names(nearestFXN_out) %in% c("wells_row","nearest_CLU_ID","nearest_dist_m","nearest_CLUAcres","nearest2_ID","nearest2_dist_m","CLUAcres")]
names(nearestFXN_out)[4] <- "nearest2_CLU_ID"
names(nearestFXN_out)[7] <- "nearest2_CLUAcres"

#Expand parellelized outputs into full size
n_vect <- as.data.frame(c(1:n))
names(n_vect) <- "wells_row"
edgeFXN_out_expanded <- left_join(n_vect, edgeFXN_out, by="wells_row")
neighborFXN_out_expanded <- left_join(n_vect, neighborFXN_out, by="wells_row")
nearestFXN_out_expanded <- left_join(n_vect, nearestFXN_out, by="wells_row")

#Tranfer nearest outcomes to main dataset
wells_out <- st_drop_geometry(wells_sf)
wells_out <- cbind(wells_out,edgeFXN_out_expanded,neighborFXN_out_expanded,nearestFXN_out_expanded)

#Diagnostics
summary(as.numeric(wells_out$in_clu))
summary(wells_out[wells_out$in_clu==0,]$nearest_dist_m)

#Drop extraneous variables
wells_out <- wells_out[,names(wells_out) %in% c("wcrnumber","well_latitude","well_longitude",
                                                "in_clu","CLU_ID","CLUAcres","edge_dist_m",
                                                "neighbor_CLU_ID","neighbor_dist_m","neighbor_CLUAcres",
                                                "nearest_CLU_ID","nearest_dist_m","nearest_CLUAcres",
                                                "nearest2_CLU_ID","nearest2_dist_m","nearest2_CLUAcres")]

#Export results to csv
filename <- paste0(path,"/misc/wcr_well_coord_polygon_clu.csv")
write.csv(wells_out, file=filename , row.names=FALSE, quote=FALSE)

