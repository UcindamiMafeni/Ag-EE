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


##########################################
### 1. Prep all relevant in shapefiles ###
##########################################

#Load Common Land Units shapefile
setwd("S:/Matt/ag_pump/data/cleaned_spatial/CLU")
CLUs_sf <- readRDS("clu.RDS")
  
#Export list of CLUs 
setwd("S:/Matt/ag_pump/data/misc")
CLUs_data <- CLUs_sf
st_geometry(CLUs_data) <- NULL
filename <- "CLUs_cleaned.csv"
write.csv(CLUs_data, file=filename , row.names=FALSE, col.names=TRUE, quote=FALSE, append=FALSE)

#Convert SF to SP
CLUs <- sf:::as_Spatial(CLUs_sf$geom)
CLUs <- SpatialPolygonsDataFrame(CLUs, CLUs_data, match.ID=FALSE)

#Load CA state outline
setwd("S:/Matt/ag_pump/data/spatial")
CAoutline <- readOGR(dsn = "State", layer = "CA_State_TIGER2016")
proj4string(CAoutline)
CAoutline <- spTransform(CAoutline, CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0"))

#Confirm CLUs align with California map
#ggplot() + 
#  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
#               color="grey30", fill=NA, alpha=1) +
#  geom_polygon(data=CLUs, aes(x=long, y=lat, group=group, color=rgb(0,0,1)), 
#               color=rgb(0,1,0), fill=rgb(0,0,1), alpha=1) 


#####################################
### 2. Assign SP lat/lons to CLUs ###
#####################################

#Read PGE coordinates
setwd("S:/Matt/ag_pump/data/misc")
prems <- read.delim2("pge_prem_coord_3pulls.txt",header=TRUE,sep=",",stringsAsFactors=FALSE)
prems$longitude <- as.numeric(prems$prem_lon)
prems$latitude <- as.numeric(prems$prem_lat)

#Convert to SpatialPointsDataFrame
coordinates(prems) <- ~ longitude + latitude
proj4string(prems) <- proj4string(CLUs)

#Assign each lat/lon to the CLU polygon it's contained in
prems@data$IN_CLU <- over(prems, CLUs)

#Reproject everything into planar coordinates
utmStr <- "+proj=utm +zone=%d +datum=NAD83 +units=m +no_defs +ellps=GRS80 +towgs84=0,0,0"
crs <- CRS(sprintf(utmStr, 10))
CLUsUTM <- spTransform(CLUs, crs) #reproject into planar coordinates, because that's what the distance function uses
premsUTM <- spTransform(prems, crs)

#Create empy vectors to loop over, to calculate distance to a CLU
n <- nrow(prems@data)
nearestCLU_ID <- numeric(n)
nearestCLU_dist_km <- numeric(n)

#Create vector of only those observations where CLU is missing
missings <- c(1:n)[is.na(prems@data$IN_CLU$CLU_ID)]

#Calculate distance to a CLU polygon, for lat/lons not contained in a polygon
for (j in seq_along(missings)) {
  i <- missings[j]
  temp <- CLUsUTM@data[which.min(gDistance(premsUTM[i,], CLUsUTM, byid=TRUE)),]
  nearestCLU_ID[i]   <- as.character(temp$CLU_ID)
  nearestCLU_dist_km[i] <- min(gDistance(premsUTM[i,], CLUsUTM, byid=TRUE))/1000
}

#Convert back to regular dataframe
prems <- as.data.frame(prems)

#Assign in_CLU dummy and store CLU ID and area
prems$CLU_ID <- prems$IN_CLU.CLU_ID
prems$CLU_county <- prems$IN_CLU.County
prems$CLU_acres <- prems$IN_CLU.CLUAcres
prems$in_CLU <- as.numeric(is.na(prems$CLU_ID)==0)
summary(prems$in_CLU)
summary(prems$in_CLU[prems$bad_geocode_flag==0])
summary(prems$in_CLU[prems$bad_geocode_flag==0 & prems$pull=="20180719"])

#Append nearest CLU variables
prems <- cbind(prems, nearestCLU_ID, nearestCLU_dist_km)
summary(prems[prems$in_CLU==0,]$nearestCLU_dist_km)
summary(prems[prems$in_CLU==0 & prems$bad_geocode_flag==0 & prems$pull=="20180719",]$nearestCLU_dist_km)

#Plot points in CLUs
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  #geom_polygon(data=CLUs, aes(x=long, y=lat, group=group), 
  #             color="green", fill=NA, alpha=1) +
  geom_point(data=prems[prems$in_CLU==1,], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Plot points NOT in CLUs
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  #geom_polygon(data=CLUs, aes(x=long, y=lat, group=group), 
  #             color="green", fill=NA, alpha=1) +
  geom_point(data=prems[(prems$in_CLU==0 & prems$bad_geocode_flag==0),], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Plot points NOT in CLUs that are in APEP
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  #geom_polygon(data=CLUs, aes(x=long, y=lat, group=group), 
  #             color="green", fill=NA, alpha=1) +
  geom_point(data=prems[(prems$in_CLU==0 & prems$bad_geocode_flag==0 & prems$pull=="20180719"),], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Plot points NOT in CLUs that are in APEP, and are <2km from nearest CLUs
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  #geom_polygon(data=CLUs, aes(x=long, y=lat, group=group), 
  #             color="green", fill=NA, alpha=1) +
  geom_point(data=prems[(prems$in_CLU==0 & prems$bad_geocode_flag==0 & prems$pull=="20180719" & prems$nearestCLU_dist_km<2),], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Drop extraneous variables
prems <- prems[c("sp_uuid","prem_lat","prem_long","longitude","latitude",
                 "bad_geocode_flag","pull","CLU_ID","in_CLU","CLU_county","CLU_acres",
                 "nearestCLU_ID", "nearestCLU_dist_km")]

#Export results to csv
filename <- "pge_prem_coord_polygon_clu.csv"
write.csv(prems, file=filename , row.names=FALSE, col.names=TRUE, quote=FALSE, append=FALSE)


#######################################
### 3. Assign APEP lat/lons to CLUs ###
#######################################

#Read APEP coordinates
setwd("S:/Matt/ag_pump/data/misc")
pumps <- read.delim2("apep_pump_coord.txt",header=TRUE,sep=",",stringsAsFactors=FALSE)
pumps$longitude <- as.numeric(pumps$pump_lon)
pumps$latitude <- as.numeric(pumps$pump_lat)

#Convert to SpatialPointsDataFrame
coordinates(pumps) <- ~ longitude + latitude
proj4string(pumps) <- proj4string(CLUs)

#Assign each lat/lon to the CLU polygon it's contained in
pumps@data$IN_CLU <- over(pumps, CLUs)

#Reproject everything into planar coordinates
utmStr <- "+proj=utm +zone=%d +datum=NAD83 +units=m +no_defs +ellps=GRS80 +towgs84=0,0,0"
crs <- CRS(sprintf(utmStr, 10))
CLUsUTM <- spTransform(CLUs, crs) #reproject into planar coordinates, because that's what the distance function uses
pumpsUTM <- spTransform(pumps, crs)

#Create empy vectors to loop over, to calculate distance to a CLU
n <- nrow(pumps@data)
nearestCLU_ID <- numeric(n)
nearestCLU_dist_km <- numeric(n)

#Create vector of only those observations where CLU is missing
missings <- c(1:n)[is.na(pumps@data$IN_CLU$CLU_ID)]

#Calculate distance to a CLU polygon, for lat/lons not contained in a polygon
for (j in seq_along(missings)) {
  i <- missings[j]
  temp <- CLUsUTM@data[which.min(gDistance(pumpsUTM[i,], CLUsUTM, byid=TRUE)),]
  nearestCLU_ID[i]   <- as.numeric(as.character(temp$CLU_ID))
  nearestCLU_dist_km[i] <- min(gDistance(pumpsUTM[i,], CLUsUTM, byid=TRUE))/1000
}

#Convert back to regular dataframe
pumps <- as.data.frame(pumps)

#Assign in_CLU dummy and store CLU ID and area
pumps$CLU_ID <- pumps$IN_CLU.CLU_ID
pumps$CLU_county <- pumps$IN_CLU.County
pumps$CLU_acres <- pumps$IN_CLU.CLUAcres
pumps$in_CLU <- as.numeric(is.na(pumps$CLU_ID)==0)
summary(pumps$in_CLU)

#Append nearest CLU variables
pumps <- cbind(pumps, nearestCLU_ID, nearestCLU_dist_km)
summary(pumps[pumps$in_CLU==0,]$nearestCLU_dist_km)

#Plot points in CLUs
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  #geom_polygon(data=CLUs, aes(x=long, y=lat, group=group), 
  #             color="green", fill=NA, alpha=1) +
  geom_point(data=pumps[pumps$in_CLU==1,], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Plot points NOT in CLUs
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  #geom_polygon(data=CLUs, aes(x=long, y=lat, group=group), 
  #             color="green", fill=NA, alpha=1) +
  geom_point(data=pumps[(pumps$in_CLUs==0),], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Plot points NOT in CLUs that are <2km from nearest CLU
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  #geom_polygon(data=CLUs, aes(x=long, y=lat, group=group), 
  #             color="green", fill=NA, alpha=1) +
  geom_point(data=pumps[(pumps$in_CLU==0 & pumps$nearestCLU_dist_km<2),], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 


#Drop extraneous variables
pumps <- pumps[c("latlon_group","pump_lat","pump_long","longitude","latitude",
                 "CLU_ID","in_CLU","CLU_county","CLU_acres",
                 "nearestCLU_ID", "nearestCLU_dist_km")]

#Export results to csv
filename <- "apep_pump_coord_polygon_clu.csv"
write.table(pumps, file=filename , row.names=FALSE, col.names=TRUE, quote=FALSE, append=FALSE)


