######################################################
#  Script to assign PGE SPs, APEP pumps, and SCE SPs #
#  to California surface water district polygons     #
######################################################
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
library(maptools) #, lib.loc=libP)
library(rgdal) #, lib.loc=libP)
library(rgeos) #, lib.loc=libP)
library(raster) #, lib.loc=libP)
library(SDMTools) #, lib.loc=libP)

path <- "T:/Projects/Pump Data/"

#####################################################################
### 1. Prep all relevant in shapefiles (Hagerty selected poygons) ###
#####################################################################

#Load Water Districts shapefile
setwd(paste0(path,"data/surface_water"))
wdist <- readOGR(dsn = "Subsetted_User_SF", layer = "users_positive_alloc_filtered")
proj4string(wdist)
wdist <- spTransform(wdist, CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0"))

#Load CA state outline
setwd(paste0(path,"data/spatial"))
CAoutline <- readOGR(dsn = "State", layer = "CA_State_TIGER2016")
proj4string(CAoutline)
CAoutline <- spTransform(CAoutline, CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0"))

#Confirm water districts align with California map
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=wdist, aes(x=long, y=lat, group=group, color=rgb(0,0,1)), 
               color=rgb(0,1,0), fill=rgb(0,0,1), alpha=1) 

#Calculate area of each water district polygon
wdist@data$area_km2 <- area(wdist)/1000000
data <- wdist@data
summary(data$area_km2)


####################################################
### 2. Assign PGE SP lat/lons to water districts ###
####################################################

#Read PGE SP coordinates
setwd(paste0(path,"data/misc"))
prems <- read.delim2("pge_prem_coord_3pulls.txt",header=TRUE,sep=",",stringsAsFactors=FALSE)
prems$longitude <- as.numeric(prems$prem_lon)
prems$latitude <- as.numeric(prems$prem_lat)

#Convert to SpatialPointsDataFrame
coordinates(prems) <- ~ longitude + latitude
proj4string(prems) <- proj4string(wdist)

#Assign each lat/lon to the water district polygon it's contained in
prems@data$IN_wdist <- over(prems, wdist)

#Reproject everything into planar coordinates
utmStr <- "+proj=utm +zone=%d +datum=NAD83 +units=m +no_defs +ellps=GRS80 +towgs84=0,0,0"
crs <- CRS(sprintf(utmStr, 10))
wdistUTM <- spTransform(wdist, crs) #reproject into planar coordinates, because that's what the distance function uses
premsUTM <- spTransform(prems, crs)

#Create empy vectors to loop over, to calculate distance to a water district
n <- nrow(prems@data)
nearestWDist <- character(n)
nearestWDist_ID <- numeric(n)
nearestWDist_dist_m <- numeric(n)

#Create vector of only those observations where water district is missing
missings <- c(1:n)[is.na(prems@data$IN_wdist$user_id)]

#Calculate distance to a water district polygon, for lat/lons not contained in a polygon
for (j in seq_along(missings)) {
  i <- missings[j]
  temp <- wdistUTM@data[which.min(gDistance(premsUTM[i,], wdistUTM, byid=TRUE)),]
  nearestWDist[i]      <- as.character(temp$username)
  nearestWDist_ID[i]   <- as.numeric(as.character(temp$user_id))
  nearestWDist_dist_m[i] <- min(gDistance(premsUTM[i,], wdistUTM, byid=TRUE))
}

#Convert back to regular dataframe
prems <- as.data.frame(prems)

#Assign in_wdist dummy and store water district name and area
prems$wdist <- prems$IN_wdist.username
prems$in_wdist <- as.numeric(is.na(prems$wdist)==0)
prems$wdist_id <- as.numeric(as.character(prems$IN_wdist.user_id))
summary(prems$in_wdist)
summary(prems$in_wdist[prems$bad_geocode_flag==0])
summary(prems$in_wdist[prems$bad_geocode_flag==0 & prems$pull=="20180719"])

#Append nearest water district variables
prems <- cbind(prems, nearestWDist, nearestWDist_ID, nearestWDist_dist_m)
summary(prems[prems$in_wdist==0,]$nearestWDist_dist_m)
summary(prems[prems$in_wdist==0 & prems$bad_geocode_flag==0 & prems$pull=="20180719",]$nearestWDist_dist_m)

#Plot points in water districts
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=wdist, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=prems[prems$in_wdist==1,], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Plot points NOT in water districts
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=wdist, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=prems[(prems$in_wdist==0 & prems$bad_geocode_flag==0),], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Plot points NOT in water districts that are in APEP
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=wdist, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=prems[(prems$in_wdist==0 & prems$bad_geocode_flag==0 & prems$pull=="20180719"),], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Plot points NOT in water districts that are in APEP, and are <2km from nearest water district
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=wdist, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=prems[(prems$in_wdist==0 & prems$bad_geocode_flag==0 & prems$pull=="20180719" & prems$nearestWDist_dist_m<2000),], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Drop extraneous variables
prems <- prems[c("sp_uuid","prem_lat","prem_long","longitude","latitude",
                 "bad_geocode_flag","pull","wdist","in_wdist","wdist_id",
                 "nearestWDist", "nearestWDist_ID", "nearestWDist_dist_m")]

#Export results to txt
filename <- "pge_prem_coord_polygon_wdist_hag.txt"
write.table(prems, file=filename , row.names=FALSE, col.names=TRUE, sep="%", quote=FALSE, append=FALSE)


##################################################
### 3. Assign APEP lat/lons to water districts ###
##################################################

#Read APEP pump coordinates
setwd(paste0(path,"data/misc"))
pumps <- read.delim2("apep_pump_coord.txt",header=TRUE,sep=",",stringsAsFactors=FALSE)
pumps$longitude <- as.numeric(pumps$pump_lon)
pumps$latitude <- as.numeric(pumps$pump_lat)

#Convert to SpatialPointsDataFrame
coordinates(pumps) <- ~ longitude + latitude
proj4string(pumps) <- proj4string(wdist)

#Assign each lat/lon to the water district polygon it's contained in
pumps@data$IN_wdist <- over(pumps, wdist)

#Reproject everything into planar coordinates
utmStr <- "+proj=utm +zone=%d +datum=NAD83 +units=m +no_defs +ellps=GRS80 +towgs84=0,0,0"
crs <- CRS(sprintf(utmStr, 10))
wdistUTM <- spTransform(wdist, crs) #reproject into planar coordinates, because that's what the distance function uses
pumpsUTM <- spTransform(pumps, crs)

#Create empy vectors to loop over, to calculate distance to a water district
n <- nrow(pumps@data)
nearestWDist <- character(n)
nearestWDist_ID <- numeric(n)
nearestWDist_dist_m <- numeric(n)

#Create vector of only those observations where water district is missing
missings <- c(1:n)[is.na(pumps@data$IN_wdist$user_id)]

#Calculate distance to a water district polygon, for lat/lons not contained in a polygon
for (j in seq_along(missings)) {
  i <- missings[j]
  temp <- wdistUTM@data[which.min(gDistance(pumpsUTM[i,], wdistUTM, byid=TRUE)),]
  nearestWDist[i]      <- as.character(temp$username)
  nearestWDist_ID[i]   <- as.numeric(as.character(temp$user_id))
  nearestWDist_dist_m[i] <- min(gDistance(pumpsUTM[i,], wdistUTM, byid=TRUE))
}

#Convert back to regular dataframe
pumps <- as.data.frame(pumps)

#Assign in_wdist dummy and store water district name and area
pumps$wdist <- pumps$IN_wdist.username
pumps$in_wdist <- as.numeric(is.na(pumps$wdist)==0)
pumps$wdist_id <- as.numeric(as.character(pumps$IN_wdist.user_id))
summary(pumps$in_wdist)

#Append nearest water district variables
pumps <- cbind(pumps, nearestWDist, nearestWDist_ID, nearestWDist_dist_m)
summary(pumps[pumps$in_wdist==0,]$nearestWDist_dist_m)

#Plot points in water districts
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=wdist, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=pumps[pumps$in_wdist==1,], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Plot points NOT in water districts
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=wdist, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=pumps[(pumps$in_wdist==0),], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Plot points NOT in water districts that are in APEP, and are <2km from nearest water district
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=wdist, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=pumps[(pumps$in_wdist==0 & pumps$nearestWDist_dist_m<2000),], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Drop extraneous variables
pumps <- pumps[c("latlon_group","pump_lat","pump_long","longitude","latitude",
                 "wdist","in_wdist","wdist_id",
                 "nearestWDist", "nearestWDist_ID", "nearestWDist_dist_m")]

#Export results to txt
filename <- "apep_pump_coord_polygon_wdist_hag.txt"
write.table(pumps, file=filename , row.names=FALSE, col.names=TRUE, sep="%", quote=FALSE, append=FALSE)


####################################################
### 4. Assign SCE SP lat/lons to water districts ###
####################################################

#Read SCE SP coordinates
setwd(paste0(path,"data/misc"))
prems <- read.delim2("sce_prem_coord_1pull.txt",header=TRUE,sep=",",stringsAsFactors=FALSE)
prems$longitude <- as.numeric(prems$prem_lon)
prems$latitude <- as.numeric(prems$prem_lat)

#Convert to SpatialPointsDataFrame
coordinates(prems) <- ~ longitude + latitude
proj4string(prems) <- proj4string(wdist)

#Assign each lat/lon to the water district polygon it's contained in
prems@data$IN_wdist <- over(prems, wdist)

#Reproject everything into planar coordinates
utmStr <- "+proj=utm +zone=%d +datum=NAD83 +units=m +no_defs +ellps=GRS80 +towgs84=0,0,0"
crs <- CRS(sprintf(utmStr, 10))
wdistUTM <- spTransform(wdist, crs) #reproject into planar coordinates, because that's what the distance function uses
premsUTM <- spTransform(prems, crs)

#Create empy vectors to loop over, to calculate distance to a water district
n <- nrow(prems@data)
nearestWDist <- character(n)
nearestWDist_ID <- numeric(n)
nearestWDist_dist_m <- numeric(n)

#Create vector of only those observations where water district is missing
missings <- c(1:n)[is.na(prems@data$IN_wdist$user_id)]

#Calculate distance to a water district polygon, for lat/lons not contained in a polygon
for (j in seq_along(missings)) {
  i <- missings[j]
  temp <- wdistUTM@data[which.min(gDistance(premsUTM[i,], wdistUTM, byid=TRUE)),]
  nearestWDist[i]      <- as.character(temp$username)
  nearestWDist_ID[i]   <- as.numeric(as.character(temp$user_id))
  nearestWDist_dist_m[i] <- min(gDistance(premsUTM[i,], wdistUTM, byid=TRUE))
}

#Convert back to regular dataframe
prems <- as.data.frame(prems)

#Assign in_wdist dummy and store water district name and area
prems$wdist <- prems$IN_wdist.username
prems$in_wdist <- as.numeric(is.na(prems$wdist)==0)
prems$wdist_id <- as.numeric(as.character(prems$IN_wdist.user_id))
summary(prems$in_wdist)
summary(prems$in_wdist[prems$bad_geocode_flag==0])

#Append nearest water district variables
prems <- cbind(prems, nearestWDist, nearestWDist_ID, nearestWDist_dist_m)
summary(prems[prems$in_wdist==0,]$nearestWDist_dist_m)

#Plot points in water districts
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=wdist, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=prems[prems$in_wdist==1,], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Plot points NOT in water districts
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=wdist, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=prems[(prems$in_wdist==0 & prems$bad_geocode_flag==0),], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Plot points NOT in water districts that and are <2km from nearest water district
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=wdist, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=prems[(prems$in_wdist==0 & prems$bad_geocode_flag==0 & prems$nearestWDist_dist_m<2000),], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Drop extraneous variables
prems <- prems[c("sp_uuid","prem_lat","prem_long","longitude","latitude",
                 "bad_geocode_flag","pull","wdist","in_wdist","wdist_id",
                 "nearestWDist", "nearestWDist_ID", "nearestWDist_dist_m")]

#Export results to txt
filename <- "sce_prem_coord_polygon_wdist_hag.txt"
write.table(prems, file=filename , row.names=FALSE, col.names=TRUE, sep="%", quote=FALSE, append=FALSE)


