##########################################
#  Script to assign PGE SPs, APEP pumps, #
#  and SCE SPs to California counties    #
##########################################
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

##########################################
### 1. Prep all relevant in shapefiles ###
##########################################

setwd(paste0(path,"data/spatial"))

#Load Counties shapefile
counties <- readOGR(dsn = "Counties", layer = "CA_Counties_TIGER2016")
proj4string(counties)
counties <- spTransform(counties, CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0"))

#Load CA state outline
CAoutline <- readOGR(dsn = "State", layer = "CA_State_TIGER2016")
proj4string(CAoutline)
CAoutline <- spTransform(CAoutline, CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0"))

#Confirm counties align with California map
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=counties, aes(x=long, y=lat, group=group, color=rgb(0,0,1)), 
               color=rgb(0,1,0), fill=rgb(0,0,1), alpha=1) 


#############################################
### 2. Assign PGE SP lat/lons to counties ###
#############################################

#Read PGE coordinates
setwd(paste0(path,"data/misc"))
prems <- read.delim2("pge_prem_coord_3pulls.txt",header=TRUE,sep=",",stringsAsFactors=FALSE)
prems$longitude <- as.numeric(prems$prem_lon)
prems$latitude <- as.numeric(prems$prem_lat)

#Convert to SpatialPointsDataFrame
coordinates(prems) <- ~ longitude + latitude
proj4string(prems) <- proj4string(counties)

#Assign each lat/lon to the county polygon it's contained in
prems@data$IN_county <- over(prems, counties)

#Reproject everything into planar coordinates
utmStr <- "+proj=utm +zone=%d +datum=NAD83 +units=m +no_defs +ellps=GRS80 +towgs84=0,0,0"
crs <- CRS(sprintf(utmStr, 10))
countiesUTM <- spTransform(counties, crs) #reproject into planar coordinates, because that's what the distance function uses
premsUTM <- spTransform(prems, crs)

#Create empy vectors to loop over, to calculate distance to a county
n <- nrow(prems@data)
nearestCounty <- character(n)
nearestCounty_ID <- numeric(n)
nearestCounty_dist_km <- numeric(n)

#Create vector of only those observations where county is missing
missings <- c(1:n)[is.na(prems@data$IN_county$COUNTYFP)]

#Calculate distance to a county, for lat/lons not contained in a polygon
for (j in seq_along(missings)) {
  i <- missings[j]
  temp <- countiesUTM@data[which.min(gDistance(premsUTM[i,], countiesUTM, byid=TRUE)),]
  nearestCounty[i]      <- as.character(temp$NAME)
  nearestCounty_ID[i]   <- as.numeric(as.character(temp$COUNTYFP))
  nearestCounty_dist_km[i] <- min(gDistance(premsUTM[i,], countiesUTM, byid=TRUE))/1000
}

#Convert back to regular dataframe
prems <- as.data.frame(prems)

#Assign in_county dummy and store county name
prems$county <- prems$IN_county.NAME
prems$in_county <- as.numeric(is.na(prems$county)==0)
prems$county_id <- as.numeric(as.character(prems$IN_county.COUNTYFP))
summary(prems$in_county)
summary(prems$in_county[prems$bad_geocode_flag==0])

#Append nearest county variables
prems <- cbind(prems, nearestCounty, nearestCounty_ID, nearestCounty_dist_km)
summary(prems[prems$in_county==0,]$nearestCounty_dist_km)

#Plot points in counties
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=counties, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=prems[prems$in_county==1,], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Plot points NOT in counties // all have bad geocodes
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=counties, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=prems[(prems$in_county==0),], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Drop extraneous variables
prems <- prems[c("sp_uuid","county","in_county","IN_county.GEOID")]
names(prems)[4] <- "fips"

#Export results to txt
filename <- "pge_prem_coord_polygon_counties.txt"
write.table(prems, file=filename , row.names=FALSE, col.names=TRUE, sep="%", quote=FALSE, append=FALSE)


##################################################
### 3. Assign APEP lat/lons to counties ###
##################################################

#Read APEP coordinates
setwd(paste0(path,"data/misc"))
pumps <- read.delim2("apep_pump_coord.txt",header=TRUE,sep=",",stringsAsFactors=FALSE)
pumps$longitude <- as.numeric(pumps$pump_lon)
pumps$latitude <- as.numeric(pumps$pump_lat)

#Convert to SpatialPointsDataFrame
coordinates(pumps) <- ~ longitude + latitude
proj4string(pumps) <- proj4string(counties)

#Assign each lat/lon to the county polygon it's contained in
pumps@data$IN_county <- over(pumps, counties)

#Reproject everything into planar coordinates
utmStr <- "+proj=utm +zone=%d +datum=NAD83 +units=m +no_defs +ellps=GRS80 +towgs84=0,0,0"
crs <- CRS(sprintf(utmStr, 10))
countiesUTM <- spTransform(counties, crs) #reproject into planar coordinates, because that's what the distance function uses
pumpsUTM <- spTransform(pumps, crs)

#Create empy vectors to loop over, to calculate distance to a county
n <- nrow(pumps@data)
nearestCounty <- character(n)
nearestCounty_ID <- numeric(n)
nearestCounty_dist_km <- numeric(n)

#Create vector of only those observations where county is missing
missings <- c(1:n)[is.na(pumps@data$IN_county$COUNTYFP)]

#Calculate distance to a county polygon, for lat/lons not contained in a polygon
for (j in seq_along(missings)) {
  i <- missings[j]
  temp <- countiesUTM@data[which.min(gDistance(pumpsUTM[i,], countiesUTM, byid=TRUE)),]
  nearestCounty[i]      <- as.character(temp$NAME)
  nearestCounty_ID[i]   <- as.numeric(as.character(temp$COUNTYFP))
  nearestCounty_dist_km[i] <- min(gDistance(pumpsUTM[i,], countiesUTM, byid=TRUE))/1000
}

#Convert back to regular dataframe
pumps <- as.data.frame(pumps)

#Assign in_county dummy and store county name
pumps$county <- pumps$IN_county.NAME
pumps$in_county <- as.numeric(is.na(pumps$county)==0)
pumps$county_id <- as.numeric(as.character(pumps$IN_county.COUNTYFP))
summary(pumps$in_county)

#Append nearest county variables
pumps <- cbind(pumps, nearestCounty, nearestCounty_ID, nearestCounty_dist_km)
summary(pumps[pumps$in_county==0,]$nearestCounty_dist_km)

#Plot points in counties
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=counties, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=pumps[pumps$in_county==1,], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Plot points NOT in counties // all are not in California
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=counties, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=pumps[(pumps$in_county==0),], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Plot points NOT in counties that are in APEP, and are <2km from nearest county // 4 pumps in the ocean...
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=counties, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=pumps[(pumps$in_county==0 & pumps$nearestCounty_dist_km<2),], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Drop extraneous variables
pumps <- pumps[c("latlon_group","county","in_county","IN_county.GEOID")]
names(pumps)[4] <- "fips"

#Export results to txt
filename <- "apep_pump_coord_polygon_counties.txt"
write.table(pumps, file=filename , row.names=FALSE, col.names=TRUE, sep="%", quote=FALSE, append=FALSE)


#############################################
### 4. Assign SCE SP lat/lons to counties ###
#############################################

#Read PGE coordinates
setwd(paste0(path,"data/misc"))
socal <- read.delim2("sce_prem_coord_1pull.txt",header=TRUE,sep=",",stringsAsFactors=FALSE)
socal$longitude <- as.numeric(socal$prem_lon)
socal$latitude <- as.numeric(socal$prem_lat)

#Convert to SpatialPointsDataFrame
coordinates(socal) <- ~ longitude + latitude
proj4string(socal) <- proj4string(counties)

#Assign each lat/lon to the county polygon it's contained in
socal@data$IN_county <- over(socal, counties)

#Reproject everything into planar coordinates
utmStr <- "+proj=utm +zone=%d +datum=NAD83 +units=m +no_defs +ellps=GRS80 +towgs84=0,0,0"
crs <- CRS(sprintf(utmStr, 10))
countiesUTM <- spTransform(counties, crs) #reproject into planar coordinates, because that's what the distance function uses
socalUTM <- spTransform(socal, crs)

#Create empy vectors to loop over, to calculate distance to a county
n <- nrow(socal@data)
nearestCounty <- character(n)
nearestCounty_ID <- numeric(n)
nearestCounty_dist_km <- numeric(n)

#Create vector of only those observations where county is missing
missings <- c(1:n)[is.na(socal@data$IN_county$COUNTYFP)]

#Calculate distance to a county, for lat/lons not contained in a polygon
for (j in seq_along(missings)) {
  i <- missings[j]
  temp <- countiesUTM@data[which.min(gDistance(socalUTM[i,], countiesUTM, byid=TRUE)),]
  nearestCounty[i]      <- as.character(temp$NAME)
  nearestCounty_ID[i]   <- as.numeric(as.character(temp$COUNTYFP))
  nearestCounty_dist_km[i] <- min(gDistance(socalUTM[i,], countiesUTM, byid=TRUE))/1000
}

#Convert back to regular dataframe
socal <- as.data.frame(socal)

#Assign in_county dummy and store county name
socal$county <- socal$IN_county.NAME
socal$in_county <- as.numeric(is.na(socal$county)==0)
socal$county_id <- as.numeric(as.character(socal$IN_county.COUNTYFP))
summary(socal$in_county)
summary(socal$in_county[socal$bad_geocode_flag==0])

#Append nearest county variables
socal <- cbind(socal, nearestCounty, nearestCounty_ID, nearestCounty_dist_km)
summary(socal[socal$in_county==0,]$nearestCounty_dist_km)

#Plot points in counties
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=counties, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=socal[socal$in_county==1,], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Plot points NOT in counties // all have bad geocodes
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=counties, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=socal[(socal$in_county==0),], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Drop extraneous variables
socal <- socal[c("sp_uuid","county","in_county","IN_county.GEOID")]
names(socal)[4] <- "fips"

#Export results to txt
filename <- "sce_prem_coord_polygon_counties.txt"
write.table(socal, file=filename , row.names=FALSE, col.names=TRUE, sep="%", quote=FALSE, append=FALSE)

