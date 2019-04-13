#######################################################
#  Script to assign SP premises and APEP pumps to     #
#  California water basin polygons                    #
####################################################### 
rm(list = ls())

#Data downloaded from: https://data.cnra.ca.gov/dataset/ca-bulletin-118-groundwater-basins

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

#Load Water Basins shapefile
wbasn <- readOGR(dsn = "CA_Bulletin_118_Groundwater_Basins", layer = "CA_Bulletin_118_Groundwater_Basins")
proj4string(wbasn)
wbasn <- spTransform(wbasn, CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0"))

#Load CA state outline
CAoutline <- readOGR(dsn = "State", layer = "CA_State_TIGER2016")
proj4string(CAoutline)
CAoutline <- spTransform(CAoutline, CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0"))

#Confirm water basins align with California map
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=wbasn, aes(x=long, y=lat, group=group, color=rgb(0,0,1)), 
               color=rgb(0,1,0), fill=rgb(0,0,1), alpha=1) 

#Calculate area of each water basin polygon
wbasn@data$area_km2 <- area(wbasn)/1000000
data <- wbasn@data
summary(data$area_km2)

#Export dataset of water basins
setwd(paste0(path,"data/misc"))
filename <- "ca_water_basins_raw.txt"
write.table(data, file=filename , row.names=FALSE, col.names=TRUE, sep="%", quote=FALSE, append=FALSE)



#############################################
### 2. Assign SP lat/lons to water basins ###
#############################################

#Read PGE coordinates
setwd(paste0(path,"data/misc"))
prems <- read.delim2("pge_prem_coord_3pulls.txt",header=TRUE,sep=",",stringsAsFactors=FALSE)
prems$longitude <- as.numeric(prems$prem_lon)
prems$latitude <- as.numeric(prems$prem_lat)

#Convert to SpatialPointsDataFrame
coordinates(prems) <- ~ longitude + latitude
proj4string(prems) <- proj4string(wbasn)

#Assign each lat/lon to the water basin polygon it's contained in
prems@data$IN_wbasn <- over(prems, wbasn)

#Reproject everything into planar coordinates
utmStr <- "+proj=utm +zone=%d +datum=NAD83 +units=m +no_defs +ellps=GRS80 +towgs84=0,0,0"
crs <- CRS(sprintf(utmStr, 10))
wbasnUTM <- spTransform(wbasn, crs) #reproject into planar coordinates, because that's what the distance function uses
premsUTM <- spTransform(prems, crs)

#Create empy vectors to loop over, to calculate distance to a water basin
n <- nrow(prems@data)
nearestwbasn_ID <- numeric(n)
nearestwbasn_dist_km <- numeric(n)

#Create vector of only those observations where water basin is missing
missings <- c(1:n)[is.na(prems@data$IN_wbasn$OBJECTID)]

#Calculate distance to a water basin polygon, for lat/lons not contained in a polygon
for (j in seq_along(missings)) {
  i <- missings[j]
  temp <- wbasnUTM@data[which.min(gDistance(premsUTM[i,], wbasnUTM, byid=TRUE)),]
  nearestwbasn_ID[i]   <- as.numeric(as.character(temp$OBJECTID))
  nearestwbasn_dist_km[i] <- min(gDistance(premsUTM[i,], wbasnUTM, byid=TRUE))/1000
}

#Convert back to regular dataframe
prems <- as.data.frame(prems)

#Assign in_wbasn dummy and store water basin name and area
prems$wbasn <- prems$IN_wbasn.Basin_Name
prems$in_wbasn <- as.numeric(is.na(prems$wbasn)==0)
prems$wbasn_id <- as.numeric(as.character(prems$IN_wbasn.OBJECTID))
summary(prems$in_wbasn)
summary(prems$in_wbasn[prems$bad_geocode_flag==0])
summary(prems$in_wbasn[prems$bad_geocode_flag==0 & prems$pull=="20180719"])

#Append nearest water basin variables
prems <- cbind(prems, nearestwbasn_ID, nearestwbasn_dist_km)
summary(prems[prems$in_wbasn==0,]$nearestwbasn_dist_km)
summary(prems[prems$in_wbasn==0 & prems$bad_geocode_flag==0 & prems$pull=="20180719",]$nearestwbasn_dist_km)

#Plot points in water basins
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=wbasn, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=prems[prems$in_wbasn==1,], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Plot points NOT in water basins
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=wbasn, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=prems[(prems$in_wbasn==0 & prems$bad_geocode_flag==0),], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Plot points NOT in water basins that are in APEP
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=wbasn, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=prems[(prems$in_wbasn==0 & prems$bad_geocode_flag==0 & prems$pull=="20180719"),], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Plot points NOT in water basins that are in APEP, and are <2km from nearest water basin
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=wbasn, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=prems[(prems$in_wbasn==0 & prems$bad_geocode_flag==0 & prems$pull=="20180719" & prems$nearestwbasn_dist_km<2),], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Drop extraneous variables
prems <- prems[c("sp_uuid","prem_lat","prem_long","longitude","latitude",
                 "bad_geocode_flag","pull","wbasn","in_wbasn","wbasn_id",
                 "nearestwbasn_ID", "nearestwbasn_dist_km")]

#Export results to txt
filename <- "pge_prem_coord_polygon_wbasn.txt"
write.table(prems, file=filename , row.names=FALSE, col.names=TRUE, sep="%", quote=FALSE, append=FALSE)


###############################################
### 3. Assign APEP lat/lons to water basins ###
###############################################

#Read APEP coordinates
setwd(paste0(path,"data/misc"))
pumps <- read.delim2("apep_pump_coord.txt",header=TRUE,sep=",",stringsAsFactors=FALSE)
pumps$longitude <- as.numeric(pumps$pump_lon)
pumps$latitude <- as.numeric(pumps$pump_lat)

#Convert to SpatialPointsDataFrame
coordinates(pumps) <- ~ longitude + latitude
proj4string(pumps) <- proj4string(wbasn)

#Assign each lat/lon to the water basin polygon it's contained in
pumps@data$IN_wbasn <- over(pumps, wbasn)

#Reproject everything into planar coordinates
utmStr <- "+proj=utm +zone=%d +datum=NAD83 +units=m +no_defs +ellps=GRS80 +towgs84=0,0,0"
crs <- CRS(sprintf(utmStr, 10))
wbasnUTM <- spTransform(wbasn, crs) #reproject into planar coordinates, because that's what the distance function uses
pumpsUTM <- spTransform(pumps, crs)

#Create empy vectors to loop over, to calculate distance to a water basin
n <- nrow(pumps@data)
nearestwbasn_ID <- numeric(n)
nearestwbasn_dist_km <- numeric(n)

#Create vector of only those observations where water basin is missing
missings <- c(1:n)[is.na(pumps@data$IN_wbasn$OBJECTID)]

#Calculate distance to a water basin polygon, for lat/lons not contained in a polygon
for (j in seq_along(missings)) {
  i <- missings[j]
  temp <- wbasnUTM@data[which.min(gDistance(pumpsUTM[i,], wbasnUTM, byid=TRUE)),]
  nearestwbasn_ID[i]   <- as.numeric(as.character(temp$OBJECTID))
  nearestwbasn_dist_km[i] <- min(gDistance(pumpsUTM[i,], wbasnUTM, byid=TRUE))/1000
}

#Convert back to regular dataframe
pumps <- as.data.frame(pumps)

#Assign in_wbasn dummy and store water basin name and area
pumps$wbasn <- pumps$IN_wbasn.Basin_Name
pumps$in_wbasn <- as.numeric(is.na(pumps$wbasn)==0)
pumps$wbasn_id <- as.numeric(as.character(pumps$IN_wbasn.OBJECTID))
summary(pumps$in_wbasn)

#Append nearest water basin variables
pumps <- cbind(pumps, nearestwbasn_ID, nearestwbasn_dist_km)
summary(pumps[pumps$in_wbasn==0,]$nearestwbasn_dist_km)

#Plot points in water basins
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=wbasn, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=pumps[pumps$in_wbasn==1,], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Plot points NOT in water basins
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=wbasn, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=pumps[(pumps$in_wbasn==0),], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Plot points NOT in water basins that are in APEP, and are <2km from nearest water basin
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=wbasn, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=pumps[(pumps$in_wbasn==0 & pumps$nearestwbasn_dist_km<2),], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Drop extraneous variables
pumps <- pumps[c("latlon_group","pump_lat","pump_long","longitude","latitude",
                 "wbasn","in_wbasn","wbasn_id",
                 "nearestwbasn_ID", "nearestwbasn_dist_km")]

#Export results to txt
filename <- "apep_pump_coord_polygon_wbasn.txt"
write.table(pumps, file=filename , row.names=FALSE, col.names=TRUE, sep="%", quote=FALSE, append=FALSE)


