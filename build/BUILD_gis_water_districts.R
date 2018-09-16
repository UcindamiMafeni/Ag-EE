#######################################################
#  Script to validate lat/lon coordinates from PGE    #
#       data, based on (repoted) CA climate zones     #
####################################################### 
rm(list = ls())

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


##########################################
### 1. Prep all relevant in shapefiles ###
##########################################

setwd("S:/Matt/ag_pump/data/spatial")

#Load Water Districts shapefile
wdist <- readOGR(dsn = "Water_Districts", layer = "Water_Districts")
proj4string(wdist)
wdist <- spTransform(wdist, CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0"))

#Load CA state outline
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


################################################
### 2. Assign SP lat/lons to water districts ###
################################################

#Read PGE coordinates
setwd("S:/Matt/ag_pump/data/misc")
prems <- read.delim2("pge_prem_coord_3pulls.txt",header=TRUE,sep=",",stringsAsFactors=FALSE)
prems$longitude <- as.numeric(prems$prem_lon)
prems$latitude <- as.numeric(prems$prem_lat)

#Convert to SpatialPointsDataFrame
coordinates(prems) <- ~ longitude + latitude
proj4string(prems) <- proj4string(wdist)

#Assign each lat/lon to the water district polygon it's contained in
prems@data$IN_wdist <- over(prems, wdist)

#Calculate distance to a water district polygon, for lat/lons not contained in a polygon
utmStr <- "+proj=utm +zone=%d +datum=NAD83 +units=m +no_defs +ellps=GRS80 +towgs84=0,0,0"
crs <- CRS(sprintf(utmStr, 10))
wdistUTM <- spTransform(wdist, crs) #reproject into planar coordinates, because that's what the distance function uses
premsUTM <- spTransform(prems, crs)

n <- nrow(prems@data)
nearestWDist <- character(n)
nearestWDist_ID <- character(n)
nearestWDist_dist <- numeric(n)

for (i in seq_along(nearestWDist)) {
  nearestWDist[i]      <- wdistUTM@data$AGENCYNAME[which.min(gDistance(premsUTM[i,], wdistUTM, byid=TRUE))]
  nearestWDist_ID[i]   <- wdistUTM@data$AGENCYUNIQ[which.min(gDistance(premsUTM[i,], wdistUTM, byid=TRUE))]
  nearestWDist_dist[i] <- min(gDistance(premsUTM[i,], wdistUTM, byid=TRUE))
}




#Convert back to regular dataframe
prems <- as.data.frame(prems)

#Assign in_wdist dummy and store water district name and area
prems$wdist <- prems$IN_wdist.AGENCYNAME
prems$in_wdist <- as.numeric(is.na(prems$wdist)==0)
prems$wdist_id <- as.numeric(as.character(prems$IN_wdist.AGENCYUNIQ))
prems$wdist_area_km2 <- as.numeric(prems$IN_wdist.area_km2)
summary(prems$in_wdist)
summary(prems$in_wdist[prems$bad_geocode_flag==0])
summary(prems$in_wdist[prems$bad_geocode_flag==0 & prems$pull=="20180719"])


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

#Drop extraneous variables
prems <- prems[c("sp_uuid","prem_lat","prem_long","longitude","latitude",
                 "bad_geocode_flag","pull","wdist","in_wdist","wdist_id","wdist_area_km2")]


#Convert to SpatialPointsDataFrame
coordinates(prems) <- ~ longitude + latitude
proj4string(prems) <- proj4string(wdist)



## 2.4 Deal with lat/lons that still haven't been classified

#Assign edge cases
prems$edge_sce <- as.numeric(prems$in_calif==1 & prems$in_pge==0 & prems$in_pou==0 & prems$longitude>(-120) & 
                             prems$longitude<(-118.5) & prems$latitude>33.8 & prems$latitude<37.5)
prems$edge_coast1 <- as.numeric(prems$in_calif==1 & prems$in_pge==0 & prems$in_pou==0 & prems$longitude>(-125) & 
                               prems$longitude<(-123) & prems$latitude>37.5 & prems$latitude<40)
prems$edge_coast2 <- as.numeric(prems$in_calif==1 & prems$in_pge==0 & prems$in_pou==0 & prems$longitude>(-123) & 
                                  prems$longitude<(-122.5) & prems$latitude>37.5 & prems$latitude<39)
prems$edge_coast3 <- as.numeric(prems$in_calif==1 & prems$in_pge==0 & prems$in_pou==0 & prems$longitude>(-121.5) & 
                                  prems$longitude<(-120) & prems$latitude>35 & prems$latitude<35.5)
prems$edge_coast <- prems$edge_coast1 + prems$edge_coast2 + prems$edge_coast3
prems$edge_mid <- as.numeric(prems$in_calif==1 & prems$in_pge==0 & prems$in_pou==0 & prems$longitude>(-121.5) & 
                               prems$longitude<(-120) & prems$latitude>37.5 & prems$latitude<38.1)
prems$edge_lodi <- as.numeric(prems$in_calif==1 & prems$in_pge==0 & prems$in_pou==0 & prems$longitude>(-121.5) & 
                               prems$longitude<(-120) & prems$latitude>38.1 & prems$latitude<38.5)
prems$edge_redding <- as.numeric(prems$in_calif==1 & prems$in_pge==0 & prems$in_pou==0 & prems$longitude>(-123) & 
                                prems$longitude<(-121) & prems$latitude>40 & prems$latitude<41)

#Plot edge cases
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=pge, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=prems[prems$edge_sce==1,], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) +
  geom_point(data=prems[prems$edge_coast==1,], aes(x=longitude, y=latitude), color=rgb(1,0,0), shape=19, 
             alpha=1, size=1) +
  geom_point(data=prems[prems$edge_mid==1,], aes(x=longitude, y=latitude), color=rgb(0,0,0), shape=19, 
             alpha=1, size=1) +
  geom_point(data=prems[prems$edge_lodi==1,], aes(x=longitude, y=latitude), color=rgb(0,1,1), shape=19, 
             alpha=1, size=1) +
  geom_point(data=prems[prems$edge_redding==1,], aes(x=longitude, y=latitude), color=rgb(0.2,0.2,0.2), shape=19, 
             alpha=1, size=1) +
  geom_point(data=prems[(prems$in_calif==1 & prems$in_pge==0 & prems$in_pou==0 & prems$edge_sce==0 & prems$edge_coast==0 &
                         prems$edge_mid==0 & prems$edge_lodi==0 & prems$edge_redding==0),], aes(x=longitude, y=latitude), color=rgb(1,1,0), shape=19, 
             alpha=1, size=1) 

#Clean up and consolidate
prems$in_pge[prems$edge_coast==1] <- 1    # 3 edge cases are really in PGE proper
prems$in_pou[prems$edge_lodi==1] <- 1     # Lodi Electric Utility
prems$pou[prems$edge_lodi==1] <- "Lodi Electric Utility"
prems$in_pou[prems$edge_redding==1] <- 1     # Redding Electric Utility
prems$pou[prems$edge_redding==1] <- "Redding Electric Utility"
prems$in_pou[prems$edge_mid==1] <- 1      # Area served by PGE and Merced Irrigation District
prems$pou <- as.character(prems$pou)
prems$pou[prems$edge_mid==1] <- "PGE/Merced Irrigation District"
prems$in_pge[prems$edge_sce==1] <- 1      # several farms on the PGE/SCE border
prems$pou[prems$edge_sce==1] <- "On PGE/SCE border"
prems <- prems[c("sp_uuid","prem_lat","prem_long","climate_zone_cd","longitude","latitude","czone","in_calif","in_pge","in_pou","pou")]

#Flag bad lat/lons: not in PGE service territory or in a POU
prems$bad_geocode <- as.numeric(prems$in_calif==0 | (prems$in_pge==0 & prems$in_pou==0))
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=pge, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=prems[prems$bad_geocode==1,], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 
  


#########################
### 3.  Climate Zones ###
#########################

#Convert to SpatialPointsDataFrame
coordinates(prems) <- ~ longitude + latitude
proj4string(prems) <- proj4string(pge)

#Use Cliamte ZOne shapefile to assign 
prems@data$Zone <- over(prems, cz)

#Convert back to regular dataframe
prems <- as.data.frame(prems)

#Rename GIS assigned climate zone
names(prems)[13] <- "czone_gis"

#Export results to CSV
filename <- "pge_prem_coord_polygon_20180827.csv"
write.csv(prems, file=filename , row.names=FALSE, col.names=TRUE, sep=",", quote=FALSE, append=FALSE)


