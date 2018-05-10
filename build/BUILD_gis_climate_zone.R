#######################################################
#  Script to validate lat/lon coordinates from PGE    #
#       data, based on (repoted) CA climate zones     #
####################################################### 
rm(list = ls())
library(ggmap)
library(ggplot2)
library(gstat)
library(sp)
library(maptools)
library(rgdal)
library(rgeos)
library(raster)
library(SDMTools)


##########################################
### 1. Prep all relevant in shapefiles ###
##########################################

setwd("S:/Matt/ag_pump/data/spatial")

#Load CLimate Zones shapefile
cz <- readOGR(dsn = "CEC_climate_zones", layer = "CA_Building_Standards_Climate_Zones")
proj4string(cz)
cz <- spTransform(cz, CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0"))

#Load CA state outline
CAoutline <- readOGR(dsn = "State", layer = "CA_State_TIGER2016")
proj4string(CAoutline)
CAoutline <- spTransform(CAoutline, CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0"))

#Confirm climate zones tesselate California
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=cz, aes(x=long, y=lat, group=group, color=rgb(0,0,1)), 
               color=rgb(0,1,0), fill=rgb(0,0,1), alpha=1) 

#Load CA utility IOUs
IOUs <- readOGR(dsn = "Service territories", layer = "CA_Electric_Investor_Owned_Utilities_IOUs")
proj4string(IOUs)
IOUs <- spTransform(IOUs, CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0"))

#Load California utility POUs
POUs <- readOGR(dsn = "Service territories", layer = "POU")
proj4string(POUs)
POUs <- spTransform(POUs, CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0"))

#Confirm CA utilities (mostly) tesselate California
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=IOUs, aes(x=long, y=lat, group=group, color=rgb(0,0,1)), 
               color=rgb(0,1,0), fill=rgb(0,0,1), alpha=1) +
  geom_polygon(data=POUs, aes(x=long, y=lat, group=group, color=rgb(1,0,0)), 
               color=rgb(1,0,0), fill=rgb(1,0,0), alpha=1) 

#Isolate PGE
pge <- IOUs[IOUs$LABEL=="Pacific Gas & Electric Company",]

#Plot PGE Service territory inside CA outline
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=pge, aes(x=long, y=lat, group=group, color=rgb(0,0,1)), 
               color=rgb(0,1,0), fill=NA, alpha=1) 

#Identify POUs inside PGE territory
POUs@data$IN_pge <- over(POUs,pge)
POUs@data$in_pge <- as.numeric(is.na(POUs@data$IN_pge$LABEL)==0)
POUs <- POUs[POUs$in_pge==1,]
POUs <- POUs[POUs$LABEL!="Los Angeles Department of Water & Power",]

#Plot POUs inside PGE Service territory inside CA outline
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=pge, aes(x=long, y=lat, group=group, color=rgb(0,0,1)), 
               color=rgb(0,1,0), fill=NA, alpha=1) +
  geom_polygon(data=POUs, aes(x=long, y=lat, group=group, color=rgb(1,0,0)), 
               color=rgb(1,0,0), fill=rgb(1,0,0), alpha=1) 



############################################
### 2. Validate lat/lons (CA, PGE, POUs) ###
############################################

#Read PGE coordinates
setwd("S:/Matt/ag_pump/data/misc")
prems <- read.delim2("pge_prem_coord_raw.txt",header=TRUE,sep=",",stringsAsFactors=FALSE)
prems$longitude <- as.numeric(prems$prem_lon)
prems$latitude <- as.numeric(prems$prem_lat)
prems$czone <- as.numeric(substr(prems$climate_zone_cd,2,3))

## 2.1 In Calfornia?

#Convert to SpatialPointsDataFrame
coordinates(prems) <- ~ longitude + latitude
proj4string(prems) <- proj4string(pge)

#Test whether each lat/lon is in California
prems@data$IN_calif <- over(prems, CAoutline)
# prems$in_calif <- mapply(function(x,y) 
#   pnt.in.poly(cbind(x,y),
#               fortify(CAoutline)[c("long","lat")])$pip,
#   prems$longitude,
#   prems$latitude)
# mean(prems$in_calif)

#Convert back to regular dataframe
prems <- as.data.frame(prems)

#Assign in_calif dummy
prems$in_calif <- as.numeric(is.na(prems$IN_calif.NAME)==0)
summary(prems$in_calif)

#Plot points in CA
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_point(data=prems[prems$in_calif==1,], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Plot points NOT in CA
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_point(data=prems[prems$in_calif==0,], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Drop extraneous variables
prems <- prems[c("sp_uuid","prem_lat","prem_long","climate_zone_cd","longitude","latitude","czone","in_calif")]


## 2.2 In PGE?

#Convert to SpatialPointsDataFrame
coordinates(prems) <- ~ longitude + latitude
proj4string(prems) <- proj4string(pge)

#Test whether each lat/lon is in PGE Service Territory
prems@data$IN_pge <- over(prems, pge)
# prems$in_pge <- mapply(function(x,y) 
#   pnt.in.poly(cbind(x,y),
#               fortify(pge)[c("long","lat")])$pip,
#   prems$longitude,
#   prems$latitude)
# mean(prems$in_pge)

#Convert back to regular dataframe
prems <- as.data.frame(prems)

#Assign in_pge dummy
prems$in_pge <- as.numeric(is.na(prems$IN_pge.LABEL)==0)
summary(prems$in_pge)

#Plot points in PGE Service Territory
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=pge, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=prems[prems$in_pge==1,], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Plot points NOT in PGE Service Territory
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=pge, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=prems[prems$in_pge==0,], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Drop extraneous variables
prems <- prems[c("sp_uuid","prem_lat","prem_long","climate_zone_cd","longitude","latitude","czone","in_calif","in_pge")]


## 2.3 In a POUs enveloped by PGE?

#Convert to SpatialPointsDataFrame
coordinates(prems) <- ~ longitude + latitude
proj4string(prems) <- proj4string(pge)

#Test whether each lat/lon is in a PGE-enveloped POU
prems@data$IN_pou <- over(prems, POUs)

#Convert back to regular dataframe
prems <- as.data.frame(prems)

#Assign in_pou dummy and store POU name
prems$pou <- prems$IN_pou.LABEL
prems$in_pou <- as.numeric(is.na(prems$pou)==0)
summary(prems$in_pou)

#Plot points in POUs enveloped by PGE Service Territory
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=pge, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=prems[prems$in_pou==1,], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Plot points NOT in PGE Service Territory and NOT in a PGE-enveloped POU
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=pge, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=prems[(prems$in_pge==0 & prems$in_pou==0),], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Drop extraneous variables
prems <- prems[c("sp_uuid","prem_lat","prem_long","climate_zone_cd","longitude","latitude","czone","in_calif","in_pge","in_pou","pou")]


## 2.4 Deal with lat/lons that still haven't been classified

#Assign edge cases
prems$edge_sce <- as.numeric(prems$in_calif==1 & prems$in_pge==0 & prems$in_pou==0 & prems$longitude>(-120) & 
                             prems$longitude<(-118.5) & prems$latitude>35 & prems$latitude<37.5)
prems$edge_coast <- as.numeric(prems$in_calif==1 & prems$in_pge==0 & prems$in_pou==0 & prems$longitude>(-125) & 
                               prems$longitude<(-123) & prems$latitude>37.5 & prems$latitude<40)
prems$edge_mid <- as.numeric(prems$in_calif==1 & prems$in_pge==0 & prems$in_pou==0 & prems$longitude>(-121.5) & 
                               prems$longitude<(-120) & prems$latitude>37.5 & prems$latitude<38.1)
prems$edge_lodi <- as.numeric(prems$in_calif==1 & prems$in_pge==0 & prems$in_pou==0 & prems$longitude>(-121.5) & 
                               prems$longitude<(-120) & prems$latitude>38.1 & prems$latitude<38.5)

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
  geom_point(data=prems[(prems$in_calif==1 & prems$in_pge==0 & prems$in_pou==0 & prems$edge_sce==0 & prems$edge_coast==0 &
                         prems$edge_mid==0 & prems$edge_lodi==0),], aes(x=longitude, y=latitude), color=rgb(1,1,0), shape=19, 
             alpha=1, size=1) 

#Clean up and consolidate
prems$in_pge[prems$edge_coast==1] <- 1    # single edge case is really in PGE proper
prems$in_pou[prems$edge_lodi==1] <- 1     # Lodi Electric Utility
prems$pou[prems$edge_lodi==1] <- "Lodi Electric Utility"
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
### 3.  Climate ZOnes ###
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
filename <- "pge_prem_coord_polygon.csv"
write.csv(prems, file=filename , row.names=FALSE, col.names=TRUE, sep=",", quote=FALSE)


