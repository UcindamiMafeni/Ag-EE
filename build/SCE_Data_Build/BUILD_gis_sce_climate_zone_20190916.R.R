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

path <- "T:/Projects/Pump Data/"


##########################################
### 1. Prep all relevant in shapefiles ###
##########################################

setwd(paste0(path,"data/spatial"))

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

#Isolate SCE
sce <- IOUs[IOUs$LABEL=="Southern California Edison",]

#Plot SCE Service territory inside CA outline
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=sce, aes(x=long, y=lat, group=group, color=rgb(0,0,1)), 
               color=rgb(0,1,0), fill=NA, alpha=1) 

#Identify POUs inside PGE territory
POUs@data$IN_sce <- over(POUs,sce)
POUs@data$in_sce <- as.numeric(is.na(POUs@data$IN_sce$LABEL)==0)
POUs <- POUs[POUs$in_sce==1,]
POUs <- POUs[POUs$LABEL!="Los Angeles Department of Water & Power",]

#Plot POUs inside SCE Service territory inside CA outline
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=sce, aes(x=long, y=lat, group=group, color=rgb(0,0,1)), 
               color=rgb(0,1,0), fill=NA, alpha=1) +
  geom_polygon(data=POUs, aes(x=long, y=lat, group=group, color=rgb(1,0,0)), 
               color=rgb(1,0,0), fill=rgb(1,0,0), alpha=1) 



############################################
### 2. Validate lat/lons (CA, SCE, POUs) ###
############################################

#Read SCE coordinates
setwd(paste0(path,"data/misc"))
prems <- read.delim2("sce_prem_coord_raw_20190916.txt",header=TRUE,sep=",",stringsAsFactors=FALSE)
prems$longitude <- as.numeric(prems$longitude)
prems$latitude <- as.numeric(prems$latitude)
prems$czone <- as.numeric(substr(prems$climate_zone,2,3))

## 2.1 In Calfornia?

#Convert to SpatialPointsDataFrame
coordinates(prems) <- ~ longitude + latitude
proj4string(prems) <- proj4string(sce)

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
prems <- prems[c("sa_uuid","latitude","longitude","climate_zone","longitude","latitude","czone","in_calif")]


## 2.2 In SCE?

#Convert to SpatialPointsDataFrame
coordinates(prems) <- ~ longitude + latitude
proj4string(prems) <- proj4string(sce)

#Test whether each lat/lon is in SCE Service Territory
prems@data$IN_sce <- over(prems, sce)

#Convert back to regular dataframe
prems <- as.data.frame(prems)

#Assign in_sce dummy
prems$in_sce <- as.numeric(is.na(prems$IN_sce.LABEL)==0)
summary(prems$in_sce)

#Plot points in SCE Service Territory
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=sce, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=prems[prems$in_sce==1,], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Plot points NOT in SCE Service Territory
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=sce, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=prems[prems$in_sce==0,], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Drop extraneous variables
prems <- prems[c("sa_uuid","latitude","longitude","climate_zone","longitude","latitude","czone","in_calif","in_sce")]


## 2.3 In a POUs enveloped by SCE?

#Convert to SpatialPointsDataFrame
coordinates(prems) <- ~ longitude + latitude
proj4string(prems) <- proj4string(sce)

#Test whether each lat/lon is in a PGE-enveloped POU
prems@data$IN_pou <- over(prems, POUs)

#Convert back to regular dataframe
prems <- as.data.frame(prems)

#Assign in_pou dummy and store POU name
prems$pou <- prems$IN_pou.LABEL
prems$in_pou <- as.numeric(is.na(prems$pou)==0)
summary(prems$in_pou)

#Plot points in POUs enveloped by SCE Service Territory
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=sce, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=prems[prems$in_pou==1,], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Plot points NOT in SCE Service Territory and NOT in a SCE-enveloped POU
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=sce, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=prems[(prems$in_sce==0 & prems$in_pou==0),], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 

#Drop extraneous variables
prems <- prems[c("sa_uuid","latitude","longitude","climate_zone","longitude","latitude","czone","in_calif","in_sce","in_pou","pou")]


## 2.4 Deal with lat/lons that still haven't been classified

#Assign edge cases
prems$edge <- as.numeric(prems$in_calif==1 & prems$in_sce==0 & prems$in_pou==0 & prems$latitude<36.25)

#Plot edge cases
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=sce, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=prems[prems$edge_pge==1,], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1)  +
  geom_point(data=prems[(prems$in_calif==1 & prems$in_sce==0 & prems$in_pou==0 & prems$edge==0),], aes(x=longitude, y=latitude), color=rgb(1,1,0), shape=19, 
             alpha=1, size=1) 

#Clean up and consolidate
prems$in_sce[prems$edge==1] <- 1    # assigning edge cases for all but cluster near 36.3 N

prems <- prems[c("sa_uuid","latitude","longitude","climate_zone","longitude","latitude","czone","in_calif","in_sce","in_pou","pou")]

#Flag bad lat/lons: not in PGE service territory or in a POU
prems$bad_geocode <- as.numeric(prems$in_calif==0 | (prems$in_sce==0 & prems$in_pou==0))
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=sce, aes(x=long, y=lat, group=group), 
               color="green", fill=NA, alpha=1) +
  geom_point(data=prems[prems$bad_geocode==1,], aes(x=longitude, y=latitude), color=rgb(0,0,1), shape=19, 
             alpha=1, size=1) 



#########################
### 3.  Climate Zones ###
#########################

#Convert to SpatialPointsDataFrame
coordinates(prems) <- ~ longitude + latitude
proj4string(prems) <- proj4string(sce)

#Use Cliamte ZOne shapefile to assign 
prems@data$Zone <- over(prems, cz)

#Convert back to regular dataframe
prems <- as.data.frame(prems)

#Rename GIS assigned climate zone
names(prems)[13] <- "czone_gis"

#Export results to CSV
filename <- "sce_prem_coord_polygon_20190916.csv"
write.csv(prems, file=filename , row.names=FALSE, col.names=TRUE, sep=",", quote=FALSE, append=FALSE)