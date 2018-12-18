################################################
#  Script to estimate open access externality! #
################################################ 
rm(list = ls())

#Data downloaded from: https://data.cnra.ca.gov/dataset/ca-bulletin-118-groundwater-basins

# install.packages("ggmap")
# install.packages("ggplot2")
# install.packages("gstat")
# install.packages("sp")
# install.packages("sf")
# install.packages("maptools")
# install.packages("rgdal")
# install.packages("rgeos")
# install.packages("raster")
# install.packages("SDMTools")
# install.packages("tidyverse")

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


################################
### 1. Prep basin shapefiles ###
################################

setwd("S:/Matt/ag_pump/data/spatial")

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


##########################################################################
### 2. Import panel data of SPs locations, pump specs, and water p & q ###
##########################################################################

#Read PGE coordinates
setwd("S:/Matt/ag_pump/data/misc")
panel <- read.csv("panel_for_externality_calcs.csv",header=TRUE,sep=",",stringsAsFactors=FALSE)
panel$longitude <- as.numeric(panel$prem_lon)
panel$latitude <- as.numeric(panel$prem_lat)

#Convert to SpatialPointsDataFrame
coordinates(panel) <- ~ longitude + latitude
proj4string(panel) <- proj4string(wbasn)


############################################
### 3. Calculate open-access externality ###
############################################

# Step 1: transform data
wbasn_sf <- st_as_sf(wbasn)
wbasn_sf <- st_transform(wbasn_sf,3310)

panel_sf <- st_as_sf(panel)
panel_sf <- st_transform(panel_sf,3310)

panel_sf_june <- panel_sf[panel_sf$month==6,]
panel_sf_july <- panel_sf[panel_sf$month==7,]

# Step 2: establish distance radii
radii <- c(1,2,5,10,20) # miles
radii <- radii*1609.344  # convert to meters, the units of the SF objects

# Step 3: Draw circles around each unit
circles1 <- st_buffer(panel_sf_june, dist=radii[1])
circles2 <- st_buffer(panel_sf_june, dist=radii[2])
circles3 <- st_buffer(panel_sf_june, dist=radii[3])
circles4 <- st_buffer(panel_sf_june, dist=radii[4])
circles5 <- st_buffer(panel_sf_june, dist=radii[5])

# Step 4: Intersect circles with basin polygons (COME BACK TO THIS)
# wbasn_by_sp merge
# circles1 <- st_intersection(wbasn_by_sp, circles1)

# Step 5: Calculate area of each circle (sq meters)
areas1 <- st_area(circles1)
areas2 <- st_area(circles2)
areas3 <- st_area(circles3)
areas4 <- st_area(circles4)
areas5 <- st_area(circles5)

# Step 6: define groundwater variables and size of decrease in pumping for each unit i
stub      <- "rast_dd_mth_2SP"
P_var     <- paste0("mean_p_af_",stub)
Q_var     <- paste0("af_",stub)
kwhaf_var <- paste0("kwhaf_",stub)
swl_var   <- paste0("gw_",gsub("_dd_","_depth_",stub))
dd_var    <- ifelse(grepl("ddhat",stub),paste0("ddhat_",gsub("_dd_","_",stub)),"drwdwn_apep")

# Step 7: Calculate unit i's lost CS (for June) from pumping less
dCS_i          <- panel_sf_june[[P_var]]
dCS_i          <- as.data.frame(dCS_i)
names(dCS_i)   <- "P_old"
dCS_i$Q_old    <- panel_sf_june[[Q_var]]
dCS_i$delta_Q  <- ifelse(dCS_i$Q_old>1,1,NaN) #start with 1 AF, and see what happens\
dCS_i$eps      <- panel_sf_june$elast_water
dCS_i$Q_new    <- dCS_i$Q_old - dCS_i$delta_Q
dCS_i$P_new    <- (dCS_i$Q_new/dCS_i$Q_old)^(1/(dCS_i$eps))*dCS_i$P_old
dCS_i$integral <- (dCS_i$Q_old/(dCS_i$eps+1))*((dCS_i$P_old^(-dCS_i$eps))*(dCS_i$P_new^(dCS_i$eps+1))-dCS_i$P_old)
#dCS_i$integral2<- (dCS_i$Q_new * dCS_i$P_new - dCS_i$Q_old * dCS_i$P_old)/(dCS_i$eps+1)
dCS_i$rectangle<- dCS_i$Q_new * (dCS_i$P_new - dCS_i$P_old)
dCS_i$dCS_i    <- -(dCS_i$integral - dCS_i$rectangle)

# Step 8: Calculate how much the water level rises in each circle when unit i pumps less
delta_swl1 <- dCS_i$delta_Q/(areas1/4046.856) #convert acres from sq meters to acres
delta_swl2 <- dCS_i$delta_Q/(areas2/4046.856) #convert acres from sq meters to acres
delta_swl3 <- dCS_i$delta_Q/(areas3/4046.856) #convert acres from sq meters to acres
delta_swl4 <- dCS_i$delta_Q/(areas4/4046.856) #convert acres from sq meters to acres
delta_swl5 <- dCS_i$delta_Q/(areas5/4046.856) #convert acres from sq meters to acres

# Step 9: Flag July SPs in each June circle, making separate data frame with J units
#in polygon function to subset SPs

# Step 10: Impose a uniform delta SWL, and calculate effect on P_water for all J units


# Step 11: Calculate increase in CS (for July) for all J units

# Step 12: Sum all changes in CS_j

# Step 13: Store changes in CS_j, CS_i, and number of J units in circle




i <- 197









#Assign each lat/lon to the water basin polygon it's contained in
panel@data$IN_wbasn <- over(prems, wbasn)

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
setwd("S:/Matt/ag_pump/data/misc")
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


