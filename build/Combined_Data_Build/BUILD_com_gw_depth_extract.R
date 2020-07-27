#######################################################################
# Script to predict groundwater depth from PGE SP, APEP pumps, SCE SP #
#    by extracting values from monthly/quarterly rasters              #
#######################################################################

library(ggmap)
library(ggplot2)
library(gstat)
library(sp)
library(maptools)
library(rgdal)
library(raster)
library(dplyr)
library(sf)

#######################################################################################################
### 1. Extract groundwater depths for each PGE SP and APEP pump, from each monthly/quarterly raster ###
#######################################################################################################

rm(list = ls())
path <- "T:/Projects/Pump Data/"
setwd(paste0(path,"data/misc"))

#Load monthly/quarterly rasters
load("gw_idw_rasters.RData")

#Read PGE coordinates
prems <- read.delim2("pge_prem_coord_3pulls.txt",header=TRUE,sep=",",stringsAsFactors=FALSE)
prems$x <- as.numeric(prems$prem_lon)
prems$y <- as.numeric(prems$prem_lat)

#Read APEP coordinates
pumps <- read.delim2("apep_pump_coord.txt",header=TRUE,sep=",",stringsAsFactors=FALSE)
pumps$x <- as.numeric(pumps$pump_lon)
pumps$y <- as.numeric(pumps$pump_lat)

#Create SpatialPointsDataFrame analogs
prems_2 <- prems
coordinates(prems_2) <- ~ x + y
proj4string(prems_2) <- CRS("+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
pumps_2 <- pumps
coordinates(pumps_2) <- ~ x + y
proj4string(pumps_2) <- CRS("+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")

#Loop over months to extract monthly groundwater depths
for (ym in levels(gwmth$modate)) {

  #Store ym's monthly rasters
  temp_ra1 <- get(paste0("gwmth_rast_1_",ym))
  temp_ra2 <- get(paste0("gwmth_rast_2_",ym))
  temp_ra3 <- get(paste0("gwmth_rast_3_",ym))
  
  #Create names for extracted values from rasters
  name_1s <- paste0("depth_1s_",ym)
  name_1b <- paste0("depth_1b_",ym)
  name_2s <- paste0("depth_2s_",ym)
  name_2b <- paste0("depth_2b_",ym)
  name_3s <- paste0("depth_3s_",ym)
  name_3b <- paste0("depth_3b_",ym)
  
  #Extract groundwater dephts from rasters (SP lat/lons)
  prems[[name_1s]] <- extract(temp_ra1,prems_2,method='simple'  ,df=TRUE)$layer
  prems[[name_1b]] <- extract(temp_ra1,prems_2,method='bilinear',df=TRUE)$layer
  prems[[name_2s]] <- extract(temp_ra2,prems_2,method='simple'  ,df=TRUE)$layer
  prems[[name_2b]] <- extract(temp_ra2,prems_2,method='bilinear',df=TRUE)$layer
  prems[[name_3s]] <- extract(temp_ra3,prems_2,method='simple'  ,df=TRUE)$layer
  prems[[name_3b]] <- extract(temp_ra3,prems_2,method='bilinear',df=TRUE)$layer

  #Extract groundwater dephts from rasters (pump lat/lons)
  pumps[[name_1s]] <- extract(temp_ra1,pumps_2,method='simple'  ,df=TRUE)$layer
  pumps[[name_1b]] <- extract(temp_ra1,pumps_2,method='bilinear',df=TRUE)$layer
  pumps[[name_2s]] <- extract(temp_ra2,pumps_2,method='simple'  ,df=TRUE)$layer
  pumps[[name_2b]] <- extract(temp_ra2,pumps_2,method='bilinear',df=TRUE)$layer
  pumps[[name_3s]] <- extract(temp_ra3,pumps_2,method='simple'  ,df=TRUE)$layer
  pumps[[name_3b]] <- extract(temp_ra3,pumps_2,method='bilinear',df=TRUE)$layer
  
  #Store ym's monthly data frames
  temp_df1 <- get(paste0("gwmth_1_",ym))
  temp_df2 <- get(paste0("gwmth_2_",ym))
  temp_df3 <- get(paste0("gwmth_3_",ym))
  
  #Reproject spatial data frames to put units in meters
  temp_df1 <- spTransform(temp_df1,"+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
  temp_df2 <- spTransform(temp_df2,"+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
  temp_df3 <- spTransform(temp_df3,"+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
  
  #Create names for distances to nearest groundwater reading
  name_1d <- paste0("distkm_1_",ym)
  name_2d <- paste0("distkm_2_",ym)
  name_3d <- paste0("distkm_3_",ym)
  
  #Calculate distance to nearest groundwater reading (SP lat/lons)
  prems[[name_1d]] <- do.call(pmin, as.data.frame(pointDistance(prems_2,temp_df1,lonlat=TRUE)))/1000
  prems[[name_2d]] <- do.call(pmin, as.data.frame(pointDistance(prems_2,temp_df2,lonlat=TRUE)))/1000
  prems[[name_3d]] <- do.call(pmin, as.data.frame(pointDistance(prems_2,temp_df3,lonlat=TRUE)))/1000
  
  #Calculate distance to nearest groundwater reading (pump lat/lons)
  pumps[[name_1d]] <- do.call(pmin, as.data.frame(pointDistance(pumps_2,temp_df1,lonlat=TRUE)))/1000
  pumps[[name_2d]] <- do.call(pmin, as.data.frame(pointDistance(pumps_2,temp_df2,lonlat=TRUE)))/1000
  pumps[[name_3d]] <- do.call(pmin, as.data.frame(pointDistance(pumps_2,temp_df3,lonlat=TRUE)))/1000
  
  #Intermediate output
  print(paste(ym,"  ",Sys.time()))
  
  #Remove ym's monthly rasters and data frames that we no longer need, to save memory
  rm(list=c(paste0("gwmth_rast_1_",ym)))
  rm(list=c(paste0("gwmth_rast_2_",ym)))
  rm(list=c(paste0("gwmth_rast_3_",ym)))
  rm(list=c(paste0("gwmth_1_",ym)))
  rm(list=c(paste0("gwmth_2_",ym)))
  rm(list=c(paste0("gwmth_3_",ym)))
  
}  

#Export monthly SP and pump results to CSV
filename <- paste0("prems_gw_depths_from_rasters_mth.csv")
write.csv(prems, file=filename , row.names=FALSE, quote=FALSE)
filename <- paste0("pumps_gw_depths_from_rasters_mth.csv")
write.csv(pumps, file=filename , row.names=FALSE, quote=FALSE)

#Remove monthly columns from output dataframes
prems <- prems %>% select(c(sp_uuid,prem_lat,prem_long,bad_geocode_flag,missing_geocode_flag,pull,x,y))
pumps <- pumps %>% select(c(pump_lat,pump_long,latlon_group,x,y))

#Loop over quarters to extract quarterly groundwater depths
for (yq in levels(gwqtr$qtr)) {
  
  #Store yq's quarterly rasters
  temp_ra1 <- get(paste0("gwqtr_rast_1_",yq))
  temp_ra2 <- get(paste0("gwqtr_rast_2_",yq))
  temp_ra3 <- get(paste0("gwqtr_rast_3_",yq))
  
  #Create names for extracted values from rasters
  name_1s <- paste0("depth_1s_",yq)
  name_1b <- paste0("depth_1b_",yq)
  name_2s <- paste0("depth_2s_",yq)
  name_2b <- paste0("depth_2b_",yq)
  name_3s <- paste0("depth_3s_",yq)
  name_3b <- paste0("depth_3b_",yq)
  
  #Extract groundwater dephts from rasters (SP lat/lons)
  prems[[name_1s]] <- extract(temp_ra1,prems_2,method='simple'  ,df=TRUE)$layer
  prems[[name_1b]] <- extract(temp_ra1,prems_2,method='bilinear',df=TRUE)$layer
  prems[[name_2s]] <- extract(temp_ra2,prems_2,method='simple'  ,df=TRUE)$layer
  prems[[name_2b]] <- extract(temp_ra2,prems_2,method='bilinear',df=TRUE)$layer
  prems[[name_3s]] <- extract(temp_ra3,prems_2,method='simple'  ,df=TRUE)$layer
  prems[[name_3b]] <- extract(temp_ra3,prems_2,method='bilinear',df=TRUE)$layer
  
  #Extract groundwater dephts from rasters (pump lat/lons)
  pumps[[name_1s]] <- extract(temp_ra1,pumps_2,method='simple'  ,df=TRUE)$layer
  pumps[[name_1b]] <- extract(temp_ra1,pumps_2,method='bilinear',df=TRUE)$layer
  pumps[[name_2s]] <- extract(temp_ra2,pumps_2,method='simple'  ,df=TRUE)$layer
  pumps[[name_2b]] <- extract(temp_ra2,pumps_2,method='bilinear',df=TRUE)$layer
  pumps[[name_3s]] <- extract(temp_ra3,pumps_2,method='simple'  ,df=TRUE)$layer
  pumps[[name_3b]] <- extract(temp_ra3,pumps_2,method='bilinear',df=TRUE)$layer
  
  #Store yq's quarterly data frames
  temp_df1 <- get(paste0("gwqtr_1_",yq))
  temp_df2 <- get(paste0("gwqtr_2_",yq))
  temp_df3 <- get(paste0("gwqtr_3_",yq))
  
  #Reproject spatial data frames to put units in meters
  temp_df1 <- spTransform(temp_df1,"+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
  temp_df2 <- spTransform(temp_df2,"+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
  temp_df3 <- spTransform(temp_df3,"+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
  
  #Create names for distances to nearest groundwater reading
  name_1d <- paste0("distkm_1_",yq)
  name_2d <- paste0("distkm_2_",yq)
  name_3d <- paste0("distkm_3_",yq)
  
  #Calculate distance to nearest groundwater reading (SP lat/lons)
  prems[[name_1d]] <- do.call(pmin, as.data.frame(pointDistance(prems_2,temp_df1,lonlat=TRUE)))/1000
  prems[[name_2d]] <- do.call(pmin, as.data.frame(pointDistance(prems_2,temp_df2,lonlat=TRUE)))/1000
  prems[[name_3d]] <- do.call(pmin, as.data.frame(pointDistance(prems_2,temp_df3,lonlat=TRUE)))/1000
  
  #Calculate distance to nearest groundwater reading (pump lat/lons)
  pumps[[name_1d]] <- do.call(pmin, as.data.frame(pointDistance(pumps_2,temp_df1,lonlat=TRUE)))/1000
  pumps[[name_2d]] <- do.call(pmin, as.data.frame(pointDistance(pumps_2,temp_df2,lonlat=TRUE)))/1000
  pumps[[name_3d]] <- do.call(pmin, as.data.frame(pointDistance(pumps_2,temp_df3,lonlat=TRUE)))/1000
  
  #Intermediate output
  print(paste(yq,"  ",Sys.time()))
  
  #Remove yq's quarterly rasters and data drames that we no longer need, to save memory
  rm(list=c(paste0("gwqtr_rast_1_",yq)))
  rm(list=c(paste0("gwqtr_rast_2_",yq)))
  rm(list=c(paste0("gwqtr_rast_3_",yq)))
  rm(list=c(paste0("gwqtr_1_",yq)))
  rm(list=c(paste0("gwqtr_2_",yq)))
  rm(list=c(paste0("gwqtr_3_",yq)))
  
}  

#Export quarterly SP and pump results to CSV
filename <- paste0("prems_gw_depths_from_rasters_qtr.csv")
write.csv(prems, file=filename , row.names=FALSE, quote=FALSE)
filename <- paste0("pumps_gw_depths_from_rasters_qtr.csv")
write.csv(pumps, file=filename , row.names=FALSE, quote=FALSE)



#############################################################################################################################
### 2. Extract groundwater depths for each PGE SP and APEP pumps, from each monthly/quarterly raster (SAN JOAQUIN VALLEY) ###
#############################################################################################################################


rm(list = ls())
path <- "T:/Projects/Pump Data/"
setwd(paste0(path,"data/misc"))

#Load monthly/quarterly rasters
load("gw_idw_rasters_SJ.RData")

#Read PGE coordinates
prems <- read.delim2("pge_prem_coord_3pulls.txt",header=TRUE,sep=",",stringsAsFactors=FALSE)
prems$x <- as.numeric(prems$prem_lon)
prems$y <- as.numeric(prems$prem_lat)

#Read APEP coordinates
pumps <- read.delim2("apep_pump_coord.txt",header=TRUE,sep=",",stringsAsFactors=FALSE)
pumps$x <- as.numeric(pumps$pump_lon)
pumps$y <- as.numeric(pumps$pump_lat)

#Create SpatialPointsDataFrame analogs
prems_2 <- prems
coordinates(prems_2) <- ~ x + y
proj4string(prems_2) <- CRS("+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
pumps_2 <- pumps
coordinates(pumps_2) <- ~ x + y
proj4string(pumps_2) <- CRS("+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")

#Loop over months to extract monthly groundwater depths
for (ym in levels(gwmth$modate)) {
  
  #Store ym's monthly rasters
  temp_ra1 <- get(paste0("gwmth_sj_rast_1_",ym))
  temp_ra2 <- get(paste0("gwmth_sj_rast_2_",ym))
  temp_ra3 <- get(paste0("gwmth_sj_rast_3_",ym))
  
  #Create names for extracted values from rasters
  name_1s <- paste0("depth_1s_",ym)
  name_1b <- paste0("depth_1b_",ym)
  name_2s <- paste0("depth_2s_",ym)
  name_2b <- paste0("depth_2b_",ym)
  name_3s <- paste0("depth_3s_",ym)
  name_3b <- paste0("depth_3b_",ym)
  
  #Extract groundwater dephts from rasters (SP lat/lons)
  prems[[name_1s]] <- extract(temp_ra1,prems_2,method='simple'  ,df=TRUE)$layer
  prems[[name_1b]] <- extract(temp_ra1,prems_2,method='bilinear',df=TRUE)$layer
  prems[[name_2s]] <- extract(temp_ra2,prems_2,method='simple'  ,df=TRUE)$layer
  prems[[name_2b]] <- extract(temp_ra2,prems_2,method='bilinear',df=TRUE)$layer
  prems[[name_3s]] <- extract(temp_ra3,prems_2,method='simple'  ,df=TRUE)$layer
  prems[[name_3b]] <- extract(temp_ra3,prems_2,method='bilinear',df=TRUE)$layer
  
  #Extract groundwater dephts from rasters (pump lat/lons)
  pumps[[name_1s]] <- extract(temp_ra1,pumps_2,method='simple'  ,df=TRUE)$layer
  pumps[[name_1b]] <- extract(temp_ra1,pumps_2,method='bilinear',df=TRUE)$layer
  pumps[[name_2s]] <- extract(temp_ra2,pumps_2,method='simple'  ,df=TRUE)$layer
  pumps[[name_2b]] <- extract(temp_ra2,pumps_2,method='bilinear',df=TRUE)$layer
  pumps[[name_3s]] <- extract(temp_ra3,pumps_2,method='simple'  ,df=TRUE)$layer
  pumps[[name_3b]] <- extract(temp_ra3,pumps_2,method='bilinear',df=TRUE)$layer
  
  #Store ym's monthly data frames
  temp_df1 <- get(paste0("gwmth_sj_1_",ym))
  temp_df2 <- get(paste0("gwmth_sj_2_",ym))
  temp_df3 <- get(paste0("gwmth_sj_3_",ym))
  
  #Reproject spatial data frames to put units in meters
  temp_df1 <- spTransform(temp_df1,"+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
  temp_df2 <- spTransform(temp_df2,"+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
  temp_df3 <- spTransform(temp_df3,"+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
  
  #Create names for distances to nearest groundwater reading
  name_1d <- paste0("distkm_sj_1_",ym)
  name_2d <- paste0("distkm_sj_2_",ym)
  name_3d <- paste0("distkm_sj_3_",ym)
  
  #Calculate distance to nearest groundwater reading (SP lat/lons)
  prems[[name_1d]] <- do.call(pmin, as.data.frame(pointDistance(prems_2,temp_df1,lonlat=TRUE)))/1000
  prems[[name_2d]] <- do.call(pmin, as.data.frame(pointDistance(prems_2,temp_df2,lonlat=TRUE)))/1000
  prems[[name_3d]] <- do.call(pmin, as.data.frame(pointDistance(prems_2,temp_df3,lonlat=TRUE)))/1000
  
  #Calculate distance to nearest groundwater reading (pump lat/lons)
  pumps[[name_1d]] <- do.call(pmin, as.data.frame(pointDistance(pumps_2,temp_df1,lonlat=TRUE)))/1000
  pumps[[name_2d]] <- do.call(pmin, as.data.frame(pointDistance(pumps_2,temp_df2,lonlat=TRUE)))/1000
  pumps[[name_3d]] <- do.call(pmin, as.data.frame(pointDistance(pumps_2,temp_df3,lonlat=TRUE)))/1000
  
  #Intermediate output
  print(paste(ym,"  ",Sys.time()))
  
  #Remove ym's monthly rasters and data frames that we no longer need, to save memory
  rm(list=c(paste0("gwmth_sj_rast_1_",ym)))
  rm(list=c(paste0("gwmth_sj_rast_2_",ym)))
  rm(list=c(paste0("gwmth_sj_rast_3_",ym)))
  rm(list=c(paste0("gwmth_sj_1_",ym)))
  rm(list=c(paste0("gwmth_sj_2_",ym)))
  rm(list=c(paste0("gwmth_sj_3_",ym)))
  
}  

#Export monthly SP and pump results to CSV
filename <- paste0("prems_gw_depths_from_rasters_mth_SJ.csv")
write.csv(prems, file=filename , row.names=FALSE, quote=FALSE)
filename <- paste0("pumps_gw_depths_from_rasters_mth_SJ.csv")
write.csv(pumps, file=filename , row.names=FALSE, quote=FALSE)

#Remove monthly columns from output dataframes
prems <- prems %>% select(c(sp_uuid,prem_lat,prem_long,bad_geocode_flag,missing_geocode_flag,pull,x,y))
pumps <- pumps %>% select(c(pump_lat,pump_long,latlon_group,x,y))

#Loop over quarters to extract quarterly groundwater depths
for (yq in levels(gwqtr$qtr)) {
  
  #Store yq's quarterly rasters
  temp_ra1 <- get(paste0("gwqtr_sj_rast_1_",yq))
  temp_ra2 <- get(paste0("gwqtr_sj_rast_2_",yq))
  temp_ra3 <- get(paste0("gwqtr_sj_rast_3_",yq))
  
  #Create names for extracted values from rasters
  name_1s <- paste0("depth_1s_",yq)
  name_1b <- paste0("depth_1b_",yq)
  name_2s <- paste0("depth_2s_",yq)
  name_2b <- paste0("depth_2b_",yq)
  name_3s <- paste0("depth_3s_",yq)
  name_3b <- paste0("depth_3b_",yq)
  
  #Extract groundwater dephts from rasters (SP lat/lons)
  prems[[name_1s]] <- extract(temp_ra1,prems_2,method='simple'  ,df=TRUE)$layer
  prems[[name_1b]] <- extract(temp_ra1,prems_2,method='bilinear',df=TRUE)$layer
  prems[[name_2s]] <- extract(temp_ra2,prems_2,method='simple'  ,df=TRUE)$layer
  prems[[name_2b]] <- extract(temp_ra2,prems_2,method='bilinear',df=TRUE)$layer
  prems[[name_3s]] <- extract(temp_ra3,prems_2,method='simple'  ,df=TRUE)$layer
  prems[[name_3b]] <- extract(temp_ra3,prems_2,method='bilinear',df=TRUE)$layer
  
  #Extract groundwater dephts from rasters (pump lat/lons)
  pumps[[name_1s]] <- extract(temp_ra1,pumps_2,method='simple'  ,df=TRUE)$layer
  pumps[[name_1b]] <- extract(temp_ra1,pumps_2,method='bilinear',df=TRUE)$layer
  pumps[[name_2s]] <- extract(temp_ra2,pumps_2,method='simple'  ,df=TRUE)$layer
  pumps[[name_2b]] <- extract(temp_ra2,pumps_2,method='bilinear',df=TRUE)$layer
  pumps[[name_3s]] <- extract(temp_ra3,pumps_2,method='simple'  ,df=TRUE)$layer
  pumps[[name_3b]] <- extract(temp_ra3,pumps_2,method='bilinear',df=TRUE)$layer
  
  #Store yq's quarterly data frames
  temp_df1 <- get(paste0("gwqtr_sj_1_",yq))
  temp_df2 <- get(paste0("gwqtr_sj_2_",yq))
  temp_df3 <- get(paste0("gwqtr_sj_3_",yq))
  
  #Reproject spatial data frames to put units in meters
  temp_df1 <- spTransform(temp_df1,"+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
  temp_df2 <- spTransform(temp_df2,"+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
  temp_df3 <- spTransform(temp_df3,"+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
  
  #Create names for distances to nearest groundwater reading
  name_1d <- paste0("distkm_sj_1_",yq)
  name_2d <- paste0("distkm_sj_2_",yq)
  name_3d <- paste0("distkm_sj_3_",yq)
  
  #Calculate distance to nearest groundwater reading (SP lat/lons)
  prems[[name_1d]] <- do.call(pmin, as.data.frame(pointDistance(prems_2,temp_df1,lonlat=TRUE)))/1000
  prems[[name_2d]] <- do.call(pmin, as.data.frame(pointDistance(prems_2,temp_df2,lonlat=TRUE)))/1000
  prems[[name_3d]] <- do.call(pmin, as.data.frame(pointDistance(prems_2,temp_df3,lonlat=TRUE)))/1000
  
  #Calculate distance to nearest groundwater reading (pump lat/lons)
  pumps[[name_1d]] <- do.call(pmin, as.data.frame(pointDistance(pumps_2,temp_df1,lonlat=TRUE)))/1000
  pumps[[name_2d]] <- do.call(pmin, as.data.frame(pointDistance(pumps_2,temp_df2,lonlat=TRUE)))/1000
  pumps[[name_3d]] <- do.call(pmin, as.data.frame(pointDistance(pumps_2,temp_df3,lonlat=TRUE)))/1000
  
  #Intermediate output
  print(paste(yq,"  ",Sys.time()))
  
  #Remove yq's quarterly rasters and data frames that we no longer need, to save memory
  rm(list=c(paste0("gwqtr_sj_rast_1_",yq)))
  rm(list=c(paste0("gwqtr_sj_rast_2_",yq)))
  rm(list=c(paste0("gwqtr_sj_rast_3_",yq)))
  rm(list=c(paste0("gwqtr_sj_1_",yq)))
  rm(list=c(paste0("gwqtr_sj_2_",yq)))
  rm(list=c(paste0("gwqtr_sj_3_",yq)))
  
}  

#Export quarterly SP and pump results to CSV
filename <- paste0("prems_gw_depths_from_rasters_qtr_SJ.csv")
write.csv(prems, file=filename , row.names=FALSE, quote=FALSE)
filename <- paste0("pumps_gw_depths_from_rasters_qtr_SJ.csv")
write.csv(pumps, file=filename , row.names=FALSE, quote=FALSE)



###########################################################################################################################
### 3. Extract groundwater depths for each PGE SP and APEP pump, from each monthly/quarterly raster (SACRAMENTO VALLEY) ###
###########################################################################################################################

rm(list = ls())
path <- "T:/Projects/Pump Data/"
setwd(paste0(path,"data/misc"))

#Load monthly/quarterly rasters
load("gw_idw_rasters_SAC.RData")

#Read PGE coordinates
prems <- read.delim2("pge_prem_coord_3pulls.txt",header=TRUE,sep=",",stringsAsFactors=FALSE)
prems$x <- as.numeric(prems$prem_lon)
prems$y <- as.numeric(prems$prem_lat)

#Read APEP coordinates
pumps <- read.delim2("apep_pump_coord.txt",header=TRUE,sep=",",stringsAsFactors=FALSE)
pumps$x <- as.numeric(pumps$pump_lon)
pumps$y <- as.numeric(pumps$pump_lat)

#Create SpatialPointsDataFrame analogs
prems_2 <- prems
coordinates(prems_2) <- ~ x + y
proj4string(prems_2) <- CRS("+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
pumps_2 <- pumps
coordinates(pumps_2) <- ~ x + y
proj4string(pumps_2) <- CRS("+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")

#Loop over months to extract monthly groundwater depths
for (ym in levels(gwmth$modate)) {
  
  #Store ym's monthly rasters
  temp_ra1 <- get(paste0("gwmth_sac_rast_1_",ym))
  temp_ra2 <- get(paste0("gwmth_sac_rast_2_",ym))
  temp_ra3 <- get(paste0("gwmth_sac_rast_3_",ym))
  
  #Create names for extracted values from rasters
  name_1s <- paste0("depth_1s_",ym)
  name_1b <- paste0("depth_1b_",ym)
  name_2s <- paste0("depth_2s_",ym)
  name_2b <- paste0("depth_2b_",ym)
  name_3s <- paste0("depth_3s_",ym)
  name_3b <- paste0("depth_3b_",ym)
  
  #Extract groundwater dephts from rasters (SP lat/lons)
  prems[[name_1s]] <- extract(temp_ra1,prems_2,method='simple'  ,df=TRUE)$layer
  prems[[name_1b]] <- extract(temp_ra1,prems_2,method='bilinear',df=TRUE)$layer
  prems[[name_2s]] <- extract(temp_ra2,prems_2,method='simple'  ,df=TRUE)$layer
  prems[[name_2b]] <- extract(temp_ra2,prems_2,method='bilinear',df=TRUE)$layer
  prems[[name_3s]] <- extract(temp_ra3,prems_2,method='simple'  ,df=TRUE)$layer
  prems[[name_3b]] <- extract(temp_ra3,prems_2,method='bilinear',df=TRUE)$layer
  
  #Extract groundwater dephts from rasters (pump lat/lons)
  pumps[[name_1s]] <- extract(temp_ra1,pumps_2,method='simple'  ,df=TRUE)$layer
  pumps[[name_1b]] <- extract(temp_ra1,pumps_2,method='bilinear',df=TRUE)$layer
  pumps[[name_2s]] <- extract(temp_ra2,pumps_2,method='simple'  ,df=TRUE)$layer
  pumps[[name_2b]] <- extract(temp_ra2,pumps_2,method='bilinear',df=TRUE)$layer
  pumps[[name_3s]] <- extract(temp_ra3,pumps_2,method='simple'  ,df=TRUE)$layer
  pumps[[name_3b]] <- extract(temp_ra3,pumps_2,method='bilinear',df=TRUE)$layer
  
  #Store ym's monthly data frames
  temp_df1 <- get(paste0("gwmth_sac_1_",ym))
  temp_df2 <- get(paste0("gwmth_sac_2_",ym))
  temp_df3 <- get(paste0("gwmth_sac_3_",ym))
  
  #Reproject spatial data frames to put units in meters
  temp_df1 <- spTransform(temp_df1,"+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
  temp_df2 <- spTransform(temp_df2,"+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
  temp_df3 <- spTransform(temp_df3,"+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
  
  #Create names for distances to nearest groundwater reading
  name_1d <- paste0("distkm_sac_1_",ym)
  name_2d <- paste0("distkm_sac_2_",ym)
  name_3d <- paste0("distkm_sac_3_",ym)
  
  #Calculate distance to nearest groundwater reading (SP lat/lons)
  prems[[name_1d]] <- do.call(pmin, as.data.frame(pointDistance(prems_2,temp_df1,lonlat=TRUE)))/1000
  prems[[name_2d]] <- do.call(pmin, as.data.frame(pointDistance(prems_2,temp_df2,lonlat=TRUE)))/1000
  prems[[name_3d]] <- do.call(pmin, as.data.frame(pointDistance(prems_2,temp_df3,lonlat=TRUE)))/1000
  
  #Calculate distance to nearest groundwater reading (pump lat/lons)
  pumps[[name_1d]] <- do.call(pmin, as.data.frame(pointDistance(pumps_2,temp_df1,lonlat=TRUE)))/1000
  pumps[[name_2d]] <- do.call(pmin, as.data.frame(pointDistance(pumps_2,temp_df2,lonlat=TRUE)))/1000
  pumps[[name_3d]] <- do.call(pmin, as.data.frame(pointDistance(pumps_2,temp_df3,lonlat=TRUE)))/1000
  
  #Intermediate output
  print(paste(ym,"  ",Sys.time()))
  
  #Remove ym's monthly rasters and data frames that we no longer need, to save memory
  rm(list=c(paste0("gwmth_sac_rast_1_",ym)))
  rm(list=c(paste0("gwmth_sac_rast_2_",ym)))
  rm(list=c(paste0("gwmth_sac_rast_3_",ym)))
  rm(list=c(paste0("gwmth_sac_1_",ym)))
  rm(list=c(paste0("gwmth_sac_2_",ym)))
  rm(list=c(paste0("gwmth_sac_3_",ym)))
  
}  

#Export monthly SP and pump results to CSV
filename <- paste0("prems_gw_depths_from_rasters_mth_SAC.csv")
write.csv(prems, file=filename , row.names=FALSE, quote=FALSE)
filename <- paste0("pumps_gw_depths_from_rasters_mth_SAC.csv")
write.csv(pumps, file=filename , row.names=FALSE, quote=FALSE)

#Remove monthly columns from output dataframes
prems <- prems %>% select(c(sp_uuid,prem_lat,prem_long,bad_geocode_flag,missing_geocode_flag,pull,x,y))
pumps <- pumps %>% select(c(pump_lat,pump_long,latlon_group,x,y))

#Loop over quarters to extract quarterly groundwater depths
for (yq in levels(gwqtr$qtr)) {
  
  #Store yq's quarterly rasters
  temp_ra1 <- get(paste0("gwqtr_sac_rast_1_",yq))
  temp_ra2 <- get(paste0("gwqtr_sac_rast_2_",yq))
  temp_ra3 <- get(paste0("gwqtr_sac_rast_3_",yq))
  
  #Create names for extracted values from rasters
  name_1s <- paste0("depth_1s_",yq)
  name_1b <- paste0("depth_1b_",yq)
  name_2s <- paste0("depth_2s_",yq)
  name_2b <- paste0("depth_2b_",yq)
  name_3s <- paste0("depth_3s_",yq)
  name_3b <- paste0("depth_3b_",yq)
  
  #Extract groundwater dephts from rasters (SP lat/lons)
  prems[[name_1s]] <- extract(temp_ra1,prems_2,method='simple'  ,df=TRUE)$layer
  prems[[name_1b]] <- extract(temp_ra1,prems_2,method='bilinear',df=TRUE)$layer
  prems[[name_2s]] <- extract(temp_ra2,prems_2,method='simple'  ,df=TRUE)$layer
  prems[[name_2b]] <- extract(temp_ra2,prems_2,method='bilinear',df=TRUE)$layer
  prems[[name_3s]] <- extract(temp_ra3,prems_2,method='simple'  ,df=TRUE)$layer
  prems[[name_3b]] <- extract(temp_ra3,prems_2,method='bilinear',df=TRUE)$layer
  
  #Extract groundwater dephts from rasters (pump lat/lons)
  pumps[[name_1s]] <- extract(temp_ra1,pumps_2,method='simple'  ,df=TRUE)$layer
  pumps[[name_1b]] <- extract(temp_ra1,pumps_2,method='bilinear',df=TRUE)$layer
  pumps[[name_2s]] <- extract(temp_ra2,pumps_2,method='simple'  ,df=TRUE)$layer
  pumps[[name_2b]] <- extract(temp_ra2,pumps_2,method='bilinear',df=TRUE)$layer
  pumps[[name_3s]] <- extract(temp_ra3,pumps_2,method='simple'  ,df=TRUE)$layer
  pumps[[name_3b]] <- extract(temp_ra3,pumps_2,method='bilinear',df=TRUE)$layer
  
  #Store yq's quarterly data frames
  temp_df1 <- get(paste0("gwqtr_sac_1_",yq))
  temp_df2 <- get(paste0("gwqtr_sac_2_",yq))
  temp_df3 <- get(paste0("gwqtr_sac_3_",yq))
  
  #Reproject spatial data frames to put units in meters
  temp_df1 <- spTransform(temp_df1,"+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
  temp_df2 <- spTransform(temp_df2,"+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
  temp_df3 <- spTransform(temp_df3,"+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
  
  #Create names for distances to nearest groundwater reading
  name_1d <- paste0("distkm_sac_1_",yq)
  name_2d <- paste0("distkm_sac_2_",yq)
  name_3d <- paste0("distkm_sac_3_",yq)
  
  #Calculate distance to nearest groundwater reading (SP lat/lons)
  prems[[name_1d]] <- do.call(pmin, as.data.frame(pointDistance(prems_2,temp_df1,lonlat=TRUE)))/1000
  prems[[name_2d]] <- do.call(pmin, as.data.frame(pointDistance(prems_2,temp_df2,lonlat=TRUE)))/1000
  prems[[name_3d]] <- do.call(pmin, as.data.frame(pointDistance(prems_2,temp_df3,lonlat=TRUE)))/1000
  
  #Calculate distance to nearest groundwater reading (pump lat/lons)
  pumps[[name_1d]] <- do.call(pmin, as.data.frame(pointDistance(pumps_2,temp_df1,lonlat=TRUE)))/1000
  pumps[[name_2d]] <- do.call(pmin, as.data.frame(pointDistance(pumps_2,temp_df2,lonlat=TRUE)))/1000
  pumps[[name_3d]] <- do.call(pmin, as.data.frame(pointDistance(pumps_2,temp_df3,lonlat=TRUE)))/1000
  
  #Intermediate output
  print(paste(yq,"  ",Sys.time()))
  
  #Remove yq's quarterly rasters and data frames that we no longer need, to save memory
  rm(list=c(paste0("gwqtr_sac_rast_1_",yq)))
  rm(list=c(paste0("gwqtr_sac_rast_2_",yq)))
  rm(list=c(paste0("gwqtr_sac_rast_3_",yq)))
  rm(list=c(paste0("gwqtr_sac_1_",yq)))
  rm(list=c(paste0("gwqtr_sac_2_",yq)))
  rm(list=c(paste0("gwqtr_sac_3_",yq)))
  
}  

#Export quarterly SP and pump results to CSV
filename <- paste0("prems_gw_depths_from_rasters_qtr_SAC.csv")
write.csv(prems, file=filename , row.names=FALSE, quote=FALSE)
filename <- paste0("pumps_gw_depths_from_rasters_qtr_SAC.csv")
write.csv(pumps, file=filename , row.names=FALSE, quote=FALSE)



#########################################################################################
### 4. Extract groundwater depths for each SCE SP, from each monthly/quarterly raster ###
#########################################################################################

rm(list = ls())
path <- "T:/Projects/Pump Data/"
setwd(paste0(path,"data/misc"))

#Load monthly/quarterly rasters
load("gw_idw_rasters.RData")

#Read SCE coordinates
socal <- read.delim2("sce_prem_coord_1pull.txt",header=TRUE,sep=",",stringsAsFactors=FALSE)
socal$x <- as.numeric(socal$prem_lon)
socal$y <- as.numeric(socal$prem_lat)

#Create SpatialPointsDataFrame analogs
socal_2 <- socal
coordinates(socal_2) <- ~ x + y
proj4string(socal_2) <- CRS("+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")

#Loop over months to extract monthly groundwater depths
for (ym in levels(gwmth$modate)) {
  
  #Store ym's monthly rasters
  temp_ra1 <- get(paste0("gwmth_rast_1_",ym))
  temp_ra2 <- get(paste0("gwmth_rast_2_",ym))
  temp_ra3 <- get(paste0("gwmth_rast_3_",ym))
  
  #Create names for extracted values from rasters
  name_1s <- paste0("depth_1s_",ym)
  name_1b <- paste0("depth_1b_",ym)
  name_2s <- paste0("depth_2s_",ym)
  name_2b <- paste0("depth_2b_",ym)
  name_3s <- paste0("depth_3s_",ym)
  name_3b <- paste0("depth_3b_",ym)
  
  #Extract groundwater dephts from rasters (SP lat/lons)
  socal[[name_1s]] <- extract(temp_ra1,socal_2,method='simple'  ,df=TRUE)$layer
  socal[[name_1b]] <- extract(temp_ra1,socal_2,method='bilinear',df=TRUE)$layer
  socal[[name_2s]] <- extract(temp_ra2,socal_2,method='simple'  ,df=TRUE)$layer
  socal[[name_2b]] <- extract(temp_ra2,socal_2,method='bilinear',df=TRUE)$layer
  socal[[name_3s]] <- extract(temp_ra3,socal_2,method='simple'  ,df=TRUE)$layer
  socal[[name_3b]] <- extract(temp_ra3,socal_2,method='bilinear',df=TRUE)$layer
  
  #Store ym's monthly data frames
  temp_df1 <- get(paste0("gwmth_1_",ym))
  temp_df2 <- get(paste0("gwmth_2_",ym))
  temp_df3 <- get(paste0("gwmth_3_",ym))
  
  #Reproject spatial data frames to put units in meters
  temp_df1 <- spTransform(temp_df1,"+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
  temp_df2 <- spTransform(temp_df2,"+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
  temp_df3 <- spTransform(temp_df3,"+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
  
  #Create names for distances to nearest groundwater reading
  name_1d <- paste0("distkm_1_",ym)
  name_2d <- paste0("distkm_2_",ym)
  name_3d <- paste0("distkm_3_",ym)
  
  #Calculate distance to nearest groundwater reading (SP lat/lons)
  socal[[name_1d]] <- do.call(pmin, as.data.frame(pointDistance(socal_2,temp_df1,lonlat=TRUE)))/1000
  socal[[name_2d]] <- do.call(pmin, as.data.frame(pointDistance(socal_2,temp_df2,lonlat=TRUE)))/1000
  socal[[name_3d]] <- do.call(pmin, as.data.frame(pointDistance(socal_2,temp_df3,lonlat=TRUE)))/1000
  
  #Intermediate output
  print(paste(ym,"  ",Sys.time()))
  
  #Remove ym's monthly rasters and data frames that we no longer need, to save memory
  rm(list=c(paste0("gwmth_rast_1_",ym)))
  rm(list=c(paste0("gwmth_rast_2_",ym)))
  rm(list=c(paste0("gwmth_rast_3_",ym)))
  rm(list=c(paste0("gwmth_1_",ym)))
  rm(list=c(paste0("gwmth_2_",ym)))
  rm(list=c(paste0("gwmth_3_",ym)))
  
}  

#Export monthly SP results to CSV
filename <- paste0("socal_gw_depths_from_rasters_mth.csv")
write.csv(socal, file=filename , row.names=FALSE, quote=FALSE)

#Remove monthly columns from output dataframes
socal <- socal %>% select(c(sp_uuid,prem_lat,prem_long,bad_geocode_flag,missing_geocode_flag,pull,x,y))

#Loop over quarters to extract quarterly groundwater depths
for (yq in levels(gwqtr$qtr)) {
  
  #Store yq's quarterly rasters
  temp_ra1 <- get(paste0("gwqtr_rast_1_",yq))
  temp_ra2 <- get(paste0("gwqtr_rast_2_",yq))
  temp_ra3 <- get(paste0("gwqtr_rast_3_",yq))
  
  #Create names for extracted values from rasters
  name_1s <- paste0("depth_1s_",yq)
  name_1b <- paste0("depth_1b_",yq)
  name_2s <- paste0("depth_2s_",yq)
  name_2b <- paste0("depth_2b_",yq)
  name_3s <- paste0("depth_3s_",yq)
  name_3b <- paste0("depth_3b_",yq)
  
  #Extract groundwater dephts from rasters (SP lat/lons)
  socal[[name_1s]] <- extract(temp_ra1,socal_2,method='simple'  ,df=TRUE)$layer
  socal[[name_1b]] <- extract(temp_ra1,socal_2,method='bilinear',df=TRUE)$layer
  socal[[name_2s]] <- extract(temp_ra2,socal_2,method='simple'  ,df=TRUE)$layer
  socal[[name_2b]] <- extract(temp_ra2,socal_2,method='bilinear',df=TRUE)$layer
  socal[[name_3s]] <- extract(temp_ra3,socal_2,method='simple'  ,df=TRUE)$layer
  socal[[name_3b]] <- extract(temp_ra3,socal_2,method='bilinear',df=TRUE)$layer
  
  #Store yq's quarterly data frames
  temp_df1 <- get(paste0("gwqtr_1_",yq))
  temp_df2 <- get(paste0("gwqtr_2_",yq))
  temp_df3 <- get(paste0("gwqtr_3_",yq))
  
  #Reproject spatial data frames to put units in meters
  temp_df1 <- spTransform(temp_df1,"+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
  temp_df2 <- spTransform(temp_df2,"+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
  temp_df3 <- spTransform(temp_df3,"+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
  
  #Create names for distances to nearest groundwater reading
  name_1d <- paste0("distkm_1_",yq)
  name_2d <- paste0("distkm_2_",yq)
  name_3d <- paste0("distkm_3_",yq)
  
  #Calculate distance to nearest groundwater reading (SP lat/lons)
  socal[[name_1d]] <- do.call(pmin, as.data.frame(pointDistance(socal_2,temp_df1,lonlat=TRUE)))/1000
  socal[[name_2d]] <- do.call(pmin, as.data.frame(pointDistance(socal_2,temp_df2,lonlat=TRUE)))/1000
  socal[[name_3d]] <- do.call(pmin, as.data.frame(pointDistance(socal_2,temp_df3,lonlat=TRUE)))/1000
  
  #Intermediate output
  print(paste(yq,"  ",Sys.time()))
  
  #Remove yq's quarterly rasters and data drames that we no longer need, to save memory
  rm(list=c(paste0("gwqtr_rast_1_",yq)))
  rm(list=c(paste0("gwqtr_rast_2_",yq)))
  rm(list=c(paste0("gwqtr_rast_3_",yq)))
  rm(list=c(paste0("gwqtr_1_",yq)))
  rm(list=c(paste0("gwqtr_2_",yq)))
  rm(list=c(paste0("gwqtr_3_",yq)))
  
}  

#Export quarterly SP and pump results to CSV
filename <- paste0("socal_gw_depths_from_rasters_qtr.csv")
write.csv(socal, file=filename , row.names=FALSE, quote=FALSE)



###############################################################################################
### 5. Extract groundwater depths for each CLU centroid, from each monthly/quarterly raster ###
###############################################################################################

rm(list = ls())
path <- "T:/Projects/Pump Data/"
setwd(paste0(path,"data/misc"))

#Load monthly/quarterly rasters
load("gw_idw_rasters.RData")

#Read CLU coordinates
clu_sf <- readRDS(file=paste0(path,"data/cleaned_spatial/CLU/clu_centroids.RDS"))
latlon <- do.call(rbind, st_geometry(clu_sf)) %>%  as_tibble() %>% setNames(c("lon","lat")) %>% as.data.frame(stringsAsFactors=FALSE)
coords <- as.data.frame(cbind(as.data.frame(clu_sf$rowID),latlon))
coords <- coords[is.na(coords$lat)==0,]
coords <- coords[is.na(coords$lon)==0,]
names(coords) <- c("rowID","lon","lat")
class(coords)

clus <- coords
clus$x <- as.numeric(clus$lon)
clus$y <- as.numeric(clus$lat)

#Create SpatialPointsDataFrame analogs
clus_2 <- clus
coordinates(clus_2) <- ~ x + y
proj4string(clus_2) <- CRS("+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")

#Loop over months to extract monthly groundwater depths
for (ym in levels(gwmth$modate)) {
  
  #Store ym's monthly rasters
  temp_ra1 <- get(paste0("gwmth_rast_1_",ym))
  temp_ra2 <- get(paste0("gwmth_rast_2_",ym))
  temp_ra3 <- get(paste0("gwmth_rast_3_",ym))
  
  #Create names for extracted values from rasters
  name_1s <- paste0("depth_1s_",ym)
  name_1b <- paste0("depth_1b_",ym)
  name_2s <- paste0("depth_2s_",ym)
  name_2b <- paste0("depth_2b_",ym)
  name_3s <- paste0("depth_3s_",ym)
  name_3b <- paste0("depth_3b_",ym)
  
  #Extract groundwater dephts from rasters (CLU centroids)
  clus[[name_1s]] <- extract(temp_ra1,clus_2,method='simple'  ,df=TRUE)$layer
  clus[[name_1b]] <- extract(temp_ra1,clus_2,method='bilinear',df=TRUE)$layer
  clus[[name_2s]] <- extract(temp_ra2,clus_2,method='simple'  ,df=TRUE)$layer
  clus[[name_2b]] <- extract(temp_ra2,clus_2,method='bilinear',df=TRUE)$layer
  clus[[name_3s]] <- extract(temp_ra3,clus_2,method='simple'  ,df=TRUE)$layer
  clus[[name_3b]] <- extract(temp_ra3,clus_2,method='bilinear',df=TRUE)$layer
  
  #Store ym's monthly data frames
  temp_df1 <- get(paste0("gwmth_1_",ym))
  temp_df2 <- get(paste0("gwmth_2_",ym))
  temp_df3 <- get(paste0("gwmth_3_",ym))
  
  #Reproject spatial data frames to put units in meters
  temp_df1 <- spTransform(temp_df1,"+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
  temp_df2 <- spTransform(temp_df2,"+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
  temp_df3 <- spTransform(temp_df3,"+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
  
  #Create names for distances to nearest groundwater reading
  name_1d <- paste0("distkm_1_",ym)
  name_2d <- paste0("distkm_2_",ym)
  name_3d <- paste0("distkm_3_",ym)
  
  #Calculate distance to nearest groundwater reading (CLU centroids)
  clus[[name_1d]] <- do.call(pmin, as.data.frame(pointDistance(clus_2,temp_df1,lonlat=TRUE)))/1000
  clus[[name_2d]] <- do.call(pmin, as.data.frame(pointDistance(clus_2,temp_df2,lonlat=TRUE)))/1000
  clus[[name_3d]] <- do.call(pmin, as.data.frame(pointDistance(clus_2,temp_df3,lonlat=TRUE)))/1000
  
  #Intermediate output
  print(paste(ym,"  ",Sys.time()))
  
  #Remove ym's monthly rasters and data frames that we no longer need, to save memory
  rm(list=c(paste0("gwmth_rast_1_",ym)))
  rm(list=c(paste0("gwmth_rast_2_",ym)))
  rm(list=c(paste0("gwmth_rast_3_",ym)))
  rm(list=c(paste0("gwmth_1_",ym)))
  rm(list=c(paste0("gwmth_2_",ym)))
  rm(list=c(paste0("gwmth_3_",ym)))
  
}  

#Export monthly CLU centroid results to CSV
filename <- paste0("clus_gw_depths_from_rasters_mth.csv")
write.csv(clus, file=filename , row.names=FALSE, quote=FALSE)

#Remove monthly columns from output dataframes
clus <- clus %>% select(c(rowID,lon,lat,x,y))

#Loop over quarters to extract quarterly groundwater depths
for (yq in levels(gwqtr$qtr)) {
  
  #Store yq's quarterly rasters
  temp_ra1 <- get(paste0("gwqtr_rast_1_",yq))
  temp_ra2 <- get(paste0("gwqtr_rast_2_",yq))
  temp_ra3 <- get(paste0("gwqtr_rast_3_",yq))
  
  #Create names for extracted values from rasters
  name_1s <- paste0("depth_1s_",yq)
  name_1b <- paste0("depth_1b_",yq)
  name_2s <- paste0("depth_2s_",yq)
  name_2b <- paste0("depth_2b_",yq)
  name_3s <- paste0("depth_3s_",yq)
  name_3b <- paste0("depth_3b_",yq)
  
  #Extract groundwater dephts from rasters (CLU centroids)
  clus[[name_1s]] <- extract(temp_ra1,clus_2,method='simple'  ,df=TRUE)$layer
  clus[[name_1b]] <- extract(temp_ra1,clus_2,method='bilinear',df=TRUE)$layer
  clus[[name_2s]] <- extract(temp_ra2,clus_2,method='simple'  ,df=TRUE)$layer
  clus[[name_2b]] <- extract(temp_ra2,clus_2,method='bilinear',df=TRUE)$layer
  clus[[name_3s]] <- extract(temp_ra3,clus_2,method='simple'  ,df=TRUE)$layer
  clus[[name_3b]] <- extract(temp_ra3,clus_2,method='bilinear',df=TRUE)$layer
  
  #Store yq's quarterly data frames
  temp_df1 <- get(paste0("gwqtr_1_",yq))
  temp_df2 <- get(paste0("gwqtr_2_",yq))
  temp_df3 <- get(paste0("gwqtr_3_",yq))
  
  #Reproject spatial data frames to put units in meters
  temp_df1 <- spTransform(temp_df1,"+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
  temp_df2 <- spTransform(temp_df2,"+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
  temp_df3 <- spTransform(temp_df3,"+proj=longlat +datum=WGS84 +units=m +ellps=WGS84 +towgs84=0,0,0")
  
  #Create names for distances to nearest groundwater reading
  name_1d <- paste0("distkm_1_",yq)
  name_2d <- paste0("distkm_2_",yq)
  name_3d <- paste0("distkm_3_",yq)
  
  #Calculate distance to nearest groundwater reading (CLU centroids)
  clus[[name_1d]] <- do.call(pmin, as.data.frame(pointDistance(clus_2,temp_df1,lonlat=TRUE)))/1000
  clus[[name_2d]] <- do.call(pmin, as.data.frame(pointDistance(clus_2,temp_df2,lonlat=TRUE)))/1000
  clus[[name_3d]] <- do.call(pmin, as.data.frame(pointDistance(clus_2,temp_df3,lonlat=TRUE)))/1000
  
  #Intermediate output
  print(paste(yq,"  ",Sys.time()))
  
  #Remove yq's quarterly rasters and data drames that we no longer need, to save memory
  rm(list=c(paste0("gwqtr_rast_1_",yq)))
  rm(list=c(paste0("gwqtr_rast_2_",yq)))
  rm(list=c(paste0("gwqtr_rast_3_",yq)))
  rm(list=c(paste0("gwqtr_1_",yq)))
  rm(list=c(paste0("gwqtr_2_",yq)))
  rm(list=c(paste0("gwqtr_3_",yq)))
  
}  

#Export quarterly CLU centroid results to CSV
filename <- paste0("clus_gw_depths_from_rasters_qtr.csv")
write.csv(clus, file=filename , row.names=FALSE, quote=FALSE)


