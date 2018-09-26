#############################################################
# Script to rasterize DWR groundwater data by quarter/month #
#############################################################

rm(list = ls())
library(ggmap)
library(ggplot2)
library(gstat)
library(sp)
library(maptools)
library(rgdal)
library(raster)
library(dplyr)


###################################################
### 1. Rasterize panels of groundwater readings ###
###################################################

#Read groundwater depths by station and by month/quarter
setwd("S:/Matt/ag_pump/data/misc")
gwmth <- read.delim2("ca_dwr_depth_latlon_month.txt",header=TRUE,sep=",",stringsAsFactors=FALSE)
gwqtr <- read.delim2("ca_dwr_depth_latlon_quarter.txt",header=TRUE,sep=",",stringsAsFactors=FALSE)

#Convert from character to numeric
gwmth$latitude <- as.numeric(as.character(gwmth$latitude))
gwmth$longitude <- as.numeric(as.character(gwmth$longitude))
gwmth$gs_ws_depth_1 <- as.numeric(as.character(gwmth$gs_ws_depth_1))
gwmth$gs_ws_depth_2 <- as.numeric(as.character(gwmth$gs_ws_depth_2))
gwmth$gs_ws_depth_3 <- as.numeric(as.character(gwmth$gs_ws_depth_3))
gwqtr$latitude <- as.numeric(as.character(gwqtr$latitude))
gwqtr$longitude <- as.numeric(as.character(gwqtr$longitude))
gwqtr$gs_ws_depth_1 <- as.numeric(as.character(gwqtr$gs_ws_depth_1))
gwqtr$gs_ws_depth_2 <- as.numeric(as.character(gwqtr$gs_ws_depth_2))
gwqtr$gs_ws_depth_3 <- as.numeric(as.character(gwqtr$gs_ws_depth_3))

#Subset monthly panel by month, and create 3 rasters for each month
grd_step <- 0.01
gwmth$modate <- as.factor(gwmth$modate)
for (ym in levels(gwmth$modate)) {
  
  temp1 <- gwmth[(gwmth$modate==ym & is.na(gwmth$gs_ws_depth_1)==0),] # subset by month ym
  temp1$x <- temp1$longitude 
  temp1$y <- temp1$latitude
  temp1 <- temp1[,names(temp1) %in% c("casgem_station_id","modate","year","month","x","y","basin_id","gs_ws_depth_1")]
  coordinates(temp1) <- ~x + y # convert to spatial points data frame
  proj4string(temp1) <- CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0") # assign a standard projection
  temp_bbox1 <- t(temp1@bbox) # define outer box
  temp_grd1 <- expand.grid(x = seq(from = temp_bbox1[1,1], to = temp_bbox1[2,1], by = grd_step), 
                           y = seq(from = temp_bbox1[1,2], to = temp_bbox1[2,2], by = grd_step)) # expand points to grid
  coordinates(temp_grd1) <- ~x + y  #convert grid to spatial points data frame
  proj4string(temp_grd1) <- CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0") # assign a standard projection
  gridded(temp_grd1) <- TRUE
  temp_idw1 <- idw(formula=gs_ws_depth_1 ~ 1, locations=temp1, 
                   newdata=temp_grd1, idp=2) # apply idw model for the depth data
  temp_idw1 <- as.data.frame(temp_idw1)[,1:3] # convert gridded pixels data frame into regular data frame
  names(temp_idw1)[1:3] <- c("x", "y", "gw") # label columns
  coordinates(temp_idw1) <- ~x + y # convert regular data frame into spatial points data frame
  proj4string(temp_idw1) <- CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0") # assign a standard projection
  rast <- raster() # define raster
  extent(rast) <- extent(c(temp_bbox1[1,1]-grd_step/2, temp_bbox1[2,1]+grd_step/2, 
                           temp_bbox1[1,2]-grd_step/2, temp_bbox1[2,2]+grd_step/2)) # make raster same size as IDW grid
  res(rast) <- grd_step # set resolution of raster to be same as resolution of IDW grid
  temp_rast1 <- rasterize(temp_idw1, rast, temp_idw1$gw, fun=mean) # RASTERIZE!
  assign(paste0("gwmth_1_",ym),temp1) # store subseted monthly dataset
  assign(paste0("gwmth_rast_1_",ym),temp_rast1) # store monthly raster
  
  temp2 <- gwmth[(gwmth$modate==ym & is.na(gwmth$gs_ws_depth_2)==0),] # subset by month ym
  temp2$x <- temp2$longitude 
  temp2$y <- temp2$latitude
  temp2 <- temp2[,names(temp2) %in% c("casgem_station_id","modate","year","month","x","y","basin_id","gs_ws_depth_2")]
  coordinates(temp2) <- ~x + y # convert to spatial points data frame
  proj4string(temp2) <- CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0") # assign a standard projection
  temp_bbox2 <- t(temp2@bbox) # define outer box
  temp_grd2 <- expand.grid(x = seq(from = temp_bbox2[1,1], to = temp_bbox2[2,1], by = grd_step), 
                           y = seq(from = temp_bbox2[1,2], to = temp_bbox2[2,2], by = grd_step)) # expand points to grid
  coordinates(temp_grd2) <- ~x + y  #convert grid to spatial points data frame
  proj4string(temp_grd2) <- CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0") # assign a standard projection
  gridded(temp_grd2) <- TRUE
  temp_idw2 <- idw(formula=gs_ws_depth_2 ~ 1, locations=temp2, 
                   newdata=temp_grd2, idp=2) # apply idw model for the depth data
  temp_idw2 <- as.data.frame(temp_idw2)[,1:3] # convert gridded pixels data frame into regular data frame
  names(temp_idw2)[1:3] <- c("x", "y", "gw") # label columns
  coordinates(temp_idw2) <- ~x + y # convert regular data frame into spatial points data frame
  proj4string(temp_idw2) <- CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0") # assign a standard projection
  rast <- raster() # define raster
  extent(rast) <- extent(c(temp_bbox2[1,1]-grd_step/2, temp_bbox2[2,1]+grd_step/2, 
                           temp_bbox2[1,2]-grd_step/2, temp_bbox2[2,2]+grd_step/2)) # make raster same size as IDW grid
  res(rast) <- grd_step # set resolution of raster to be same as resolution of IDW grid
  temp_rast2 <- rasterize(temp_idw2, rast, temp_idw2$gw, fun=mean) # RASTERIZE!
  assign(paste0("gwmth_2_",ym),temp2) # store subseted monthly dataset
  assign(paste0("gwmth_rast_2_",ym),temp_rast2) # store monthly raster
  
  temp3 <- gwmth[(gwmth$modate==ym & is.na(gwmth$gs_ws_depth_3)==0),] # subset by month ym
  temp3$x <- temp3$longitude 
  temp3$y <- temp3$latitude
  temp3 <- temp3[,names(temp3) %in% c("casgem_station_id","modate","year","month","x","y","basin_id","gs_ws_depth_3")]
  coordinates(temp3) <- ~x + y # convert to spatial points data frame
  proj4string(temp3) <- CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0") # assign a standard projection
  temp_bbox3 <- t(temp3@bbox) # define outer box
  temp_grd3 <- expand.grid(x = seq(from = temp_bbox3[1,1], to = temp_bbox3[2,1], by = grd_step), 
                           y = seq(from = temp_bbox3[1,2], to = temp_bbox3[2,2], by = grd_step)) # expand points to grid
  coordinates(temp_grd3) <- ~x + y  #convert grid to spatial points data frame
  proj4string(temp_grd3) <- CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0") # assign a standard projection
  gridded(temp_grd3) <- TRUE
  temp_idw3 <- idw(formula=gs_ws_depth_3 ~ 1, locations=temp3, 
                   newdata=temp_grd3, idp=2) # apply idw model for the depth data
  temp_idw3 <- as.data.frame(temp_idw3)[,1:3] # convert gridded pixels data frame into regular data frame
  names(temp_idw3)[1:3] <- c("x", "y", "gw") # label columns
  coordinates(temp_idw3) <- ~x + y # convert regular data frame into spatial points data frame
  proj4string(temp_idw3) <- CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0") # assign a standard projection
  rast <- raster() # define raster
  extent(rast) <- extent(c(temp_bbox3[1,1]-grd_step/2, temp_bbox3[2,1]+grd_step/2, 
                           temp_bbox3[1,2]-grd_step/2, temp_bbox3[2,2]+grd_step/2)) # make raster same size as IDW grid
  res(rast) <- grd_step # set resolution of raster to be same as resolution of IDW grid
  temp_rast3 <- rasterize(temp_idw3, rast, temp_idw3$gw, fun=mean) # RASTERIZE!
  assign(paste0("gwmth_3_",ym),temp3) # store subseted monthly dataset
  assign(paste0("gwmth_rast_3_",ym),temp_rast3) # store monthly raster
  
  print(ym)
  print(Sys.time())
  
  #Save image of workspace
  save.image(file = "S:/Matt/ag_pump/data/misc/temp_gw_idw_rasters.RData")
  
}


#Subset quarterly panel by quarter, and create 3 rasters for each quarter
grd_step <- 0.01
gwqtr$qtr <- as.factor(gwqtr$qtr)
for (yq in levels(gwqtr$qtr)) {
  
  temp1 <- gwqtr[(gwqtr$qtr==yq & is.na(gwqtr$gs_ws_depth_1)==0),] # subset by quarter yq
  temp1$x <- temp1$longitude 
  temp1$y <- temp1$latitude
  temp1 <- temp1[,names(temp1) %in% c("casgem_station_id","qtr","year","quarter","x","y","basin_id","gs_ws_depth_1")]
  coordinates(temp1) <- ~x + y # convert to spatial points data frame
  proj4string(temp1) <- CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0") # assign a standard projection
  temp_bbox1 <- t(temp1@bbox) # define outer box
  temp_grd1 <- expand.grid(x = seq(from = temp_bbox1[1,1], to = temp_bbox1[2,1], by = grd_step), 
                           y = seq(from = temp_bbox1[1,2], to = temp_bbox1[2,2], by = grd_step)) # expand points to grid
  coordinates(temp_grd1) <- ~x + y  #convert grid to spatial points data frame
  proj4string(temp_grd1) <- CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0") # assign a standard projection
  gridded(temp_grd1) <- TRUE
  temp_idw1 <- idw(formula=gs_ws_depth_1 ~ 1, locations=temp1, 
                   newdata=temp_grd1, idp=2) # apply idw model for the depth data
  temp_idw1 <- as.data.frame(temp_idw1)[,1:3] # convert gridded pixels data frame into regular data frame
  names(temp_idw1)[1:3] <- c("x", "y", "gw") # label columns
  coordinates(temp_idw1) <- ~x + y # convert regular data frame into spatial points data frame
  proj4string(temp_idw1) <- CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0") # assign a standard projection
  rast <- raster() # define raster
  extent(rast) <- extent(c(temp_bbox1[1,1]-grd_step/2, temp_bbox1[2,1]+grd_step/2, 
                           temp_bbox1[1,2]-grd_step/2, temp_bbox1[2,2]+grd_step/2)) # make raster same size as IDW grid
  res(rast) <- grd_step # set resolution of raster to be same as resolution of IDW grid
  temp_rast1 <- rasterize(temp_idw1, rast, temp_idw1$gw, fun=mean) # RASTERIZE!
  assign(paste0("gwqtr_1_",yq),temp1) # store subseted quarterly dataset
  assign(paste0("gwqtr_rast_1_",yq),temp_rast1) # store quarterly raster
  
  temp2 <- gwqtr[(gwqtr$qtr==yq & is.na(gwqtr$gs_ws_depth_2)==0),] # subset by quarter yq
  temp2$x <- temp2$longitude 
  temp2$y <- temp2$latitude
  temp2 <- temp2[,names(temp2) %in% c("casgem_station_id","qtr","year","quarter","x","y","basin_id","gs_ws_depth_2")]
  coordinates(temp2) <- ~x + y # convert to spatial points data frame
  proj4string(temp2) <- CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0") # assign a standard projection
  temp_bbox2 <- t(temp2@bbox) # define outer box
  temp_grd2 <- expand.grid(x = seq(from = temp_bbox2[1,1], to = temp_bbox2[2,1], by = grd_step), 
                           y = seq(from = temp_bbox2[1,2], to = temp_bbox2[2,2], by = grd_step)) # expand points to grid
  coordinates(temp_grd2) <- ~x + y  #convert grid to spatial points data frame
  proj4string(temp_grd2) <- CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0") # assign a standard projection
  gridded(temp_grd2) <- TRUE
  temp_idw2 <- idw(formula=gs_ws_depth_2 ~ 1, locations=temp2, 
                   newdata=temp_grd2, idp=2) # apply idw model for the depth data
  temp_idw2 <- as.data.frame(temp_idw2)[,1:3] # convert gridded pixels data frame into regular data frame
  names(temp_idw2)[1:3] <- c("x", "y", "gw") # label columns
  coordinates(temp_idw2) <- ~x + y # convert regular data frame into spatial points data frame
  proj4string(temp_idw2) <- CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0") # assign a standard projection
  rast <- raster() # define raster
  extent(rast) <- extent(c(temp_bbox2[1,1]-grd_step/2, temp_bbox2[2,1]+grd_step/2, 
                           temp_bbox2[1,2]-grd_step/2, temp_bbox2[2,2]+grd_step/2)) # make raster same size as IDW grid
  res(rast) <- grd_step # set resolution of raster to be same as resolution of IDW grid
  temp_rast2 <- rasterize(temp_idw2, rast, temp_idw2$gw, fun=mean) # RASTERIZE!
  assign(paste0("gwqtr_2_",yq),temp2) # store subseted quarterly dataset
  assign(paste0("gwqtr_rast_2_",yq),temp_rast2) # store quarterly raster
  
  temp3 <- gwqtr[(gwqtr$qtr==yq & is.na(gwqtr$gs_ws_depth_3)==0),] # subset by quarter yq
  temp3$x <- temp3$longitude 
  temp3$y <- temp3$latitude
  temp3 <- temp3[,names(temp3) %in% c("casgem_station_id","qtr","year","quarter","x","y","basin_id","gs_ws_depth_3")]
  coordinates(temp3) <- ~x + y # convert to spatial points data frame
  proj4string(temp3) <- CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0") # assign a standard projection
  temp_bbox3 <- t(temp3@bbox) # define outer box
  temp_grd3 <- expand.grid(x = seq(from = temp_bbox3[1,1], to = temp_bbox3[2,1], by = grd_step), 
                           y = seq(from = temp_bbox3[1,2], to = temp_bbox3[2,2], by = grd_step)) # expand points to grid
  coordinates(temp_grd3) <- ~x + y  #convert grid to spatial points data frame
  proj4string(temp_grd3) <- CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0") # assign a standard projection
  gridded(temp_grd3) <- TRUE
  temp_idw3 <- idw(formula=gs_ws_depth_3 ~ 1, locations=temp3, 
                   newdata=temp_grd3, idp=2) # apply idw model for the depth data
  temp_idw3 <- as.data.frame(temp_idw3)[,1:3] # convert gridded pixels data frame into regular data frame
  names(temp_idw3)[1:3] <- c("x", "y", "gw") # label columns
  coordinates(temp_idw3) <- ~x + y # convert regular data frame into spatial points data frame
  proj4string(temp_idw3) <- CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0") # assign a standard projection
  rast <- raster() # define raster
  extent(rast) <- extent(c(temp_bbox3[1,1]-grd_step/2, temp_bbox3[2,1]+grd_step/2, 
                           temp_bbox3[1,2]-grd_step/2, temp_bbox3[2,2]+grd_step/2)) # make raster same size as IDW grid
  res(rast) <- grd_step # set resolution of raster to be same as resolution of IDW grid
  temp_rast3 <- rasterize(temp_idw3, rast, temp_idw3$gw, fun=mean) # RASTERIZE!
  assign(paste0("gwqtr_3_",yq),temp3) # store subseted quarterly dataset
  assign(paste0("gwqtr_rast_3_",yq),temp_rast3) # store quarterly raster
  
  print(yq)
  print(Sys.time())
  
  #Save image of workspace
  save.image(file = "S:/Matt/ag_pump/data/misc/temp_gw_idw_rasters.RData")
  
}

#Save image of workspace
save.image(file = "S:/Matt/ag_pump/data/misc/temp_gw_idw_rasters.RData")


