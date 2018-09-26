###############################################################
# Script to predict groundwater depth from SPs and APEP pumps #
#    by extracting values from monthly/quarterly rasters      #
###############################################################

rm(list = ls())
library(ggmap)
library(ggplot2)
library(gstat)
library(sp)
library(maptools)
library(rgdal)
library(raster)
library(dplyr)


#####################################################################################
### 1. Extract groundwater depths for each SP, from each monthly/quarterly raster ###
#####################################################################################




load "S:/Matt/ag_pump/data/misc/temp_gw_idw_rasters.RData"



#Reformat mines lat/lon
mines_2 <- mines[c("msha_id","longitude","latitude")]
mines_2$longitude <- as.numeric(mines_2$longitude)
mines_2$latitude <- as.numeric(mines_2$latitude)
mines_2$x <- as.numeric(mines_2$longitude)
mines_2$y <- as.numeric(mines_2$latitude)
mines_2$x <- sapply(mines_2$x, function(x) max(usgs_2_box[1,1]+grd_step/4,x))
mines_2$x <- sapply(mines_2$x, function(x) min(usgs_2_box[2,1]-grd_step/4,x))
mines_2$y <- sapply(mines_2$y, function(x) max(usgs_2_box[1,2]+grd_step/4,x))
mines_2$y <- sapply(mines_2$y, function(x) min(usgs_2_box[2,2]-grd_step/4,x))
# 0.5% of observations are outside the USGS grid, and I correct 
# these lat/lons to bring them to the edge of the grid
mines_3 <- mines_2[c("msha_id","x","y")]
coordinates(mines_3) <- ~x + y


#Extract raster values to mine coordinates, looping over IDW variables
for (i in names(idw)[3:ncol(idw)]){
  print(i)
  
  #Convert IDW values into a SpatialPointsDataFrame
  idw_temp <- idw[c("x","y",i)]
  names(idw_temp)[3] <- "idw_value"
  coordinates(idw_temp) <- ~x + y
  
  #Define raster with same grid size and resolution as USGS grid
  rast <- raster()
  extent(rast) <- extent(c(usgs_2_box[1,1]-grd_step/2,
                           usgs_2_box[2,1]+grd_step/2,
                           usgs_2_box[1,2]-grd_step/2,
                           usgs_2_box[2,2]+grd_step/2))
  res(rast) <- grd_step
  
  #Rasterize!
  rast_temp <- rasterize(idw_temp, rast, idw_temp$idw_value, fun=mean)
  
  #Extract raster values for each pair of mine coordinates (simple)
  name_temp <- paste0(i,"_sim")
  mines_2[[name_temp]] <- extract(rast_temp,mines_3,method='simple',df=TRUE)$layer
  
  #Extract raster values for each pair of mine coordinates (bilinear)
  name_temp <- paste0(i,"_bil")
  mines_2[[name_temp]] <- extract(rast_temp,mines_3,method='bilinear',df=TRUE)$layer
}  


#Export results to CSV
filename <- paste0("mine_depth_thickness_idw_grd",gsub("[.]","",grd_step),".csv")
write.csv(mines_2, file=filename , row.names=FALSE, quote=FALSE)

plot(rast_temp,xlim=c(-83,-82),ylim=c(36.5,38.5))
mines_2_na <- mines_2[is.na(mines_2$depth_idw2_sim)==TRUE,]
