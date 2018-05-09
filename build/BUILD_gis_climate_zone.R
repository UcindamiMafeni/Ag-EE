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
library(raster)
library(SDMTools)

setwd("S:/Matt/ag_pump/data/misc")


#Load CLimate Zones shapefile
cz <- readOGR(dsn = "CEC_climate_zones", layer = "CA_Building_Standards_Climate_Zones")

#Read PGE  coordinates and mine coordinates
prems <- read.delim2("pge_prem_coord_raw.txt",header=TRUE,sep=",",stringsAsFactors=FALSE)
prems$longitude <- as.numeric(prems$prem_lon)
prems$latitude <- as.numeric(prems$prem_lat)
prems$czone <- as.numeric(substr(prems$climate_zone_cd,2,3))

#Test whether each lat/lon is in its assigned state
prems$czone_poly <- mapply(function(x,y,z) 
  pnt.in.poly(cbind(x,y),
              fortify(cz[cz$Zone==z,])[c("long","lat")])$pip,
  prems$longitude,
  prems$latitude,
  prems$czone)
mean(prems$czone_poly)

#Find which county lat/lon is in, for non-matches
bad_czone <- which(prems$czone_poly==0)
czone_list <- cz@data$Zone
prems$czone_poly_assign <- NA
for (i in bad_czone) {
  for (j in czone_list) {
    if (is.na(prems$czone_poly_assign[i])) {
      tmp <- pnt.in.poly(cbind(prems$longitude[i],prems$latitude[i]),
                         fortify(cz[cz$Zone==j,])[c("long","lat")])$pip
      if (as.numeric(tmp)==1) {
        prems$czone_poly_assign[i] <- j
        print(c(i,j))  
      }
    }
  }
}  


#Export results to CSV
filename <- "pge_prem_coord_polygon.csv"
write.csv(prems, file=filename , row.names=FALSE, col.names=TRUE, sep=",", quote=FALSE)


