######################################################################
## Code to find distances of all matches and keep the relevant ones ##
######################################################################
rm(list = ls())

#install.packages("sf")
library(sf)
library(ggmap) #, lib.loc=libP)
library(ggplot2) #, lib.loc=libP)
library(gstat) #, lib.loc=libP)
library(sp) #, lib.loc=libP)
library(maptools) #, lib.loc=libP)
library(rgdal) #, lib.loc=libP)
library(rgeos) #, lib.loc=libP)
library(raster) #, lib.loc=libP)
library(SDMTools) #, lib.loc=libP)
library(dplyr)

path <- "C:/Users/Chimmay Lohani/Dropbox/EW_Downloads/Spatial_verification"
setwd(path)

# Read in the shapefiles and coordinates, reproject
wdist <- readOGR(dsn = "Water_Districts.shp", layer = "Water_Districts")
proj4string(wdist)
wdist <- spTransform(wdist, CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0"))
coords <- read.delim2("Mysterious_string_matches.csv",header=TRUE,sep=",",stringsAsFactors=FALSE)
coords$latitude <- as.numeric(coords$latitude)
coords$longitude <- as.numeric(coords$longitude)
coords <-coords[complete.cases(coords), ]

#Convert the matched coordinates to SpatialPointsDataFrame
coords_sp <- coords
keeps <- c("name_DBF", "latitude", "longitude")
coords_sp <- coords_sp[keeps]
coords_sp$latitude <- as.numeric(coords_sp$latitude)
coords_sp$longitude <- as.numeric(coords_sp$longitude)

coordinates(coords_sp) <- ~ longitude + latitude
proj4string(coords_sp) <- proj4string(wdist)

#add a column to original dataframe to save the distance distance
m <- nrow(coords)

distance <- c(1:m)
coords <- cbind(coords,distance)

#change things to planar coordinates to get distances
utmStr <- "+proj=utm +zone=%d +datum=NAD83 +units=m +no_defs +ellps=GRS80 +towgs84=0,0,0"
utm <- CRS(sprintf(utmStr, 10))
coords_sp <- spTransform(coords_sp, utm)
wdist <- spTransform(wdist, utm)

n <- nrow(wdist@data)
for (i in 1:n) { #wdist
  for (j in 1:m) { #coords
    if (wdist@data$AGENCYNAME[i]==coords$name_DBF[j]) {
      dist <- gDistance(wdist[i,], coords_sp[j,], byid=TRUE)

      coords$distance[j] <- dist
    } #if
  } #j
} #i 

write.csv(coords, file= "Match_with_distances.csv")