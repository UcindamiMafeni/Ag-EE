#############################################################
## Code to find where points are mapped to by string match ##
#############################################################
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

path <- "C:/Users/Chimmay Lohani/Dropbox/EW_Downloads"
setwd(path)

# Read in the shapefiles and coordinates, reproject
wdist <- readOGR(dsn = "Water_Districts.shp", layer = "Water_Districts")
proj4string(wdist)
wdist <- spTransform(wdist, CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0"))
coords <- read.delim2("Mysterious_string_matches.csv",header=TRUE,sep=",",stringsAsFactors=FALSE)

#merge the shapefiles and coordinates
DBF <- merge(wdist, coords, by.x = "AGENCYNAME", by.y = "name_DBF")

#save this object
writeOGR(DBF, dsn="path", layer="Water_Districts_2", driver="ESRI Shapefile",overwrite_layer=T)

#convert to SF, add indexing to keep track of things, back to ST 
temp <-  st_as_sf(DBF)
n <- nrow(DBF@data)
index <- c(1:n)
temp <- cbind(temp,index)

#save only those for which we don't have NA values
DBF <-temp[complete.cases(temp$geo_matched), ]
DBF <- as(DBF, "Spatial")

#save the names and coordinates associated with the matches
coords <- as(DBF, "data.frame")
keeps <- c("AGENCYNAME", "lat", "lon","index")
coords <- coords[keeps]

#Convert to SpatialPointsDataFrame
coords$lat <- as.numeric(coords$lat)
coords$lon <- as.numeric(coords$lon)
coordinates(coords) <- ~ lon + lat
proj4string(coords) <- proj4string(wdist)

#change things to planar coordinates to get distances
utmStr <- "+proj=utm +zone=%d +datum=NAD83 +units=m +no_defs +ellps=GRS80 +towgs84=0,0,0"
utm <- CRS(sprintf(utmStr, 10))
coords_UTM <- spTransform(coords, utm)
DBF_UTM <- spTransform(DBF, utm)

#get distances
n<-646
dist_int <- numeric(n)
for (j in 1:n) {
  dist_int[j] <- gDistance(DBF_UTM[j,], coords_UTM[j,], byid=TRUE)
}

#change to SF to plot
dbf_sf <- st_as_sf(DBF)
coords_sf <- st_as_sf(coords)

rownames(dbf_sf) <- 1:nrow(dbf_sf)
rownames(coords_sf) <- 1:nrow(coords_sf)

#Load CA state outline
CAoutline <- readOGR(dsn = path, layer = "CA_State_TIGER2016")
proj4string(CAoutline)
CAoutline <- spTransform(CAoutline, CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0"))
CAoutline_sf <- st_as_sf(CAoutline)

remove(index)

for (i in 0:31) {
  m <- i*20
  m_i<- m+ 20
  
  dbf_sf_i <- dbf_sf[c(m:m_i),]
  coords_sf_i <- coords_sf[c(m:m_i),]

  ggplot() +
    geom_sf(data=CAoutline_sf,colour="grey30") +
    geom_sf(data=dbf_sf_i,aes(colour=index)) +
    geom_sf(data=coords_sf_i,aes(colour=index),shape=19,size=1)+
    theme(legend.position = "none")
  #p <-  p + geom_sf(data=coords_sf,aes(colour=rownames))
  #p
  #ggsave('check_plot.pdf')
  fname <- paste("check_plot",i,".pdf")
  ggsave(fname)
}


#for (i in 0:31) {
#  m <- i*20
#  m_i<- m+ 20
  
#  dbf_sf_i <- dbf_sf[c(m:m_i),]
#  coords_sf_i <- coords_sf[c(m:m_i),]
  
#  dbf_i <- as(dbf_sf_i, "Spatial")
#  coords_i <- as(coords_sf_i, "Spatial")
#  coords_i <- as(coords_i, "data.frame")
  
#  ggplot() + 
#    geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
#                 color="grey30", fill=NA, alpha=1) +
#    geom_polygon(data=dbf_i, aes(x=long, y=lat, group=group, colour=index), color="black") +
#    geom_point(data=coords_i, aes(x=coords.x1, y=coords.x2, colour=index), color="black")
  
  
#  ggplot() +
#    geom_sf(data=CAoutline_sf,colour="grey30") +
#    geom_sf(data=dbf_sf_i,aes(colour=index)) +
#    geom_sf(data=coords_sf_i,aes(colour=index),shape=19,size=1)+
#    theme(legend.position = "none")

#  fname <- paste("check_plot",i,".pdf")
#  ggsave(fname)
#}