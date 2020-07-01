# Created by Chinmay Lohani in November 2019
# Find the intersections between basins and CLUs
# Additional notes exist on validity of intersections dropped in Dbox "Chinmay_notes_spatial_Dec19.pdf"

rm(list = ls())

#Data downloaded from: https://data.cnra.ca.gov/dataset/ca-bulletin-118-groundwater-basins

library(tidyverse)
library(dplyr)
library(ggmap) #, lib.loc=libP)
library(ggplot2) #, lib.loc=libP)
library(gstat) #, lib.loc=libP)
library(maptools) #, lib.loc=libP)
library(rgdal) #, lib.loc=libP)
library(rgeos) #, lib.loc=libP)
library(raster) #, lib.loc=libP)
library(SDMTools) #, lib.loc=libP)
library(sf)
library(assertr)
library(lwgeom)
library(parallel)
library(cluster)

m2_to_acre <- 0.000247105

path <- "T:/Projects/Pump Data/"

########################################################################
### 1. Export data on basins, and confirm they are within California ###
########################################################################

setwd(paste0(path,"data/spatial"))

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

#Export dataset of water basins
setwd(paste0(path,"data/misc"))
filename <- "ca_water_basins_raw.txt"
write.table(data, file=filename , row.names=FALSE, col.names=TRUE, sep="%", quote=FALSE, append=FALSE)

rm(wbasn,CAoutline,data,filename)



####################################
### 2. Proceed with spatial join ###
####################################

path_basins <- paste0(path,"data/spatial/CA_Bulletin_118_Groundwater_Basins")
path_clu <- paste0(path,"data/cleaned_spatial/CLU/clu_poly")
path_output <- paste0(path,"data/misc")

#create sf objects for CLUs and basins
clu <- st_read(file.path(path_clu, "clu_poly.shp"))
basins <- st_read(file.path(path_basins, "CA_Bulletin_118_Groundwater_Basins.shp"))

crs <- st_crs(clu)
crs_2 <- st_crs(basins)
clu <- st_transform(clu, crs_2)

basins <- basins %>% 
  mutate(totAcres = as.numeric(st_area(.)) * m2_to_acre)

#check why geometries are invalid
if (1==0) {
  invalid_clu <- clu %>% 
    mutate(is_valid= st_is_valid(.)) %>%
    filter(is_valid!="TRUE")
  valid_clu <- lwgeom::st_make_valid(clu)
  
  invalid_wdist <- basins %>% 
    mutate(is_valid= st_is_valid(.)) %>%
    filter(is_valid!="TRUE")
  valid_wdist <- lwgeom::st_make_valid(basins)
}

#use st_intersect to get dataframe with x*p_x number of observations
inter <- st_intersection(lwgeom::st_make_valid(clu), lwgeom::st_make_valid(basins))

#find area of these things
#create a variable which sums total area of these things
inter <- inter %>% 
  mutate(IntAcres = as.numeric(st_area(.)) * m2_to_acre) %>%
  st_set_geometry(NULL) %>%
  group_by(CLU_ID) %>%
  mutate(tot_int_area= sum(IntAcres)) %>%
  ungroup

#subset CLUs to isolate non-matches
clu_matched <- as.data.frame(inter$CLU_ID)
names(clu_matched) <- "CLU_ID"
clu_unmatched <- anti_join(clu, clu_matched, by="CLU_ID")

#for unmatched CLUs, create function to find distance to nearest basin
nearest_dist <- function(clu_indiv){
  d_which <- st_nearest_feature(lwgeom::st_make_valid(clu_indiv), lwgeom::st_make_valid(basins))
  d_min <- st_distance(lwgeom::st_make_valid(clu_indiv), lwgeom::st_make_valid(basins)[d_which,])
  d_nearest <- st_drop_geometry(basins[d_which,])
  d_out <- cbind(st_drop_geometry(clu_indiv),d_min,d_nearest, stringsAsFactors=FALSE)
  return(d_out)
} 

#parallelize, and execute this function
cores <- detectCores()
cl <- makeCluster(24)
clusterEvalQ(cl, library(lwgeom))
clusterEvalQ(cl, library(sf))
clusterSetRNGStream(cl, 12345)
clusterExport(cl=cl, varlist=c('clu_unmatched','basins','nearest_dist'))
out_nearest <- as.data.frame(t(parSapply(cl=cl, 1:nrow(clu_unmatched), function(i) nearest_dist(clu_unmatched[i,]))),stringsAsFactors=FALSE)
stopCluster(cl)

#unlist parsapply results, because parSapply is stupid af
out_nearest <- out_nearest %>% unnest(CLUAcres)
out_nearest <- out_nearest %>% unnest(County)
out_nearest <- out_nearest %>% unnest(CLU_ID)
out_nearest <- out_nearest %>% unnest(d_min)
out_nearest <- out_nearest %>% unnest(OBJECTID)
out_nearest <- out_nearest %>% unnest(Basin_Numb)
out_nearest <- out_nearest %>% unnest(Basin_Subb)
out_nearest <- out_nearest %>% unnest(Basin_Name)
out_nearest <- out_nearest %>% unnest(Basin_Su_1)
out_nearest <- out_nearest %>% unnest(Region_Off)
out_nearest <- out_nearest %>% unnest(GlobalID)
out_nearest <- out_nearest %>% unnest(totAcres)

out_nearest <- out_nearest %>% mutate(CLUAcres = as.numeric(CLUAcres))
out_nearest <- out_nearest %>% mutate(County = as.character(County))
out_nearest <- out_nearest %>% mutate(CLU_ID = as.character(CLU_ID))
out_nearest <- out_nearest %>% mutate(d_min = as.numeric(d_min))
out_nearest <- out_nearest %>% mutate(OBJECTID = as.numeric(OBJECTID))
out_nearest <- out_nearest %>% mutate(Basin_Numb = as.character(Basin_Numb))
out_nearest <- out_nearest %>% mutate(Basin_Subb = as.character(Basin_Subb))
out_nearest <- out_nearest %>% mutate(Basin_Name = as.character(Basin_Name))
out_nearest <- out_nearest %>% mutate(Basin_Su_1 = as.character(Basin_Su_1))
out_nearest <- out_nearest %>% mutate(Region_Off = as.character(Region_Off))
out_nearest <- out_nearest %>% mutate(GlobalID = as.character(GlobalID))
out_nearest <- out_nearest %>% mutate(totAcres = as.numeric(totAcres))

#export results
outfile <- full_join(inter,out_nearest, by="CLU_ID")
file <- paste0(path_output,"/CLU_basins.csv")
write.csv(outfile, file, sep=",", row.names=FALSE, col.names=TRUE)


