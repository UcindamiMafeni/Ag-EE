# Created by Chinmay Lohani in November 2019
# Find the intersections between water districts and CLUs
# Additional notes exist on the task, to be dropped in Dbox

rm(list = ls())

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

if(Sys.getenv("USERNAME") == "clohani"){
  root_gh <- "C:/Users/clohani/OneDrive/Documents/github/Ag-EE"
}

m2_to_acre <- 0.000247105

path_wdis <- "D:/Water_matching/Spatial"
path_cal <- "D:/Water_matching/California health tracking/service_areas"
path_output <- "D:/Water_matching"

#create shapefiles objects for CLUs and waterdistricts
cal <- st_read(file.path(path_cal, "service_areas.shp"))
wdist <- st_read(file.path(path_wdis, "Water_Districts.shp"))

wdist <- wdist %>% 
  mutate(totAcres = as.numeric(st_area(.)) * m2_to_acre)

#use st_intersect to get dataframe with x*p_x number of observations
inter <- st_intersection(lwgeom::st_make_valid(wdist), lwgeom::st_make_valid(cal))

saveRDS(inter, file.path(path_output, "Intersected_data"))