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
path_clu <- "D:/Water_matching/clu_poly"
path_output <- "C:/Users/clohani/Dropbox/Water_Districts"

#create shapefiles objects for CLUs and waterdistricts
clu <- st_read(file.path(path_clu, "clu_poly.shp"))
wdist <- st_read(file.path(path_wdis, "Water_Districts.shp"))

#check why geometries are invalid
if (1==1) {
  invalid_clu <- clu %>% 
    mutate(is_valid= st_is_valid(.)) %>%
    filter(is_valid!="TRUE")
  valid_clu <- lwgeom::st_make_valid(invalid_clu)
  
  invalid_wdist <- wdist %>% 
    mutate(is_valid= st_is_valid(.)) %>%
    filter(is_valid!="TRUE")
  valid_wdist <- lwgeom::st_make_valid(invalid_wdist)
}

#use st_intersect to get dataframe with x*p_x number of observations
inter <- st_intersection(lwgeom::st_make_valid(clu), lwgeom::st_make_valid(wdist))

#find area of these things
#create a variable which sums total area of these things

inter <- inter %>% 
  mutate(IntAcres = as.numeric(st_area(.)) * m2_to_acre) %>%
  st_set_geometry(NULL) %>%
  group_by(CLU_ID) %>%
  mutate(tot_area= sum(IntAcres)) %>%
  ungroup
  
#save outputs
saveRDS(inter, file.path(path_output, "Intersected_data"))

#check if the total area of intersections is atleast as much as the CLU area
inter %>%
  verify(tot_area>CLUAcres)
  
#create cutoffs and be done
cutoff <- 0.05
filtered <- inter %>%
  mutate(frac_area=IntAcres/tot_area) %>%
  filter(frac_area>=0.05)

saveRDS(inter, file.path(path_output, "Intersected_data_filtered"))