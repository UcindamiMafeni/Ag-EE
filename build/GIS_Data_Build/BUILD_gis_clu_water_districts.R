# Created by Chinmay Lohani in November 2019
# Find the intersections between water districts and CLUs
# Additional notes exist on validity of intersections dropped in Dbox "Chinmay_notes_spatial_Dec19.pdf"

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

m2_to_acre <- 0.000247105

path <- "T:/Projects/Pump Data/"
path_wdist <- paste0(path,"data/surface_water/ca_Final_shapefiles_Nick/")
path_clu <- paste0(path,"data/cleaned_spatial/CLU/clu_poly")
path_output <- paste0(path,"data/misc")

#create sf objects for CLUs and water districts
clu <- st_read(file.path(path_clu, "clu_poly.shp"))
wdists <- st_read(file.path(path_wdist, "users_final.shp"))

crs <- st_crs(clu)
crs_2 <- st_crs(wdists)
clu <- st_transform(clu, crs_2)

wdists <- wdists %>% 
  mutate(totAcres = as.numeric(st_area(.)) * m2_to_acre)

#check why geometries are invalid
if (1==0) {
  invalid_clu <- clu %>% 
    mutate(is_valid= st_is_valid(.)) %>%
    filter(is_valid!="TRUE")
  valid_clu <- lwgeom::st_make_valid(clu)
  
  invalid_wdist <- wdists %>% 
    mutate(is_valid= st_is_valid(.)) %>%
    filter(is_valid!="TRUE")
  valid_wdist <- lwgeom::st_make_valid(wdists)
}

#use st_intersect to get dataframe with x*p_x number of observations
inter <- st_intersection(lwgeom::st_make_valid(clu), lwgeom::st_make_valid(wdists))

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

## We're treating these water district boundaries as shrap, so no need to code up 
## (distance to) nearest polygon for unmatched CLUs

#export results
outfile <- full_join(inter, st_drop_geometry(clu_unmatched), by="CLU_ID")
file <- paste0(path_output,"/CLU_water_districts.csv")
write.csv(outfile, file, sep=",", row.names=FALSE, col.names=TRUE)


