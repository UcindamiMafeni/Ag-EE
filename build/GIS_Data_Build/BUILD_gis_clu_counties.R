# Created by Chinmay Lohani in November 2019
# Find the intersections between counties and CLUs
# Additional notes exist on validity of intersections dropped in Dbox "Chinmay_notes_spatial_Dec19.pdf"
##############################################################
#  Script to execute polygon-to-polygon merge: CLU to county #
############################################################## 

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

path_counties <- paste0(path,"data/spatial/Counties")
path_clu <- paste0(path,"data/cleaned_spatial/CLU/clu_poly")
path_output <- paste0(path,"data/misc")

#create sf objects for CLUs and counties
clu <- st_read(file.path(path_clu, "clu_poly.shp"))
counties <- st_read(file.path(path_counties, "CA_Counties_TIGER2016.shp"))

crs <- st_crs(clu)
crs_2 <- st_crs(counties)
clu <- st_transform(clu, crs_2)

counties <- counties %>% 
  mutate(totAcres = as.numeric(st_area(.)) * m2_to_acre)

#check why geometries are invalid
if (1==0) {
  invalid_clu <- clu %>% 
    mutate(is_valid= st_is_valid(.)) %>%
    filter(is_valid!="TRUE")
  valid_clu <- lwgeom::st_make_valid(clu)
  
  invalid_wdist <- counties %>% 
    mutate(is_valid= st_is_valid(.)) %>%
    filter(is_valid!="TRUE")
  valid_wdist <- lwgeom::st_make_valid(counties)
}

#use st_intersect to get dataframe with x*p_x number of observations
inter <- st_intersection(lwgeom::st_make_valid(clu), lwgeom::st_make_valid(counties))

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

#for unmatched CLUs, calculate distance to nearest county
for (i in 1:nrow(clu_unmatched)) {
  d_all <- st_distance(lwgeom::st_make_valid(clu_unmatched)[i,], lwgeom::st_make_valid(counties))
  d_min <- min(d_all)
  d_which <- which(d_min==d_all)
  d_nearest <- st_drop_geometry(counties[d_which,])
  d_out <- cbind(st_drop_geometry(clu_unmatched[i,]),d_min,d_nearest)
  if (i==1){
    out_nearest <- d_out
  }
  else {
    out_nearest <- rbind(out_nearest,d_out)  
  }
  print(i)
}  
summary(out_nearest$d_min)

#export results
outfile <- full_join(inter,out_nearest, by="CLU_ID")
file <- paste0(path_output,"/CLU_counties.csv")
write.csv(outfile, file, sep=",", row.names=FALSE, col.names=TRUE)

