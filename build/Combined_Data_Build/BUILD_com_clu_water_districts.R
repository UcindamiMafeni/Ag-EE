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
path_wdist <- paste0(path,"data/surface_water/")
path_clu <- paste0(path,"data/cleaned_spatial/CLU/clu_poly")
path_output <- paste0(path,"data/misc")

#create sf objects for CLUs and water districts
clu <- st_read(file.path(path_clu, "clu_poly.shp"))
wdists_all <- st_read(file.path(path_wdist, "ca_Final_shapefiles_Nick/users_final.shp"))
wdists_pos <- st_read(file.path(path_wdist, "Subsetted_User_SF/users_positive_alloc.shp"))
wdists_hag <- st_read(file.path(path_wdist, "Subsetted_User_SF/users_positive_alloc_filtered.shp"))
  # this 3rd one is the one that we want, tagged "Hag" because it's closest to Nick Hagerty's set of polygons!

crs <- st_crs(clu)
crs_2 <- st_crs(wdists_all)
wdists_pos <- st_transform(wdists_pos, crs_2)
wdists_hag <- st_transform(wdists_hag, crs_2)
clu <- st_transform(clu, crs_2)

wdists_all <- wdists_all %>% mutate(totAcres = as.numeric(st_area(.)) * m2_to_acre)
wdists_pos <- wdists_pos %>% mutate(totAcres = as.numeric(st_area(.)) * m2_to_acre)
wdists_hag <- wdists_hag %>% mutate(totAcres = as.numeric(st_area(.)) * m2_to_acre)

#use st_intersect to get dataframe with x*p_x number of observations
inter_all <- st_intersection(lwgeom::st_make_valid(clu), lwgeom::st_make_valid(wdists_all))
inter_pos <- st_intersection(lwgeom::st_make_valid(clu), lwgeom::st_make_valid(wdists_pos))
inter_hag <- st_intersection(lwgeom::st_make_valid(clu), lwgeom::st_make_valid(wdists_hag))

#find area of these things
#create a variable which sums total area of these things
inter_all <- inter_all %>% 
  mutate(IntAcres = as.numeric(st_area(.)) * m2_to_acre) %>%
  st_set_geometry(NULL) %>%
  group_by(CLU_ID) %>%
  mutate(tot_int_area= sum(IntAcres)) %>%
  ungroup
inter_pos <- inter_pos %>% 
  mutate(IntAcres = as.numeric(st_area(.)) * m2_to_acre) %>%
  st_set_geometry(NULL) %>%
  group_by(CLU_ID) %>%
  mutate(tot_int_area= sum(IntAcres)) %>%
  ungroup
inter_hag <- inter_hag %>% 
  mutate(IntAcres = as.numeric(st_area(.)) * m2_to_acre) %>%
  st_set_geometry(NULL) %>%
  group_by(CLU_ID) %>%
  mutate(tot_int_area= sum(IntAcres)) %>%
  ungroup

#subset CLUs to isolate non-matches
clu_matched_all <- as.data.frame(inter_all$CLU_ID)
names(clu_matched_all) <- "CLU_ID"
clu_unmatched_all <- anti_join(clu, clu_matched_all, by="CLU_ID")

clu_matched_pos <- as.data.frame(inter_pos$CLU_ID)
names(clu_matched_pos) <- "CLU_ID"
clu_unmatched_pos <- anti_join(clu, clu_matched_pos, by="CLU_ID")

clu_matched_hag <- as.data.frame(inter_hag$CLU_ID)
names(clu_matched_hag) <- "CLU_ID"
clu_unmatched_hag <- anti_join(clu, clu_matched_hag, by="CLU_ID")

## We're treating these water district boundaries as shrap, so no need to code up 
## (distance to) nearest polygon for unmatched CLUs

#export results
outfile_all <- full_join(inter_all, st_drop_geometry(clu_unmatched_all), by="CLU_ID")
file_all <- paste0(path_output,"/CLU_water_districts_all.csv")
write.csv(outfile_all, file_all, sep=",", row.names=FALSE, col.names=TRUE)

outfile_pos <- full_join(inter_pos, st_drop_geometry(clu_unmatched_pos), by="CLU_ID")
file_pos <- paste0(path_output,"/CLU_water_districts_pos.csv")
write.csv(outfile_pos, file_pos, sep=",", row.names=FALSE, col.names=TRUE)

outfile_hag <- full_join(inter_hag, st_drop_geometry(clu_unmatched_hag), by="CLU_ID")
file_hag <- paste0(path_output,"/CLU_water_districts_hag.csv")
write.csv(outfile_hag, file_hag, sep=",", row.names=FALSE, col.names=TRUE)


#export attributes from both water district shapefiles
data_all <- st_drop_geometry(wdists_all)
file_all <- paste0(path_output,"/water_districts_all.csv")
write.csv(data_all, file_all, sep=",", row.names=FALSE, col.names=TRUE)

data_pos <- st_drop_geometry(wdists_pos)
file_pos <- paste0(path_output,"/water_districts_pos.csv")
write.csv(data_pos, file_pos, sep=",", row.names=FALSE, col.names=TRUE)

data_hag <- st_drop_geometry(wdists_hag)
file_hag <- paste0(path_output,"/water_districts_hag.csv")
write.csv(data_hag, file_hag, sep=",", row.names=FALSE, col.names=TRUE)
