##########################################################################
### Code to subset User shapefiles to a smaller relevant set           ###
### Input: Nick's list of User shapes from /surface_water/hagerty      ###
### Output: Filtered shapefile resting in /surface_water/Subsetted..SF ###
##########################################################################


rm(list = ls())

library(ggmap) #, lib.loc=libP)
library(ggplot2) #, lib.loc=libP)
library(gstat) #, lib.loc=libP)
library(sp) #, lib.loc=libP)
library(maptools) #, lib.loc=libP)
library(rgdal) #, lib.loc=libP)
library(rgeos) #, lib.loc=libP)
library(raster) #, lib.loc=libP)
library(SDMTools) #, lib.loc=libP)
library(sf)
library(tidyverse)
library(foreign)
library(haven)
library(stringr)

path_users_shp <- "T:/Projects/Pump Data/data/surface_water/ca_Final_shapefiles_Nick"
path_allocations <- "T:/Projects/Pump Data/data/surface_water/hagerty"
path_out <- "T:/Projects/Pump Data/data/surface_water/Subsetted_User_SF"
path_output <- "C:/Users/clohani/Desktop/Code_Chinmay"

m2_to_km2 <- 0.000001

users_shp <-st_read(file.path(path_users_shp, "users_final.shp")) %>%
  st_zm()

users_relevant <- read_dta(file = file.path(path_allocations, "allocations_subset_polygonusers.dta")) %>%
  select(user_id,std_name,vol_maximum_ag) %>% 
  mutate(vol_maximum_ag= ifelse(is.na(vol_maximum_ag),0,vol_maximum_ag)) %>%
  mutate(vol_maximum_ag= max(vol_maximum_ag)) %>%
  distinct(user_id,std_name,vol_maximum_ag) #%>%
  #mutate(exists=1)

#input user shapes with intersected (2014) crop area
users_crop_filtered <- readRDS(file.path(path_output, "User_Crop.rds")) %>%
  select(user_id,geometry) %>%
  mutate(tot_user_m2= as.numeric(st_area(.)) * m2_to_km2) %>%
  st_set_geometry(NULL) %>%
  group_by(user_id) %>%
  mutate(tot_user_m2= sum(tot_user_m2)) %>%
  slice(1) %>%
  ungroup() %>%
  filter(tot_user_m2>=10) %>% #filtered to 627 here
  select(user_id) %>%
  right_join(users_shp,.) %>%
  left_join(.,users_relevant) %>%
  filter(!str_detect(std_name,"CITY OF")) %>% #175 here
  filter(vol_maximum_ag> 1) %>% #175 finally 
  select(user_id) %>%
  st_set_geometry(NULL) 


###
users_shp_2 <- users_shp %>%
  st_zm() %>%
  right_join(.,users_relevant) 

users_shp_3 <- users_shp %>%
  st_zm() %>%
  right_join(.,users_crop_filtered) 

plot <- ggplot() + geom_sf(data=users_shp_2)
ggsave(file.path(path_output, "Filtered_Nick_Shapes.png"),dpi=300, plot = plot)

st_write(users_shp_2, file.path(path_out, "users_positive_alloc.shp"),delete_dsn=TRUE)
st_write(users_shp_3, file.path(path_out, "users_positive_alloc_filtered.shp"),delete_dsn=TRUE)

# plot allocations also
