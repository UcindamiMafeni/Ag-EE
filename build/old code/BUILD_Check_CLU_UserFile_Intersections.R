###################################################################################
### Code to create plot exhibits of CLU intersections w User files              ###
### Input: Intermediate intersection files in data/intermediate/CLU_Assignment  ###
### Output: CLUs plotted w colors based on their level of intersection w user   ###
###################################################################################

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

path_users_shp <- "T:/Projects/Pump Data/data/surface_water/Subsetted_User_SF"
path_pa <- "T:/Projects/Pump Data/data/surface_water/Water_Plan_Planning_Areas-shp"
path_allocations <- "T:/Projects/Pump Data/data/surface_water/hagerty"
#path_clu <- "T:/Projects/Pump Data/data/cleaned_spatial/CLU/clu_poly"
path_inter <- "T:/Projects/Pump Data/data/intermediate/CLU_Assignment"
path_outline <- "T:/Projects/Pump Data/data/surface_water/ca-state-boundary"
path_crop <- "T:/Projects/Pump Data/data/surface_water/Crop__Mapping_2014-shp"
#path_out <- "C:/Users/clohani/Desktop/Code_Chinmay"

#get shapefiles, define constants
users_shp <- st_read(file.path(path_users_shp, "users_positive_alloc_filtered.shp"))
pa_shp <- st_read(file.path(path_pa, "Water_Plan_Planning_Areas.shp"))
crop_shp <- st_read(file.path(path_crop,"Crop__Mapping_2014.shp")) %>% st_transform(.,4326)
outline <- st_read(file.path(path_outline, "CA_State_TIGER2016.shp")) %>%
  st_transform(.,4326)
m2_to_acre <- 0.000247105
colors <- c( "#b6edf0",  "#81c0eb", "#4695e3", "#216bd1","#1c3ab0", "#0a0a91")

inter_user_crop <- readRDS(file.path(path_inter, "User_Crop.rds"))
# choose a user shapefile

#set.seed(1) for exhibit 1
#set.seed(12) for exhibit 2
set.seed(123)
r_sample <- users_shp %>% select(user_id)  %>% 
  st_set_geometry(NULL) %>% distinct() %>% sample_n(1) %>%
  unlist(use.names = FALSE)

subset_user <- inter_user_crop %>% 
  filter(user_id %in% r_sample) %>%
  select(user_id,geometry) 
  
# restrict to all the CLUs that intersect with the user file
clu_rows <- st_intersects(lwgeom::st_make_valid(inter_clu_crop), lwgeom::st_make_valid(subset_user),sparse=TRUE) %>%
  as.data.frame() %>%
  select(row.id) %>%
  unlist(use.names = FALSE)

clu_subset <- inter_clu_crop %>%
  mutate(id = row_number()) %>%
  filter(id %in% clu_rows) %>%
  mutate(totAcres= as.numeric(st_area(.))*m2_to_acre) %>%
  group_by(CLU_ID) %>%
  mutate(totAcres= sum(totAcres)) %>%
  ungroup()
  
# find intersection and colour code
clu_percentage <- st_intersection(lwgeom::st_make_valid(clu_subset),lwgeom::st_make_valid(subset_user)) %>%
  mutate(intAcres= as.numeric(st_area(.))*m2_to_acre) %>%
  st_set_geometry(NULL) %>%
  select(CLU_ID,intAcres,totAcres) %>%
  group_by(CLU_ID) %>%
  mutate(intAcres= sum(intAcres)) %>%
  slice(1) %>%
  ungroup() %>%
  mutate(fracAcres= intAcres/totAcres) %>%
  select(CLU_ID,fracAcres)

clu_subset <- left_join(clu_subset,clu_percentage)

cls <- c("#f7fbff","#deebf7","#c6dbef","#9ecae1","#6baed6","#4292c6","#2171b5","#08519c","#08306b")

plot_all <- ggplot() +
  #geom_sf(data=subset_user) +
  geom_sf(data= clu_subset,aes(fill=fracAcres), colour=NA) +
  scale_fill_gradientn(colours = cls,space = "Lab")

ggsave(file.path(path_inter, "Exhibit_3.jpg"),dpi=400, plot = plot_all)
# fin