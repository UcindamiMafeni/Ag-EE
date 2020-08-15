###############################################################################################
### Code to assign surface water to CLUs similar to Nick's exercise                         ###
### Input:  1. Polygon and Point user allocations from Nick                                 ###
###         2. CLU shapefile, PA shapefile and 2014 crop dataset downloaded                 ###
### Output: 1. Intermediate intersection files, crop plots in intermediate/CLU_Assignment   ###
###         2. CLU-year panel of surface water allocs in surface water/CLU_PA...Only.rds    ###
### Script choices:                                                                         ###
###  1. I use PAs to aggregate point users' water allocs (Nick certified)                   ###
###  2. 2014 california crop areas are used to get cropping information                     ###
###  3. I only count a CLU drawing from a user shapefile if it has >0.5 of its area, seems fine
###    since most CLUs, when accounting for cropped areas, have large intersections w Users ###
###  4. Codeblocks with heavy intersection tasks are commented out, and their data is saved ###
###    in intermediate/CLU_Assignment. Different modelling choices (choice of shapefiles)   ###
###    will require a redoing of these blocks                                               ###
###############################################################################################

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
path_clu <- "T:/Projects/Pump Data/data/cleaned_spatial/CLU/clu_poly"
path_output <- "T:/Projects/Pump Data/data/surface_water"
path_outline <- "T:/Projects/Pump Data/data/surface_water/ca-state-boundary"
path_crop <- "T:/Projects/Pump Data/data/surface_water/Crop__Mapping_2014-shp"
path_inter <- "T:/Projects/Pump Data/data/intermediate/CLU_Assignment"
path_out <- "T:/Projects/Pump Data/data/surface_water"

#get shapefiles, define constants
users_shp <- st_read(file.path(path_users_shp, "users_positive_alloc_filtered.shp"))
pa_shp <- st_read(file.path(path_pa, "Water_Plan_Planning_Areas.shp"))
crop_shp <- st_read(file.path(path_crop,"Crop__Mapping_2014.shp")) %>% st_transform(.,4326)
outline <- st_read(file.path(path_outline, "CA_State_TIGER2016.shp")) %>%
 st_transform(.,4326)
m2_to_acre <- 0.000247105
colors <- c( "#b6edf0",  "#81c0eb", "#4695e3", "#216bd1","#1c3ab0", "#0a0a91")

#get shape areas
users_shp <- users_shp %>%
  mutate(user_acres=as.numeric(st_area(.)) * m2_to_acre) 


pa_shp <- pa_shp %>%
  mutate(pa_acres=as.numeric(st_area(.)) * m2_to_acre)

area_users <- users_shp %>%
  mutate(areaAcres = as.numeric(st_area(.)) * m2_to_acre) %>%
  select(user_id,areaAcres) %>%
  st_set_geometry(NULL)
  
crop_shp <- crop_shp %>%
  filter(!(Crop2014=="Idle" | Crop2014=="Urban")) %>%
  select(geometry) 

clu <- st_read(file.path(path_clu, "clu_poly.shp"))
clu <- clu %>% 
  mutate(totAcres = as.numeric(st_area(.)) * m2_to_acre)


#Get point users' and polygon users' allocations

allocations_polygon <- read_dta(file = file.path(path_allocations, "allocations_subset_polygonusers.dta")) %>%
  group_by(std_name, year, user_id) %>%
  replace(is.na(.), 0) %>%
  summarise(vol_deliv_cy_ag= sum(vol_deliv_cy_ag)) %>%
  select(year,user_id,std_name,vol_deliv_cy_ag) %>%
  distinct(.) %>%
  ungroup() %>%
  select(user_id,year,vol_deliv_cy_ag) %>%
  rename(user_alloc=vol_deliv_cy_ag)

users_relevant <- users_shp %>%
  st_set_geometry(NULL) %>%
  select(user_id) %>%
  mutate(zeroes=0)

allocations_pa <- read_dta(file = file.path(path_allocations, "allocations_subset_pointusers.dta"))
pa_sp <- as_Spatial(pa_shp)
#Feed point users' allocations to the PAs (Planning area shapefiles)
allocations_pa <- allocations_pa %>%
  mutate(rights_pod_latitude= ifelse(is.na(rights_pod_latitude), lat_manual,rights_pod_latitude)) %>%
  mutate(rights_pod_longitude= ifelse(is.na(rights_pod_longitude), lat_manual,rights_pod_longitude)) %>%
  filter_at(vars(rights_pod_latitude,rights_pod_longitude),all_vars(!is.na(.))) %>%
  st_as_sf(coords=c("rights_pod_longitude","rights_pod_latitude"), crs = 4326) %>%
  as_Spatial(.) 

allocations_pa@data$Polygon <- over(allocations_pa,pa_sp)
allocations_pa <- st_as_sf(allocations_pa)
allocations_pa$PA_NO <- allocations_pa$Polygon$PA_NO
allocations_pa$pa_acres <- allocations_pa$Polygon$pa_acres
allocations_pa$Polygon <- NA

allocations_pa<- allocations_pa %>%
  st_set_geometry(NULL) %>%
  select(year,vol_deliv_cy_ag,PA_NO) %>%
  filter(year>=2007) %>%
  group_by(PA_NO,year) %>%
  summarise(vol_deliv_cy_ag= sum(vol_deliv_cy_ag),na.rm = TRUE) %>%
  ungroup(PA_NO,year) %>%
  rename(pa_alloc= vol_deliv_cy_ag) %>%
  distinct(.) %>%
  select(PA_NO,pa_alloc,year)

if (1==0) {
### Intersect PAs with cropped areas to get relevant segments

inter_pa_crop <- st_intersection(lwgeom::st_make_valid(pa_shp), lwgeom::st_make_valid(crop_shp)) 
saveRDS(inter_pa_crop, file.path(path_inter, "PA_Crop.rds"))
} #This code gives us intersection bw PA allocation shape and 2014 crop shapefile

inter_pa_crop <- readRDS(file.path(path_inter, "PA_Crop.rds")) 

if (1==0) {
inter_user_crop <- st_intersection(lwgeom::st_make_valid(users_shp), lwgeom::st_make_valid(crop_shp)) 
saveRDS(inter_user_crop, file.path(path_inter, "User_Crop.rds"))
} #Codeblock for intersecting user polygons and 2014 crops

inter_user_crop <- readRDS(file.path(path_inter, "User_Crop.rds"))

#find intersection between CLUs and user shapefiles
if (1==0) {
inter_cluPoly <- st_intersection(lwgeom::st_make_valid(clu), lwgeom::st_make_valid(inter_user_crop))
saveRDS(inter_cluPoly, file.path(path_inter, "CLU_polygon_intersection_cropcrctd.rds"))
}
inter_cluPoly <- read_rds(file.path(path_inter, "CLU_polygon_intersection_cropcrctd.rds")) %>%
  left_join(.,users_relevant) %>%
  filter(!is.null(zeroes))

#find intersections between CLUs and PAs.
if (1==0) {
inter <- st_intersection(lwgeom::st_make_valid(clu), lwgeom::st_make_valid(inter_pa_crop))
saveRDS(inter, file.path(path_inter, "CLU_PA_intersection_cropcrctd.rds"))
}
inter_clu_pa <- read_rds(file.path(path_inter, "CLU_PA_intersection_cropcrctd.rds"))

#calculate CLU area that is cropped
if (1==0) {
inter_clu_crop <- st_intersection(lwgeom::st_make_valid(clu), lwgeom::st_make_valid(crop_shp)) 
saveRDS(inter_clu_crop, file.path(path_inter, "CLU_Crop_intersection.rds"))

inter_clu_crop_area <- inter_clu_crop %>%
  mutate(CropCLU= as.numeric(st_area(.)) * m2_to_acre) %>%
  st_set_geometry(NULL) %>%
  select(CLU_ID,CropCLU) %>%
  group_by(CLU_ID)%>%
  mutate(CropCLU= sum(CropCLU))%>%
  slice(1)%>%
  ungroup()
  
saveRDS(inter_clu_crop_area, file.path(path_inter, "CLU_Crop_intersection_area.rds"))
}
inter_clu_crop <- read_rds(file.path(path_inter, "CLU_Crop_intersection_area.rds"))

#### Assignment of water off of the user-polygon allocations

inter <- inter_cluPoly %>% 
  mutate(IntAcres = as.numeric(st_area(.)) * m2_to_acre) %>%
  st_set_geometry(NULL) %>%
  group_by(CLU_ID,user_id) %>%
  mutate(IntAcres=sum(IntAcres)) %>%
  slice(1) %>%
  ungroup() %>%
  select(IntAcres,CLU_ID,user_id) %>%
  left_join(.,inter_clu_crop) %>%
  filter((IntAcres/CropCLU)>=0.5) %>%
  select(CLU_ID,user_id,IntAcres)

area_user_crop <- inter_user_crop %>%
  select(user_id,geometry) %>%
  mutate(tot_user_acres= as.numeric(st_area(.)) * m2_to_acre) %>%
  st_set_geometry(NULL) %>%
  group_by(user_id) %>%
  mutate(tot_user_acres= sum(tot_user_acres)) %>%
  slice(1) %>%
  ungroup() 

CLU_user_alloc <- inter %>%
  left_join(.,area_user_crop) %>%
  left_join(.,allocations_polygon) %>%
  group_by(CLU_ID,user_id,year) %>%
  mutate(polygon_alloc= user_alloc * IntAcres/tot_user_acres) %>%
  ungroup() %>%
  group_by(CLU_ID,year) %>%
  mutate(polygon_alloc= sum(polygon_alloc)) %>%
  slice(1) %>%
  ungroup() %>%
  select(CLU_ID,year,polygon_alloc) %>%
  mutate(polygon_alloc= ifelse(is.na(polygon_alloc),0,polygon_alloc))

#### Assignment of water off of the PA allocations 

area_pa_crop <- inter_pa_crop %>%
  select(PA_NO,geometry) %>%
  mutate(tot_pa_acres= as.numeric(st_area(.)) * m2_to_acre) %>%
  st_set_geometry(NULL) %>%
  group_by(PA_NO) %>%
  mutate(tot_pa_acres= sum(tot_pa_acres)) %>%
  slice(1) %>%
  ungroup()

inter <- inter_clu_pa %>% 
  mutate(IntAcres = as.numeric(st_area(.)) * m2_to_acre) %>%
  st_set_geometry(NULL) %>%
  group_by(CLU_ID,PA_NO) %>%
  mutate(IntAcres=sum(IntAcres)) %>%
  slice(1) %>%
  ungroup() %>%
  select(CLU_ID,PA_NO,IntAcres)

CLU_pa_alloc <- inter %>%
  inner_join(.,area_pa_crop) %>%
  inner_join(.,allocations_pa) %>%
  group_by(CLU_ID,PA_NO,year) %>%
  mutate(pa_alloc= pa_alloc * IntAcres/tot_pa_acres) %>%
  ungroup() %>%
  group_by(CLU_ID,year) %>%
  mutate(pa_alloc= sum(pa_alloc)) %>%
  mutate(clu_area= sum(IntAcres)) %>%  #given PAs cover the region, this will give total CLU_area that is cropped
  slice(1) %>%
  ungroup() %>%
  select(CLU_ID,year,pa_alloc,clu_area) %>%
  mutate(pa_alloc= ifelse(is.na(pa_alloc),0,pa_alloc))

###########################

CLU_allocation <- CLU_user_alloc %>%
  inner_join(.,CLU_pa_alloc) %>%
  mutate(surface_alloc = pa_alloc+ polygon_alloc)
saveRDS(CLU_allocation, file.path(path_output, "CLU_PA_cropctd_assignment_majorOnly.rds"))


if (1==0) {
for(i in 2009:2018) {
  clu_allocations_yr <- CLU_allocation %>%
    filter(year==i) %>%
    group_by(CLU_ID) %>%
    inner_join(clu,.)%>%
    ungroup()
  
  wdist_wdata <- clu_allocations_yr %>% 
    mutate(w_by_acres= as.numeric(surface_alloc)/clu_area) %>%
    mutate(cat= cut(w_by_acres, breaks=c(-Inf,1,1.5,2,3,5,Inf)))
  
  setwd(path_out)
  plot_all <- ggplot() +
    geom_sf(data=outline,color="grey30", fill=NA, alpha=1, show.legend = FALSE) +
    geom_sf(data=wdist_wdata, aes(fill= cat), color = NA, lwd = 0) + 
    scale_fill_manual(values = colors) +
    theme_bw() +
    theme(plot.background = element_rect(fill='white'),
          panel.grid.major = element_line(colour = "transparent"),
          panel.grid.minor = element_line(colour = "transparent"),
          panel.border = element_blank(),
          plot.margin = margin(t=0,r=0,b=0,l=0)
    )
 # dev.off()
  ggsave(file.path(path_inter, paste("Clu_alloc_pa_cropctd_majorOnly_",i,".png")),dpi=300, plot = plot_all)
  
}
} # codeblock for plotting surface water allocations by year