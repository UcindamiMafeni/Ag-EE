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

#define paths of IO objects
path_users_shp <- "T:/Projects/Pump Data/data/surface_water/ca_Final_shapefiles_Nick"
path_huc8_shp <- "T:/Projects/Pump Data/data/surface_water/ca_Final_shapefiles_Nick"
path_allocations <- "T:/Projects/Pump Data/data/surface_water/hagerty"
path_clu <- "T:/Projects/Pump Data/data/cleaned_spatial/CLU/clu_poly"
path_output <- "C:/Users/clohani/Desktop/Code_Chinmay"
path_outline <- "C:/Users/clohani/Desktop/Code_Chinmay/ca-state-boundary"
path_out <- "C:/Users/clohani/Desktop/Code_Chinmay/"

#get shapefiles, define constants
users_shp <- st_read(file.path(path_users_shp, "users_final.shp"))
dauco_shp <- st_read(file.path(path_users_shp, "dauco_final.shp"))
outline <- st_read(path_outline)
outline <- st_transform(outline,4326)
m2_to_acre <- 0.000247105
colors <- c( "#b6edf0",  "#81c0eb", "#4695e3", "#216bd1","#1c3ab0", "#0a0a91")

#get shape areas
users_shp <- users_shp %>%
  mutate(user_acres=as.numeric(st_area(.)) * m2_to_acre)

dauco_shp <- dauco_shp %>%
  mutate(dauco_acres=as.numeric(st_area(.)) * m2_to_acre)

#Get point users' and polygon users' allocations
allocations_dauco <- read_dta(file = file.path(path_allocations, "allocations_subset_pointusers.dta"))
allocations_polygon <- read_dta(file = file.path(path_allocations, "allocations_subset_polygonusers.dta"))

#collapse allocations to average and trim data, convert dauco allocations to sf object
allocations_dauco <- allocations_dauco %>%
  filter_at(vars(rights_pod_latitude,rights_pod_longitude),all_vars(!is.na(.))) %>%
  st_as_sf(coords=c("rights_pod_longitude","rights_pod_latitude"), crs = 4326) %>%
  st_join(.,dauco_shp, join=st_intersects) %>%
  filter(year>=2007 & year<=2018) %>%
  group_by(DAU_CODE,year) %>%
  summarise(vol_deliv_cy_ag= sum(vol_deliv_cy_ag),na.rm = TRUE) %>%
  ungroup(DAU_CODE,year) %>%
  group_by(DAU_CODE) %>%
  summarise(vol_deliv_cy_ag= mean(vol_deliv_cy_ag),na.rm = TRUE) %>%
  ungroup(DAU_CODE) %>%
  rename(dauco_alloc= vol_deliv_cy_ag) %>%
  distinct(.)

allocations_polygon <- allocations_polygon %>%
  filter(year>=2007 & year<=2018) %>%
  #mutate(vol_deliv_cy_ag = ifelse(is.na(vol_deliv_cy_ag), 0,vol_deliv_cy_ag)) %>%
  group_by(user_id,std_name) %>%
  summarise(vol_deliv_cy_ag= mean(vol_deliv_cy_ag),na.rm = TRUE) %>%
  ungroup() %>%
  select(user_id,std_name,vol_deliv_cy_ag) %>%
  rename(user_alloc= vol_deliv_cy_ag)

###

# Plot user shapefiles 

setwd(path_out)
plot_all <- ggplot() +
  geom_sf(data=outline,color="black", fill=NA, alpha=1, show.legend = FALSE) +
  geom_sf(data=users_shp, aes(color= "grey30"), lwd = 0) + 
  scale_fill_manual(values = colors) +
  theme_bw() +
  theme(plot.background = element_rect(fill='white'),
        panel.grid.major = element_line(colour = "transparent"),
        panel.grid.minor = element_line(colour = "transparent"),
        panel.border = element_blank(),
        plot.margin = margin(t=0,r=0,b=0,l=0)
  )
dev.off()
ggsave(file.path(path_out, "users_hag_shp.png"),dpi=300, plot = plot_all)

#### 

# Plot with just user allocations

wdist_wdata <- users_shp %>%
  inner_join(.,allocations_polygon) %>% 
  #filter(str_detect(std_name, "I.D.")) %>%
  mutate(w_by_acres= user_alloc/user_acres) %>%
  mutate(cat= cut(w_by_acres, breaks=c(-Inf,1,1.5,2,3,5,Inf)))

setwd(path_out)
plot_all <- ggplot() +
  geom_sf(data=outline,color="grey30", fill=NA, alpha=1, show.legend = FALSE) +
  geom_sf(data=wdist_wdata, aes(fill= cat), lwd = 0) + 
  scale_fill_manual(values = colors) +
  theme_bw() +
  theme(plot.background = element_rect(fill='white'),
        panel.grid.major = element_line(colour = "transparent"),
        panel.grid.minor = element_line(colour = "transparent"),
        panel.border = element_blank(),
        plot.margin = margin(t=0,r=0,b=0,l=0)
  )
dev.off()
ggsave(file.path(path_out, "huc8_none_allocations.png"),dpi=300, plot = plot_all)

# Plot with just dauco allocations

wdist_wdata <- allocations_dauco %>%
  mutate(w_by_acres= dauco_alloc/dauco_acres) %>%
  mutate(cat= cut(w_by_acres, breaks=c(-Inf,1,1.5,2,3,5,Inf)))

setwd(path_out)
plot_all <- ggplot() +
  geom_sf(data=outline,color="grey30", fill=NA, alpha=1, show.legend = FALSE) +
  geom_sf(data=wdist_wdata, aes(fill= cat), lwd = 0) + 
  scale_fill_manual(values = colors) +
  theme_bw() +
  theme(plot.background = element_rect(fill='white'),
        panel.grid.major = element_line(colour = "transparent"),
        panel.grid.minor = element_line(colour = "transparent"),
        panel.border = element_blank(),
        plot.margin = margin(t=0,r=0,b=0,l=0)
  )
dev.off()
ggsave(file.path(path_out, "dauco_only_allocations.png"),dpi=300, plot = plot_all)

### Code for intersections ###

#inter_clu_huc8 <- st_intersection(lwgeom::st_make_valid(users_shp), lwgeom::st_make_valid(huc8_shp))
#saveRDS(inter_clu_huc8, file.path(path_output, "HUC8_polygon_intersection.rds"))
#inter_users_dauco <- st_intersection(lwgeom::st_make_valid(users_shp), lwgeom::st_make_valid(allocations_dauco))
#saveRDS(inter_users_dauco, file.path(path_output, "dauco_polygon_intersection.rds"))
inter_users_dauco <- readRDS(file.path(path_output, "dauco_polygon_intersection.rds"))



# Plot of allocations with largest dauco allocation only

inter <- inter_users_dauco %>% 
  mutate(IntAcres = as.numeric(st_area(.)) * m2_to_acre) %>%
  st_set_geometry(NULL) 

clu_dta <-users_shp %>%
  st_set_geometry(NULL)

union_clus <- inter %>%
  mutate(has_intersection=1) %>%
  bind_rows(clu_dta,.)

#saves one obs for each user, the one with the largest huc8 intersection
largest_dauco_user_inter <- union_clus %>%
  group_by(user_id) %>%
  arrange(desc(IntAcres), .by_group = TRUE) %>%
  slice(1)%>%
  ungroup() %>%
  select(user_id,DAUCO_CODE,dauco_alloc,IntAcres,user_acres,dauco_acres)

#HUC8 allocations wtd by fraction of intersected area added
users_largest_alloc <- largest_dauco_user_inter %>%
  inner_join(.,allocations_polygon) %>%
  mutate(frac_area=IntAcres/dauco_acres) %>%
  mutate(user_alloc= frac_area*dauco_alloc + user_alloc) %>%
  mutate(user_alloc = ifelse(is.na(user_alloc), 0,user_alloc))

wdist_wdata <- users_shp %>%
  inner_join(.,users_largest_alloc) %>% 
  #filter(str_detect(std_name, "I.D.")) %>%
  mutate(w_by_acres= user_alloc/user_acres) %>%
  mutate(cat= cut(w_by_acres, breaks=c(-Inf,1,1.5,2,3,5,Inf)))

setwd(path_out)
plot_all <- ggplot() +
  geom_sf(data=outline,color="grey30", fill=NA, alpha=1, show.legend = FALSE) +
  geom_sf(data=wdist_wdata, aes(fill= cat), lwd = 0) + 
  scale_fill_manual(values = colors) +
  theme_bw() +
  theme(plot.background = element_rect(fill='white'),
        panel.grid.major = element_line(colour = "transparent"),
        panel.grid.minor = element_line(colour = "transparent"),
        panel.border = element_blank(),
        plot.margin = margin(t=0,r=0,b=0,l=0)
  )
dev.off()
ggsave(file.path(path_out, "dauco_largest_allocations.png"),dpi=300, plot = plot_all)


###################

# allocations with weighted sum of huc8 intersections

inter <- inter_users_dauco %>% 
  mutate(IntAcres = as.numeric(st_area(.)) * m2_to_acre) %>%
  st_set_geometry(NULL) 

#HUC8 allocations wtd by fraction of intersected area added
users_all_alloc <- inter %>%
  mutate(frac_area=IntAcres/dauco_acres) %>%
  mutate(inter_alloc= frac_area*dauco_alloc) %>%
  group_by(user_id) %>%
  arrange(desc(IntAcres), .by_group = TRUE) %>%
  mutate(inter_alloc=sum(inter_alloc)) %>%
  slice(1) %>%
  ungroup(user_id) %>%
  inner_join(.,allocations_polygon) %>%
  mutate(final_alloc= user_alloc + inter_alloc)

wdist_wdata <- users_shp %>%
  inner_join(.,users_all_alloc) %>% 
  #filter(str_detect(std_name, "I.D.")) %>%
  mutate(w_by_acres= final_alloc/user_acres) %>%
  mutate(cat= cut(w_by_acres, breaks=c(-Inf,1,1.5,2,3,5,Inf)))


setwd(path_out)
plot_all <- ggplot() +
  geom_sf(data=outline,color="grey30", fill=NA, alpha=1, show.legend = FALSE) +
  geom_sf(data=wdist_wdata, aes(fill= cat), lwd = 0) + 
  scale_fill_manual(values = colors) +
  theme_bw() +
  theme(plot.background = element_rect(fill='white'),
        panel.grid.major = element_line(colour = "transparent"),
        panel.grid.minor = element_line(colour = "transparent"),
        panel.border = element_blank(),
        plot.margin = margin(t=0,r=0,b=0,l=0)
  )
dev.off()
ggsave(file.path(path_out, "dauco_all_allocations.png"),dpi=300, plot = plot_all)


