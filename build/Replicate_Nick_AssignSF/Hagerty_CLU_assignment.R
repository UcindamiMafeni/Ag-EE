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

path_users_shp <- "T:/Projects/Pump Data/data/surface_water/ca_Final_shapefiles_Nick"
path_huc8_shp <- "T:/Projects/Pump Data/data/surface_water/ca_Final_shapefiles_Nick"
#path_huc8_shp <- "C:/Users/clohani/Desktop/Code_Chinmay/ca_Final_shapefiles_Nick"
path_allocations <- "T:/Projects/Pump Data/data/surface_water/hagerty"
path_clu <- "T:/Projects/Pump Data/data/cleaned_spatial/CLU/clu_poly"
path_output <- "C:/Users/clohani/Desktop/Code_Chinmay"

#get shapefiles
users_shp <- st_read(file.path(path_users_shp, "users_final.shp"))
huc8_shp <- st_read(file.path(path_users_shp, "huc8_final.shp"))


#Get point users' and polygon users' allocations
allocations_huc8 <- read_dta(file = file.path(path_allocations, "allocations_subset_pointusers.dta"))
allocations_polygon <- read_dta(file = file.path(path_allocations, "allocations_subset_polygonusers.dta"))

#Feed point users' allocations to the huc8
allocations_huc8 <- allocations_huc8 %>%
  group_by(huc8, year) %>%
  replace(is.na(.), 0) %>%
  summarise(vol_deliv_cy_ag= sum(vol_deliv_cy_ag)) %>%
  select(year,huc8,vol_deliv_cy_ag) %>%
  rename(HUC_8 = huc8) %>%
  ungroup(HUC_8) %>%
  mutate(HUC_8=as.numeric(HUC_8)) %>%
  mutate(HUC_8= factor(HUC_8)) %>%
  distinct(.)

#Feed polygon users' allocations to the user shapefiles

allocations_polygon <- allocations_polygon %>%
  group_by(std_name, year, user_id) %>%
  replace(is.na(.), 0) %>%
  summarise(vol_deliv_cy_ag= sum(vol_deliv_cy_ag)) %>%
  select(year,user_id,std_name,vol_deliv_cy_ag) %>%
  distinct(.)

#read in CLU shapefiles
m2_to_acre <- 0.000247105
clu <- st_read(file.path(path_clu, "clu_poly.shp"))
clu <- clu %>% 
  mutate(totAcres = as.numeric(st_area(.)) * m2_to_acre)

#find intersection between CLUs and user shapefiles
#inter <- st_intersection(lwgeom::st_make_valid(clu), lwgeom::st_make_valid(users_shp))
#saveRDS(inter, file.path(path_output, "CLU_polygon_intersection.rds"))
inter <- read_rds(file.path(path_output, "CLU_polygon_intersection.rds"))

inter <- inter %>% 
  mutate(IntAcres = as.numeric(st_area(.)) * m2_to_acre) %>%
  st_set_geometry(NULL) %>%
  mutate(frac_area=IntAcres/totAcres)

clu_dta <-clu %>%
  st_set_geometry(NULL)

union_clus <- inter %>%
  mutate(has_intersection=1) %>%
  bind_rows(clu_dta,.)

largest_CLU_user_inter <- union_clus %>%
  group_by(CLU_ID) %>%
  arrange(desc(IntAcres), .by_group = TRUE) %>%
  slice(1)%>%
  ungroup()

#allocate based on this

area_users <- users_shp %>%
  mutate(areaAcres = as.numeric(st_area(.)) * m2_to_acre) %>%
  select(user_id,areaAcres) %>%
  st_set_geometry(NULL)

area_huc8 <- huc8_shp %>%
  mutate(areaAcres = as.numeric(st_area(.)) * m2_to_acre) %>%
  select(HUC_8,areaAcres) %>%
  st_set_geometry(NULL)
  

users_shp_alloc <- largest_CLU_user_inter %>%
  inner_join(.,allocations_polygon) %>%
  inner_join(.,area_users) %>%
  mutate(frac_area=IntAcres/areaAcres) %>%
  mutate(clu_alloc= frac_area*vol_deliv_cy_ag) %>%
  replace(is.na(.), 0)
saveRDS(users_shp_alloc, file.path(path_output, "CLU_polygon_assignment.rds"))

#find intersections between CLUs and huc8s. allocate based on this
#inter <- st_intersection(lwgeom::st_make_valid(clu), lwgeom::st_make_valid(huc8_shp))
#saveRDS(inter, file.path(path_output, "CLU_huc8_intersection.rds"))
inter <- read_rds(file.path(path_output, "CLU_huc8_intersection.rds"))

inter <- inter %>% 
  mutate(IntAcres = as.numeric(st_area(.)) * m2_to_acre) %>%
  st_set_geometry(NULL) %>%
  mutate(frac_area=IntAcres/totAcres)

clu_dta <-clu %>%
  st_set_geometry(NULL)

union_clus <- inter %>%
  mutate(has_intersection=1) %>%
  bind_rows(clu_dta,.)

largest_CLU_huc8_inter <- union_clus %>%
  group_by(CLU_ID) %>%
  arrange(desc(IntAcres), .by_group = TRUE) %>%
  slice(1) %>%
  ungroup()

#allocate based on this

huc8_shp_alloc <- largest_CLU_huc8_inter %>%
  inner_join(.,allocations_huc8) %>%
  inner_join(.,area_huc8) %>%
  mutate(frac_area=IntAcres/areaAcres) %>%
  mutate(clu_alloc= frac_area*vol_deliv_cy_ag) %>%
  replace(is.na(.), 0)
saveRDS(huc8_shp_alloc, file.path(path_output, "CLU_huc8_assignment.rds"))


#add two allocations and plot

#collapse allocations to CLU_ID level
huc8_shp_alloc_skim <- huc8_shp_alloc %>%
  select(year,CLU_ID,clu_alloc) %>%
  group_by(year,CLU_ID) %>%
  summarise(clu_alloc= sum(clu_alloc)) %>%
  distinct(.)

users_shp_alloc_skim <- users_shp_alloc %>%
  select(year,CLU_ID,clu_alloc) %>%
  group_by(year,CLU_ID) %>%
  summarise(clu_alloc= sum(clu_alloc)) %>%
  distinct(.)

clu_allocations <- huc8_shp_alloc_skim %>%
  bind_rows(.,users_shp_alloc_skim) %>%
  group_by(year,CLU_ID) %>%
  summarise(clu_alloc= sum(clu_alloc)) %>%
  distinct(.)

clu_allocations_mean_shp <- clu_allocations %>%
  filter(year>=2007 | year<=2018) %>%
  group_by(CLU_ID) %>%
  summarise(clu_alloc= mean(clu_alloc)) %>%
  inner_join(clu,.)


path_outline <- "C:/Users/clohani/Desktop/Code_Chinmay/ca-state-boundary"
path_out <- "C:/Users/clohani/Desktop/Code_Chinmay/"
outline <- st_read(path_outline)

wdist_wdata <- clu_allocations_mean_shp %>% 
  mutate(w_by_acres= as.numeric(clu_alloc)/totAcres) %>%
  mutate(cat= cut(w_by_acres, breaks=c(-Inf,1,1.5,2,3,5,Inf)))

small_plot <- wdist_wdata %>%
  slice(1:1000)

colors <- c( "lightcyan1",  "lightblue1", "lightblue3", "steelblue3","dodgerblue2", "dodgerblue4")

outline <- st_transform(outline,4326)

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
dev.off()
ggsave(file.path(path_out, "Clu_all.png"),dpi=300, plot = plot_all)


setwd(path_out)
plot_all <- ggplot() +
  geom_sf(data=outline,color="grey30", fill=NA, alpha=1, show.legend = FALSE) +
  geom_sf(data=clu, aes(fill= "steelblue3"), color = NA, lwd = 0) + 
  scale_fill_manual(values = colors) +
  theme_bw() +
  theme(plot.background = element_rect(fill='white'),
        panel.grid.major = element_line(colour = "transparent"),
        panel.grid.minor = element_line(colour = "transparent"),
        panel.border = element_blank(),
        plot.margin = margin(t=0,r=0,b=0,l=0)
  )
dev.off()
ggsave(file.path(path_out, "Clu_areas.png"),dpi=300, plot = plot_all)

for(i in 2007:2018) {
  clu_allocations_yr <- clu_allocations %>%
    filter(year==i) %>%
    group_by(CLU_ID) %>%
    summarise(clu_alloc= mean(clu_alloc)) %>%
    inner_join(clu,.)
  
  wdist_wdata <- clu_allocations_yr %>% 
    mutate(w_by_acres= as.numeric(clu_alloc)/totAcres) %>%
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
  dev.off()
  ggsave(file.path(path_out, paste("Clu_alloc_",i,".png")),dpi=300, plot = plot_all)
  
}

for(i in 2007:2018) {
  clu_allocations_yr <- clu_allocations %>%
    filter(year==i) %>%
    group_by(CLU_ID) %>%
    summarise(clu_alloc= mean(clu_alloc)) %>%
    inner_join(clu,.)
  
  saveRDS(clu_allocations_yr,file=file.path(path_out, paste("Clu_alloc_",i,".rds")))
  
  clu_alloc_dta <-clu_allocations_yr %>%
    st_set_geometry(NULL)
  
  write.csv(clu_alloc_dta, file=file.path(path_out, paste("Clu_alloc_",i,".csv")) )
}