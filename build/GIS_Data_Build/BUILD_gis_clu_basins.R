# Created by Chinmay Lohani in November 2019
# Find the intersections between basins and CLUs
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

if(Sys.getenv("USERNAME") == "clohani"){
  root_gh <- "C:/Users/clohani/OneDrive/Documents/github/Ag-EE"
}

m2_to_acre <- 0.000247105

path_counties <- "T:/Projects/Pump Data/data/spatial/Counties"
path_clu <- "T:/Projects/Pump Data/data/cleaned_spatial/CLU/clu_poly"
path_output <- "C:/Users/clohani/Desktop/Code_Chinmay"
path_basins <- "T:/Projects/Pump Data/data/spatial/CA_Bulletin_118_Groundwater_Basins"

#create sf objects for CLUs and basins
clu <- st_read(file.path(path_clu, "clu_poly.shp"))
basins <- st_read(file.path(path_basins, "CA_Bulletin_118_Groundwater_Basins.shp"))

crs <- st_crs(clu)
crs_2 <- st_crs(basins)
clu <- st_transform(clu, crs_2)

basins <- basins %>% 
  mutate(totAcres = as.numeric(st_area(.)) * m2_to_acre)

#check why geometries are invalid
if (1==0) {
  invalid_clu <- clu %>% 
    mutate(is_valid= st_is_valid(.)) %>%
    filter(is_valid!="TRUE")
  valid_clu <- lwgeom::st_make_valid(clu)
  
  invalid_wdist <- basins %>% 
    mutate(is_valid= st_is_valid(.)) %>%
    filter(is_valid!="TRUE")
  valid_wdist <- lwgeom::st_make_valid(basins)
}

#use st_intersect to get dataframe with x*p_x number of observations
inter <- st_intersection(lwgeom::st_make_valid(clu), lwgeom::st_make_valid(basins))

#find area of these things
#create a variable which sums total area of these things

inter <- inter %>% 
  mutate(IntAcres = as.numeric(st_area(.)) * m2_to_acre) %>%
  st_set_geometry(NULL) %>%
  group_by(CLU_ID) %>%
  mutate(tot_int_area= sum(IntAcres)) %>%
  ungroup

## save outputs
#saveRDS(inter, file.path(path_output, "CLU_Basins.RDS"))
## check if the total area of intersections is atleast as much as the CLU area
#inter %>%
#  verify(tot_area>CLUAcres)

# define a variable that tells whether intersected or not, take union with original, select largest intersected

union_clus <- inter %>%
  mutate(has_intersection=1) %>%
  bind_rows(valid_clu,.)

sliced_union_clus <- union_clus %>%
  group_by(CLU_ID) %>%
  arrange(desc(IntAcres), .by_group = TRUE) %>%
  slice(1)%>%
  ungroup()

# code for summarising things 

union_clus %>% 
  mutate(all=1) %>%
  group_by(all) %>%
  summarise(`1%`=quantile(frac_area, probs=0.01, na.rm = TRUE),
            `5%`=quantile(frac_area, probs=0.5, na.rm = TRUE),
            `10%`=quantile(frac_area, probs=0.10, na.rm = TRUE),
            `25%`=quantile(frac_area, probs=0.25, na.rm = TRUE),
            `50%`=quantile(frac_area, probs=0.5, na.rm = TRUE),
            `75%`=quantile(frac_area, probs=0.75, na.rm = TRUE),
            `90%`=quantile(frac_area, probs=0.9, na.rm = TRUE),
            `99%`=quantile(frac_area, probs=0.99, na.rm = TRUE),
            avg=mean(frac_area, na.rm = TRUE)
  )

saveRDS(union_clus, file.path(path_output, "Intersected_CLU_basin.rds"))