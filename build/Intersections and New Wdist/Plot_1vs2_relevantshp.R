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

path_wdis <- "C:/Users/clohani/Dropbox/Water_Districts/Spatial"
path_out <- "C:/Users/clohani/Dropbox/New_Water_Districts"
path_marker <- "C:/Users/clohani/OneDrive/Desktop"
path_outline <- "C:/Users/clohani/Downloads/ca-state-boundary"

#Read shapefiles
#Load Water Districts shapefile
#wdist <- readOGR(dsn = "Water_Districts.shp", layer = "Water_Districts")
wdist <- st_read(file.path(path_wdis, "Water_Districts.shp"))
outline <- st_read(file.path(path_outline, "CA_State_TIGER2016.shp"))
wdist <- wdist %>%
    mutate(user=str_to_upper(AGENCYNAME))
marker_dta <- read.delim2(file.path(path_marker, "Crosswalk_shpfile.csv"),header=TRUE,sep=",",stringsAsFactors=FALSE)
marker_dta <- marker_dta %>%
    mutate(marker=1)
wdist <- left_join(wdist,marker_dta,by = "user")
wdist <- wdist %>% mutate(marker = replace(marker, is.na(marker), 0))
wdist_present <- wdist %>% filter(marker==1)

plot_filtered <- ggplot() +
  geom_sf(data=outline,color="grey30", fill=NA, alpha=1, show.legend = FALSE) +
  geom_sf(data=wdist_present, aes(color= factor(marker)))
ggsave(file.path(path_out, "Listed_wdis.png"), plot = plot_filtered)