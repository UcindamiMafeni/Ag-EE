
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


path_wdis <- "C:/Users/clohani/Dropbox/New_Water_Districts/California health tracking/service_areas/"
wdist <- st_read(file.path(path_wdis, "service_areas.shp"))
lst <- read_csv("C:/Users/clohani/Dropbox/New_Water_Districts/Acceptable_matches_new_Wdis/Acceptable_newWdis.csv")

filtered <- inner_join(wdist, lst)

st_write(filtered,"C:/Users/clohani/Dropbox/New_Water_Districts/Acceptable_matches_new_Wdis/Acceptable_newWdis.shp", "Acceptable_newWdis.shp" )