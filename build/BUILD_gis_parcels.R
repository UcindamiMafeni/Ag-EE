# Created by Yixin Sun in October 2018
# Goal is to create a database of parcel data (refer to issue 19)
# Note that parcels are not very standardized 

library(sf)
library(dplyr)
library(purrr)

if(Sys.getenv("USERNAME") == "Yixin Sun"){
	root_gh <- "C:/Users/Yixin Sun/Documents/Github/Ag-EE"
	root_db <- "C:/Users/Yixin Sun/Documents/Dropbox/Energy Water Project"
}

raw_spatial <- file.path(root_db, "Data/Spatial Data")
build_spatial <- file.path(root_db, "Data/cleaned_spatial")

# many of the raw folders just have 1 shapefile with the polygons.
# find these folders, loop over and read these in before handling the harder ones
folder_names <- list.dirs(file.path(raw_spatial, "Parcels"), full.names = TRUE, recursive = F)
county_count <-
  folder_names %>%
  map_dbl(~ length(list.files(., pattern = "*.shp$"))) %>%
  bind_cols(County = basename(folder_names), shp_no = .)


