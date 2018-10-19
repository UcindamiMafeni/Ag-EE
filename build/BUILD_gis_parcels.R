# Created by Yixin Sun in October 2018
# Goal is to create a database of parcel data (refer to issue 19)
# Note that parcels are not very standardized 

library(sf)
library(dplyr)
library(purrr)
library(stringr)
library(lwgeom)
library(data.table)

if(Sys.getenv("USERNAME") == "Yixin Sun"){
	root_gh <- "C:/Users/Yixin Sun/Documents/Github/Ag-EE"
	root_db <- "C:/Users/Yixin Sun/Documents/Dropbox/Energy Water Project"
}

raw_spatial <- file.path(root_db, "Data/Spatial Data")
build_spatial <- file.path(root_db, "Data/cleaned_spatial")
main_crs <- 4326
m2_to_acre <- 0.000247105

# ===========================================================================
# read in counties with just 1 shapefile
# ===========================================================================
# many of the raw folders just have 1 shapefile with the polygons.
# find these folders, loop over and read these in before handling the harder ones
folder_names <- list.dirs(file.path(raw_spatial, "Parcels"), full.names = TRUE, recursive = F)
county_count <-
  folder_names %>%
  map_dbl(~ length(list.files(., pattern = "*.shp$"))) %>%
  bind_cols(County = basename(folder_names), shp_no = .)

# function for reading in shapefiles and choosing column names
read_simple_counties <- function(path){
	parcel_shp <-
	  st_read(path, stringsAsFactors = FALSE) %>%
	  select(Page = matches(regex("page", ignore_case = T)), 
	  		 Book = matches(regex("book", ignore_case = T)), 
	  		 Parcel = matches(regex("^parcel", ignore_case = T)), 
	  		 Subparcel = matches(regex("sub", ignore_case = T)), 
	  		 APN = matches(regex("^apn", ignore_case = T))) %>%
	  st_transform(main_crs) %>%
	  st_make_valid() %>%
	  cbind(st_coordinates(st_centroid(.)), .) %>%
	  rename(Longitude = X, 
	  		 Latitude = Y) %>%
	  mutate(County = basename(dirname(path)), 
	  		 ParcelID = paste(County, round(Longitude, digits = 3), round(Latitude, digits = 3), sep = "-")) 
}

# apply function for reading in parcel data 
simple_counties <-
  county_count %>%
  filter(shp_no == 1) %>%
  pull(County) %>%
  file.path(raw_spatial, "Parcels", .) %>% 
  list.files(., pattern = "*.shp$", full.names = TRUE) %>%
  map(read_simple_counties) %>%
  rbindlist(., use.names = TRUE, fill = TRUE)
save(simple_counties, file = file.path(build_spatial, "simple_counties.Rda")) # REMEMBER TO DELETE THIS LATER

# =========================================================================
 # Read in counties with more than 1 shapefile
# =========================================================================
# Colusa 
colusa <- 
  list.files(file.path(raw_spatial, "Parcels/Colusa"), pattern = ".shp$", full.names = TRUE) %>%
  map(read_simple_counties) %>%
  rbindlist(., use.names = TRUE, fill = TRUE)

# ----------------------------------------
# Kern - read in all years and deduplicate
kern_parcels <-
  list.files(file.path(raw_spatial, "Parcels/Kern"), pattern = "kern(.+).shp$", full.names = TRUE) %>%
  map(~ st_read(., stringsAsFactors = F)) %>%
  rbindlist(., use.names = TRUE, fill = TRUE) %>%
  st_sf() %>%
  st_transform(main_crs) %>%
  filter(!st_is_empty(.)) %>%
  mutate(acres = as.numeric(st_area(.)) * m2_to_acre) %>%
  st_make_valid()

kern_ag <-
  list.files(file.path(raw_spatial, "Parcels/Kern"), pattern = "kc_ag_(.+).shp$", full.names = TRUE) %>%
  map(~ st_read(., stringsAsFactors = F)) %>%
  rbindlist(., use.names = TRUE, fill = TRUE)


check <-
  kern_parcels %>%
  group_by(PERMIT, SITEID) %>%
  summarise(max_acres = max(acres),
  	min_acres = min(acres)) %>%
  ungroup %>% 
  mutate(acres = as.numeric(st_area(.)) * m2_to_acre)

kern_int <-
  st_intersection(kern_all, kern_all) %>%
  mutate(int_acres = as.numeric(st_area(.)) * m2_to_acre, 
  		 int_perc = int_acres / pmin(acres, acres.1)) 

