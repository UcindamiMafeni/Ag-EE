# Created by Yixin Sun in October 2018
# Link parcels to CLU
# Parcels are very messy, so let's go north to south

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


standardize_parcels <- function(df, countyname){
	df <-
	  df %>%
	  select(Page = matches(regex("page", ignore_case = T)), 
	  		 Book = matches(regex("book", ignore_case = T)), 
	  		 Parcel = matches(regex("^parcel", ignore_case = T)), 
	  		 Subparcel = matches(regex("sub", ignore_case = T)), 
	  		 APN = matches(regex("^apn", ignore_case = T))) %>%
	  st_make_valid() %>%
	  cbind(st_coordinates(st_centroid(.)), .) %>%
	  rename(Longitude = X, 
	  		 Latitude = Y) %>%
	  mutate(County = countyname), 
	  		 ParcelID = paste(County, round(Longitude, digits = 3), round(Latitude, digits = 3), sep = "-")) 
}


# Del Norte
# Siskiyou
# Modoc
# Humboldt
# Shasta
# Lassen

# ==========================================================================
# Del Norte
# ==========================================================================
del_norte_raw <- readRDS(file.path(raw_spatial, "Parcels_R/Del Norte/Del_Norte.RDS"))
del_norte14_raw <- readRDS(file.path(raw_spatial, "Parcels_R/2014/Del_Norte.RDS"))

# bind together the two datasets and melt down geometries with the same APN number
del_norte <-
  select(del_norte_raw, APN) %>%
  rbind(select(del_norte14_raw, APN, geometry = SHAPE)) %>%
  mutate(pre_area = as.numeric(st_area(.))) %>%
  group_by(APN) %>%
  summarise(pre_area = min(pre_area)) %>%
  mutate(post_area = as.numeric(st_area(.)),
  	County = "Del Norte")