
# Created by Yixin Sun in October 2018
# Goal is to convert current nonstandardized shapefiles to folders that are 
# easily looped over (refer to issue 19)

library(sf)
library(dplyr)
library(purrr)
library(stringr)
library(lwgeom)
library(tigris)

if(Sys.getenv("USERNAME") == "Yixin Sun"){
	root_gh <- "C:/Users/Yixin Sun/Documents/Github/Ag-EE"
	root_db <- "C:/Users/Yixin Sun/Documents/Dropbox/Energy Water Project"
}

raw_spatial <- file.path(root_db, "Data/Spatial Data")
build_spatial <- file.path(root_db, "Data/cleaned_spatial")
main_crs <- 4326
m2_to_acre <- 0.000247105
memory.limit(13000000000000)

# ===========================================================================
# Clean and combine parcel data 
# ===========================================================================

# we have 2 potential sources of parcel shapefiles
  #1. parcels from the county clerk
  #2. Big 2014 file
# first step is we want to combine these 2 and melt down by APN numbers 

# function for standardizing parcel data to same variable names 
  # also want to check validitiy
standardize_parcels <- function(df, countyname){
	df <-
	  df %>%
	  select(APN = matches(regex("^apn", ignore_case = T))) %>%
	  st_make_valid() %>%
	  mutate(Acres = as.numeric(st_area(.)) * m2_to_acre) %>%
	  group_by(APN) %>%
	  summarise(MinAcres = min(Acres), 
	  			MaxAcres = max(Acres)) %>%
	  cbind(st_coordinates(st_centroid(.)), .) %>%
	  rename(Longitude = X, 
	  		 Latitude = Y) %>%
	  mutate(County = countyname, 
	  		 ParcelAcres = as.numeric(st_area(.)) * m2_to_acre,
	  		 ParcelID = paste(County, round(Longitude, digits = 3), round(Latitude, digits = 3), sep = "-")) %>%
	  group_by(ParcelID) %>%
	  arrange(ParcelID, ParcelAcres) %>%
	  mutate(ParcelID = paste(ParcelID, row_number(), sep = "-")) %>%
	  ungroup
}

# write a function that checks if the county is available in both sets of data
 # and if available in both, then summarise() and save in cleaned_data
combine_parcels <- function(countyname){
	# check if county is available in the big 2014 dataset
	all_counties14 <- 
	  list.files(file.path(raw_spatial, "Parcels_R/2014"), pattern = "*.RDS") %>%
	  str_replace_all(., "_", " ") %>%
	  str_replace_all(., ".RDS", "")

	all_counties <-
	  list.dirs(file.path(raw_spatial, "Parcels_R"), recursive = FALSE, full.names = FALSE) 

	county_path <- paste0(str_replace_all(countyname, " ", "_"), ".RDS")

	if(countyname %in% all_counties14 & countyname %in% all_counties){
		county <- 
		  readRDS(file.path(raw_spatial, "Parcels_R", countyname, county_path)) %>%
		  select(APN = matches(regex("^apn|parc_py_id", ignore_case = T))) 

		county14 <- 
		  readRDS(file.path(raw_spatial, "Parcels_R/2014", county_path)) %>%
		  select(APN = matches(regex("^apn", ignore_case = T)), geometry = SHAPE) 

		county_all <-
		  rbind(county, county14) %>%
		  standardize_parcels(., countyname)

	} else if(countyname %in% all_counties14){
		county_all <-
		  readRDS(file.path(raw_spatial, "Parcels_R/2014", county_path)) %>%
		  select(APN = matches(regex("^apn", ignore_case = T)), geometry = SHAPE) %>%
		  standardize_parcels(., countyname)
	}
	else{
		county_all <- 
		  readRDS(file.path(raw_spatial, "Parcels_R", countyname, county_path)) %>%
		  select(APN = matches(regex("^apn", ignore_case = T))) %>%
		  standardize_parcels(., countyname)
	}

	check <- 
	  county_all %>%
	  group_by(ParcelID) %>%
	  filter(n() > 1)
	if(nrow(check) > 1){
		stop("Parcel ID is not unique")
	}

	saveRDS(county_all, file = file.path(build_spatial, "Parcels", county_path))
	print(countyname)
}

to_ignore <- 
  c("2014", "Alameda", "Contra Costa", "Santa Clara", "San Diego", "Merced", 
  	"Orange", "Kern", "Nevada")
all_counties <- 
	  list.files(file.path(raw_spatial, "Parcels_R/2014"), pattern = "*.RDS") %>%
	  str_replace_all(., "_", " ") %>%
	  str_replace_all(., ".RDS", "") %>%
	  c(., list.dirs(file.path(raw_spatial, "Parcels_R"), full.names = FALSE, recursive = FALSE)) 

all_counties[-which(all_counties %in% to_ignore )] %>%
  walk(combine_parcels)


# ===========================================================================
# Read in and convert CLU data to R files
# ===========================================================================
# pull in tigris shapefile to get dictionary of county fip #s and county names
ca_fips <- 
  counties(state = "ca", class = "sf") %>%
  st_set_geometry(NULL) %>%
  select(fips = COUNTYFP, County = NAME)

read_clu <- function(path){
	path <- str_replace(path, "\n", "")
	fipno <- str_extract(basename(path), "[0-9]+")
	countyname <- 
	  ca_fips %>%
	  filter(fips == fipno) %>%
	  pull(County)
	outpath <- paste0(str_replace_all(countyname, " ", "_"), ".RDS")

	clu <- 
	   path %>%
	   st_read(stringsAsFactors = F) %>%
	   st_transform(main_crs) %>%
	   select(CLUAcres = CALCACRES) %>%
	   cbind(st_coordinates(st_centroid(.)), .) %>%
	   mutate(County = countyname, 
	   		  CLU_ID = paste(County, round(X, digits = 3), round(Y, digits = 3), sep = "-")) %>%
	   select(-X, -Y)

	saveRDS(clu, file.path(build_spatial, "CLU", outpath))
	print(countyname)
}

list.dirs(file.path(raw_spatial, "CLU"), full.names = TRUE, recursive = FALSE) %>%
  walk(read_clu)
