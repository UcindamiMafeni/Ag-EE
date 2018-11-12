# Created by Yixin Sun in October 2018
# Create concordance between CLU and parcels
library(tictoc)
library(sf)
library(dplyr)
library(purrr)
library(stringr)
library(lwgeom)
library(tigris)
library(furrr)

if(Sys.getenv("USERNAME") == "Yixin Sun"){
	root_gh <- "C:/Users/Yixin Sun/Documents/Github/Ag-EE"
	root_db <- "C:/Users/Yixin Sun/Documents/Dropbox/Energy Water Project"
}
raw_spatial <- file.path(root_db, "Data/Spatial Data")
build_spatial <- file.path(root_db, "Data/cleaned_spatial")

main_crs <- 4326
m2_to_acre <- 0.000247105

num_workers <- 4

# function for intersecting CLU with Parcels
clu_parcel_int <- function(countyname){
  tic(countyname)
  input_path <- 
    str_replace_all(basename(countyname), " ", "_") %>%
    paste0(., ".RDS")
  parcel <- readRDS(file.path(build_spatial, "Parcels", input_path))
  clu <- readRDS(file.path(build_spatial, "CLU", input_path))

  clu_parcel <-
    st_intersection(parcel, clu) %>%
    mutate(IntAcres = as.numeric(st_area(.)) * m2_to_acre, 
           IntPerc = IntAcres / ParcelAcres) %>%
    select(ParcelID, CLU_ID, IntAcres, IntPerc) %>%
    mutate(County = countyname) %>%
    st_set_geometry(NULL)
  toc()
}


# Del Norte
# Siskiyou
# Modoc
# Humboldt
# Shasta
# Lassen

# CORRECT THE SAN BERNADINO COUNTY PARCEL


# ==========================================================================
# FILTER OUT FRESNO FOR RIGHT NOW
# ==========================================================================
plan(multiprocess(workers = eval(num_workers)))

check <- 
  list.files(file.path(build_spatial, "Parcels")) %>%
  str_replace_all(".RDS", "") %>%
  str_replace_all("_", " ") %>%
  as.list() %>%
  future_map_dfr(clu_parcel_int)


del_norte_raw <- readRDS(file.path(raw_spatial, "Parcels_R/Del Norte/Del_Norte.RDS"))
del_norte14_raw <- readRDS(file.path(raw_spatial, "Parcels_R/2014/Del_Norte.RDS"))

# bind together the two datasets and melt down geometries with the same APN number
del_norte <-
  select(del_norte_raw, APN) %>%
  rbind(select(del_norte14_raw, APN, geometry = SHAPE)) %>%
  standardize_parcels(., "Del Norte")

del_norte_clu <- standardize_clu("Del Norte")

clu_parcel <-
  st_intersection(del_norte, clu15) %>%
  mutate(int_acres = as.numeric(st_area(.)) * m2_to_acre, 
  		 int_perc = int_acres / pmin(CLUAcres, ParcelAcres)) %>%
  filter(int_perc > .05)

# TO DO 
  # Write a function that checks if the county is available in both sets of data
  	# if available in both, then summarise() and save in cleaned_data
  	# otherwise just save the one in cleaned_data after applying the standardize parcels function
  # figure out if CLU codes correspond to County codes in any way
  	# check using mapview - if it's not clear just ask fiona/louis

# ============================================================================
# Scratch space
# ============================================================================

