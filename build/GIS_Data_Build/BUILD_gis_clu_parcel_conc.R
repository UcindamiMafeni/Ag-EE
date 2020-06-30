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

raw_spatial <- "T:/Projects/Pump Data/Data/Spatial Data"
build_spatial <- "T:/Projects/Pump Data/Data/cleaned_spatial"

main_crs <- 4326
m2_to_acre <- 0.000247105

num_workers <- 2

# function for intersecting CLU with Parcels
clu_parcel_int <- function(countyname){

  # read in clu and parcel shapefiles
  tic(countyname)
  input_path <- 
    str_replace_all(basename(countyname), " ", "_") %>%
    paste0(., ".RDS")
  parcel <- readRDS(file.path(build_spatial, "Parcels/parcels_counties", input_path))
  clu <- readRDS(file.path(build_spatial, "CLU/clu_counties", input_path))

  # intersect parcel and clu - calculate intersection acreage and mark 
  # which parcel has most of the CLU 
  clu_parcel <-
    st_intersection(parcel, clu) %>%
    mutate(IntAcres = as.numeric(st_area(.)) * m2_to_acre, 
           IntPerc = IntAcres / CLUAcres) %>%
    st_set_geometry(NULL) %>%
    select(ParcelID, CLU_ID, IntAcres, IntPerc) %>%
    mutate(County = countyname, 
      IntPerc = if_else(IntPerc > 1, 1, IntPerc)) %>%
    filter(IntPerc < Inf) %>%
    group_by(CLU_ID) %>%
    arrange(-IntAcres) %>%
    mutate(Largest = row_number() == 1) %>%
    ungroup

  saveRDS(clu_parcel, file.path(build_spatial, "cross/clu_parcel", input_path))
  toc()

  return(clu_parcel)
}

# ==========================================================================
# Intersect CLU and Parcel shapefiles
# ==========================================================================
plan(multiprocess(workers = eval(num_workers)))

# find list of counties where both clu and parcel shapefiles are cleaned
parcel_counties <- 
  list.files(file.path(build_spatial, "Parcels/parcels_counties")) %>%
  str_replace_all(".RDS", "") %>%
  str_replace_all("_", " ") 

clu_counties <-
  list.files(file.path(build_spatial, "CLU/clu_counties")) %>%
  str_replace_all(".RDS", "") %>%
  str_replace_all("_", " ") 

conc_counties <- intersect(parcel_counties, clu_counties)

# loop over counties to intersect clu and parcels
clu_parcel_conc <- 
  conc_counties %>%
  future_map_dfr(clu_parcel_int, .progress = TRUE) 

saveRDS(clu_parcel_conc, file.path(build_spatial, "cross/clu_parcel_conc.RDS"))

summary(clu_parcel_conc)

