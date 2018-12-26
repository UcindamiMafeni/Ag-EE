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

source(file.path(root_gh, "build/constants.R"))

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
    st_set_geometry(NULL) %>%
    group_by(CLU_ID) %>%
    mutate(Largest = IntAcres == max(IntAcres)) %>%
    ungroup

  saveRDS(clu_parcel, file.path(build_spatial, "cross/clu_parcel", input_path))
  toc()

  return(clu_parcel)
}

# ==========================================================================
# FILTER OUT FRESNO FOR RIGHT NOW
# ==========================================================================
plan(multiprocess(workers = eval(num_workers)))

parcel_counties <- 
  list.files(file.path(build_spatial, "Parcels")) %>%
  str_replace_all(".RDS", "") %>%
  str_replace_all("_", " ") 

clu_counties <-
  list.files(file.path(build_spatial, "CLU")) %>%
  str_replace_all(".RDS", "") %>%
  str_replace_all("_", " ") 

conc_counties <- intersect(parcel_counties, clu_counties)

clu_parcel_conc <- 
  conc_counties %>%
  future_map_dfr(clu_parcel_int, .progress = TRUE)

saveRDS(clu_parcel_conc, file.path(build_spatial, "cross/clu_parcel_conc.RDS"))