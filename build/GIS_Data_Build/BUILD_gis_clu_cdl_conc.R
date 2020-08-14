# Created by Yixin Sun in November 2018
# Goal is  overlay crop data raster onto CLU and calculate area of each crop 
# category for each CLU polygon. Do this for every year we have crop data
# The first part of this is done in arcpy for extreme speed improvements.
  # Because Tabulate Area (the function for extracting raster overlays) does not
  # work for overlapping polygons, I read in the data generated from the arcpy
  # program, and then tabulate raster overlay counts for the rest of the poygons

library(tidyverse)
library(rgdal)
library(raster)
library(furrr)
library(purrr)
library(tictoc)
library(sf)

raw_spatial <- "T:/Projects/Pump Data/Data/Spatial Data"
build_spatial <- "T:/Projects/Pump Data/Data/cleaned_spatial"

main_crs <- 4326
m2_to_acre <- 0.000247105

P <- 3
G <- 10

main_proj <- "+proj=longlat +datum=WGS84 +no_defs"
options(future.globals.maxSize = 3250585600)

# ===========================================================================
# create function for extracting classification values for each lease
# ===========================================================================
st_extract <- function(shape_group, raster_file){
  single_extract <- function(shape, clu_no){
    shape <- st_sfc(shape, crs = projection(raster_file)) 
    shape <- as(shape, "Spatial")

    count_cdl <-
      tryCatch({
                raster_temp <- crop(raster_file, extent(raster(shape)))
                as.data.frame(table(extract(raster_temp, shape)))},
        error = function(e) {return(count_cdl = tibble())} )
    
    if (nrow(count_cdl) == 0) {
      count_cdl <- tibble(CLU_ID = clu_no, Var1 = NA, Freq = NA) 
    } else{
      count_cdl <- cbind("CLU_ID" = clu_no, count_cdl)
    }

    return(count_cdl)   
  }

  map2_df(shape_group$geometry, shape_group$CLU_ID, 
          function(x, y) single_extract(x, y)) 
}


# function for parallelizing the st_extract function on clu polygons
furrr_extract <- function(ras, year, shp, P, G){
  print(year)
  shp <- split(shp, sample(rep(1:(P * G), nrow(clu) / (P * G))))
  future_map_dfr(shp, function(x) st_extract(x, ras), .progress = TRUE) %>%
    mutate(Year = !! year) %>% 
    rename(Value = Var1) %>%
    mutate(Value = as.numeric(Value))
}

# ===========================================================================
# read in dictionary for raster values
# ===========================================================================
cdl_dict <- 
  file.path(raw_spatial, "Cropland Data Layer/cdl_dict.txt") %>%
  read_delim(delim = ",") %>%
  dplyr::select(Value = VALUE, LandType = CLASS_NAME) %>%
  mutate(LandType = ifelse(LandType == " ", NA, LandType))

# ===========================================================================
# Run arcpy script and read in data generated from that program
# ===========================================================================
# shell('python "GIS_Data_Build/BUILD_gis_clu_cdl_conc.py"')

# reading in clu_cdl concordance
read_clu_cdl <- function(path){
  read_csv(path) %>%
    dplyr::select(-OBJECTID) %>%
    gather(Value, Count, c(-CLU_ID, -Year)) %>%
    filter(Count != 0) %>% 
    mutate(Value = as.numeric(str_replace(Value, "VALUE_", ""))) %>%
    group_by(CLU_ID, Year) %>%
    mutate(Total = sum(Count, na.rm = TRUE)) 
}

clu_cdl <- 
  file.path(build_spatial, "cross/clu_cdl") %>%
  list.files(pattern = "*.csv", full.names = TRUE) %>%
  map_df(read_clu_cdl) %>%
  ungroup %>%
  mutate(Fraction = Count / Total) %>%
  left_join(cdl_dict) %>%
  mutate(Overlapping = FALSE)


# ===========================================================================
# read in rasters and clu polygons 
# ===========================================================================
cdl <-
  2007:2019 %>%
  paste0("CDL_", ., "_06") %>%
  file.path(raw_spatial, "CropLand Data Layer", ., paste0(., ".tif")) %>%
  map(raster)

clu <- 
  file.path(build_spatial, "CLU") %>%
  list.files(full.names = TRUE, pattern = "*.RDS") %>%
  map_dfr(readRDS) %>%
  st_sf(crs = main_crs) %>%
  mutate_if(is.factor, as.character) %>%
  st_transform(projection(cdl[[1]]))

# ===========================================================================
# use extract() within R to tabulate cropland for remaining CLU polygons
# ===========================================================================
# Filter down to CLU-Year combinations not computed in arcpy output
clu_year <-
  crossing(clu$CLU_ID, 2007:2019) %>%
  setNames(c("CLU_ID", "Year")) %>%
  mutate(CLU_ID = as.character(CLU_ID)) %>%
  anti_join(clu_cdl) %>%
  right_join(clu, .)

# keep clu only if it intersects with the extent of cdl
cdl_extent <- 
  extent(cdl[[1]]) %>%
  st_bbox(crs = projection(cdl[[1]])) 

clu_year <- 
  clu_year %>%
  st_transform(projection(cdl[[1]])) %>%
  st_crop(cdl_extent)

# ===========================================================================
# extract cdl rasters using clu polygons
# ===========================================================================
# we want to look over all the years of the CDL data, but also want to 
  # split up the clu data for parallelization
tic("CDL CLU Extract")
plan(multisession, .init = P)
clu_year <- split(clu_year, clu_year$Year)
clu_cdl_overlap <- 
  pmap_df(list(cdl, 2007:2019, clu_year), furrr_extract, P, G) %>%
  group_by(CLU_ID, Year) %>% 
  mutate(Total = sum(Freq), 
         Fraction = Freq / Total) %>%
  left_join(cdl_dict) %>%
  mutate(Overlapping = TRUE) %>%
  as_tibble() %>%
  rename(Count = Freq)
toc()

clu_cdl <-
  clu_cdl %>%
  rbind(clu_cdl_overlap) %>%
  filter(!is.na(Fraction))

# ===========================================================================
# check and save
# ===========================================================================
check <- 
  clu_cdl %>% 
  group_by(CLU_ID, Value, Year) %>% 
  filter(n() > 1)
if(nrow(check) > 0){
  stop("Extract is duplicated")
}

clu_cdl %>% 
  map(~sum(is.na(.))) %>%
  unlist %>%
  print

saveRDS(clu_cdl, file = file.path(build_spatial, "cross/clu_cdl_conc.RDS"))

