# Created by Yixin Sun in November 2018
# Goal is  overlay crop data raster onto CLU and calculate area of each crop 
# category for each CLU polygon. Do this for every year we have crop data

library(dplyr)
library(raster)
library(furrr)
library(purrr)
library(tictoc)
library(sf)

#if(Sys.getenv("USERNAME") == "Yixin Sun"){
#	root_gh <- "C:/Users/Yixin Sun/Documents/Github/Ag-EE"
#	root_db <- "C:/Users/Yixin Sun/Documents/Dropbox/Energy Water Project"
#}
#source(file.path(root_gh, "build/constants.R"))

raw_spatial <- file.path(root_db, "Data/Spatial Data")
build_spatial <- file.path(root_db, "Data/cleaned_spatial")

main_crs <- 4326
m2_to_acre <- 0.000247105

P <- 14
G <- 10

main_proj <- "+proj=longlat +datum=WGS84 +no_defs"

# ===========================================================================
# create function for extracting classification values for each lease
# ===========================================================================
st_extract <- function(shape_group, cdl1){
  single_extract <- function(shape, clu_no){
    print(clu_no)
    shape <- 
      st_sfc(shape, crs = 4326) %>% 
      st_transform(projection(cdl1))

    shape <- as(shape, "Spatial")
    temp <- raster(shape)

    tryCatch({
      raster_temp <- crop(cdl1, extent(temp))
      count_cdl <- as.data.frame(table(extract(raster_temp, shape)))}, 
      error = function(e){print("extents do not overlap")})

    if(!exists("count_cdl")){
      count_cdl <- tibble("CLU_ID" = clu_no) 
    } else if (nrow(count_cdl) == 0) {
      count_cdl <- tibble("CLU_ID" = clu_no) 
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
# read in rasters and clu polygons 
# ===========================================================================
plan(multisession, .init = P)
clu <- 
  file.path(build_spatial, "CLU") %>%
  list.files(full.names = TRUE) %>%
  map_dfr(readRDS) %>%
  st_sf(crs = main_crs) %>%
  sample_n(50)

cdl <-
  2007:2017 %>%
  paste0("CDL_", ., "_06") %>%
  file.path(raw_spatial, "CropLand Data Layer", ., paste0(., ".tif")) %>%
  map(raster)

# ===========================================================================
# extract cdl rasters using clu polygons
# ===========================================================================
# we want to look over all the years of the CDL data, but also want to 
  # split up the clu data for parallelization
tic("CDL CLU Extract")
clu_cdl <- 
  map2_df(cdl, 2007:2017, furrr_extract, clu, P, G) %>%
  group_by(CLU_ID) %>% 
  mutate(Total = sum(Freq), 
         Frac = Freq / Total) %>%
  left_join(cdl_dict)
toc()

saveRDS(clu_cdl, file = file.path(build_spatial, "clu_cdl.RDS"))

