# Created by Yixin Sun in November 2018
# Goal is  overlay crop data raster onto CLU and calculate area of each crop 
# category for each CLU polygon. Do this for every year we have crop data

library(dplyr)
library(raster)
library(furrr)
library(purrr)
library(tictoc)
library(sf)

if(Sys.getenv("USERNAME") == "Yixin Sun"){
	root_gh <- "C:/Users/Yixin Sun/Documents/Github/Ag-EE"
	root_db <- "C:/Users/Yixin Sun/Documents/Dropbox/Energy Water Project"
}
source(file.path(root_gh, "build/constants.R"))

P <- 4
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

furrr_extract <- function(ras, year, shp, P, G){
  shp <- split(shp, sample(rep(1:(P * G), nrow(clu) / (P * G))))
  future_map_dfr(shp, function(x) st_extract(x, ras), .progress = TRUE) %>%
    mutate(Year = !! year)
}

# ===========================================================================
# read in rasters and clu polygons and extract
# ===========================================================================
plan(multisession, .init = P)

tic()
clu <- 
  file.path(build_spatial, "CLU") %>%
  list.files(full.names = TRUE) %>%
  map_dfr(readRDS) %>%
  st_sf(crs = main_crs) %>%
  sample_n(500)
toc()

cdl <-
  2007:2017 %>%
  paste0("CDL_", ., "_06") %>%
  file.path(raw_spatial, "CropLand Data Layer", ., paste0(., ".tif")) %>%
  map(raster)

# we want to look over all the years of the CDL data, but also want to 
  # split up the clu data for parallelization
tic()
clu_cdl <- 
  map2_df(cdl, 2007:2017, furrr_extract, clu, P, G) %>%
  group_by(CLU_ID) %>% 
  mutate(Total = sum(Freq), 
         Frac = Freq / Total)  
toc()

# 13 minutes for 100 polygons
# 27 minutes for 500 polygons

# figure out dictionary for this raster

Imperial--114.584-32.732