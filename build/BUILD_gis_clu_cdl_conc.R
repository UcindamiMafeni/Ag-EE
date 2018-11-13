# Created by Yixin Sun in November 2018
# Goal is  overlay crop data raster onto CLU and calculate area of each crop 
# category for each CLU polygon. Do this for every year we have crop data

library(dplyr)
library(raster)

if(Sys.getenv("USERNAME") == "Yixin Sun"){
	root_gh <- "C:/Users/Yixin Sun/Documents/Github/Ag-EE"
	root_db <- "C:/Users/Yixin Sun/Documents/Dropbox/Energy Water Project"
}
source(file.path(root_gh, "build/constants.R"))

P <- 4

# ===========================================================================
# create function for extracting classification values for each lease
# ===========================================================================

lc_extract <- function(lease, lc = landcover){
  lc_temp <- 
    as_tibble(lease) %>%
    left_join(classification_dict, by = "value") %>%
    group_by(Classification) %>%
    summarise(n = n()) %>%
    tidyr::spread(Classification, n) 

  return(lc_temp)
}

extract_loop <- function(leases_group){
  map2_df(leases_group$geometry, leases_group$Lease_Number, function(x, y) lease_extract(x, y)) 
}

cdl13 <-
  file.path(raw_spatial, "CropLand Data Layer/CDL_2013_06/CDL_2013_06.tif") %>%
  raster 
