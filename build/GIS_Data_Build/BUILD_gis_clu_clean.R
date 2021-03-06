# Created by Yixin Sun in October 2018
# Goal is to convert CLU files from shapefiles to R files and clean

library(sf)
library(dplyr)
library(purrr)
library(stringr)
library(lwgeom)
library(tigris)

raw_spatial <- "T:/Projects/Pump Data/Data/Spatial Data"
build_spatial <- "T:/Projects/Pump Data/Data/cleaned_spatial"

main_crs <- 4326
m2_to_acre <- 0.000247105

memory.limit(13000000000000)

# ===========================================================================
# Read in and convert CLU data to R files
# ===========================================================================
# raw clu files are saved according to its fips no
# pull in tigris shapefile to get dictionary of county fip #s and county names
ca_fips <- 
  counties(state = "ca", class = "sf") %>%
  st_set_geometry(NULL) %>%
  select(fips = COUNTYFP, County = NAME)

# function for finding county according to its fips no, then read in CLU and 
  # clean up
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
	   		  poly_acres = as.numeric(st_area(.)) * m2_to_acre,
	   		  CLU_ID = paste(County, 
	   		  				 round(X, digits = 3), 
	   		  				 round(Y, digits = 3),  
	   		  				 round(poly_acres, 4), sep = "-")) %>%
	   select(-X, -Y, -poly_acres)

	saveRDS(clu, file.path(build_spatial, "CLU/clu_counties", outpath))
	print(countyname)
}

list.dirs(file.path(raw_spatial, "CLU"), full.names = TRUE, recursive = FALSE) %>%
  walk(read_clu)

clu <- 
  file.path(build_spatial, "CLU/clu_counties") %>%
  list.files(full.names = TRUE, pattern = "*.RDS") %>%
  map_dfr(readRDS) %>%
  st_sf(crs = main_crs)
st_write(clu, file.path(build_spatial, "CLU/clu_poly"), 
	driver = "ESRI Shapefile", delete_layer = TRUE)
saveRDS(clu, file.path(build_spatial, "CLU/clu.RDS"))


## Extract CLU polygon centroids
clus <- readRDS(file.path(build_spatial, "CLU/clu.RDS"))
centroids <- st_centroid(clus)
head(centroids)
centroids <- centroids[,names(centroids) %in% c("CLU_ID","geometry")]
head(centroids)
centroids$rowID <- seq.int(nrow(centroids))
saveRDS(centroids, file.path(build_spatial, "CLU/clu_centroids.RDS"))
