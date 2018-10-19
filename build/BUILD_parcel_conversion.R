# Created by Yixin Sun in October 2018
# Goal is to convert current nonstandardized shapefiles to folders that are 
# easily looped over (refer to issue 19)

library(sf)
library(dplyr)
library(purrr)
library(stringr)
library(lwgeom)
library(data.table)
library(readr)

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
# read in parcels for each county and save them as Rda file
# ===========================================================================
# create folders in Parcels_R based on parcel names in Parcel
basename(list.dirs(file.path(raw_spatial, "Parcels"))) %>%
  map_chr(~ file.path(raw_spatial, "Parcels_Rda", .)) %>%
  walk(dir.create)

folder_names <- list.dirs(file.path(raw_spatial, "Parcels"), full.names = TRUE, recursive = F)
county_count <-
  folder_names %>%
  map_dbl(~ length(list.files(., pattern = "*.shp$"))) %>%
  bind_cols(County = basename(folder_names), shp_no = .)

# function for reading in shapefiles and saving to the parallel folder
read_parcel <- function(path){
	county_name <- basename(dirname(path))
	outpath <- file.path(raw_spatial, "Parcels_R", county_name,  paste0(county_name, ".RDS"))
	output <- st_read(path, stringsAsFactors = FALSE)
	saveRDS(output, file = outpath)
}

read_mult <- function(path){
	county_name <- basename(dirname(path))
	outpath <- file.path(raw_spatial, "Parcels_R", county_name,  paste0(str_replace(basename(path), ".shp", ""), ".RDS"))
	output <- st_read(path, stringsAsFactors = FALSE)
	saveRDS(output, file = outpath)
}

# ===========================================================================
# read in single parcel shapefile counties
# ===========================================================================
county_count %>%
  filter(shp_no == 1) %>%
  pull(County) %>%
  file.path(raw_spatial, "Parcels", .) %>% 
  list.files(., pattern = "*.shp$", full.names = TRUE) %>%
  walk(read_parcel) 

gc()

# ===========================================================================
# read in and save files for counties with multiple shapefiles
# ===========================================================================
# Colusa 
colusa_ind <-
  st_read(file.path(raw_spatial, "Parcels/Colusa/Commercial_and_Industrial_Parcels.shp"), stringsAsFactors = FALSE) %>%
  mutate(ParcelType = "Commercial and Industrial")

colusa_res <- 
  file.path(raw_spatial, "Parcels/Colusa/Parcels_2016.shp") %>%
  st_read(stringsAsFactors = FALSE) %>% 
  mutate(ParcelType = "Residential")

colusa <- 
  bind_rows(colusa_ind, colusa_res) %>%
  st_sf()

save(colusa, file = file.path(raw_spatial, "Parcels_R", "Colusa", "Colusa.Rda"))

# ----------------------------------------
# Kern - read in all years of parcel data and save as one file
kern_parcels <-
  list.files(file.path(raw_spatial, "Parcels/Kern"), pattern = "kern(.+).shp$", full.names = TRUE) %>%
  map(~ st_read(., stringsAsFactors = F)) %>%
  rbindlist(., use.names = TRUE, fill = TRUE) %>%
  st_sf() %>%
  st_transform(main_crs) %>%
  filter(!st_is_empty(.)) %>%
  mutate(acres = as.numeric(st_area(.)) * m2_to_acre) %>%
  st_make_valid()
saveRDS(kern_parcels, file = file.path(raw_spatial, "Parcels_Rda", "Kern", paste0("Kern", ".RDS")))

# for other types of parcels, save as individual files
file.path(raw_spatial, "Parcels/Kern") %>%
  list.files(pattern = "kc_(.+).shp$", full.names = TRUE) %>%
  walk(read_mult)

# ----------------------------------------
# read in all merced files and save as individual datasets
file.path(raw_spatial, "Parcels/Merced") %>%
  list.files(pattern = "*.shp$", full.names = TRUE) %>%
  walk(read_mult)

# ----------------------------------------
# read in nevada files
file.path(raw_spatial, "Parcels/Nevada") %>%
  list.files(pattern = "*.shp$", full.names = TRUE) %>%
  walk(read_mult)

# ----------------------------------------
# read in orange files
file.path(raw_spatial, "Parcels/Orange") %>%
  list.files(pattern = "*.shp$", full.names = TRUE) %>%
  walk(read_mult)

# ----------------------------------------
# 4 shapefiles for san diego - PARCELS, PARCELS_EAST, PARCELS_SOUTH, PARCELS_NORTH
  # Looks like parcels is a superset of the other 3
  # save the 4 files separately
file.path(raw_spatial, "Parcels/San Diego") %>%
  list.files(pattern = ".*shp$", full.names = TRUE) %>%
  walk(read_mult)

# ----------------------------------------
# 2 shapefiles for plumas - combine
plumas <-
  file.path(raw_spatial, "Parcels/Plumas") %>%
  list.files(pattern = ".*shp$", full.names = TRUE) %>%
  map(function(x) st_read(x, stringsAsFactors = FALSE) %>% 
  		  			mutate(year = str_extract(basename(x), "[0-9]+"))) %>%
  rbindlist(use.names = TRUE, fill = TRUE)
saveRDS(plumas, file = file.path(raw_spatial, "Parcels_R", "Plumas", "Plumas.Rds"))

# ----------------------------------------
file.path(raw_spatial, "Parcels/Santa Clara") %>%
  list.files(pattern = ".*shp$", full.names = TRUE) %>%
  walk(read_mult)

# ----------------------------------------
yolo <-
  file.path(raw_spatial, "Parcels/Yolo") %>%
  list.files(pattern = "Crops(.*)shp$", full.names = TRUE) %>%
  map(~ st_read(., stringsAsFactors = F)) %>%
  reduce(rbind)
saveRDS(yolo, file = file.path(raw_spatial, "Parcels_R", "Yolo", "Yolo.Rds"))

yolo_tax_parcels <- 
  file.path(raw_spatial, "Parcels/Yolo/Tax Parcels 071615.shp") %>%
  st_read(stringsAsFactors = FALSE) 
saveRDS(yolo_tax_parcels, file = file.path(raw_spatial, "Parcels_R", "Yolo", "yolo_tax_parcels.Rds"))


  
# ===========================================================================
# Read in geodatabase files
# ===========================================================================
# 2014 Parcels - just read in merged file that houses shapes from every county
parcels14 <- 
  file.path(raw_spatial, "Parcels/2014/Parcels_CA_2014.gdb") %>%
  st_read(layer = "CA_Merged", stringsAsFactors = FALSE)
saveRDS(parcels14, file = file.path(raw_spatial, "Parcels_R/2014/parcels14.Rds"))


# Los Angeles
los_angeles <- 
  file.path(raw_spatial, "Parcels/Los Angeles/Parcels_2014.gdb") %>%
  st_read(stringsAsFactors = FALSE)
saveRDS(los_angeles, file = file.path(raw_spatial, "Parcels_R/Los Angeles/Los_Angeles.Rds"))

