# Created by Yixin Sun in October 2018
# Goal is to clean parcel R files and get rid of duplicate geometries
# Think about using data.tables for dissolving polygons that have the same APN

library(sf)
library(dplyr)
library(purrr)
library(stringr)
library(lwgeom)
library(tigris)
library(igraph)

raw_spatial <- "T:/Projects/Pump Data/Data/Spatial Data"
build_spatial <- "T:/Projects/Pump Data/Data/cleaned_spatial"

main_crs <- 4326
m2_to_acre <- 0.000247105

memory.limit(13000000000000)

#=======================================================================
# report a pasted version of unique values in a list
#========================================================================
upaste <- function(l) {
  if(length(unique(l)) == 1) {
    return(l[1])
  }
  else {
    return(paste(l, collapse = "; "))
  }
}

# ===========================================================================
# Functions to clean and combine parcel data 
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
	  select(APN = matches(regex("^apn", ignore_case = T)))  %>%
	  st_zm() %>%
	  mutate(APN = str_replace_all(APN, "-", "")) %>%
	  st_make_valid() %>%
	  mutate(Acres = as.numeric(st_area(.)) * m2_to_acre) 

	# want to melt down parcels with the same APN Numbers 
	# create a MinAcre and MaxAcres variable as checks for whether polygons
	# that are super different still get melted down 
	apn_melt_df <-
	  df %>%
	  st_set_geometry(NULL) %>%
	  group_by(APN) %>%
	  summarise(MinAcres = min(Acres), 
	  			MaxAcres = max(Acres), 
	   			N_Dissolve = n()) 

	apn_melt <-
	  df %>% 
	  filter(!is.na(APN)) %>% 
	  group_by(APN) %>%
	  arrange(-Acres) %>%
	  filter(row_number() == 1) %>%
	  left_join(apn_melt_df)

	# want to find polygons that are essentially the same -
	  # round to third decimal in longitude/latitude and 
	  # if centroids are same, then melt down into one polygon
	df <- 
	  df %>%
	  filter(is.na(APN)) %>%
	  mutate(MinAcres = Acres, 
	  	MaxAcres = Acres,
	  	N_Dissolve = 1) %>%
	  select(-Acres) %>%
	  rbind(apn_melt) %>%
	  cbind(st_coordinates(st_centroid(.)), .) %>%
	  rename(Longitude = X, 
	  		 Latitude = Y) %>%
	  mutate(County = countyname, 
	  		 ParcelID = paste(round(Longitude, digits = 3), 
	  		 				  round(Latitude, digits = 3))) %>%
	  group_by(ParcelID) %>%
	  summarise(County = unique(County), 
	  			MinAcres = min(MinAcres), 
	  			MaxAcres = max(MaxAcres), 
	  			N_Dissolved = sum(N_Dissolve), 
	  			APN = upaste(APN)) %>%
	  mutate(ParcelAcres = as.numeric(st_area(.)) * m2_to_acre, 
	  	ParcelID = paste(County, round(ParcelAcres, digits = 4), ParcelID))
}

# write a function that checks if the county is available in both sets of data
 # and if available in both, then summarise() and save in cleaned_data
combine_parcels <- function(countyname){
	print(countyname)
	start <- Sys.time()

	apn_names <- 
	  "^apn|parc_py_id|^ain$|^prop_id$|AsmtNum|^asmt$|blk_lot$" %>%
	  regex(ignore_case = T)

	not_apn <-
	  "apn_chr|apn_flag|apnnodash|apnpath|apn10|apn_suffix|^apn2$" %>%
	  regex(ignore_case = T) 

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
		  select(-matches(not_apn)) %>%
		  select(matches(apn_names), 
		  		 geometry = matches(regex("^geometry$|^shape$", ignore_case = T))) %>%
		  select(APN = 1) 

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
	} else{
		county_all <- 
		  readRDS(file.path(raw_spatial, "Parcels_R", countyname, county_path)) %>%
		  select(-matches(not_apn)) %>%
		  select(APN = matches(regex("^apn", ignore_case = T))) %>%
		  standardize_parcels(., countyname) %>%
		  st_transform(main_crs)
	}

	check <- 
	  county_all %>%
	  group_by(ParcelID) %>%
	  filter(n() > 1)

	if(nrow(check) > 1 & ncol(check) > 2){
		stop("Parcel ID is not unique")
	}

	print(Sys.time() - start)
	saveRDS(county_all, file = file.path(build_spatial, "Parcels/parcels_counties", county_path))
}

# ===========================================================================
# run functions through counties that just have one shapefile and are small
# ===========================================================================
to_ignore <- c("2014", "parcels14", "Kern", "Merced", "Yolo", "Los Angeles", 
	"Santa Clara", "Nevada", "Riverside", "Orange", "San Diego")

all_counties <- 
	  list.files(file.path(raw_spatial, "Parcels_R/2014"), pattern = "*.RDS") %>%
	  str_replace_all(., "_", " ") %>%
	  str_replace_all(., ".RDS", "") %>%
	  c(., list.dirs(file.path(raw_spatial, "Parcels_R"), full.names = FALSE, recursive = FALSE)) 

all_counties[-which(all_counties %in% to_ignore )] %>%
  walk(combine_parcels)

# ============================================================================
# Clean Kern
# ============================================================================
# kern has a lot of parcels that seem to change shape over time - instead of
# the usual method, we want to intersect all kern parcels with each other
# and check that parcels that are wholly contained in another are melted down
# Just use 2014 parcels - unsure what purchased parcels really represent

# use graph theory magic - if a intersects b and b intersects c, we want
# a, b, and c to all be in one group
int_bundles <- function(x, y, name){
  pairs <- 
    map2(x, y, c) %>% 
    map(as.character)

  pairs_embed <- do.call("rbind", lapply(pairs, embed, 2))
  pairs_vert <- graph.edgelist(pairs_embed, directed = F)
  pairs_id <- split(V(pairs_vert)$name, clusters(pairs_vert)$membership)

  # collapse list of ids in the same group to a dataframe 
  pairs_id %>%
    map_df(~ cbind(., paste(., collapse = "-")) %>% 
             as_tibble() %>%
             setNames(c(name, "sameid")))
}

kern14 <- 
  readRDS(file.path(raw_spatial, "Parcels_R/2014", "Kern.RDS")) %>%
  select(APN, geometry = SHAPE) %>%
  st_make_valid() %>%
  mutate(APN = str_replace_all(APN, "-", ""), 
  	Acres =  as.numeric(st_area(.)) * m2_to_acre, 
  	id = row_number())

tic()
kern_int <-
  st_intersection(kern14, kern14) 
toc()
  
  filter(id < id.1) %>%
  mutate(int_area = as.numeric(st_area(.)) * m2_to_acre, 
  	int_perc = int_area / pmin(Acres, Acres.1)) %>%
  filter(int_perc > .5) %>%
  st_set_geometry(NULL) %>%
  as_tibble()

kern <-
  with(kern_int, int_bundles(id, id.1, "id")) %>%
  left_join(mutate(kern14, id = as.character(id)), .) %>%
  mutate(sameid = if_else(is.na(sameid), id, sameid)) %>%
  group_by(sameid) %>%
  summarise(County = "Kern", 
  	APN = upaste(APN),
  	MinAcres = min(Acres), 
  	MaxAcres = max(Acres), 
  	N_Dissolve = n()) %>%
  mutate(ParcelAcres = as.numeric(st_area(.)) * m2_to_acre) %>%
  cbind(st_coordinates(st_centroid(.)), .) %>%
  mutate(ParcelID = paste(County, round(ParcelAcres, digits = 4), round(X, digits = 3), round(Y, digits = 3))) %>%
  group_by(ParcelID) %>%
  mutate(ParcelID1 = paste(ParcelID, row_number())) %>%
  ungroup %>%
  select(-X, -Y, -ParcelID) %>%
  rename(ParcelID = ParcelID1) 

saveRDS(kern, file = file.path(build_spatial, "Parcels", "parcels_counties", "Kern.RDS"))

# ============================================================================
# Clean Merced
# ============================================================================
# The only parcel shapefiles seem to be Assessment_Parcels, 
  # and Williamson_Act_2010
  # co_merced_asr_agricultural_preserve appear to be contained within the 
  # previous two 
merced_parcel <-
  file.path(raw_spatial, "Parcels_R/Merced/Assessment_Parcels.RDS") %>%
  readRDS %>%
  st_transform(main_crs) %>%
  select(APN = Name)


merced_williamson <-
  file.path(raw_spatial, "Parcels_R/Merced/Williamson_Act_2010.RDS") %>%
  readRDS %>%
  select(APN) %>%
  st_transform(main_crs)

merced14 <-
  readRDS(file.path(raw_spatial, "Parcels_R/2014", "Merced.RDS")) %>%
  select(APN, geometry = SHAPE) 

merced <-
  rbind(merced_parcel, merced_williamson, merced14) %>%
  standardize_parcels(., "Merced")

saveRDS(merced, file = file.path(build_spatial, "Parcels", "parcels_counties", "Merced.RDS"))

# ============================================================================
# Clean Yolo
# ============================================================================
# Crop files are contained in the tax parcels - decide if we want to utilize
  # this later for verification of land use
yolo_tax <- 
  readRDS(file.path(raw_spatial, "Parcels_R/Yolo/yolo_tax_parcels.RDS")) %>%
  st_transform(main_crs) %>%
  select(APN)

# yolo_crops <-
#   readRDS(file.path(raw_spatial, "Parcels_R/Yolo/Yolo.RDS")) %>%
#   st_transform(main_crs)

yolo14 <-
  readRDS(file.path(raw_spatial, "Parcels_R/2014", "Yolo.RDS")) %>%
  select(APN, geometry = SHAPE) 

yolo <-
  rbind(yolo_tax, yolo14) %>%
  standardize_parcels(., "Yolo")

saveRDS(yolo, file = file.path(build_spatial, "Parcels", "parcels_counties", "Yolo.RDS"))

# ============================================================================
# Clean Santa Clara
# ============================================================================
# Air Parcel polygons are basically all entirely contained in a Land Parcel
sc_land <- 
  file.path(raw_spatial, "Parcels_R/Santa Clara/FY2016_LAND_PARCELS.RDS") %>%
  readRDS() %>%
  st_transform(main_crs)

sc2014 <-
  readRDS(file.path(raw_spatial, "Parcels_R/2014", "Santa_Clara.RDS")) %>%
  select(APN, geometry = SHAPE) 

Santa_Clara <-
  sc_land %>%
  select(APN) %>%
  rbind(sc2014) %>%
  standardize_parcels("Santa Clara")

saveRDS(Santa_Clara, file = file.path(build_spatial, "Parcels", "parcels_counties", "Santa_Clara.RDS"))

# ============================================================================
# Clean Nevada
# ============================================================================
# Parcel_Information and Parcel_Situs_Address shapefiles have the same
  # geometry information - just houses different attribute info
  # we'll just keep parcel_information for our purpsoes
nevada_info <-
  file.path(raw_spatial, "Parcels_R/Nevada/Parcel_Information.RDS") %>%
  readRDS() %>%
  st_transform(main_crs) %>%
  select(APN)

nevada14 <-
  readRDS(file.path(raw_spatial, "Parcels_R/2014", "Nevada.RDS")) %>%
  select(APN, geometry = SHAPE) 

Nevada <-
  rbind(nevada_info, nevada14) %>%
  standardize_parcels("Nevada")

saveRDS(Nevada, file = file.path(build_spatial, "Parcels", "parcels_counties", "Nevada.RDS"))

# ============================================================================
# Los Angeles
# ============================================================================

# ============================================================================
# Orange
# ============================================================================
# just read in parcel polygons file, not irrigation
orange_parcels <-
  file.path(raw_spatial, "Parcels_R/Orange/Parcel_Polygons.RDS") %>%
  readRDS() %>%
  st_transform(main_crs) %>%
  select(geometry) %>%
  mutate(APN = NA)

orange14 <-
  readRDS(file.path(raw_spatial, "Parcels_R/2014", "Orange.RDS")) %>%
  select(APN, geometry = SHAPE) 

Orange <-
  rbind(orange14, orange_parcels)%>%
  st_buffer(0.00001) %>%
  standardize_parcels("Orange") 

saveRDS(Orange, file = file.path(build_spatial, "Parcels", "parcels_counties", "Orange.RDS"))

# ============================================================================
# Riverside
# ============================================================================
riverside <-
  file.path(raw_spatial, "Parcels_R/Riverside/Riverside.RDS") %>%
  readRDS() %>%
  st_transform(main_crs) %>%
  select(APN, geometry = SHAPE) 

riverside14 <-
  readRDS(file.path(raw_spatial, "Parcels_R/2014", "Riverside.RDS")) %>%
  select(APN, geometry = SHAPE) 

Riverside <-
  rbind(riverside, riverside14) %>%
  #mutate(check = st_is_valid(geometry)) %>%
  #filter(check) %>%
  #st_buffer(0.00001) %>%
  standardize_parcels("Riverside")

saveRDS(Riverside, file = file.path(build_spatial, "Parcels", "parcels_counties", "Riverside.RDS"))

# ============================================================================
# San Diego
# ============================================================================
# parcels file is a superset of the east, north, and south files
sandiego <- 
  file.path(raw_spatial, "Parcels_R/San Diego/PARCELS.RDS") %>%
  readRDS() %>%
  st_transform(main_crs) %>%
  select(APN)

sandiego14 <-
  readRDS(file.path(raw_spatial, "Parcels_R/2014", "San_Diego.RDS")) %>%
  select(APN, geometry = SHAPE) 


San_Diego %>%
  rbind(sandiego, sandiego14) %>%
  standardize_parcels("San Diego")

saveRDS(San_Diego, file = file.path(build_spatial, "Parcels", "parcels_counties", "San_Diego.RDS"))

# ============================================================================
# combine parcels into one file
# ============================================================================
parcels <-
  file.path(build_spatial, "Parcels", "parcels_counties") %>%
  list.files(full.names = TRUE, pattern = "*.RDS") %>%
  map_dfr(readRDS) %>%
  st_sf(crs = main_crs)
saveRDS(parcels, file.path(build_spatial, "Parcels/parcels.RDS"))



