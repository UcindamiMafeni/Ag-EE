# Created by Yixin Sun in December 2018
# Print maps showing coverage of CLU and CDL shapefiles

library(tigris)
library(dplyr)
library(stringr)
library(ggplot2)
library(sf)
library(gridExtra)

if(Sys.getenv("USERNAME") == "Yixin Sun"){
	root_gh <- "C:/Users/Yixin Sun/Documents/Github/Ag-EE"
	root_db <- "C:/Users/Yixin Sun/Documents/Dropbox/Energy Water Project"
}

source(file.path(root_gh, "build/constants.R"))
memory.limit(13000000000000)

# function for an empty background used in ggplot mapping
theme_nothing <- function(base_size = 12, title_size = 15){
  return(
    theme(panel.grid.major = element_line(colour = "transparent"), 
          panel.grid.minor = element_line(colour = "transparent"),
          panel.background = element_blank(), axis.line = element_blank(), 
          axis.text.x = element_blank(),
          axis.text.y = element_blank(), 
          axis.title = element_blank(), 
          axis.ticks = element_blank(),
          plot.title = element_text(size = title_size, hjust = 0.5), 
          plot.caption = element_text(size = 6))) 
}


# ========================================================================
# read in counties that parcels and clu have coverage for
# ========================================================================
# some parcels counties were excluded because shapefiles needed more advanced
# cleaning or shapefiles are too big. Show coverage for parcels we have as well
# as coverage for parcels currently cleaned
parcel_coverage_all <-
  list.dirs(file.path(raw_spatial, "Parcels_R")) %>%
  basename() %>%
  str_replace_all(".RDS", "") %>%
  str_replace_all("_", " ") 

parcel_coverage <- 
  list.files(file.path(build_spatial, "Parcels/parcels_counties")) %>%
  str_replace_all(".RDS", "") %>%
  str_replace_all("_", " ") 

# clu coverage
clu_coverage <-
  list.files(file.path(build_spatial, "CLU/clu_counties")) %>%
  str_replace_all(".RDS", "") %>%
  str_replace_all("_", " ") 


# ========================================================================
# read in map of California counties using tigris census package and plot
# ========================================================================
ca_counties <- 
  counties(state = "ca", class = "sf") %>%
  dplyr::select(County = NAME)

coverage_counties <-
  ca_counties %>%
  mutate(Parcel = case_when(
  	  County %in% parcel_coverage ~ "Cleaned Parcel", 
  	  County %in% parcel_coverage_all ~ "Raw Parcel", 
  	  TRUE ~ "No Coverage"), 
  	CLU = County %in% clu_coverage, 
  	Coverage = case_when(
  		County %in% parcel_coverage & County %in% clu_coverage ~ "CLU & Cleaned Parcel", 
  		County %in% parcel_coverage_all & County %in% clu_coverage ~ "CLU & Raw Parcel", 
  		TRUE ~ "Missing Coverage"))

 missing_labels <-
  coverage_counties %>%
  cbind(st_coordinates(st_centroid(.))) %>%
  st_set_geometry(NULL) %>%
  filter(Coverage != "CLU & Cleaned Parcel") 


coverage_map <-
  ggplot(coverage_counties, aes(fill = Coverage)) +
  geom_sf() + 
  geom_text(data = missing_labels, size = 2,
  	aes(x = X, y = Y, label = County)) +
  theme_nothing() + 
  scale_fill_manual(values = c("#66c2a5", "#fc8d62", "white"))
ggsave(coverage_map, file = file.path(root_gh, "build/scratch/clu_parcel_coverage.png"))