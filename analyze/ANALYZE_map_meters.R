######################################################################
##### CODE TO PLOT TREATMENT VS. CONTROL SCHOOLS
##### 
######################################################################


############################ SETUP ####################################
rm(list=ls())

### DIRECTORY PATHS
# main directory
setwd("S:/Matt/ag_pump")

# main data directory
dataDir <- paste(getwd(), "/data", sep = "")
# spatial data directory
spatialDir <- paste(dataDir, "/spatial", sep = "")
# state shapefile directory
spatialDirState <- paste(spatialDir, "/State", sep = "")
# iou shapefile directory
spatialDirIOU <- paste(spatialDir, "/Service territories", sep = "")
# output directory
mapsDir <- paste(getwd(), "/output", sep = "")

### PACKAGES
library(GISTools)
library(readr)
library(dplyr)
library(ggplot2)
library(rgeos)
library(rgdal)
library(maptools)
library(broom)
library(haven)


### FUNCTIONS
#fxn to convert data to tbl_df's in one step
as.tbl_df <- function(data) {
  dataset <- as.data.frame(data) %>%
    tbl_df()
}

mapToDF <- function(shapefile) {
  # first assign an identifier to the main dataset
  shapefile@data$id <- rownames(shapefile@data)
  # now ``tidy'' our data to convert it into 
  #  dataframe that's usable by ggplot2
  #  the region command keeps polygons together
  mapDF <- tidy(shapefile) %>% 
    # and this data onto the information attached to the shapefile
    left_join(., shapefile@data, by = "id") %>%
    as.tbl_df()
  return(mapDF)
}

### GGPLOT2 SETUP

myThemeStuff <- theme(panel.background = element_rect(fill = NA),
                      panel.border = element_blank(),
                      panel.grid.major = element_blank(),
                      panel.grid.minor = element_blank(),
                      axis.ticks = element_blank(),
                      axis.text = element_blank(),
                      axis.title = element_blank(),
                      legend.key = element_blank())

# custom colors for graphing
myBlue <- rgb(0/255, 128/255, 255/255, 1)
dknavy<- rgb(30/255, 45/255, 83/255, 1)
eltblue <- rgb(130/255, 192/255, 233/255, 1)
myGray <- rgb(224/255, 224/255, 224/255, 1)

################################################################
### SPATIAL DATA


## load the utility map shapefile
utility <- readOGR(dsn=spatialDirIOU, layer = "CA_Electric_Investor_Owned_Utilities_IOUs")

utility <- spTransform(utility, CRS("+proj=longlat +datum=WGS84"))

utilityDF <- mapToDF(utility)


## load in the US states shapefile
ca <- readOGR(dsn=spatialDirState, layer = "CA_State_TIGER2016")
# re-project to be in lat-long
ca <- spTransform(ca, CRS(proj4string(utility)))

caDF <- mapToDF(ca) 

# keep PGE territory only
pgeDF <- filter(utilityDF, id == 3, piece == 1)


customerDF_1 <- read_dta(paste(dataDir, "/pge_cleaned/pge_cust_detail_20180719.dta", sep = "")) %>%
  filter(in_pge == 1)

customerDF_2 <- read_dta(paste(dataDir, "/pge_cleaned/pge_cust_detail_20180322.dta", sep = "")) %>%
  filter(in_pge == 1)

customerDF_3 <- read_dta(paste(dataDir, "/pge_cleaned/pge_cust_detail_20180827.dta", sep = "")) %>%
  filter(in_pge == 1)


################################################################
### METER MAP


meterMap <- ggplot() +
  geom_polygon(data = pgeDF, aes(x = long, y = lat, group = group),
               color = 'gray75', fill = 'NA') +
  geom_polygon(data = caDF, aes(x = long, y = lat, group = group),
               color = 'black', fill = 'NA') +
  geom_point(data = customerDF_2, aes(x = prem_long, y = prem_lat),
             size = 0.05, color = eltblue, shape = 21)  +
  geom_point(data = customerDF_3, aes(x = prem_long, y = prem_lat),
             size = 0.05, color = eltblue, shape = 21)  +
  geom_point(data = customerDF_1, aes(x = prem_long, y = prem_lat),
             size = 0.05, color = dknavy, shape = 21)  +
  coord_fixed(ratio = 1.25) +
  guides(colour = guide_legend(""),
         shape = guide_legend("")) +
  myThemeStuff

meterMap
ggsave(paste0(mapsDir,"/customer_map.eps"), width=127, units = "mm")
ggsave(paste0(mapsDir,"/customer_map.pdf"), width=127, units = "mm")

