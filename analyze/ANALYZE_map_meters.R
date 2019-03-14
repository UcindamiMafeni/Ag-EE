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



################################################################
### METER MAP with legend

customerDF_1 <- customerDF_1[customerDF_1$in_pge==1,]
customerDF_2 <- customerDF_2[customerDF_2$in_pge==1,]
customerDF_3 <- customerDF_3[customerDF_3$in_pge==1,]


latlonDF_1 <- customerDF_1[,names(customerDF_1)%in%c("sp_uuid","prem_long","prem_lat")]
latlonDF_2 <- customerDF_2[,names(customerDF_2)%in%c("sp_uuid","prem_long","prem_lat")]
latlonDF_3 <- customerDF_3[,names(customerDF_3)%in%c("sp_uuid","prem_long","prem_lat")]
latlonDF_23 <- rbind(latlonDF_2,latlonDF_3)


cols <- c("c1"=eltblue,"c2"=dknavy)
shapes <- c("s1"=21,"s2"=22)
labels  <- c("Unmatched ag service points",
             "Matched ag service points")

postscript(paste0(mapsDir,"/pge_ca_map.eps"),width=360)
  ggplot() +
  geom_polygon(data = pgeDF, aes(x = long, y = lat, group = group),
               color = 'gray75', fill = 'NA') +
  geom_polygon(data = caDF, aes(x = long, y = lat, group = group),
               color = 'black', fill = 'NA') +
  geom_point(data = latlonDF_3, aes(x = prem_long, y = prem_lat,
             color = "c1"), shape = 19, alpha=1, size=0.1) +
  geom_point(data = latlonDF_1, aes(x = prem_long, y = prem_lat,
             color = "c2"), shape = 19, alpha=1, size=0.1) +
  coord_fixed(ratio = 1.25) +
  theme_bw() +
  theme(panel.background = element_rect(fill = NA),
        axis.ticks = element_blank(),
        axis.text = element_blank(),
        axis.title = element_blank(),
        legend.key = element_rect(fill='white'),
        legend.title = element_text(size=1),
        legend.text = element_text(size=15),
        legend.position = c(0.7,0.76),
        legend.margin = margin(t=0,r=0,b=0,l=0),
        legend.box.margin = margin(t=0,r=0,b=0,l=0),
        plot.background = element_rect(fill='white'),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        panel.border = element_blank(),
        plot.margin = margin(t=0,r=0,b=0,l=0)
   ) +
   labs(x="", y="", title="") +
   scale_color_manual(name = "",
                      breaks = c("c1","c2"), 
                      values = cols,
                      labels = labels) +
    guides(colour = guide_legend(override.aes = list(size=3)))

  # scale_shape_manual(name = "",
  #                    breaks = c("s1","s2"),
  #                    values = shapes,
  #                    labels = labels) 
  # 
dev.off()



################################################################
### METER MAP with legend with counts


cols <- c("c1"=eltblue,"c2"=dknavy)
shapes <- c("s1"=21,"s2"=22)
labels  <- c("96,321 unmatched",
             "11,851 matched")

postscript(paste0(mapsDir,"/pge_ca_map_counts.eps"),width=360)
ggplot() +
  geom_polygon(data = pgeDF, aes(x = long, y = lat, group = group),
               color = 'gray75', fill = 'NA') +
  geom_polygon(data = caDF, aes(x = long, y = lat, group = group),
               color = 'black', fill = 'NA') +
  geom_point(data = latlonDF_3, aes(x = prem_long, y = prem_lat,
                                    color = "c1"), shape = 19, alpha=1, size=0.1) +
  geom_point(data = latlonDF_1, aes(x = prem_long, y = prem_lat,
                                    color = "c2"), shape = 19, alpha=1, size=0.1) +
  coord_fixed(ratio = 1.25) +
  theme_bw() +
  theme(panel.background = element_rect(fill = NA),
        axis.ticks = element_blank(),
        axis.text = element_blank(),
        axis.title = element_blank(),
        legend.key = element_rect(fill='white'),
        legend.key.size = unit(1.5, 'lines'),
        legend.title = element_text(size=1),
        legend.text = element_text(size=24),
        legend.position = c(0.73,0.76),
        legend.margin = margin(t=0,r=0,b=0,l=0),
        legend.box.margin = margin(t=0,r=0,b=0,l=0),
        plot.background = element_rect(fill='white'),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        panel.border = element_blank(),
        plot.margin = margin(t=0,r=0,b=0,l=0)
  ) +
  labs(x="", y="", title="") +
  scale_color_manual(name = "",
                     breaks = c("c1","c2"), 
                     values = cols,
                     labels = labels) +
  guides(colour = guide_legend(override.aes = list(size=3)))

  # scale_shape_manual(name = "",
  #                    breaks = c("s1","s2"),
  #                    values = shapes,
  #                    labels = labels) 
  # 
dev.off()


