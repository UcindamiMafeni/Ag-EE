###################################################
#  Script to export RDS parcel/CLU files to csvs  #
################################################### 
rm(list = ls())
library(sf)

path <- "T:/Projects/Pump Data/"

#Load parcel-CLU concordance, export as .csv
setwd(paste0(path,"data/cleaned_spatial/cross"))
clu_parcel_conc <- readRDS("clu_parcel_conc.RDS")
setwd(paste0(path,"data/misc"))
filename <- "clu_parcel_conc.csv"
write.csv(clu_parcel_conc, file=filename , row.names=FALSE, col.names=TRUE, quote=FALSE)


#Load CLU-CDL concordance, export as .csv
setwd(paste0(path,"data/cleaned_spatial/cross"))
clu_cdl_conc <- readRDS("clu_cdl_conc.RDS")
setwd(paste0(path,"data/misc"))
filename <- "clu_cdl_conc.csv"
write.csv(clu_cdl_conc, file=filename , row.names=FALSE, col.names=TRUE, quote=FALSE)


#Load CLU SF data frame, export as .csv
setwd(paste0(path,"data/cleaned_spatial/CLU"))
CLUs_sf <- readRDS("clu.RDS")
CLUs_data <- CLUs_sf
st_geometry(CLUs_data) <- NULL
setwd(paste0(path,"data/misc"))
filename <- "CLUs_cleaned.csv"
write.csv(CLUs_data, file=filename , row.names=FALSE, quote=FALSE)


#Load Parcels SF data frame, export as .csv
setwd(paste0(path,"data/cleaned_spatial/Parcels"))
Parcels_sf <- readRDS("parcels.RDS")
Parcels_data <- Parcels_sf
st_geometry(Parcels_data) <- NULL
setwd(paste0(path,"data/misc"))
filename <- "Parcels_cleaned.csv"
write.csv(Parcels_data, file=filename , row.names=FALSE, quote=FALSE)
