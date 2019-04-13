####################################################
#  Script to export CLU-parcel concordance as csv  #
#################################################### 
rm(list = ls())

path <- "T:/Projects/Pump Data/"

#Load parcel-CLU concordance
setwd(paste0(path,"data/cleaned_spatial/cross"))
clu_parcel_conc <- readRDS("clu_parcel_conc.RDS")

#Export as csv 
setwd(paste0(path,"data/misc"))
filename <- "clu_parcel_conc.csv"
write.csv(clu_parcel_conc, file=filename , row.names=FALSE, col.names=TRUE, quote=FALSE, append=FALSE)


