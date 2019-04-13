#######################################################
#  Script to export CLU-CDL concordance/panel as csv  #
####################################################### 
rm(list = ls())

path <- "T:/Projects/Pump Data/"

#Load CLU-CDL concordance
setwd(paste0(path,"data/cleaned_spatial/cross"))
clu_cdl_conc <- readRDS("clu_cdl_conc.RDS")

#Export as csv 
setwd(paste0(path,"data/misc"))
filename <- "clu_cdl_conc.csv"
write.csv(clu_cdl_conc, file=filename , row.names=FALSE, col.names=TRUE, quote=FALSE, append=FALSE)


