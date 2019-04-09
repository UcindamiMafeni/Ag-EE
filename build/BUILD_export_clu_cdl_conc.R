#######################################################
#  Script to export CLU-CDL concordance/panel as csv  #
####################################################### 
rm(list = ls())


#Load CLU-CDL concordance
setwd("S:/Matt/ag_pump/data/cleaned_spatial/cross")
clu_cdl_conc <- readRDS("clu_cdl_conc.RDS")

#Export as csv 
setwd("S:/Matt/ag_pump/data/misc")
filename <- "clu_cdl_conc.csv"
write.csv(clu_cdl_conc, file=filename , row.names=FALSE, col.names=TRUE, quote=FALSE, append=FALSE)


