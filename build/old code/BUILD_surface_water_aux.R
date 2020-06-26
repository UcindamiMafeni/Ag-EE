######################################################################
#  Auxility script for initial build of WRIMS surface water datasets #
#             (modified from script written by Xinyi Wang)           #
###################################################################### 
rm(list = ls())

library(dplyr)
library(stringr)
library(matrixStats)
library(tidyverse)

path <- "T:/Projects/Pump Data/data/surface_water/"

data <- read.csv(file=paste0(path,'raw data/haggerty_wr70.csv'),header=T, sep=',',
                 stringsAsFactors=FALSE) #95535 rows

#-------------------------------------------------------------------#-------------------------------------------------------------------#-------------------------------------------------------------------
#####cleaning#####

#Pre-processing
data[10926:10931,] <- c(data[10926:10931,1:37], data[10926:10931,39:171])
data[10940,] <- c(data[10940,1:37], data[10940,39:171])
data[22218:22222,] <- c(data[22218:22222,1:37], ' ', data[22218:22222,38:171])
data[22218,38] <- "Appropriative"
data[22219,38] <- "Appropriative"
data[22220,38] <- "Appropriative"
data[22221,38] <- "Appropriative"
data[22222,38] <- "Appropriative"



###1
#drop not activate rights
data <- data %>%
  filter(!STATUS_TYPE %in% c('Cancelled','Inactive','Removed','Revoked','Pending')) #75486 rows 
as.data.frame(table(data$STATUS_TYPE)) #freq table #checked
#as.data.frame(table(data$POD_STATUS)) #POD and WR status variables are different

#drop minor rights
as.data.frame(table(data$WR_TYPE)) #stockpond: 5335, livestock: 305
data <- data %>%
  filter(!WR_TYPE %in% c('Stockpond','Registration Livestock')) #69874 rows

###2
#drop duplicates, make observations unique by the three keys
data <- data[!duplicated(data[c('WR_WATER_RIGHT_ID','POD_ID','BENEFICIAL_USE')]),] #69874

#reshape
#skipped for now

###3
#construct the year a right first began
data['YEAR_RIGHT_BEGAN'] = data$YEAR_FIRST_USE
sum(is.na(data['YEAR_RIGHT_BEGAN'])) #41361 na's

data['PERMIT_ORIGINAL_ISSUE_YEAR'] <- as.numeric(sapply(data['PERMIT_ORIGINAL_ISSUE_DATE'], str_sub, -4,-1)) #trick:The original missing data is not in the form of NAs(Can't find by is.na()). as.numeric('') generates NA's from non-numeric text.
sum(data['PERMIT_ORIGINAL_ISSUE_DATE']=='') #35228 na's
sum(is.na(as.numeric(sapply(data['PERMIT_ORIGINAL_ISSUE_DATE'], str_sub, -4,-1)))) #35233

data[is.na(data['YEAR_RIGHT_BEGAN']),]['YEAR_RIGHT_BEGAN'] = data[is.na(data['YEAR_RIGHT_BEGAN']),]['PERMIT_ORIGINAL_ISSUE_YEAR']
sum(is.na(data['YEAR_RIGHT_BEGAN'])) #6933

data['LICENSE_ORIGINAL_ISSUE_YEAR'] <- as.numeric(sapply(data['LICENSE_ORIGINAL_ISSUE_DATE'], str_sub, -4,-1))
sum(is.na(data$LICENSE_ORIGINAL_ISSUE_YEAR)) #41845
data[is.na(data['YEAR_RIGHT_BEGAN']),]['YEAR_RIGHT_BEGAN'] = data[is.na(data['YEAR_RIGHT_BEGAN']),]['LICENSE_ORIGINAL_ISSUE_YEAR']
sum(is.na(data['YEAR_RIGHT_BEGAN'])) #5807

data['STATUS_YEAR'] <- as.numeric(sapply(data['STATUS_DATE'], str_sub, -4,-1))
sum(is.na(data$STATUS_YEAR)) #70
data[is.na(data['YEAR_RIGHT_BEGAN']),]['YEAR_RIGHT_BEGAN'] = data[is.na(data['YEAR_RIGHT_BEGAN']),]['STATUS_YEAR']
sum(is.na(data['YEAR_RIGHT_BEGAN'])) #23 missing YEAR_RIGHT_BEGAN's at the end

#construct the year a right ended
as.data.frame(table(data$STATUS_TYPE)) #'Cancelled','Inactive','Removed','Revoked','Pending' already removed in the 1st step, only 78 rejected and 87 closed
data['YEAR_RIGHT_END'] = ifelse(data$STATUS_TYPE %in% c('Closed','Rejected'), data$STATUS_YEAR, NA)
as.data.frame(table(data$YEAR_RIGHT_END)) #checked

###4
#remove non-consumptive diversions
data <- data[!((data$BENEFICIAL_USE_LIST == 'Aquaculture')|(data$BENEFICIAL_USE_LIST =='Power')|(data$BENEFICIAL_USE_LIST =='Aquaculture; Power')|(data$BENEFICIAL_USE_LIST =='Power; Aquaculture')),] #69235 rows

#for rights that reprot no diversion to storage, set diversions to zero
data$DIRECT_DIVERSION_AMOUNT = ifelse(data$DIVERSION_STORAGE_AMOUNT == 0, 0, data$DIRECT_DIVERSION_AMOUNT)
as.data.frame(table(data$DIRECT_DIVERSION_AMOUNT)) #63407 0's

sum(is.na(data$FACE_VALUE_AMOUNT)) 

#for diversions that do report diversion to storage, substract the amount used from the amount diverted, sensoring negative values at zero
#skipped for now, see notes


###5


data$FACE_VALUE_AMOUNT = as.numeric(as.character(data$FACE_VALUE_AMOUNT))


###6
#correct high outliers
all_diversion <- data[c(52:63,67:78,82:93,97:108)]
all_diversion$DEC_DIVERSION <- as.numeric(all_diversion$DEC_DIVERSION)
all_diversion$JAN_DIVERSION_1 <- as.numeric(all_diversion$JAN_DIVERSION_1)
all_diversion$DEC_DIVERSION_1 <- as.numeric(all_diversion$DEC_DIVERSION_1)
all_diversion$JAN_DIVERSION_2 <- as.numeric(all_diversion$JAN_DIVERSION_2)
all_diversion$DEC_DIVERSION_2 <- as.numeric(all_diversion$DEC_DIVERSION_2)
all_diversion$DEC_DIVERSION_3 <- as.numeric(all_diversion$DEC_DIVERSION_3)
log_all_diversion <- log(all_diversion)

SD=rowSds(as.matrix(log_all_diversion)) #lots of 0's in diversion amount, so many invalid values 
data['SD_Diversion'] <- SD
as.data.frame(table(data$SD_Diversion>2)) #345 rows > 2

MEAN=rowSums2(as.matrix(all_diversion))/4
data['Annual_mean_diversion'] <- MEAN
as.data.frame(table((data$Annual_mean_diversion-data$FACE_VALUE_AMOUNT)>100)) #1598 > 100
#as.data.frame(table(data$FACE_VALUE_UNITS))

annual_diversion <- data[c(64,79,94,109)]
annual_diversion$REPORTED_DIVERSION_TOTAL <- as.numeric(annual_diversion$REPORTED_DIVERSION_TOTAL)
annual_diversion$REPORTED_DIVERSION_TOTAL_1 <- as.numeric(annual_diversion$REPORTED_DIVERSION_TOTAL_1)
annual_diversion$REPORTED_DIVERSION_TOTAL_2 <- as.numeric(annual_diversion$REPORTED_DIVERSION_TOTAL_2)
annual_diversion$REPORTED_DIVERSION_TOTAL_3 <- as.numeric(annual_diversion$REPORTED_DIVERSION_TOTAL_3)


MAX=rowMaxs(as.matrix(annual_diversion))
data['Max_annaul_Diversion'] <- MAX
MIN=rowMins(as.matrix(annual_diversion))
data['Min_annaul_Diversion'] <- MIN
as.data.frame(table((data$Max_annaul_Diversion/data$Min_annaul_Diversion) > 100)) #6914


as.data.frame(table(((data$Annual_mean_diversion-data$FACE_VALUE_AMOUNT)>100) & (data$SD_Diversion>2) & (data$Max_annaul_Diversion/data$Min_annaul_Diversion) > 100)) #97

Outliers <- which(((data$Annual_mean_diversion-data$FACE_VALUE_AMOUNT)>100) & (data$SD_Diversion>2) & (data$Max_annaul_Diversion/data$Min_annaul_Diversion) > 100) #index of the outliers

error <- data #in case I make any mistakes
for (i in 52:64){
  error[,i] <- as.numeric(error[,i])
}
for (i in 67:79){
  error[,i] <- as.numeric(error[,i])
}
for (i in 82:94){
  error[,i] <- as.numeric(error[,i])
}
for (i in 97:109){
  error[,i] <- as.numeric(error[,i])
}

for (n in Outliers){
  for (i in 52:64){
      error[n,i]=ifelse(error[n,64]==error[n,179],error[n,i]*error[n,180]/error[n,64],error[n,i])
  }
  for (i in 67:79){
    error[n,i]=ifelse(error[n,79]==error[n,179],error[n,i]*error[n,180]/error[n,79],error[n,i])
  }
  for (i in 82:94){
    error[n,i]=ifelse(error[n,94]==error[n,179],error[n,i]*error[n,180]/error[n,94],error[n,i])
  }
  for (i in 97:109){
    error[n,i]=ifelse(error[n,109]==error[n,179],error[n,i]*error[n,180]/error[n,109],error[n,i])
  }  
}

data <- error #68897

#Further sample restrictions

#drop rights held by federal or state
data <- data %>%
  filter((STATUS_TYPE != 'State Filing') & (FEDERAL_CONTRACTOR_FLAG != 'Y')) #67153 rows

#drop non-consumptive rights
as.data.frame(table(data$BENEFICIAL_USE))
data <- data %>%
  filter(!BENEFICIAL_USE %in% c('Aesthetic','Aquaculture','Fish and Wildlife Preservation and Enhancement',
                                'Incidental Power','Power','Recreational','Snow Making')) #55220

data <- data %>%
  filter(!PRIMARY_OWNER %in% c('CALIFORNIA DEPARTMENT OF FISH AND WILDLIFE',
                               '  California Department of Parks & Recreation- Tehachapi District',
                               '  California Department of Parks and Recreation-Oceano Dunes District',
                               '  THE NATURE CONSERVANCY',
                               '  PINE MOUNTAIN LAKE ASSOCIATION',
                               '  TUSCANY RESEARCH INSTITUTE',
                               '  US BUREAU OF LAND MANAGEMENT',
                               '  U S NATIONAL PARK SERVICE',
                               '  U S FOREST SERVICE',
                               "  WOODY'S ON THE RIVER LLC, ET AL",
                               '  PACIFIC GAS AND ELECTRIC COMPANY',
                               '  SOUTHERN CALIFORNIA EDISON COMPANY')) #53017

data = data[!grepl('DUCK CLUB',data$PRIMARY_OWNER),] 
data = data[!grepl('GUN CLUB',data$PRIMARY_OWNER),]
data = data[!grepl('POWER',data$PRIMARY_OWNER),]
data = data[!grepl('Power',data$PRIMARY_OWNER),]
data = data[!grepl('PRESERVATION',data$PRIMARY_OWNER),]
data = data[!grepl('SHOOTING CLUB',data$PRIMARY_OWNER),]
data = data[!grepl('WATERFOWL',data$PRIMARY_OWNER),]
data = data[!grepl('WETLAND',data$PRIMARY_OWNER),]
#52699 rows

#drop rights with no location info
sum(is.na(data$LONGITUDE)) #2022 NA's
data <- data[!is.na(data$LONGITUDE),] #50674 rows


#-------------------------------------------------------------------#-------------------------------------------------------------------#-------------------------------------------------------------------
#Sector
as.data.frame(table(data$BENEFICIAL_USE))

data['SECTOR'] = ifelse(data$BENEFICIAL_USE %in% c('Irrigation','Stockwatering'), 'Agricultural', 'Municipal')
as.data.frame(table(data$SECTOR))

write.csv(data, file=paste0(path,'/haggerty_wr70_cleaned.csv'), row.names=FALSE) #50674


    
#-------------------------------------------------------------------#-------------------------------------------------------------------#-------------------------------------------------------------------
#-------------------------------------------------------------------#-------------------------------------------------------------------#-------------------------------------------------------------------
###Info From Haggerty

data <- read.csv(file=paste0(path,'/haggerty_wr70_cleaned.csv'),header=T, sep=',',row.names=NULL) 

#select the info columns
info <- data[c(1:50,172:176,181)]

#move keys columns to the front
info <- info %>%
  select('WR_WATER_RIGHT_ID','POD_ID','BENEFICIAL_USE_LIST',everything())

#drop duplicates
info <- info[!duplicated(info[c('WR_WATER_RIGHT_ID','POD_ID')]),] #33772

write.csv(info, file=paste0(path,'/haggerty_info.csv'), row.names=FALSE)

#-------------------------------------------------------------------#-------------------------------------------------------------------#-------------------------------------------------------------------
#-------------------------------------------------------------------#-------------------------------------------------------------------#-------------------------------------------------------------------
###Diversion data from Haggerty and merged by eWRIMS data (2007-2017)
#Data were downloaded from the e-WRIMS portal here: http://ciwqs.waterboards.ca.gov/ciwqs/ewrims/reportingDiversionDownloadPublicSetup.do

info <- read.csv(file=paste0(path,'/haggerty_info.csv'),header=T, sep=',',row.names=NULL)

#input eWRIMS rms datasets to a list of dataframes
setwd(paste0(path,'raw data/eWRIMS water report 2007-2017/'))
temp = list.files(pattern="*.csv")
myfiles = lapply(temp, read.csv)

#drop unwanted columns
for (i in 1:11){
  myfiles[[i]] <- myfiles[[i]][c(1,5,15:66)]
}

#merge all rms datasets by application number
all_rms <- myfiles %>% reduce(full_join, by = 'APPLICATION_NUMBER')

#merge by application number to the Haggerty info dataset (get the wide file)
wide_07to17 <- merge(info, all_rms, by = 'APPLICATION_NUMBER')

write.csv(wide_07to17, file=paste0(path,'wrims_wide_07to17.csv'), row.names=FALSE)

#alternatively, make the long file (but is not merged with Haggerty info dataset)
myfiles_2 = do.call(rbind, lapply(temp, function(x) read.csv(x, stringsAsFactors = FALSE))) #bind all datasets by rows
myfiles_2 <- myfiles_2[order(myfiles_2$APPLICATION_NUMBER,myfiles_2$YEAR),]
myfiles_2 <- myfiles_2[c(1,5,15:66)]
long_07to17 <- myfiles_2

write.csv(long_07to17, file=paste0(path,'wrims_long_07to17.csv'), row.names=FALSE)
