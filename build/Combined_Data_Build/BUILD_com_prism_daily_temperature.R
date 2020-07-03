#####################################################################################
### CODE TO SCRAPE DAILY MAX/MIN TEMPERATURE AND PRECIPITATION RASTERS FROM PRISM ###
### AND ASSIGN DAILY VALUES TO 3 SETS OF LAT/LONS: PGE SPs, APEP PUMPS, SCE SPs   ### 
#####################################################################################

############################################################################
################################# SETUP ####################################

rm(list = ls())

# Script to download PRISM weather data, and construct a daily temperatures/precip at all relevant lat/lons
# Modified from Dave McLaughlin's excellent R script

library(raster)
library(sp)
library(data.table)
library(reshape2)
library(lubridate)
library(dplyr)

setwd("T:/Projects/Pump Data/data")


############################################################################
############################################################################

## 1. Download daily max/min temperature files from PRISM

date <- seq(as.Date("2008-01-01"),as.Date("2019-12-31"),1)
date.dt <- as.data.table(date)

date.dt[, day_id := 1:dim(date.dt)[1]]
#date.dt[, week_id := floor((day_id -1) /7) + 1]

#date.dt <- date.dt[week_id %in% seq(54, 65, 1)]

date.dt[, date.char := as.character(date)]
date.dt[, date.url := gsub("-", "", date.char)]

date.df <- as.data.frame(date.dt)

date.df$urls_tmax <- paste0("http://services.nacse.org/prism/data/public/4km/", 
                            "tmax", "/", date.df$date.url)
date.df$urls_tmin <- paste0("http://services.nacse.org/prism/data/public/4km/", 
                            "tmin", "/", date.df$date.url)
date.df$urls_ppt <- paste0("http://services.nacse.org/prism/data/public/4km/", 
                            "ppt", "/", date.df$date.url)

date.df$filenames_tmax <- paste0("./prism/raw/", "tmax", date.df$date.url, ".zip")
date.df$filenames_tmin <- paste0("./prism/raw/", "tmin", date.df$date.url, ".zip")
date.df$filenames_ppt <- paste0("./prism/raw/", "ppt", date.df$date.url, ".zip")

# urls <- list("http://services.nacse.org/prism/data/public/4km/tmin/20090405",
#             "http://services.nacse.org/prism/data/public/4km/tmax/20090405")
# 
# 
# end.day <- "200904"
# 
# filenames <- list("./data/tmin20090405.zip",
#                  "./data/tmax20090405.zip")

down.fun <- function(url, filename){
  download.file(url=url, destfile = filename, method="auto", quiet = FALSE, mode = "wb",
                cacheOK = TRUE,
                extra = getOption("download.file.extra"))
  return(NULL)
}

mapply(down.fun, url=date.df$urls_tmax, filename=date.df$filenames_tmax)
mapply(down.fun, url=date.df$urls_tmin, filename=date.df$filenames_tmin)
mapply(down.fun, url=date.df$urls_ppt,  filename=date.df$filenames_ppt)


############################################################################
############################################################################

## 2. Unzip raw data folders

# Unzip raw folders
zip.names <- list.files(path="./prism/raw", pattern=".zip$", full.names=TRUE)
out.paths <- gsub("raw", "unzipped", gsub(".zip$", "", zip.names))
unzip.fun <- function(zip.names, out.paths){
  unzip(zipfile = zip.names, exdir=out.paths)
  return(NULL)
}
mapply(unzip.fun, zip.names=zip.names, out.paths=out.paths)

# Identify bad downloads (i.e. empty folders)
dirs <- list.dirs(path = "./prism/unzipped")
nfiles <- sapply(dirs,function(x) length(list.files(x)))
head(nfiles)
rescrapes <- dirs[nfiles==0]

# Rescrape bad downloads
rescrape_urls <- paste0("http://services.nacse.org/prism/data/public/4km/", rescrapes)
rescrape_urls <- gsub("ppt","ppt/",rescrape_urls)
rescrape_urls <- gsub("tmax","tmax/",rescrape_urls)
rescrape_urls <- gsub("tmin","tmin/",rescrape_urls)
rescrape_urls <- gsub("./prism/unzipped/","",rescrape_urls)
rescrape_urls
rescrape_files <- paste0(rescrapes,".zip")
rescrape_files <- gsub("unzipped","raw",rescrape_files)
rescrape_files
mapply(down.fun, url=rescrape_urls, filename=rescrape_files)

# Unzip new downloads
out.paths2 <- gsub("raw", "unzipped", gsub(".zip$", "", rescrape_files))
mapply(unzip.fun, zip.names=rescrape_files, out.paths=out.paths2)

# Identify bad downloads (i.e. empty folders), take 2
dirs <- list.dirs(path = "./prism/unzipped")
nfiles <- sapply(dirs,function(x) length(list.files(x)))
rescrapes2 <- dirs[nfiles==0]

# Rescrape bad downloads, take 2
rescrape_urls <- paste0("http://services.nacse.org/prism/data/public/4km/", rescrapes2)
rescrape_urls <- gsub("ppt","ppt/",rescrape_urls)
rescrape_urls <- gsub("tmax","tmax/",rescrape_urls)
rescrape_urls <- gsub("tmin","tmin/",rescrape_urls)
rescrape_urls <- gsub("./prism/unzipped/","",rescrape_urls)
rescrape_urls
rescrape_files <- paste0(rescrapes2,".zip")
rescrape_files <- gsub("unzipped","raw",rescrape_files)
rescrape_files
mapply(down.fun, url=rescrape_urls, filename=rescrape_files)

# Unzip new downloads
out.paths3 <- gsub("raw", "unzipped", gsub(".zip$", "", rescrape_files))
mapply(unzip.fun, zip.names=rescrape_files, out.paths=out.paths3)

# Identify bad downloads (i.e. empty folders), take 3
dirs <- list.dirs(path = "./prism/unzipped")
nfiles <- sapply(dirs,function(x) length(list.files(x)))
head(nfiles)
rescrapes3 <- dirs[nfiles==0]

# Rescrape bad downloads, take 3
rescrape_urls <- paste0("http://services.nacse.org/prism/data/public/4km/", rescrapes3)
rescrape_urls <- gsub("ppt","ppt/",rescrape_urls)
rescrape_urls <- gsub("tmax","tmax/",rescrape_urls)
rescrape_urls <- gsub("tmin","tmin/",rescrape_urls)
rescrape_urls <- gsub("./prism/unzipped/","",rescrape_urls)
rescrape_urls
rescrape_files <- paste0(rescrapes3,".zip")
rescrape_files <- gsub("unzipped","raw",rescrape_files)
rescrape_files
mapply(down.fun, url=rescrape_urls, filename=rescrape_files)

# Unzip new downloads
out.paths4 <- gsub("raw", "unzipped", gsub(".zip$", "", rescrape_files))
mapply(unzip.fun, zip.names=rescrape_files, out.paths=out.paths4)

# Confirm no remaining bad downloads
dirs <- list.dirs(path = "./prism/unzipped")
nfiles <- sapply(dirs,function(x) length(list.files(x)))
length(dirs[nfiles==0])


############################################################################
############################################################################

## 3. Stack rasters, read in PGE SP coordinates and assign each a daily {precip, tmax, tmin}

# Stack daily rasters for plunking
raster.names <- list.files(path="./prism/unzipped", pattern=".bil$", full.names=TRUE, recursive=TRUE)
raster.stack.temp <- stack(raster.names[grep("20080101",raster.names)])

# Prep points to plunk
coords <- read.csv(file="./misc/pge_prem_coord_3pulls.txt")
coords <- coords[names(coords) %in% c("sp_uuid","prem_lat","prem_long")]
coords <- coords[is.na(coords$prem_lat)==0,]
coords <- coords[is.na(coords$prem_long)==0,]
coords <- as.data.frame(cbind(coords$sp_uuid, coords$prem_long, coords$prem_lat))
names(coords) <- c("sp_uuid","lon","lat")
map.data <- as.data.table(coords)
map.data <- map.data[,list(sp_uuid, lon, lat)]
map.data <- map.data[is.na(lon) == FALSE & is.na(lat) == FALSE]
store.points <- SpatialPointsDataFrame(coords=map.data[,list(lon, lat)], 
                                       data=map.data,
                                       proj4string = CRS("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"))
store.points <- spTransform(store.points, CRS(as.character(crs(raster.stack.temp))))

# Loop over sample months
for (y in 2008:2019){
  for (m in 1:12){

    # Add leading zero to month
    mm <- ifelse(m<10,paste0("0",m),paste0(m))
    yyyymm <- paste0(y,mm)
    
    # Subset rasters for the month, and stack
    raster.names.month <- raster.names[grep(yyyymm,raster.names)]
    raster.stack <- stack(raster.names.month)
    
    # Plunk
    system.time(value <- extract(raster.stack, store.points, df=TRUE))

    # Simpligy names of plunking output
    names(value) <- gsub("PRISM_", "", names(value))
    names(value) <- gsub("stable_4kmD2_", "", names(value))
    names(value) <- gsub("provisional_4kmD2_", "", names(value))
    names(value) <- gsub("_bil", "", names(value))
    names(value)
    
    # Merge back into main data frame, and reshape
    output <- cbind(map.data, value)
    output <- melt(output, id.vars=c("sp_uuid", "lon", "lat", "ID"))
    output$which <- gsub("_","",substring(output$variable,1,4))
    output$date <- gsub("_","",substring(output$variable,5,13))
    output <- as.data.frame(cbind(output$sp_uuid, output$date, output$which, output$value))
    names(output) <- c("sp_uuid","date","which","value")
    
    # Filter on precipitation, for manual reshape wide
    ppt <- output %>% filter(which=="ppt")
    ppt <- ppt[,names(ppt)!="which"]
    names(ppt)[3] <- "ppt"
    ppt$ppt <- round(as.numeric(as.character(ppt$ppt)), digits=3)
    
    # Filter on max temperature, for manual reshape wide
    tmax <- output %>% filter(which=="tmax")
    tmax <- tmax[,names(tmax)!="which"]
    names(tmax)[3] <- "tmax"
    tmax$tmax <- round(as.numeric(as.character(tmax$tmax)), digits=3)
    
    # Filter on min temperature, for manul reshape wide
    tmin <- output %>% filter(which=="tmin")
    tmin <- tmin[,names(tmin)!="which"]
    names(tmin)[3] <- "tmin"
    tmin$tmin <- round(as.numeric(as.character(tmin$tmin)), digits=3)
    
    # Join three metrics into a single wide data.frame
    output2 <- full_join(ppt,tmax, by= c("sp_uuid","date"))
    output2 <- full_join(output2,tmin, by= c("sp_uuid","date"))
    
    # Write output to csv, and remove everything from memory
    write.csv(output2, file=paste0("./prism/temp/pge_prem_coord_daily_temperatures_",yyyymm,".csv"))
    
    # Remove loop variables before next iteration
    rm(raster.names.month,raster.stack,value,output,ppt,tmax,tmin,output2)
    gc()
    
    # Print intermediate output
    print(c(as.character(Sys.time()),yyyymm))
  }  
}

############################################################################
############################################################################

## 4. Stack rasters, read in APEP pump coordinates and assign each a daily {precip, tmax, tmin}

# Stack daily rasters for plunking
raster.names <- list.files(path="./prism/unzipped", pattern=".bil$", full.names=TRUE, recursive=TRUE)
raster.stack.temp <- stack(raster.names[grep("20080101",raster.names)])

# Prep points to plunk
coords <- read.csv(file="./misc/apep_pump_coord.txt")
coords <- coords[names(coords) %in% c("latlon_group","pump_lat","pump_long")]
coords <- coords[is.na(coords$pump_lat)==0,]
coords <- coords[is.na(coords$pump_long)==0,]
coords <- as.data.frame(cbind(coords$latlon_group, coords$pump_long, coords$pump_lat))
names(coords) <- c("latlon_group","lon","lat")
map.data <- as.data.table(coords)
map.data <- map.data[,list(latlon_group, lon, lat)]
map.data <- map.data[is.na(lon) == FALSE & is.na(lat) == FALSE]
store.points <- SpatialPointsDataFrame(coords=map.data[,list(lon, lat)], 
                                       data=map.data,
                                       proj4string = CRS("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"))
store.points <- spTransform(store.points, CRS(as.character(crs(raster.stack.temp))))

# Loop over sample months
for (y in 2008:2019){
  for (m in 1:12){
    
    # Add leading zero to month
    mm <- ifelse(m<10,paste0("0",m),paste0(m))
    yyyymm <- paste0(y,mm)
    
    # Subset rasters for the month, and stack
    raster.names.month <- raster.names[grep(yyyymm,raster.names)]
    raster.stack <- stack(raster.names.month)
    
    # Plunk
    system.time(value <- extract(raster.stack, store.points, df=TRUE))
    
    # Simpligy names of plunking output
    names(value) <- gsub("PRISM_", "", names(value))
    names(value) <- gsub("stable_4kmD2_", "", names(value))
    names(value) <- gsub("provisional_4kmD2_", "", names(value))
    names(value) <- gsub("_bil", "", names(value))
    names(value)
    
    # Merge back into main data frame, and reshape
    output <- cbind(map.data, value)
    output <- melt(output, id.vars=c("latlon_group", "lon", "lat", "ID"))
    output$which <- gsub("_","",substring(output$variable,1,4))
    output$date <- gsub("_","",substring(output$variable,5,13))
    output <- as.data.frame(cbind(output$latlon_group, output$date, output$which, output$value))
    names(output) <- c("latlon_group","date","which","value")
    
    # Filter on precipitation, for manual reshape wide
    ppt <- output %>% filter(which=="ppt")
    ppt <- ppt[,names(ppt)!="which"]
    names(ppt)[3] <- "ppt"
    ppt$ppt <- round(as.numeric(as.character(ppt$ppt)), digits=3)
    
    # Filter on max temperature, for manual reshape wide
    tmax <- output %>% filter(which=="tmax")
    tmax <- tmax[,names(tmax)!="which"]
    names(tmax)[3] <- "tmax"
    tmax$tmax <- round(as.numeric(as.character(tmax$tmax)), digits=3)
    
    # Filter on min temperature, for manul reshape wide
    tmin <- output %>% filter(which=="tmin")
    tmin <- tmin[,names(tmin)!="which"]
    names(tmin)[3] <- "tmin"
    tmin$tmin <- round(as.numeric(as.character(tmin$tmin)), digits=3)
    
    # Join three metrics into a single wide data.frame
    output2 <- full_join(ppt,tmax, by= c("latlon_group","date"))
    output2 <- full_join(output2,tmin, by= c("latlon_group","date"))
    
    # Write output to csv, and remove everything from memory
    write.csv(output2, file=paste0("./prism/temp/apep_pump_coord_daily_temperatures_",yyyymm,".csv"))
    
    # Remove loop variables before next iteration
    rm(raster.names.month,raster.stack,value,output,ppt,tmax,tmin,output2)
    gc()
    
    # Print intermediate output
    print(c(as.character(Sys.time()),yyyymm))
  }  
}

############################################################################
############################################################################

## 5. Stack rasters, read in SCE SP coordinates and assign each a daily {precip, tmax, tmin}

# Stack daily rasters for plunking
raster.names <- list.files(path="./prism/unzipped", pattern=".bil$", full.names=TRUE, recursive=TRUE)
raster.stack.temp <- stack(raster.names[grep("20080101",raster.names)])

# Prep points to plunk
coords <- read.csv(file="./misc/sce_prem_coord_1pull.txt")
coords <- coords[names(coords) %in% c("sp_uuid","prem_lat","prem_long")]
coords <- coords[is.na(coords$prem_lat)==0,]
coords <- coords[is.na(coords$prem_long)==0,]
coords <- as.data.frame(cbind(coords$sp_uuid, coords$prem_long, coords$prem_lat))
names(coords) <- c("sp_uuid","lon","lat")
map.data <- as.data.table(coords)
map.data <- map.data[,list(sp_uuid, lon, lat)]
map.data <- map.data[is.na(lon) == FALSE & is.na(lat) == FALSE]
store.points <- SpatialPointsDataFrame(coords=map.data[,list(lon, lat)], 
                                       data=map.data,
                                       proj4string = CRS("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"))
store.points <- spTransform(store.points, CRS(as.character(crs(raster.stack.temp))))

# Loop over sample months
for (y in 2008:2019){
  for (m in 1:12){
  
    # Add leading zero to month
    mm <- ifelse(m<10,paste0("0",m),paste0(m))
    yyyymm <- paste0(y,mm)
    
    # Subset rasters for the month, and stack
    raster.names.month <- raster.names[grep(yyyymm,raster.names)]
    raster.stack <- stack(raster.names.month)
    
    # Plunk
    system.time(value <- extract(raster.stack, store.points, df=TRUE))
    
    # Simpligy names of plunking output
    names(value) <- gsub("PRISM_", "", names(value))
    names(value) <- gsub("stable_4kmD2_", "", names(value))
    names(value) <- gsub("provisional_4kmD2_", "", names(value))
    names(value) <- gsub("_bil", "", names(value))
    names(value)
    
    # Merge back into main data frame, and reshape
    output <- cbind(map.data, value)
    output <- melt(output, id.vars=c("sp_uuid", "lon", "lat", "ID"))
    output$which <- gsub("_","",substring(output$variable,1,4))
    output$date <- gsub("_","",substring(output$variable,5,13))
    output <- as.data.frame(cbind(output$sp_uuid, output$date, output$which, output$value))
    names(output) <- c("sp_uuid","date","which","value")
    
    # Filter on precipitation, for manual reshape wide
    ppt <- output %>% filter(which=="ppt")
    ppt <- ppt[,names(ppt)!="which"]
    names(ppt)[3] <- "ppt"
    ppt$ppt <- round(as.numeric(as.character(ppt$ppt)), digits=3)
    
    # Filter on max temperature, for manual reshape wide
    tmax <- output %>% filter(which=="tmax")
    tmax <- tmax[,names(tmax)!="which"]
    names(tmax)[3] <- "tmax"
    tmax$tmax <- round(as.numeric(as.character(tmax$tmax)), digits=3)
    
    # Filter on min temperature, for manul reshape wide
    tmin <- output %>% filter(which=="tmin")
    tmin <- tmin[,names(tmin)!="which"]
    names(tmin)[3] <- "tmin"
    tmin$tmin <- round(as.numeric(as.character(tmin$tmin)), digits=3)
    
    # Join three metrics into a single wide data.frame
    output2 <- full_join(ppt,tmax, by= c("sp_uuid","date"))
    output2 <- full_join(output2,tmin, by= c("sp_uuid","date"))
    
    # Write output to csv, and remove everything from memory
    write.csv(output2, file=paste0("./prism/temp/sce_prem_coord_daily_temperatures_",yyyymm,".csv"))
    
    # Remove loop variables before next iteration
    rm(raster.names.month,raster.stack,value,output,ppt,tmax,tmin,output2)
    gc()
    
    # Print intermediate output
    print(c(as.character(Sys.time()),yyyymm))
  }  
}

############################################################################
############################################################################
