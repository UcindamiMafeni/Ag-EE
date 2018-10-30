############################################################################
### CODE TO SCRAPE DAILY MAX/MIN TEMPERATURE AT sERVICE POINTS AND PUMPS ### 
############################################################################

############################################################################
################################# SETUP ####################################

rm(list = ls())

# Script to download PRISM weather data, and construct a daily maximum temperature at all coal palnts
# Lightly edited from Dave McLaughlin's excellent R script

library(raster)
library(sp)
library(data.table)
library(reshape2)
library(lubridate)

setwd("S:/Matt/ag_pump/data")


############################################################################
############################################################################

## 1. Download daily max/min temperature files from PRISM

date <- seq(as.Date("2008-01-01"),as.Date("2017-12-31"),1)
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

date.df$filenames_tmax <- paste0("./prism/raw/", "tmax", date.df$date.url, ".zip")
date.df$filenames_tmin <- paste0("./prism/raw/", "tmin", date.df$date.url, ".zip")

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

mapply(down.fun, url = date.df$urls_tmax,
       filename=date.df$filenames_tmax)
mapply(down.fun, url = date.df$urls_tmin,
       filename=date.df$filenames_tmin)


############################################################################
############################################################################

## 2. Unzip raw data folders

zip.names <- list.files(path="./prism/raw", pattern=".zip$", full.names=TRUE)

out.paths <- gsub("raw", "unzipped", gsub(".zip$", "", zip.names))

unzip.fun <- function(zip.names, out.paths){
  unzip(zipfile = zip.names, exdir=out.paths)
  return(NULL)
}

mapply(unzip.fun, zip.names=zip.names,
       out.paths=out.paths)


############################################################################
############################################################################

## 3. Stack rasters, read in SP coordinates and assign each a daily max temperature

raster.names <- list.files(path="./prism/unzipped", pattern=".bil$", full.names=TRUE, recursive=TRUE)
raster.stack <- stack(raster.names)

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

store.points <- spTransform(store.points, CRS(as.character(crs(raster.stack))))

system.time(value <- extract(raster.stack, store.points, df=TRUE))

names(value) <- gsub("PRISM_(.*)_stable_4kmD1_(.*)_bil", "\\1\\2", names(value))

output <- cbind(map.data, value)
output <- melt(output, id.vars=c("sp_uuid", "lon", "lat", "ID"))
output$date_tmax <- gsub("PRISM_tmax_stable_4kmD1_", "", output$variable)
output$date_tmin <- gsub("PRISM_tmin_stable_4kmD1_", "", output$variable)
output$date_tmax <- gsub("_bil", "", output$date_tmax)
output$date_tmin <- gsub("_bil", "", output$date_tmin)
output <- as.data.frame(cbind(output$facilid, output$lon, output$lat, output$date, output$value))
names(output) <- c("facilid","lon","lat","date","tmax")

write.csv(output, file="./prism/prism_daily_max_temperature.csv")


############################################################################
############################################################################





