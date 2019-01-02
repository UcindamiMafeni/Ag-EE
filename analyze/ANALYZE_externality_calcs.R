################################################
#  Script to estimate open access externality! #
################################################ 
rm(list = ls())

# install.packages("ggmap")
# install.packages("ggplot2")
# install.packages("gstat")
# install.packages("sp")
# install.packages("sf")
# install.packages("maptools")
# install.packages("rgdal")
# install.packages("rgeos")
# install.packages("raster")
# install.packages("SDMTools")
# install.packages("tidyverse")

#libP <- "C:/Program Files/Microsoft/R Open/R-3.4.4/library"

library(ggmap) #, lib.loc=libP)
library(ggplot2) #, lib.loc=libP)
library(gstat) #, lib.loc=libP)
library(sp) #, lib.loc=libP)
library(sf) #, lib.loc=libP)
library(maptools) #, lib.loc=libP)
library(rgdal) #, lib.loc=libP)
library(rgeos) #, lib.loc=libP)
library(raster) #, lib.loc=libP)
library(SDMTools) #, lib.loc=libP)
library(tidyverse)


################################
### 1. Prep basin shapefiles ###
################################

setwd("S:/Matt/ag_pump/data/spatial")

#Load Water Basins shapefile
wbasn <- readOGR(dsn = "CA_Bulletin_118_Groundwater_Basins", layer = "CA_Bulletin_118_Groundwater_Basins")
proj4string(wbasn)
wbasn <- spTransform(wbasn, CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0"))

#Load CA state outline
CAoutline <- readOGR(dsn = "State", layer = "CA_State_TIGER2016")
proj4string(CAoutline)
CAoutline <- spTransform(CAoutline, CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=GRS80 +towgs84=0,0,0"))

#Confirm water basins align with California map
ggplot() + 
  geom_polygon(data=CAoutline, aes(x=long, y=lat, group=group), 
               color="grey30", fill=NA, alpha=1) +
  geom_polygon(data=wbasn, aes(x=long, y=lat, group=group, color=rgb(0,0,1)), 
               color=rgb(0,1,0), fill=rgb(0,0,1), alpha=1) 

#Calculate area of each water basin polygon
wbasn@data$area_km2 <- area(wbasn)/1000000
data <- wbasn@data
summary(data$area_km2)

#Join sub-basin polygons into basin polygons
#wbasn2 <- unionSpatialPolygons(wbasn,wbasn@data$Basin_Numb)


##########################################################################
### 2. Import panel data of SPs locations, pump specs, and water p & q ###
##########################################################################

#Read PGE coordinates
setwd("S:/Matt/ag_pump/data/misc")
panel <- read.csv("panel_for_externality_calcs.csv",header=TRUE,sep=",",stringsAsFactors=FALSE)
panel$longitude <- as.numeric(panel$prem_lon)
panel$latitude <- as.numeric(panel$prem_lat)

#Convert to SpatialPointsDataFrame
coordinates(panel) <- ~ longitude + latitude
proj4string(panel) <- proj4string(wbasn)


############################################
### 3. Calculate open-access externality ###
############################################

# Step 1: transform data
wbasn_sf <- st_as_sf(wbasn)
wbasn_sf <- st_transform(wbasn_sf,3310)

panel_sf <- st_as_sf(panel)
panel_sf <- st_transform(panel_sf,3310)

panel_sf_june <- panel_sf[panel_sf$month==6,]
panel_sf_july <- panel_sf[panel_sf$month==7,]

# Step 2: establish distance radii
radii_miles <- c(1,2,5,10,20,30,40) # miles
radii <- radii_miles*1609.344  # convert to meters, the units of the SF objects
r <- length(radii)

# Step 3: Draw circles around each unit
for (i in 1:r){
  assign(paste0("circles",i), st_buffer(panel_sf_june, dist=radii[i]))
}  

# Step 4: Intersect circles with basin polygons (COME BACK TO THIS)
# wbasn_by_sp merge
# circles1 <- st_intersection(wbasn_by_sp, circles1)

# Step 5: Calculate area of each circle (sq meters)
for (i in 1:r){
  assign(paste0("areas",i), st_area(get(paste0("circles",i))))
}  

# Step 6: define groundwater variables and size of decrease in pumping for each unit i
stub      <- "rast_dd_mth_2SP"
P_var     <- paste0("mean_p_af_",stub)
Q_var     <- paste0("af_",stub)
kwhaf_var <- paste0("kwhaf_",stub)
swl_var   <- paste0("gw_",gsub("_dd_","_depth_",stub))
dd_var    <- ifelse(grepl("ddhat",stub),paste0("ddhat_",gsub("_dd_","_",stub)),"drwdwn_apep")
tdh_var   <- paste0("tdh_",stub)
ope_var   <- paste0("ope_",stub)
eps_var   <- "elast_water"

# Step 7: Calculate unit i's lost CS (for June) from pumping less
dCS_i          <- panel_sf_june[[P_var]]
dCS_i          <- as.data.frame(dCS_i)
names(dCS_i)   <- "P_old"
dCS_i$Q_old    <- panel_sf_june[[Q_var]]
dCS_i$delta_Q  <- ifelse(dCS_i$Q_old>1,1,NaN) #start with 1 AF, and see what happens\
dCS_i$eps      <- panel_sf_june[[eps_var]]
dCS_i$Q_new    <- dCS_i$Q_old - dCS_i$delta_Q
dCS_i$P_new    <- (dCS_i$Q_new/dCS_i$Q_old)^(1/(dCS_i$eps))*dCS_i$P_old
dCS_i$integral <- (dCS_i$Q_old/(dCS_i$eps+1))*((dCS_i$P_old^(-dCS_i$eps))*(dCS_i$P_new^(dCS_i$eps+1))-dCS_i$P_old)
#dCS_i$integral2<- (dCS_i$Q_new * dCS_i$P_new - dCS_i$Q_old * dCS_i$P_old)/(dCS_i$eps+1)
dCS_i$rectangle<- dCS_i$Q_new * (dCS_i$P_new - dCS_i$P_old)
dCS_i$dCS_i    <- -(dCS_i$integral - dCS_i$rectangle)

# Step 8: Calculate how much the water level rises in each circle when unit i pumps less
for (i in 1:r){
  assign(paste0("delta_swl",i), dCS_i$delta_Q/(get(paste0("areas",i))/4046.856)) #convert from sq meters to acres
}  

# Step 9: Grab set J of July points in each i-specific June circle
for (i in 1:r){
  assign(paste0("pts_in_circles",i), st_intersects(get(paste0("circles",i)),panel_sf_july)) 
}  

# Step 10: Remove unit i from each i-specific set of neighbors J
sp_matches <- match(panel_sf_june$sp_group,panel_sf_july$sp_group)
for (i in 1:r){
  assign(paste0("pts_in_circles",i), mapply(function(x,y) setdiff(x,y), get(paste0("pts_in_circles",i)), sp_matches)) 
}  

# Step 11: For each i-specific et of J neighbors, subset the correct rows of July panel 
for (i in 1:r){
  assign(paste0("dfs_circles",i), lapply(get(paste0("pts_in_circles",i)), function(x) panel_sf_july[x,])) 
}  

# Step 12: Impose i-specific uniform increases in depth, and calculate new SWL for each unit j in each set J
for (i in 1:r){
  swl_news <- mapply(function(x,y) x[[swl_var]] - as.numeric(y), 
                     get(paste0("dfs_circles",i)),get(paste0("delta_swl",i))
              )
  assign(paste0("dfs_circles",i), Map(cbind, get(paste0("dfs_circles",i)), swl_new = swl_news))

  # Something's getting systematically messed up in the conversion from depth to lift, 
  # either from how I've averaged the "tdh_adder" variable across multiple pumps in an SP-month
  # or from how I've interpolated it across time. This makes not sense to me, because it's all additive!
  # As a stop-gap, I'm just skipping straight to TDH (i.e. lift) and subtracting delta_swl from that...
  # Need to come back and fix this!
  tdh_news <- mapply(function(x,y) x[[tdh_var]] - as.numeric(y), 
                     get(paste0("dfs_circles",i)),get(paste0("delta_swl",i))
  )
  assign(paste0("dfs_circles",i), Map(cbind, get(paste0("dfs_circles",i)), tdh_new = tdh_news))
}

# Step 13: Calculate depth_new --> lift_new --> kwhaf_new --> P_new --> dCS_j
for (i in 1:r){
  
  #lift_news <- lapply(get(paste0("dfs_circles",i)), function(x) x[["swl_new"]] + x[[dd_var]] + x[["tdh_adder"]])
  lift_news <- lapply(get(paste0("dfs_circles",i)), function(x) x[["tdh_new"]])
  
  kwhaf_news <- mapply(function(x,y) y * 102.26728 / x[[ope_var]], get(paste0("dfs_circles",i)), lift_news)

  P_news <- mapply(function(x,y) x[["mean_p_kwh"]] * y, get(paste0("dfs_circles",i)), kwhaf_news)
  
  dCS_js <- mapply(function(x,y) {
                   -(x[[Q_var]]/(x[[eps_var]]+1))*((x[[P_var]]^(-x[[eps_var]]))*(y^(x[[eps_var]]+1))-x[[P_var]])
                    }, get(paste0("dfs_circles",i)), P_news
                   )
  
  assign(paste0("dfs_circles",i), Map(cbind, get(paste0("dfs_circles",i)), lift_new = lift_news))
  assign(paste0("dfs_circles",i), Map(cbind, get(paste0("dfs_circles",i)), kwhaf_new = kwhaf_news))
  assign(paste0("dfs_circles",i), Map(cbind, get(paste0("dfs_circles",i)), P_new = P_news))
  assign(paste0("dfs_circles",i), Map(cbind, get(paste0("dfs_circles",i)), dCS_j = dCS_js))
}

# Step 14: Store outputs -- sum (positive) changes in CS_j, avg price decrease, number of (positive) units
for (i in 1:r){
  
  sum_dCS_js <- lapply(get(paste0("dfs_circles",i)), function(x) sum(x[["dCS_j"]]))
  
  sum_dCS_j_poss <- lapply(get(paste0("dfs_circles",i)), function(x) sum(x[x[["dCS_j"]]>0,][["dCS_j"]]))
                          
  mean_dP_js <- lapply(get(paste0("dfs_circles",i)), function(x) mean(x[[P_var]] - x[["P_new"]]))

  n_js <- lapply(get(paste0("dfs_circles",i)), function(x) nrow(x))
  
  n_j_poss <- lapply(get(paste0("dfs_circles",i)), function(x) nrow(x[x[[Q_var]]>0,]))
  
  assign(paste0("out_circles",i), Map(cbind, sum_dCS_j = sum_dCS_js,  sum_dCS_j_pos = sum_dCS_j_poss))
  assign(paste0("out_circles",i), Map(cbind, get(paste0("out_circles",i)), mean_dP_j = mean_dP_js))
  assign(paste0("out_circles",i), Map(cbind, get(paste0("out_circles",i)), n_j = n_js))
  assign(paste0("out_circles",i), Map(cbind, get(paste0("out_circles",i)), n_j_pos = n_j_poss))
}  
  
# Step 15: Merge j outputs into dCS_i data frame
for (i in 1:r){
  out_df <- as.data.frame(Reduce(rbind, get(paste0("out_circles",i))))
  j <- radii_miles[i]
  names(out_df) <- sapply(names(out_df), function(x) paste0(x,j))
  assign(paste0("out_df",i), out_df)
}
ids <- as.data.frame(panel_sf_june)
ids <- ids[ names(ids)[names(ids) %in% c("sp_uuid","modate","basin_group","in_regs","basin_dist",
                                         "basin_dist_miles","basin_id","basin_name")] ]
out_df <- cbind(ids,dCS_i)
for (i in 1:r){
  out_df <- cbind(out_df,get(paste0("out_df",i)))
}

# Step 16: Export results
filename <- paste0("externality_calcs_june2016_",stub,".csv")
write.csv(out_df, file=filename , row.names=FALSE, quote=FALSE)
