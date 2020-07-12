rm(list = ls())

library(ggmap) #, lib.loc=libP)
library(ggplot2) #, lib.loc=libP)
library(gstat) #, lib.loc=libP)
library(sp) #, lib.loc=libP)
library(maptools) #, lib.loc=libP)
library(rgdal) #, lib.loc=libP)
library(rgeos) #, lib.loc=libP)
library(raster) #, lib.loc=libP)
library(SDMTools) #, lib.loc=libP)
library(sf)
library(tidyverse)
library(foreign)
library(haven)

# paths of objects

path_clu <- "T:/Projects/Pump Data/data/cleaned_spatial/CLU/clu_poly"
path_probs <- "T:/Projects/Pump Data/data/results"
#path_conc <- "C:/Users/clohani/Desktop/Code_Chinmay/"
path_out <- "T:/Projects/Pump Data/output"
path_outline <- "T:/Projects/Pump Data/data/spatial/ca-state-boundary"
#path_counties <- "T:/Projects/Pump Data/data/spatial/Counties"

# "Intersected_CLU_county.rds"
clu <- st_read(file.path(path_clu, "clu_poly.shp")) %>%
  mutate(CLU_ID= as.factor(CLU_ID))
outline <- st_read(file.path(path_outline, "CA_State_TIGER2016.shp")) %>%
  st_transform(.,4326)
clu_data <- read_dta(file.path(path_probs,"probit_crop_choice_clu.dta")) %>%
  mutate(clu_id= as.factor(clu_id))
#counties <- st_read(file.path(path_counties, "CA_Counties_TIGER2016.shp")) %>%
#    mutate(County=NAME)


# merging data to shapefile
clu_prob <- clu_data %>%
  rename(CLU_ID= clu_id) %>%
  inner_join(clu,.) 

# average to a larger shapefile (coarsen)
if (1==0) {
  county_prob <- clu_prob %>%
    st_set_geometry(NULL) %>%
    mutate(diff= prob_10tax_1 - prob_0tax_1) %>%
    inner_join(counties,.) %>%
    group_by(County) %>%
    mutate(diff=mean(diff))%>%
    slice(1) %>%
    ungroup()
}

#plot of just clus
if (1==1) {
  plot_pts <- ggplot() +
    geom_sf(data=outline,color="grey30", fill=NA, alpha=1, show.legend = FALSE) +
    geom_sf(data=clu_prob,lwd=0) +
    theme_bw() +
    theme(plot.background = element_rect(fill='white'),
          panel.grid.major = element_line(colour = "transparent"),
          panel.grid.minor = element_line(colour = "transparent"),
          panel.border = element_blank(),
          plot.margin = margin(t=0,r=0,b=0,l=0)
    )
  ggsave(file.path(path_out, paste("CLUs_included.png")),dpi=300, plot = plot_pts)
}

#Code to generate centroids safely
crs <- st_crs(clu_prob)
clu_centroids <- clu_prob %>%
  st_set_geometry(NULL)
clu_centroids$geometry <- clu_prob %>%
  st_centroid() %>% 
  st_transform(., st_crs(clu_prob)) %>%
  st_geometry()
clu_centroids <- st_as_sf(clu_centroids)

# small slice for testing
clu_test <- clu_centroids %>%
  slice(1:100)

### Generate plots for different types of crops, defining palette for each ###

## *** Category 1: No crop

## Using the purple scheme for 9 data classes: https://colorbrewer2.org/#type=sequential&scheme=Blues&n=9
cls <- c("#fcfbfd","#efedf5","#dadaeb","#bcbddc","#9e9ac8","#807dba","#6a51a3","#54278f","#3f007d")
cls <- c("#f7fbff", "#0080ff", "#1a476f")

fun <- function(varname) {
  clu_centroids <- clu_centroids %>%
    mutate(diff:= (!!as.name(varname) - prob_0tax_1)*100)
  
  setwd(path_out)
  plot_all <- ggplot() +
    ggtitle("No crop \n \n") +
    geom_sf(data=outline,color="grey30", fill=NA, alpha=1, show.legend = FALSE) +
    geom_sf(data=clu_centroids, aes(color= diff),size=1, lwd = 0,inherit.aes=FALSE) + 
    #coord_sf(xlim=c(-122,-118.5),ylim = c(34.5, 37.5),expand = FALSE) +
    coord_sf() +
    ## Note: here I'm letting ggplot choose the span of lowest and highest points for each plot separately
    ## if we want to fix the lowest and highest point in the difference
    ## use option `limits = c(lowest,highest)` inside scale_color_gradientn()
    scale_color_gradient2(low="#6d109c", high = "#0080ff",space = "Lab", name = "Change in probability \n relative to no tax\n (pp)", limits = (c(-4,4)))+ 
    labs(fill = "Change in probability relative to no tax (pp).") +
    theme_bw() +
    theme(plot.background = element_rect(fill='white'),
          panel.grid.major = element_line(colour = "transparent"),
          panel.grid.minor = element_line(colour = "transparent"),
          panel.border = element_blank(),
          plot.margin = margin(t=0,r=0,b=0,l=0),
          plot.title = element_text(hjust=0.5, size=22),
          legend.title.align=0.5,
          legend.position = "none"
    ) 
  dev.off()
  ggsave(file.path(path_out, paste0("Clu_",varname,"_fullstate.png")),dpi=400, plot = plot_all)
}

varlist <- c("prob_5tax_1","prob_10tax_1","prob_15tax_1","prob_20tax_1","prob_25tax_1")

varlist <- c("prob_10tax_1")

lapply(varlist,fun)




## *** Category 2: Annuals

## Using the blue scheme for 9 data classes: https://colorbrewer2.org/#type=sequential&scheme=Purples&n=9
cls <- c("#f7fbff", "#0080ff", "#1a476f")

fun <- function(varname) {
  clu_centroids <- clu_centroids %>%
    mutate(diff:= (!!as.name(varname) - prob_0tax_2)*100)
  
  setwd(path_out)
  plot_all <- ggplot() +
    ggtitle("Annuals \n \n") +
    geom_sf(data=outline,color="grey30", fill=NA, alpha=1, show.legend = FALSE) +
    geom_sf(data=clu_centroids, aes(color= diff),size=1, lwd = 0,inherit.aes=FALSE) + 
    coord_sf(xlim=c(-122,-118.5),ylim = c(34.5, 37.5),expand = FALSE) +
    ## Note: here I'm letting ggplot choose the span of lowest and highest points for each plot separately
    ## if we want to fix the lowest and highest point in the difference
    ## use option `limits = c(lowest,highest)` inside scale_color_gradientn()
    scale_color_gradient2(low="#6d109c", high = "#0080ff",space = "Lab", name = "Change in probability \n relative to no tax\n (pp)", limits = (c(-4,4)))+ 
    labs(fill = "Change in probability relative to no tax (pp).") +
    theme_bw() +
    theme(plot.background = element_rect(fill='white'),
          panel.grid.major = element_line(colour = "transparent"),
          panel.grid.minor = element_line(colour = "transparent"),
          panel.border = element_blank(),
          plot.margin = margin(t=0,r=0,b=0,l=0),
          plot.title = element_text(hjust=0.5, size=22),
          legend.title.align=0.5,
          legend.position = "none"
    ) 
  dev.off()
  ggsave(file.path(path_out, paste0("Clu_",varname,".png")),dpi=400, plot = plot_all)
}

varlist <- c("prob_10tax_2")

lapply(varlist,fun)


## *** Category 3: Fruit or nut perennials

## Using the orange scheme for 9 data classes: https://colorbrewer2.org/#type=sequential&scheme=Oranges&n=9
cls <- c("#fff5eb","#fee6ce","#fdd0a2","#fdae6b","#fd8d3c","#f16913","#d94801","#a63603","#7f2704")

cls <- c("#f7fbff", "#0080ff", "#1a476f")

fun <- function(varname) {
  clu_centroids <- clu_centroids %>%
    mutate(diff:= (!!as.name(varname) - prob_0tax_3)*100)
  
  setwd(path_out)
  plot_all <- ggplot() +
    ggtitle("Fruit/nut perennials \n \n") +
    geom_sf(data=outline,color="grey30", fill=NA, alpha=1, show.legend = FALSE) +
    geom_sf(data=clu_centroids, aes(color= diff),size=1, lwd = 0,inherit.aes=FALSE) + 
    coord_sf(xlim=c(-122,-118.5),ylim = c(34.5, 37.5),expand = FALSE) +
    ## Note: here I'm letting ggplot choose the span of lowest and highest points for each plot separately
    ## if we want to fix the lowest and highest point in the difference
    ## use option `limits = c(lowest,highest)` inside scale_color_gradientn()
    scale_color_gradient2(low="#6d109c", high = "#0080ff",space = "Lab", name = "Change in probability \n relative to no tax\n (pp)", limits = (c(-4,4)))+ 
    labs(fill = "Change in probability relative to no tax (pp).") +
    theme_bw() +
    theme(plot.background = element_rect(fill='white'),
          panel.grid.major = element_line(colour = "transparent"),
          panel.grid.minor = element_line(colour = "transparent"),
          panel.border = element_blank(),
          plot.margin = margin(t=0,r=0,b=0,l=0),
          plot.title = element_text(hjust=0.5, size=22),
          legend.title.align=0.5,
          legend.position = "none"
    ) 
  dev.off()
  ggsave(file.path(path_out, paste0("Clu_",varname,".png")),dpi=400, plot = plot_all)
}

varlist <- c("prob_5tax_1","prob_10tax_1","prob_15tax_1","prob_20tax_1","prob_25tax_1")

varlist <- c("prob_10tax_3")

lapply(varlist,fun)



## *** Category 4: Other perennials

## Using the green scheme for 9 data classes: https://colorbrewer2.org/#type=sequential&scheme=Greens&n=9
cls <- c("#f7fcf5","#e5f5e0","#c7e9c0","#a1d99b","#74c476","#41ab5d","#238b45","#006d2c","#00441b")
cls <- c("#f7fbff", "#0080ff", "#1a476f")

fun <- function(varname) {
  clu_centroids <- clu_centroids %>%
    mutate(diff:= (!!as.name(varname) - prob_0tax_4)*100)
  
  setwd(path_out)
  plot_all <- ggplot() +
    ggtitle("Other perennials \n \n") +
    geom_sf(data=outline,color="grey30", fill=NA, alpha=1, show.legend = FALSE) +
    geom_sf(data=clu_centroids, aes(color= diff),size=1, lwd = 0,inherit.aes=FALSE) + 
    coord_sf(xlim=c(-122,-118.5),ylim = c(34.5, 37.5),expand = FALSE) +
    ## Note: here I'm letting ggplot choose the span of lowest and highest points for each plot separately
    ## if we want to fix the lowest and highest point in the difference
    ## use option `limits = c(lowest,highest)` inside scale_color_gradientn()
    scale_color_gradient2(low="#6d109c", high = "#0080ff",space = "Lab", name = "Change in probability \n relative to no tax\n (pp)", limits = (c(-4,4)))+ 
    labs(fill = "Change in probability relative to no tax (pp).") +
    theme_bw() +
    theme(plot.background = element_rect(fill='white'),
          panel.grid.major = element_line(colour = "transparent"),
          panel.grid.minor = element_line(colour = "transparent"),
          panel.border = element_blank(),
          plot.margin = margin(t=0,r=0,b=0,l=0),
          plot.title = element_text(hjust=0.5, size=22),
          legend.title.align=0.5,
          legend.position = "none"
    ) 
  dev.off()
  ggsave(file.path(path_out, paste0("Clu_",varname,".png")),dpi=400, plot = plot_all)
}

varlist <- c("prob_5tax_1","prob_10tax_1","prob_15tax_1","prob_20tax_1","prob_25tax_1")

varlist <- c("prob_10tax_4")

lapply(varlist,fun)













## *** Category 1: No crop

## Using the purple scheme for 9 data classes: https://colorbrewer2.org/#type=sequential&scheme=Blues&n=9
cls <- c("#fcfbfd","#efedf5","#dadaeb","#bcbddc","#9e9ac8","#807dba","#6a51a3","#54278f","#3f007d")
cls <- c("#f7fbff", "#0080ff", "#1a476f")

fun <- function(varname) {
  clu_centroids <- clu_centroids %>%
    mutate(diff:= (!!as.name(varname) - prob_0tax_1)*100)
  
  setwd(path_out)
  plot_all <- ggplot() +
    ggtitle("No crop \n") +
    geom_sf(data=outline,color="grey30", fill=NA, alpha=1, show.legend = FALSE) +
    geom_sf(data=clu_centroids, aes(color= diff),size=1, lwd = 0,inherit.aes=FALSE) + 
    #coord_sf(xlim=c(-122,-118.5),ylim = c(34.5, 37.5),expand = FALSE) +
    coord_sf() +
    ## Note: here I'm letting ggplot choose the span of lowest and highest points for each plot separately
    ## if we want to fix the lowest and highest point in the difference
    ## use option `limits = c(lowest,highest)` inside scale_color_gradientn()
    scale_color_gradient2(low="#6d109c", high = "#0080ff",space = "Lab", name = "Change in probability \n relative to no tax\n (pp)", limits = (c(-5,5)))+ 
    labs(fill = "Change in probability relative to no tax (pp).") +
    theme_bw() +
    theme(plot.background = element_rect(fill='white'),
          panel.grid.major = element_line(colour = "transparent"),
          panel.grid.minor = element_line(colour = "transparent"),
          panel.border = element_blank(),
          plot.margin = margin(t=0,r=0,b=0,l=0),
          plot.title = element_text(hjust=0.5, size=22),
          legend.title.align=0.5,
          legend.position = "none",
          axis.title.x = element_blank(),
          axis.title.y = element_blank(),
          axis.text.x = element_blank(),
          axis.text.y = element_blank(),
          axis.ticks.x = element_blank(),
          axis.ticks.y = element_blank()
    ) 
  dev.off()
  ggsave(file.path(path_out, paste0("Clu_",varname,"_fullstate.png")),dpi=400, plot = plot_all)
}

varlist <- c("prob_5tax_1","prob_10tax_1","prob_15tax_1","prob_20tax_1","prob_25tax_1")

varlist <- c("prob_10tax_1")

lapply(varlist,fun)




## *** Category 2: Annuals

## Using the blue scheme for 9 data classes: https://colorbrewer2.org/#type=sequential&scheme=Purples&n=9
cls <- c("#f7fbff", "#0080ff", "#1a476f")

fun <- function(varname) {
  clu_centroids <- clu_centroids %>%
    mutate(diff:= (!!as.name(varname) - prob_0tax_2)*100)
  
  setwd(path_out)
  plot_all <- ggplot() +
    ggtitle("Annuals \n") +
    geom_sf(data=outline,color="grey30", fill=NA, alpha=1, show.legend = FALSE) +
    geom_sf(data=clu_centroids, aes(color= diff),size=1, lwd = 0,inherit.aes=FALSE) + 
    #coord_sf(xlim=c(-122,-118.5),ylim = c(34.5, 37.5),expand = FALSE) +
    coord_sf() +
    ## Note: here I'm letting ggplot choose the span of lowest and highest points for each plot separately
    ## if we want to fix the lowest and highest point in the difference
    ## use option `limits = c(lowest,highest)` inside scale_color_gradientn()
    scale_color_gradient2(low="#6d109c", high = "#0080ff",space = "Lab", name = "Change in probability \n relative to no tax\n (pp)", limits = (c(-5,5)))+ 
    labs(fill = "Change in probability relative to no tax (pp).") +
    theme_bw() +
    theme(plot.background = element_rect(fill='white'),
          panel.grid.major = element_line(colour = "transparent"),
          panel.grid.minor = element_line(colour = "transparent"),
          panel.border = element_blank(),
          plot.margin = margin(t=0,r=0,b=0,l=0),
          plot.title = element_text(hjust=0.5, size=22),
          legend.title.align=0.5,
          legend.position = "none",
          axis.title.x = element_blank(),
          axis.title.y = element_blank(),
          axis.text.x = element_blank(),
          axis.text.y = element_blank(),
          axis.ticks.x = element_blank(),
          axis.ticks.y = element_blank()
    ) 
  dev.off()
  ggsave(file.path(path_out, paste0("Clu_",varname,"_fullstate.png")),dpi=400, plot = plot_all)
}

varlist <- c("prob_10tax_2")

lapply(varlist,fun)


## *** Category 3: Fruit or nut perennials

## Using the orange scheme for 9 data classes: https://colorbrewer2.org/#type=sequential&scheme=Oranges&n=9
cls <- c("#fff5eb","#fee6ce","#fdd0a2","#fdae6b","#fd8d3c","#f16913","#d94801","#a63603","#7f2704")

cls <- c("#f7fbff", "#0080ff", "#1a476f")

fun <- function(varname) {
  clu_centroids <- clu_centroids %>%
    mutate(diff:= (!!as.name(varname) - prob_0tax_3)*100)
  
  setwd(path_out)
  plot_all <- ggplot() +
    ggtitle("Fruit/nut perennials \n") +
    geom_sf(data=outline,color="grey30", fill=NA, alpha=1, show.legend = FALSE) +
    geom_sf(data=clu_centroids, aes(color= diff),size=1, lwd = 0,inherit.aes=FALSE) + 
    #coord_sf(xlim=c(-122,-118.5),ylim = c(34.5, 37.5),expand = FALSE) +
    coord_sf() +
    ## Note: here I'm letting ggplot choose the span of lowest and highest points for each plot separately
    ## if we want to fix the lowest and highest point in the difference
    ## use option `limits = c(lowest,highest)` inside scale_color_gradientn()
    scale_color_gradient2(low="#6d109c", high = "#0080ff",space = "Lab", name = "Change in probability \n relative to no tax\n (pp)", limits = (c(-5,5)))+ 
    labs(fill = "Change in probability relative to no tax (pp).") +
    theme_bw() +
    theme(plot.background = element_rect(fill='white'),
          panel.grid.major = element_line(colour = "transparent"),
          panel.grid.minor = element_line(colour = "transparent"),
          panel.border = element_blank(),
          plot.margin = margin(t=0,r=0,b=0,l=0),
          plot.title = element_text(hjust=0.5, size=22),
          legend.title.align=0.5,
          legend.position = "none",
          axis.title.x = element_blank(),
          axis.title.y = element_blank(),
          axis.text.x = element_blank(),
          axis.text.y = element_blank(),
          axis.ticks.x = element_blank(),
          axis.ticks.y = element_blank()
    ) 
  
  dev.off()
  ggsave(file.path(path_out, paste0("Clu_",varname,"_fullstate.png")),dpi=400, plot = plot_all)
}

varlist <- c("prob_5tax_1","prob_10tax_1","prob_15tax_1","prob_20tax_1","prob_25tax_1")

varlist <- c("prob_10tax_3")

lapply(varlist,fun)



## *** Category 4: Other perennials

## Using the green scheme for 9 data classes: https://colorbrewer2.org/#type=sequential&scheme=Greens&n=9
cls <- c("#f7fcf5","#e5f5e0","#c7e9c0","#a1d99b","#74c476","#41ab5d","#238b45","#006d2c","#00441b")
cls <- c("#f7fbff", "#0080ff", "#1a476f")

fun <- function(varname) {
  clu_centroids <- clu_centroids %>%
    mutate(diff:= (!!as.name(varname) - prob_0tax_4)*100)
  
  setwd(path_out)
  plot_all <- ggplot() +
    ggtitle("Other perennials \n") +
    geom_sf(data=outline,color="grey30", fill=NA, alpha=1, show.legend = FALSE) +
    geom_sf(data=clu_centroids, aes(color= diff),size=1, lwd = 0,inherit.aes=FALSE) + 
    #coord_sf(xlim=c(-122,-118.5),ylim = c(34.5, 37.5),expand = FALSE) +
    coord_sf() +
    ## Note: here I'm letting ggplot choose the span of lowest and highest points for each plot separately
    ## if we want to fix the lowest and highest point in the difference
    ## use option `limits = c(lowest,highest)` inside scale_color_gradientn()
    scale_color_gradient2(low="#6d109c", high = "#0080ff",space = "Lab", name = "Change in probability \n relative to no tax\n (pp)", limits = (c(-5,5)))+ 
    labs(fill = "Change in probability relative to no tax (pp).") +
    theme_bw() +
    theme(plot.background = element_rect(fill='white'),
          panel.grid.major = element_line(colour = "transparent"),
          panel.grid.minor = element_line(colour = "transparent"),
          panel.border = element_blank(),
          plot.margin = margin(t=0,r=0,b=0,l=0),
          plot.title = element_text(hjust=0.5, size=22),
          legend.title.align=0.5,
          legend.position = "none",
          axis.title.x = element_blank(),
          axis.title.y = element_blank(),
          axis.text.x = element_blank(),
          axis.text.y = element_blank(),
          axis.ticks.x = element_blank(),
          axis.ticks.y = element_blank()
    ) 
  
  dev.off()
  ggsave(file.path(path_out, paste0("Clu_",varname,"_fullstate.png")),dpi=400, plot = plot_all)
}

varlist <- c("prob_5tax_1","prob_10tax_1","prob_15tax_1","prob_20tax_1","prob_25tax_1")

varlist <- c("prob_10tax_4")

lapply(varlist,fun)





## *** Category 4: Other perennials

## Using the green scheme for 9 data classes: https://colorbrewer2.org/#type=sequential&scheme=Greens&n=9
cls <- c("#f7fcf5","#e5f5e0","#c7e9c0","#a1d99b","#74c476","#41ab5d","#238b45","#006d2c","#00441b")
cls <- c("#f7fbff", "#0080ff", "#1a476f")

fun <- function(varname) {
  clu_centroids <- clu_centroids %>%
    mutate(diff:= (!!as.name(varname) - prob_0tax_4)*100)
  
  setwd(path_out)
  plot_all <- ggplot() +
    ggtitle("Other perennials \n") +
    geom_sf(data=outline,color="grey30", fill=NA, alpha=1, show.legend = FALSE) +
    geom_sf(data=clu_centroids, aes(color= diff),size=1, lwd = 0,inherit.aes=FALSE) + 
    #coord_sf(xlim=c(-122,-118.5),ylim = c(34.5, 37.5),expand = FALSE) +
    coord_sf() +
    ## Note: here I'm letting ggplot choose the span of lowest and highest points for each plot separately
    ## if we want to fix the lowest and highest point in the difference
    ## use option `limits = c(lowest,highest)` inside scale_color_gradientn()
    scale_color_gradient2(low="#6d109c", high = "#0080ff",space = "Lab", name = "Change in probability \n relative to no tax\n (pp)", limits = (c(-5,5)))+ 
    labs(fill = "Change in probability relative to no tax (pp).") +
    theme_bw() +
    theme(plot.background = element_rect(fill='white'),
          panel.grid.major = element_line(colour = "transparent"),
          panel.grid.minor = element_line(colour = "transparent"),
          panel.border = element_blank(),
          plot.margin = margin(t=0,r=0,b=0,l=0),
          plot.title = element_text(hjust=0.5, size=22),
          legend.title.align=0.5,
          axis.title.x = element_blank(),
          axis.title.y = element_blank(),
          axis.text.x = element_blank(),
          axis.text.y = element_blank(),
          axis.ticks.x = element_blank(),
          axis.ticks.y = element_blank()
    ) 
  
  dev.off()
  ggsave(file.path(path_out, paste0("Clu_",varname,"_legend.png")),dpi=400, plot = plot_all)
}

varlist <- c("prob_5tax_1","prob_10tax_1","prob_15tax_1","prob_20tax_1","prob_25tax_1")

varlist <- c("prob_10tax_4")

lapply(varlist,fun)
