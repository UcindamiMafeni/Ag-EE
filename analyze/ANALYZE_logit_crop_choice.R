########################################################################
## Script to model crop choice as a multinomial logit discreet choice ##
########################################################################

rm(list = ls())

library(glue)
library(haven)
library(tidyverse)
library(mlogit)
library(dfidx)

dirpath <- 'T:/Projects/Pump Data'
dirpath_data <- glue('{dirpath}/data')

################################################
################################################

### load annual water dataset
water_data <- glue('{dirpath_data}/merged_pge/sp_annual_water_panel.dta') %>% 
  read_dta()
## drop bad observations
water_data <- water_data %>% 
  filter(flag_nem == 0, 
         flag_geocode_badmiss == 0,
         flag_irregular_bill == 0,
         flag_weird_cust == 0,
         flag_weird_pump == 0,
         mode_Annual + mode_FruitNutPerennial + 
           mode_OtherPerennial + mode_Noncrop <= 1)
## create modal crop type factor variable
water_data <- water_data %>% 
  mutate(mode_crop = if_else(mode_Annual == 1, 
                             1,
                             if_else(mode_FruitNutPerennial == 1, 
                                     2,
                                     if_else(mode_OtherPerennial == 1, 
                                             3, 
                                             0)))) %>% 
  mutate(mode_crop = as.factor(mode_crop))
## format year, county, and year-county as factors
water_data <- water_data %>% 
  mutate(year_county = glue('{year}_{county_group}'),
         year = as.factor(year),
         county_group = as.factor(county_group),
         year_county = as.factor(year_county))

### estimate model with county and year fixed effects 
## format dataset for mlogit
water_data_idx <- water_data %>% 
  mutate(uniq_id = 1:n()) %>% 
  select(uniq_id, sp_uuid, year, county_group, year_county, 
         mode_crop, mean_p_af_rast_dd_mth_2SP, mean_p_kwh_ag_default) %>%
  as.data.frame() %>% 
  dfidx(shape = 'wide', choice = 'mode_crop', idx = list(c('uniq_id', 'sp_uuid')))
