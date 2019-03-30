



### This script generates part 2 of Xinyi's memo on calibrating EE subsidy and expected savings
### Replace the file "data_1024.dta" with "ag_pump/data/merged/sp_apep_proj_merged_anon.dta"

### (This is an anonymized version of "ag_pump/data/merged/sp_apep_proj_merged_anon.dta", with the
### de-anonymous crosswalk "ag_pump/data/merged/sp_apep_proj_merged_anon_xwalk.dta")



##--------------------------2. Dealing with Multiple Tests----------------------------------------------------------
###2.1  Creation of the project dataset------------------------------------
library(haven)
data_1024 <- read_dta("E:/Chicago/Work/EPIC/data_1024.dta")
project <- data_1024[data_1024[,'linked_to_project']==1,] #a subset with tests linked to projects only
#project <- project[project[,'proj_date_test_subs']==1,] #project incentives calibrated to this test #doesn't mean it
unique(project['flag_date_problem'])
project <- project[project[,'customertype']=='Individ Farms',] #Individ Farms
project <- project[project[,'end_use_ag']==1,] #end_use_ag
project <- project[project[,'flag_date_problem']!=1,] #exclude tests with date issues
#project <- project[order(project$sp_id_anon),]
project <- project[order(project$sp_id_anon,project$test_year,project$test_month),]


###2.2	Identification of One Test------------------------------------------
library(lubridate)
project['proj_date'] = make_date(project$proj_year,project$proj_month) #ignores 2nd project and test for now
project['test_date'] = make_date(project$test_year,project$test_month)
project['month_diff_test-project'] = round((project$test_date-project$proj_date)/30)

write.csv(project, file='E:/Chicago/Work/EPIC/project_1126.csv')

#mark the single tests
###define essential functions
absmin <- function(x) {x[which.min(abs(x))]} #min absolute value
#absmin(c(-1,-3,3)) #test
maxneg <- function(x) {x[x<=0][which.min(abs(x[x<=0]))]} #max negative value
#maxneg(c(-1,-3,3)) #test
minpos <- function(x) {x[x>=0][which.min(abs(x[x>=0]))]} #min positive value
#minpos(c(-1,-3,1,3,0)) #test

###closest_test
closest_value <- aggregate(project_1126$month_diff_test.project, by=list(project_1126$sp_id_anon),FUN=absmin)
test <- merge(project_1126,closest_value,by.x="sp_id_anon", by.y="Group.1")
#colnames(test)[colnames(test)=='x'] <- 'closest_value'
test['closest_test'] = ifelse(test$x == test$month_diff_test.project, 1, 0) #1:721, 0:471
#summary(test$closest_test>0) #check the statistics summary

###first_test_after_project
first_after_value <- aggregate(project_1126$month_diff_test.project, by=list(project_1126$sp_id_anon),FUN=minpos)
test2 <- merge(project_1126,first_after_value,by.x="sp_id_anon", by.y="Group.1")
test2['first_test_after_project'] = ifelse(test2$x == test2$month_diff_test.project, 1, 0) #1:385, 0:400, NA:407
#test2[is.na(test2$first_test_after_project),]['first_test_after_project'] <- 0

###last_test_before_project
last_before_value <- aggregate(project_1126$month_diff_test.project, by=list(project_1126$sp_id_anon),FUN=maxneg)
test3 <- merge(project_1126,last_before_value,by.x="sp_id_anon", by.y="Group.1")
test3['last_test_before_project'] = ifelse(test3$x == test3$month_diff_test.project, 1, 0) #1:596, 0:427, NA: 169
#test3[is.na(test3$last_test_before_project),]['last_test_before_project'] <- 0

###merge to the project dataset
project_0111 <- project_1126
project_0111['closest_test'] = test['closest_test']
project_0111['first_test_after_project'] = test2['first_test_after_project']
project_0111['last_test_before_project'] = test3['last_test_before_project']

summary(project_0111$closest_test==1) #721
summary(project_0111$first_test_after_project==1) #385
summary(project_0111$last_test_before_project==1) #596

write.csv(project_0111, file='E:/Chicago/Work/EPIC/project_0111.csv')

#----------------------3.	Reconstruction of Estimated 1st-year energy savings in kWh--------------------------------------
##start with using all the data
project <- read.csv(file="E:/Chicago/Work/EPIC/project_1126.csv", header=TRUE, sep=",")

###3.1	Direct Reconstruction-----------------------------
#from kwhperyr that is constructed
constructed_savings_kwh_yr1 <- project$kwhperyr-project$kwhperyr_after
summary(constructed_savings_kwh_yr1>0)
cbind(constructed_savings_kwh_yr1,project$est_savings_kwh_yr) #compare
#plot(project$annualcost/project$avgcost_kwh,project$kwhperyr) #test how kwhperyr constructed
plot(constructed_savings_kwh_yr1,project$est_savings_kwh_yr)
abline(1,1, col='red')


#from annualcost and avgcost_kwh
#annual_kwh <- project$annualcost/project$avgcost_kwh
#annual_kwh_exp <-project$annualcost_after/project$avgcost_kwh_after
#constructed_savings_kwh_yr2 <- annual_kwh-annual_kwh_exp
#cbind(constructed_savings_kwh_yr2,project$est_savings_kwh_yr) #compare #the same as 1

#from manual
project['low_eff'] <- as.integer(project['ope']<=50)
constructed_savings_kwh_yr3 <- project$low_eff * 0.25 * project$kwhperyr + (1-project$low_eff) * (1-project$ope/project$ope_after) * project$kwhperyr
cbind(constructed_savings_kwh_yr3,project$est_savings_kwh_yr) #compare
plot(constructed_savings_kwh_yr3,project$est_savings_kwh_yr)
abline(1,1, col='red')

kwh_savings_compare <- cbind(constructed_savings_kwh_yr3,project$est_savings_kwh_yr)
kwh_savings_compare <- kwh_savings_compare[kwh_savings_compare[,1]>0,] #drop those negative and 0 ones (a lot)
plot(kwh_savings_compare)
abline(1,1, col='red')

sum((constructed_savings_kwh_yr3-project$est_savings_kwh_yr<0),na.rm=TRUE) #1028: mostly smaller computed velues
sum((round(constructed_savings_kwh_yr3,2)-round(project$est_savings_kwh_yr,2)==0),na.rm=TRUE) #0, so no (rounded) same values


###3.2	Other Comparison Methods--------------------------------
#kwh_hr_difference
project['est_savings_kwh_hr'] <- project$est_savings_kwh_yr/project$hrs_per_year
project['kwh_hr_difference'] <- project$kwhaf*project$af24hrs/24-project$kwhaf_after*project$af24hrs_after/24
cbind(project['est_savings_kwh_hr'],project['kwh_hr_difference']) #compare
plot(project$est_savings_kwh_hr,project$kwh_hr_difference,xlim=c(0,500),ylim=c(-500,0)) #??why negative

summary(project$kwh_hr_difference<0)
summary(project$kwhaf-project$kwhaf_after>0)
summary(project$kwhaf-project$kwhaf_after<0)
summary(project$af24hrs-project$af24hrs_after<=0)

##for interpretation
kwhaf_diff <- (project$kwhaf-project$kwhaf_after)
kwhaf_diff <- na.omit(kwhaf_diff)
hist(kwhaf_diff, probability = TRUE)
lines(density(kwhaf_diff), col='red')

af24hrs_diff <- project$af24hrs-project$af24hrs_after
af24hrs_diff <- na.omit(af24hrs_diff)
hist(af24hrs_diff, probability = TRUE)
lines(density(af24hrs_diff), col='red')


#kwh_af_difference
project['est_savings_kwh_af_after'] <- project$est_savings_kwh_hr/(project$af24hrs_after/24)
project['est_savings_kwh_af'] <- project$est_savings_kwh_hr/(project$af24hrs/24)
project['kwh_af_difference'] <- project$kwhaf-project$kwhaf_after
plot(project$est_savings_kwh_af_after,project$kwh_af_difference,xlim=c(0,2000),ylim=c(-1000,1000))
plot(project$est_savings_kwh_af,project$kwh_af_difference,xlim=c(0,2000),ylim=c(-1000,1000))

summary(project$est_savings_kwh_af<0)

###3.3	Repeat with Single-test Data---------------------------------
project_0111 <- read.csv(file="E:/Chicago/Work/EPIC/project_0111.csv", header=TRUE, sep=",")


project <- project_0111[project_0111['last_test_before_project']== '1',]
project <- project_0111[project_0111['first_test_after_project']== '1',]
project <- project_0111[project_0111['closest_test']== '1',]
#then repeat all 3.1 and 3.2 steps

#project <- project_1126[!is.na(project_1126['closest_test']),] 
#project <- project_1126[!is.na(project_1126['first_test_after_project']),] 
#project <- project_1126[!is.na(project_1126['last_test_before_project']),]
#then repeat all 3.1 and 3.2 steps


#-----------------------------4.	Reconstruction of Incentives Amount-------------------------------------------------------------------------
###4.1	Reconstruction from Given est_savings_kwh_yr-----------------

#project <- project_1126
project <- project_0111 #make sure essential variables are in project dataset

project['est_savings_kwh_yr_%'] <- project$est_savings_kwh_yr/project$kwhperyr
summary(project$`est_savings_kwh_yr_%`) #should not be larger than 1

project['flag_too_high_kwh_savings'] <- as.integer(project$`est_savings_kwh_yr_%`>1)
sum(project$flag_too_high_kwh_savings) #169 observations have higher savings than energy use(reasons?)

#------construct kw save rate
install.packages('dplyr')
library(dplyr)
project %>%
  mutate(
    kw_save_rate = case_when(
      `est_savings_kwh_yr_%` <= 0.05 ~ 0, 
      `est_savings_kwh_yr_%` > 0.05 & `est_savings_kwh_yr_%` <= 0.10 ~ 0.009694,
      `est_savings_kwh_yr_%` > 0.10 & `est_savings_kwh_yr_%` <= 0.15 ~ 0.02237,
      `est_savings_kwh_yr_%` > 0.15 & `est_savings_kwh_yr_%` <= 0.20 ~ 0.03729,
      `est_savings_kwh_yr_%` > 0.20 & `est_savings_kwh_yr_%` <= 0.25 ~ 0.05369,
      `est_savings_kwh_yr_%` > 0.25 & `est_savings_kwh_yr_%` <= 0.30 ~ 0.07159,
      `est_savings_kwh_yr_%` > 0.30 & `est_savings_kwh_yr_%` <= 0.35 ~ 0.08874,
      `est_savings_kwh_yr_%` > 0.35 & `est_savings_kwh_yr_%` <= 0.40 ~ 0.1044,
      `est_savings_kwh_yr_%` > 0.40 & `est_savings_kwh_yr_%` <= 0.45 ~ 0.1163,
      `est_savings_kwh_yr_%` > 0.45 & `est_savings_kwh_yr_%` <= 0.50 ~ 0.126,
      `est_savings_kwh_yr_%` > 0.50 & `est_savings_kwh_yr_%` <= 0.55 ~ 0.132,
      `est_savings_kwh_yr_%` > 0.55 & `est_savings_kwh_yr_%` <= 0.60 ~ 0.1357,
      `est_savings_kwh_yr_%` > 0.60 & `est_savings_kwh_yr_%` <= 1 ~ 0.184
    )
  )#doesn't work: package version issue

#high efficiency
project['kw_save_rate'] = 0 #not listed in the rule
project['kw_save_rate'] = ifelse(project$`est_savings_kwh_yr_%` > 0.05, 0.009694, project$kw_save_rate)
project['kw_save_rate'] = ifelse(project$`est_savings_kwh_yr_%` > 0.10, 0.02237, project$kw_save_rate)
project['kw_save_rate'] = ifelse(project$`est_savings_kwh_yr_%` > 0.15, 0.03729, project$kw_save_rate)
project['kw_save_rate'] = ifelse(project$`est_savings_kwh_yr_%` > 0.20, 0.05369, project$kw_save_rate)
project['kw_save_rate'] = ifelse(project$`est_savings_kwh_yr_%` > 0.25, 0.07159, project$kw_save_rate)
project['kw_save_rate'] = ifelse(project$`est_savings_kwh_yr_%` > 0.30, 0.08874, project$kw_save_rate)
project['kw_save_rate'] = ifelse(project$`est_savings_kwh_yr_%` > 0.35, 0.1044, project$kw_save_rate)
project['kw_save_rate'] = ifelse(project$`est_savings_kwh_yr_%` > 0.40, 0.1163, project$kw_save_rate)
project['kw_save_rate'] = ifelse(project$`est_savings_kwh_yr_%` > 0.45, 0.126, project$kw_save_rate)
project['kw_save_rate'] = ifelse(project$`est_savings_kwh_yr_%` > 0.50, 0.132, project$kw_save_rate)
project['kw_save_rate'] = ifelse(project$`est_savings_kwh_yr_%` > 0.55, 0.1357, project$kw_save_rate)
project['kw_save_rate'] = ifelse(((project$`est_savings_kwh_yr_%` > 0.60) & (project$`est_savings_kwh_yr_%` <= 1)), 0.184, project$kw_save_rate)
project['kw_save_rate'] = ifelse(project$`est_savings_kwh_yr_%` > 1, NA, project$kw_save_rate) #NA for observations with higher savings than energy use
summary(project$kw_save_rate)
cbind(project$`est_savings_kwh_yr_%`,project['kw_save_rate']) #checked

#low efficiency
project['low_eff'] <- as.integer(project['ope']<=50)
project['kw_save_rate'] = ifelse(project$low_eff == 1, 0.07159, project$kw_save_rate)
cbind(project$`est_savings_kwh_yr_%`,project$ope,project['kw_save_rate']) #checked

#-------date tags
#date tag for kw rate (07/01/2014)
project['kw_date<07012014'] = ifelse((project$proj_year <2014) | ((project$proj_year ==2014) & (project$proj_month < 7)), 1, 0)
#date tag for kwh rate (05/14/2014) #not necessarily used, can also use real avgcost_kwh
project['kwh_date<05142014'] = ifelse((project$proj_year <2014) | ((project$proj_year ==2014) & (project$proj_month < 5)), 1, 0)

#-------demand savings
project['demand_savings_kw_year'] <- project$kw_save_rate * project$kw_input
project['demand_subsidy'] <- project$kw_save_rate * project$kw_input * 100 * project$`kw_date<07012014` + project$kw_save_rate * project$kw_input * 150 * (1-project$`kw_date<07012014`)

#-------compute project incentives
#project['consumption_subsidy_by_avgcost'] <- project$est_savings_kwh_yr * project$avgcost_kwh
#project['consumption_subsidy_by_avgcost_after'] <- project$est_savings_kwh_yr * project$avgcost_kwh_after
project['consumption_subsidy_by_fixedrate'] <- project$est_savings_kwh_yr * project$`kwh_date<05142014` * 0.09 + project$est_savings_kwh_yr * (1-project$`kwh_date<05142014`) * 0.12 #right

#project['subsidy_proj_computed_by_avgcost'] = project$demand_subsidy +project$consumption_subsidy_by_avgcost
#project['subsidy_proj_computed_by_avgcost_after'] = project$demand_subsidy +project$consumption_subsidy_by_avgcost_after
project['subsidy_proj_computed_by_fixedrate'] = project$demand_subsidy +project$consumption_subsidy_by_fixedrate #right

#project[,c('subsidy_proj','subsidy_proj_computed_by_avgcost','subsidy_proj_computed_by_fixedrate','subsidy_proj_computed_by_avgcost_after')]

#-------test annualcost/kwhperyr ?= avgcost_kwh
project['avgcost_kwh_computed'] = project$annualcost/project$kwhperyr
project[,c('avgcost_kwh_computed','avgcost_kwh')] #checked! Yes, they equal to each other.

#-------test kw_input_direct ?= kw_input
#project[,c('kw_input_direct','kw_input')] #checked

#-------Compare
#write.csv(project,"E:/Chicago/Work/EPIC/project.csv",row.names=FALSE)
#project <- read.csv(file="E:/Chicago/Work/EPIC/project.csv", header=TRUE, sep=",")
plot(project$subsidy_proj,project$subsidy_proj_computed_by_fixedrate)
abline(1,1, col='red')

summary(round(project$subsidy_proj_computed_by_fixedrate-project$subsidy_proj)>=0)

#using subsets with only one test
project <- project_0111[project_0111[,'last_test_before_project']== '1',]
project <- project_0111[project_0111[,'first_test_after_project']== '1',]
project <- project_0111[project_0111[,'closest_test']== '1',]
#and repeat



###4.2	Reconstruction from the Pump Test Results Only-----------------------------------------


#----------------------------------------------------------------------------------------------------------------------