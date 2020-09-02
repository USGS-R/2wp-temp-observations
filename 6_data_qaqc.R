library(scipiper)
library(dplyr)
set.seed(10)
#getting the data from google drive and changing the file to RDS file without ind
gd_get("5_data_munge/out/daily_temperatures.rds.ind")
# to get the sites longtitude and latitude data from google drive,
gd_get("5_data_munge/out/all_sites.rds.ind")

#creating an r object for the data
daily_dat <- readRDS('5_data_munge/out/daily_temperatures.rds')
head(daily_dat)
summary(daily_dat)

# aiming to find the 5 degree bins (interval) from the site data:
# creating latitude bins (interval) using the cut function.
# using the seq function and setting the interval starts with min of latitude and ends with max of latitude, and each interval has a size of 5.# 
sites_dat <- readRDS("5_data_munge/out/all_sites.rds") %>%
  filter(!is.na(latitude)) %>%
  mutate(bins_lat = cut(latitude,  breaks = seq(from = floor(min(latitude)), 
                                                to = ceiling(max(latitude)), 
                                                by = 5)))
# pulling the site_id and latitude bins from the data
latit_sit_id <- sites_dat %>% select(site_id, bins_lat)

# selecting a random sample from the daily temps data.
daily_dat_subset <- daily_dat %>%
  filter(!is.na(date)) %>%
  slice_sample(n = 500000, replace = FALSE) 

#left join the bins to the daily temp data subset.
#daily_latit_subset <- left_join(daily_dat_subset, latit_sit_id) %>%
 # group_by(bins_lat, month = lubridate::month(date))

# to find summary and distribution of the subset data.
summary(daily_latit_subset)
hist(daily_latit_subset$temp_degC, prob=TRUE, col="grey")
lines(density(daily_latit_subset$temp_degC), col="blue")

# finding the mean and std of the subset
mean_daily_lat <- mean(daily_dat_subset$temp_degC)
std_daily_lat <- sd(daily_dat_subset$temp_degC)
# setting cut off limits for outliers 
outlier_cut_off <- 2 * std_daily_lat
low_bound <- mean_daily_lat - outlier_cut_off
up_bound <- mean_daily_lat + outlier_cut_off
# compare 2 *std from the mean to percentiles:
percent_5_95 <- quantile(daily_latit_subset$temp_degC, c(0.05, 0.95)) 

#left join the bins to the daily temp data subset.
# adding the flag column to detect for any temperature in the critical region. 
# critical region is the tail region, pass the cut-off values.
daily_latit_subset <- daily_latit_subset %>%
  left_join(daily_dat_subset, latit_sit_id) %>%
  group_by(bins_lat, month = lubridate::month(date))%>%
  mutate(flag = ifelse(temp_degC <= low_bound | temp_degC >= up_bound,
                       "o", NA)) 
# summarizing the number of flagged temps observations. 
daily_latit_flags <- daily_latit_subset %>%
  summarize(n_obse = n(),
            flag)

# creating subset of flagged temps observation. 


#finding the missing data in the date column
mis_dat <- daily_dat %>%
  filter(is.na(date))
  
site_count <- mis_dat %>%
  group_by(site_id) %>%
  summarize(n_site = n())
#creating a missing dataset to investigate the missing dates. 
source_count <- mis_dat %>%
  group_by(source) %>%
  summarize(n_source = n())
# give you rows where date is not NA and where source is Ecosheds
  filter(!is.na(date) & source %in% 'Ecosheds')
  

