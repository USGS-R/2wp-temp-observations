library(scipiper)
library(dplyr)
library(tidyverse)
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
latit_sit_id <- sites_dat %>% select(site_id, bins_lat) %>% distinct()

# selecting a random sample from the daily temps data.
daily_dat_subset <- daily_dat %>%
  filter(!is.na(date)) %>%
  slice_sample(n = 500000, replace = FALSE) 

# to find summary and distribution of the subset data.
summary(daily_dat_subset)
hist(daily_dat_subset$temp_degC, prob=TRUE, col="grey")
lines(density(daily_dat_subset$temp_degC), col="blue")

#left join the bins to the daily temp data subset.
# adding the flag column to detect for any temperature in the critical region. 
# critical region is the tail region, pass the cut-off values.
daily_latit_subset <- left_join(daily_dat_subset, latit_sit_id) %>%
  group_by(bins_lat, month = lubridate::month(date))%>%
  # finding the mean and  setting cut off limits for outliers
  mutate(temp_mean = mean(temp_degC),  
         outlier_cut_off = 2 * sd(temp_degC),
         low_bound = temp_mean - outlier_cut_off,
         up_bound = temp_mean + outlier_cut_off) %>%
  ungroup() %>%
  mutate(flag = ifelse(temp_degC <= low_bound | temp_degC >= up_bound,
                       "o", NA)) 


# creating plot to see how is outlier detection is preforming. 
# 1) creating subset of flagged temps observation. 
flaged_obs <- daily_latit_subset %>% 
  filter(flag == 'o') 
sites <- unique(flaged_obs$site_id)

# Finding the site and year most outliers observation from the daily_latitude subset
max_sit_year <- flaged_obs %>%
  mutate(year = lubridate::year(date)) %>%
  group_by(site_id, year ) %>%
  summarize(n_obs = n())
# to find the 3 maximum of the stie-date subset
max_3_obs <-  max_sit_year %>%
  top_n(n =4)


 
for (temp_site in sites) {
  temp_dat <- flaged_obs %>%
    (date = as.Date(date)) %>%
    filter(site_id %in% temp_site) 
p <- ggplot(flaged_obs, aes(x = month, y = date)) +
  geom_point() +
  theme_bw() 
  #cowplot::theme_cowplot() +
  #ggtitle(paste0("Timeseries to Detect Outlier: ", temp_site)) 
  #facet_wrap(~ flag)
print(p)
}
# summarizing the number of flagged temps observations. 
# creating subset of flagged temps observation. 
#falged_obs <- daily_latit_subset %>% 
 # length(which(flag == 'o'))
#  select(flag == 'o', ) %>% unique(seg_id)




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
  

