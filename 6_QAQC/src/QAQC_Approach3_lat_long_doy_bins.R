library(scipiper)
library(dplyr)
library(plotly)
library(tidyverse)
set.seed

#getting the data from google drive and changing the file to RDS file without ind
gd_get("5_data_munge/out/daily_temperatures.rds.ind")
# to get the sites longitude and latitude data from google drive,
gd_get("5_data_munge/out/all_sites.rds.ind")

#creating an r object for the daily temp data
daily_dat <- readRDS('5_data_munge/out/daily_temperatures.rds') %>%
  filter(!is.na(date)) %>%
  mutate(month = lubridate::month(date), 
         doy = lubridate::yday(date))

# aiming to find the 1 degree longitude and 1 degree latitude bins (interval) from the site data:
# creating  bins (interval) using the cut function.
# using the seq function and setting the interval starts with min of long/latitude
#and ends with max of latitude, and each interval size is 1.

long_deg <- 1
lat_deg <- 1
sites_dat <- readRDS("5_data_munge/out/all_sites.rds") %>%
  filter(!is.na(latitude)) %>%
  mutate(lat_bins = cut(latitude, breaks = 
                          seq(from = floor(min(latitude)),
                              to = ceiling(max(latitude)),
                              by = lat_deg),
                        right = FALSE),
         long_bins = cut(longitude, breaks = 
                           seq(from = floor(min(longitude)),
                               to = ceiling(max(longitude)),
                               by = long_deg), right = FALSE))

# removing duplicated sites_data
sites_dat_unique <- unique(sites_dat)

# merging the daily temp data and the sites data.
# creating day bins with 6 days.
doy_lim <- 6
binned_daily_data_complete <- left_join(daily_dat, sites_dat_unique) %>%
  mutate(doy_bins = cut(doy, breaks = 
                          seq(from = 0,
                              to = max(doy),
                              by = doy_lim)))
# finding the flagged data in the binned daily temperature data
# using 1 degree longitude and 1 latitude bins, 6 days of the year bins.
# finding the mean and standard deviation of the temperature based on the above bins
# finding lower and up bounds to find the flagged data. 
yard_stick <- 5
lb <- 10  # lower bound
ub <- 15  # upper bound

source_lat_long_doy <- binned_daily_data_complete %>%
  select(-c(n_obs, latitude, longitude)) %>%
  group_by(source, site_type, lat_bins, long_bins, doy_bins) %>%
  mutate(n_per_group = n(),
         temp_mean = mean(temp_degC), 
         temp_std = sd(temp_degC),
         outlier_cut_off = ifelse(is.na(temp_std) | (3 * temp_std) < lb |
                                    (3 * temp_std) > ub,  
                                  yard_stick, 3 * temp_std),
         low_bound = temp_mean - outlier_cut_off,
         up_bound = temp_mean + outlier_cut_off) %>%
  ungroup() %>%
  mutate(flag = ifelse(temp_degC < low_bound | temp_degC > up_bound,
                       "o", NA))

