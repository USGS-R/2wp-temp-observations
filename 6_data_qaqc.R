library(scipiper)
library(dplyr)
library(plotly)
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

sites <- unique(daily_dat$site_id) # selecting unique site_id

#finding the flagged data for the whole daily temps data
daily_latit_whole <- left_join(daily_dat, latit_sit_id) %>%
  group_by(bins_lat, month = lubridate::month(date))%>%
  # finding the mean and  setting cut off limits for outliers
  mutate(temp_mean = mean(temp_degC),  
         outlier_cut_off = 3 * sd(temp_degC),
         low_bound = temp_mean - outlier_cut_off,
         up_bound = temp_mean + outlier_cut_off) %>%
  ungroup() %>%
  mutate(flag = ifelse(temp_degC <= low_bound | temp_degC >= up_bound,
                       "o", NA)) 
hist(daily_latit_whole$temp_degC, prob = TRUE, col = "grey")
lines(density(daily_latit_whole$temp_degC), col = "blue")

# creating bins and threshold data
daily_lat_thres_dat <- left_join(daily_dat, latit_sit_id)  %>%
  group_by(bins_lat, month = lubridate::month(date))%>%
  summarize(temp_mean = mean(temp_degC),  
            outlier_cut_off = 2.5 * sd(temp_degC),
            low_bound = mean(temp_degC) - outlier_cut_off,
            up_bound = mean(temp_degC) + outlier_cut_off) 
  ungroup()
  
# finding a flagged subset of the whole daily temps
flaged_whole <- daily_latit_whole %>%
  filter(flag == 'o') 
  ##group_by(site_id, month) 


#finding th years with max obs
sites <- unique(daily_latit_whole$site_id)

max_site_year_whole <- flaged_whole %>%
  filter(site_id %in% sites) %>%
  mutate(year = lubridate::year(date)) %>%
  group_by(site_id, year) %>%
  summarize(n_obs = n())
# to find the 3 maximum of the site-date subset
site_year_w_outlier <-  max_site_year_whole %>%
  ungroup() %>%
  slice_max(n_obs, n = 15)

for (temp_bins in bins) {
  bin_dat <- daily_latit_whole %>%
    group_by(site_id, month) %>%
    filter(bins_lat %in% temp_bins)
  
  p2 <- ggplot(data = bin_dat, aes(x = month, y = low_bound)) +
    #geom_line(size = 1) +
    geom_point(data = bin_dat, aes(x = month, y = up_bound),
               alpha = 0.35, colour = "black", size = 1) +
    facet_wrap( ~ bins_lat, strip.position = "top", ncol = 1) +
    theme_bw() +
    cowplot::theme_cowplot() +
    ggtitle(paste0("Timeseries Temperature for Segment Id: ", temp_bins)) 
  print(ggplotly(p2))
}

examine_site_bins <- flaged_whole %>%
  filter(bins_lat == bins)
summary(examine_site)
plotlist = list()

bins <- unique(daily_latit_whole$bins_lat)

# finding outlier for bins to plot
max_site_year_whole <- flaged_whole %>%
  filter(site_id %in% sites) %>%
  #mutate(year = lubridate::year(date)) %>%
  group_by(bins_lat) %>%
  summarize(n_obs = n())
# to find the 3 maximum of the site-date subset
site_year_w_outlier <-  max_site_year_whole %>%
  ungroup() %>%
  slice_max(n_obs, n = 15)

for (temp_bins in max_site_year_whole$bins_lat) {
  bin_dat <- daily_latit_whole %>%
    group_by(site_id, month) %>%
    filter(bins_lat %in% temp_bins)
  
  p2 <- ggplot(data = bin_dat, aes(x = month, y = low_bound)) +
    #geom_line(size = 1) +
    geom_point(data = bin_dat, aes(x = month, y = up_bound),
               alpha = 0.35, colour = "black", size = 1) +
    facet_wrap( ~ bins_lat, strip.position = "top", ncol = 1) +
    theme_bw() +
    cowplot::theme_cowplot() +
    ggtitle(paste0("Timeseries Temperature for Segment Id: ", temp_bins)) 
  print(ggplotly(p2))
}

# Looping through the sites to plot 
for (i in 1:nrow(site_year_w_outlier)) {
  temp_dat <- daily_latit_whole %>%
    filter(site_id %in% site_year_w_outlier$site_id[i]) %>%
    filter(lubridate::year(date) %in% site_year_w_outlier$year[i])
  
  p <- ggplot(temp_dat, aes(x = date, y = temp_degC, colour = flag)) +
    geom_point(aes(shape = source)) +
    #geom_line() +
    theme_bw() +
    cowplot::theme_cowplot() +
    #scale_y_continuous(limits = c (0, 30), breaks = c(5, 10, 15, 20, 25, 30)) +
    ggtitle(paste0("Timeseries to Detect Outlier: ", 
                   site_year_w_outlier$site_id[i]))     
    temp_out <- paste0("6_QAQC/out/", 'Timeseries_outlier_',
                       site_year_w_outlier$site_id[i], '.png')
    ggsave(temp_out, p, height = 7.5)
    
    print(ggplotly(p))
    plotly::ggplotly(p)
}
#for (temp_site in max_3_whole$site_id) {
#  temp_dat <- daily_latit_whole %>%
 #   group_by(site_id) %>%
    #filter(!is.na(flag)) %>%
  #  filter(site_id %in% temp_site) %>% 
   # filter(lubridate::year(date) %in% temp_year) 
#p <- ggplot(temp_dat, aes(x = date, y = temp_degC, colour = flag)) +
 # geom_point() +
  #theme_bw() +
  #cowplot::theme_cowplot() +
  #scale_y_continuous(limits = c (0, 30), breaks = c(5, 10, 15, 20, 25, 30)) +
  #ggtitle(paste0("Timeseries to Detect Outlier: ", temp_site)) 
  #facet_wrap(~ flag)
#temp_out <- paste0("6_QAQC/out/", 'Timeseries_outlier_', temp_site, '.png')
#ggsave(temp_out, p, height = 7.5)
#}

sites <- unique(daily_dat$site_id) # selecting unique site_id
site_sub <- sample(sites, 5000)  # selecting sample/subset of sites
# selecting a random sample from the daily temps data. 
# filtering to the sites samples selected above after removing NA. 
daily_dat_subset <- daily_dat %>%
  filter(!is.na(date)) %>%
  filter(site_id %in% site_sub) # to select a random subset from daily-temp data

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
         outlier_cut_off = 3 * sd(temp_degC),
         low_bound = temp_mean - outlier_cut_off,
         up_bound = temp_mean + outlier_cut_off) %>%
  ungroup() %>%
  mutate(flag = ifelse(temp_degC <= low_bound | temp_degC >= up_bound,
                       "o", NA))

# creating plot to see how is outlier detection is preforming. 
# 1) creating subset of flagged temps observation. 
flaged_obs <- daily_latit_subset %>% 
  filter(flag == 'o') 
#sites <- unique(flaged_obs$site_id)

# Taking subset of combined site & latitude data with flagged sites only.
# Then, finding the site and year most outliers observation,
max_sit <- flaged_obs %>%
  group_by(site_id) %>%
  summarize(n_obs = n())
# to find the 3 maximum of the site-date subset
max_3_obs <-  max_sit %>%
  ungroup() %>%
  slice_max(n_obs, n = 3)

#finding th years with max obs
max_year <- flaged_obs %>%
  mutate(year = lubridate::year(date)) %>%
  group_by(site_id, year) %>%
  summarize(n_obs = n())
# to find the 3 maximum of the site-date subset
max_3_year <-  max_year %>%
  ungroup() %>%
  slice_max(n_obs, n = 3)

temp_year = max_year$year
# needed to find the reason for date = NA
# to do so we looked over the source and site_id 
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
  

