#creating an r object for the data
daily_dat <- readRDS('5_data_munge/out/daily_temperatures.rds') %>%
  filter(!is.na(date))

# aiming to find the 5 degree bins (interval) from the site data:
# creating latitude bins (interval) using the cut function.
# using the seq function and setting the interval starts with min of latitude and ends with max of latitude, and each interval has a size of 5.# 
sites_dat <- readRDS("5_data_munge/out/all_sites.rds") %>%
  filter(!is.na(latitude)) %>%
  mutate(bins_lat = cut(latitude,  breaks = c(-90, -10,
                                              seq(-5, 75, by = 5),
                                              80, 91)))

# pulling the site_id and latitude bins from the data
latit_sit_id <- sites_dat %>% select(site_id, bins_lat) %>% distinct()

#group by site_ud, source, month, day.
# cut off line 5 C from mean.
sites_source_particular_day <- left_join(daily_dat, latit_sit_id) %>%
  group_by(site_id, source, month = lubridate::month(date),
           day = lubridate::day(date))%>%
  mutate(temp_mean = mean(temp_degC), 
         temp_std = sd(temp_degC),
         #temp_std = ifelse(temp_std == NA, 0, temp_std),
         outlier_cut_off = ifelse(temp_std == NA | (3 * temp_std) < 10 |
         (3 * temp_std) > 15,  10, 3 * temp_std),
         low_bound = temp_mean - outlier_cut_off,
         up_bound = temp_mean + outlier_cut_off) %>%
  ungroup() %>%
  mutate(flag = ifelse(temp_degC < low_bound | temp_degC > up_bound,
                       "o", NA))
# looking at specific latitude_bin
jan_40_45_2010 <- sites_source_particular_day %>%
  filter(bins_lat %in% '(40,45]') %>%
  filter(month == 1) 
  
sample_jan_40_45_2010 <- jan_40_45_2010[sample(nrow(jan_40_45_2010), 50, replace = FALSE, prob = NULL),]

ggplot(sample_jan_40_45_2010, aes(x = site_id, y = temp_degC, color = flag)) +
  geom_point()

# creating plot to see how is outlier detection is preforming. 
# 1) creating subset of flagged temps observation. 
flaged_obs <- sites_source_particular_day %>% 
  filter(flag == 'o') 
#sites <- unique(flaged_obs$site_id)

# Taking subset of combined site & latitude data with flagged sites only.
# Then, finding the site and year most outliers observation,
max_sit <- flaged_obs %>%
  group_by(site_id) %>%
  summarize(n_out = n())
# to find the 3 maximum of the site-date subset
max_3_obs <-  max_sit %>%
  ungroup() %>%
  slice_max(n_out, n = 3)


outliers_subset_I <- sites_source_particular_day %>% 
  mutate(year = lubridate::year(date)) %>%
  filter( site_id %in% 'USGS-10265150') %>%
  filter(year == 1986)

pI <- ggplot(outliers_subset_I, aes(x = date, y = temp_degC, 
                                    colour = flag)) +
  geom_point(aes(shape = source)) +
  theme_bw() 
print(pI)
ggplotly(pI)


