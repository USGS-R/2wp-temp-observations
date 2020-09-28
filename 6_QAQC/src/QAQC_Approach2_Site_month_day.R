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
         outlier_cut_off = ifelse(is.na(temp_std) | (3 * temp_std) < 10 |
         (3 * temp_std) > 15,  10, 3 * temp_std),
         low_bound = temp_mean - outlier_cut_off,
         up_bound = temp_mean + outlier_cut_off) %>%
  ungroup() %>%
  mutate(flag = ifelse(temp_degC < low_bound | temp_degC > up_bound,
                       "o", NA))

#finding the number of unique sites. 
sites <- unique(sites_source_particular_day$site_id)
#finding the number of observation  per site using this grouping. 
count_site_source_day <- left_join(daily_dat, latit_sit_id) %>%
  group_by(site_id, source, month = lubridate::month(date),
           day = lubridate::day(date))%>%
  summarize(n_per_site = n(),
            temp_mean = mean(temp_degC), 
         temp_std = sd(temp_degC),
         outlier_cut_off = ifelse(is.na(temp_std) | (3 * temp_std) < 10 |
                                    (3 * temp_std) > 15,  10, 3 * temp_std),
         low_bound = temp_mean - outlier_cut_off,
         up_bound = temp_mean + outlier_cut_off) %>%
  ungroup()
#
length(unique(which(count_site_source_day$n_per_site <= 5)))
# number of sites in our data
sites <- unique(count_site_source_day$site_id)
# number of unique sites that used stats to calculate cutoff limit. 
cal_stat_sit <- sites_source_particular_day %>%
  filter(!outlier_cut_off %in% 10)
length(unique(cal_stat_sit$site_id))

# number of sites which have few temperature observation.
low_obs_site <- count_site_source_day %>%
  filter(n_per_site <= 1 & source %in% "wqp" & month == 1 & day == 21)
number_site_month_day <- length(unique(low_obs_site$site_id))
length(unique(count_site_source_day$site_id))

# finding how many sites required the std replacement. 
num_std_replacement <- table(count_site_source_day$outlier_cut_off == 10)

# and how many observation does theses sites have. 
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
n_outleier_per_sit <- flaged_obs %>%
  filter(site_id %in% sites) %>%
  group_by(site_id) %>%
  summarize(n_outliers = n())
# to find the 3 maximum of the site-date subset
site_max_n_outlier <-  n_outleier_per_sit %>%
  ungroup() %>%
  slice_max(n_outliers, n = 5)


outliers_subset_I <- sites_source_particular_day %>% 
  mutate(year = lubridate::year(date)) %>%
  filter( site_id %in% 'USGS-12398600') %>%
  filter(year >= 1986 & year <= 1990)

pI <- ggplot(outliers_subset_I, aes(x = date, y = temp_degC, 
                                    colour = flag)) +
  geom_point(aes(shape = source)) +
  theme_bw() +
  ggtitle('Site_id:USGS-12398600') +
  labs(x = 'Date',
       y = 'Temperature') +
  cowplot::theme_cowplot() +
  theme(plot.title = element_text(hjust = 0.5))
plot_outI <- paste0("6_QAQC/out/ApprocahII_site_USGS-12398600",'.png')
ggsave(plot_outI, pI, height = 5)
print(pI)
ggplotly(pI)

outliers_subset_II <- sites_source_particular_day %>% 
  mutate(year = lubridate::year(date)) %>%
  filter( site_id %in% 'USGS-02160105') %>% 
filter(year >= 1984 & year <= 1986)

pII <- ggplot(outliers_subset_II, aes(x = date, y = temp_degC, 
                                    colour = flag)) +
  geom_point(aes(shape = source)) +
  theme_bw() +
  ggtitle('Site_id:USGS-02160105') +
  labs(x = 'Date',
       y = 'Temperature') +
  cowplot::theme_cowplot() +
  theme(plot.title = element_text(hjust = 0.5))
plot_outI <- paste0("6_QAQC/out/ApprocahII_site_USGS-02160105",'.png')
ggsave(plot_outI, pII, height = 5)
print(pII)
ggplotly(pII)


outliers_subset_III <- sites_source_particular_day %>% 
  mutate(year = lubridate::year(date)) %>%
  filter( site_id %in% 'USGS-07381350') %>%
  filter(year >= 1982 & year <= 1984)

pIII <- ggplot(outliers_subset_III, aes(x = date, y = temp_degC, 
                                      colour = flag)) +
  geom_point(aes(shape = source)) +
  theme_bw() +
  ggtitle('Site_id:USGS-07381350') +
  labs(x = 'Date',
       y = 'Temperature') +
  cowplot::theme_cowplot() +
  theme(plot.title = element_text(hjust = 0.5))
plot_outI <- paste0("6_QAQC/out/ApprocahII_site_USGS-07381350",'.png')
ggsave(plot_outI, pIII, height = 5)
print(pIII)
ggplotly(pIII)

special_site_subset <- sites_source_particular_day %>% 
  mutate(year = lubridate::year(date)) %>%
  filter( site_id %in% 'USGS-10265150') %>%
  filter(year == 1999)

p_special <- ggplot(special_site_subset, aes(x = date, y = temp_degC)) +
  geom_point(aes(shape = source), colour = "grey60") +
  theme_bw() +
  ggtitle('ApproachII-Site_id: USGS-10265150') +
  labs(x = 'Date',
       y = 'Temperature') +
  cowplot::theme_cowplot() +
  theme(plot.title = element_text(hjust = 0.5))
plot_outI <- paste0("6_QAQC/out/USGS-10265150_approachII",'.png')
ggsave(plot_outI, p_special, height = 5)
print(p_special)
ggplotly(p_special)
