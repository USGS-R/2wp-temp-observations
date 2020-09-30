library(scipiper)
library(dplyr)
library(plotly)
library(tidyverse)
set.seed

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
  group_by(site_id, source, month = lubridate::month(date), day_of_month = lubridate::day(date))%>%
  mutate(temp_mean = mean(temp_degC), 
         temp_std = sd(temp_degC),
         outlier_cut_off = ifelse(is.na(temp_std) | (3 * temp_std) < 10 |
         (3 * temp_std) > 15,  10, 3 * temp_std),
         low_bound = temp_mean - outlier_cut_off,
         up_bound = temp_mean + outlier_cut_off) %>%
  ungroup() %>%
  mutate(flag = ifelse(temp_degC < low_bound | temp_degC > up_bound,
                       "o", NA))

# to find the number of observation and the number of flagged per group. 
count_flag_groups <- sites_source_particular_day %>%
  group_by(site_id, source, month, day_of_month) %>%
  summarize(n_group = n(),
            n_flagged = length(which(flag %in% 'o')),
            prop_flagged = round(length(which(flag %in% 'o'))/n(), 4))
# creating a subset that contains the flagged groups only. 
flagged_groups_subset <- count_flag_groups %>%
  filter(n_flagged != 0) %>%
  group_by(n_group)

flags_frequency <- table(flagged_groups_subset$n_flagged)
percent_flagged_groups <- (nrow(flagged_groups_subset)/ 
                          nrow(count_flag_groups)) * 100
# plotting the flagged subset, number per group vs the number of flags. 
p_flag_count = ggplot(flagged_groups_subset, aes(x = n_group, y =  n_flagged)) +
  geom_point(aes(color = n_flagged)) +#, alpha = 0.15) + #aes(color = n_flagged
  theme_bw() +
  scale_x_continuous(limits = c(0,45),breaks = 0:45) +
  cowplot::theme_cowplot() +
  labs(title = 'Number of Outliers in Each Group',
       x = 'Number of Observation in a Group',
       y = 'Number of Outliers') +
  theme(plot.title = element_text(hjust = 0.5))
p_flag_count
ggplotly(p_flag_count)
# creating a histogram for the number of flagged 
p_flag_freq = ggplot(flagged_groups_subset, aes(x = n_flagged)) +
  geom_freqpoly() +
  #theme_bw() +
  #cowplot::theme_cowplot() +
  labs(title = 'The Frequency of Outliers in Groups',
       x = 'Number of Outliers',
       y = 'Frequency') +
  theme(plot.title = element_text(hjust = 0.5))
p_flag_freq
ggplotly(p_flag_freq) 

# number of unique sites that used stats to calculate cutoff limit. 
# sites that required the std replacement.
fix_value_group <- sites_source_particular_day %>%
  #filter(!is.na(temp_std)) %>%
  filter(!outlier_cut_off %in% 10) 
# Finding the different std values
site_w_na_std <- length(which(sites_source_particular_day$temp_std %in% NA))
site_w_0_std <- length(which(sites_source_particular_day$temp_std == 0))
site_w_large_std <- length(which((3 * sites_source_particular_day$temp_std) >= 15))
site_w_small_std <- length(which((3 * sites_source_particular_day$temp_std) > 0 
                                 & (3 * sites_source_particular_day$temp_std) <= 10))
# proportions of the different std-values. 
prop_na_std <- site_w_na_std / nrow(sites_source_particular_day) * 100
prop_0_std <- site_w_0_std/nrow(sites_source_particular_day) * 100
prop_na_0_std <- (site_w_na_std + site_w_0_std) / nrow(sites_source_particular_day) * 100
prop_large_std <- site_w_large_std/ nrow(sites_source_particular_day) * 100
prop_small_std <- site_w_small_std/nrow(sites_source_particular_day) * 100
# finding how many sites required the std replacement. 
length(which(count_site_source_day$outlier_cut_off == 10))

#finding the number of unique sites. 
sites <- unique(sites_source_particular_day$site_id)

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

#finding the number of observation  per site using this grouping. 
count_site_source_day <- left_join(daily_dat, latit_sit_id) %>%
  group_by(site_id, source, month = lubridate::month(date), 
           day_of_month = lubridate::day(date))%>%
  summarize(n_per_site = n(),
            temp_mean = mean(temp_degC), 
            temp_std = sd(temp_degC),
            outlier_cut_off = ifelse(is.na(temp_std) | (3 * temp_std) < 10 |
                                       (3 * temp_std) > 15,  10, 3 * temp_std),
            low_bound = temp_mean - outlier_cut_off,
            up_bound = temp_mean + outlier_cut_off) %>%
            #flag = ifelse(temp_degC < low_bound | temp_degC > up_bound,
             #                    "o", NA)) %>%
  ungroup()
  
            # get the number of site-days that meet that category in the data.
                     # n_site_days = length(unique(unique_id))) 
# to find the number of groups with n observation
# number of observation n (1,2,3,5,10,20) per group
n_site_day = c(1, 2, 3, 5, 10, 15, 20)
n_obs_per_group <- count_site_source_day %>%
  filter(n_per_site %in% n_site_day) %>%
  group_by(n_per_site) %>%
  summarize(n_group = n()) %>%
  mutate(prop_group = round(n_group / nrow(count_site_source_day)* 100 ,4))
# making histogram of n-observation per group:
p_group_obs = ggplot(n_obs_per_group, aes(x = n_per_site, y = prop_group)) +
  geom_col(color = "black", fill = "antiquewhite") +
  theme_bw() +
  cowplot::theme_cowplot() +
  labs(title = 'Proportion of Observation in Each Group',
       x = 'N per Group',
       y = 'Proportion') +
  theme(plot.title = element_text(hjust = 0.5))
p_group_obs
ggplotly(p_group_obs)
# number of sites in our data
sites <- unique(count_site_source_day$site_id)


# and how many observation does theses sites have. 
# looking at specific latitude_bin
jan_40_45_2010 <- sites_source_particular_day %>%
  filter(bins_lat %in% '(40,45]') %>%
  filter(month == 1) 

sample_jan_40_45_2010 <- jan_40_45_2010[sample(nrow(jan_40_45_2010), 50, replace = FALSE, prob = NULL),]

ggplot(sample_jan_40_45_2010, aes(x = site_id, y = temp_degC, color = flag)) +
  geom_point()
