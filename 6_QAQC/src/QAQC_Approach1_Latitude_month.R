library(scipiper)
library(dplyr)
library(plotly)
library(tidyverse)
set.seed

#getting the data from google drive and changing the file to RDS file without ind
gd_get("5_data_munge/out/daily_temperatures.rds.ind")
# to get the sites longtitude and latitude data from google drive,
gd_get("5_data_munge/out/all_sites.rds.ind")

#creating an r object for the data
daily_dat <- readRDS('5_data_munge/out/daily_temperatures.rds') %>%
  filter(!is.na(date))
head(daily_dat)
summary(daily_dat)

# plot histogram with density curve over
# changing bin's width changes how the values are grouped for the histogram, #so 10 means that they are binned in increments of 10
ggplot(data = daily_dat, aes(x = temp_degC)) +
  geom_histogram(aes(y = ..density..), binwidth = 1,
                 color = "grey30", fill = "white") +
  geom_density(alpha = .2, fill = "antiquewhite3") +
  cowplot::theme_cowplot()
  
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

#finding the flagged data for the whole daily temps data
latitude_particular_month <- left_join(daily_dat, latit_sit_id) %>%
  mutate(year = lubridate::year(date)) %>%
  group_by(bins_lat, month = lubridate::month(date))%>%
  # finding the mean and  setting cut off limits for outliers
  mutate(temp_mean = mean(temp_degC), 
         temp_std = sd(temp_degC),
         outlier_cut_off = ifelse(is.na(temp_std) | (3 * temp_std) < 10 |
                                    (3 * temp_std) > 15,  10, 3 * temp_std),
         low_bound = temp_mean - outlier_cut_off,
         up_bound = temp_mean + outlier_cut_off) %>%
  ungroup() %>%
  mutate(flag = ifelse(temp_degC < low_bound | temp_degC > up_bound,
                       "o", NA))  

# finding a flagged subset of the whole daily temps
flaged_whole <- latitude_particular_month %>%
  filter(flag == 'o') 

sites <- unique(latitude_particular_month$site_id) # selecting unique site_id
##group_by(site_id, month)
max_site_year_whole <- flaged_whole %>%
  filter(site_id %in% sites) %>%
  #mutate(year = lubridate::year(date)) %>%
  group_by(site_id) %>%
  summarize(n_obs = n())

# to find the 3 maximum of the site-date subset
site_year_w_outlier <-  max_site_year_whole %>%
  ungroup() %>%
  slice_max(n_obs, n = 5)

# Looping through the sites to plot 
for (i in 1:nrow(site_year_w_outlier)) {
  temp_dat <- latitude_particular_month %>%
    filter(site_id %in% site_year_w_outlier$site_id[i])
    #filter(lubridate::year(date) %in% site_year_w_outlier$year[i])
  
  p <- ggplot(temp_dat, aes(x = date, y = temp_degC, colour = flag)) +
    geom_point(aes(shape = source)) +
    theme_bw() +
    cowplot::theme_cowplot() +
    ggtitle(paste0("Timeseries to Detect Outlier: ", 
                   site_year_w_outlier$site_id[i]))     
  temp_out <- paste0("6_QAQC/out/", 'Timeseries_outlier_',
                     site_year_w_outlier$site_id[i], '.png')
  #ggsave(temp_out, p, height = 7.5)
  
  print(p)
  plotly::ggplotly(p)
}
# looking at one site with most outliers with specific year. 
outliers_subset_I <- latitude_particular_month %>% 
  filter( site_id %in% 'USGS-10265150') %>%
  filter(year == 1999)
# plotting the subset for 1 site.
pI <- ggplot(outliers_subset_I, aes(x = date, y = temp_degC, 
                                    colour = flag)) +
  geom_point() +
  theme_bw() +
  ggtitle('Site_id:USGS-10265150') +
  labs(x = 'Date',
       y = 'Temperature') +
  cowplot::theme_cowplot() +
  theme(plot.title = element_text(hjust = 0.5))
plot_outI <- paste0("6_QAQC/out/site_USGS-10265150",'.png')
ggsave(plot_outI, pI, height = 5)
print(pI)
ggplotly(pI)


# creating bins and threshold data
latitude_month_threshold <- left_join(daily_dat, latit_sit_id)  %>%
  group_by(bins_lat, month = lubridate::month(date))%>%
  summarize(temp_mean = mean(temp_degC),  
            temp_std = sd(temp_degC),
            outlier_cut_off = 3 * temp_std,
            low_bound = temp_mean - outlier_cut_off,
            up_bound = temp_mean + outlier_cut_off) %>%
  ungroup()

table(is.na(latitude_month_threshold))


Plot_threshold = (c("Low_bound" = "grey56", "Up_bound" = "lightpink"))
p2 <- ggplot(data = latitude_month_threshold, aes(x = month, y = low_bound, 
                                             color = "Low_bound"))  +
  geom_point() +
  geom_point(data = latitude_month_threshold, aes(x = month, y = up_bound, 
                                             color = "Up_bound"))+
                                             #alpha = 0.25) + 
                                             #colour = 'black'))+#size = 1)) +
  
    facet_wrap( ~ bins_lat, strip.position = "right", ncol = 4) +
    theme_bw() +
    cowplot::theme_cowplot() +
  scale_x_continuous(limits = c(1, 12), breaks = 1:12) +
  scale_color_manual(values = Plot_threshold) +
    labs(x = "Date (Month)",
       y = "Threshold",
       color = "Threshold Boundaries ") +
    theme(plot.title = element_text(hjust = 0.5), legend.position = "top",
          #legend. title = element_blank(),
          strip.background = element_rect(fill = "gray96",
                                        color = "black"))
    #ggtitle(paste0("Timeseries Temperature for Segment Id: ", temp_bins)) 
  print(p2)
  print(ggplotly(p2)) %>% layout(hovermode = "x")



