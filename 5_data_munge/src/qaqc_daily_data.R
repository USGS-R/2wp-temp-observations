############ Function to QAQC the daily temperature  #######
############ Outlier detection using mean and standard deviation.
############ created groups using site type, longitude latitude and doy bins, 
############  calculated the mean and standard derivation to preform outlier detection. 
qaqc_daily_temp_site_data <- function(temp_in_ind, site_in_ind, out_ind) {

  # reading the daily temperature data-in, removing NA dates, 
  # and creating day of the year column.
  #creating doy bins with 6 days intervals.
  daily_dat_mod <- readRDS(sc_retrieve(temp_in_ind), remake_file = 'getters.yml') %>%
    filter(!is.na(date)) %>%
    mutate(doy = lubridate::yday(date),
         doy_bins = cut(doy, breaks = 
                          seq(from = 0,
                              to = max(doy),
                              by = doy_lim)))

  # Reading all sites data in
  # creating 1 degree longitude and 1 degree latitude bins (interval) from the site data:
  # creating  bins (interval) using the cut function.
  # using the seq function and setting the interval starts with min of long/latitude
  # and ends with max of latitude, and each interval size is 1.
  sites_dat_mod <- readRDS(sc_retrieve(site_in_ind), remake_file = 'getters.yml') %>%
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
  
  # merging the daily temperature data and the sites data.
  # using 1 degree longitude and 1 latitude bins, 6 days of the year bins.
  # finding the mean and standard deviation of the temperature based on the above bins.
  # finding lower and up bounds then finding the flagged binned daily temperature data. 
  binned_daily_data_complete <- left_join(daily_dat_mod, sites_dat_mod) %>%
    group_by(site_type, lat_bins, long_bins, doy_bins) %>%
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
  # save the data file 
  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(binned_daily_data_complete, data_file)
  gd_put(out_ind)
}
