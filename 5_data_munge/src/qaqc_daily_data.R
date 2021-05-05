############ Function to QAQC the daily temperature  #######
############ Outlier detection using mean and standard deviation.
############ created groups using site type, longitude latitude and doy bins,
############  calculated the mean and standard derivation to preform outlier detection.
qaqc_daily_temp_site_data <- function(temp_in_ind, site_in_ind,
                                      doy_lim, long_deg, lat_deg,
                                      out_ind) {
  # read in sites
  sites <- readRDS(sc_retrieve(site_in_ind, remake_file = 'getters.yml'))
  # filter to stream sites in the U.S.
  dat_red <- readRDS(sc_retrieve(temp_in_ind, remake_file = 'getters.yml')) %>%
    filter(site_id %in% unique(sites$site_id))
  # reading the daily temperature data-in, removing NA dates,
  # and creating day of the year column.
  #creating doy bins with 6 days intervals.
  dat_red <- dat_red %>%
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
  sites_dat_mod <- sites %>%
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
  yard_stick <- 5
  lb <- 10  # lower bound
  ub <- 15  # upper bound

  dat_red <- left_join(dat_red, sites_dat_mod) %>%
    group_by(site_type, lat_bins, long_bins, doy_bins) %>%
    mutate(n_per_group = n(),
           temp_mean = mean(mean_temp_degC, na.rm = TRUE),
           temp_std = sd(mean_temp_degC, na.rm = TRUE),
           outlier_cut_off = ifelse(is.na(temp_std) | (3 * temp_std) < lb |
                                      (3 * temp_std) > ub,
                                    yard_stick, 3 * temp_std),
           low_bound = temp_mean - outlier_cut_off,
           up_bound = temp_mean + outlier_cut_off) %>%
    ungroup() %>%
    mutate(flag_o = ifelse(mean_temp_degC < low_bound | mean_temp_degC > up_bound,
                         "o", NA)) %>%
    mutate(flag = case_when(
      is.na(flag_o) ~ flag,
      !is.na(flag_o) & !is.na(flag) ~ paste(flag, flag_o, sep = '; '),
      !is.na(flag_o) & is.na(flag) ~ flag_o,
      TRUE ~ NA_character_)) %>%
    select(site_id, date, mean_temp_degC, min_temp_degC, max_temp_degC, n_obs, source, flag)
  # save the data file
  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(dat_red, data_file)
  gd_put(out_ind)
}
