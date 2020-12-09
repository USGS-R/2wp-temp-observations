###### function that count amount of data points that was flagged 
###### and the percentage of the flagged data points.

outliers_summary <- function(in_ind, out_file) {
  daily_dat_flagged <- readRDS(sc_retrieve(in_ind), remake_file = 'getters.yml')%>%
  summarize(number_flags = length(which(flag %in% 'o')),
            percent_flags = round(number_flags/ nrow(in_ind) * 100, 4))
  #saving the summary data 
  readr::write_csv(daily_dat_flagged, out_file)
}


################### function to get summarizes about the binned daily data ####
#################  number and percentage of flagged temperature observation 
#################     and sites in a group/bin 

summary_qaqc_daily_temp_site <- function(in_ind, out_file) {

  # reading the qaqc daily temperature data-in,
  # to find the number of observation and the number of flagged per group/bin.
  # provides the number of sites in each group/bin. 
  qaqc_flagged_temp <- readRDS(sc_retrieve(in_ind), remake_file = 'getters.yml') %>%
    group_by(site_type, lat_bins, long_bins, doy_bins) %>%
    summarize(n_per_group = n(),
              n_flagged = length(which(flag %in% 'o')),
              prop_flagged = round(length(which(flag %in% 'o'))/n(), 4),
              number_of_sites = length(unique(site_id))) %>%
    ungroup()
  # getting summaries about the bins
  # counting the flagged bins, median of number of observation
  # number of bins with 3 of less observation.  
  bins_summary <- qaqc_flagged_temp %>%
    summarize(count_bins_flagged = length(which(n_flagged > 0)),
              percent_bins_flagged = count_bins_flagged/n() * 100,
              median_obs_per_bin = median(n_per_group),
              percent_bins_w_3_less_obs = (length(which(n_per_group <= 3))/n()) * 100) 
  
    #saving the summary data 
    readr::write_csv(bins_summary, out_file)
}


