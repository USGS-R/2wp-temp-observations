################### function to get summarizes about the binned daily data ####
#################  number and proportions of flagged temperature observation 
#################     and sites in a group/bin 

summary_qaqc_daily_temp_site <- function(in_ind, out_file) {

  # reading the qaqc daily temperature data-in,
  # to find the number of observation and the number of flagged per group/bin.
  # provides the number of sites in each group/bin. 
  qaqc_flagged_temp <- readRDS(sc_retrieve(in_ind), remake_file = '5_data_munge.yml') %>%
    group_by(site_type, lat_bins, long_bins, doy_bins) %>%
    summarize(n_per_group = n(),
              n_flagged = length(which(flag %in% 'o')),
              prop_flagged = round(length(which(flag %in% 'o'))/n(), 4),
              number_of_sites = length(unique(site_id))) %>%
    ungroup()
  
    #saving the summary data 
    readr::write_csv(qaqc_daily_data_summary, out_file)
}

