f_to_c <- function(f) {
  c <- (f - 32) * (5/9)
  return(c)
}

munge_nwis <- function(dv_ind, uv_ind, min_value, max_value, out_ind) {

  dv <- readRDS(sc_retrieve(dv_ind, remake_file = 'getters.yml')) %>%
    mutate(source = 'nwis_dv')
  
  uv <- readRDS(sc_retrieve(uv_ind, remake_file = 'getters.yml')) %>%
    mutate(source = 'nwis_uv')
  
  nwis <- bind_rows(dv, uv) %>%
    filter(grepl('A', cd_value, ignore.case = FALSE)) %>%
    filter(temp_value > min_value & temp_value < max_value)
  
  # save
  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(nwis, data_file)
  gd_put(out_ind, data_file)
  
}

munge_ecosheds <- function(in_ind, min_value, max_value, out_ind) {
  dat <- readRDS(sc_retrieve(in_ind, remake_file = 'getters.yml'))
  
  # some data appears to be in fahrenheit (values > 45)
  # find those series and convert to celsius
  
  over_45 <- filter(dat, mean >= 45)
  high_series<- filter(dat, series_id %in% unique(over_45$series_id)) %>%
    mutate(mean = f_to_c(mean))
  
  # bind with "good" data
  # remove data above 35, below 0
  dat_out <- filter(dat, !series_id %in% unique(over_45$series_id)) %>%
    bind_rows(high_series) %>%
    filter(mean > min_value,
           mean < max_value)

  message(nrow(dat) - nrow(dat_out), ' temperature observations dropped because they were outside of 0-35 deg C.')
  
  dat_out2 <- dat_out %>%
    filter(!grepl('delete data|inaccurate|setup|bad value|preplacement|dewatered|out of water|out ofwater|outofwater|removal|set up|too warm', comment, ignore.case = TRUE))
  
  message(nrow(dat_out) - nrow(dat_out2), ' temperature observations dropped due to comments that indicated poor data quality.')
  
  saveRDS(dat_out2, file = as_data_file(out_ind))  
  gd_put(out_ind)
  
}

combine_all_dat <- function(wqp_ind, nwis_ind, ecosheds_ind, out_ind) {
  wqp <- readRDS(sc_retrieve(wqp_ind, remake_file = 'getters.yml')) %>%
    ungroup() %>%
    mutate(date = as.Date(ActivityStartDate), source = 'wqp') %>%
    select(site_id = MonitoringLocationIdentifier, 
           date,
           temp_degC = temperature_mean_daily,
           n_obs = n_day, source)
  
  nwis <- readRDS(sc_retrieve(nwis_ind, remake_file = 'getters.yml')) %>%
    mutate(site_id = paste0('USGS-', site_no)) %>%
    select(site_id, date = Date, temp_degC = temp_value, n_obs = n, source)
  
  ecosheds <- readRDS(sc_retrieve(ecosheds_ind, remake_file = 'getters.yml')) %>%
    mutate(source = 'ecosheds',
           site_id = as.character(location_id)) %>%
    select(site_id, temp_degC = mean, n_obs = n, source)
  
  all_dat <- bind_rows(wqp, nwis, ecosheds) %>%
    distinct(site_id, date, temp_degC, .keep_all = TRUE)
    
  # save
  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(all_dat, data_file)
  gd_put(out_ind)
  
}
  
combine_all_sites <- function(nwis_dv_sites_ind, nwis_uv_sites_ind, wqp_sites_ind, ecosheds_sites_ind, out_ind){

  nwis_sites <- feather::read_feather(sc_retrieve(nwis_dv_sites_ind, remake_file = 'getters.yml')) %>%
    mutate(source = 'nwis_dv') %>%
    bind_rows(feather::read_feather(sc_retrieve(nwis_uv_sites_ind, remake_file = 'getters.yml')) %>% 
                mutate(source = 'nwis_uv')) %>%
    mutate(site_id = paste0('USGS-', site_no)) %>%
    select(site_id, site_type = site_tp_cd, latitude = dec_lat_va, longitude = dec_long_va, source)
  
  wqp_sites <- feather::read_feather(sc_retrieve(wqp_sites_ind, remake_file = 'getters.yml')) %>%
    mutate(source = 'wqp') %>%
    select(site_id = MonitoringLocationIdentifier, latitude, longitude, 
           site_type = ResolvedMonitoringLocationTypeName, source)
  
  ecosheds_sites <- readRDS(sc_retrieve(ecosheds_sites_ind, remake_file = 'getters.yml')) %>%
    mutate(source = 'ecosheds', site_type = 'stream',
           site_id = as.character(location_id)) %>%
    select(site_id, latitude, longitude, site_type, source)
    
  all_sites <- bind_rows(nwis_sites, wqp_sites, ecosheds_sites)
  
  saveRDS(all_sites, as_data_file(out_ind))
  gd_put(out_ind)
}

summarize_all_dat <- function(in_ind, out_file) {
  dat_summary <- readRDS(sc_retrieve(in_ind, remake_file = 'getters.yml'))
  
  summary <- data.frame(n_obs = nrow(dat_summary), n_sites = length(unique(dat_summary$site_id)))
  
  readr::write_csv(summary, out_file)
}