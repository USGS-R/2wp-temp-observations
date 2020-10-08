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

munge_norwest <- function(dat_ind, sites_ind, min_value, max_value, out_ind) {

  dat <- feather::read_feather(sc_retrieve(dat_ind, 'getters.yml'))
  sites <- readRDS(sc_retrieve(sites_ind, 'getters.yml')) %>%
    sf::st_drop_geometry() %>%
    mutate(site_meta = TRUE) %>%
    select(OBSPRED_ID, Source, UOM, region, site_meta) %>% distinct()
  dat_out <-  dat %>%
    mutate(NorWeST_ID = ifelse(is.na(NorWeST_ID), NoRWeST_ID, NorWeST_ID),
           DailySD = ifelse(is.na(DailySD), DAILYSD, DailySD)) %>%
    mutate(NorWeST_ID = ifelse(is.na(NorWeST_ID), NorWest_ID, NorWeST_ID)) %>%
    select(-NoRWeST_ID, -NorWest_ID, -DAILYSD) %>%
    left_join(sites, by = c('region', 'OBSPRED_ID')) %>%
    filter(!grepl('nwis', Source, ignore.case = TRUE)) %>% # assume we picked these up in our own NWIS call
    filter(!is.na(DailyMean)) %>%
    filter(!is.na(site_meta)) # there were 32 sites from the "OregonCoast" files that did not have metadata

  # handle units
  # most have units, but 2.4 million do not
  # group by site and year, find min, max, range
  no_units <- filter(dat_out, is.na(UOM)|UOM %in% 'Y') %>%
    mutate(year = lubridate::year(SampleDate)) %>%
    group_by(region, NorWeST_ID, year) %>%
    summarize(min_temp = min(DailyMean),
              max_temp = max(DailyMean),
              nobs = n()) %>%
    mutate(UOM2 = case_when(max_temp < 35 & min_temp < 32 ~ 'deg C',
                           min_temp >= 32 & max_temp >=35 ~ 'deg F',
                           max_temp < 32 ~ 'deg C',
                           min_temp < 30 ~ 'deg C',
                           TRUE ~ 'unknown')) %>%
    select(region, NorWeST_ID, year, UOM2)

  dat_out <- left_join(mutate(dat_out, year = lubridate::year(SampleDate)), no_units) %>%
    mutate(UOM = ifelse(is.na(UOM)|UOM %in% 'Y', UOM2, UOM)) %>%
    select(-UOM2)

  # convert units
  dat_out <- dat_out %>%
    mutate(DailyMean = ifelse(grepl('C', UOM, ignore.case = TRUE), DailyMean, f_to_c(DailyMean)),
           DailyMin = ifelse(grepl('C', UOM, ignore.case = TRUE), DailyMin, f_to_c(DailyMin)),
           DailyMax = ifelse(grepl('C', UOM, ignore.case = TRUE), DailyMax, f_to_c(DailyMax))) %>%
    filter(DailyMean > 0 & DailyMean < 35)

  # select columns for output
  dat_out <- select(dat_out, -DailySD, -DailyRange, -SampleYear, -UOM, -site_meta, -year) %>%
    distinct()

  # filter out nwis data, assume we got it in our nwis pull
  saveRDS(dat_out, file = as_data_file(out_ind))
  gd_put(out_ind)
}

combine_all_dat <- function(wqp_ind, nwis_ind, ecosheds_ind, norwest_ind, out_ind) {
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
    select(site_id, date, temp_degC = mean, n_obs = n, source)

  norwest <- readRDS(sc_retrieve(norwest_ind, 'getters.yml')) %>%
    mutate(source = 'norwest',
           site_id = paste(region, OBSPRED_ID, sep = '_')) %>%
    select(site_id, date = SampleDate, temp_degC = DailyMean, n_obs = Nobs, source)

  all_dat <- bind_rows(wqp, nwis, ecosheds, norwest) %>%
    distinct(site_id, date, temp_degC, .keep_all = TRUE)

  # save
  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(all_dat, data_file)
  gd_put(out_ind)

}

combine_all_sites <- function(nwis_dv_sites_ind, nwis_uv_sites_ind, wqp_sites_ind, ecosheds_sites_ind, norwest_sites_ind, out_ind){
  norwest_sites <- readRDS(sc_retrieve(norwest_sites_ind, 'getters.yml')) %>%
    mutate(source = 'norwest',
           original_source = Source,
           site_id = paste(region, OBSPRED_ID, sep = '_'),
           site_type = 'ST') %>%
    st_transform(crs = "+proj=longlat +datum=WGS84") %>%
    mutate(longitude = st_coordinates(.)[,1],
           latitude = st_coordinates(.)[,2]) %>%
    select(site_id, site_type, latitude, longitude, source, original_source) %>%
    st_drop_geometry()

  nwis_sites <- feather::read_feather(sc_retrieve(nwis_dv_sites_ind, remake_file = 'getters.yml')) %>%
    mutate(source = 'nwis_dv') %>%
    bind_rows(feather::read_feather(sc_retrieve(nwis_uv_sites_ind, remake_file = 'getters.yml')) %>%
                mutate(source = 'nwis_uv')) %>%
    mutate(site_id = paste0('USGS-', site_no)) %>%
    select(site_id, site_type = site_tp_cd, latitude = dec_lat_va, longitude = dec_long_va, source, original_source = agency_cd)

  wqp_sites <- feather::read_feather(sc_retrieve(wqp_sites_ind, remake_file = 'getters.yml')) %>%
    mutate(source = 'wqp') %>%
    select(site_id = MonitoringLocationIdentifier, latitude, longitude,
           site_type = ResolvedMonitoringLocationTypeName, source, original_source = OrganizationIdentifier)

  ecosheds_sites <- readRDS(sc_retrieve(ecosheds_sites_ind, remake_file = 'getters.yml')) %>%
    mutate(source = 'ecosheds', site_type = 'stream',
           site_id = as.character(location_id)) %>%
    select(site_id, latitude, longitude, site_type, source, original_source = agency_description)

  all_sites <- bind_rows(nwis_sites, wqp_sites, ecosheds_sites, norwest_sites)

  saveRDS(all_sites, as_data_file(out_ind))
  gd_put(out_ind)
}

summarize_all_dat <- function(in_ind, out_file) {
  dat_summary <- readRDS(sc_retrieve(in_ind, remake_file = 'getters.yml'))

  summary <- data.frame(n_obs = nrow(dat_summary), n_sites = length(unique(dat_summary$site_id)))

  readr::write_csv(summary, out_file)
}
