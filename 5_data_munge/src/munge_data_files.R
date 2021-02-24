f_to_c <- function(f) {
  c <- (f - 32) * (5/9)
  return(c)
}

munge_nwis <- function(dv_ind, uv_ind, min_value, max_value, out_ind) {

  dv <- readRDS(sc_retrieve(dv_ind, remake_file = 'getters.yml')) %>%
    mutate(source = 'nwis_dv')

  uv <- readRDS(sc_retrieve(uv_ind, remake_file = 'getters.yml')) %>%
    mutate(source = 'nwis_uv')

  # keeping provisional data for now
  # need to keep rows where mean is NA because sometimes still min/max values
  nwis <- bind_rows(dv, uv) %>%
    #filter(grepl('A', cd_value, ignore.case = FALSE)) %>%
    filter(Mean_temperature > min_value & Mean_temperature < max_value|is.na(Mean_temperature),
           Max_temperature > min_value & Max_temperature < max_value|is.na(Max_temperature),
           Min_temperature > min_value & Min_temperature < max_value|is.na(Min_temperature))

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
    mutate(mean = f_to_c(mean),
           min = f_to_c(min),
           max = f_to_c(max))

  # bind with "good" data
  # remove data above 35, below 0
  dat_out <- filter(dat, !series_id %in% unique(over_45$series_id)) %>%
    bind_rows(high_series) %>%
    filter(mean > min_value & mean < max_value) %>%
    filter(min > min_value & min < max_value) %>%
    filter(max > min_value & max < max_value)

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
    filter(!is.na(DailyMean)|!is.na(DailyMin)|!is.na(DailyMax)) %>%
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
    filter(DailyMean > min_value & DailyMean < max_value|is.na(DailyMean),
           DailyMin > min_value & DailyMin < max_value|is.na(DailyMin),
           DailyMax > min_value & DailyMax < max_value|is.na(DailyMax))

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
           mean_temp_degC = temperature_mean_daily,
           min_temp_degC = temperature_min_daily,
           max_temp_degC = temperature_max_daily,
           n_obs, source)

  nwis <- readRDS(sc_retrieve(nwis_ind, remake_file = 'getters.yml')) %>%
    mutate(site_id = paste0('USGS-', site_no)) %>%
    select(site_id, date = Date, mean_temp_degC = Mean_temperature,
           min_temp_degC = Min_temperature, max_temp_degC = Max_temperature, n_obs, source)

  ecosheds <- readRDS(sc_retrieve(ecosheds_ind, remake_file = 'getters.yml')) %>%
    mutate(source = 'ecosheds',
           site_id = as.character(location_id)) %>%
    select(site_id, date, mean_temp_degC = mean, min_temp_degC = min, max_temp_degC = max, n_obs = n, source)

  norwest <- readRDS(sc_retrieve(norwest_ind, 'getters.yml')) %>%
    mutate(source = 'norwest',
           site_id = paste(region, OBSPRED_ID, sep = '_')) %>%
    select(site_id, date = SampleDate, mean_temp_degC = DailyMean,
           min_temp_degC = DailyMin, max_temp_degC = DailyMax, n_obs = Nobs, source)

  all_dat <- bind_rows(nwis, wqp, ecosheds, norwest) %>%
    distinct(site_id, date, mean_temp_degC, min_temp_degC, max_temp_degC, .keep_all = TRUE)

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

clean_sites <- function(in_ind, out_ind) {

  proj_albers <- "+proj=laea +lat_0=45 +lon_0=-100 +x_0=0 +y_0=0 +a=6370997 +b=6370997 +units=m +no_defs"

  states <- st_as_sf(maps::map("state", plot = FALSE, fill = TRUE)) %>%
    bind_rows(mutate(st_as_sf(maps::map('world', 'USA:Alaska', plot = FALSE, fill = TRUE)), ID = 'alaska'),
              mutate(st_as_sf(maps::map('world', 'USA:Hawaii', plot = FALSE, fill = TRUE)), ID = 'hawaii'),
              mutate(st_as_sf(maps::map('world', 'Puerto Rico', plot = FALSE, fill = TRUE)), ID = 'puerto rico')) %>%
    st_transform(crs = proj_albers)

  #states_with_buffer <- st_buffer(states, dist = 1)
  # filter sites that are outside of the bounding box
  sites <- readRDS(sc_retrieve(in_ind, 'getters.yml')) %>%
    distinct() %>%
    # filter to stream sites
    filter(site_type %in% c('ST', 'ST-TS', 'ST-CA','ST-DCH', 'Spring', 'Stream', 'stream', '')) %>%
    filter(!is.na(longitude)|!is.na(latitude)) %>%
    st_as_sf(coords = c('longitude', 'latitude'), crs = 4326) %>% #create sf object
    st_transform(crs = proj_albers) %>%
    st_join(states, join = st_within) # match sites that are within state polygons

  # find path forward for those sites outside of polygons
  sites_no_state <- filter(sites, is.na(ID))

  # draw a bbox with small buffer for main geographic areas - lower 48, hawaii, alaska, PR
  # puerto rico
  puerto <- st_intersects(sites_no_state, st_buffer(states[52, ], dist = 3000))
  pot_puerto <- sites_no_state[which(!is.na(as.numeric(puerto))), ]

  # alaska
  # some island weirdness here (low resolution shapefile?), so had to up the buffer distance
  alaska <- st_intersects(sites_no_state, st_buffer(states[50, ], dist = 40000))
  pot_alaska <- sites_no_state[which(!is.na(as.numeric(alaska))), ]

  # hawaii
  hawaii <- st_intersects(sites_no_state, st_buffer(states[51, ], dist = 4000))
  pot_hawaii <- sites_no_state[which(!is.na(as.numeric(hawaii))), ]

  # lower 48
  # have to use a big buffer to get some of the estuary sites out east
  lower_shape <- st_transform(st_as_sf(maps::map('world','USA(?!:hawaii)(?!:alaska)', plot = FALSE, fill = TRUE)), crs = proj_albers)
  lower <- st_intersects(sites_no_state, st_buffer(lower_shape, dist = 20000))
  pot_lower <- sites_no_state[which(!is.na(as.numeric(lower))), ] %>% select(-ID)

  # find nearest shape for each of these potential sites
  # for out-states, we assume those to be the outstate
  # for lower 48, we use nearest feature join
  sites_fixed_ids <- mutate(pot_puerto, ID = 'puerto rico') %>%
    bind_rows(mutate(pot_alaska, ID = 'alaska')) %>%
    bind_rows(mutate(pot_hawaii, ID = 'hawaii')) %>%
    bind_rows(st_join(x = pot_lower, y = states, join = st_nearest_feature))

  # check sites that have no match
  # these are ALL WQP sites
  # some of these are in Samoan Islands, some appear to have the wrong longitude (high positive numbers)
  # e.g., site 	21FLCOSP_WQX-45-03, if I turn the longitude to a negative value, lands in a stream in Florida
  # just ignoring these for now
  test <- filter(sites_no_state, !site_id %in% sites_fixed_ids$site_id)

  sites_fixed <- filter(sites, !is.na(ID)) %>%
    bind_rows(sites_fixed_ids) %>%
    st_transform(crs = 4326) %>%
    mutate(longitude = sf::st_coordinates(.)[,1],
           latitude = sf::st_coordinates(.)[,2])

  saveRDS(sites_fixed, as_data_file(out_ind))
  gd_put(out_ind)

}
