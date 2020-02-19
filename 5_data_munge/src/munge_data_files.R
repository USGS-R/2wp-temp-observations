f_to_c <- function(f) {
  c <- (f - 32) * (5/9)
  return(c)
}

munge_wqp_withdepths <- function(in_ind, min_value, max_value, max_daily_range, out_ind) {
  dat <- readRDS(sc_retrieve(in_ind)) %>%
    filter(!is.na(`ResultMeasure/MeasureUnitCode`)) %>%
    filter(ActivityMediaName %in% 'Water') %>%
    filter(!is.na(`ActivityDepthHeightMeasure/MeasureValue`)|
                         !is.na(`ResultDepthHeightMeasure/MeasureValue`)|
                         !is.na(`ActivityTopDepthHeightMeasure/MeasureValue`)|
                         !is.na(`ActivityBottomDepthHeightMeasure/MeasureValue`)) %>% 
    filter(ActivityStartDate > as.Date('1800-01-01') & ActivityStartDate < as.Date('2020-01-01'))
  
  # create depth column
  dat <- mutate(dat, sample_depth = case_when(
    !is.na(`ActivityDepthHeightMeasure/MeasureValue`) ~ `ActivityDepthHeightMeasure/MeasureValue`,
    is.na(`ActivityDepthHeightMeasure/MeasureValue`) & !is.na(`ResultDepthHeightMeasure/MeasureValue`) ~ `ResultDepthHeightMeasure/MeasureValue`,
    is.na(`ActivityDepthHeightMeasure/MeasureValue`) & is.na(`ResultDepthHeightMeasure/MeasureValue`) & !is.na(`ActivityTopDepthHeightMeasure/MeasureValue`) ~ `ActivityTopDepthHeightMeasure/MeasureValue`,
    is.na(`ActivityDepthHeightMeasure/MeasureValue`) & is.na(`ResultDepthHeightMeasure/MeasureValue`) & is.na(`ActivityTopDepthHeightMeasure/MeasureValue`) ~ `ActivityBottomDepthHeightMeasure/MeasureValue`
  ))
  
  dat <- mutate(dat, sample_depth_unit_code = case_when(
    !is.na(`ActivityDepthHeightMeasure/MeasureValue`) ~ `ActivityDepthHeightMeasure/MeasureUnitCode`,
    is.na(`ActivityDepthHeightMeasure/MeasureValue`) & !is.na(`ResultDepthHeightMeasure/MeasureValue`) ~ `ResultDepthHeightMeasure/MeasureUnitCode`,
    is.na(`ActivityDepthHeightMeasure/MeasureValue`) & is.na(`ResultDepthHeightMeasure/MeasureValue`) & !is.na(`ActivityTopDepthHeightMeasure/MeasureValue`) ~ `ActivityTopDepthHeightMeasure/MeasureUnitCode`,
    is.na(`ActivityDepthHeightMeasure/MeasureValue`) & is.na(`ResultDepthHeightMeasure/MeasureValue`) & is.na(`ActivityTopDepthHeightMeasure/MeasureValue`) ~ `ActivityBottomDepthHeightMeasure/MeasureUnitCode`
  ))
  

  # now reduce to daily measures
  dat_reduced <- dat %>%
    filter(grepl('deg C|deg F', `ResultMeasure/MeasureUnitCode`, ignore.case = TRUE)) %>%
    mutate(ResultMeasureValue = ifelse(grepl('deg F', `ResultMeasure/MeasureUnitCode`, ignore.case = TRUE),
                                       f_to_c(ResultMeasureValue), ResultMeasureValue)) %>%
    filter(ResultMeasureValue > min_value,
           ResultMeasureValue < max_value) %>%
    mutate(`ResultMeasure/MeasureUnitCode` = 'deg C')
    
  dat_daily <- group_by(dat_reduced, MonitoringLocationIdentifier, ActivityStartDate, sample_depth) %>%
    summarize(temperature_mean_daily = mean(ResultMeasureValue),
              activity_start_times = paste(`ActivityStartTime/Time`, collapse = ', '),
              n_day = n())
  
  dat_daily_meta <- select(dat_reduced, -ResultMeasureValue, -`ActivityStartTime/Time`) %>%
    group_by(MonitoringLocationIdentifier, ActivityStartDate, sample_depth) %>%
    summarize_all(first)
  
  # bring it together
  dat_daily <- left_join(dat_daily, dat_daily_meta)
  
  # save
  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(dat_daily, data_file)
  gd_put(out_ind, data_file)
    
}

munge_wqp_withoutdepths <- function(in_ind, min_value, max_value, max_daily_range, out_ind) {
  dat <- readRDS(sc_retrieve(in_ind)) %>%
    select(MonitoringLocationIdentifier, ActivityStartDate, ResultMeasureValue, 
           `ResultMeasure/MeasureUnitCode`, `ActivityStartTime/Time`, ActivityMediaName, 
           `ActivityDepthHeightMeasure/MeasureValue`, `ResultDepthHeightMeasure/MeasureValue`, 
           `ActivityTopDepthHeightMeasure/MeasureValue`, `ActivityBottomDepthHeightMeasure/MeasureValue`) %>%
    filter(!is.na(`ResultMeasure/MeasureUnitCode`)) %>%
    filter(ActivityMediaName %in% 'Water') %>%
    filter(is.na(`ActivityDepthHeightMeasure/MeasureValue`)&
             is.na(`ResultDepthHeightMeasure/MeasureValue`)&
             is.na(`ActivityTopDepthHeightMeasure/MeasureValue`)&
             is.na(`ActivityBottomDepthHeightMeasure/MeasureValue`)) %>%
    filter(ActivityStartDate > as.Date('1800-01-01') & ActivityStartDate < as.Date('2020-01-01'))
  
 
  # now reduce to daily measures
  # first clean up obviously bad data
  dat_reduced <- dat %>%
    filter(grepl('deg C|deg F', `ResultMeasure/MeasureUnitCode`, ignore.case = TRUE)) %>%
    mutate(ResultMeasureValue = ifelse(grepl('deg F', `ResultMeasure/MeasureUnitCode`, ignore.case = TRUE),
                                       f_to_c(ResultMeasureValue), ResultMeasureValue)) %>%
    mutate(`ResultMeasure/MeasureUnitCode` = 'deg C') %>%
    filter(ResultMeasureValue > min_value, ResultMeasureValue < max_value)
  
  dat_daily <- group_by(dat_reduced, MonitoringLocationIdentifier, ActivityStartDate) %>%
    summarize(temperature_mean_daily = mean(ResultMeasureValue),
              activity_start_times = paste(`ActivityStartTime/Time`, collapse = ', '),
              n_day = n())
  
  # save
  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(dat_daily, data_file)
  gd_put(out_ind, data_file)
  
}

munge_nwis_uv <- function(in_ind, min_value, max_value, max_daily_range, out_ind) {
  nwis_df <- readRDS(sc_retrieve(in_ind)) %>%
    filter(!is.na(temp_value), !is.na(dateTime)) %>%
    filter(grepl('A', cd_value, ignore.case = FALSE)) %>%
    filter(temp_value > min_value,
           temp_value < max_value) %>%
    mutate(dateTime = as.Date(dateTime)) %>%
    group_by(site_no, col_name, dateTime) %>%
    summarize(temperature_mean_daily = round(mean(temp_value), 3), n_day = n())
  
  # save
  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(nwis_df, data_file)
  gd_put(out_ind, data_file)
}

munge_nwis_dv <- function(in_ind, min_value, max_value, max_daily_range, out_ind) {

  dat <- readRDS(sc_retrieve(in_ind)) %>%
    filter(grepl('A', cd_value, ignore.case = FALSE)) %>%
    filter(temp_value > min_value,
           temp_value < max_value)
  
  # save
  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(dat, data_file)
  gd_put(out_ind, data_file)
  
}

combine_all_dat <- function(wqp_ind, nwis_dv_ind, nwis_uv_ind, out_ind) {
  wqp <- readRDS(sc_retrieve(wqp_ind)) %>%
    mutate(date = as.Date(ActivityStartDate), source = 'wqp') %>%
    select(site_id = MonitoringLocationIdentifier, 
           date,
           temp_degC = temperature_mean_daily,
           n_obs = n_day, source)
  
  nwis_dv <- readRDS(sc_retrieve(nwis_dv_ind)) %>%
    mutate(date = as.Date(dateTime),
           source = 'nwiw_dv',
           site_id = paste0('USGS-', site_no)) %>%
    select(site_id, date, temp_degC = temp_value, source)
  
  nwis_uv <- readRDS(sc_retrieve(nwis_uv_ind)) %>%
    ungroup() %>%
    mutate(date = as.Date(dateTime),
           source = 'nwiw_uv',
           site_id = paste0('USGS-', site_no)) %>%
    select(site_id, date, temp_degC = temperature_mean_daily, n_obs = n_day, source)
  
  all_dat <- bind_rows(wqp, nwis_dv, nwis_uv)
    
  # save
  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(all_dat, data_file)
  gd_put(out_ind, data_file)
  
}
  
combine_all_sites <- function(nwis_dv_sites_ind, nwis_uv_sites_ind, wqp_sites_ind, out_ind){

  nwis_sites <- feather::read_feather(sc_retrieve(nwis_dv_sites_ind)) %>%
    mutate(source = 'nwis_dv') %>%
    bind_rows(feather::read_feather(sc_retrieve(nwis_uv_sites_ind)) %>% 
                mutate(source = 'nwis_uv')) %>%
    mutate(site_id = paste0('USGS-', site_no)) %>%
    select(site_id, site_type = site_tp_cd, latitude = dec_lat_va, longitude = dec_long_va, source)
  
  wqp_sites <- feather::read_feather(sc_retrieve(wqp_sites_ind)) %>%
    mutate(source = 'wqp') %>%
    select(site_id = MonitoringLocationIdentifier, latitude, longitude, 
           site_type = ResolvedMonitoringLocationTypeName, source)
  
  all_sites <- bind_rows(nwis_sites, wqp_sites)
  
  saveRDS(all_sites, as_data_file(out_ind))
  gd_put(out_ind, as_data_file(out_ind))
}

make_sites_shp <- function(all_sites_ind, all_temps_ind, out_ind) {
  sites <- readRDS(gd_get(all_sites_ind)) # replace gd_get with sc_retrieve when pipeline is shored up
  temps <- readRDS(gd_get(all_temps_ind)) # replace gd_get with sc_retrieve when pipeline is shored up
  
  temp_summary <- temps %>%
    filter(!is.na(date), !is.na(n_obs)) %>%
    group_by(site_id) %>%
    summarize(n_dates = length(unique(date)),
              n_obs = sum(n_obs, na.rm=TRUE), # why are some n_obs=NA?
              sources=paste(sort(unique(source)), collapse=','))
  
  sites_sf <- sites %>%
    left_join(temp_summary, by='site_id') %>%
    filter(!is.na(latitude), !is.na(longitude)) %>%
    sf::st_as_sf(coords=c('longitude','latitude'), crs=sf::st_crs('+proj=longlat +datum=WGS84'))
  
  if(!dir.exists(dirname(out_ind))) dir.create(dirname(out_ind))
  sf::st_write(sites_sf, scipiper::as_data_file(out_ind), layer='sites', delete_layer=TRUE)
  scipiper::sc_indicate(out_ind, data_file=scipiper::as_data_file(out_ind))
  ## TO DO: gd_put instead of just indicate
}

plot_sites_ndates <- function(sites_shp_ind, out_ind) {
  sites_sf <- sf::st_read(scipiper::as_data_file(sites_shp_ind)) # replace as_data_file with sc_retrieve when practical
  
  library(rnaturalearth)
  USA <- ne_countries() %>%
    sf::st_as_sf() %>%
    filter(sovereignt == "United States of America") # to exclude PR, use geounit instead of sovereignt
  
  sites_usa <- sites_sf %>%
    filter(!is.na(n_dates), !is.na(n_obs)) %>%
    sample_n(10000) %>%
    sf::st_intersection(USA) # takes a few minutes!

  g <- sites_usa %>%
    ggplot() +
    geom_sf(data=USA, fill=NA) +
    geom_sf(aes(color=n_dates), size=0.2) +
    scale_color_gradient(trans='log10', low = "#56B1F7", high = "#132B43") +
    theme_bw()
  
  ggsave(scipiper::as_data_file(out_ind), plot=g, width=10, height=8)
  ## TO DO: gd_put instead of just indicate
}
