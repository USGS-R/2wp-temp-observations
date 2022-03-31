# mungers for WQP data

filter_site_types <- function(in_ind, sites_ind, keep_types, out_ind) {

  sites <- feather::read_feather(sc_retrieve(sites_ind, remake_file = 'getters.yml')) %>%
    filter(ResolvedMonitoringLocationTypeName %in% keep_types) %>%
    pull(MonitoringLocationIdentifier)

  dat <- readRDS(sc_retrieve(in_ind, remake_file = 'getters.yml')) %>%
    filter(MonitoringLocationIdentifier %in% sites) %>%
    filter(!CharacteristicName %in% 'Water, sample')

  saveRDS(dat, as_data_file(out_ind))
  gd_put(out_ind)


}

munge_wqp_withdepths <- function(in_ind, min_value, max_value, max_daily_range, out_ind) {
  dat <- readRDS(sc_retrieve(in_ind, remake_file = 'getters.yml')) %>%
    filter(!is.na(`ResultMeasure/MeasureUnitCode`)) %>%
    filter(ActivityMediaName %in% 'Water') %>%
    filter(!is.na(`ActivityDepthHeightMeasure/MeasureValue`)|
             !is.na(`ResultDepthHeightMeasure/MeasureValue`)|
             !is.na(`ActivityTopDepthHeightMeasure/MeasureValue`)|
             !is.na(`ActivityBottomDepthHeightMeasure/MeasureValue`))
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
    mutate(`ResultMeasure/MeasureUnitCode` = 'deg C')

  dat_daily <- group_by(dat_reduced, MonitoringLocationIdentifier, ActivityStartDate, sample_depth) %>%
    summarize(temperature_mean_daily = mean(ResultMeasureValue),
              temperature_min_daily = min(ResultMeasureValue),
              temperature_max_daily = max(ResultMeasureValue),
              n_obs = n()) %>%
    filter(temperature_mean_daily > min_value & temperature_mean_daily < max_value,
           temperature_min_daily > min_value & temperature_min_daily < max_value,
           temperature_max_daily > min_value & temperature_max_daily < max_value) %>%
    filter(sample_depth > 0)

  dat_daily_meta <- select(dat_reduced, -ResultMeasureValue, -`ActivityStartTime/Time`) %>%
    group_by(MonitoringLocationIdentifier, ActivityStartDate, sample_depth) %>%
    summarize_all(first)

  # bring it together
  dat_daily <- left_join(dat_daily, dat_daily_meta)

  # save
  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(dat_daily, data_file)
  gd_put(out_ind)

}

munge_wqp_withoutdepths <- function(in_ind, min_value, max_value, max_daily_range, out_ind) {
  dat <- readRDS(sc_retrieve(in_ind, remake_file = 'getters.yml')) %>%
    select(MonitoringLocationIdentifier, ActivityStartDate, ResultMeasureValue,
           `ResultMeasure/MeasureUnitCode`, `ActivityStartTime/Time`, ActivityMediaName,
           `ActivityDepthHeightMeasure/MeasureValue`, `ResultDepthHeightMeasure/MeasureValue`,
           `ActivityTopDepthHeightMeasure/MeasureValue`, `ActivityBottomDepthHeightMeasure/MeasureValue`) %>%
    filter(!is.na(`ResultMeasure/MeasureUnitCode`)) %>%
    filter(ActivityMediaName %in% 'Water') %>%
    filter(is.na(`ActivityDepthHeightMeasure/MeasureValue`)&
             is.na(`ResultDepthHeightMeasure/MeasureValue`)&
             is.na(`ActivityTopDepthHeightMeasure/MeasureValue`)&
             is.na(`ActivityBottomDepthHeightMeasure/MeasureValue`))


  # now reduce to daily measures
  # first clean up obviously bad data
  dat_reduced <- dat %>%
    filter(grepl('deg C|deg F', `ResultMeasure/MeasureUnitCode`, ignore.case = TRUE)) %>%
    mutate(ResultMeasureValue = ifelse(grepl('deg F', `ResultMeasure/MeasureUnitCode`, ignore.case = TRUE),
                                       f_to_c(ResultMeasureValue), ResultMeasureValue)) %>%
    mutate(`ResultMeasure/MeasureUnitCode` = 'deg C')

  dat_daily <- group_by(dat_reduced, MonitoringLocationIdentifier, ActivityStartDate) %>%
    summarize(temperature_mean_daily = mean(ResultMeasureValue),
              temperature_min_daily = min(ResultMeasureValue),
              temperature_max_daily = max(ResultMeasureValue),
              n_obs = n()) %>%
    filter(temperature_mean_daily > min_value & temperature_mean_daily < max_value,
           temperature_min_daily > min_value & temperature_min_daily < max_value,
           temperature_max_daily > min_value & temperature_max_daily < max_value)

  # save
  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(dat_daily, data_file)
  gd_put(out_ind)

}
