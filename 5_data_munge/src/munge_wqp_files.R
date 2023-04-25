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
    filter(sample_depth >= 0)

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
    select(MonitoringLocationIdentifier, ActivityStartDate, ResultMeasureValue, StatisticalBaseCode,
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

  dat_reduced_statcode <- ungroup(dat_reduced) %>%
    filter(!is.na(StatisticalBaseCode)) %>%
    group_by(MonitoringLocationIdentifier, ActivityStartDate) %>%
    mutate(temperature_mean_daily = ifelse(grepl('mean', StatisticalBaseCode, ignore.case = TRUE), ResultMeasureValue, NA),
           temperature_min_daily = ifelse(grepl('min', StatisticalBaseCode, ignore.case = TRUE), ResultMeasureValue, NA),
           temperature_max_daily = ifelse(grepl('max', StatisticalBaseCode, ignore.case = TRUE), ResultMeasureValue, NA)) %>%
    # we don't know the number of observations here because stat codes were used
    mutate(n_obs = NA) %>%
    group_by(MonitoringLocationIdentifier, ActivityStartDate) %>%
    summarize(across(c(temperature_mean_daily, temperature_min_daily, temperature_max_daily, n_obs) , ~ first(na.omit(.))))


  dat_daily <- ungroup(dat_reduced) %>%
    filter(is.na(StatisticalBaseCode)) %>%
    group_by(MonitoringLocationIdentifier, ActivityStartDate) %>%
    summarize(n_obs = n(),
              temperature_mean_daily = mean(ResultMeasureValue),
              # we don't want to propagate min and max if there is only one value
              temperature_min_daily = ifelse(n_obs>1, min(ResultMeasureValue), NA),
              temperature_max_daily = ifelse(n_obs>1, max(ResultMeasureValue), NA),
              time = ifelse(n_obs == 1, `ActivityStartTime/Time`, NA)) %>%
    bind_rows(dat_reduced_statcode) %>%
    filter(temperature_mean_daily > min_value & temperature_mean_daily < max_value|is.na(temperature_mean_daily),
           temperature_min_daily > min_value & temperature_min_daily < max_value|is.na(temperature_min_daily),
           temperature_max_daily > min_value & temperature_max_daily < max_value|is.na(temperature_max_daily))

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(dat_daily, data_file)
  gd_put(out_ind)

}

resolve_statcodes <- function(in_ind, out_ind) {

  dat <- readRDS(sc_retrieve(in_ind, remake_file = 'getters.yml')) %>%
    ungroup()

  nrow_o <- nrow(dat)

  dat <- dat %>%
  # drop values that are estimated or blank-corrected
  # drop values that are not min, mean, max
    filter(!ResultValueTypeName %in% c('Estimated', 'Blank Corrected Calc')) %>%
    filter(StatisticalBaseCode %in% c(NA, 'Mean', 'Minimum', 'Maximum', 'mean',
                                      'Geometric Mean', 'Daily Maximum', 'Daily Minimum',
                                      'Daily Geometric Mean'))


  # print message that says how many observations we lost when dropped
  message(paste(nrow_o - nrow(dat), 'observations were dropped due to estimation, blank correction, or statcode that was not mean, min, max'))

  # for some data, the start and end dates are different, and data providers
  # seem to be using these as a date range of the whole dataset
  # sometimes, the proper collection date is in the comment field
  # we're dropping data that has a StatisticalBaseCode because we don't want
  # values averaged over multiple days
  range_dates <- filter(dat, !is.na(ActivityEndDate)) %>%
    filter(ActivityStartDate != ActivityEndDate) %>%
    filter(is.na(StatisticalBaseCode)) %>%
    filter(grepl('Collected', ResultCommentText)) %>%
    mutate(newActivityStartDate = gsub('(Collected: )(.*\\d{4})(\\s*\\d+.*)', '\\2', ResultCommentText, perl = TRUE),
           newActivityStartTime = gsub('(Collected: .*\\d{4}\\s*)(\\d+.*)', '\\2', ResultCommentText, perl = TRUE)) %>%
    mutate(newActivityStartTime = format(strptime(newActivityStartTime, format = '%I:%M %p'), '%H:%M:%S'),
           newActivityStartDate = as.Date(newActivityStartDate, format = "%b %d %Y")) %>%
    select(-ActivityStartDate, -`ActivityStartTime/Time`) %>%
    rename(ActivityStartDate = newActivityStartDate,
           `ActivityStartTime/Time` = newActivityStartTime)

  # print message about date recoveries
  message(paste(nrow(range_dates), 'observations with mismatching start/end dates were recovered by extracting collection dates from comments'))

  # those that don't have collected in the comments
  other <- filter(dat, !is.na(ActivityEndDate)) %>%
    filter(ActivityStartDate != ActivityEndDate) %>%
    filter(is.na(StatisticalBaseCode)) %>%
    filter(!grepl('Collected', ResultCommentText))

  # look at the number of obs per day per site-date to weed out bad sites
  # only keep sites that have one obs per day
  # site dates with > 1440 obs (which is 1obs/minute) was most of the drops here,
  # so we're confident we're dropping bad data
  keep_onesiteday <- other %>%
    group_by(MonitoringLocationIdentifier, ActivityStartDate) %>%
    mutate(n = n()) %>%
    filter(n == 1) %>%
    select(-n) %>% ungroup()

  drop <- other %>%
    group_by(MonitoringLocationIdentifier, ActivityStartDate) %>%
    summarize(n = n()) %>%
    filter(n > 1)

  widnr <- filter(drop, grepl('WIDNR', MonitoringLocationIdentifier))

  message(paste(nrow(drop), 'site-dates and', sum(drop$n),
                'raw observations were dropped because n>1 obs per site-date and start-end dates did not match.',
                'WIDNR was responsible for', nrow(widnr), 'site-dates and', sum(widnr$n), 'raw observations.'))

  statdiffdates <- filter(dat, !is.na(ActivityEndDate)) %>%
    filter(ActivityStartDate != ActivityEndDate) %>%
    filter(!is.na(StatisticalBaseCode))

  message(length(unique(paste(statdiffdates$MonitoringLocationIdentifier, statdiffdates$ActivityStartDate))),
          ' site-dates and ', nrow(statdiffdates), ' raw observations were dropped because the observation had a stat code but different start/end dates')


  out <- filter(dat, ActivityStartDate == ActivityEndDate | is.na(ActivityEndDate)) %>% # keep all data where start/end dates are same
    bind_rows(range_dates) %>% # keep all data where we fixed the dates from the comments
    bind_rows(keep_onesiteday) # keep all data where the start/end was different but there was only one value per site-date

  perc_keep <- round((nrow(dat) - nrow(out))/nrow(dat)*100, 1)
  raw_dropped <- nrow(dat) - nrow(out)


  message(paste(perc_keep, 'percent of observations were dropped...or',
                raw_dropped, 'raw observations were dropped due to mismatch start/end dates'))

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(out, data_file)
  gd_put(out_ind)
}
