# --- functions used in task table ---

# read the partitions file and pull out just those sites that go with a single
# PullTask and are worth requesting because they have >0 observations for that
# ParamGroup
filter_partitions <- function(partitions_ind, pull_task) {
  partitions <- feather::read_feather(sc_retrieve(partitions_ind, 'getters.yml'))
  dplyr::filter(partitions, PullTask==pull_task, SiteNumObs > 0)
}

# pull a batch of WQP observations, save locally, return .tind file
get_wqp_data <- function(data_file, partition, wqp_pull_params, verbose = TRUE) {

  # prepare the arguments to pass to readWQPdata
  wqp_args <- list()
  wqp_args$characteristicName <- wqp_pull_params$characteristicName$temperature
  wqp_args$siteid <- partition$MonitoringLocationIdentifier
  # do the data pull
  # first pull using readWQPdata, then if that fails, try POST

  wqp_post_try <- function(wqp_args) {

    time_start <- Sys.time()
    wqp_dat <- wqp_POST(wqp_args)
    time_end <- Sys.time()
    time_diff <- time_end - time_start

    return(list(time_diff, wqp_dat, 'POST'))
  }

  wqp_readWQP_try <- function(wqp_args) {
    time_start <- Sys.time()
    wqp_dat <- readWQPdata(siteid = wqp_args$siteid,
                           characteristicName = wqp_args$characteristicName)
    time_end <- Sys.time()
    time_diff <- time_end - time_start

    return(list(time_diff, wqp_dat, 'readWQPdata'))
  }

    wqp_out <- try(wqp_post_try(wqp_args))

    if (class(wqp_out) == 'try-error') {
      message("Error with call to POST, trying dataRetrieval::readWQPdata")
      wqp_out <- wqp_readWQP_try(wqp_args)
    }

  wqp_dat_time <- wqp_out

  # wqp_dat_time <- system.time({
  #   wqp_dat <- wqp_POST(wqp_args)
  # })
  #
  # wqp_dr_dat_time <- system.time(dat_dr <- readWQPdata(siteid = wqp_args$siteid,
  #                                                      characteristicName = wqp_args$characteristicName))
  if (verbose){
    message(sprintf(
      'WQP pull (using %s) for %s took %0.1f %s and returned %d rows',
      wqp_dat_time[[3]],
      partition$PullTask[1],
      as.numeric(wqp_dat_time[[1]]),
      attr(wqp_dat_time[[1]], 'units'),
      nrow(wqp_dat_time[[2]])))
  }
  # make wqp_dat a tibble, converting either from data.frame (the usual case) or
  # NULL (if there are no results)
  wqp_dat <- as_tibble(wqp_dat_time[[2]])

  # write the data to rds file. do this even if there were 0
  # results because remake expects this function to always create the target
  # file
  saveRDS(wqp_dat, data_file)
}

# hack around w/ https://github.com/USGS-R/dataRetrieval/issues/434
wqp_POST <- function(wqp_args_list){
  wqp_url <- "https://www.waterqualitydata.us/data/Result/search"


  wqp_args_list$siteid <- wqp_args_list$siteid
  post_body = jsonlite::toJSON(wqp_args_list, pretty = TRUE)

  download_location <- tempfile(pattern = ".zip")
  pull_metadata <- POST(paste0(wqp_url,"?mimeType=tsv&zip=yes"),
                        body = post_body,
                        content_type("application/json"),
                        accept("application/zip"),
                        httr::write_disk(download_location))

  headerInfo <- httr::headers(pull_metadata)
  unzip_location <- tempdir()
  unzipped_filename <- utils::unzip(download_location, exdir=unzip_location)
  unlink(download_location)
  dat_out <- suppressWarnings(
    read_delim(
      unzipped_filename,
      col_types = cols(`ActivityStartTime/Time` = col_character(),
                       `ActivityEndTime/Time` = col_character(),
                       USGSPCode = col_character(),
                       ResultCommentText=col_character(),
                       `ActivityDepthHeightMeasure/MeasureValue` = col_number(),
                       `DetectionQuantitationLimitMeasure/MeasureValue` = col_number(),
                       ResultMeasureValue = col_number(),
                       `WellDepthMeasure/MeasureValue` = col_number(),
                       `WellHoleDepthMeasure/MeasureValue` = col_number(),
                       `HUCEightDigitCode` = col_character(),
                       `ActivityEndTime/TimeZoneCode` = col_character()),
      quote = "", delim = "\t"))
  unlink(unzipped_filename)
  return(dat_out)
}


# extract and post the data (dropping the site info attribute), creating an
# .ind file that we will want to share because it represents the shared cache
combine_wqp_dat <- function(ind_file, ...){

  rds_files <- c(...)
  df_list <- list()

  # create a readRDS function that accomdates some column type issues
  readRDS2 <- function(.) {

    dat_mod <- readRDS(.)

    # this is to catch pulls made with dataRetrieval, which returns
    # periods in the columns names versus forward slashes
    if (any(grepl('\\.', names(dat_mod)))) {
      names(dat_mod) <- gsub('\\.', '\\/', names(dat_mod))
    }
    dat_mod <- dat_mod %>%
      filter(!is.na(ResultMeasureValue)) %>%
      select(MonitoringLocationIdentifier, ActivityMediaName, ActivityStartDate, `ActivityStartTime/Time`, `ActivityStartTime/TimeZoneCode`,
             `ActivityDepthHeightMeasure/MeasureValue`, `ActivityDepthHeightMeasure/MeasureUnitCode`,
             `ActivityTopDepthHeightMeasure/MeasureValue`,`ActivityTopDepthHeightMeasure/MeasureUnitCode`,
             `ActivityBottomDepthHeightMeasure/MeasureValue`, `ActivityBottomDepthHeightMeasure/MeasureUnitCode`,
             ActivityCommentText, `SampleCollectionMethod/MethodIdentifier`, `SampleCollectionMethod/MethodIdentifierContext`,
             `SampleCollectionMethod/MethodName`,CharacteristicName, ResultMeasureValue,
             `ResultMeasure/MeasureUnitCode`, ResultValueTypeName, PrecisionValue,
             ResultCommentText, `ResultDepthHeightMeasure/MeasureValue`, `ResultDepthHeightMeasure/MeasureUnitCode`, ProviderName) %>%
      mutate(PrecisionValue = as.numeric(PrecisionValue)) %>%
      mutate_at(vars(contains('MeasureValue')), as.numeric) %>%
      mutate_at(vars(contains('Method')), as.character) %>%
      mutate_at(vars(contains('Comment')), as.character) %>%
      mutate_if(is.logical, as.character) %>%
      select_if(~!all(is.na(.)))
  }

  for (i in seq_len(length(rds_files))){
    message(paste0('Reading in file ', rds_files[i]))
    df_list[[i]] <- readRDS2(rds_files[i])
  }
  message('Binding all WQP files.')
  wqp_df <- do.call("bind_rows", df_list)

  data_file <- scipiper::as_data_file(ind_file)
  saveRDS(wqp_df, data_file)
  gd_put(ind_file, data_file)
}
