#### pull ####

# we've been asked to please NOT use a cluster because a postdoc took down the
# entire WQP a couple of years ago by doing that, and 12 simultaneous requests
# would be way too many. Alternative ways we can speed up the pull:
# * subset spatially rather than temporally - loop over state/HUC rather than years
# * probably fastest to pull all variables at once
# * probably faster to request whole states at once rather than giving explicit site lists
# we'll follow the first two guidelines but won't be able to make use of the third this time

# prepare a plan for downloading (from WQP) and posting (to GD) one data file
# per state
plan_wqp_pull <- function(partitions_ind) {

  folders <- list(
    tmp='1_wqp_pull/tmp',
    out='1_wqp_pull/out',
    log='1_wqp_pull/log')
  partitions <- feather::read_feather(scipiper::sc_retrieve(partitions_ind))

  # isolate the partition info for just one task
  partition <- scipiper::create_task_step(
    step_name = 'partition',
    target_name = function(task_name, step_name, ...) {
      sprintf('partition_%s', task_name)
    },
    command = function(task_name, ...) {
      sprintf("filter_partitions(partitions_ind='%s', I('%s'))", partitions_ind, task_name)
    }
  )

  # download from WQP, save, and create .ind file promising the download is
  # complete, with extension .tind ('temporary indicator') because we don't want
  # scipiper to make a build/status file for it
  download <- scipiper::create_task_step(
    step_name = 'download',
    target_name = function(task_name, step_name, ...) {
      file.path(folders$tmp, sprintf('%s.rds', task_name))
    },
    command = function(steps, ...) {
      paste(
        "get_wqp_data(",
        "data_file=target_name,",
        sprintf("partition=%s,", steps$partition$target_name),
        "wqp_pull_params=wqp_pull_parameters)",
        sep="\n      ")
    }
  )

  # put the steps together into a task plan
  task_plan <- scipiper::create_task_plan(
    task_names=sort(partitions$PullTask),
    task_steps=list(partition, download),
    final_steps=c('download'),
    add_complete=FALSE)

}

create_wqp_pull_makefile <- function(makefile, task_plan, final_targets) {

  # after all wanted data have been pulled, this function will be called but
  # doesn't need to create anything much, so just write an empty file
  if(is.null(task_plan)) {
    message('WQP pull is up to date, so writing empty task remake file')
    readr::write_lines('', makefile)
    return()
  }

  create_task_makefile(
    makefile = makefile, 
    task_plan = task_plan,
    include = '1_wqp_pull.yml',
    packages = c('dplyr', 'dataRetrieval', 'feather', 'scipiper', 'yaml', 'httr', 'readr'),
    file_extensions = c('ind','rds'),
    final_targets = final_targets, 
    finalize_funs = 'combine_wqp_dat')
}

loop_wqp_tasks <- function(ind_file, task_plan, ...) {
  if(is.null(task_plan)) {
    message('WQP pull is up to date, so writing empty task .ind file')
    readr::write_lines('WQP pull is up to date', ind_file)
    return()
  }
  loop_tasks(task_plan, ...)
}

# --- functions used in task table ---

# read the partitions file and pull out just those sites that go with a single
# PullTask and are worth requesting because they have >0 observations for that
# ParamGroup
filter_partitions <- function(partitions_ind, pull_task) {
  partitions <- feather::read_feather(sc_retrieve(partitions_ind))
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
  
 
  getWQP <- function(wqp_args) {
    wqp_dat_time <- tryCatch(
      {
        time_start <- Sys.time()
        wqp_dat <- wqp_POST(wqp_args)
        time_end <- Sys.time()
        time_diff <- time_end - time_start
        
        return(list(time_diff, wqp_dat, 'POST'))
       
        
      },
      error = function(cond) {
        message('Call to WQP using POST failed. Trying via readWQPdata. Original error message:')
        message(cond)
        
        time_start <- Sys.time()
        wqp_dat <- readWQPdata(siteid = wqp_args$siteid,
                               characteristicName = wqp_args$characteristicName)
        time_end <- Sys.time()
        time_diff <- time_end - time_start
        
        return(list(time_diff, wqp_dat, 'readWQPdata'))
      }
    )
    return(wqp_dat_time)
  }
  
  wqp_dat_time <- getWQP(wqp_args)
  
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
  wqp_url <- "https://www.waterqualitydata.us/Result/search"
  
  
  wqp_args_list$siteid <- wqp_args_list$siteid
  post_body = jsonlite::toJSON(wqp_args_list, pretty = TRUE)
  
  temp <- tempfile()
  temp <- paste0(temp,".zip")
  x <- POST(paste0(wqp_url,"?mimeType=tsv&zip=yes"),
            body = post_body,
            content_type("application/json"),
            accept("application/zip"),
            httr::write_disk(temp))
  
  headerInfo <- httr::headers(x)
  file1 <- tempdir()
  doc <- utils::unzip(temp, exdir=file1)
  unlink(temp)
  retval <- suppressWarnings(read_delim(doc, 
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
  unlink(doc)
  return(retval)
}

# extract and post the data (dropping the site info attribute), creating an
# .ind file that we will want to share because it represents the shared cache
combine_wqp_dat <- function(ind_file, ...){
  
  rds_files <- c(...)
  df_list <- list()
  
  # create a readRDS function that accomdates some column type issues
  readRDS2 <- function(.) {
    dat_mod <- readRDS(.) %>%
      filter(!is.na(ResultMeasureValue)) %>%
      select(MonitoringLocationIdentifier, ActivityMediaName, ActivityMediaSubdivisionName, ActivityStartDate, `ActivityStartTime/Time`, `ActivityStartTime/TimeZoneCode`,
             `ActivityDepthHeightMeasure/MeasureValue`, `ActivityDepthHeightMeasure/MeasureUnitCode`,   
            `ActivityTopDepthHeightMeasure/MeasureValue`,`ActivityTopDepthHeightMeasure/MeasureUnitCode`, 
            `ActivityBottomDepthHeightMeasure/MeasureValue`, `ActivityBottomDepthHeightMeasure/MeasureUnitCode`,
            ActivityCommentText, `SampleCollectionMethod/MethodIdentifier`, `SampleCollectionMethod/MethodIdentifierContext`,
            `SampleCollectionMethod/MethodName`,ResultDetectionConditionText, CharacteristicName, ResultMeasureValue, 
            `ResultMeasure/MeasureUnitCode`, MeasureQualifierCode,
            ResultStatusIdentifier, StatisticalBaseCode, ResultValueTypeName, PrecisionValue, 
            ResultCommentText, USGSPCode, `ResultDepthHeightMeasure/MeasureValue`, `ResultDepthHeightMeasure/MeasureUnitCode`, ProviderName) %>%
      mutate(PrecisionValue = as.numeric(PrecisionValue)) %>%
      mutate_at(vars(contains('MeasureValue')), is.numeric) %>%
      mutate_if(is.logical, as.character) %>%
      select_if(~!all(is.na(.)))
  }
  
  for (i in seq_len(length(rds_files))){
    df_list[[i]] <- readRDS2(rds_files[i])
  }
  message('Binding all WQP files.')
  wqp_df <- do.call("bind_rows", df_list)
  
  data_file <- scipiper::as_data_file(ind_file)
  saveRDS(wqp_df, data_file)
  gd_put(ind_file, data_file)
}
