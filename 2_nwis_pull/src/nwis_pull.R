#### pull ####

# prepare a plan for downloading (from NWIS) and posting (to GD) one data file
# per state
plan_nwis_pull <- function(partitions_ind, service) {

  folders <- list(
    tmp='2_nwis_pull/tmp',
    out='2_nwis_pull/out',
    log='2_nwis_pull/log')

  partitions <- feather::read_feather(scipiper::sc_retrieve(partitions_ind, remake_file = 'getters.yml'))

  # after all wanted data have been pulled, this function will be called but
  # doesn't need to create anything much, so just return NULL
  # isolate the partition info for just one task

  partition <- scipiper::create_task_step(
    step_name = 'partition',
    target_name = function(task_name, step_name, ...) {
      sprintf('%s_partition_%s', service, task_name)
    },
    command = function(task_name, ...) {
      sprintf("filter_partitions(partitions_ind='%s', I('%s'))", partitions_ind, task_name)
    }
  )

  # download from NWIS, save, and create .ind file promising the download is
  # complete, with extension .tind ('temporary indicator') because we don't want
  # scipiper to make a build/status file for it

  download <- scipiper::create_task_step(
    step_name = 'download',
    target_name = function(task_name, step_name, ...) {
      file.path(folders$tmp, sprintf('%s_%s.qs', service, task_name))
    },
    command = function(steps, ...) {
      paste(
        "get_nwis_data(",
        "data_file=target_name,",
        paste0("service = I('", service, "'),"),
        sprintf("partition=%s,", steps$partition$target_name),
       "nwis_pull_params = nwis_pull_parameters)",
        sep="\n      ")
    }
  )



  # put the steps together into a task plan
  task_plan <- scipiper::create_task_plan(
    task_names=sort(partitions$PullTask),
    task_steps=list(partition, download),
    final_steps=c('download'),
    add_complete=FALSE,
    ind_dir=folders$tmp)

}

create_nwis_pull_makefile <- function(makefile, task_plan, final_targets) {

  create_task_makefile(
    makefile=makefile, task_plan=task_plan,
    include = c('2_nwis_pull.yml'),
    sources = '2_nwis_pull/src/nwis_pull.R',
    packages=c('dplyr', 'dataRetrieval', 'feather', 'scipiper', 'yaml', 'stringr'),
    file_extensions=c('ind','feather'), finalize_funs = 'combine_nwis_data', final_targets = final_targets)
}

# extract and post the data (dropping the site info attribute), creating an
# .ind file that we will want to share because it represents the shared cache
combine_nwis_data <- function(ind_file, ...){

  qs_files <- c(...)
  df_list <- list()

  for (i in seq_len(length(qs_files))){

    message(paste0("Reading in and processing file ", qs_files[i]))
    temp_dat <- qs::qread(qs_files[i])

    if (grepl('_uv_', ind_file)) {
      reduced_dat <- choose_temp_column_uv(temp_dat)
    } else {
      reduced_dat <- choose_temp_column_dv(temp_dat)
    }


    df_list[[i]] <- reduced_dat
  }

  nwis_df <- do.call("bind_rows", df_list)


  data_file <- scipiper::as_data_file(ind_file)
  saveRDS(nwis_df, data_file)
  gd_put(ind_file, data_file)
}

# --- functions used in task table ---

# read the partitions file and pull out just those sites that go with a single
# PullTask
filter_partitions <- function(partitions_ind, pull_task) {

  partitions <- feather::read_feather(sc_retrieve(partitions_ind, remake_file = 'getters.yml'))

  these_partitions <- dplyr::filter(partitions, PullTask==pull_task, count_nu > 0)

  return(these_partitions)
}

# pull a batch of NWIS observations, save locally, return .tind file
get_nwis_data <- function(data_file, partition, nwis_pull_params, service, verbose = TRUE) {

  #nwis_pull_params$service <- service
  nwis_pull_params$site <- partition$site_no

  if (service == 'dv') {
    nwis_pull_params$statCd <- c('00001', '00002', '00003')

    # do the data pull
    nwis_dat_time <- system.time({
      nwis_dat <- do.call(readNWISdv, nwis_pull_params)
    })
  }

  if (service == 'uv') {
    # do the data pull
    nwis_dat_time <- system.time({
      nwis_dat <- do.call(readNWISuv, nwis_pull_params)
    })
  }

  if (verbose){
    message(sprintf(
      'NWIS pull for %s took %0.0f seconds and returned %d rows',
      partition$PullTask[1],
      nwis_dat_time['elapsed'],
      nrow(nwis_dat)))
  }

  # make nwis_dat a tibble, converting either from data.frame (the usual case) or
  # NULL (if there are no results)
  nwis_dat <- as_data_frame(nwis_dat)

  # write the data to rds file. do this even if there were 0
  # results because remake expects this function to always create the target
  # file
  qs::qsave(nwis_dat, data_file)
}

choose_temp_column_dv <- function(temp_dat) {
  # take all temperature columns and put into long df
  date_col <- grep('date', names(temp_dat), ignore.case = TRUE, value = TRUE)

  # make long version, with mean, max, min columns of values and codes
  rev_dat <- temp_dat %>%
    dataRetrieval::renameNWISColumns() %>%
    mutate(Date = as.Date(.data[[date_col]])) %>%
    rename_if(grepl('Wtemp$', names(.)),
              list(~sprintf('%s_Mean', .))) %>%
    rename_if(grepl('Mean$|Min$|Max$', names(.)),
              list(~ sprintf('%s_temperature', .)))

  # handle some edge cases
  # one site had ` after the stat which led to new columns
  names(rev_dat) <- gsub("`", "", names(rev_dat))
  names(rev_dat) <- gsub("\\\\", "", names(rev_dat))

  dat_long <- pivot_longer(rev_dat,
                           cols = contains('Wtemp'),
                           names_to = c('location_info', '.value'),
                           names_pattern = '(.*Wtemp|.*Min|.*Max|.*Mean)_(.*)',
                           values_drop_na = TRUE) %>%
    mutate(cd = ifelse(is.na(Mean_temperature), ifelse(is.na(Max_temperature), Min_cd, Max_cd), cd)) %>%
    select(-Min_cd, -Max_cd, -agency_cd)

  # find which col_name has the most records for each site,
  # and keep that column

  # finds the number of records per site across the whole dataset
  # In instances where there are more than one site per date, the
  # the site with the most overall values is chosen.
  # fixed_dups <- dat_long %>%
  #   group_by(site_no, location_info) %>%
  #   mutate(count_nu = n()) %>%
  #   ungroup() %>%
  #   group_by(site_no, Date) %>%
  #   slice(which.max(count_nu)) %>%
  #   ungroup() %>%
  #   select(-agency_cd, -count_nu)

  if (!all(names(dat_long) %in%
           c("site_no", "Date", "location_info", "Mean_temperature",
             "Min_temperature", "Max_temperature", "cd"))) {
    message("!!Some weird column naming convention is out-smarting your pattern matching!!")
  }
  # fixed_dups <- filter(fixed_dups, !is.na(Mean_temperature)|!is.na(Min_temperature)|!is.na(Max_temperature))

  return(dat_long)
}

choose_temp_column_uv <- function(temp_dat) {
  # take all temperature columns and put into long df
  date_col <- grep('date', names(temp_dat), ignore.case = TRUE, value = TRUE)

  # make long version, with mean, max, min columns of values and codes
  rev_dat <- temp_dat %>%
    dataRetrieval::renameNWISColumns() %>%
    mutate(Date = as.Date(.data[[date_col]])) %>%
    rename_if(grepl('Inst$', names(.)),
              list(~sprintf('%s_temperature', .)))

  dat_long <- pivot_longer(rev_dat,
                           cols = contains('Wtemp'),
                           names_to = c('location_info', '.value'),
                           names_pattern = '(.*Wtemp)_Inst_(.*)',
                           values_drop_na = TRUE) %>%
    group_by(site_no, Date, location_info) %>%
    summarize(Mean_temperature = mean(temperature, na.rm = TRUE),
              Min_temperature = min(temperature, na.rm = TRUE),
              Max_temperature = max(temperature, na.rm = TRUE),
              n_obs = n(),
              cd = paste(unique(cd), collapse = '; '))

  # find which col_name has the most records for each site,
  # and keep that column

  # finds the number of records per site across the whole dataset
  # In instances where there are more than one site per date, the
  # the site with the most overall values is chosen.
  # fixed_dups <- dat_long %>%
  #   group_by(site_no, location_info) %>%
  #   mutate(count_nu = n()) %>%
  #   ungroup() %>%
  #   group_by(site_no, Date) %>%
  #   slice(which.max(count_nu)) %>%
  #   ungroup() %>%
  #   select(-count_nu)

  if (!all(names(dat_long) %in%
           c("site_no", "Date", "location_info", "Mean_temperature",
             "Min_temperature", "Max_temperature", "n_obs", "cd"))) {
    message("!!Some weird column naming convention is out-smarting your pattern matching!!")
  }
  #
  # fixed_dups <- filter(fixed_dups, !is.na(Mean_temperature)|!is.na(Min_temperature)|!is.na(Max_temperature))
  return(dat_long)
}
