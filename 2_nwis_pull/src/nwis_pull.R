#### pull ####

# prepare a plan for downloading (from NWIS) and posting (to GD) one data file
# per state
plan_nwis_pull <- function(partitions_ind, service) {

  folders <- list(
    tmp='2_nwis_pull/tmp',
    out='2_nwis_pull/out',
    log='2_nwis_pull/log')
  
  partitions <- feather::read_feather(scipiper::sc_retrieve(partitions_ind))

  # after all wanted data have been pulled, this function will be called but
  # doesn't need to create anything much, so just return NULL
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

  # download from NWIS, save, and create .ind file promising the download is
  # complete, with extension .tind ('temporary indicator') because we don't want
  # scipiper to make a build/status file for it

  download <- scipiper::create_task_step(
    step_name = 'download',
    target_name = function(task_name, step_name, ...) {
      file.path(folders$tmp, sprintf('%s.rds', task_name))
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
    packages=c('dplyr', 'dataRetrieval', 'feather', 'scipiper', 'yaml', 'stringr'),
    file_extensions=c('ind','feather'), finalize_funs = 'combine_nwis_data', final_targets = final_targets)
}

# extract and post the data (dropping the site info attribute), creating an
# .ind file that we will want to share because it represents the shared cache
combine_nwis_data <- function(ind_file, ...){

  rds_files <- c(...)
  df_list <- list()

  for (i in seq_len(length(rds_files))){
    df_list[[i]] <- readRDS(rds_files[i])
  }

  nwis_df <- do.call("rbind", df_list)

  data_file <- scipiper::as_data_file(ind_file)
  saveRDS(nwis_df, data_file)
  gd_put(ind_file, data_file)
}

# --- functions used in task table ---

# read the partitions file and pull out just those sites that go with a single
# PullTask 
filter_partitions <- function(partitions_ind, pull_task) {
  
  partitions <- feather::read_feather(sc_retrieve(partitions_ind))

  these_partitions <- dplyr::filter(partitions, PullTask==pull_task, count_nu > 0)
  
  return(these_partitions)  
}

# pull a batch of NWIS observations, save locally, return .tind file
get_nwis_data <- function(data_file, partition, nwis_pull_params, service, verbose = TRUE) {

  nwis_pull_params$service <- service
  nwis_pull_params$site <- partition$site_no
  nwis_pull_params$endDate <- Sys.Date()
  
  if (service == 'dv') { nwis_pull_params$statCd <- '00003' }
  
  # do the data pull
  nwis_dat_time <- system.time({
    nwis_dat <- do.call(readNWISdata, nwis_pull_params)
  })
  
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
  saveRDS(nwis_dat, data_file)
}
