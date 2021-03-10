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
  partitions <- feather::read_feather(scipiper::sc_retrieve(partitions_ind, 'getters.yml'))

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
    sources = c('1_wqp_pull/src/get_wqp_data.R'),
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




