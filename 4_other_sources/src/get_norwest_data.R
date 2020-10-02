# function to retreive all of the daily filenames for download
get_filenames <- function(url) {

  # get html from downloads website
  html <- paste(readLines(url), collapse = '\n')
  matched <- str_match_all(html,  "<a href=\"(.*?)\"")
  matched2 <- unlist(matched)

  # find the daily observations

  zips <- grep(pattern = '\\.zip', x = matched2, value = TRUE)
  dailies <- unique(grep('Daily', zips, value = TRUE))
  dailies <- dailies[!grepl('href', dailies)]
  dailies <- grep('AllDays', dailies, value = TRUE)

  # find the site points
  points <- unique(grep('^downloads.*ObservedTempPoints', zips, value = TRUE))

  # observation data files
  obs_files <- gsub('downloads/ObservedStreamTemperatureMaps/', '', dailies)
  obs_tasks <- gsub('/NorWeST.*', '', obs_files)
  obs_tasks <- gsub('\\d|_|-', '', obs_tasks)

  obs_files <- data.frame(data_files = obs_files, tasks = obs_tasks)

  # site data files
  site_files <- gsub('downloads/ObservedStreamTemperatureMaps/', '', points)
  site_tasks <- gsub('/NorWeST.*', '', site_files)
  site_tasks <- gsub('\\d|_|-', '', site_tasks)

  site_files <- data.frame(site_files = site_files, tasks = site_tasks)

  # create data frame for downloads
  file_downloads <- left_join(obs_files, site_files, by = 'tasks')

  return(file_downloads)
}

do_data_file_tasks <- function(files, base_url, out_file, ...) {
  task_name <- files$tasks
  task_file <- '4_norwest_datafile_tasks.yml'

  download_temp_data <- create_task_step(
    step_name = 'download_temp_data',
    target_name = function(task_name, ...) {
      sprintf('%s_temperature_data', task_name)
    },
    command = function(task_name, ...) {
      sprintf("fetch_data_files(base_url = download_base_url, files = dl_filenames, region = I('%s'))", task_name)
    }
  )

  download_site_data <- create_task_step(
    step_name = 'download_site_data',
    target_name = function(task_name, ...) {
      sprintf('%s_site_data', task_name)
    },
    command = function(task_name, ...) {
      sprintf("fetch_site_files(base_url = download_base_url, files = dl_filenames, region = I('%s'))", task_name)
    }
  )

  task_plan <- create_task_plan(
    task_names = task_name,
    task_steps = list(download_temp_data, download_site_data),
    add_complete = FALSE,
    final_steps = c('download_temp_data', 'download_site_data')
  )
  # Create the task remakefile
  create_task_makefile(
    task_plan = task_plan,
    makefile = task_file,
    include = 'remake.yml',
    sources = c(...),
    packages = c('dplyr', 'stringr', 'sf', 'readxl', 'readr'),
    tickquote_combinee_objects = TRUE,
    finalize_funs = c('combine_temp_data', 'combine_site_data'),
    final_targets = c('4_other_sources/out/norwest_raw_temp_data.feather.ind', '4_other_sources/out/norwest_raw_site_data.rds.ind'),
    as_promises = TRUE)

  loop_tasks(task_plan, task_file)
  combine_to_ind(out_file, c('4_other_sources/out/norwest_raw_temp_data.feather', '4_other_sources/out/norwest_raw_site_data.rds'))
  #scmake('raw_norwest_data.rds.ind_promise', remake_file='4_norwest_datafile_tasks.yml')
}
# download and unzip file, return rds
fetch_data_files <- function(base_url, files, region, tmp_loc = '4_other_sources/tmp') {
  # create url for download
  file_name <- files$data_files[files$tasks %in% region]
  temp_url <- file.path(base_url, file_name)
  order <- which(files$tasks %in% region)
  # create a temporary space to download zip
  ext_directory <- file.path(tmp_loc, region)

  if (!file.exists(ext_directory)) {
    message(sprintf("    %d of %d: Downloading Norwest temperature file %s", order, nrow(files), file_name))
    download.file(temp_url, ext_directory)
  }

  # unzip to tmp file location
  unzip(ext_directory, exdir = tmp_loc)


  # read in data
  outfile <- gsub('.zip', '', gsub('.*/', '', file_name))
  outfile <- list.files(tmp_loc)[grepl(outfile, list.files(tmp_loc))]

  if (grepl('xlsx', outfile)) {
    dat <- read_xlsx(file.path(tmp_loc, outfile))
  } else if (grepl('txt', outfile)) {
    dat <- readr::read_delim(file.path(tmp_loc, outfile), delim = ',') %>%
      mutate(SampleDate = as.Date(SampleDate, format = '%m/%d/%Y'))

  }
  dat <- mutate(dat, region = region)
  return(dat)
}

fetch_site_files <- function(base_url, files, region, tmp_loc = '4_other_sources/tmp') {
  # create url for download
  file_name <- files$site_files[files$tasks %in% region]
  temp_url <- file.path(base_url, file_name)
  order <- which(files$tasks %in% region)
  # create a temporary space to download zip
  ext_directory <- file.path(tmp_loc, paste0(region, '_sites'))

  if (!file.exists(ext_directory)) {
    message(sprintf("    %d of %d: Downloading Norwest site file %s", order, nrow(files), file_name))
    download.file(temp_url, ext_directory)
  }
  # unzip to tmp file location
  unzip(ext_directory, exdir = tmp_loc)

  # read in data
  outfile <- gsub('.zip', '', gsub('.*/', '', file_name))

  # annoying name change for region NorthernCaliforniaCoastalKlamath
  if (region == "NorthernCaliforniaCoastalKlamath") {
    outfile <- 'NorWeST_ObservedTempPoints_NorthCalifCoastalKlamath'
  }
  # read in shape file
  geo_dat <- sf::st_read(file.path(tmp_loc, paste0(outfile, '.shp'))) %>%
    mutate(region = region)

  return(geo_dat)
}

namedList <- function(...) {
  L <- list(...)
  snm <- sapply(substitute(list(...)),deparse)[-1]
  if (is.null(nm <- names(L))) nm <- snm
  if (any(nonames <- nm=="")) nm[nonames] <- snm[nonames]
  setNames(L,nm)
}
combine_temp_data <- function(ind_file, ...) {

  data_list <- namedList(...)
  subset_data_list <- data_list[grepl('temperature', names(data_list))]

  dat_out <- bind_rows(subset_data_list) %>%
    distinct()
  # write and indicate the data file
  data_file <- scipiper::as_data_file(ind_file)
  feather::write_feather(dat_out, data_file)
  gd_put(ind_file)

}

combine_site_data <- function(ind_file, ...) {
  data_list <- namedList(...)
  subset_data_list <- data_list[which(grepl('site', names(data_list)))]
  dat_out <- bind_rows(subset_data_list)

  # write and indicate the data file
  data_file <- scipiper::as_data_file(ind_file)
  saveRDS(dat_out, data_file)
  gd_put(ind_file)

}

