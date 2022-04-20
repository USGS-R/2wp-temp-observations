unzip_merge_values <- function(zip_ind, out_ind) {
  extract_dir <- file.path(tempdir(),
                           tools::file_path_sans_ext(basename(as_data_file(zip_ind))))
  unzip(sc_retrieve(zip_ind, 'getters.yml'), exdir = extract_dir)

  temp_files <- list.files(extract_dir, full.names = TRUE)

  # read in values
  dat <- readr::read_csv(grep('values.csv', temp_files, value = TRUE), col_types = 'dDddddlc')

  # read in series data
  series <- readr::read_csv(grep('series.csv', temp_files, value = TRUE))

  # merge dat and series to get location metadata
  dat_out <- left_join(dat, series)

  saveRDS(dat_out, file = as_data_file(out_ind))
  unlink(extract_dir, recursive = TRUE)
  gd_put(out_ind)
}

unzip_extract_sites <- function(zip_ind, out_ind) {
  extract_dir <- file.path(tempdir(),
                           tools::file_path_sans_ext(basename(as_data_file(zip_ind))))
  unzip(sc_retrieve(zip_ind, 'getters.yml'), exdir = extract_dir)
  temp_files <- list.files(extract_dir, full.names = TRUE)

  # read in location metadata
  locations <- readr::read_csv(grep('locations.csv', temp_files, value = TRUE))

  # read in agency metadata
  agencies <- readr::read_csv(grep('agencies.csv', temp_files, value = TRUE))

  # merge to create site-specific metadata
  meta <- left_join(locations, agencies)

  saveRDS(meta, file = as_data_file(out_ind))
  unlink(extract_dir, recursive = TRUE)
  gd_put(out_ind)
}

summarize_ecosheds_data <- function(data_ind, out_file) {
  ecosheds_data <- readRDS(sc_retrieve(data_ind, remake_file = 'getters.yml'))

  summary <- data.frame(n_obs = nrow(ecosheds_data),
                        n_sites = length(unique(ecosheds_data$location_id)))

  readr::write_csv(summary, out_file)

}
