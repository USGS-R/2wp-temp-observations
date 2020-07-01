unzip_merge_values <- function(zip_ind, out_ind) {
  extract_dir <- file.path(tempdir(),
                           tools::file_path_sans_ext(basename(as_data_file(zip_ind))))
  unzip(sc_retrieve(zip_ind, 'getters.yml'), exdir = extract_dir)
  
  temp_files <- list.files(extract_dir)
  
  # read in values
  dat <- readr::read_csv(file.path(extract_dir, 'values.csv'), col_types = 'dDddddlc')
  
  # read in series data
  series <- readr::read_csv(file.path(extract_dir, 'series.csv'))
  
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
  

  # read in location metadata
  locations <- readr::read_csv(file.path(extract_dir, 'locations.csv'))
  
  # read in agency metadata
  agencies <- readr::read_csv(file.path(extract_dir, 'agencies.csv'))
  
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