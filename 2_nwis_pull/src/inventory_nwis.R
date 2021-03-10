inventory_nwis <- function(inv_ind, nwis_pull_params, service) {
  
  hucs <- stringr::str_pad(1:21, width = 2, pad = "0")
  nwis_pull_params$service <- service
  nwis_pull_params$statCd <- '00003'
  
  # what NWIS data does not accept start/end dates
  nwis_pull_params$startDate <- NULL
  nwis_pull_params$endDate <- NULL
  
  all_dat <- data.frame()
  for(huc in hucs){
    nwis_pull_params$huc = huc
    sites <- do.call(whatNWISdata, nwis_pull_params)
    
    all_dat <- rbind(all_dat, sites)
    
  }
  data_file <- scipiper::as_data_file(inv_ind)
  feather::write_feather(all_dat, data_file)
  gd_put(inv_ind)
  
}

summarize_nwis_inventory <- function(inv_ind, out_file) {
  
  nwis_inventory <- feather::read_feather(sc_retrieve(inv_ind, remake_file = 'getters.yml'))
  
  all <- data.frame(n_sites = nrow(nwis_inventory), 
                    n_records = sum(nwis_inventory$count_nu), 
                    earliest = min(nwis_inventory$begin_date),
                    latest = max(nwis_inventory$end_date), stringsAsFactors = FALSE)
  
  
  readr::write_csv(all, out_file)
  
}

summarize_nwis_data <- function(data_ind, out_file) {
  nwis_data <- readRDS(sc_retrieve(data_ind, remake_file = 'getters.yml'))
  
  summary <- data.frame(n_obs = nrow(nwis_data),
                        n_sites = length(unique(nwis_data$site_no)))
  
  readr::write_csv(summary, out_file)
  
}