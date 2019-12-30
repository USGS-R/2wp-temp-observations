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

summarize_inventory <- function(inv_ind, out_file) {
  
  nwis_inventory <- feather::read_feather(sc_retrieve(inv_ind))
  
  all <- data.frame(StateName = 'All', 
                    n_sites = nrow(nwis_inventory), 
                    n_records = sum(nwis_inventory$count_nu), 
                    earliest = min(nwis_inventory$begin_date),
                    latest = max(nwis_inventory$end_date), stringsAsFactors = FALSE)
  
  
  write.csv(all, out_file, row.names = FALSE)
  
}