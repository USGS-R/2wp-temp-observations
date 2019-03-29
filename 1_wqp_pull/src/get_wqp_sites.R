
get_wqp_sites <- function(site_ind, wqp_params) {
  
  # collapse all parameter names into one vector
  all_vars <- unlist(wqp_params$characteristicName)
  
  all_sites <- whatWQPsites(siteType = wqp_params$Sitetype, 
                           sampleMedia = wqp_params$sampleMedia, 
                           characteristicName = all_vars)

  feather::write_feather(all_sites, as_data_file(site_ind))
  gd_put(site_ind)
  
}

subset_sites <- function(in_ind, out_ind) {
  sites <- feather::read_feather(sc_retrieve(in_ind)) %>%
    slice(1:50)
  
  feather::write_feather(sites, as_data_file(out_ind))
  gd_put(out_ind)
  
}