
get_wqp_sites <- function(wqp_params) {
  
  # collapse all parameter names into one vector
  all_vars <- unlist(wqp_params$characteristicName)
  
  all_sites <- whatWQPsites(siteType = wqp_params$Sitetype, 
                           samplemedia = wqp_params$sampleMedia, 
                           characteristicname = all_vars)

  return(all_sites)
}