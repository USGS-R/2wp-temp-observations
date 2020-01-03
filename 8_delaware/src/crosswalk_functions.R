
crosswalk_sites_to_reaches <- function(network_ind, boundary_ind, sites_ind, out_ind) {
  
  # read in network and boundary files
  drb_net <- readRDS(sc_retrieve(network_ind))
  drb_boundary <- sf::read_sf(sc_retrieve(boundary_ind))
  
  # read in sites from WQP, nwisdv, nwisuv
  sites <- readRDS(sc_retrieve(sites_ind)) %>%
    filter(site_type %in% c('ST', 'Stream'))
  
  # Convert to sfc
  obs_site_points <- purrr::map2(sites$longitude, sites$latitude, function(lat, lon) {
    st_point(c(lat, lon), dim='XY')})
  
  obs_sites <- sites %>%
    st_set_geometry(st_sfc(obs_site_points)) %>%
    st_set_crs(4326) %>%
    st_transform(crs=st_crs(drb_net$vertices))
  
  # Subset to sites in the Delaware River Basin
  drb_sites <- obs_sites[st_intersects(drb_boundary, obs_sites)[[1]], ] # 5009 rows
  
  #### Match sites to reaches ####
  
  source('src/subset_closest.R')
  system.time({ # 97 seconds
    crosswalk <- subset_closest(sites=drb_sites, reaches=drb_net$edges, vertices=drb_net$vertices)
    # warns: site USGS-01433005 has diverse coordinates across databases, with bbox diagonal = 6.261 m
    # (the algorithm will have matched to the first version of that site, probably the wqp USGS-NY one)
  })
  
  # add geospatial info (drb_sites) and seg_id_nat (drb_net$edges)
  crosswalk_site_reach <- left_join(drb_sites, crosswalk, by='site_id') %>%
    left_join(st_drop_geometry(select(drb_net$edges, subseg_id, seg_id_nat)), by='subseg_id')
  
  # write file and push to GD
  saveRDS(crosswalk_site_reach, as_data_file(out_ind))
  gd_put(as_data_file(out_ind), out_ind)
}
