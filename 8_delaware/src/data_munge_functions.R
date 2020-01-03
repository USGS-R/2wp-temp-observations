subset_data <- function(out_ind, crosswalk_ind, dat_ind) {
  
  crosswalk <- readRDS(sc_retrieve(crosswalk_ind))
  
  basin_dat <- readRDS(sc_retrieve(dat_ind)) %>%
    filter(site_id %in% crosswalk$site_id)
  
  saveRDS(as_data_file(out_ind))
  gd_pput(out_ind, as_data_file(out_ind))
    
}

generate_site_summary <- function(dat_ind, out_ind) {
  
}

crosswalk_site_reach <- readRDS('out/crosswalk_site_reach.rds')

delaware_pts <- crosswalk_site_reach %>%
  dplyr::mutate(
    dist_to_reach_km = bird_dist_to_subseg_m/1000,
    nobsBin = base::cut(n_obs, breaks=c(min(n_obs), 10, 100, 1000, max(n_obs)+1), right=FALSE)) %>%
  dplyr::select(
    site_id,
    dist_to_reach_km,
    matched_reach_id = seg_id_nat,
    n_obs,
    nobsBin) %>%
  sf::st_transform(crs = 4326)

delaware_geojson <- geojsonsf::sf_geojson(delaware_pts)
geojsonio::geojson_write(delaware_geojson, file = 'out/delaware_sites_summary.geojson', convert_wgs84 = TRUE)
