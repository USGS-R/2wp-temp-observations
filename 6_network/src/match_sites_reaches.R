get_site_flowlines <- function(outind, reaches_direction_ind, sites, search_radius) {

  reaches_direction <- readRDS(sc_retrieve(reaches_direction_ind, 
                                   remake_file = "6_network.yml"))  
  #set up NHDPlus fields used by get_flowline_index
  reaches_nhd_fields <- reaches_direction %>% 
    select(COMID = seg_id, Shape) %>% 
    mutate(REACHCODE = COMID, ToMeas = 100, FromMeas = 100) %>% 
    st_as_sf()

  sites <- readRDS(sites) #replace when connecting to rest of pipeline
  sites_sf <- sites %>% rowwise() %>%  
    filter(across(c(longitude, latitude), ~ !is.na(.x))) %>% 
    mutate(Shape = list(st_point(c(longitude, latitude), dim = "XY"))) %>% 
    st_as_sf() %>% st_set_crs(4326) %>% 
    st_transform(st_crs(reaches_nhd_fields)) %>% 
    st_geometry()
  gc()
  row_midpoint <- round(nrow(sites_sf)/2)
  
  # sites_list <- list(chunk1 = sites_sf[1:row_midpoint], 
  #                    chunk2 = sites_sf[(row_midpoint+1):length(sites_sf)])
  # #split up into 2 chunks
  # flowline_indices_list <- lapply(X = sites_list, 
  #                                 FUN = nhdplusTools::get_flowline_index,
  #                                 flines = reaches_nhd_fields,
  #                                 points = X,
  #                                 max_matches = 1, 
  #                                 search_radius = search_radius)
  flowline_indices <- nhdplusTools::get_flowline_index(flines = reaches_nhd_fields,
                                                       points = sites_sf,
                                                       max_matches = 1,
                                                       search_radius = search_radius)
  sites_sf_index <- tibble(Shape_site = sites_sf,
                           index = 1:length(sites_sf))
  #TODO: move to earlier steps, this should be set up for use elsewhere
  sites_crs <- st_crs(sites_sf)
  reaches_direction <- reaches_direction %>% 
    st_sf(crs = sites_crs, sf_column_name = 'Shape') %>% 
    mutate(up_point = st_sfc(up_point, crs = sites_crs),
           down_point = st_sfc(down_point, crs = sites_crs))
           #end_points = st_sfc(end_points, crs = sites_crs)) 

  
  #rejoin to original reaches df, get up/downstream distance
  flowline_indices_joined <- flowline_indices %>% 
    select(seg_id = COMID, -REACHCODE, -REACH_meas, everything()) %>% 
    left_join(reaches_direction, by = c("seg_id")) %>% 
    left_join(sites_sf_index, by = c(id = "index")) %>% 
    mutate(site_upstream_distance = st_distance(x = Shape_site, y = up_point,
                                                by_element = TRUE),
           site_downstream_distance = st_distance(x = Shape_site, y = down_point,
                                                  by_element = TRUE),
           down_up_ratio = as.numeric(site_downstream_distance / site_upstream_distance))
  sites_move_upstream <- flowline_indices_joined %>% 
    rowwise() %>% 
    mutate(seg_id_reassign = if_else(down_up_ratio > 1,
                                     true = check_upstream_reach(matched_seg_id = seg_id,
                                                                 down_up_ratio = down_up_ratio,
                                                                 reaches_direction = reaches_direction),
                                     false = list(seg_id),
                                     missing = list(NA_integer_)))
  saveRDS(sites_move_upstream, file = as_data_file(outind))
  gd_put(out_ind)
}


#' get upstream reach: if multiple, return both if down_up_ratio > 4;
#' else if down_up_ratio < 4, return same seg_id
#' if only one upstream reach, return it
#' 
#' Expects only reaches where up_down_ratio > 1
check_upstream_reach <- function(matched_seg_id, down_up_ratio, reaches_direction) {
  #if_else will replace this value
  if(down_up_ratio <= 1 || is.na(down_up_ratio)) {
    return(list(NA))
  }
  
  upstream_reaches <- filter(reaches_direction, to_seg == matched_seg_id)
  
  if(nrow(upstream_reaches) > 1) { #upstream point is a confluence
    if(down_up_ratio > 4) {
      return(list(upstream_reaches$seg_id))
    } else {
      return(list(matched_seg_id))
    }
  } else if(nrow(upstream_reaches) == 1){
    return(list(upstream_reaches$seg_id))
  } else { #a source reach, nothing upstream
    return(list(matched_seg_id))
  }
}
