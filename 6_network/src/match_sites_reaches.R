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
  message('matching flowlines with reaches...')
  flowline_indices <- nhdplusTools::get_flowline_index(flines = reaches_nhd_fields,
                                                       points = sites_sf,
                                                       max_matches = 1,
                                                       search_radius = search_radius)
  sites_sf_index <- tibble(Shape_site = sites_sf,
                           index = 1:length(sites_sf))

  message("rejoining with other geometries, adjusting matches for upstream proximity...")
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
  message("uploading...")
  gd_put(outind)
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

sample_reaches <- function(matched_sites_ind) {
  #TODO: add stream order from NHD
  matched_sites <- readRDS(sc_retrieve(matched_sites_ind, remake_file = "6_network.yml")) %>% 
    ungroup()
  random_site_reaches_seg_ids <- matched_sites %>% select(seg_id) %>% 
    distinct() %>% slice_sample(n = 100) %>% pull(seg_id)
  random_site_reaches <- matched_sites %>% filter(seg_id %in% random_site_reaches_seg_ids)
  longest_reaches <- matched_sites %>% slice_max(order_by = shape_length, n = 20)
  shortest_reaches <- matched_sites %>% slice_min(order_by = shape_length, n = 20)
  most_sites_reaches_seg_ids <- matched_sites %>% select(seg_id, id) %>% 
    group_by(seg_id) %>% 
    summarize(n_sites = n()) %>% 
    slice_max(order_by = n_sites, n = 20) %>% 
    pull(seg_id)
  most_sites_reaches <- matched_sites %>% filter(seg_id %in% most_sites_reaches_seg_ids)
  reaches_list <- list(random_site_reaches = random_site_reaches,
                       longest_reaches = longest_reaches,
                       shortest_reaches = shortest_reaches,
                       most_sites_reaches = most_sites_reaches)
  return(reaches_list)
}

plot_reach_and_matched_sites <- function(outfile, reach_and_sites, network_latlon,
                                         category = NULL) {
  #list on map: site id, reach id, distance, number of sites matched to reach, 
  #plot network, with reach highlighted, and matched site
  #use satellite basemap
  #first get a bbox to use
  assert_that(length(unique(reach_and_sites$Shape)) == 1)
  reach_bbox <- reach_and_sites$Shape %>% 
    st_transform(crs = 4326) %>% 
    st_bbox() + c(-0.1, -0.05, 0.1, 0.05)
  reach_latlon <- reach_and_sites$Shape[1] %>% 
    st_transform(4326)
  sites_latlon <- reach_and_sites %>%
    st_set_geometry(value = 'Shape_site') %>% 
    st_transform(4326) %>% 
    mutate(id = as.character(id))
  downstream_point <- reach_and_sites$down_point[1] %>% 
    st_transform(4326)
  reaches_clipped_latlon <- network_latlon %>% 
    st_crop(reach_bbox)
  names(reach_bbox) <- c('left', 'bottom', 'right', 'top')
  base_map <- get_map(location = reach_bbox)
  map_title <- sprintf("%s seg_id: %s; %s sites matched", category,
                       reach_and_sites$seg_id[1], nrow(reach_and_sites))
  browser()
  final_map <- ggmap(base_map) +
    geom_sf(data = reaches_clipped_latlon, inherit.aes = FALSE) +
    geom_sf(data=reach_latlon, inherit.aes = FALSE,
            color = "red") + 
    geom_sf(data = downstream_point, shape = 15, inherit.aes = FALSE) +
    geom_sf(data = sites_latlon, inherit.aes = FALSE,
            mapping = aes(color = id), shape = 1, size = 3) +
    ggtitle(map_title, subtitle = sprintf("Matched reach length: %d m\nBlack square = reach outlet", 
                                          round(reach_and_sites$shape_length)))
  ggsave(filename = outfile, plot = final_map)  
}

transform_network_file <- function(network_ind, crs) {
  network <- readRDS(sc_retrieve(network_ind))
  network$Shape <- st_transform(network$Shape, crs)
}