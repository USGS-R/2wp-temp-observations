get_site_flowlines <- function(outind, reaches_direction_ind, sites_ind, search_radius) {

  reaches_direction <- readRDS(sc_retrieve(reaches_direction_ind,
                                   remake_file = "getters.yml"))
  sites <- readRDS(sc_retrieve(sites_ind,
                                   remake_file = "getters.yml"))
  #set up NHDPlus fields used by get_flowline_index
  reaches_nhd_fields <- reaches_direction %>%
    select(COMID = seg_id, Shape) %>%
    mutate(REACHCODE = COMID, ToMeas = 100, FromMeas = 100) %>%
    st_as_sf()

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
                                     missing = list(seg_id))) %>%
    rename(seg_id_orig_match = seg_id) %>%
    unnest_longer(seg_id_reassign) %>% #handle when site matched to two reaches at intersection
    select(-to_seg, -seg_id_nhm, -Version, -shape_length, -Shape, -end_points,
           -which_end_up, -up_point, -down_point) %>%
    #drop columns and rejoin to get correct geometries for sites that were
    #moved upstream
    left_join(reaches_direction, by = c(seg_id_reassign = 'seg_id'))

  #make sure each site is matched to only one reach
  assert_that(class(sites_move_upstream$seg_id_reassign) == 'integer')
  assert_that(length(unique(sites_move_upstream$id)) == nrow(sites_move_upstream),
              msg = 'There are repeated site IDs in the site-reach matches')
  assert_that(sum(is.na(sites_move_upstream$seg_id_reassign)) == 0,
              msg = 'There are NAs in the matched reach column')
  saveRDS(sites_move_upstream, file = as_data_file(outind))
  message("uploading...")
  gd_put(outind)
}


#' get upstream reach: if multiple, return original match;
#' if only one upstream reach, return it
#'
#' Expects only reaches where up_down_ratio > 1
check_upstream_reach <- function(matched_seg_id, down_up_ratio, reaches_direction) {
  #if_else will replace this value
  if(down_up_ratio <= 1 || is.na(down_up_ratio)) {
    return(list(NA))
  }

  upstream_reaches <- filter(reaches_direction, to_seg == matched_seg_id)

  if(nrow(upstream_reaches) > 1 | nrow(upstream_reaches) == 0) { #upstream point is a confluence or source
      return(list(matched_seg_id))
  } else if(nrow(upstream_reaches) == 1){
    return(list(upstream_reaches$seg_id))
  }
}

sample_reaches <- function(matched_sites_ind, full_network) {

  matched_sites <- readRDS(sc_retrieve(matched_sites_ind, remake_file = "getters.yml"))

  random_site_reaches_seg_ids <- matched_sites %>% select(seg_id_reassign) %>%
    distinct() %>% slice_sample(n = 100) %>% pull(seg_id_reassign)
  random_site_reaches <- matched_sites %>% filter(seg_id_reassign %in% random_site_reaches_seg_ids)
  longest_reaches <- matched_sites %>% slice_max(order_by = shape_length, n = 20)
  shortest_reaches <- matched_sites %>% slice_min(order_by = shape_length, n = 20)
  most_sites_reaches_seg_ids <- matched_sites %>% select(seg_id_reassign, id) %>%
    group_by(seg_id_reassign) %>%
    summarize(n_sites = n()) %>%
    slice_max(order_by = n_sites, n = 20) %>%
    pull(seg_id_reassign)
  most_sites_reaches <- matched_sites %>% filter(seg_id_reassign %in% most_sites_reaches_seg_ids)

  order_one_reach_ids <- get_first_order_reaches(matched_sites, full_network = full_network) %>%
    sample(size = 20)
  order_one_reaches <- matched_sites %>% filter(seg_id_reassign %in% order_one_reach_ids)

  reaches_list <- list(random_site_reaches = random_site_reaches,
                       longest_reaches = longest_reaches,
                       shortest_reaches = shortest_reaches,
                       most_sites_reaches = most_sites_reaches,
                       order_one_reaches = order_one_reaches)
  return(reaches_list)
}

transform_network_file <- function(network_ind, crs) {
  network <- readRDS(sc_retrieve(network_ind, remake_file = "getters.yml"))
  network$Shape <- st_transform(network$Shape, crs)
  return(network)
}

#' @param reaches has seg_id column, of segments with matched reaches
#' @param full_network has seg_id and to_seg columns of full network
#' The geofabric doesn't have a stream order field, but we can find the first order
#' reaches just by finding which reaches are missing from the to_seg column
get_first_order_reaches <- function(reaches, full_network) {
  network_first_order_reaches <- full_network %>%
    filter(!seg_id %in% to_seg) %>%
    pull(seg_id)
  reaches %>% filter(seg_id_reassign %in% network_first_order_reaches) %>%
    distinct() %>%
    pull(seg_id_reassign)
}

create_crosswalk <- function(out_ind, matched_ind, sites_ind) {
  matched <- readRDS(sc_retrieve(matched_ind, 'getters.yml'))
  sites <- readRDS(sc_retrieve(sites_ind, 'getters.yml')) %>%
    mutate(id = 1:nrow(.))

  names(sites)

  crosswalk <- left_join(sites,
                         select(matched, seg_id_orig_match, id, seg_id_reassign, site_upstream_distance, site_downstream_distance, offset), by = 'id') %>%
    st_drop_geometry() %>%
    # clean up site types
    mutate(site_type = case_when(
      site_type %in% c('ST', 'stream', 'Stream') ~ 'ST',
      site_type %in% c('SP', 'Spring') ~ 'SP',
      TRUE ~ site_type))

  saveRDS(crosswalk, as_data_file(out_ind))
  gd_put(out_ind)

}
