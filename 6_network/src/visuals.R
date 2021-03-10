plot_reach_and_matched_sites <- function(outfile, reach_and_sites, network_latlon,
                                         category = NULL) {
  #list on map: site id, reach id, distance, number of sites matched to reach,
  #plot network, with reach highlighted, and matched site
  #use satellite basemap
  #first get a bbox to use
  assert_that(length(unique(reach_and_sites$Shape)) == 1)
  reach_bbox <- reach_and_sites$Shape %>%
    st_transform(crs = 4326) %>%
    st_bbox() + c(-0.2, -0.1, 0.2, 0.1)
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

  #message("If you don't have a registered Google API key, this function will fail. See ?register_google for more information")
  base_map <- get_map(location = reach_bbox)
  map_title <- sprintf("%s seg_id: %s; %s sites matched", category,
                       reach_and_sites$seg_id_reassign[1], nrow(reach_and_sites))
  use_site_legend <- ifelse(nrow(sites_latlon) > 20, FALSE, TRUE)

  offset <- (reach_bbox[['top']]-reach_bbox[['bottom']])/20
  # some troubleshooting
  offset <- ifelse(reach_bbox[['top']]-reach_bbox[['bottom']] < 0.3, 0.09, offset)
  scale_dist <- ceiling(unique(round(reach_and_sites$shape_length))/20000)

  final_map1 <- ggmap(base_map) +
    geom_sf(data = reaches_clipped_latlon, inherit.aes = FALSE) +
    geom_sf(data=reach_latlon, inherit.aes = FALSE,
            color = "red") +
    geom_sf(data = downstream_point, shape = 15, inherit.aes = FALSE) +
    geom_sf(data = sites_latlon, inherit.aes = FALSE,
            mapping = aes(color = id), shape = 1, size = 3,
            show.legend = use_site_legend) +
    labs(title = map_title,
         subtitle = sprintf("Matched reach length: %d m\nBlack square = reach outlet",
                            round(reach_and_sites$shape_length)),
         caption = paste('Rendered', Sys.time())) +
    north(data = reaches_clipped_latlon)

  final_map <-
  tryCatch(
    {
      final_map1 +
      scalebar(data = reaches_clipped_latlon, location = "bottomright", dist = scale_dist,
               dist_unit = "km", transform = TRUE,  model = 'WGS84',
               anchor = c(y = reach_bbox[['bottom']] + offset, x = reach_bbox[['right']] - offset))
      },
    error = function(e) final_map1)


  ggsave(filename = outfile, plot = final_map, height = 10, width = 10)
}

site_reach_distance_plot <- function(outind, sites_reaches_ind) {
  reaches_matched <- readRDS(sc_retrieve(sites_reaches_ind, 'getters.yml'))
  ggplot(data = reaches_matched, aes(x = offset)) +
    geom_histogram(binwidth = 10) +
    labs(title = "Distribution of site offsets from initially matched reach*",
         subtitle = "Bin width = 10",
         caption = "*Prior to sites being matched to upstream reach based on up/down distance",
         x = 'Offset (m)')
  ggsave(filename = as_data_file(outind))
  gd_put(outind)
}

sites_per_reach_plot <- function(outind, sites_reaches_ind) {
  reaches_matched <- readRDS(sc_retrieve(sites_reaches_ind, 'getters.yml'))

  sites_per_reach <- reaches_matched %>%
    group_by(seg_id_reassign) %>%
    summarize(n = n())
  ggplot(sites_per_reach, aes(y = n)) + geom_boxplot() +
    scale_y_log10()+ theme(axis.text.x = element_blank(),
                           axis.ticks.x = element_blank()) +
    labs(title = "Sites matched per reach",
         y = "Number of sites")
  ggsave(filename = as_data_file(outind))
  gd_put(outind)
}

distance_vs_reach_length_plot <- function(outind, sites_reaches_ind) {
  reaches_matched <- readRDS(sc_retrieve(sites_reaches_ind, 'getters.yml'))
  sites_distances_long <- reaches_matched %>%
    select(shape_length, site_upstream_distance, site_downstream_distance) %>%
    pivot_longer(cols = contains('distance'), names_to = 'measure',
                 names_prefix = 'site_', values_to = 'distance')
  ggplot(sites_distances_long, aes(x = shape_length/1000, y = as.numeric(distance)/1000)) +
    geom_hex() + facet_wrap('measure') +
    labs(x = "Reach length (km)", y = "Distance to reach endpoint (km)") + coord_fixed()
  ggsave(filename = as_data_file(outind))
  gd_put(outind)
}
