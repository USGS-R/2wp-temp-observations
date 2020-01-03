library(tidyverse)
library(sf)

#' Match each site point to an edge (reach), preferring edges for which the
#' downstream vertex (endpoint) is close to the site point
#'
#' Algorithm: Locate the nearest reach. If the nearest point is downstream, then
#' return a match to that nearest reach. If the nearest reach endpoint (vertex)
#' is upstream, and either (1) there is no branching at the upstream vertex or
#' (2) the upstream vertex is >4x closer than the downstream vertex, then return
#' a match to the nearest upstream reach. Still report bird_dist_to_subseg_m as
#' the distance to the initially identified nearest reach.
#'
#' @param reaches sf Simple Feature Collection with reach identifiers in
#'   seg_id_nat column
#' @param sites inventory data.frame with MonitoringLocationIdentifier,
#'   latitude, and longitude at a minimum
#' @return data.frame of site info: MonitoringLocationIdentifier, seg_id_nat,
#'   lat, lon, and distance from site lat/lon to centroid of the reach segment
#' @examples
#' reaches <- readRDS('out/network_full.rds')$edges
#' vertices <- readRDS('out/network_full.rds')$vertices
#' sites <- drb_sites # build drb_sites in map_sites_to_reaches.R
subset_closest <- function(sites, reaches, vertices) {
  
  # see https://gis.stackexchange.com/questions/288570/find-nearest-point-along-polyline-using-sf-package-in-r
  # for the guidance I used for implementation
  system.time({ # 1.7 seconds
    # Locate the nearest endpoint=vertex and the nearest reach (by nearest point within that reach)
    nearest_subseg <- reaches[sf::st_nearest_feature(sites, reaches), ]
    bird_dist_to_subseg_m <- st_length(st_nearest_points(sites, nearest_subseg, pairwise=TRUE))
    nearest_vertex <- vertices[sf::st_nearest_feature(sites, vertices), ]
    bird_dist_to_vertex_m <- st_length(st_nearest_points(sites, nearest_vertex, pairwise=TRUE))
    nearest <- tibble(
      site_id = sites$site_id,
      nearest_subseg = nearest_subseg, # creates a cool horizontally nested tibble structure with multiple geometries
      # nearest_subseg_id = nearest_subseg$subseg_id,
      bird_dist_to_subseg_m = bird_dist_to_subseg_m %>% units::drop_units(),
      nearest_vertex = nearest_vertex, # creates a cool horizontally nested tibble structure with multiple geometries
      # nearest_point_ids = nearest_vertex$point_ids,
      bird_dist_to_vertex_m = bird_dist_to_vertex_m %>% units::drop_units()
    ) %>%
      st_set_geometry(st_geometry(sites))
  })
  
  # For each site, use nested conditionals to navigate a set of possibilities
  # for what the best site-reach match is. See notes below
  system.time({ # 106 seconds with message
    crosswalk <- nearest %>%
      split(.$site_id) %>% # generally rowwise, but sometimes both nwis and wqp have the same site_id
      purrr:::map_dfr(function(site_sf) {
        # Deal with duplicated site_id
        if(nrow(site_sf) > 1) {
          bbox_diagonal <- st_length(st_sfc(st_linestring(matrix(st_bbox(site_sf), ncol=2, byrow=TRUE)), crs=st_crs(site_sf)))
          if(bbox_diagonal > units::set_units(1, m)) {
            warning(sprintf('site %s has diverse coordinates across databases, with bbox diagonal = %0.03f m', site_sf$site_id[1], bbox_diagonal))
          }
          site_sf <- site_sf[1,] # just keep one 
        }
        
        # Start the site processing
        # message(site_sf$site_id)
        site <- tibble(site_id = site_sf$site_id) # initialize a new non-sf tibble to return
        
        # Calculate distances to the downstream and upstream vertices of the
        # matched reach. I tried using st_split but see
        # https://gis.stackexchange.com/questions/288570/find-nearest-point-along-polyline-using-sf-package-in-r
        # -- that often doesn't actually split the line even if you snap the
        # point first
        vertex_upstream <- filter(vertices, point_ids == site_sf$nearest_subseg$start_pt)
        vertex_downstream <- filter(vertices, point_ids == site_sf$nearest_subseg$end_pt)
        subseg_as_points <- st_cast(st_geometry(site_sf$nearest_subseg), 'POINT') # would work poorly if there were a big straight reach with no intermediate points
        point_pos_in_subseg <- st_nearest_feature(site_sf, subseg_as_points)
        stopifnot(st_nearest_feature(vertex_upstream, subseg_as_points) == 1) # confirm that the points are listed upstream to downstream
        fish_dist_upstream_m <- st_combine(subseg_as_points[1:point_pos_in_subseg]) %>%
          st_cast('LINESTRING') %>% st_length() %>% units::drop_units()
        fish_dist_downstream_m <- st_combine(subseg_as_points[point_pos_in_subseg:length(subseg_as_points)]) %>%
          st_cast('LINESTRING') %>% st_length() %>% units::drop_units()
        
        # Decide which reach to use. Because the model predicts values for an
        # the downstream point of each stream reach, we will sometimes want to
        # use the reach upstream of the matched reach (if the site point was
        # very close to the upstream point). So we need some conditionals.
        if(fish_dist_downstream_m < fish_dist_upstream_m) {
          # The nearest point (by fish distance) is downstream, so use the current reach
          site$subseg_id <- site_sf$nearest_subseg$subseg_id
          site$bird_dist_to_subseg_m <- site_sf$bird_dist_to_subseg_m
          site$fish_dist_to_outlet_m <- fish_dist_downstream_m
        } else {
          # The nearest point is upstream, so count the reaches immediately
          # upstream to decide what to do
          upstream_subsegs <- filter(reaches, end_pt == vertex_upstream$point_ids)
          if(nrow(upstream_subsegs) == 0) {
            # The current reach is a headwater, so use the downstream point and
            # this reach
            site$subseg_id <- site_sf$nearest_subseg$subseg_id
            site$fish_dist_to_outlet_m <- fish_dist_downstream_m
          } else if(nrow(upstream_subsegs) == 1) {
            # The upstream reach exists and does not fork, so match to the
            # upstream point and the reach it drains
            site$subseg_id <- upstream_subsegs$subseg_id
            site$fish_dist_to_outlet_m <- -fish_dist_upstream_m # negative distance to indicate upstream
          } else if(nrow(upstream_subsegs) > 1) {
            # The upstream reach does fork, so just stick to matching to the current reach
            site$subseg_id <- site_sf$nearest_subseg$subseg_id
            site$fish_dist_to_outlet_m <- fish_dist_downstream_m
          }
        }
        # Regardless of whether we return the current or upstream subseg as the
        # best match, the as-a-bird-flies distance to the river should still be
        # the distance to the initial nearest reach, so that we can use this
        # measure to decide whether we were able to snap the site to the river
        # network successfully
        site$bird_dist_to_subseg_m <- site_sf$bird_dist_to_subseg_m
        
        return(site)
      })
  })
  
  return(crosswalk)
}
