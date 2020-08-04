# get national geofabric
get_national_gf <- function(sb_id, sb_name, out_ind) {
  
  # check against the local hash, if already present, and don't download if not needed
  out_dir <- scipiper::as_data_file(out_ind)
  if(dir.exists(out_dir) && file.exists(out_ind)) {
    current_files <- tools::md5sum(dir(out_dir, full.names=TRUE))
    expected_files <- unlist(yaml::yaml.load_file(out_ind))
    if(all.equal(current_files, expected_files)) {
      message('GF is already downloaded and matches the .ind file; doing nothing')
      return()
    } else {
      stop("GF is downloaded but doesn't match the .ind file. Stopping to let you manually update/delete one or both to save time (it's a big download!)")
    }
  }
  
  # if the data don't yet exist, download and unzip. if they do already exist,
  # then either (1) the ind file also exists and we will have returned or
  # stopped above, or (2) the ind file doesn't exist and will get created below.
  # we'll post a message if it's case 2.
  if(!dir.exists(out_dir)) {
    message("Downloading from Sciencebase...")
    temp_loc <- tempfile()
    authenticate_sb()
    sbtools::item_file_download(sb_id = sb_id, names = sb_name, destinations = temp_loc)
    if(dir.exists(out_dir)) unlink(out_dir, recursive=TRUE)
    unzip(temp_loc, exdir=dirname(out_dir))
  } else {
    message('GF is already downloaded; creating an .ind file to match')
  }
  
  # write an ind file with hash for each file in the directory
  sc_indicate(out_ind, data_file=dir(out_dir, full.names=TRUE))
}

find_endpoints <- function(national_network, out_ind) {
  
  gf_reaches <- sf::read_sf(national_network, layer='nsegment_v1_1') %>%
    mutate_at(c("tosegment_v1_1"), .funs = na_if, 0) %>% # tosegment is the downstream segment to which water flows next. replace 0 (none next) with NA
    st_zm(drop = TRUE, what = "ZM") %>% #M dimension is inconsistent; prevents some subsetting
    rename(seg_id = nsegment_v1_1, to_seg = tosegment_v1_1,
           shape_length = Shape_Length)
  
  # inspect. lessons:
  # Modify reaches according to lessons learned
  #other edge cases? is this still relevant w/GFv1.1?

  # Augment gf_reaches with all end points of all reaches. I initially tried to
  # find points in gf_points, but at least one doesn't exist (the outlet to 155)
  # and I realized what I wanted most was the end points anyway.
  #should take about 9 minutes for national, single-threaded
  cl <- makeCluster(detectCores() - 1, setup_strategy = "sequential")
  gf_reaches_indices <- clusterSplit(cl, 1:nrow(gf_reaches))
  gf_reaches_split <- lapply(X = gf_reaches_indices, FUN = function(x) {gf_reaches[x,]})
  clusterEvalQ(cl, library(sf))
  clusterEvalQ(cl, library(dplyr))
  add_endpoints <- function(df){
    df %>% rowwise() %>%
    mutate(end_points = sf::st_cast(Shape, "POINT") %>% {list(c(head(.,1),tail(.,1)))})
  }
  gf_applied <- parLapply(cl, X = gf_reaches_split, fun = add_endpoints)
  stopCluster(cl)
  gf_reaches_endpoints <- do.call(bind_rows, gf_applied)
  saveRDS(gf_reaches_endpoints, file = as_data_file(out_ind))
  gd_put(out_ind)
}

get_reach_direction <- function(seg_ids, reaches_bounded) {
  seg_ids_df <- dplyr::filter(reaches_bounded, seg_id %in% seg_ids)
  reaches_bounded_direction_subset <- seg_ids_df %>%
    dplyr::mutate(
      which_end_up = sapply(seg_id, function(segid) {
        reach <- dplyr::filter(reaches_bounded, seg_id == segid)
        if(!is.na(reach$to_seg)) { # non-river-mouths
          # upstream endpoint is the one furthest from the next reach downstream
          to_reach <- dplyr::filter(reaches_bounded, seg_id == reach$to_seg)
          which.max(sf::st_distance(reach$end_points[[1]], to_reach$Shape))
        } else { # river mouths
          # upstream endpoint is the one closest to a previous reach upstream
          from_reach <- dplyr::filter(reaches_bounded, to_seg == reach$seg_id) 
          if(nrow(from_reach) > 0) { 
            which.min(sf::st_distance(reach$end_points[[1]], from_reach$Shape))
          } else { #A river mouth with no upstream segment (small coastal streams)
            NA_integer_  
          }
        } # turns out that the first point is always the upstream point for the DRB at least. that's nice, but we won't rely on it.
      }),
      up_point = purrr::map2(end_points, which_end_up, function(endpoints, whichendup) { endpoints[[whichendup]] }),
      down_point = purrr::map2(end_points, which_end_up, function(endpoints, whichendup) { endpoints[[c(1,2)[-whichendup]]] })) 
  return(reaches_bounded_direction_subset)
}

compute_up_down_stream_endpoints <- function(reaches_bounded_ind, out_ind) {
  
  reaches_bounded <- readRDS(sc_retrieve(reaches_bounded_ind)) %>% 
    ungroup()
  #~26 minutes for national single-threaded, ~5 minutes on 7 cores
  cl <- makeCluster(detectCores() - 1)
  clusterEvalQ(cl, lapply(c('sf', 'dplyr', 'purrr'), library, character.only = TRUE))
  seg_ids_split <- clusterSplit(cl, reaches_bounded$seg_id)
  cluster_results <- parLapply(cl = cl, X = seg_ids_split, 
                               fun = get_reach_direction, reaches_bounded)
  stopCluster(cl)
  results_bound <- do.call(bind_rows, cluster_results)
  saveRDS(results_bound, file = as_data_file(out_ind))
  gd_put(out_ind)
} 

# 
#   # split reaches into segments where necessary so that every pair of points is
#   # connected by a segment, and no segments pass through and beyond a point
#   reach_net_info <- lapply(unique(reaches_bounded$seg_id), function(segid) {
#     reach <- filter(reaches_bounded, seg_id == segid)
#     from_reaches <- filter(reaches_bounded, to_seg == reach$seg_id)
# 
#     reach_points <- tibble(
#       point_raw = c(reach$up_point, from_reaches$down_point, reach$down_point),
#       pt_seg = c(segid, from_reaches$seg_id, segid),
#       pt_seg_flowend = c('up', rep('down', length(point_raw) - 1)), # 'up' and 'down' are positions relative to flow
#       point_snapped = lapply(point_raw, sf::st_snap, reach, tol=1e-9)) %>%
#       st_set_geometry('point_snapped') %>%
#       st_set_crs(st_crs(reach)) %>%
#       mutate(point_id = sprintf('%d%s', pt_seg, substring(pt_seg_flowend, 1, 1)))
# 
#     # split the reach into segments between neighboring pairs of points
#     net_geoms <- lwgeom::st_split(st_geometry(reach), st_geometry(reach_points)) %>%
#       st_collection_extract("LINESTRING")
#     net_edges <- tibble(
#       subseg_seg = rep(segid, length(net_geoms))) %>%
#       mutate(tmp_subseg_id = 1:n()) %>% # temporary id until we can rename by segment order
#       st_set_geometry(net_geoms) %>%
#       mutate(
#         left = lapply(geometry, function(geom) {
#           st_line_sample(geom, sample=0) %>% st_cast('POINT')
#         }),
#         right = lapply(geometry, function(geom) {
#           st_line_sample(geom, sample=1) %>% st_cast('POINT')
#         })) # 'left' and 'right' are positions within the linestring
#     net_vertices <- net_edges %>% # one row per edge end (non-unique vertex) but geometry is still the edge
#       gather('subseg_lineend', 'point', left, right)
# 
#     # navigate the set of subsegments by point to figure out their order
#     start_point <- filter(reach_points, pt_seg_flowend == 'up')
#     # vertex_order will contain indices into net_vertices, ordered from upstream to downstream
#     net_vertices_sf <- st_set_crs(do.call(c, net_vertices$point), st_crs(reach)) # format vertex points for computing distances to reach points
#     vertex_order <- which.min(st_distance(start_point, net_vertices_sf)) # first element of vertex_order
#     while(length(vertex_order) < nrow(net_vertices)) {
#       edge_start <- net_vertices[tail(vertex_order,1),]
#       edge_end_index <- net_vertices %>%
#         mutate(is_edge_end = (tmp_subseg_id == edge_start$tmp_subseg_id & subseg_lineend != edge_start$subseg_lineend)) %>%
#         pull(is_edge_end) %>% which()
#       vertex_order <- append(vertex_order, edge_end_index)
#       if(length(vertex_order) < nrow(net_vertices)) {
#         edge_end <- net_vertices[edge_end_index,]
#         next_edge_index <- net_vertices %>%
#           mutate(is_next_edge = (sapply(point, function(pt) { pt == edge_end$point[[1]] }) & tmp_subseg_id != edge_end$tmp_subseg_id)) %>%
#           pull(is_next_edge) %>% which()
#         vertex_order <- append(vertex_order, next_edge_index)
#       }
#     }
#     # create a flat table of edges (including the geometry) and their two
#     # vertices apiece (one vertex per row), ordered by flow from upstream to
#     # downstream
#     net_vertices_ordered <- net_vertices[vertex_order, ] %>%
#       mutate(
#         vertex_order = 1:n(),
#         subseg_id = sprintf('%d_%d', subseg_seg, ceiling((1:n())/2))) %>%
#       group_by(subseg_id) %>%
#       arrange(vertex_order) %>%
#       mutate(
#         subseg_updown=paste(substring(subseg_lineend,1,1), collapse=''), # lr means left side of linestring is upstream, flows to right side = downstream
#         pt_subseg_flowend=ifelse(substring(subseg_updown,1,1) == substring(subseg_lineend,1,1), 'up', 'down')) %>%
#       ungroup() %>%
#       select(subseg_id, subseg_seg, subseg_updown, geometry, pt_subseg_flowend, point)
# 
#     # join with info about how these subsegment endpoints relate to the original network
#     net_vertices_ord_sf <- st_set_crs(do.call(c, net_vertices_ordered$point), st_crs(reach)) # format REORDERED vertex points for computing distances to reach points
#     net_dists <- st_distance(net_vertices_ord_sf, reach_points) # row = net_vertex_ordered, col=reach_point
#     net_info_flat <- net_vertices_ordered %>%
#       mutate(reach_point_matches = lapply(seq_len(nrow(net_dists)), function(i) {
#         which(net_dists[i,] < units::set_units(1, 'm')) # find essentially equal points. should be completely equal (dist = 0, but be tolerant)
#       })) %>%
#       unnest(reach_point_matches) %>%
#       mutate(point_id = reach_points$point_id[reach_point_matches]) %>%
#       left_join(st_drop_geometry(reach_points), by='point_id') %>%
#       select(-reach_point_matches)
# 
#     # pull out a table of unique vertices, weeding out duplication in (1)
#     # original reach endpoints or contributing reach end points and (2) the end
#     # and start of subsequent line subsegments generated in this loop
#     net_vertices_final <- net_info_flat %>%
#       group_by(subseg_id, pt_subseg_flowend) %>%
#       summarize(
#         point_geom = unique(point_raw),
#         point_ids = paste(sort(point_id), collapse=';')) %>%
#       ungroup() %>%
#       group_by(point_ids) %>%
#       st_drop_geometry() %>%
#       summarize(
#         point_geom = unique(point_geom),
#         starts_subseg = if(any(pt_subseg_flowend=='up')) subseg_id[pt_subseg_flowend=='up'] else as.character(NA),
#         ends_subseg = if(any(pt_subseg_flowend=='down')) subseg_id[pt_subseg_flowend=='down'] else as.character(NA)) %>%
#       { st_set_geometry(., st_set_crs(do.call(st_sfc, .$point_geom), st_crs(reach))) } %>%
#       select(-point_geom)
# 
#     # pull out a table of unique edges (subsegments)
#     net_edges_dups <- net_info_flat %>%
#       group_by(subseg_id) %>%
#       mutate(
#         starts_with_pts = paste(sort(point_id[pt_subseg_flowend=='up']), collapse=';'),
#         ends_with_pts = paste(sort(point_id[pt_subseg_flowend=='down']), collapse=';'),
#         from_segs = paste(sort(pt_seg[pt_seg != segid & pt_subseg_flowend=='up']), collapse=';')
#       ) %>%
#       ungroup()
#     first_dups <- sapply(unique(net_edges_dups$subseg_id), function(ssid) which(net_edges_dups$subseg_id == ssid)[1])
#     net_edges_final <- net_edges_dups %>%
#       slice(first_dups) %>%
#       mutate(
#         subseg_id_num = as.integer(sapply(strsplit(subseg_id, '_'), `[`, 2)),
#         to_seg = ifelse(subseg_id_num == max(subseg_id_num), reach$to_seg, NA),
#         to_subseg = c(subseg_id[-1], ''),
#         subseg_length = st_length(geometry)) %>%
#       select(subseg_id, subseg_seg, subseg_updown, subseg_length, starts_with_pts, ends_with_pts, from_segs, to_seg, to_subseg, geometry)
# 
#     return(list(vertices=net_vertices_final, edges=net_edges_final))
#   })
# 
#   # create vertex table from reach_net_info, then remove duplicates and
#   # consolidate info in starts_subseg and ends_subseg
#   reach_net_vertices <- do.call(rbind, lapply(reach_net_info, `[[`, 'vertices')) %>%
#     mutate(
#       X = st_coordinates(geometry)[,'X'],
#       Y = st_coordinates(geometry)[,'Y'],
#       XY = sprintf('%0.5f,%0.5f', X, Y)) %>%
#     group_by(XY) %>% # can't group by geometry, but this yields the same number of unique points
#     summarize(
#       point_ids = paste(sort(unique(unlist(strsplit(point_ids, ';')))), collapse=';'),
#       starts_subseg = paste(unique(na.exclude(starts_subseg)), collapse=';'), # should only be 0 or 1 per row, but this paste() turns 0 into ''
#       ends_subseg = paste(sort(unique(na.exclude(ends_subseg))), collapse=';'))
# 
#   # Edge table needs more infor on to_subseg that can only be acquired now that we
#   # have all the subsegs prepared. For each subseg, find out which seg it feeds
#   # into and identify the specific subseg it feeds to
#   reach_net_edges <- do.call(rbind, lapply(reach_net_info, `[[`, 'edges'))
#   for(e in reach_net_edges$subseg_id) {
#     reach <- filter(reach_net_edges, subseg_id == e)
#     if(reach$to_subseg == '') {
#       if(!is.na(reach$to_seg)) {
#         to_subseg <- filter(
#           reach_net_edges,
#           reach$to_seg == subseg_seg,
#           grepl(sprintf('(^|;)%d(;|$)',reach$subseg_seg), from_segs))
#         if(nrow(to_subseg) != 1) {
#           print(to_subseg)
#           stop(sprintf("found no to_subseg for %s, to_seg=%d", e, reach$to_seg))
#         }
#         to_subseg_id <- to_subseg$subseg_id
#       } else {
#         to_subseg_id <- NA
#       }
#       reach_net_edges[which(reach_net_edges$subseg_id == e), 'to_subseg'] <- to_subseg_id
#     }
#   }
#   # Revise and attach point information
#   reach_net_edges <- reach_net_edges %>%
#     group_by(subseg_id) %>%
#     mutate(
#       # no need to revise starts_with_pts because each reach already knows about all its parents
#       # starts_with_pts = paste(grep(sprintf('(^|;)(%s)(;|$)', strsplit(starts_with_pts, ';')[[1]][1]), reach_net_vertices$point_ids, value=TRUE), collapse=';'),
#       ends_with_pts = paste(grep(sprintf('(^|;)(%s)(;|$)', ends_with_pts), reach_net_vertices$point_ids, value=TRUE), collapse=';')) %>%
#     ungroup() %>%
#     rename(start_pt=starts_with_pts, end_pt=ends_with_pts)# it's really just one point at each end of each subseg
# 
#   # For plotting's sake, augment reach info with connections to the upstream reaches. In each row of
#   # reaches_updown (an edge in the graph), the outlet (pour point) of from_seg is the upstream point, seg_id is used as
#   # the ID of the segment's downstream-most point, and the segment
#   # identified by seg_id (between from_seg and seg_id) has length seg_length.
#   reach_key_info <- reach_net_edges %>% select(subseg_id, to_subseg, subseg_length)
#   reaches_updown <- reach_key_info %>%
#     rename(next_subseg = to_subseg) %>% # to_seg will just be confusing in a minute, but we need it to decide whether something is an outlet, so rename it
#     #mutate(to_seg = seg_id) %>% # add a column to clarify that the seg_id and the to_pt share an ID
#     left_join(
#       reach_key_info %>% st_set_geometry(NULL) %>% select(prev_subseg = subseg_id, subseg_id = to_subseg),
#       by = 'subseg_id')
# 
#   # add seg_id_nat back into the reaches table
#   reach_net_edges_nat <- reach_net_edges %>%
#     mutate(last_in_seg = mapply(function(ss_s, e_p) { grepl(sprintf('(^|;)%dd($|;)', ss_s), e_p) }, subseg_seg, end_pt)) %>%
#     left_join(
#       gf_reaches %>% st_drop_geometry %>% select(seg_id_nat, seg_id),
#       by=c('subseg_seg'='seg_id')) %>%
#     mutate(seg_id_nat = ifelse(last_in_seg, seg_id_nat, NA)) %>%
#     select(-last_in_seg)
#   # combine reach_net_edges and reach_net_vertices into a final gloriously complete list for the sake of export
#   reach_net <- list(
#     edges = reach_net_edges_nat,
#     vertices = reach_net_vertices)
# 
#   saveRDS(reach_net, as_data_file(out_ind))
#   gd_put(out_ind)
# }
