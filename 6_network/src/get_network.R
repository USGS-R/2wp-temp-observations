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
  gf_reaches_endpoints <- bind_rows(gf_applied)
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

  reaches_bounded <- readRDS(sc_retrieve(reaches_bounded_ind, 'getters.yml')) %>%
    ungroup()
  #~26 minutes for national single-threaded, ~5 minutes on 7 cores
  cl <- makeCluster(detectCores() - 1)
  clusterEvalQ(cl, lapply(c('sf', 'dplyr', 'purrr'), library, character.only = TRUE))
  seg_ids_split <- clusterSplit(cl, reaches_bounded$seg_id)
  cluster_results <- parLapply(cl = cl, X = seg_ids_split,
                               fun = get_reach_direction, reaches_bounded)
  stopCluster(cl)
  results_bound <- bind_rows(cluster_results)

  shape_crs <- st_crs(results_bound$Shape)
  reaches_direction <- results_bound %>%
    st_sf(crs = shape_crs, sf_column_name = 'Shape') %>%
    mutate(up_point = st_sfc(up_point, crs = shape_crs),
           #would make sense to do the same here for end_points
           down_point = st_sfc(down_point, crs = shape_crs))

  saveRDS(reaches_direction, file = as_data_file(out_ind))
  gd_put(out_ind)
}
