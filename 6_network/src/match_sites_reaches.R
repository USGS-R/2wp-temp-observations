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
  browser()
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
  #rejoin to original reaches df
}