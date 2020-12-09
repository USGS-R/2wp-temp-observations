#scratch space
#rerun with increasing df size, see how run time scales
#to see parallelization needed

time_df <- tibble()
for(i in c(100, 500, 1000, 5000, 10000)) {
  start_time <- Sys.time()
  gf_subset <- gf_reaches[1:i,]
  reaches_bounded <- gf_subset %>% mutate(
    end_points = lapply(seg_id, function(segid) {
      reach <- filter(gf_subset, seg_id==segid)
      end_points <- reach[1,] %>% {st_cast(sf::st_geometry(.), "POINT")} %>% {c(head(.,1),tail(.,1))}
      return(end_points)
    }))
  end_time <- Sys.time()
  time_df <- bind_rows(time_df, tibble(i = i, time = as.numeric(end_time - start_time)))
  message("Done with ", i, "run")
}
#this scales linearly
ggplot(time_df, aes(x = i, y = time)) + geom_point()
time_df$time <- as.numeric(time_df$time)
scale_formula <- glm(formula = time ~ i, data = time_df)
predict(scale_formula, data.frame(i = nrow(gf_reaches)))  #should be about 9 minutes

#---------------
time_df <- tibble()
for(i in c(100, 500, 1000, 5000, 10000)) {
  start_time <- Sys.time()
  reaches_bounded_subset <- reaches_bounded[1:i,]
  
  reaches_direction_sub <- reaches_bounded_subset %>%
    mutate(
      which_end_up = sapply(seg_id, function(segid) {
        reach <- filter(reaches_bounded_subset, seg_id == segid)
        if(!is.na(reach$to_seg)) { # non-river-mouths
          # upstream endpoint is the one furthest from the next reach downstream
          to_reach <- filter(reaches_bounded_subset, seg_id == reach$to_seg)
          which.max(st_distance(reach$end_points[[1]], to_reach$Shape))
        } else { # river mouths
          # upstream endpoint is the one closest to a previous reach upstream
          from_reach <- filter(reaches_bounded_subset, to_seg == reach$seg_id)[1,] # only need one
          which.min(st_distance(reach$end_points[[1]], from_reach$Shape))
        } # turns out that the first point is always the upstream point for the DRB at least. that's nice, but we won't rely on it.
      }))#, 
      #up_point = purrr::map2(end_points, which_end_up, function(endpoints, whichendup) { endpoints[[whichendup]] }),
      #down_point = purrr::map2(end_points, which_end_up, function(endpoints, whichendup) { endpoints[[c(1,2)[-whichendup]]] })) %>%
    #select(-end_points)
  
  end_time <- Sys.time()
  time_df <- bind_rows(time_df, tibble(i = i, time = as.numeric(end_time - start_time, units = "secs")))
  message("Done with ", i, "run")
}

scale_formula <- glm(formula = time ~ i, data = time_df)
predict(scale_formula, data.frame(i = nrow(reaches_bounded))) 
ggplot(time_df, aes(x = i, y = time)) + geom_point() + geom_smooth(method = "lm")

#####

time_df <- tibble()
for(i in c(100, 500, 1000, 5000, 10000, 30000)) {
  start_time <- Sys.time()
  sites_subset <- sites_sf[1:i,]
  
  flowline_indices <- nhdplusTools::get_flowline_index(flines = reaches_nhd_fields,
                                                       points = sites_subset,
                                                       max_matches = 1,
                                                       search_radius = 1000)
  end_time <- Sys.time()
  time_df <- bind_rows(time_df, tibble(i = i, time = as.numeric(end_time - start_time, units = "secs")))
  message("Done with ", i, "run")
  print(time_df)
}                                                     
scale_formula <- glm(formula = time ~ i, data = time_df)
predict(scale_formula, data.frame(i = nrow(sites_sf))) 
ggplot(time_df, aes(x = i, y = time)) + geom_point() + geom_smooth(method = "lm")
