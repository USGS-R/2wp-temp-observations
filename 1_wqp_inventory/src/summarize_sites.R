summarize_sites <- function(wqp_sites, site_key) {
  
  state_summ <- wqp_sites %>%
    filter(grepl(site_key, MonitoringLocationTypeName, ignore.case = TRUE)) %>%
    group_by(StateCode) %>%
    summarize(n_sites = n()) %>%
    left_join(dataRetrieval::stateCd, by = c('StateCode' = 'STATE')) %>%
    mutate(region = tolower(STATE_NAME)) %>%
    filter(!is.na(region))
 
  return(state_summ)
  
}

plot_summary <- function(summary_in, file_out) {
  us_states_sites <- map_data('state') %>%
    left_join(summary_in)
  
  
  wrapper <- function(x, ...) {
    paste(strwrap(x, ...), collapse = "\n")
  }
  
  keyword <- ifelse(grepl('lake', file_out), 'lake', 'stream')
  titletext <- paste('Number of unique', keyword, 'sites with water temperature observations in WQP')
  
  p <- ggplot(data = us_states_sites, 
              mapping = aes(x = long, y = lat, group = group)) +
    geom_polygon(aes(fill = n_sites), color = "gray70", size = 0.1) +
    coord_map(projection = "albers", lat0 = 39, lat1 = 45) +
    scale_fill_gradient(low = "cornsilk", high = "#CB454A") +
    theme_map() +
    labs(title = wrapper(titletext, width = 60),
         fill = 'Sites')
  ggsave(file_out, p, width = 6, height = 4)
}