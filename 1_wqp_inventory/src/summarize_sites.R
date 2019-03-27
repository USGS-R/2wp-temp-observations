make_site_summaries <- function(wqp_sites) {
  
  county_summ <- wqp_sites %>%
    group_by(StateCode, CountyCode) %>%
    summarize(n_sites = n()) %>%
    left_join(dataRetrieval::countyCd, by = c('StateCode' = 'STATE', 'CountyCode' = 'COUNTY'))
  
  state_summ_streams <- wqp_sites %>%
    filter(grepl('stream|river', MonitoringLocationTypeName, ignore.case = TRUE)) %>%
    group_by(StateCode) %>%
    summarize(n_sites = n()) %>%
    left_join(dataRetrieval::stateCd, by = c('StateCode' = 'STATE')) %>%
    mutate(region = tolower(STATE_NAME)) %>%
    filter(!is.na(region))
  
  state_summ_lakes <- wqp_sites %>%
    filter(grepl('lake|reserv|impound', MonitoringLocationTypeName, ignore.case = TRUE)) %>%
    group_by(StateCode) %>%
    summarize(n_sites = n()) %>%
    left_join(dataRetrieval::stateCd, by = c('StateCode' = 'STATE')) %>%
    mutate(region = tolower(STATE_NAME)) %>%
    filter(!is.na(region))
  
  library(maps)
  library(ggplot2)
  library(ggthemes)
  
  us_states_stream <- map_data('state') %>%
    left_join(state_summ_streams)
  
  us_states_lakes <- map_data('state') %>%
    left_join(state_summ_lakes)

  ggplot(data = us_states_stream, 
         mapping = aes(x = long, y = lat, group = group)) +
    geom_polygon(aes(fill = n_sites), color = "gray70", size = 0.1) +
    coord_map(projection = "albers", lat0 = 39, lat1 = 45) +
    scale_fill_gradient(low = "cornsilk", high = "#CB454A") +
    theme_map()
  
  ggplot(data = us_states_lakes, 
         mapping = aes(x = long, y = lat, group = group)) +
    geom_polygon(aes(fill = n_sites), color = "gray70", size = 0.1) +
    coord_map(projection = "albers", lat0 = 39, lat1 = 45) +
    scale_fill_gradient(low = "cornsilk", high = "#CB454A") +
    theme_map()
}