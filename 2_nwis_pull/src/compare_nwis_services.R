# compare NWIS dv and uv services

compare_services <- function(dv_ind, uv_ind, compare_ind) {
  uv <- feather::read_feather(sc_retrieve(uv_ind))
  dv <- feather::read_feather(sc_retrieve(dv_ind))
  
  uv <- uv %>%
    select(site_no, uv_begin_date = begin_date, uv_end_date = end_date, uv_count = count_nu)
  
  dv <- dv %>%
    select(site_no, dv_begin_date = begin_date, dv_end_date = end_date, dv_count = count_nu)
  
  compare <- full_join(uv, dv) %>%
    rowwise() %>%
    mutate(in_both = ifelse(any(is.na(c(dv_begin_date, uv_begin_date))), FALSE, TRUE),
           begin_dates_diff = dv_begin_date - uv_begin_date,
           end_dates_diff = dv_end_date - uv_end_date)
  
  feather::write_feather(as_data_file(compare_ind))
  gd_put(compare_ind)
}