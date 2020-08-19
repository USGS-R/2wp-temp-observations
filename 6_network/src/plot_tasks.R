make_site_reach_plots <- function(outind, reaches_to_check, category, ...) {
  reaches_to_check_category <- reaches_to_check[[category]]
  
  task_names <- as.character(unique(reaches_to_check_category$seg_id_reassign))
  task_makefile_name <- sprintf('6_network_plots_%s_tasks.yml', category)
  plot_step <- create_task_step(
    step_name = 'plot',
    target_name = function(task_name, ...) {
      sprintf("6_network/out/%s_%s_plot.png", category, task_name)
    },
    command = function(task_name, ...) {
      psprintf("subset_and_plot_reach(outfile = target_name,",
               "reach_and_sites = '6_network/out/site_flowlines.rds.ind',",
               "network_latlon = reaches_direction_latlon,",
                "seg_id_to_plot = I('%s')," = task_name,
                "category = I('%s'))" = category)
    }
  )
  
  task_plan <- create_task_plan(
    task_names = task_names,
    task_steps = list(plot_step),
    add_complete = FALSE,
    final_step = 'plot'
  )
  
  create_task_makefile(
    task_plan = task_plan,
    makefile = task_makefile_name,
    include = '6_network.yml',
    sources = c(...),
    packages = c('sf', 'tidyverse', 'ggmap', 'assertthat', 'ggsn'),
    finalize_funs = 'zip_up_plots',
    final_targets = sprintf('6_network/out/%s_plots.zip.ind', category),
    as_promises = TRUE
  )
  
  loop_tasks(task_plan = task_plan, task_makefile = task_makefile_name,
            num_tries = 1)
}

subset_and_plot_reach <- function(outfile, reach_and_sites_ind, network_latlon,
                                  seg_id_to_plot,
                                  category) {
  reach_and_sites <- readRDS(sc_retrieve(reach_and_sites_ind))
  one_reach_and_sites <- reach_and_sites %>% 
    filter(seg_id_reassign == seg_id_to_plot)
  plot_reach_and_matched_sites(outfile = outfile, 
                               reach_and_sites = one_reach_and_sites, 
                               network_latlon = network_latlon,
                               category = category)
}

summarize_plots <- function(...) {
  dots <- list(...)
  png_dots <- dots[purrr::map_lgl(dots, grepl, pattern = ".png")]
  unlist(png_dots)
}

zip_up_plots <- function(zip_ind, ...) {
  dots <- list(...)
  png_dots <- dots[purrr::map_lgl(dots, grepl, pattern = ".png")]
  zip(as_data_file(zip_ind), files = unlist(png_dots))
  gd_put(zip_ind)
}