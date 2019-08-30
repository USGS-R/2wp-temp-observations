#' Get an inventory of the number of records per site/variable combination in
#' WQP
#'
#' Depending on the size of the request, calls to WQP may need to be
#' partitioned based on record size. This gets the inventory of data available
#' on WQP that we need to fullfil our "needs" - which is a series of
#' site/variable combinations.
#'
#' @param inv_ind Indicator file for a table with columns site and variable
#'   which describes what we going to pull from WQP, which was calculated as
#'   the difference between our data "wants" and our data "haves".
#' @param wqp_pull_params List of lists that contain parameters of interest
#'   (e.g., temperature) and all corresponding synonyms available in WQP (e.g.,
#'   "Temperature" and "Temperature, water"), plus other pull parameters.
#' @param wqp_partition_config configuration information for the data/inventory
#'   pulls
#' @return A dataframe returned by the function dataRetrieval::whatWQPdata, with
#'   one row per site/variable combination and the 'resultCount' being the
#'   variable from which we will make decisions about partitioning data pull
#'   requests.
inventory_wqp <- function(inv_ind, wqp_pull_params) {
  
  wqp_call <- function(fun, args) {
    time <- system.time(out <- do.call(fun, args))
    info <- list(
      time = time[['elapsed']],
      nrow = nrow(out),
      out = out
    )
    return(info)
  }
  
  wqp_args <- wqp_pull_params
  
  # collapse all constituents into single vector
  wqp_args$characteristicName <- as.character(unlist(wqp_args$characteristicName))
  
  # only filter using characteristic names (and huc for test)
  wqp_dat <- wqp_call(whatWQPdata, wqp_args['characteristicName'])
  
  # keep columns of interest, put back lat/long
  # keeping lat/long here in case there was a situation where you'd want to 
  # take stock/map sites before pulling data
  dat_out <- wqp_dat$out %>%
    select(OrganizationIdentifier, MonitoringLocationIdentifier, ResolvedMonitoringLocationTypeName, 
           StateName, CountyName, HUCEightDigitCode, latitude = lat, longitude = lon, resultCount)
  
  # spit out nrows and time it took to get inventory
  message(sprintf('sample inventory complete, required %0.2f hours to retrieve %d rows', wqp_dat$time/(60*60), wqp_dat$nrow))
  
  # write and indicate the data file
  data_file <- scipiper::as_data_file(inv_ind)
  feather::write_feather(dat_out, data_file)
  gd_put(inv_ind)
  
}

#' Partition calls to WQP based on number of records available in WQP and a
#' number of records that is a reasonable call to WQP.
#'
#' @param partitions_ind Filename of the partitions indicator file to create.
#' @param inventory_ind .ind filename of a table with WQP record counts (from
#'   the output of dataRetrieval::whatWQPdata).
#' @param wqp_pull_params List of lists that contain parameters of interest
#'   (e.g., temperature) and all corresponding synonyms available in WQP (e.g.,
#'   "Temperature" and "Temperature, water"), plus other pull parameters.
#' @param wqp_partition_config YAML file containing an element called
#'   $target_pull_size, giving the maximum number of records that should be in a
#'   single call to WQP.
#' @return Nothing useful is returned; however, this function (1) Writes to
#'   partitions_ind a table having the same number of rows as wqp_needs - e.g.,
#'   for each site/variable combination. The dataframe stores the number of
#'   observations for that site/variable in WQP and the unique task identifier
#'   that partitions site/variables into WQP pulls.
partition_wqp_inventory <- function(partitions_ind, wqp_pull_params, inventory_ind, wqp_partition_cfg) {
  
  # Read in the inventory & config
  wqp_inventory <- feather::read_feather(sc_retrieve(inventory_ind))
  wqp_partition_config <- wqp_partition_cfg
  
  # filter out site types that are not of interest
  
  wqp_inventory <- wqp_inventory %>%
    filter(!(ResolvedMonitoringLocationTypeName %in% wqp_pull_params$DropLocationTypeName))
  
  # filter out bad org names
  bad_orgs <- grep(' |\\.|/', wqp_inventory$OrganizationIdentifier, value = TRUE)
  bad_orgs_sites <- filter(wqp_inventory, OrganizationIdentifier %in% bad_orgs)
  
  if (nrow(bad_orgs_sites) > 0){
    message(sprintf("**dropping %s sites and %s results due to bad MonIDs", nrow(bad_orgs_sites), sum(bad_orgs_sites$resultCount)))
  }
  
  wqp_inventory <- filter(wqp_inventory, !OrganizationIdentifier %in% bad_orgs)
  
  # Define the atomic_groups to use in setting up data pull partitions. An
  # atomic group is a combination of parameters that can't be reasonably split
  # into multiple WQP pulls. We're defining the atomic groups as distinct
  # combinations of constituent (a group of characteristicNames) and NHD-based
  # site ID
  atomic_groups <- wqp_inventory %>%
    #group_by(site_id) %>%
    #summarize(LakeNumObs=sum(resultCount)) %>%
    arrange(desc(resultCount))
  
  # Partition the full pull into sets of atomic groups that form right-sized
  # partitions. Use an old but fairly effective paritioning heuristic: pick
  # the number of partitions desired, sort the atomic groups by descending
  # size, and then go down the list, each time adding the next atomic group to
  # the partition that's currently smallest. With this approach we can balance
  # the distribution of data across partitions while ensuring that each site's
  # observations are completely contained within one file.
  
  # Decide how many partitions to create. This will be (A) the number of sites
  # (or lakes) with more observations than the target partition size, because
  # each of these sites/lakes will get its own partition + (B) the number of
  # remaining observations divided by the target partition size.
  target_pull_size <- wqp_partition_config$target_pull_size
  target_site_size <- wqp_partition_config$target_inv_size
  n_single_site_partitions <- filter(atomic_groups, resultCount >= target_pull_size) %>% nrow()
  n_multi_site_partitions_byresult <- filter(atomic_groups, resultCount < target_pull_size) %>%
    pull(resultCount) %>%
    { ceiling(sum(.)/target_pull_size) }
  
  # take into account ~1000 sites per pull
  n_multi_site_partitions_bysite <- nrow(filter(atomic_groups, resultCount < target_pull_size))/target_site_size
  n_multi_site_partitions <- ceiling(ifelse(n_multi_site_partitions_bysite >= n_multi_site_partitions_byresult, n_multi_site_partitions_bysite, n_multi_site_partitions_byresult))
  
  num_partitions <- n_single_site_partitions + n_multi_site_partitions
  
  # Assign each site to a partition. Sites with huge numbers
  # of observations will each get their own partition.
  partition_sizes <- rep(0, num_partitions)
  partition_site_sizes <- rep(0, num_partitions)
  assignments <- rep(NA, nrow(atomic_groups)) 
  partition_index <- 1:num_partitions
  
  # use a vector rather than adding a col to atomic_groups b/c it'll be way faster
  for(i in 1:nrow(atomic_groups)) {
    indexing_true <- partition_index[partition_site_sizes < target_site_size]
    smallest_partition_nonindexed <- which.min(partition_sizes[partition_site_sizes < target_site_size])
    smallest_partition <- indexing_true[smallest_partition_nonindexed]
    
    assignments[i] <- smallest_partition
    size_i <- atomic_groups[[i,"resultCount"]]
    partition_sizes[smallest_partition] <- partition_sizes[smallest_partition] + size_i
    partition_site_sizes[smallest_partition] <- partition_site_sizes[smallest_partition] + 1
  }
  
  
  # Prepare one data_frame containing info about each site, including
  # the pull, constituent, and task name (where task name will become the core
  # of the filename)
  pull_time <- Sys.time()
  attr(pull_time, 'tzone') <- 'UTC'
  pull_id <- format(pull_time, '%y%m%d%H%M%S')
  partitions <- atomic_groups %>%
    mutate(
      PullDate = pull_time,
      PullTask = sprintf('%s_%04d', pull_id, assignments)) %>%
    left_join(select(wqp_inventory, MonitoringLocationIdentifier, SiteNumObs=resultCount), by='MonitoringLocationIdentifier') %>%
    select(MonitoringLocationIdentifier, SiteNumObs, PullTask, PullDate)
  
  
  # Also write the data_frame to a location that will get overwritten with
  # each new pass through this function
  feather::write_feather(partitions, scipiper::as_data_file(partitions_ind))
  gd_put(partitions_ind) # 1-arg version requires scipiper 0.0.11+
}

