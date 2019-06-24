library(scipiper)
library(data.table)
library(feather)
library(rgdal)
library(geosphere)
library(dplyr)
library(raster)
library(sf)
library(googledrive)
library(parallel)

#' @param sites the file path for an inventory file with MonitoringLocationIdentifier, latitude, and longitude at a minimum
#' @param reaches the file path to a shapefile or gdb with a reach identifier
#' @param layerName the name of the gdb layer
#' @param isGDB boolean TRUE if the reaches file is a gdb, FALSE if it is a shapefile
#' reaches <- "/home/megan/Documents/2wp-temp-observations/delaware_segments.shp"
#' sites <- "1_wqp_pull/inout/wqp_inventory.feather.ind"
getNearestSegment <- function(sites, reaches, layerName=NULL, isGDB=FALSE){
  
  ## read in site data
  featherPath <- gd_get(sites)
  sitesToMatch <- read_feather(featherPath, columns = NULL)
  
  ## read in reach data
  if(isGDB) {
    reachLayer <- readOGR(dsn=reaches,layer=layerName)
  } else {
    reachLayer <- readOGR(reaches)
  }

  ## make points spatial so we can subset them by the reach layer
  projection <- "+proj=longlat +datum=WGS84"
  siteCoords <- dplyr::select(sitesToMatch, longitude, latitude) %>%
    sp::SpatialPoints(proj4string = CRS("+proj=longlat +datum=WGS84")) %>%
    sp::spTransform(CRS(projection))
  siteCoordsSPDF <- SpatialPointsDataFrame(siteCoords, proj4string = projection, data=sitesToMatch)
 
  ## project reach layer to wgs84
  reachLayer <- spTransform(reachLayer, CRS("+proj=longlat +datum=WGS84"))

  ## change reachLayer ID to be the reachId
  for (i in 1:nrow(reachLayer)) {
    reachLayer@lines[[i]]@ID <- as.character(reachLayer[i, ]@data[[1]])
  }
  
  ## create a bbox around the reaches
  reachBbox <- st_bbox(reachLayer)
  reachPoly <- as(raster::extent(reachBbox[[1]],reachBbox[[3]],reachBbox[[2]],reachBbox[[4]]), "SpatialPolygons")
  
  ## project poly layer to wgs84
  crs(reachPoly) <- "+proj=longlat +datum=WGS84"
  
  ## geospatially subset points by reachPolygon
  points_subset <- siteCoordsSPDF[reachPoly,]
  
  ## subset to keep only data we need
  drops <- c("OrganizationIdentifier","MonitoringLocationIdentifier", "ResolvedMonitoringLocationTypeName", "StateName", "CountyName", "HUCEightDigitCode", "resultCount")
  init_coords <- points_subset[,!(names(points_subset) %in% drops)]
  
  ## parallel set up
  no_cores <- detectCores() - 1
  n <- 1000
  parts <- split(1:nrow(init_coords), cut(1:nrow(init_coords), n))
  cl <- makeCluster(no_cores, type = "FORK")
  print(cl)
  system.time(distParts <- parLapply(cl = cl, 
                                     X = 1:n, 
                                     fun = function(x) {
                                       points.sp <- init_coords[parts[[x]],]
                                       points.sp$system.index <- init_coords$system.index[parts[[x]]]
                                       dist <- geosphere::dist2Line(p = points.sp, line = reachLayer)
                                       # Convert dist to data.frame
                                       dist.df <- as.data.frame(dist)
                                       dist.df$site.lat <- init_coords@data$latitude[parts[[x]]]
                                       dist.df$site.lon <- init_coords@data$longitude[parts[[x]]]
                                       dist.df$reachId <- reachLayer[, ]@lines[[dist.df$ID[parts[[x]]]]]@ID
                                       colnames(dist.df) <- c("distance", "lon", "lat", "ID", "site.lat", "site.lon", "reachId")
                                       gc(verbose = FALSE) # free memory
                                       return(dist.df)
                                     }))
  
  stopCluster(cl)
  distBind <- do.call("rbind", distParts)

  ## get the monitoring Location Identifier from the site coords
  dist.df <- dplyr::left_join(dist.df, sitesToMatch, by = c("site.lat" = "latitude", "site.lon" = "longitude"))
  
  ## remove unnecessary columns
  dist.df <- dplyr::select(dist.df, MonitoringLocationIdentifier, reachId)
  
  return(dist.df)
}