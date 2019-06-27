library(SearchTrees)
library(scipiper)
library(data.table)
library(feather)
library(dplyr)
library(rgdal)
library(sf)

#' @param sites the file path for an inventory file with MonitoringLocationIdentifier, latitude, and longitude at a minimum
#' @param reaches the file path to a shapefile or gdb with a reach identifier
#' @param layerName the name of the gdb layer
#' @param isGDB boolean TRUE if the reaches file is a gdb, FALSE if it is a shapefile
#' reaches <- "C:/Users/mhines/Downloads/delaware_stream_temp_by_segment/delaware_segments/delaware_segments.shp"
#' sites <- "1_wqp_pull/inout/wqp_inventory.feather.ind"
subset_closest <- function(reaches, sites, layerName=NULL, isGDB=FALSE){
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
  
  #features <- features[features$order_ > 1, ]
  box.cent <- matrix(rep(NA, length(reachLayer) *2), ncol = 2)
  
  for (j in 1:nrow(box.cent)){
    box.cent[j, ] <- reachLayer[j, ]@lines[[1]]@Lines[[1]]@coords[1, 1:2]
  }

  sites <- as.data.table(init_coords)
  sites <- sites[, .(lon = longitude, lat = latitude)]
  
  reach.pts <- SpatialPoints(coords = box.cent, proj4string = CRS(proj4string(reachLayer)))
  tree <- createTree(coordinates(reach.pts))
  inds <- c()
 
  inds <- knnLookup(tree, newx = sites$lon, newy=sites$lat, k=1)

  inds.df <- as.data.frame(inds)
  for (i in 1:nrow(inds.df)) {
    inds.df$reachId[i] <- reachLayer@data$seg_id_nat[inds.df[i,1]]
    inds.df$site.lat[i] <- sites$lat[i]
    inds.df$site.lon[i] <- sites$lon[i]
  }
  
  ## get the monitoring Location Identifier from the site coords
  inds.df <- dplyr::left_join(inds.df, sitesToMatch, by = c("site.lat" = "latitude", "site.lon" = "longitude"))
  
  ## remove unnecessary columns
  inds.df <- dplyr::select(inds.df, MonitoringLocationIdentifier, reachId)
  
  return(inds.df)
  
}
