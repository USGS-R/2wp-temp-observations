library(scipiper)
library(data.table)
library(feather)
library(rgdal)
library(geosphere)
library(dplyr)
library(raster)
library(sf)

#' @param sites the file path for an inventory file with MonitoringLocationIdentifier, latitude, and longitude at a minimum
#' @param reaches the file path to a shapefile or gdb with a reach identifier
#' @param layerName the name of the gdb layer
#' @param isGDB boolean TRUE if the reaches file is a gdb, FALSE if it is a shapefile
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
  siteCoords <- select(sitesToMatch, longitude, latitude) %>%
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
  init_coords <- as.data.table(points_subset)
  init_coords <- init_coords[, .(lon = longitude, lat = latitude)]
  
  ## get nearest line to each point
  dist <- geosphere::dist2Line(p=init_coords, line=reachLayer)
  dist.df <- as.data.frame(dist)
  
  ## get the actual reachId and the original site coords
  for (i in 1:nrow(dist.df)) {
    dist.df$reachId[i] <- reachLayer[, ]@lines[[dist.df$ID[i]]]@ID
    dist.df$site.lat[i] <- init_coords$lat[i]
    dist.df$site.lon[i] <- init_coords$lon[i]
  }

  ## get the monitoring Location Identifier from the site coords
  dist.df <- dplyr::left_join(dist.df, sitesToMatch, by = c("site.lat" = "latitude", "site.lon" = "longitude"))
  
  ## remove unnecessary columns
  dist.df <- dplyr::select(dist.df, MonitoringLocationIdentifier, reachId)
  
  return(dist.df)
}