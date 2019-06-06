library(scipiper)
library(feather)
library(rgdal)
library(dplyr)
library(tibble)
library(sp)
library(SearchTrees)

#' match sites to nearest reach
#' 
#' @description given an inventory file with sites and a geospatial file with reaches, return MonitoringLocationIdentifier and reach identifier
#' @param sites the file path for an inventory file with MonitoringLocationIdentifier, latitude, and longitude at a minimum
#' @param reaches the file path to a shapefile or gdb with a reach identifier
#' @param layerName the name of the gdb layer
#' @param isGDB boolean TRUE if the reaches file is a gdb, FALSE if it is a shapefile
match_reach <- function(sites, reaches, layerName, isGDB) {
  
  #read in site data
  featherPath <- gd_get(sites)
  sitesToMatch <- read_feather(featherPath, columns = NULL)

  #read in reach data
  if(isGDB) {
    reachLayer <- readOGR(dsn=reaches,layer=layerName)
  } else {
    reachLayer <- readOGR(reaches)
  }
  
  projection <- "+proj=longlat +datum=WGS84"
    
  siteCoords <- select(sitesToMatch, latitude, longitude) %>%
    sp::SpatialPoints(proj4string = CRS("+proj=longlat +datum=WGS84")) %>%
    sp::spTransform(CRS(projection))
  
  siteCoordsDf <- as.data.frame(matrix(rep(NA, length(siteCoords) *3), ncol = 3))
  siteCoordsSPDF <- SpatialPointsDataFrame(siteCoords, proj4string = projection, data=sitesToMatch)
  
  #project reach layer to wgs84
  reachLayer <- spTransform(reachLayer, CRS("+proj=longlat +datum=WGS84"))
  
  #create centroid for each reach
  crs <- CRS(proj4string(reachLayer))
  box.cent <- matrix(rep(NA, length(reachLayer) *3), ncol = 3)

  #fill matrix with reach centroid and reach identifier
  for (j in 1:nrow(box.cent)){
    box.cent[j,c(1,2)] <- reachLayer[j, ]@lines[[1]]@Lines[[1]]@coords[1, 1:2]
    box.cent[j,3] <- reachLayer[j, ]@data[[1]]
  }
  
  #create spatialpoints object with reach centroids
  reach.pts <- SpatialPoints(coords = box.cent[,1:2], proj4string = crs)
  
  #create search tree of site coordinates
  tree <- createTree(coordinates(siteCoords))
  
  #find closest 1 nearest reach index for each site
  inds <- knnLookup(tree, newdat=coordinates(reach.pts), k=1)
  
  #get reach identifier for each site
  #not sure how this would work, can do siteCoords[inds] and get a coordinate response which i presume is the coordinates from the reach centroid, but not the ID
  
}
