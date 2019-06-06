library(maptools)
library(dplyr)
library(rgdal)

#' match sites to nearest reach
#' 
#' @description given an inventory file with sites and a geospatial file with reaches, return MonitoringLocationIdentifier and reach identifier
#' @param sites the file path for an inventory file with MonitoringLocationIdentifier, latitude, and longitude at a minimum
#' @param reaches the file path to a shapefile or gdb with a reach identifier
#' @param layerName the name of the gdb layer
#' @param isGDB boolean TRUE if the reaches file is a gdb, FALSE if it is a shapefile
snap_reach <- function(sites, reaches, layerName, isGDB) {

# reaches <- "C:/Users/mhines/Downloads/delaware_stream_temp_by_segment/delaware_segments/delaware_segments.shp"
# sites <- "1_wqp_pull/inout/wqp_inventory.feather.ind"
# isGDB <- FALSE
# layerName <- NULL

projection <- "+proj=utm +zone=18 +ellps=WGS84 +datum=WGS84 +units=m +no_defs "

siteCoords <- select(sitesToMatch, latitude, longitude) %>%
  sp::SpatialPoints(proj4string = CRS("+proj=utm +zone=18 +ellps=WGS84 +datum=WGS84 +units=m +no_defs ")) %>%
  sp::spTransform(CRS(projection))

siteCoordsSPDF <- SpatialPointsDataFrame(siteCoords, proj4string = projection, data=sitesToMatch)

#project reach layer to wgs84
reachLayer <- spTransform(reachLayer, CRS("+proj=utm +zone=18 +ellps=WGS84 +datum=WGS84 +units=m +no_defs "))

snappedPoints = snapPointsToLines(siteCoordsSPDF, reachLayer, idField="seg_id_nat")

#snappedPoints seems to provide the same inds that the knnLookup provides, so will have to figure out how to work with that. Also, don't use this, it takes forever to run..

}  
