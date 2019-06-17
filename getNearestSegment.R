library(scipiper)
library(data.table)
library(SearchTrees)
library(feather)
library(rgdal)
library(sp)
library(geosphere)

#' @param sites the file path for an inventory file with MonitoringLocationIdentifier, latitude, and longitude at a minimum
#' @param reaches the file path to a shapefile or gdb with a reach identifier
#' @param layerName the name of the gdb layer
#' @param isGDB boolean TRUE if the reaches file is a gdb, FALSE if it is a shapefile
getNearestSegment <- function(sites, reaches, layerName=NULL, isGDB=FALSE, max_dist=1000){
  
  ## read in site data
  featherPath <- gd_get(sites)
  sitesToMatch <- read_feather(featherPath, columns = NULL)
  
  ## read in reach data
  if(isGDB) {
    reachLayer <- readOGR(dsn=reaches,layer=layerName)
  } else {
    reachLayer <- readOGR(reaches)
  }
  
  ## subset site data to only keep lon, lat, id
  init_coords <- as.data.table(sitesToMatch)
  init_coords <- init_coords[, .(lon = longitude, lat = latitude, ID = MonitoringLocationIdentifier)]
  
  #project reach layer to wgs84
  reachLayer <- spTransform(reachLayer, CRS("+proj=longlat +datum=WGS84"))
  
  ## extract coordinates from reach shapefile
  res <- lapply(slot(reachLayer, "lines"), function(x) lapply(slot(x, "Lines"),function(y) slot(y, "coords")))
  names(res)<-reachLayer@data$seg_id_nat
  segsID<-sapply(lapply(res,"[[",1),nrow)
  
  coords<-as.data.frame(do.call("rbind",lapply(res,"[[",1)),stringsAsFactors=F)
  colnames(coords)<- c("lon","lat")
  coords$ID<-rep(names(segsID),segsID)
  
  ## create a 'search tree' from coordinates
  tree <- createTree(coords[,1:2]) # required to calculate the nearest neighbour for each set of coordinates
  
  idsList <- vector() # new
  for (i in 1:nrow(init_coords)){
    neighbor <- knnLookup(tree,newdat=init_coords[i,1:2],k=1) # lookup nearest neighbour, 'k' determines the number of nearest neighbors
    
    D<-coords[neighbor,]
    D<-as.data.frame(D)
    D$dist<-geosphere::distm(D[,1:2], init_coords[i,1:2],fun=distVincentyEllipsoid)

    neigh_segm <- D$ID[D$dist<max_dist] # segments with parts below max_dist, new
    if (length(neigh_segm) == 0) warning(paste("No segments found within the specified maximum distance for ",i,"th coordinate.",sep=""))
    
    idsList <- unique(c(idsList,neigh_segm)) # new
  }
  return(idsList)
}