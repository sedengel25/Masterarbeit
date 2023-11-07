################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains all functions to execute 09_snn_map_odpoints_on_network.R
#############################################################################
# Documentation: transform_num_to_WGS84_to_UTM32
# Usage: transform_num_to_WGS84_to_UTM32(dt, coords)
# Description: Transforms num-coords of dt into utm32
# Args/Options: dt, coords
# Returns: sf_object
# Output: ...
# Action: ...
transform_num_to_WGS84_to_UTM32 <- function(dt, coords) {
	sp_points <- SpatialPoints(coords = dt[,..coords],
														 proj4string = CRS("+proj=longlat +datum=WGS84"))
	
	# Transform to UTM Zone 32N
	sp_utm32 <- spTransform(sp_points, CRS("+proj=utm +zone=32 +datum=WGS84"))
	
	# Convert back to an sf object
	sf_points <- st_as_sf(sp_utm32)
	
	
	return(sf_points)
}


# Documentation: map_points_on_road_network
# Usage: map_points_on_road_network(sf_points, buffer_size)
# Description: Maps points on road network
# Args/Options: sf_points, buffer_size (m)
# Returns: ...
# Output: ...
# Action: Overwrites sf-object
map_points_on_road_network <- function(sf_points, buffer_size) {
	pb <- txtProgressBar(min = 0, max = nrow(sf_points), style = 3)
	for(i in 1:nrow(sf_points)){
		# i <- 80
		# buffer_size <- 50
		node <- sf_points[i, "geometry"]
		# Create a buffer such that only relevant linestrings are looked at
		buffer <- st_buffer(node, buffer_size)
		
		# Get all linestrings within buffer
		intersec <- st_intersection(buffer, sf_ls)
		
		if (nrow(intersec) == 0) {
			next
		}
		intersec <- intersec$geometry
		# Get all unique geom types
		geom_type_unique <- st_geometry_type(intersec) %>% unique
		
		
		# If a MULTILINESTRING is contained...
		if ("MULTILINESTRING" %in% geom_type_unique) {
			
			#... there are either MULTILINESTRINGS AND LINESTRINGS
			if(length(geom_type_unique)>1){
				intersec_mls <-
					intersec[st_geometry_type(intersec) == "MULTILINESTRING"]
				
				intersec_ls_sub <- st_sfc(split_multiline_in_line(intersec_mls))
				
				intersec_ls <-
					intersec[st_geometry_type(intersec) == "LINESTRING"]
				
				st_crs(intersec_ls_sub) <- st_crs(intersec_ls)
				intersec_ls <- c(intersec_ls, intersec_ls_sub)

			} else{ #... or only MULTILINESTRINGS
				intersec_mls <-
					intersec[st_geometry_type(intersec) == "MULTILINESTRING"]
				
				intersec_ls <- st_sfc(split_multiline_in_line(intersec_mls))
			}

		} else { #... if there is no MULTILINESTRING, no trafos need to be done
			intersec_ls <- intersec
		}
		


		# Split linestrigns into points
		intersec_pts <- st_segmentize(intersec_ls, 10)
		intersec_pts <- st_coordinates(intersec_pts)
		intersec_pts <- intersec_pts %>% as.data.table
		intersec_pts <- st_as_sf(intersec_pts, coords = c("X", "Y"))
		
		st_crs(intersec_pts) <- st_crs(node)
		
		# Get closest point
		closest_point <- st_distance(node, intersec_pts) %>% which.min
		closest_point <- intersec_pts[closest_point, "geometry"]
		
		# Overwrite sf_points with point in road network
		sf_points[i, "geometry"] <- closest_point
		setTxtProgressBar(pb, i)
	}
	
	return(sf_points)
}
