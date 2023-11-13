################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains all functions to execute 08_snn_map_od_points_on_network.R
#############################################################################
# Documentation: transform_num_to_WGS84
# Usage: transform_num_to_WGS84(dt, coords)
# Description: Transforms num-coords of dt into wgs84
# Args/Options: dt, coords
# Returns: sf_object
# Output: ...
# Action: ...
transform_num_to_WGS84 <- function(dt, coords) {
	sp_points <- SpatialPoints(coords = dt[,..coords],
														 proj4string = CRS("+proj=longlat +datum=WGS84"))
	
	# Transform to UTM Zone 32N
	# sp_utm32 <- spTransform(sp_points, CRS("+proj=utm +zone=32 +datum=WGS84"))
	
	# Convert back to an sf object
	sf_points <- st_as_sf(sp_points)
	
	
	return(sf_points)
}


# Documentation: map_points_on_road_network
# Usage: map_points_on_road_network(sf_points, buffer_size)
# Description: Maps points on road network
# Args/Options: sf_points, buffer_size (m)
# Returns: sf
# Output: ...
# Action: ...
map_points_on_road_network <- function(sf_points, sf_linestrings) {
	pb <- txtProgressBar(min = 0, max = nrow(sf_points), style = 3)
	for(i in 1:nrow(sf_points)){
		print(i)
		# i <- 667
		buffer_size <- 50
		# sf_linestrings <- sf_edges
		sf_points <- sf_origin
		
		sf_point <- sf_points[i, "geometry"]

		sf_buffer <- st_buffer(sf_point, dist = buffer_size)

		sf_intersections <- st_intersection(sf_buffer, sf_linestrings)
		
		while(nrow(sf_intersections) == 0){
			buffer_size <- buffer_size + 50
			sf_buffer <- st_buffer(sf_point, dist = buffer_size)
			sf_intersections <- st_intersection(sf_buffer, sf_linestrings)
		}
		
		int_idx_mls <- which(st_geometry_type(sf_intersections) == "MULTILINESTRING")
		
		if(length(int_idx_mls) > 0){
			sf_intersec_ls <- sf_intersections[-int_idx_mls,]
			sf_intersec_mls <- sf_intersections[int_idx_mls,]
			sf_intersec_ls_2 <- st_cast(sf_intersec_mls, "LINESTRING")

			sf_intersections <- rbind(sf_intersec_ls, sf_intersec_ls_2)
		}

		sf_intersec_lines_as_multipt <- st_line_sample(sf_intersections, n = 10)

		sf_intersec_lines_as_pt <- st_cast(sf_intersec_lines_as_multipt, "POINT")

		int_idx_closest_point <- st_distance(sf_point, sf_intersec_lines_as_pt) %>% which.min
		int_idx_closest_point <- int_idx_closest_point[1]
		geom_closest_point <- sf_intersec_lines_as_pt[int_idx_closest_point] %>% st_as_sf
		
		# ggplot() +
		# 	geom_sf(data = sf_buffer) +
		# 	geom_sf(data = sf_intersections) +
		# 	geom_sf(data = sf_point, aes(color = "red")) +
		# 	geom_sf(data = geom_closest_point, aes(color = "green"))
		
		# Overwrite sf_points with point in road network
		sf_points[i, "geometry"] <- geom_closest_point
		
		# setTxtProgressBar(pb, i)
	}
	
	return(sf_points)
}
