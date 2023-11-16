################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains all functions to execute 09_snn_map_od_points_on_network.R
#############################################################################
# Documentation: dt_to_sf_network
# Usage: dt_to_sf_network(dt)
# Description: Transforms the shortest_path-csv from DBeaver into sf_network
# Args/Options: dt
# Returns: sf_network
# Output: ...
# Action: ...
dt_to_sf_network <- function(dt) {
	sf_network <- dt %>% 
		select(geom_edge) %>% 
		dplyr::rename(geometry = geom_edge) %>% 
		st_as_sf %>% 
		as_sfnetwork()
	
	return(sf_network)
}


# Documentation: extract_nodes_from_sf_network
# Usage: extract_nodes_from_sf_network(sf_network)
# Description: Extracts nodes from sf_network and returns them as sf-object
# Args/Options: sf_network
# Returns: sf-object
# Output: ...
# Action: ...
extract_nodes_from_sf_network <- function(sf_network) {
	sf_nodes <- sf_network %>%
		as.data.table %>%
		st_as_sf
	
	sf_nodes <- sf_nodes %>%
		mutate(id = 1:nrow(sf_nodes))
	
	return(sf_nodes)
}

# Documentation: extract_edges_from_sf_network
# Usage: extract_edges_from_sf_network(sf_network)
# Description: Extracts edges from sf_network and returns them as sf-object
# Args/Options: sf_network
# Returns: sf-object
# Output: ...
# Action: ...
extract_edges_from_sf_network <- function(sf_network) {
	sf_edges <- sf_network %>%
		activate(edges) %>%
		as.data.table %>%
		st_as_sf
	
	return(sf_edges)
}







# Documentation: map_points_on_road_network
# Usage: map_points_on_road_network(sf_points, buffer_size)
# Description: Maps points on road network
# Args/Options: sf_points, buffer_size (m)
# Returns: sf
# Output: ...
# Action: ...
map_points_on_road_network <- function(sf_points, sf_linestrings) {
	# pb <- txtProgressBar(min = 0, max = nrow(sf_points), style = 3)

	for(i in 1:nrow(sf_points)){
		print(i)
		buffer_size <- 25
		# i <- 600
		# sf_linestrings <- sf_edges
		# sf_points <- sf_origin
		sf_point <- sf_points[i, "geometry"]

		sf_buffer <- st_buffer(sf_point, dist = buffer_size)


		
		### Map OD-points on road-network ------------------------------------------
		int_idx_intersections <- st_intersects(sf_buffer, sf_linestrings) %>% unlist
		sf_intersections <- sf_linestrings[int_idx_intersections,]
		
		
		while(nrow(sf_intersections) == 0 && buffer_size < 50){
			print("increase buffer by 25")
			buffer_size <- buffer_size + 25
			sf_buffer <- st_buffer(sf_point, dist = buffer_size)
			int_idx_intersections <- st_intersects(sf_buffer, sf_linestrings) %>% unlist
			sf_intersections <- sf_linestrings[int_idx_intersections,]
		}
		
		if(nrow(sf_intersections) == 0){
			print("no near street")
			sf_points[i, "note"] <- "not_mapped"
			next
		}

		sf_intersec_lines_as_multipt <- st_line_sample(sf_intersections, n = 10)

		sf_intersec_lines_as_pt <- st_cast(sf_intersec_lines_as_multipt, "POINT")

		int_idx_closest_point <- st_distance(sf_point, sf_intersec_lines_as_pt) %>% 
			which.min
		int_idx_closest_point <- int_idx_closest_point[1]
		
		geom_closest_point <- sf_intersec_lines_as_pt[int_idx_closest_point] %>% 
			st_as_sf
		
		# ggplot() +
		# 	geom_sf(data = sf_buffer) +
		# 	geom_sf(data = sf_intersections) +
		# 	geom_sf(data = sf_point, aes(color = "red")) +
		# 	geom_sf(data = geom_closest_point, aes(color = "green"))
		
		# Overwrite sf_points with point in road network
		sf_points[i, "geometry"] <- geom_closest_point
		
		
		### Get dist to start- & end-node of road segment --------------------------
		geom_closest_point_buffer <- st_buffer(geom_closest_point, 1)
		
		int_idx_line_with_point <-
			st_intersects(geom_closest_point_buffer, sf_intersections) %>% unlist
		
		sf_line_with_point <- sf_intersections[int_idx_line_with_point,]
		
		# Get start and end point of line as multipoint
		mulitpoints_boundary <- st_boundary(sf_line_with_point)
		points_boundary <- st_cast(mulitpoints_boundary, "POINT")
		

		# Extract start-point from multipoint
		point_edge_start <- points_boundary[1, "geometry"]
		
		# Extract end-point from multipoint
		point_edge_end <- points_boundary[2, "geometry"]
		

		dist_start <-
			st_distance(geom_closest_point, point_edge_start) %>% as.numeric
		
		dist_end <-
			st_distance(geom_closest_point, point_edge_end) %>% as.numeric
		
		sf_points[i, "dist_to_from"] <- dist_start
		sf_points[i, "dist_to_to"] <- dist_end
		
		# setTxtProgressBar(pb, i)
	}
	
	return(sf_points)
}
