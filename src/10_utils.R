################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains all functions to execute 10_snn_calc_netowrk_dist.R
#############################################################################
# Documentation: create_df_od_points_dist_to_intersections
# Usage: create_df_od_points_dist_to_intersections(sf_points)
# Description: Gets distances of OD-points to closest start & end of roadsegment
# Args/Options: sf_points
# Returns: dataframe
# Output: ...
create_df_od_points_dist_to_intersections <- function(sf_points) {
	
	df <- data.frame(
		id = integer(),
		edge_start = integer(),
		edge_end = integer(),
		dist_start = numeric(),
		dist_end = numeric()
	)
	
	for(i in 1:nrow(sf_points)){
		i <- 1
		sf_points <- sf_origin
		buffer_size <- 200
		sf_ls_network <- sf_network
		int_idx_ls_p_network <- integer()
		
		# Set a small buffer such that not too many linestrings need to be checked...
		#...but danger of not getting the whole road segment of the point...
		#...therefore while-loop that increaes buffer-size if road segment not found
		while(length(int_idx_ls_p_network) == 0){
			origin <- sf_points[i, "geometry"]
			
			# Small buffer such that point definitely lies on a line
			buffer_small <- st_buffer(origin, dist = 0.5)
			
			# Big buffer to reduce the number of lines to be checked
			buffer_big <- st_buffer(origin, buffer_size)
			
			# Get linestring within buffer
			linestring_sub <- st_intersection(buffer_big, sf_ls_network)
			linestring_sub <- linestring_sub %>%
				filter(st_geometry_type(linestring_sub)=="LINESTRING")
			
			# Convert to network such that road segments (separated by intersections)
			# ...are identified
			network_sub <- linestring_sub %>% as_sfnetwork()
			
			# Extract sub-networks's edges
			linestring_network_sub <- network_sub %>%
				activate(edges) %>%
				as.data.table %>%
				st_as_sf
			st_geometry(linestring_network_sub) <- "geometry"
			
			# Get line-idx in sub-network that contains point
			int_idx_ls_p <-
				st_intersects(buffer_small, linestring_network_sub) %>% unlist
			int_idx_ls_p <- int_idx_ls_p[1]
			
			# Get geometry of that line
			linestring_with_point <- linestring_network_sub[int_idx_ls_p,]
			
			# Find line-index in full network
			int_idx_ls_p_network <-
				st_equals(sf_network_edges, linestring_with_point) %>% as.integer()
			int_idx_ls_p_network <- which(!is.na(int_idx_ls_p_network))
			buffer_size <- buffer_size + 100
		}
		
		# Get actual line in full network
		linestring_network_point <- sf_network_edges[int_idx_ls_p_network,]
		
		# Get start and end point of line as multipoint
		mulitpoints_boundary <- st_boundary(linestring_network_point)
		points_boundary <- st_cast(mulitpoints_boundary, "POINT")
		
		# Extract start-point from multipoint
		point_edge_start <- points_boundary %>% as.data.table
		point_edge_start <- point_edge_start[1, "geometry"] %>% pull
		
		# Extract end-point from multipoint
		point_edge_end <- points_boundary %>% as.data.table
		point_edge_end <- point_edge_end[2, "geometry"] %>% pull
		
		# Extract start-node-id from multipoint
		id_start <- points_boundary %>% as.data.table
		id_start <- id_start[1, "from"] %>% pull
		
		# Extract end-node-id from multipoint
		id_end <- points_boundary %>% as.data.table
		id_end <- id_end[1, "to"] %>% pull
		
		dist_start <- st_distance(origin, point_edge_start) %>% as.numeric
		dist_end <- st_distance(origin, point_edge_end) %>% as.numeric
		
		# ggplot() +
		# 	geom_sf(data = sf_points[i,]) +
		# 	geom_sf(data = linestring_network_point) +
		# 	geom_sf(data = points_boundary)
		
		df[i, "id"] <- sf_points[i,] %>% as.data.table %>% select(id)
		df[i, "edge_start"] <- id_start
		df[i, "edge_end"] <- id_end
		df[i, "dist_start"] <- dist_start
		df[i, "dist_end"] <- dist_end
		print(df)
		if(i>3){
			break
		}
	}
	return(df)
}
