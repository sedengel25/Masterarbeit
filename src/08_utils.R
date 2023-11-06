################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains all functions to execute 08_snn_create_network_dist_map.R
#############################################################################
# Documentation: split_multiline_in_line
# Usage: split_multiline_in_line(sf_mls)
# Description: Splits sf-multilinestrigns into sf-linestrings
# Args/Options: sf_mls
# Returns: list
# Output: ...
split_multiline_in_line <- function(sf_mls) {
	list_of_ls <- list()
	idx <- 1
	for(i in 1:length(sf_mls)){
		for(j in 1:length(st_cast(sf_mls[i], "LINESTRING"))){
			ls <- st_cast(sf_mls[i], "LINESTRING")[j] 
			list_of_ls[idx] <- ls
			idx <- idx + 1
		}
	}
	
	return(list_of_ls)
}



# Documentation: create_road_nodes_df
# Usage: create_road_nodes_df(list_sf_road_segments_ls)
# Description: Creates a df containing all nodes of all linestrings
# Args/Options: list_sf_road_segments_ls
# Returns: dataframe
# Output: ...
create_road_nodes_df <- function(list_sf_road_segments_ls) {
	dt_nodes <- data.frame(
		id = integer(),
		x_coord = numeric(),
		y_coord = numeric()
	)
	
	idx <- 1
	for(i in 1:length(list_sf_road_segments_ls)){

		sf_point <- st_cast(list_sf_road_segments_ls[[i]], "POINT")
		
		x_coord <- sf_point[1]
		y_coord <- sf_point[2]
		id <- idx
		dt_nodes[idx, "id"] = id
		dt_nodes[idx, "x_coord"] = x_coord
		dt_nodes[idx, "y_coord"] = y_coord
		
		idx <- idx + 1
		
		
		sf_point <- st_cast(st_reverse(list_sf_road_segments_ls[[i]]), "POINT")
		
		x_coord <- sf_point[1]
		y_coord <- sf_point[2]
		id <- idx
		dt_nodes[idx, "id"] = id
		dt_nodes[idx, "x_coord"] = x_coord
		dt_nodes[idx, "y_coord"] = y_coord

		idx <- idx + 1
	}
	
	return(dt_nodes)
}



# Documentation: create_dist_mat
# Usage: create_dist_mat(nodes, buffer_size)
# Description: Creates dist mat based on sub-matrices for each buffer-network
# Args/Options: nodes, buffer_size (m)
# Returns: list
# Output: ...
create_dist_mat <- function(nodes, buffer_size) {
	list_of_dts <- list()
	pb <- txtProgressBar(min = 0, max = nrow(nodes), style = 3)
	
	# Loop through each node of the network
	for(i in 1:nrow(nodes)){
		node <- nodes[i] %>% st_as_sf
		node_geom <- node$x
		
		# Create a buffer
		buffer <- st_buffer(node_geom, buffer_size)
		
		# Get all linestrings within buffer
		intersec <- st_intersection(buffer, sf_ls)
		intersec_ls <- intersec[st_geometry_type(intersec)=="LINESTRING"]
		
		# Create sub-network based on linestrings within the buffer
		sub_network <- as_sfnetwork(intersec_ls)
		
		# Extract network's nodes
		nodes_sub_network <- sub_network %>%
			as.data.table
		
		# Extract network's edges
		edges_sub_network <- sub_network %>%
			activate(edges) %>%
			as.data.table
		
		# We compare the ls found in the sub-net with the whole net to ensure that ...
		# ... only full ls and not cuttet ls (due to buffer) are used and ...
		# ... the ids of the points are the same (vs. starting at 1 in each sub-net)
		same_edges <- st_equals( edges$x, edges_sub_network$x) %>% as.integer()
		same_edges <- which(!is.na(same_edges))
		edges_sub_network <- edges[same_edges,]
		
		
		if(nrow(edges_sub_network)==0){
			next
		}
		
		# Get length of network's edges
		num_edges_weight <- edges_sub_network$x %>% st_as_sf %>% st_length
		
		# Create an igraph
		g <- graph_from_data_frame(
			data.frame(
				from = edges_sub_network$from,
				to = edges_sub_network$to,
				weight = num_edges_weight
			)
		)
		g <- as.undirected(g)
		
		# Get shortest paths between all nodes contained in the network
		mat_shortest_path <- shortest.paths(g, v=V(g), to=V(g))
		mat_shortest_path[is.infinite(mat_shortest_path)] <- NA
		
		
		
		mat_df <- as.data.frame(mat_shortest_path)
		mat_df$from <- rownames(mat_df)
		mat_dt <- mat_df %>%
			pivot_longer(cols = -from, names_to = "to", values_to = "distance") %>%
			filter(!is.na(distance)) %>%
			as.data.table
		
		mat_dt <- mat_dt %>%
			filter(distance < buffer_size)
		
		list_of_dts[[i]] <- mat_dt
		setTxtProgressBar(pb, i)
		
	}
	return(list_of_dts)
}

