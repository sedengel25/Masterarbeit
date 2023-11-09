################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains all functions to execute 08_snn_create_network_dist_map.R
#############################################################################
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
create_dist_mat <- function(edges, buffer_size, sf_ls) {

	list_of_dts <- list()
	pb <- txtProgressBar(min = 0, max = nrow(edges), style = 3)

	# Loop through each node of the network
	for(i in 1:nrow(edges)){
		# i <- 1
		# buffer_size <- 1000
		edge <- edges[i,]
		print(edge)
		# Create a buffer
		buffer <- st_buffer(edge, buffer_size)
		print(buffer)
		# Get all linestrings within buffer
		intersec_ls <- st_intersection(buffer, sf_ls)
		print(intersec_ls)
		# Create sub-network based on linestrings within the buffer
		sub_network <- as_sfnetwork(intersec_ls$x)
		
		# Extract network's nodes
		# nodes_sub_network <- sub_network %>%
		# 	as.data.table

		
		# Extract network's edges
		edges_sub_network <- sub_network %>%
			activate(edges) %>%
			as.data.table %>%
			st_as_sf
		
		print(edges_sub_network)
		# We compare the ls found in the sub-net with the whole net to ensure that ...
		# ... only full ls and not cuttet ls (due to buffer) are used and ...
		# ... the ids of the points are the same (vs. starting at 1 in each sub-net)
		
		# Second problem: If a LS is only partly in buffer we loose a street
		same_edges <- st_equals(edges, edges_sub_network) %>% as.integer()
		same_edges <- which(!is.na(same_edges))
		edges_sub_network <- edges[same_edges,]
		

		

		if(nrow(edges_sub_network)==0){
			next
		}
		
		# Get length of network's edges
		num_edges_weight <- edges_sub_network %>% st_length
		
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
			filter(from < to)
		
		print(mat_dt)
		list_of_dts[[i]] <- mat_dt
		setTxtProgressBar(pb, i)
		
	}
	return(list_of_dts)
}

