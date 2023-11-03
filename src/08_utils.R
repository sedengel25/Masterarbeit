################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains all functions to execute 08_snn_map_matching.R
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
