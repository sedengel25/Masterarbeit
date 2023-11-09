################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains all functions that are needed over and over again
################################################################################
# Documentation: sample_subset
# Usage: sample_subset(dt)
# Description: Takes a subset of dt based on a random selection dep. on size
# Args/Options: dt
# Returns: datatable
# Output: ...
sample_subset <- function(dt, size) {
	vector_rand_inx <- sample(1:nrow(dt), size = size)
	
	dt_sub <- dt %>%
		slice(vector_rand_inx)
	
	return(dt_sub)
}


# Documentation: shp_network_to_linestrings
# Usage: shp_network_to_linestrings(shp_file)
# Description: Takes a shp-streetnetwork and returns all linestrings contained
# Args/Options: shp_file
# Returns: sf-linestrings
# Output: ...
shp_network_to_linestrings <- function(shp_file) {
	shp <- st_read(shp_file)
	sf_road_segments_mls <- shp$geometry
	list_sf_road_segments_ls <- split_multiline_in_line(sf_road_segments_mls)
	sf_ls <- st_sfc(list_sf_road_segments_ls)
	
	return(sf_ls)
}


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

