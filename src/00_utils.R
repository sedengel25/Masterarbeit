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



# Documentation: get_cleaned_trip_data
# Usage: get_cleaned_trip_data(path_clean, path_org)
# Description: Combines the dt with ride & id of clean trips with org. data
# Args/Options: path_clean, path_org
# Returns: dt
# Output: ...
# Action: ...
get_cleaned_trip_data <- function(path_clean, path_org) {
	# Read in clean scooter trip data (only 'id' and 'ride)
	dt_clean <- read_rds(path_clean)
	
	# Read in full original data
	dt_org <- read_rds(path_org)
	
	# Merge at 'id' and 'ride'
	dt <- dt_clean %>%
		inner_join(dt_org, c("id", "ride"))
	
	return(dt)
}



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

# Documentation: connect_to_postgresql_db
# Usage: connect_to_postgresql_db(user, pw, dbname, host)
# Description: Creates connection to PostgreSQL-DB
# Args/Options: user, pw, dbname, host
# Returns: DB-Connection
# Output: ...
# Action: ...
connect_to_postgresql_db <- function(user, pw, dbname, host) {
	# Create a connection to the PostgreSQL database
	drv <- RPostgres::Postgres()
	con <- dbConnect(drv, 
									 dbname = dbname, 
									 host = host, 
									 user = user, 
									 password = pw,
									 port = 5432)
	return(con)
}
