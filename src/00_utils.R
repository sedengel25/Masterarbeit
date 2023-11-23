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



# Documentation: psql_drop_old_if_new_exists
# Usage: psql_drop_old_if_new_exists(con, old_table, new_table)
# Description: Drops old table if new adjusted table exists
# Args/Options: con, old_table, new_table
# Returns: ...
# Output: ...
# Action: psql-query
psql_drop_old_if_new_exists <- function(con, old_table, new_table) {
	query <- paste0("
		DO $$
		BEGIN
		  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' 
		  AND tablename = '", new_table, "') THEN
		    DROP TABLE IF EXISTS ", old_table, ";
		  END IF;
		END
		$$;")
	dbExecute(con, query)
}


# Documentation: psql_remove_duplicates
# Usage: psql_remove_duplicates(con, old_table, new_table, col)
# Description: Creates new table without duplicates based on old table
# Args/Options: con, old_table, new_table, col
# Returns: ...
# Output: ...
# Action: psql-query
psql_remove_duplicates <- function(con, old_table, new_table, col) {
	query <- paste0(
		"CREATE TABLE ", new_table,
		" AS SELECT DISTINCT ON (", paste(col, collapse = ", "), ") * FROM ", 
		old_table,
		";"
	)
	dbExecute(con, query)
}



# Documentation: psql_create_index
# Usage: psql_create_index(con, char_table, col)
# Description: Creates a index for chosen table on chosen column
# Args/Options: con, char_table, col
# Returns: ...
# Output: ...
# Action: psql-query
psql_create_index <- function(con, char_table, col) {
	query <- paste0("CREATE INDEX ", 
									paste0("idx_", char_table),
									" ON ",  
									char_table, 
									" USING btree (", 
									paste(col, collapse = ", "), 
									");")
	
	dbExecute(con, query)
}


# Documentation: psql_where_source_smaller_target
# Usage: psql_where_source_smaller_target(con, old_table, new_table)
# Description: Creates new table only with rows where source < target
# Args/Options: con, old_table, new_table
# Returns: ...
# Output: ...
# Action: psql-query
psql_where_source_smaller_target <- function(con, old_table, new_table) {
	query <- paste0("CREATE TABLE ", new_table,
									" AS SELECT * FROM ", 
									old_table, 
									" WHERE source < target;")
	
	dbExecute(con, query)
}



# Documentation: psql_check_indexes
# Usage: psql_check_indexes(con, char_table)
# Description: Returns indexes of table
# Args/Options: con, char_table
# Returns: ...
# Output: ...
# Action: psql-query
psql_check_indexes <- function(con, char_table) {
	query <- paste0("SELECT
  tablename,
  indexname,
  indexdef
  FROM
  pg_indexes
  WHERE
  tablename = '", char_table,"';")
	
	result <- dbSendQuery(con, query)
	data <- dbFetch(result)
	return(data)
}
