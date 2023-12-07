################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains all functions to execute 09_snn_select_k.R
#############################################################################
# Documentation: psql_dt_to_od_tables
# Usage: psql_dt_to_od_tables(con, dt)
# Description: Takes cleaned scooter-data and creates psql-tables for O & D
# Args/Options: con, dt
# Returns: ...
# Output: ...
# Action: Creates psql-table
psql_dt_to_od_tables <- function(con, dt) {
	# Turn datatable into separate sf-objects for origin and dest
	sf_origin <- transform_num_to_WGS84(dt = dt,
																			coords = c("start_loc_lon",
																								 "start_loc_lat"))
	sf_origin <- sf_origin %>%
		mutate(id = 1:nrow(sf_origin))
	sf_origin <- st_transform(sf_origin, crs = int_crs)
	
	
	sf_dest <- transform_num_to_WGS84(dt = dt,
																		coords = c("dest_loc_lon",
																							 "dest_loc_lat"))
	sf_dest <- sf_dest %>%
		mutate(id = 1:nrow(sf_dest))
	sf_dest <- st_transform(sf_dest, crs = int_crs)
	
	
	# Create table for Origin-Points
	if (dbExistsTable(con, paste0(char_origin))) {
		dbRemoveTable(con, paste0(char_origin))
		dbWriteTable(con, paste0(char_origin), sf_origin)
	} else {
		dbWriteTable(con, paste0(char_origin), sf_origin)
	}
	
	# Create table for Destination-Points
	if (dbExistsTable(con, paste0(char_dest))) {
		dbRemoveTable(con,  paste0(char_dest))
		dbWriteTable(con,  paste0(char_dest), sf_dest)
	} else {
		dbWriteTable(con,  paste0(char_dest), sf_dest)
	}
}



# Documentation: psql_map_od_points_to_network
# Usage: psql_map_od_points_to_network(con, table_mapped_points,
# table_unmapped_points, table_network)
# Description: Gets bbox of all OD-points and subsets network accordingly
# Args/Options: con, table_mapped_points, table_unmapped_points, table_network
# Returns: ...
# Output: ...
# Action: maps OD-points onto street network
psql_map_od_points_to_network <- function(con,
																					table_mapped_points,
																					table_unmapped_points,
																					table_network) {
	
	# Map origin points
	query <- paste0("DROP TABLE IF EXISTS ", table_mapped_points, ";")
	dbExecute(con, query)
	
	query <- paste0("CREATE TABLE ", table_mapped_points, " AS SELECT
    point.id,
    line.id AS line_id,
    ST_ClosestPoint(line.geometry, point.geometry) AS closest_point_on_line,
    ST_Distance(line.geometry, point.geometry) AS distance_to_line,
    ST_Distance(ST_StartPoint(line.geometry), ST_ClosestPoint(line.geometry, point.geometry)) AS distance_to_start,
    ST_Distance(ST_EndPoint(line.geometry), ST_ClosestPoint(line.geometry, point.geometry)) AS distance_to_end
  FROM ", table_unmapped_points, " AS point CROSS JOIN LATERAL
    (SELECT id, geometry
     FROM ",  table_network, "
     ORDER BY geometry <-> point.geometry
     LIMIT 1) AS line;
  ")
	dbExecute(con, query)
}