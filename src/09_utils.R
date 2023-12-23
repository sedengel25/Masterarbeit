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
	name_geom_col <- psql_get_name_of_geom_col(con, table_network)
	query <- paste0("CREATE TABLE ", table_mapped_points, " AS SELECT
    point.id,
    line.id AS line_id,
    ST_ClosestPoint(line.", name_geom_col,", point.geometry) AS closest_point_on_line,
    ST_Distance(line.", name_geom_col,", point.geometry) AS distance_to_line,
    ST_Distance(ST_StartPoint(line.", name_geom_col,
    "), ST_ClosestPoint(line.", name_geom_col, ", point.geometry)) AS distance_to_start,
    ST_Distance(ST_EndPoint(line.", name_geom_col, 
    "), ST_ClosestPoint(line.", name_geom_col, ", point.geometry)) AS distance_to_end
  FROM ", table_unmapped_points, " AS point CROSS JOIN LATERAL
    (SELECT id, ",
     name_geom_col, " FROM ",  table_network, "
     ORDER BY ",
     name_geom_col, " <-> point.geometry
     LIMIT 1) AS line;
  ")
	cat(query)
	dbExecute(con, query)
}

# Documentation: psql_create_visualisable_flows
# Usage: psql_create_visualisable_flows(con, table_origin, table_dest)
# Description: Binds points to linestrings so flows can be visualized
# Args/Options: con, table_origin, table_dest
# Returns: ...
# Output: ...
# Action: Binds points to linestrings
psql_create_visualisable_flows <- function(con, table_origin, table_dest) {
	# Create table to check whether calculating flow-nds worked: CHECK
	query <- paste0("CREATE TABLE col_vis_flows AS
  SELECT origin.id,
    ST_MakeLine(origin.closest_point_on_line , dest.closest_point_on_line) AS line_geom
  FROM ",  table_origin, " origin
  INNER JOIN ", char_mapped_d_points, " dest ON origin.id = dest.id;")
	
	dbExecute(con, table_dest)
}


# Documentation: psql_create_kmax_knn_tables
# Usage: psql_create_kmax_knn_tables(con, k_max, city_prefix, table_flows_nd)
# Description: Creates kmax psql-tables with knn-flows for each k within (1, kmax)
# Args/Options: con, k_max, city_prefix, table_flows_nd
# Returns: ...
# Output: ...
# Action: creates kmax psql-tables
psql_create_kmax_knn_tables <- function(con, k_max, city_prefix, table_flows_nd) {
	
	
	for(k in 1:k_max){
		print(k)
		char_k_nearest_flows <- paste0(city_prefix, "_", k, "_nearest_flows")
		psql_get_k_nearest_flows(con,
														 k = k,
														 table_flows = table_flows_nd,
														 table_k_nearest_flows = char_k_nearest_flows)
		
		psql_create_index(con, 
											table = char_k_nearest_flows, 
											col = c("flow_ref", "flow_other"))
	}
}


# Documentation: calc_rk 
# Usage: calc_fk(k)
# Description: Returns F_k (https://sci-hub.ee/10.1007/s11004-011-9325-x)
# Args/Options: k
# Returns: F_k
# Output: ...
# Action: ...
calc_fk <- function(k) {
	k_value <- k * factorial(2 * k) * (pi^0.5) / ((2^k * factorial(k))^2)
	return(k_value)
}

# Documentation: calc_rk
# Usage: calc_rk(k)
# Description: Calculates R_k from the snn_flow-Paper
# Args/Options: k
# Returns: R_k
# Output: ...
# Action: ...
calc_rk <- function(k) {
	true_value <- k
	ratio_value <- (true_value + 1 - ((2 * true_value + 1) / (2 * true_value))^2 *
										calc_fk(true_value)^2) / 
		(true_value - calc_fk(true_value)^2)
	cat("Ratio Value for k =", true_value, "is", ratio_value, "\n")
	return(ratio_value)
}



# Documentation: create_rkd_dt
# Usage: create_rkd_dt(con, k_max, city_prefix)
# Description: Creates at dt with the cols 'k' and 'rkd'
# Args/Options: con, k_max, city_prefix
# Returns: datatable
# Output: ...
# Action: ...
create_rkd_dt <- function(con, k_max, city_prefix) {
	num_variances <- c()
	num_rks <- c()
	for(k in 1:k_max){
		char_k_nearest_flows <- paste0(city_prefix, "_", k , "_nearest_flows")
		dt_k_nearest_flows <- RPostgres::dbReadTable(con, char_k_nearest_flows)
		num_variances[k] <- var(dt_k_nearest_flows$nd)
		num_rks[k] <- calc_rk(k = k)
	}
	num_variances_k <- num_variances
	num_variances_k_1 <- num_variances_k %>% lead
	num_variances_k_1 <- num_variances_k_1[-length(num_variances_k_1)]
	num_variances_k <- num_variances_k[-length(num_variances_k)]
	
	num_var_ratio <- num_variances_k_1/num_variances_k
	num_theo_var_ratio <- num_rks[-length(num_rks)]
	num_rkd <- num_var_ratio/ num_theo_var_ratio
	
	dt_rkd <- data.table(
		k = seq(1:(k_max-1)),
		rkd = num_rkd
	)
	
	return(dt_rkd)
}


