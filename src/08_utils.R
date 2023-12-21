################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains all functions to execute 08_snn_local_node_dist_mat.R
#############################################################################
# Documentation: psql_get_geometry_type
# Usage: psql_get_geometry_type(con, table)
# Description: Get type of the geometry column
# Args/Options: con, table
# Returns: ...
# Output: ...
# Action: Execute psql-query 
psql_get_geometry_type <- function(con, table) {
	name_geom_col <- psql_get_name_of_geom_col(con, table)
  query <- paste0("SELECT DISTINCT GeometryType(",
  								name_geom_col, ")FROM ", table, ";")
  
  geom_types <- dbGetQuery(con, query) %>% as.character()
  return(geom_types)
}


# Documentation: psql_change_geometry_type
# Usage: psql_change_geometry_type(con, char_osm2po, crs)
# Description: Change types of geometry-col of table considered
# Args/Options: con, char_osm2po, crs
# Returns: ...
# Output: ...
# Action: Change types of geometry-col of table considered
psql_transform_coordinates <- function(con, table, crs) {
	
	name_geom_col <- psql_get_name_of_geom_col(con, table)
	type_geom_col <- psql_get_geometry_type(con, table)
	query <- paste0("ALTER TABLE ", 
									table, 
									" ALTER COLUMN ",
									name_geom_col,
									" TYPE geometry(",
									type_geom_col,
									", ",
									crs, 
									") USING ST_Transform(",
									name_geom_col,
									",",
									crs, ");")
	dbExecute(con, query)
}


# Documentation: psql_create_bbox
# Usage: psql_create_bbox(con, char_shp)
# Description: Creates psql-bboxh
# Args/Options: con, char_shp
# Returns: ...
# Output: ...
# Action: Execute psql-query
psql_create_bbox <- function(con, table_shp, table_bbox) {
	psql_drop_table_if_exists(con, table_bbox)
	name_geom_col <- psql_get_name_of_geom_col(con, table_shp)
	query <- paste0("CREATE TABLE ", table_bbox, " AS 
  SELECT ST_ExteriorRing((ST_Dump(ST_Union(",
									name_geom_col,
  "))).geom) AS geometry FROM ", 
									table_shp, " LIMIT 1;")
	
	dbExecute(con, query)
}

# Documentation: psql_create_sub_network
# Usage: psql_create_sub_network(con, table_full_network, table_sub_network, 
# table_bbox)
# Description: Creates sub-network based on bbox
# Args/Options: con, table_full_network, table_sub_network, table_bbox
# Returns: ...
# Output: ...
# Action: creates new reduced PostgreSQL-table
psql_create_sub_network <- function(con, table_full_network, table_sub_network, 
																		table_bbox) {

	psql_drop_table_if_exists(con, table = table_sub_network)
	name_geom_col_network <- psql_get_name_of_geom_col(con, table_full_network)
	name_geom_col_bbox <- psql_get_name_of_geom_col(con, table_bbox)
	query <- paste0("CREATE TABLE ",
									table_sub_network,
									" AS SELECT cpp.* FROM ",
									table_full_network,
									" cpp, ",
									table_bbox,
									" bbox WHERE ST_Intersects(cpp.",
									name_geom_col_network,
									", bbox.",
									name_geom_col_bbox, ");")
	
	dbExecute(con, query)
}




# Documentation: psql_ls_to_polygon
# Usage: psql_ls_to_polygon(con, table)
# Description: Turns closed linestring into polygon
# Args/Options: con, table
# Returns: ...
# Output: ...
# Action: maps OD-points onto street network
psql_ls_to_polygon <- function(con, table) {
	name_geom_col <- psql_get_name_of_geom_col(con, table)
	
	query <- paste0("UPDATE ", table,
									" SET ",
									name_geom_col,
									"= (SELECT ST_MakePolygon(",
									name_geom_col,
									") FROM (SELECT ",
									name_geom_col,
									" FROM ",
									table,
									") AS subquery);")
	dbExecute(con, query)
}


# Documentation: all_to_all_shortest_paths_to_sqldb
# Usage: all_to_all_shortest_paths_to_sqldb(con, dt, char_dist_mat, g, buffer)
# Description: Create distance matrix for all intersections in network
# Args/Options: con, dt, char_dist_mat, g, buffer
# Returns: ...
# Output: ...
# Action: create psql-table
all_to_all_shortest_paths_to_sqldb <- function(con, dt, table, g, buffer) {
	
	dt_dist_mat <- data.table(
		source = integer(),
		target = integer(),
		path = character(),
		m = numeric()
	)
	
	psql_drop_table_if_exists(con, table)
	
	query <- paste0("CREATE TABLE ",
									table,
									" (source INTEGER,
									   target INTEGER,
									   path INTEGER[],
									   m NUMERIC);")
	dbExecute(con, query)
	

	pb <- txtProgressBar(min = 0, max = nrow(dt), style = 3)
	i <- 1
	for (source_node in dt$source){

		### Actual paths
		res = single_source_dijkstra(g, source = source_node, 
																						 weight = "m", cutoff = buffer)


		lengths = res[[1]]
		targets = lengths %>% names %>% as.integer
		sources = rep(source_node, length(targets))
		distances = lengths %>% as.integer
		paths = res[[2]] 
		paths = gsub("c", "", paths)
		paths = gsub("\\(", "", paths)
		paths = gsub("\\)", "", paths)
		paths = gsub(":", ",", paths)
		paths = sprintf("{%s}", paths)
		dt_append <- data.table(
			source = sources,
			target = targets,
			path = paths,
			m = distances
		)
		# print(dt_append)

		### Length only
		# res = single_source_dijkstra_path_length(g, source = source_node, 
		# 														 weight = "m", cutoff = buffer)

		# targets = names(res) %>% as.integer
		# sources = rep(source_node, length(targets))
		# distances = res %>% as.numeric
		# dt_append <- data.table(
		# 	source = sources,
		# 	target = targets,
		# 	m = distances
		# )
		dbAppendTable(conn = con,
														 name = table,
														 value = dt_append
		)
		# 
		i <- i +1
		setTxtProgressBar(pb, i)

	}
}

# Documentation: create_city_border_psql_table
# Usage: create_city_border_psql_table(con, crs, file_shp, file_sql)
# Description: Create a psql-table with geometries for city's border
# Args/Options: con, crs, file_shp, file_sql
# Returns: ...
# Output: ...
# Action: create psql-table
create_city_border_psql_table <- function(con, crs, file_shp, file_sql) {
	char_cmd_psql_shp_to_sql <- sprintf('"%s" -I -s %s "%s" public.%s > "%s"',
																			file_exe_shp2psql,
																			crs,
																			file_shp,
																			char_shp,
																			file_sql
	)
	
	cmd_write_sql_file(char_cmd_psql_shp_to_sql)
	psql_drop_table_if_exists(con, char_shp)
	cmd_execute_sql_file(file_sql)
}
