################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains all functions to execute 08_snn_postgis.R
#############################################################################
# Documentation: psql_change_geometry_type
# Usage: psql_change_geometry_type(con, char_osm2po, crs)
# Description: Change types of geometry-col of table considered
# Args/Options: con, char_osm2po, crs
# Returns: ...
# Output: ...
# Action: Change types of geometry-col of table considered
psql_change_geometry_type <- function(con, table, crs) {
  # Bring geometry-column of line-table and point-table to the same name
	query <- paste0("ALTER TABLE ", 
									table,
									" RENAME COLUMN geom_way TO geometry;")
	
  dbExecute(con, query)
  
  
  # Bring line-table and point-table to the same crs
  query <- paste0("ALTER TABLE ", 
  								table, 
  " ALTER COLUMN geometry TYPE geometry(LINESTRING, ",
  crs, 
  ") USING ST_Transform(geometry, ", crs, ");")

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
	psql_drop_table_if_exists(con, table = table_bbox)
	query <- paste0("CREATE TABLE ", table_bbox, " AS 
  SELECT ST_ExteriorRing((ST_Dump(ST_Union(geom))).geom) AS geometry FROM ", 
									table_shp, ";")
	
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
	query <- paste0("CREATE TABLE ",
									table_sub_network,
									" AS SELECT cpp.* FROM ",
									table_full_network,
									" cpp, ",
									table_bbox,
									" bbox WHERE ST_Intersects(cpp.geometry, bbox.geometry);")
	
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
	query <- paste0("UPDATE ", table,
									" SET geometry = (SELECT ST_MakePolygon(geometry) FROM (SELECT geometry FROM ",
									table,
									") AS subquery);")
	dbExecute(con, query)
}

all_to_all_shortest_paths_to_sqldb <- function(con, dt, char_dist_mat, g, buffer) {
	
	dt_dist_mat <- data.table(
		source = integer(),
		target = integer(),
		m = numeric()
	)
	
	dbWriteTable(con, char_dist_mat, dt_dist_mat)

	pb <- txtProgressBar(min = 0, max = nrow(dt), style = 3)
	i <- 1
	for (source_node in dt$source){

		# Compute the shortest path and length from the source to edfined target nodes
		res = single_source_dijkstra_path_length(g, source = source_node, 
																 weight = "m", cutoff = buffer)

		targets = names(res) %>% as.integer
		sources = rep(source_node, length(targets))
		distances = res %>% as.numeric
		dt_append <- data.table(
			source = sources,
			target = targets,
			m = distances
		)
		RPostgres::dbAppendTable(conn = con,
														 name = char_dist_mat,
														 value = dt_append
		)
		i <- i +1
		setTxtProgressBar(pb, i)

	}
}

