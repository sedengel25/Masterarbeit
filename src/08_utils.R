################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains all functions to execute 08_snn_postgis.R
#############################################################################
# Documentation: change_geometry_type
# Usage: change_geometry_type(con, table_osm2po, crs)
# Description: Change types of geometry-col of table considered
# Args/Options: con, table_osm2po, crs
# Returns: ...
# Output: ...
# Action: Change types of geometry-col of table considered
change_geometry_type <- function(con, table_osm2po, crs) {
  # Bring geometry-column of line-table and point-table to the same name
	query <- paste0("ALTER TABLE ", 
									table_osm2po,
									" RENAME COLUMN geom_way TO geometry;")
	
  dbExecute(con, query)
  
  
  # Bring line-table and point-table to the same crs
  query <- paste0("ALTER TABLE ", 
  table_osm2po, 
  " ALTER COLUMN geometry TYPE geometry(LINESTRING, ",
  crs, 
  ") USING ST_Transform(geometry, ", crs, ");")

  dbExecute(con, query)
}



# Documentation: create_spatial_indices
# Usage: create_spatial_indices(con, table_osm2po, o_table, d_table)
# Description: Create geometry-index for all tables involved
# Args/Options: con, table_osm2po, o_table, d_table
# Returns: ...
# Output: ...
# Action: Create geometry-index for all tables involved
create_spatial_indices <- function(con, table_osm2po, o_table, d_table) {

	query <- paste0("CREATE INDEX ON ",  table_osm2po, " USING GIST (geometry);")
  dbExecute(con, query)
  
  query <- paste0("CREATE INDEX ON ",  o_table, " USING GIST (geometry);")
  dbExecute(con, query)
  
  query <- paste0("CREATE INDEX ON ",  d_table, " USING GIST (geometry);")
  dbExecute(con, query)
  
}


# Documentation: get_sub_street_network
# Usage: get_sub_street_network(con, o_table, d_table, city_prefix)
# Description: Gets bbox of all OD-points and subsets network accordingly
# Args/Options: con, o_table, d_table, city_prefix
# Returns: ...
# Output: ...
# Action: creates new reduced PostgreSQL-table
get_sub_street_network <- function(con,
																	 o_table,
																	 d_table,
																	 table_osm2po,
																	 table_osm2po_subset) {
		
	# Drop the existing bbox temp table if it exists
	dbExecute(con, "DROP TABLE IF EXISTS bbox_geom;")

	# Calculate the bounding box with the correct SRID
	query <- paste0("WITH points AS (SELECT geometry FROM ", o_table, 
	" UNION ALL SELECT geometry FROM ", 
	d_table, 
	"), bbox AS (SELECT ST_SetSRID(ST_Extent(geometry), 32632) as geom FROM points)",
	" SELECT * INTO TEMP bbox_geom FROM bbox;")
	dbExecute(con, query)
	
	# Create a new subset table after dropping the old one if it exists
	query <- paste("DROP TABLE IF EXISTS", table_osm2po_subset)
	dbExecute(con, query)
	
	query <- paste0("CREATE TABLE ", table_osm2po_subset, " AS SELECT * FROM ", 
								 table_osm2po, " WHERE ST_Intersects(", table_osm2po, 
								 ".geometry, (SELECT geom FROM bbox_geom));")
	dbExecute(con, query)
	
	# Create spatial index
	query <- paste0("CREATE INDEX ON ",  table_osm2po_subset, " USING GIST (geometry);")
	dbExecute(con, query)
}



# move_postgis_to_schema <- function(con, schema) {
# 	dbExecute(con, "UPDATE pg_extension SET extrelocatable = true WHERE extname = 'postgis';")
# 	dbExecute(con, paste0("ALTER EXTENSION postgis SET SCHEMA ", schema))
# }

# Documentation: map_od_points_to_network
# Usage: map_od_points_to_network(con, table_mapped_points,
# char_od_table, table_osm2po_subset)
# Description: Gets bbox of all OD-points and subsets network accordingly
# Args/Options: con, table_mapped_points, char_od_table, table_osm2po_subset
# Returns: ...
# Output: ...
# Action: maps OD-points onto street network
map_od_points_to_network <- function(con,
																		 table_mapped_points,
																		 char_od_table,
																		 table_osm2po_subset) {
	
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
  FROM ", char_od_table, " AS point CROSS JOIN LATERAL
    (SELECT id, geometry
     FROM ",  table_osm2po_subset, "
     ORDER BY geometry <-> point.geometry
     LIMIT 1) AS line;
  ")
	cat(query)
	dbExecute(con, query)
}



all_to_all_shortest_paths_to_sqldb <- function(con, dt, g, buffer) {
	
	dt_dist_mat <- data.table(
		source = integer(),
		target = integer(),
		m = numeric()
	)
	
	# Create empty table for OD-matrix of local nodes
	if (dbExistsTable(con, "distance_matrix")) {
		dbRemoveTable(con, "distance_matrix")
		dbWriteTable(con, 'distance_matrix', dt_dist_mat)
	} else {
		dbWriteTable(con, 'distance_matrix', dt_dist_mat)
	}
	
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
														 name = "distance_matrix",
														 value = dt_append
		)
		i <- i +1
		setTxtProgressBar(pb, i)

	}
}

