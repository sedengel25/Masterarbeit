################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains all functions to execute 11_snn_calc_pvalues.R
#############################################################################
# Documentation: psql_create_random_od_points
# Usage: psql_create_random_od_points(con, table_network, table_all_points)
# Description: Creates a table for points every x meter on the network
# Args/Options: con, table_network, table_all_points
# Returns: ...
# Output: ...
# Action: Executing a psql-query
psql_create_random_od_points <- function(con, m, table_network, table_all_points) {
	# Create points on the network by setting a point each x meter
	query <- paste0("CREATE TABLE ", table_all_points,
									" (
  	id SERIAL PRIMARY KEY,
  	line_id INTEGER,
  	geom GEOMETRY(Point,32632),
  	distance_to_start DOUBLE PRECISION,
  	distance_to_end DOUBLE PRECISION
  );")
	
	dbExecute(con, query)
	
	
	query <- paste0("
  INSERT INTO ", table_all_points,
									" (line_id, geom, distance_to_start, distance_to_end)
  SELECT
  l.id as line_id,
  ST_LineInterpolatePoint(l.geometry, series.fraction / l.length) AS geom,
  (series.fraction) AS distance_to_start,
  (l.length - series.fraction) AS distance_to_end
  FROM
  (SELECT id, geometry, ST_Length(geometry) as length
  	FROM ",
									table_network,
									" WHERE ST_GeometryType(geometry) = 'ST_LineString') l
  CROSS JOIN LATERAL
  generate_series(0, cast(l.length as integer), ", m, ") AS series(fraction)
  WHERE
  series.fraction / l.length <= 1;")
	dbExecute(con, query)
}


# Documentation: psql_create_table_random_od_points
# Usage: psql_create_table_random_od_points(con, 
# table_random_od_points, table_all_points,random_indices)
# Description: Creates a table for the random OD-points
# Args/Options: con, table_random_od_points, table_all_points,random_indices
# Returns: ...
# Output: ...
# Action: Executing a psql-query
psql_create_table_random_od_points <- function(con, 
																							 table_random_od_points, 
																							 table_all_points,
																							 random_indices) {
	query <- paste0("DROP TABLE IF EXISTS ", table_random_od_points)
	dbExecute(con, query)
	
	query <- paste0("CREATE TABLE ", table_random_od_points,
									" (
  	id SERIAL PRIMARY KEY,
  	line_id integer,
  	geom GEOMETRY(Point,32632),
  	distance_to_start DOUBLE PRECISION,
  	distance_to_end DOUBLE PRECISION
  );")
	dbExecute(con, query)
	
	query <- paste0("INSERT INTO ", 
									table_random_od_points,
									" (geom, line_id, distance_to_start, distance_to_end)
  SELECT geom, line_id, distance_to_start, distance_to_end
  FROM ", 
									table_all_points,
									" WHERE id IN (", 
									paste(random_indices, collapse = ", "), ");")
	dbExecute(con, query)
}


# Documentation: psql_compare_directly_reachable
# Usage: psql_compare_directly_reachable(con, table_reachable_flows, 
# table_random_reachable_flows,table_reachable_flows_compared)
# Description: Creates a table for the random OD-points
# Args/Options: con, table_reachable_flows, table_random_reachable_flows,
# table_reachable_flows_compared
# Returns: ...
# Output: ...
# Action: Executing a psql-query
psql_compare_directly_reachable <- function(con,
																						table_reachable_flows,
																						table_random_reachable_flows,
																						table_reachable_flows_compared) {
	query <- paste0("
  UPDATE ", table_reachable_flows_compared,
									" SET density = ", table_reachable_flows_compared,".density +
  	CASE
  WHEN b.reachable_count IS NULL THEN 0
  WHEN b.reachable_count >= COALESCE(a.reachable_count, 0) THEN 1
  ELSE 0
  END
  FROM
  (SELECT flow1, reachable_count FROM ", table_reachable_flows, ") AS a
  FULL OUTER JOIN
  (SELECT flow1, reachable_count FROM ", table_random_reachable_flows, ") AS b ON a.flow1 = b.flow1
  WHERE ",
									table_reachable_flows_compared, ".flow1 = COALESCE(a.flow1, b.flow1);")
	dbExecute(con, query)
}
