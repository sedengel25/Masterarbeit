################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file creates random OD-flows
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/00_config_psql.R")


char_city_prefix <- "col"
char_osm2po_subset <- paste0(char_city_prefix, "_2po_4pgr_subset")
char_osm2po_points <- paste0(char_city_prefix, "_2po_4pgr_points")
char_flows_nd <- paste0(char_city_prefix, "_flows_nd")
char_random_o_points <- paste0(char_city_prefix, "_random_o_points")
char_random_d_points <- paste0(char_city_prefix, "_random_d_points")
char_random_o_nd <- paste0(char_city_prefix, "_random_o_nd")
char_random_d_nd <- paste0(char_city_prefix, "_random_d_nd")
char_random_flow_nd <- paste0(char_city_prefix, "_random_flow_nd")
char_random_k_nearest_flows <- paste0(char_city_prefix, "_random_k_nearest_flows")
char_random_common_flows <- paste0(char_city_prefix, "_random_common_flows")
char_random_reachable_flows <- paste0(char_city_prefix, "_random_reachable_flows")
char_dist_mat_red_no_dup <- paste0(char_city_prefix, "_dist_mat_red_no_dup")


# Create points on the network by setting a point each x meter
# query <- paste0("CREATE TABLE ", char_osm2po_points,
# " (
# 	id SERIAL PRIMARY KEY,
# 	line_id INTEGER,
# 	geom GEOMETRY(Point,32632),
# 	distance_to_start DOUBLE PRECISION,
# 	distance_to_end DOUBLE PRECISION
# );")
# 
# dbExecute(con, query)
# 
# 
# query <- paste0("
# INSERT INTO ", char_osm2po_points,
# " (line_id, geom, distance_to_start, distance_to_end)
# SELECT
# l.id as line_id,
# ST_LineInterpolatePoint(l.geometry, series.fraction / l.length) AS geom,
# (series.fraction) AS distance_to_start,
# (l.length - series.fraction) AS distance_to_end
# FROM
# (SELECT id, geometry, ST_Length(geometry) as length
# 	FROM ",
# char_osm2po_subset,
# " WHERE ST_GeometryType(geometry) = 'ST_LineString') l
# CROSS JOIN LATERAL
# generate_series(0, cast(l.length as integer), 5) AS series(fraction)
# WHERE
# series.fraction / l.length <= 1;")
# dbExecute(con, query)

# psql_create_index(con, char_osm2po_points, col = c("id", "line_id"))


# Get number of points created:
query <- paste0("SELECT MAX(id) FROM ",
								char_osm2po_points, ";")

result <- dbSendQuery(con, query)
data <- dbFetch(result)
int_max_id <- data %>% as.numeric


# Get number of original OD-points
query <- paste0("SELECT MAX(flow_m) FROM ",
								char_flows_nd, ";")

result <- dbSendQuery(con, query)
int_max_flow_m <- dbFetch(result) %>% as.numeric

query <- paste0("SELECT MAX(flow_n) FROM ",
								char_flows_nd, ";")

result <- dbSendQuery(con, query)
int_max_flow_n <- dbFetch(result) %>% as.numeric
int_max_flow <- max(int_max_flow_m, int_max_flow_n)

# Start loop for MONTECARLOSIM:
# 1. Choose random points using 'sample' to select OD-Points from created table
int_rand_origin <- sample(int_max_id, int_max_flow)
int_rand_dest <- sample(int_max_id, int_max_flow)




# Create tables for OD-points
psql_create_table_random_od_points(con, 
																	 table_random_od_points = char_random_o_points,
																	 table_all_points = char_osm2po_points,
																	 random_indices = int_rand_origin)

psql_create_index(con, char_random_o_points, col = c("id", "line_id"))

psql_create_table_random_od_points(con, 
																	 table_random_od_points = char_random_d_points,
																	 table_all_points = char_osm2po_points,
																	 random_indices = int_rand_dest)
psql_create_index(con, char_random_d_points, col = c("id", "line_id"))

# 2. Calc. ND between O-points and D-points
psql_calc_nd(con = con,
						 table_mapped_points = char_random_o_points,
						 table_network = char_osm2po_subset,
						 table_dist_mat =  char_dist_mat_red_no_dup,
						 table_nd =  char_random_o_nd)

psql_create_index(con, char_random_o_nd, col = c("o_m", "o_n"))
psql_calc_nd(con = con,
						 table_mapped_points = char_random_d_points,
						 table_network = char_osm2po_subset,
						 table_dist_mat =  char_dist_mat_red_no_dup,
						 table_nd =  char_random_d_nd)
psql_create_index(con, char_random_d_nd, col = c("d_m", "d_n"))
# 3. Calc. ND between flows
starttime <- Sys.time()
psql_calc_flow_nds(con = con,
									 table_o_nds = char_random_o_nd,
									 table_d_nds = char_random_d_nd,
									 table_flow_nds = char_random_flow_nd)
endtime <- Sys.time()
dur <- difftime(endtime, starttime, units = "mins")
psql_create_index(con, char_random_flow_nd, col = c("flow_m", "flow_n"))
# 4. Get k nearest flows
starttime <- Sys.time()
psql_get_k_nearest_flows(con = con,
												 k = 5,
												 table_flows = char_random_flow_nd,
												 table_k_nearest_flows = char_random_k_nearest_flows)
endtime <- Sys.time()
dur <- difftime(endtime, starttime, units = "mins")
psql_create_index(con, char_random_k_nearest_flows, col = c("flow_ref", "flow_other"))
# 5. Calc. common flows
starttime <- Sys.time()
psql_get_number_of_common_flows(con = con,
												 table_k_nearest_flows = char_random_k_nearest_flows,
												 table_common_flows = char_random_common_flows)
endtime <- Sys.time()
dur <- difftime(endtime, starttime, units = "mins")
psql_create_index(con, char_random_common_flows, col = c("flow1", "flow2"))
# Five min
# 6. Calc. directly-reachable flows
starttime <- Sys.time()
psql_get_number_directly_reachable_flows(con = con,
																				 k = 5,
																table_common_flows = char_random_common_flows,
																table_reachable_flows = char_random_reachable_flows)
endtime <- Sys.time()
dur <- difftime(endtime, starttime, units = "mins")
# fifteen mins



# Monte carlo simulation:
# Compare each i of original and random flows: Everytime the SNND of the random
# flow is greater then the SNND of the original add '1' and divide by the number
# of rounds in the monte-carlo simulation. 

# The result of the MCS is a p-value for each original flow. 

# If the p-value is under a certain signifcance level, the flow is considered
# a core flow.


# Then we build clusters based on the density-connectivity mechanism


# Eventually: Discuss the proper value for k