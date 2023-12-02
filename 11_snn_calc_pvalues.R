################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file creates rand. OD-flows, executes Monte Carlo-Simulation + calc. p-value
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/00_config_psql.R")
source("./src/11_utils.R")


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
char_reachable <- paste0(char_city_prefix, "_numb_reachable_flows")
char_random_reachable_flows <- paste0(char_city_prefix, "_random_reachable_flows")
char_dist_mat_red_no_dup <- paste0(char_city_prefix, "_dist_mat_red_no_dup")
char_flows_pvalues <- paste0(char_city_prefix, "_flows_pvalues")
int_m <- 5
int_k <- 20
int_simulations <- 99
int_alpha <- 0.05
# Create points on road network every x meter
psql_create_random_od_points(con, m = int_m, table_network = char_osm2po_subset,
														 table_all_points = char_osm2po_points)


psql_create_index(con, char_osm2po_points, col = c("id", "line_id"))


# Get number of points created:
int_max_id <- psql_get_max_of_num_col(con, 
																			num_col = 'id', 
																			table = char_osm2po_points)

# Get original number of flows
int_max_flow_m <- psql_get_max_of_num_col(con, 
																					num_col = 'flow_m', 
																					table = char_flows_nd)
int_max_flow_n <- psql_get_max_of_num_col(con, 
																					num_col = 'flow_n', 
																					table = char_flows_nd)
int_max_flow <- max(int_max_flow_m, int_max_flow_n)

################################################################################
# START MONTECARLO-SIMULATION
################################################################################
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

query <- paste0("ALTER TABLE ",
								char_random_d_nd,
								" RENAME COLUMN o_m TO d_m;")
dbExecute(con, query)

query <- paste0("ALTER TABLE ",
								char_random_d_nd,
								" RENAME COLUMN o_n TO d_n;")
dbExecute(con, query)

psql_create_index(con, char_random_d_nd, col = c("d_m", "d_n"))
# 3. Calc. ND between flows
psql_calc_flow_nds(con = con,
									 table_o_nds = char_random_o_nd,
									 table_d_nds = char_random_d_nd,
									 table_flow_nds = char_random_flow_nd)
psql_create_index(con, char_random_flow_nd, col = c("flow_m", "flow_n"))
# 4. Get k nearest flows
psql_get_k_nearest_flows(con = con,
												 k = int_k,
												 table_flows = char_random_flow_nd,
												 table_k_nearest_flows = char_random_k_nearest_flows)
psql_create_index(con, char_random_k_nearest_flows, col = c("flow_ref", "flow_other"))
# 5. Calc. common flows
psql_get_number_of_common_flows(con = con,
												 table_k_nearest_flows = char_random_k_nearest_flows,
												 table_common_flows = char_random_common_flows)
psql_create_index(con, char_random_common_flows, col = c("flow1", "flow2"))
# 6. Calc. directly-reachable flows
psql_get_number_directly_reachable_flows(con = con,
																				 k = int_k,
																table_common_flows = char_random_common_flows,
																table_reachable_flows = char_random_reachable_flows)
psql_create_index(con, char_random_reachable_flows, col = c("flow1"))
# psql_get_max_of_num_col(con, num_col = 'flow1', table = char_reachable)
# psql_count_rows(con, char_random_reachable_flows) 


# In first round of MSC:
query <- paste0("CREATE TABLE ", 
								char_flows_pvalues,
" (
	flow1 INT PRIMARY KEY,
	density INT DEFAULT 0
);")

dbExecute(con, query)

query <- paste0("INSERT INTO ", 
								char_flows_pvalues,
								"(flow1) SELECT flow1 FROM col_numb_reachable_flows;")
dbExecute(con, query)
psql_create_index(con, char_flows_pvalues, col = c("flow1"))

# In each round of MSC:
psql_compare_directly_reachable(
	con = con,
	table_reachable_flows = char_reachable,
	table_random_reachable_flows = char_random_reachable_flows,
	table_reachable_flows_compared = char_flows_pvalues)



################################################################################
# END MONTECARLO-SIMULATION
################################################################################
# After last round of MSC, divide 'density' by '1+R'
query <- paste0("ALTER TABLE ",
								char_flows_pvalues,
								" ALTER COLUMN density TYPE DOUBLE PRECISION;")
dbExecute(con, query)

query <- paste0("UPDATE ", char_flows_pvalues,
" SET density = density / ", 
int_simulations,";")
dbExecute(con, query)

# Add column 'core_flow'
query <- paste0("ALTER TABLE ",
								char_flows_pvalues,
								" ADD COLUMN core_flow TEXT;")
dbExecute(con, query)


query <- paste0("UPDATE ",
								char_flows_pvalues,
								" SET core_flow = CASE
								WHEN density < ", int_alpha, 
								" THEN 'yes' 
								ELSE 'no'
								END;")
dbExecute(con, query)


