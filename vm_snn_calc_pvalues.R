################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file creates rand. OD-flows, executes Monte Carlo-Simulation + calc. p-value
################################################################################
source("/usr/src/app/vm_config.R")
source("/usr/src/app/vm_utils.R")


################################################################################
# Configuration
################################################################################
char_city_prefix <- read_rds("/docker-entrypoint-initdb.d/city_prefix.rds")
char_pow_tod <- read_rds("/docker-entrypoint-initdb.d/pow_tod.rds")
int_buffer <- read_rds("/docker-entrypoint-initdb.d/buffer.rds")
int_k <- read_rds("/docker-entrypoint-initdb.d/k.rds")

int_m <- 5
int_simulations <- 99
int_alpha <- 0.05
int_numb_cores <- detectCores() 

################################################################################
# Create points on road network every 'int_m' meters
################################################################################
char_osm2po_subset <- paste0(char_city_prefix, "_2po_4pgr_subset")
char_osm2po_points <- paste0(char_city_prefix, "_2po_4pgr_points")
char_flows_nd <- paste0(char_city_prefix, "_", int_buffer, "_flows_nd")

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
# Montecarlo-Simulation
################################################################################
char_origin_rand <- paste0(char_city_prefix, "_origin_rand_", char_pow_tod)
char_dest_rand <- paste0(char_city_prefix, "_dest_rand_", char_pow_tod)
char_origin_nd_rand <- paste0(char_city_prefix, "_origin_nds_rand_", char_pow_tod)
char_dest_nd_rand <- paste0(char_city_prefix, "_dest_nds_rand_", char_pow_tod)
char_dist_mat <- paste0(char_city_prefix,"_",int_buffer, "_dist_mat")
char_flows_nd_rand <- paste0(char_city_prefix, "_flows_nd_rand")
char_k_nearest_flows_rand <- paste0(char_city_prefix, "_k_nearest_flows_rand")
char_common_flows_rand <- paste0(char_city_prefix, "_common_flows_rand")
char_reachable <- paste0(char_city_prefix, "_", int_k, "_numb_reachable_flows")
char_reachable_rand <- paste0(char_city_prefix, "_reachable_flows_rand")
char_flows_pvalues <- paste0(char_city_prefix, "_flows_pvalues")
char_flows_pvalues_rand <- paste0(char_city_prefix, "_flows_pvalues_rand")


for(rep in 1:int_simulations){
	cat("Rep: ",rep, "\n")
	start_time <- Sys.time()
	
	### 1. Create random points --------------------------------------------------
	int_rand_origin <- sample(int_max_id, int_max_flow)
	int_rand_dest <- sample(int_max_id, int_max_flow)

	psql_create_table_random_od_points(con, 
																		 table_random_od_points = char_origin_rand,
																		 table_all_points = char_osm2po_points,
																		 random_indices = int_rand_origin)
	
	psql_create_index(con, char_origin_rand, col = c("id", "line_id"))
	
	
	psql_create_table_random_od_points(con, 
																		 table_random_od_points = char_dest_rand,
																		 table_all_points = char_osm2po_points,
																		 random_indices = int_rand_dest)
	psql_create_index(con, char_dest_rand, col = c("id", "line_id"))
	
	end_time <- Sys.time()
	dur <- difftime(end_time, start_time, units = "mins")
	cat("Table with random OD-points created: ", dur, "\n")
	### 2. Calculate NDs between Points ------------------------------------------
	psql_parallel_calc_and_create_nd_tables(con = con,
																					table_points = char_origin_rand,
																					table_nd = char_origin_nd_rand)
	
	psql_parallel_calc_and_create_nd_tables(con = con,
																					table_points = char_dest_rand,
																					table_nd = char_dest_nd_rand)
	
	query <- paste0("ALTER TABLE ",
									char_dest_nd_rand,
									" RENAME COLUMN o_m TO d_m;")
	dbExecute(con, query)

	query <- paste0("ALTER TABLE ",
									char_dest_nd_rand,
									" RENAME COLUMN o_n TO d_n;")
	dbExecute(con, query)

	psql_create_index(con, char_dest_nd_rand, col = c("d_m", "d_n"))

	end_time <- Sys.time()
	dur <- difftime(end_time, start_time, units = "mins")
	cat("NDs between points calculated: ", dur, "\n")
	
	### 3. Calculate NDs between Flows -------------------------------------------
	start_time <- Sys.time()
	psql_calc_flow_nds(con = con,
										 table_o_nds = char_origin_nd_rand,
										 table_d_nds = char_dest_nd_rand,
										 table_flow_nds = char_flows_nd_rand)
	psql_create_index(con, char_flows_nd_rand, col = c("flow_m", "flow_n"))
	
	end_time <- Sys.time()
	dur <- difftime(end_time, start_time, units = "mins")
	cat("NDs between flows calculated: ", dur, "\n")
	
	### 4. Get KNN-Flows ---------------------------------------------------------
	start_time <- Sys.time()
	psql_get_k_nearest_flows(con = con,
													 k = int_k,
													 table_flows = char_flows_nd_rand,
													 table_k_nearest_flows = char_k_nearest_flows_rand)
	psql_create_index(con, char_k_nearest_flows_rand, col = c("flow_ref", "flow_other"))
	
	end_time <- Sys.time()
	dur <- difftime(end_time, start_time, units = "mins")
	cat("Table with k-nearest flows created: ", dur, "\n")
	
	### 5. Calculate common flows ------------------------------------------------
	start_time <- Sys.time()
	dt_knn_rand = dbReadTable(conn = con, 
														name = char_k_nearest_flows_rand) %>% as.data.table
	
	## preprocess nearest flow data, i.e., store all values as integer (memory saving), 
	## extract nearest flows per reference flow (nf.split), 
	## and produce helper objects (flow.ids and n)
	dt_knn_rand = dt_knn_rand %>% 
		mutate_all(as.integer)
	
	dt_knn_rand_split = split(dt_knn_rand$flow_other, 
														dt_knn_rand$flow_ref)
	dt_knn_rand_flow_ids = unique(dt_knn_rand$flow_ref)
	int_n_ids = length(dt_knn_rand_flow_ids)
	list_common_flows = parallel::mclapply(seq_len(int_n_ids - 1L), 
																				 helper_function_common_flows, 
																				 thresh.common = 1L, 
																				 mc.cores = int_numb_cores)
	#print(list_common_flows)
	dt_common_flows = do.call(rbind, list_common_flows) %>%
		as.data.table() %>%
		setNames(c("flow1", "flow2", "common_flows")) %>%
		filter(common_flows > int_k/2)
	t1 <- proc.time()
	dbWriteTable(conn = con, 
							 name = char_common_flows_rand, 
							 value = dt_common_flows,
							 overwrite = TRUE)
	t2 <- proc.time()
	cat("write common flow table: ", t2-t1, "\n")
	psql_create_index(con, char_common_flows_rand, col = c("flow1", "flow2"))

	end_time <- Sys.time()
	dur <- difftime(end_time, start_time, units = "mins")
	cat("Table with common flows created: ", dur, "\n")
	
	# 6. Calc. directly-reachable flows
	start_time <- Sys.time()

	psql_get_number_directly_reachable_flows(con = con,
																					 k = int_k,
																					 table_common_flows = char_common_flows_rand,
																					 table_reachable_flows = char_reachable_rand)
	psql_create_index(con, char_reachable_rand, col = c("flow1"))
	
	end_time <- Sys.time()
	dur <- difftime(end_time, start_time, units = "mins")
	cat("Column 'directly-reachable' added: ", dur, "\n")

	start_time <- Sys.time()
	if(rep==1){
		query <- paste0("DROP TABLE IF EXISTS ", char_flows_pvalues)
		dbExecute(con, query)
		
		query <- paste0("CREATE UNLOGGED TABLE ", 
										char_flows_pvalues,
										" (
			flow1 INT PRIMARY KEY,
			density INT DEFAULT 0
		);")
		
		dbExecute(con, query)
		
		query <- paste0("INSERT INTO ", 
										char_flows_pvalues,
										"(flow1) SELECT flow1 FROM ", char_reachable, ";")
		dbExecute(con, query)
		psql_create_index(con, char_flows_pvalues, col = c("flow1"))
	}
	
	psql_compare_directly_reachable(
		con = con,
		table_reachable_flows = char_reachable,
		table_random_reachable_flows = char_reachable_rand,
		table_reachable_flows_compared = char_flows_pvalues)
	
	end_time <- Sys.time()
	dur <- difftime(end_time, start_time, units = "mins")
	cat("SNN-densities compared & table updated: ", dur, "\n")
	query <- paste0("DROP TABLE IF EXISTS ", char_reachable_rand)
	dbExecute(con, query)
}
################################################################################
# Calculate p-values
################################################################################
# After last round of MSC, divide 'density' by '1+R'
query <- paste0("ALTER TABLE ",
								char_flows_pvalues,
								" ALTER COLUMN density TYPE DOUBLE PRECISION;")
dbExecute(con, query)

query <- paste0("UPDATE ", char_flows_pvalues,
								" SET density = density / ", 
								int_simulations + 1,";")
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


