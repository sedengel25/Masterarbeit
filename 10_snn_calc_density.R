################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file executes definition 1-3 of the snn-cluster-concept
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/00_config_psql.R")


int_k <- read_rds(file_rds_int_k)
char_k_nearest_flows <- paste0(char_city_prefix, "_", int_k, "_nearest_flows")
char_reachable <- paste0(char_city_prefix, "_", int_k, "_numb_reachable_flows")
char_common_flows <- paste0(char_city_prefix, "_", int_k, "_common_flows")


psql_get_k_nearest_flows(con,
												 k = int_k,
												 table_flows = char_flows_nd,
												 table_k_nearest_flows = char_k_nearest_flows)

psql_create_index(con, char_k_nearest_flows, col = c("flow_ref", "flow_other"))

# Get number of common flows for each flow combination
psql_get_number_of_common_flows(con, 
																table_k_nearest_flows = char_k_nearest_flows,
																table_common_flows = char_common_flows)

psql_create_index(con, char_common_flows, col = c("flow1", "flow2", "common_flows"))


# Get number of directly reachable flows
psql_get_number_directly_reachable_flows(con, k = int_k, 
																				 table_common_flows = char_common_flows,
																				 table_reachable_flows = char_reachable)


cmd_write_sql_dump(table = char_flows_nd, 
									 data_sub_folder = path_processed_data_9)


cmd_write_sql_dump(table = char_reachable,
									 data_sub_folder = path_processed_data_9)
