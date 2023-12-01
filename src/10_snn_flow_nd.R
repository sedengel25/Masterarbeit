################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file calc. network dist. between flows and visualizes k-nearest flows
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/00_config_psql.R")
# source("./src/10_utils.R")

char_city_prefix <- "col"
char_mapped_o_points <- paste0(char_city_prefix, "_mapped_o_points")
char_mapped_d_points <- paste0(char_city_prefix, "_mapped_d_points")
char_flows_nd <- paste0(char_city_prefix, "_flows_nd")
char_common_flows <- paste0(char_city_prefix, "_common_flows")
char_vis <- paste0(char_city_prefix, "_vis_flows")
char_reachable <- paste0(char_city_prefix, "_numb_reachable_flows")

# Create table to check whether calculating flow-nds worked: CHECK
# query <- paste0("CREATE TABLE col_vis_flows AS
# SELECT origin.id,
#   ST_MakeLine(origin.closest_point_on_line , dest.closest_point_on_line) AS line_geom
# FROM ",  char_mapped_o_points, " origin
# INNER JOIN ", char_mapped_d_points, " dest ON origin.id = dest.id;")
# 
# dbExecute(con, query)

int_k <- 5
char_k_nearest_flows <- paste0(char_city_prefix,"_",k ,"_nearest_flows")
# Get k nearest flows for each flow
psql_get_k_nearest_flows(con, 
												 k = int_k, 
												 table_flows = char_flows_nd,
												 table_k_nearest_flows = char_k_nearest_flows)

psql_create_index(con, char_k_nearest_flows, col = c("flow_ref", "flow_other"))

# Get number of common flows for each flow combination
psql_get_number_of_common_flows(con, 
																table_k_nearest_flows = char_k_nearest_flows,
																table_common_flows = char_common_flows)
psql_check_indexes(con, char_common_flows)
# psql_create_index(con, char_common_flows, col = c("flow1", "flow2", "common_flows"))


# Get number of directly reachable flows
psql_get_number_directly_reachable_flows(con, k = int_k, 
																				 table_common_flows = char_common_flows,
																				 table_reachable_flows = char_reachable)

