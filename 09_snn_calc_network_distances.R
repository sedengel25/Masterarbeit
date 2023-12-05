################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file calc. network dist. based on mapped OP-points and distance-matrix
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/00_config_psql.R")
source("./src/09_utils.R")

char_city_prefix <- "col"
char_osm2po_subset <- paste0(char_city_prefix, "_2po_4pgr_subset")
char_mapped_o_points <- paste0(char_city_prefix, "_mapped_o_points")
char_mapped_d_points <- paste0(char_city_prefix, "_mapped_d_points")
char_dist_mat_red_no_dup <- paste0(char_city_prefix, "_dist_mat_red_no_dup")
char_o_nd <- paste0(char_city_prefix, "_o_nd")
char_d_nd <- paste0(char_city_prefix, "_d_nd")
char_o_nd_no_dup <- paste0(char_city_prefix, "_o_nd_no_dup")
char_d_nd_no_dup <- paste0(char_city_prefix, "_d_nd_no_dup")
char_flows_nd <- paste0(char_city_prefix, "_flows_nd")

dt_network <- RPostgres::dbReadTable(con, char_osm2po_subset) %>%
	mutate(m = km*1000) %>%
	select(id, source, target, m) 


dt_mapped_o <- RPostgres::dbReadTable(con, char_mapped_o_points) %>%
	select(id, line_id, distance_to_start, distance_to_end)



dt_mapped_d <- RPostgres::dbReadTable(con, char_mapped_d_points) %>%
	select(id, line_id, distance_to_start, distance_to_end)



## Calculate NDs of all Origin points
psql_calc_nd(con = con,
						table_mapped_points = char_mapped_o_points,
						table_network = char_osm2po_subset,
						table_dist_mat =  char_dist_mat_red_no_dup,
						table_nd =  char_o_nd)


## Calculate NDs of all Destination points
psql_calc_nd(con = con,
						 table_mapped_points = char_mapped_d_points,
						 table_network = char_osm2po_subset,
						 table_dist_mat =  char_dist_mat_red_no_dup,
						 table_nd =  char_o_nd)




# Check indexes on char_o_nd
psql_check_indexes(con, char_o_nd)



# Remove duplicates of NDs of origin points
psql_remove_duplicates(con, old_table = char_o_nd,
											 new_table = char_o_nd_no_dup,
											 col = c("o_m", "o_n"))

psql_drop_old_if_new_exists(con, 
														old_table = char_o_nd, 
														new_table = char_o_nd_no_dup)


# Remove duplicates of NDs of destination points
psql_remove_duplicates(con, old_table = char_d_nd,
											 new_table = char_d_nd_no_dup,
											 col = c("o_m", "o_n"))

psql_drop_old_if_new_exists(con, 
														old_table = char_d_nd, 
														new_table = char_d_nd_no_dup)

# Create indexes
psql_create_index(con, char_table = char_o_nd_no_dup, col = c("o_m", "o_n"))
psql_create_index(con, char_table = char_d_nd_no_dup, col = c("o_m", "o_n"))


# Create table with flow-nd-distances
psql_calc_flow_nds(con, table_o_nds = char_o_nd_no_dup,
									 table_d_nds = char_d_nd_no_dup,
									 table_flow_nds = char_flows_nd)


# Create indexes
psql_create_index(con, char_table = char_flows_nd, col = c("flow_m", "flow_n"))


