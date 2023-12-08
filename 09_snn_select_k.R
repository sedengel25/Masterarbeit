################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file calc. network dist. based on mapped OP-points and distance-matrix
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/00_config_psql.R")
source("./src/09_utils.R")


################################################################################
# Configuration
################################################################################
int_crs <- 32632
char_city_prefix <- "col"
char_vis <- paste0(char_city_prefix, "_vis_flows")



################################################################################
# Read in clean scooter data
################################################################################
dt <- get_cleaned_trip_data(path_clean = path_dt_clustered_voi_cologne_06_05,
														path_org = path_dt_voi_cologne_06_05)

dt <- dt %>%
	mutate(
		weekday = wday(start_time, week_start = 1),
		start_hour = hour(start_time),
		pow_tod = case_when(
			weekday %in% 1:5 & start_hour >= 6 & start_hour <= 10 ~ 'wd_m',  # weekday morning
			weekday %in% 1:5 & start_hour >= 11 & start_hour <= 18 ~ 'wd_d',  # weekday day
			weekday %in% 1:5 & start_hour >= 19 & start_hour <= 23 ~ 'wd_e',  # weekday evening
			weekday %in% 1:5 & (start_hour >= 0 & start_hour <= 5) ~ 'wd_n',  # weekday night
			weekday %in% 6:7 & start_hour >= 6 & start_hour <= 10 ~ 'we_m',  # weekend morning
			weekday %in% 6:7 & start_hour >= 11 & start_hour <= 18 ~ 'we_d',  # weekend day
			weekday %in% 6:7 & start_hour >= 19 & start_hour <= 23 ~ 'we_e',  # weekend evening
			weekday %in% 6:7 & (start_hour >= 0 & start_hour <= 5) ~ 'we_n',  # weekend night
			TRUE ~ NA_character_  # NA for times that don't fit any category
		)
	)




char_pow_tod <- "wd_m"
dt <- dt %>%
	filter(pow_tod == char_pow_tod)


################################################################################
# Map points to street network
################################################################################
char_osm2po_subset <- paste0(char_city_prefix, "_2po_4pgr_subset")
char_origin <- paste0(char_city_prefix, "_origin_", char_pow_tod)
char_dest <- paste0(char_city_prefix, "_dest_", char_pow_tod)
char_origin_mapped <- paste0(char_city_prefix, "_origin_mapped_", char_pow_tod)
char_dest_mapped <- paste0(char_city_prefix, "_dest_mapped_", char_pow_tod)
char_dist_mat_red_no_dup <- paste0(char_city_prefix, "_dist_mat_red_no_dup")
char_origin_nd <- paste0(char_city_prefix, "_origin_nds_", char_pow_tod)
char_dest_nd <- paste0(char_city_prefix, "_dest_nds_", char_pow_tod)


# Create 2 psql-table for OD-points
psql_dt_to_od_tables(con, dt)



# Map Origin
psql_map_od_points_to_network(con = con, 
															table_mapped_points = char_origin_mapped,
															table_unmapped_points = char_origin,
															table_network = char_osm2po_subset)
# Map Destination
psql_map_od_points_to_network(con = con, 
															table_mapped_points = char_dest_mapped,
															table_unmapped_points = char_dest,
															table_network = char_osm2po_subset)

psql_create_index(con, table = char_origin_mapped,
									col = c("id", "line_id"))

psql_create_index(con, table = char_dest_mapped,
									col = c("id", "line_id"))



################################################################################
# Calculate network distances
################################################################################
char_origin_nd_no_dup <- paste0(char_city_prefix, "_origin_nds_no_dup_", char_pow_tod)
char_dest_nd_no_dup <- paste0(char_city_prefix, "_dest_nds_no_dup_", char_pow_tod)
char_flows_nd <- paste0(char_city_prefix, "_flows_nd")


dt_network <- RPostgres::dbReadTable(con, char_osm2po_subset) %>%
	mutate(m = km*1000) %>%
	select(id, source, target, m) 


dt_mapped_origins <- RPostgres::dbReadTable(con, char_origin_mapped) %>%
	select(id, line_id, distance_to_start, distance_to_end)



dt_mapped_dest <- RPostgres::dbReadTable(con, char_dest_mapped) %>%
	select(id, line_id, distance_to_start, distance_to_end)



# Calculate NDs of all Origin points
psql_calc_nd(con = con,
						table_mapped_points = char_origin_mapped,
						table_network = char_osm2po_subset,
						table_dist_mat =  char_dist_mat_red_no_dup,
						table_nd =  char_origin_nd)
psql_create_index(con, table = char_origin_nd, col = c("o_m", "o_n"))

# Calculate NDs of all Destination points
psql_calc_nd(con = con,
						 table_mapped_points = char_dest_mapped,
						 table_network = char_osm2po_subset,
						 table_dist_mat =  char_dist_mat_red_no_dup,
						 table_nd =  char_dest_nd)

query <- paste0("ALTER TABLE ",
								char_dest_nd,
								" RENAME COLUMN o_m TO d_m;")
dbExecute(con, query)

query <- paste0("ALTER TABLE ",
								char_dest_nd,
								" RENAME COLUMN o_n TO d_n;")
dbExecute(con, query)

psql_create_index(con, table = char_dest_nd, col = c("d_m", "d_n"))



# Remove duplicates of NDs of origin points
psql_remove_duplicates(con, old_table = char_origin_nd,
											 new_table = char_origin_nd_no_dup,
											 col = c("o_m", "o_n"))

psql_drop_old_if_new_exists(con, 
														old_table = char_origin_nd, 
														new_table = char_origin_nd_no_dup)

psql_create_index(con, table = char_origin_nd_no_dup, col = c("o_m", "o_n"))

# Remove duplicates of NDs of destination points
psql_remove_duplicates(con, old_table = char_dest_nd,
											 new_table = char_dest_nd_no_dup,
											 col = c("d_m", "d_n"))

psql_drop_old_if_new_exists(con, 
														old_table = char_dest_nd, 
														new_table = char_dest_nd_no_dup)

psql_create_index(con, table = char_dest_nd_no_dup, col = c("d_m", "d_n"))




# Create table with flow-nd-distances
psql_calc_flow_nds(con, table_o_nds = char_origin_nd_no_dup,
									 table_d_nds = char_dest_nd_no_dup,
									 table_flow_nds = char_flows_nd)


# Create indexes
psql_create_index(con, table = char_flows_nd, col = c("flow_m", "flow_n"))


################################################################################
# Select the right k for k-nearest neighbors
################################################################################
int_k_max <- 60

# psql_create_kmax_knn_tables(con, 
# 														k_max = int_k_max, 
# 														city_prefix = char_city_prefix,
# 														table_flows_nd = char_flows_nd)

dt_rkd <- create_rkd_dt(con, 
												k_max = int_k_max, 
												city_prefix = char_city_prefix)
ggplot() +
	geom_point(data = dt_rkd[-1,], aes(x = k, y = rkd))

int_k <- 45
char_k_nearest_flows <- paste0(char_city_prefix, "_", int_k, "_nearest_flows")
char_reachable <- paste0(char_city_prefix, "_", int_k, "_numb_reachable_flows")
char_common_flows <- paste0(char_city_prefix, "_", int_k, "_common_flows")

# Get number of common flows for each flow combination
psql_get_number_of_common_flows(con, 
																table_k_nearest_flows = char_k_nearest_flows,
																table_common_flows = char_common_flows)

psql_create_index(con, char_common_flows, col = c("flow1", "flow2", "common_flows"))


# Get number of directly reachable flows
psql_get_number_directly_reachable_flows(con, k = int_k, 
																				 table_common_flows = char_common_flows,
																				 table_reachable_flows = char_reachable)

