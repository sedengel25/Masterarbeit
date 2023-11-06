################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains step 1 of the SNN-flwo clustering
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/08_utils.R")


# Read shp-file for considered city
shp_cologne_streets <- st_read(path_shp_file_köln_strassen)
sf_road_segments_mls <- shp_cologne_streets$geometry

# Split multilinestrings into linestrings (get each single road segments)
list_sf_road_segments_ls <- split_multiline_in_line(sf_road_segments_mls)
sf_ls <- st_sfc(list_sf_road_segments_ls)


# Create network based on linestrings
network <- as_sfnetwork(sf_ls)

# Extract network's nodes
nodes <- network %>%
	as.data.table

# Extract network's edges
edges <- network %>%
	activate(edges) %>%
	as.data.table


# Create list of sub-networks distance matrices according to SNN-Paper
list_of_dts <- create_dist_mat(nodes = nodes, buffer_size = 1000)


# Create full distance matrix
dt_full_matrix <- rbindlist(list_of_dts)
dt_full_matrix <- dt_full_matrix %>% 
	distinct()
dt_full_matrix <- dt_full_matrix %>%
	mutate(from = as.integer(from),
				 to = as.integer(to))

write_rds(dt_full_matrix, path_dt_distance_matrix)
