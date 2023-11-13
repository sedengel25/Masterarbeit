################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file calculates the network distances between all OD-pairs
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/09_utils.R")
sf_origin <- read_rds(path_sf_origin)

df_origin <- create_df_od_points_dist_to_intersections(sf_points = sf_origin)
df_dest <- create_df_od_points_dist_to_intersections(sf_points = sf_dest)


# Exemplarisch die network-distance (ND) zwischen O_1 und O_2 berechnen
p_i <- df_origin[1, "edge_start"]
p_j <- df_origin[1, "edge_end"]
p_k <- df_origin[2, "edge_start"]
p_l <- df_origin[2, "edge_end"]


dt_dist_mat <- read_rds(path_dt_distance_matrix)

g <- graph_from_data_frame(
	data.frame(
		from = dt_dist_mat$from,
		to = dt_dist_mat$to,
		weight = dt_dist_mat$distance
	)
)
g <- as.undirected(g)

dt_dist_mat %>%
	filter(to==5)
shortest_paths(g, from = "1", to = "7")
