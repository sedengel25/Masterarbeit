################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file calculates the network distances between all OD-pairs
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/10_utils.R")


# Read in mapped OD-points 
dt_origin <- read_rds(path_dt_mapped_origin)
dt_dest <- read_rds(path_dt_mapped_dest)

sf_origin <- st_as_sf(dt_origin)

sf_origin <- sf_origin %>%
	mutate(id = c(1:nrow(sf_origin)))

sf_dest <- st_as_sf(dt_dest)

sf_dest <- sf_dest %>%
	mutate(id = c(1:nrow(sf_dest)))

# Read in street network
sf_ls_network <-
	shp_network_to_linestrings(shp_file = path_shp_file_köln_strassen)
st_crs(sf_ls_network) <- st_crs(sf_origin)


# Check whether points are actually mapped onto network
# buffer <- st_buffer(sf_origin[5, "geometry"], 100)
# sf_intersec_ls <- st_intersection(buffer, sf_ls_network)
# sf_intersec_pts <- st_intersection(buffer, sf_origin)
# ggplot() +
# 	geom_sf(data = sf_intersec) +
# 	geom_sf(data = sf_intersec_pts)






# Create network based on linestrings
network <- as_sfnetwork(sf_ls_network)


# Extract network's nodes
nodes <- network %>%
	as.data.table

nodes <- nodes %>%
	mutate(id = c(1:nrow(nodes)))


sf_network_nodes <- st_as_sf(nodes)

st_geometry(sf_network_nodes) <- "geometry"


# Extract network's edges
sf_network_edges <- network %>%
	activate(edges) %>%
	as.data.table %>%
	st_as_sf
st_geometry(sf_network_edges) <- "geometry"




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
