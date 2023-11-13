################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains step 1 of the SNN-flwo clustering
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/08_utils.R")

# Read local node distance matrix (shortest paths between node 1 and all others)
dt_shortest_paths <- read_rds("dt_shortest_paths.rds")
sf_cologne <- st_read(path_shp_file_köln_strassen)




sf_network <- dt_shortest_paths %>% 
	select(geom_edge) %>% 
	dplyr::rename(geometry = geom_edge) %>% 
	st_as_sf %>% 
	as_sfnetwork()



sf_nodes <- sf_network %>%
	as.data.table %>%
	st_as_sf

sf_nodes <- sf_nodes %>%
	mutate(id = 1:nrow(sf_nodes))

sf_edges <- sf_network %>%
	activate(edges) %>%
	as.data.table %>%
	st_as_sf




# Read in clean scooter trip data (only 'id' and 'ride)
dt_clean <- read_rds(path_dt_clustered_voi_cologne_06_05)

# Read in full original data
dt_org <- read_rds(path_dt_voi_cologne_06_05)

# Merge at 'id' and 'ride'
dt <- dt_clean %>%
	inner_join(dt_org, c("id", "ride"))

# Select morning-rush-dt to get a smaller dt
dt_wd_morning_rush <- dt %>%
	mutate(weekday = wday(start_time,week_start = 1),
				 start_hour = hour(start_time)) %>%
	filter(weekday <= 5,
				 start_hour > 6 & start_hour <= 10)




# Transform numeric coordinates into wgs84 coords
sf_origin <- transform_num_to_WGS84(dt = dt_wd_morning_rush,
																						 coords = c("start_loc_lon",
																						 					 "start_loc_lat"))

write_rds(sf_origin, "sf_origin.rds")

sf_dest <- transform_num_to_WGS84(dt = dt_wd_morning_rush,
																					 coords = c("dest_loc_lon",
																					 					 "dest_loc_lat"))

# # Assign crs of points to edges
st_crs(sf_edges) <- st_crs(sf_origin)
st_crs(sf_nodes) <- st_crs(sf_origin)

sf_edges <- st_transform(sf_edges, crs = 32632)
sf_nodes <- st_transform(sf_nodes, crs = 32632)
sf_origin <- st_transform(sf_origin, crs = 32632)
sf_dest <- st_transform(sf_dest, crs = 32632)

sf_origin <- map_points_on_road_network(sf_points = sf_origin, 
																				sf_linestrings = sf_edges)


write_rds(sf_origin, path_sf_origin)

sf_dest <- map_points_on_road_network(sf_points = sf_dest, 
																				sf_linestrings = sf_edges)


write_rds(sf_dest, path_sf_dest)
