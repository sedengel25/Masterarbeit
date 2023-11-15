################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file maps OD-points onto the network created using postgresql & PostGIS
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/09_utils.R")

# Read in sf_network
sf_network <- read_rds(path_sf_network_colgone)

sf_nodes <- extract_nodes_from_sf_network(sf_network = sf_network)

sf_edges <- extract_edges_from_sf_network(sf_network = sf_network)



dt <- get_cleaned_trip_data(path_clean = path_dt_clustered_voi_cologne_06_05,
														path_org = path_dt_voi_cologne_06_05)

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


sf_dest <- transform_num_to_WGS84(dt = dt_wd_morning_rush,
																					 coords = c("dest_loc_lon",
																					 					 "dest_loc_lat"))

# Assign crs of points to sf_edges & sf_nodes such that they have a crs
st_crs(sf_edges) <- st_crs(sf_origin)
st_crs(sf_nodes) <- st_crs(sf_origin)

# Transform crs of all sf-objects needed to 32632
sf_edges <- st_transform(sf_edges, crs = 32632)
sf_nodes <- st_transform(sf_nodes, crs = 32632)
sf_origin <- st_transform(sf_origin, crs = 32632)
sf_dest <- st_transform(sf_dest, crs = 32632)


sf_origin <- sf_origin %>%
	mutate(note = "-",
				 dist_to_from = -1,
				 dist_to_to = -1)

sf_origin <- map_points_on_road_network(sf_points = sf_origin, 
																				sf_linestrings = sf_edges)


write_rds(sf_origin, path_sf_origin)

sf_dest <- map_points_on_road_network(sf_points = sf_dest, 
																				sf_linestrings = sf_edges)


write_rds(sf_dest, path_sf_dest)
