################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file maps the OD-points on the street network
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/09_utils.R")


# Read in clean data
dt_clean <- read_rds(path_dt_clustered_voi_cologne_06_05)
dt_org <- read_rds(path_dt_voi_cologne_06_05)


dt <- dt_clean %>%
	inner_join(dt_org, c("id", "ride"))

dt_wd_morning_rush <- dt %>%
	mutate(weekday = wday(start_time,week_start = 1),
				 start_hour = hour(start_time)) %>%
	filter(weekday <= 5,
				 start_hour > 6 & start_hour <= 10)


# Read shp-file for considered city
shp_cologne_streets <- st_read(path_shp_file_köln_strassen)
sf_road_segments_mls <- shp_cologne_streets$geometry

# Split multilinestrings into linestrings (get each single road segments)
list_sf_road_segments_ls <- split_multiline_in_line(sf_road_segments_mls)
sf_ls <- st_sfc(list_sf_road_segments_ls)
sf_ls %>% st_geometry_type %>% unique

# Transform numeric coordinates into utm32 coords
sf_origin <- transform_num_to_WGS84_to_UTM32(dt = dt_wd_morning_rush,
																						 coords = c("start_loc_lon",
																						 					  "start_loc_lat"))

sf_dest <- transform_num_to_WGS84_to_UTM32(dt = dt_wd_morning_rush,
																						 coords = c("dest_loc_lon",
																						 					 "dest_loc_lat"))
# Assign crs of points to lines
st_crs(sf_ls) <- st_crs(sf_origin)

sf_origin <- map_points_on_road_network(sf_points = sf_origin, buffer_size = 50)
write_rds(sf_origin, path_dt_mapped_origin)

sf_dest <- map_points_on_road_network(sf_points = sf_dest, buffer_size = 50)
write_rds(sf_dest, path_dt_mapped_dest)

