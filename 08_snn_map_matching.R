################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains step 1 of the SNN-flwo clustering
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/08_utils.R")


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




shp_cologne_streets <- st_read(path_shp_file_köln_strassen)
sf_road_segments_mls <- shp_cologne_streets$geometry
list_sf_road_segments_ls <- split_multiline_in_line(sf_road_segments_mls)
sf_ls <- st_sfc(list_sf_road_segments_ls)


# 1.: Map OD-points onto network (closest road-segment)
#MISSING

# 2.: Calculate local road node distance matrix
# All linestrings of cologne road network



sf_test <- sf_ls[1:100]



network <- as_sfnetwork(sf_test)

nodes <- network %>%
	as.data.table


for(i in 1:10){
	node <- nodes[i] %>% st_as_sf
	node <- node$x
	buffer <- st_buffer(node, 200)
	intersec <- st_intersection(buffer, sf_ls)
	intersec_ls <- intersec[st_geometry_type(intersec)=="LINESTRING"]
	print(intersec_ls)
	network <- as_sfnetwork(intersec_ls)
	print(network)
	break
}

plot(network)
edges <- network %>%
	activate(edges) %>%
	as.data.table

num_edges_weight <- edges$x %>% st_as_sf %>% st_length

#GOAL:
edges
g <- graph_from_data_frame(
	data.frame(
		from = edges$from,
		to = edges$to,
		weight = num_edges_weight
	)
)

# 3. Calc. distance between each OD-pair and nodes on the same path
#MISSING





