################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file maps OD-points onto the network created using postgresql & PostGIS
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/00_config_psql.R")
source("./src/08_utils.R")

dt <- get_cleaned_trip_data(path_clean = path_dt_clustered_voi_cologne_06_05,
														path_org = path_dt_voi_cologne_06_05)

city <- dt[1, "city"] %>% tolower()

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


int_crs <- 32632

# Transform numeric coordinates into wgs84 coords
sf_origin <- transform_num_to_WGS84(dt = dt,
																		coords = c("start_loc_lon",
																							 "start_loc_lat"))

sf_origin <- sf_origin %>%
	mutate(id = 1:nrow(sf_origin))

sf_origin <- st_transform(sf_origin, crs = int_crs)

sf_dest <- transform_num_to_WGS84(dt = dt,
																	coords = c("dest_loc_lon",
																						 "dest_loc_lat"))

sf_dest <- sf_dest %>%
	mutate(id = 1:nrow(sf_dest))

sf_dest <- st_transform(sf_dest, crs = int_crs)


################################################################################
# osm2po, PostgreSQL & PostGIS
################################################################################
# List executable .bat-files from osm2po
# list.files(path_osm2po, pattern = "*.bat")
char_bat_file <- paste0(city, ".bat")


# Config table names
char_city_prefix <- readLines(paste0(path_osm2po, "/", char_bat_file)) %>% strsplit(" ")
char_city_prefix <- char_city_prefix[[1]][5] %>% strsplit("=")
char_city_prefix <- char_city_prefix[[1]][2]
char_o_table <- paste0(char_city_prefix, "_o_points_", char_pow_tod)
char_d_table <- paste0(char_city_prefix, "_d_points_", char_pow_tod)
char_osm2po <- paste0(char_city_prefix, "_2po_4pgr")
char_osm2po_subset <- paste0(char_city_prefix, "_2po_4pgr_subset")
char_mapped_o_points <- paste0(char_city_prefix, "_mapped_o_points")
char_mapped_d_points <- paste0(char_city_prefix, "_mapped_d_points")
char_dist_mat <- paste0(char_city_prefix, "_dist_mat")
char_dist_mat_red <- paste0(char_city_prefix, "_dist_mat_red")
char_dist_mat_red_no_dup <- paste0(char_city_prefix, "_dist_mat_red_no_dup")


# Executing the bat-file of the chosen city creates a sql file with all edges
cmd_osm2po <- paste('cmd /c "cd /d', path_osm2po, '&&', char_bat_file, '"')
system(cmd_osm2po)


# Run the sql-file to get the corresponding table
sql_filename <- list.files(paste0(path_osm2po, "/", char_city_prefix, "/"), pattern = "*.sql")
sql_file <- paste0(path_osm2po, "/", char_city_prefix, "/", sql_filename)
Sys.setenv(PGPASSWORD = pw)
cmd_psql <- sprintf('"%s" -h %s -p %s -d %s -U %s -f "%s"', 
										path_psql, host, port, dbname, user, sql_file)
system(cmd_psql)
Sys.unsetenv("PGPASSWORD")


### Reduce the streetwork to the necessary bounding box of OD-points -----------
# Create table for Origin-Points
if (dbExistsTable(con, paste0(char_o_table))) {
	dbRemoveTable(con, paste0(char_o_table))
	dbWriteTable(con, paste0(char_o_table), sf_origin)
} else {
	dbWriteTable(con, paste0(char_o_table), sf_origin)
}

# Create table for Destination-Points
if (dbExistsTable(con, paste0(char_d_table))) {
	dbRemoveTable(con,  paste0(char_d_table))
	dbWriteTable(con,  paste0(char_d_table), sf_dest)
} else {
	dbWriteTable(con,  paste0(char_d_table), sf_dest)
}



# Bring street-network-data to the same CRS as OD-points
change_geometry_type(con = con,
										 char_osm2po = char_osm2po,
										 crs = int_crs)

# Create  spatial indices to speed up calculations
create_spatial_indices(con = con,
											 char_osm2po = char_osm2po,
											 char_o_table = char_o_table,
											 char_d_table = char_d_table)

# Get sub street network by reducing network to area needed to cover OD-points
get_sub_street_network(con = con,
											 char_o_table = char_o_table,
											 char_d_table = char_d_table,
											 char_osm2po = char_osm2po,
											 char_osm2po_subset = char_osm2po_subset
											 )

# Create spatial index for sub street network
query <- paste0("CREATE INDEX ON ",  char_osm2po_subset, " USING GIST (geometry);")
dbExecute(con, query)

# Create btree index for all id-columns in sub street network
query <- paste0("CREATE INDEX idx_col_2po_4pgr_subset ON ",  
								char_osm2po_subset, 
								" USING btree (id, source, target);")
dbExecute(con, query)

# Drop original street network if sub street network - table exists
query <- paste0("IF EXISTS ", char_osm2po_subset,
								" DROP TABLE ", 
								char_osm2po, 
								";")

dbExecute(con, query)

### Map OD-points onto the street network --------------------------------------
# Map Origin
map_od_points_to_network(con = con, 
												 char_mapped_o_points = char_mapped_o_points,
												 char_od_table = char_o_table,
												 char_osm2po_subset = char_osm2po_subset)
# Map Destination
map_od_points_to_network(con = con, 
												 char_mapped_o_points = char_mapped_d_points,
												 char_od_table = char_d_table,
												 char_osm2po_subset = char_osm2po_subset)


# Create index for mapped od-points
query <- paste0("CREATE INDEX idx_col_mapped_o_points_id ON ",  
								char_mapped_o_points, 
								 " USING btree (id, line_id);")
dbExecute(con, query)

query <- paste0("CREATE INDEX idx_col_mapped_o_points_id ON ",  
								char_mapped_d_points, 
								" USING btree (id, line_id);")
dbExecute(con, query)


### Calculate Local Node Distance Matrix ---------------------------------------
# Read sub street network from psql-server
dt <- RPostgres::dbReadTable(con, char_osm2po_subset) %>%
	mutate(m = km*1000) %>%
	select(id, source, target, m)

# Connect to miniconda to execute Python code
conda_list(conda = "C:/Users/Seppi/AppData/Local/r-miniconda/_conda.exe")

# Import functions from python library networkx
source_python("src/import_py_dijkstra_functions.py")

# Create a graph from the sub street network
g <- from_pandas_edgelist(df = dt,
													source = "source",
													target = "target",
													edge_attr = "m",
													edge_key = "id")


# Calculate all shortest paths between all nodes (stop after 5000m)
# all_to_all_shortest_paths_to_sqldb(con = con, dt = dt, 
#                                      char_dist_mat = char_dist_mat, 
#                                      g = g, buffer = 5000)

# Create index for dist_mat
psql_create_index(con, 
									char_table = char_dist_mat, 
									col = c("source, target"))

# Remove unnecessary rows
psql_where_source_smaller_target(con, 
																 old_table = char_dist_mat, 
																 new_table = char_dist_mat_red)


psql_drop_old_if_new_exists(con, 
														old_table = char_dist_mat, 
														new_table = char_dist_mat_red)

# Create index for dist_mat_red
psql_create_index(con, 
									char_table = char_dist_mat_red, 
									col = c("source, target"))

# Remove duplicates
psql_remove_duplicates(con,
											 old_table = char_dist_mat_red,
											 new_table = char_dist_mat_red_no_dup,
											 col = c("source", "target"))


psql_drop_old_if_new_exists(con, 
														old_table = char_dist_mat_red, 
														new_table = char_dist_mat_red_no_dup)