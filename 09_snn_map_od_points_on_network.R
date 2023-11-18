################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file maps OD-points onto the network created using postgresql & PostGIS
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/09_utils.R")

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
# Configure connection to PostgreSQL database
user <- "postgres"
pw <- "Wurstsalat1996"
dbname <- "snn"
host <- "localhost" 
port <- "5432"
con <- connect_to_postgresql_db(user = user,
																pw = pw,
																dbname = dbname,
																host = host)

# List executable .bat-files from osm2po
list.files(path_osm2po, pattern = "*.bat")
char_bat_file <- "cologne.bat"
char_city_prefix <-readLines(paste0(path_osm2po, "/", char_bat_file)) %>% strsplit(" ")
char_city_prefix <- char_city_prefix[[1]][5] %>% strsplit("=")
char_city_prefix <- char_city_prefix[[1]][2]

# Exectung the bat-file of the chosen city creates a sql file with all edges
# of the street network in the newly created folder 'ber', 'mun', 'col' or 'hh'
# Use paste to construct the command
cmd_osm2po <- paste('cmd /c "cd /d', path_osm2po, '&&', char_bat_file, '"')
system(cmd_osm2po)


# Run the sql-file to get the corresponding table
sql_filename <- list.files(paste0(path_osm2po, "/", char_city_prefix, "/"), pattern = "*.sql")
sql_file <- paste0(path_osm2po, "/", char_city_prefix, "/", sql_filename)
Sys.setenv(PGPASSWORD = pw)
cmd_psql <- sprintf('"%s" -h %s -p %s -d %s -U %s -f "%s"', 
										path_psql, host, port, dbname, user, sql_file)


# dbExecute(con, "CREATE EXTENSION postgis;")
system(cmd_psql)
Sys.unsetenv("PGPASSWORD")


# Reduce the streetwork to the necessary bounding box of OD-points
char_o_table <- paste0(char_city_prefix, "_o_points_", char_pow_tod)
char_d_table <- paste0(char_city_prefix, "_d_points_", char_pow_tod)
# 1. Create point-table for origin- & destination-points if not already exist

# O-points
if (dbExistsTable(con, paste0(char_o_table))) {
	dbRemoveTable(con, paste0(char_o_table))
	dbWriteTable(con, paste0(char_o_table), sf_origin)
} else {
	dbWriteTable(con, paste0(char_o_table), sf_origin)
}

# D-points
if (dbExistsTable(con, paste0(char_d_table))) {
	dbRemoveTable(con,  paste0(char_d_table))
	dbWriteTable(con,  paste0(char_d_table), sf_origin)
} else {
	dbWriteTable(con,  paste0(char_d_table), sf_origin)
}

table_osm2po <- paste0(char_city_prefix, "_2po_4pgr")
table_osm2po_subset <- paste0(char_city_prefix, "_2po_4pgr_subset")

change_geometry_type(con = con,
										 table_osm2po = table_osm2po,
										 crs = int_crs)

create_spatial_indices(con = con,
											 table_osm2po = table_osm2po,
											 o_table = char_o_table,
											 d_table = char_d_table)

get_sub_street_network(con = con,
											 o_table = char_o_table,
											 d_table = char_d_table,
											 table_osm2po = table_osm2po,
											 table_osm2po_subset = table_osm2po_subset
											 )

# Create spatial index
query <- paste0("CREATE INDEX ON ",  table_osm2po_subset, " USING GIST (geometry);")
dbExecute(con, query)



table_mapped_o_points <- paste0(char_city_prefix, "_mapped_o_points")
table_mapped_d_points <- paste0(char_city_prefix, "_mapped_d_points")

# Map Origin
map_od_points_to_network(con = con, 
												 table_mapped_points = table_mapped_o_points,
												 char_od_table = char_o_table,
												 table_osm2po_subset = table_osm2po_subset)
# Map Destination
map_od_points_to_network(con = con, 
												 table_mapped_points = table_mapped_d_points,
												 char_od_table = char_d_table,
												 table_osm2po_subset = table_osm2po_subset)


# Calculate Local Node Distance Matrix
dt <- RPostgres::dbReadTable(con, "col_2po_4pgr_subset") %>%
	mutate(m = km*1000) %>%
	select(id, source, target, m) 

head(dt)


# install_miniconda()
conda_list(conda = "C:/Users/Seppi/AppData/Local/r-miniconda/_conda.exe")

source_python("src/import_py_dijkstra_functions.py")
g <- from_pandas_edgelist(df = dt, 
													source = "source", 
													target = "target",
													edge_attr = "m",
													edge_key = "id")

all_to_all_shortest_paths_to_sqldb(con = con, dt =dt, g = g, buffer = 5000)
