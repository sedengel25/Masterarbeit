################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file maps OD-points onto the network created using postgresql & PostGIS
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/00_config_psql.R")
source("./src/08_utils.R")

################################################################################
# Configuration
################################################################################
int_crs <- 32632
int_buffer <- 5000
char_city <- "cologne"
char_city_prefix <- "col"

# Write to 'output' to use in further scripts
write_rds(int_buffer, file_rds_int_buffer)
write_rds(int_crs, file_rds_int_crs)
write_rds(char_city_prefix, file_rds_char_city_prefix)

# Write to 'dockerbuild' to use on VM
write_rds(int_crs, file_rds_docker_int_crs)
write_rds(int_buffer, file_rds_docker_int_buffer)
write_rds(char_city_prefix, file_rds_docker_char_city_prefix)


################################################################################
# Use osm2po to create a network as psql-table
################################################################################
char_osm2po <- paste0(char_city_prefix, "_2po_4pgr")
char_osm2po_subset <- paste0(char_city_prefix, "_2po_4pgr_subset")
char_bat_file <- paste0(char_city, ".bat")

# Executing the bat-file of the chosen city creates a sql file with all edges
cmd_osm2po <- paste('cmd /c "cd /d', path_osm2po, '&&', char_bat_file, '"')
system(cmd_osm2po)


# Run the sql-file to get the corresponding table
sql_filename <- list.files(paste0(path_osm2po, "/", char_city_prefix, "/"), pattern = "*.sql")
sql_file <- paste0(path_osm2po, "/", char_city_prefix, "/", sql_filename)
cmd_execute_sql_file(path_to_sql_file = sql_file)


# Transform coordinate system
srid <- psql_get_srid(con, table = char_osm2po)
psql_set_srid(con, table = char_osm2po, srid = srid)
psql_transform_coordinates(con, table = char_osm2po, crs = int_crs)
psql_update_srid(con, table = char_osm2po, crs = int_crs)



################################################################################
# Create smaller bounding-box for huge network
################################################################################
file_shp <- mget(paste0("file_shp_", char_city_prefix)) %>% as.character
char_shp <- paste0(char_city_prefix, "_stadtgrenzen_shp")
char_bbox <- paste0(char_city_prefix, "_bbox")


st_write(st_read(file_shp), con, 
				 char_shp,
				 schema = "public", 
				 append = FALSE)

# Get SRID
srid <- psql_get_srid(con, table = char_shp)
# Set SRID, otherwise ST_Transform won't work
psql_set_srid(con, table = char_shp, srid = srid)
# Change values of coordinates
psql_transform_coordinates(con, table = char_shp, crs = int_crs)
# Change SRID
psql_update_srid(con, table = char_shp, crs = int_crs)
# Create bounding box based on city's borders
psql_create_bbox(con, table_shp = char_shp, table_bbox = char_bbox)
# Turn polygon into linestring
psql_ls_to_polygon(con, table = char_bbox)

psql_create_spatial_index(con, table = char_bbox)

################################################################################
# Create the sub network based on bounding-box
################################################################################
psql_create_sub_network(con,
												table_full_network = char_osm2po,
												table_sub_network = char_osm2po_subset,
												table_bbox = char_bbox)

psql_create_spatial_index(con, table =  char_osm2po_subset)


psql_create_index(con, table  = char_osm2po_subset,
									col = c("id", "source", "target"))

################################################################################
# Calculate local node distance matrix
################################################################################
char_dist_mat <- paste0(char_city_prefix,"_",int_buffer, "_dist_mat")
char_dist_mat_red <- paste0(char_city_prefix,"_",int_buffer, "_dist_mat_red")
char_dist_mat_red_no_dup <- paste0(char_city_prefix,"_",int_buffer, "_dist_mat_red_no_dup")

# Read sub-street-network into datatable
dt <- RPostgres::dbReadTable(con, char_osm2po_subset) %>%
	mutate(m = km*1000) %>%
	select(id, source, target, m)

# Connect to miniconda to execute Python code
reticulate::conda_list(conda = "C:/Users/Seppi/AppData/Local/r-miniconda/_conda.exe")

# Import functions from python library networkx
reticulate::source_python("src/import_py_dijkstra_functions.py")

# Create a graph from the sub street network
g <- from_pandas_edgelist(df = dt,
													source = "source",
													target = "target",
													edge_attr = "m",
													edge_key = "id")


# Calculate all shortest paths between all nodes (stop after 'int_buffer'-m)
all_to_all_shortest_paths_to_sqldb(con = con, dt = dt,
                                     table = char_dist_mat,
                                     g = g, buffer = int_buffer)

# Create index for dist_mat
psql_create_index(con, 
									table = char_dist_mat, 
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
									table = char_dist_mat_red, 
									col = c("source, target"))

# Remove duplicates
psql_remove_duplicates(con,
											 old_table = char_dist_mat_red,
											 new_table = char_dist_mat_red_no_dup,
											 col = c("source", "target"))


psql_drop_old_if_new_exists(con, 
														old_table = char_dist_mat_red, 
														new_table = char_dist_mat_red_no_dup)


psql_create_index(con, 
									table = char_dist_mat_red_no_dup, 
									col = c("source, target"))


# Rename table
psql_rename_table(con,
									table_old_name = char_dist_mat_red_no_dup,
									table_new_name = char_dist_mat)


# Write sql-files to dockerbuild
cmd_write_sql_dump(table = char_osm2po_subset, 
									 data_sub_folder = path_processed_data_8)

cmd_write_sql_dump(table = char_dist_mat, 
									 data_sub_folder = path_processed_data_8)

