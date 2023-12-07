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
char_city_prefix <- "col"
char_city <- "cologne"
char_osm2po <- paste0(char_city_prefix, "_2po_4pgr")
char_osm2po_subset <- paste0(char_city_prefix, "_2po_4pgr_subset")

char_shp <- paste0(char_city_prefix, "_stadtgrenzen_shp")
char_bbox <- paste0(char_city_prefix, "_bbox")

char_dist_mat <- paste0(char_city_prefix,"_",int_buffer, "_dist_mat")
char_dist_mat_red <- paste0(char_city_prefix,"_",int_buffer, "_dist_mat_red")
char_dist_mat_red_no_dup <- paste0(char_city_prefix,"_",int_buffer, "_dist_mat_red_no_dup")
################################################################################
# Use osm2po to create a network as psql-table
################################################################################
char_bat_file <- paste0(char_city, ".bat")

# Executing the bat-file of the chosen city creates a sql file with all edges
cmd_osm2po <- paste('cmd /c "cd /d', path_osm2po, '&&', char_bat_file, '"')
system(cmd_osm2po)


# Run the sql-file to get the corresponding table
sql_filename <- list.files(paste0(path_osm2po, "/", char_city_prefix, "/"), pattern = "*.sql")
sql_file <- paste0(path_osm2po, "/", char_city_prefix, "/", sql_filename)
cmd_execute_sql_file(path_to_sql_file = sql_file)

# Update geometry
psql_change_geometry_type(con, table = char_osm2po, crs = int_crs)
psql_update_srid(con, table = char_osm2po, crs = int_crs)

################################################################################
# Create smaller bounding-box for huge network
################################################################################
# Use shp2pgsql to turn shapefile of city's borders in psql-table
char_cmd_psql_shp_to_sql <- sprintf('"%s" -I -s %s "%s" public.%s > "%s"',
																		path_shp2psql_exe,
																		int_crs,
																		path_shp_col,
																		char_shp,
																		path_sql_col
)
cmd_write_sql_file(char_cmd_psql_shp_to_sql)
psql_drop_table_if_exists(con, char_shp)
cmd_execute_sql_file(path_sql_col)


psql_create_bbox(con, table_shp = char_shp, table_bbox = char_bbox)

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
all_to_all_shortest_paths_to_sqldb(con = con, dt = dt,
                                     char_dist_mat = char_dist_mat,
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


################################################################################
# Ablage
################################################################################


# city <- dt[1, "city"] %>% tolower()











# Config table names
char_city_prefix <- readLines(paste0(path_osm2po, "/", char_bat_file)) %>% strsplit(" ")
char_city_prefix <- char_city_prefix[[1]][5] %>% strsplit("=")
char_city_prefix <- char_city_prefix[[1]][2]



# char_mapped_o_points <- paste0(char_city_prefix, "_mapped_o_points")
# char_mapped_d_points <- paste0(char_city_prefix, "_mapped_d_points")


# 
# 
# 
# # Bring street-network-data to the same CRS as OD-points
# psql_change_geometry_type(con = con,
# 										 char_osm2po = char_osm2po,
# 										 crs = int_crs)
# 
# # Create  spatial indices to speed up calculations
# psql_create_spatial_index(con, char_table = char_osm2po)
# psql_create_spatial_index(con, char_table = char_o_table)
# psql_create_spatial_index(con, char_table = char_d_table)
# 
# 
# # Get sub street network by reducing network to area needed to cover OD-points
# psql_get_sub_street_network(con = con,
# 											 char_o_table = char_o_table,
# 											 char_d_table = char_d_table,
# 											 char_osm2po = char_osm2po,
# 											 char_osm2po_subset = char_osm2po_subset
#											 )





### Map OD-points onto the street network --------------------------------------
# Map Origin
psql_map_od_points_to_network(con = con, 
															table_mapped_points = char_mapped_o_points,
															table_unmapped_points = char_o_table,
															table_network = char_osm2po_subset)
# Map Destination
psql_map_od_points_to_network(con = con, 
															table_mapped_points = char_mapped_d_points,
															table_unmapped_points = char_d_table,
															table_network = char_osm2po_subset)

psql_create_index(con, char_table = char_mapped_o_points,
									col = c("id", "source", "target"))

psql_create_index(con, char_table = char_mapped_d_points,
									col = c("id", "source", "target"))
