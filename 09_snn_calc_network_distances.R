################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file calc. network dist. based on mapped OP-points and distance-matrix
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/00_config_psql.R")
source("./src/09_utils.R")

char_city_prefix <- "col"
char_osm2po_subset <- paste0(char_city_prefix, "_2po_4pgr_subset")
char_mapped_o_points <- paste0(char_city_prefix, "_mapped_o_points")
char_mapped_d_points <- paste0(char_city_prefix, "_mapped_d_points")
char_dist_mat <- paste0(char_city_prefix, "_dist_mat")

dt_network <- RPostgres::dbReadTable(con, char_osm2po_subset) %>%
	mutate(m = km*1000) %>%
	select(id, source, target, m) 


dt_mapped_o <- RPostgres::dbReadTable(con, char_mapped_o_points) %>%
	select(id, line_id, distance_to_start, distance_to_end)



dt_mapped_d <- RPostgres::dbReadTable(con, char_mapped_d_points) %>%
	select(id, line_id, distance_to_start, distance_to_end)


# dt_dist_mat <- RPostgres::dbReadTable(con, char_dist_mat)


## Calculate NDs of all Origin points
# calc_nd(con = con, 
# char_mapped_points = char_mapped_o_points, 
# char_osm2po_subset = char_osm2po_subset, 
# char_dist_mat = char_dist_mat)


## Calculate NDs of all Destination points
# calc_nd(con = con, 
# char_mapped_points = char_mapped_d_points, 
# char_osm2po_subset = char_osm2po_subset, 
# char_dist_mat = char_dist_mat)

