################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file calc. network dist. between flows and visualizes k-nearest flows
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/00_config_psql.R")
source("./src/10_utils.R")

char_city_prefix <- "col"
char_mapped_o_points <- paste0(char_city_prefix, "_mapped_o_points")
char_mapped_d_points <- paste0(char_city_prefix, "_mapped_d_points")
char_flows_nd <- paste0(char_city_prefix, "_flows_nd")

query <- paste0("SELECT
  origin.id,
  ST_MakeLine(origin.closest_point_on_line , dest.closest_point_on_line) AS line_geom
FROM ",  char_mapped_o_points, "origin
INNER JOIN ", char_mapped_d_points, "ON origin.id = dest.id;")