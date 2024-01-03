################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# Configuration file for the packages needed and the filepath specifications
################################################################################

# Load libraries
library(dplyr)
library(data.table)
library(ggplot2)
library(writexl)
library(readr)
library(here)
library(dbscan)
library(lubridate)
library(cluster)
library(factoextra)
library(scales)
library(plotly)
library(ggdendro)
library(mclust)
library(leaflet)
library(leaflet.extras)
library(sf)
library(rlang)
library(jsonlite)
library(R.utils)
library(feather)
library(stringi)
library(ggvenn)
library(tidyr)
library(shiny)
library(htmlwidgets)
library(mapview)
library(magick)
library(gridExtra)
library(grid)
library(GGally)
library(ggforce)
library(patchwork)
library(rlist)
library(igraph)
library(sfnetworks)
library(plyr)
library(sp)
library(RPostgres)
library(reticulate)
library(purrr)
library(deps)
library(NCmisc)
library(lwgeom)
library(tidyverse)
library(parallel)
library(furrr)
################################################################################
# Paths
################################################################################
### Raw data -------------------------------------------------------------------
path_trips <- here::here("data/raw/trips")
path_raw_bolt_berlin_05_12 <- here::here("data/raw/raw/BOLT/BERLIN/05_12")
path_raw_bolt_berlin_06_05 <- here::here("data/raw/raw/BOLT/BERLIN/06_05")
path_raw_voi_berlin_06_05 <- here::here("data/raw/raw/VOI/BERLIN/06_05")
path_raw_voi_cologne_06_05 <- here::here("data/raw/raw/VOI/COLOGNE/06_05")
path_raw_tier_berlin_05_12 <- here::here("data/raw/raw/TIER/BERLIN/05_12")
path_raw_tier_munich_09_13 <- here::here("data/raw/raw/TIER/MUNICH/09_13")

### Processed data -------------------------------------------------------------
path_processed_data_1 <- here::here("data/processed/01")
path_processed_data_2 <- here::here("data/processed/02")
path_processed_data_3 <- here::here("data/processed/03")
path_processed_data_4 <- here::here("data/processed/04")
path_processed_data_5 <- here::here("data/processed/05")
path_processed_data_6 <- here::here("data/processed/06")
path_processed_data_7 <- here::here("data/processed/07")
path_processed_data_8 <- here::here("data/processed/08")
path_processed_data_9 <- here::here("data/processed/09")
path_processed_data_10 <- here::here("data/processed/10")
path_processed_data_11 <- here::here("data/processed/11")
path_processed_data_12 <- here::here("data/processed/12")


path_feather_bolt_berlin_05_12 <-
	here::here(path_processed_data_4, "BOLT/BERLIN/05_12")
path_feather_bolt_berlin_06_05 <-
	here::here(path_processed_data_4, "BOLT/BERLIN/06_05")
path_feather_voi_berlin_06_05 <-
	here::here(path_processed_data_4, "VOI/BERLIN/06_05")
path_feather_voi_cologne_06_05 <-
	here::here(path_processed_data_4, "VOI/COLOGNE/06_05")
path_feather_tier_berlin_05_12 <-
	here::here(path_processed_data_4, "TIER/BERLIN/05_12")
path_feather_tier_munich_09_13 <-
	here::here(path_processed_data_4, "TIER/MUNICH/09_13")

### External data --------------------------------------------------------------
path_external_data <- here::here("data/external")
path_osm2po <- "C:/osm2po-5.5.5"
path_dockerbuild <- "C:/dockerbuild/"

### Output ---------------------------------------------------------------------
path_output_07 <- here::here("output/07")

path_output_08 <- here::here("output/08")

path_output_09 <- here::here("output/09")

path_output_12 <- here::here("output/12")

path_heatmaps_bolt_berlin_05_12 <- here::here(path_output_07, "BOLT/BERLIN/05_12")

### Docker ---------------------------------------------------------------------
path_docker <- "C:/dockerbuild/"

################################################################################
# Create folders
################################################################################
path_variables <- ls(pattern = "^path")
lapply(mget(path_variables)[-length(path_variables)], function(paths) {
	for (x in paths) {
		print(paste("Current working directory:", getwd()))
		print(paste("Attempting to create:", x))
		if (!dir.exists(x)) {
			dir.create(x, recursive = TRUE)
		}
		print(paste("Directory after creation attempt:", getwd()))
	}
})



################################################################################
# Files
################################################################################
### Processed data -------------------------------------------------------------
# 3
file_rds_dt_table <- here::here(path_processed_data_2, "dt_table.rds")
file_rds_dt_plot <- here::here(path_processed_data_2, "dt_plot.png")
file_rds_dt_bolt_berlin_05_12 <-
	here::here(path_processed_data_3, "dt_bolt_berlin_05_12.rds")
file_rds_dt_bolt_berlin_06_05 <-
	here::here(path_processed_data_3, "dt_bolt_berlin_06_05.rds")
file_rds_dt_voi_berlin_06_05 <-
	here::here(path_processed_data_3, "dt_voi_berlin_06_05.rds")
file_rds_dt_voi_cologne_06_05 <-
	here::here(path_processed_data_3, "dt_voi_cologne_06_05.rds")
file_rds_dt_tier_berlin_05_12 <-
	here::here(path_processed_data_3, "dt_tier_berlin_05_12.rds")
file_rds_dt_tier_munich_09_13 <-
	here::here(path_processed_data_3, "dt_tier_munich_09_13.rds")


# 5
file_rds_dt_charge_bolt_berlin_05_12 <-
	here::here(path_processed_data_5, "dt_charge_bolt_berlin_05_12.rds")
file_rds_dt_charge_bolt_berlin_06_05 <-
	here::here(path_processed_data_5, "dt_charge_bolt_berlin_06_05.rds")
file_rds_dt_charge_voi_berlin_06_05 <-
	here::here(path_processed_data_5, "dt_charge_voi_berlin_06_05.rds")
file_rds_dt_charge_voi_cologne_06_05 <-
	here::here(path_processed_data_5, "dt_charge_voi_cologne_06_05.rds")
file_rds_dt_charge_tier_berlin_05_12 <-
	here::here(path_processed_data_5, "dt_charge_tier_berlin_05_12.rds")
file_rds_dt_charge_tier_munich_09_13 <-
	here::here(path_processed_data_5, "dt_charge_tier_munich_09_13.rds")

# 6
file_rds_dt_clustered_bolt_berlin_05_12 <-
	here::here(path_processed_data_6, "dt_clustered_bolt_berlin_05_12.rds")
file_rds_dt_clustered_bolt_berlin_06_05 <-
	here::here(path_processed_data_6, "dt_clustered_bolt_berlin_06_05.rds")
file_rds_dt_clustered_voi_cologne_06_05 <-
	here::here(path_processed_data_6, "dt_clustered_voi_cologne_06_05.rds")
file_rds_dt_clustered_tier_berlin_05_12 <-
	here::here(path_processed_data_6, "dt_clustered_tier_berlin_05_12.rds")

# 8
file_rds_sf_network_colgone <-
	here::here(path_processed_data_8, "sf_network_cologne.rds")

# 9
file_rds_sf_origin <- here::here(path_processed_data_9, "sf_origin.rds")
file_rds_rds_sf_dest <- here::here(path_processed_data_9, "sf_dest.rds")

# 12
file_csv_p_values <- here::here(path_processed_data_12, "p_values.csv")

### External data --------------------------------------------------------------
file_exe_psql <- "C:/Program Files/PostgreSQL/16/bin/psql.exe"
file_exe_shp2psql <- "C:/Program Files/PostgreSQL/16/bin/shp2pgsql.exe"
file_exe_pg_dump <- "C:/Program Files/PostgreSQL/16/bin/pg_dump.exe"
file_shp_col <- here::here(path_external_data, "col/Stadtbezirk.shp")
file_shp_ber <- here::here(path_external_data, "ber/bezirksgrenzen.shp")



### Output ---------------------------------------------------------------------
# 8
file_sql_col <- here::here(path_output_08, "col_stadtbezirk.sql")
# file_sql_ber <- here::here(path_output_08, "ber_stadtbezirk.sql")
file_rds_int_buffer <- here::here(path_output_08, "buffer.rds")
file_rds_int_crs <- here::here(path_output_08, "crs.rds")
file_rds_char_city_prefix <- here::here(path_output_08, "city_prefix.rds")

# 9
file_rds_int_k <- here::here(path_output_09, "k.rds")
file_rds_char_pow_tod <- here::here(path_output_09, "pow_tod.rds")


### Docker ---------------------------------------------------------------------
file_rds_docker_int_k <- paste0(path_docker, "data/k.rds")
file_rds_docker_int_buffer <- paste0(path_docker, "data/buffer.rds")
file_rds_docker_int_crs <- paste0(path_docker, "data/crs.rds")
file_rds_docker_char_city_prefix <- here::here(path_docker, "data/city_prefix.rds")
file_rds_docker_char_pow_tod <- paste0(path_docker, "data/pow_tod.rds")
################################################################################
# Configurations
################################################################################
options(scipen = 999)



cbPalette <-
	c(
		"#999999",
		"#E69F00",
		"#56B4E9",
		"#009E73",
		"#F0E442",
		"#0072B2",
		"#D55E00",
		"#CC79A7"
	)