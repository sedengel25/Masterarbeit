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
################################################################################
# Paths
################################################################################
# Raw data
path_trips <- here::here("data/raw/trips")
path_raw_bolt_berlin_05_12 <- here::here("data/raw/raw/BOLT/BERLIN/05_12")
path_raw_bolt_berlin_06_05 <- here::here("data/raw/raw/BOLT/BERLIN/06_05")
path_raw_voi_berlin_06_05 <- here::here("data/raw/raw/VOI/BERLIN/06_05")
path_raw_voi_cologne_06_05 <- here::here("data/raw/raw/VOI/COLOGNE/06_05")
path_raw_tier_berlin_05_12 <- here::here("data/raw/raw/TIER/BERLIN/05_12")
path_raw_tier_munich_09_13 <- here::here("data/raw/raw/TIER/MUNICH/09_13")

# Processed data
path_processed_data_1 <- here::here("data/processed/01")
path_processed_data_2 <- here::here("data/processed/02")
path_processed_data_3 <- here::here("data/processed/03")
path_processed_data_4 <- here::here("data/processed/04")
path_processed_data_5 <- here::here("data/processed/05")
path_processed_data_6 <- here::here("data/processed/06")
path_processed_data_7 <- here::here("data/processed/07")
path_processed_data_8 <- here::here("data/processed/08")
path_processed_data_9 <- here::here("data/processed/09")


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

# External data
path_external_data <- here::here("data/external")

# Output
path_output_07 <- here::here("output/07")

################################################################################
# Files
################################################################################
# Processed data
path_dt_table <- here::here(path_processed_data_2, "dt_table.rds")
path_dt_plot <- here::here(path_processed_data_2, "dt_plot.png")
path_dt_bolt_berlin_05_12 <-
	here::here(path_processed_data_3, "dt_bolt_berlin_05_12.rds")
path_dt_bolt_berlin_06_05 <-
	here::here(path_processed_data_3, "dt_bolt_berlin_06_05.rds")
path_dt_voi_berlin_06_05 <-
	here::here(path_processed_data_3, "dt_voi_berlin_06_05.rds")
path_dt_voi_cologne_06_05 <-
	here::here(path_processed_data_3, "dt_voi_cologne_06_05.rds")
path_dt_tier_berlin_05_12 <-
	here::here(path_processed_data_3, "dt_tier_berlin_05_12.rds")
path_dt_tier_munich_09_13 <-
	here::here(path_processed_data_3, "dt_tier_munich_09_13.rds")



path_dt_charge_bolt_berlin_05_12 <-
	here::here(path_processed_data_5, "dt_charge_bolt_berlin_05_12.rds")
path_dt_charge_bolt_berlin_06_05 <-
	here::here(path_processed_data_5, "dt_charge_bolt_berlin_06_05.rds")
path_dt_charge_voi_berlin_06_05 <-
	here::here(path_processed_data_5, "dt_charge_voi_berlin_06_05.rds")
path_dt_charge_voi_cologne_06_05 <-
	here::here(path_processed_data_5, "dt_charge_voi_cologne_06_05.rds")
path_dt_charge_tier_berlin_05_12 <-
	here::here(path_processed_data_5, "dt_charge_tier_berlin_05_12.rds")
path_dt_charge_tier_munich_09_13 <-
	here::here(path_processed_data_5, "dt_charge_tier_munich_09_13.rds")


path_dt_clustered_bolt_berlin_05_12 <-
	here::here(path_processed_data_6, "dt_clustered_bolt_berlin_05_12.rds")
path_dt_clustered_bolt_berlin_06_05 <-
	here::here(path_processed_data_6, "dt_clustered_bolt_berlin_06_05.rds")
path_dt_clustered_voi_cologne_06_05 <-
	here::here(path_processed_data_6, "dt_clustered_voi_cologne_06_05.rds")
path_dt_clustered_tier_berlin_05_12 <-
	here::here(path_processed_data_6, "dt_clustered_tier_berlin_05_12.rds")


path_sf_network_colgone <-
	here::here(path_processed_data_8, "sf_network_cologne.rds")


path_sf_origin <- here::here(path_processed_data_9, "sf_origin.rds")
path_sf_dest <- here::here(path_processed_data_9, "sf_dest.rds")



# External data
# path_shp_file_berlin <- here::here(path_external_data, "shp/berlin/bezirksgrenzen.shp")
# path_shp_file_köln_strassen <-
# 	here::here(path_external_data, "shp/köln/gis_osm_roads_free_1.shp")
# 
# path_csv_cologne_shortest_paths <-
# 	here::here(path_external_data,
# 						 "shp/köln/combined_shortest_paths_202311091616.csv")
# 
# path_shp_file_köln_strassen_small <-
# 	here::here(path_external_data, "shp/köln/Strasse.shp")
path_osm2po <- "C:/osm2po-5.5.5"

path_psql <- "C:/Program Files/PostgreSQL/16/bin/psql.exe"
# Output
path_heatmaps_bolt_berlin_05_12 <- here::here(path_output_07, "BOLT/BERLIN/05_12")




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