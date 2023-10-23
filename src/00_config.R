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
#library(clusterCrit)
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
################################################################################
# Paths
################################################################################
# Raw data
path_trips <- here("data/raw/trips")
path_raw_bolt_berlin_05_12 <- here("data/raw/raw/BOLT/BERLIN/05_12")
path_raw_bolt_berlin_06_05 <- here("data/raw/raw/BOLT/BERLIN/06_05")

# Processed data
path_processed_data_1 <- here("data/processed/01")
path_processed_data_2 <- here("data/processed/02")
path_processed_data_3 <- here("data/processed/03")
path_processed_data_4 <- here("data/processed/04")
path_processed_data_5 <- here("data/processed/05")

path_processed_bolt_berlin_05_12 <-
	here(path_processed_data_4, "BOLT/BERLIN/05_12")
path_processed_bolt_berlin_06_05 <-
	here(path_processed_data_4, "BOLT/BERLIN/06_05")
path_processed_tier_berlin_05_12 <-
	here(path_processed_data_4, "tier/BERLIN/05_12")
# External data
path_external_data <- here("data/external")



# Processed files
path_dt_table <- here(path_processed_data_2, "dt_table.rds")
path_dt_plot <- here(path_processed_data_2, "dt_plot.png")
path_dt_bolt_berlin_05_12 <-
	here(path_processed_data_3, "dt_bolt_berlin_05_12.rds")
path_dt_bolt_berlin_06_05 <-
	here(path_processed_data_3, "dt_bolt_berlin_06_05.rds")
path_dt_tier_berlin_05_12 <-
	here(path_processed_data_3, "dt_tier_berlin_05_12.rds")



path_dt_charge_bolt_berlin_05_12 <-
	here(path_processed_data_4, "dt_charge_bolt_berlin_05_12.rds")


path_dt_clustered_bolt_berlin_05_12 <-
	here(path_processed_data_5, "dt_clustered_bolt_berlin_05_12.rds")
path_dt_clustered_bolt_berlin_06_05 <-
	here(path_processed_data_5, "dt_clustered_bolt_berlin_06_05.rds")
path_dt_clustered_tier_berlin_05_12 <-
	here(path_processed_data_5, "dt_clustered_tier_berlin_05_12.rds")


# External files
path_shp_file_berlin <- here(path_external_data, "bezirksgrenzen.shp")



################################################################################
# Configurations
################################################################################
options(scipen = 999)
