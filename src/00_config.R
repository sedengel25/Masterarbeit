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
library(sf)
library(rlang)
library(jsonlite)
library(R.utils)
library(feather)
library(stringi)
library(VennDiagram)
################################################################################
# Paths
################################################################################
# Raw data
path_trips <- here("data/raw/trips")
path_bolt_berlin_raw <- here("data/raw/raw/BOLT/BERLIN")

# Processed data
path_processed_data_1 <- here("data/processed/01")
path_processed_data_2 <- here("data/processed/02")
path_processed_data_3 <- here("data/processed/03")
path_processed_data_4 <- here("data/processed/04")
path_bolt_berlin_processed <- here(path_processed_data_4, "BOLT/BERLIN")

# External data
path_external_data <- here("data/external")



# Processed files
path_dt_table <- here(path_processed_data_2, "dt_table.rds")
path_dt_plot <- here(path_processed_data_2, "dt_plot.png")
path_dt <- here(path_processed_data_3,"dt.rds")
path_dt_charge <- here(path_processed_data_4,"dt_charge.rds")



# External files
path_shp_file_berlin <- here(path_external_data, "bezirksgrenzen.shp")



################################################################################
# Configurations
################################################################################
options(scipen = 999)
