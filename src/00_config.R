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
library(clusterCrit)
library(plotly)
library(ggdendro)
library(mclust)
library(leaflet)
library(sf)
library(rlang)
library(jsonlite)
library(R.utils)

# Paths
path_raw_data <- here("data/raw//")
path_processed_data_1 <- here("data/processed/01//")
path_processed_data_2 <- here("data/processed/02//")
path_processed_data_3 <- here("data/processed/03//")
path_processed_data_4 <- here("data/processed/04//")

path_dt_table <- paste0(path_processed_data_2, "dt_table.rds")
path_dt_plot <- paste0(path_processed_data_2, "dt_plot.png")
path_dt <- paste0(path_processed_data_3,"dt.rds")

path_external_data <- here("data/external//")
path_bolt_berlin <- paste0(path_external_data, "BOLT/BERLIN")
path_shp_file_berlin <- paste0(path_external_data, "bezirksgrenzen.shp")



# Configurations
options(scipen = 999)
