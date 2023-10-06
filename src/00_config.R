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

# Paths
raw_data_path <- here("data/raw//")
processed_data_1_path <- here("data/processed/01//")
processed_data_2_path <- here("data/processed/02//")

dt_table_path <- paste0(processed_data_1_path,"dt_table.rds")
dt_plot_path <- paste0(processed_data_1_path,"dt_plot.png")
dt_path <- paste0(processed_data_2_path,"dt.rds")


# Configurations
options(scipen = 999)
