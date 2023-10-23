################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file creates processed and edited rds-files based on the raw data
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/04_utils.R")


# Reads processed data
dt <- read_rds(path_dt_bolt_berlin_06_05)

create_feather_files(input_path = path_raw_bolt_berlin_06_05, 
										 output_path = path_processed_bolt_berlin_06_05)


# List raw files
list_raw_feather_files <- list.files(path = path_bolt_berlin_processed, 
																		 full.names = TRUE)

# Sort files
list_raw_feather_files <- sort_files_datetime_asc(list = list_raw_feather_files)

# Read in feather files
feather_data <- lapply(list_raw_feather_files, read_feather)

# Add column 'charge'
dt <- dt %>%
	mutate(charge = -1)




