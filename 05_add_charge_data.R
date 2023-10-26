################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file add charge-data from feather files to the datatable
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/05_utils.R")


# Reads processed data
dt <- read_rds(path_dt_tier_berlin_05_12)

# List raw files
list_feather_files <- list.files(path = path_feather_tier_berlin_05_12, 
																		 full.names = TRUE)

# Sort files
list_feather_files <- sort_files_datetime_asc(list = list_feather_files)

# Read in feather files
# feather_data <- lapply(list_feather_files, read_feather)

# feather_data <- read_rds("C:/r_projects/Masterarbeit/tier_feather_data.rds")


# Add column 'charge'
dt <- dt %>%
	mutate(charge = -1)

# Add column 'timestamp'
dt <- add_timestamp(dt = dt)




add_charge_col_tier(dt = dt, 
										input_path = path_feather_tier_berlin_05_12,
										output_path = path_dt_charge_tier_berlin_05_12)



