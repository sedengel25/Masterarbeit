################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file reads in the data and saves the combined data
################################################################################
source("./src/00_config.R")
source("./src/03_utils.R")


################################################################################
# Read all csv-files of e-scooter mobility data and combine it into a datatable
################################################################################
list_of_files <- list.files(path_processed_data_1)

# Choose the files you want to read in, based on the overview
dt_table <- read_rds(file_rds_dt_table)


list_of_ids <- choose_datasets(dt = dt_table,
															 days = 14,
															 provider = "TIER",
															 city = "MUNICH")


dt <- combine_files_to_dt(list = list_of_files[list_of_ids[4]], 
													path = path_processed_data_1) 
head(dt)



################################################################################
# Save data
################################################################################
write_rds(x = dt, file = file_rds_dt_tier_munich_09_13)

