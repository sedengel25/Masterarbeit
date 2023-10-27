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
dt_table <- read_rds(path_dt_table)


list_of_ids <- choose_datasets(dt = dt_table,
															 days = 14,
															 provider = "BOLT",
															 city = "BERLIN")


dt <- combine_files_to_dt(list = list_of_files[list_of_ids[1]], 
													path = path_processed_data_1) 



################################################################################
# Save data
################################################################################
write_rds(x = dt, file = path_dt_bolt_berlin_05_12)

