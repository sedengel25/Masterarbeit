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
list_of_files <- list.files(processed_data_1_path)

# Choose the files you want to read in, based on the overview
dt_table <- read_rds(dt_table_path)


list_of_ids <- choose_datasets(dt = dt_table,
															 days = 14,
															 provider = "BOLT",
															 city = "BERLIN")

list_of_ids
dt <- combine_files_to_dt(list = list_of_files[list_of_ids[1]], 
													path = processed_data_1_path) 



################################################################################
# Save data
################################################################################
write_rds(x = dt, file = dt_path)

